/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009, 2010 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "RobinhoodConfig.h"
#include "RobinhoodLogs.h"
#include "RobinhoodMisc.h"
#include "entry_processor.h"
#include "entry_proc_tools.h"
#include <errno.h>
#include <time.h>

/* declaration of EntryProc functions of pipeline */
int            EntryProc_get_fid( struct entry_proc_op_t *, lmgr_t * );
int            EntryProc_get_info_db( struct entry_proc_op_t *, lmgr_t * );
int            EntryProc_get_info_fs( struct entry_proc_op_t *, lmgr_t * );
int            EntryProc_reporting( struct entry_proc_op_t *, lmgr_t * );
int            EntryProc_db_apply( struct entry_proc_op_t *, lmgr_t * );
int            EntryProc_chglog_clr( struct entry_proc_op_t *, lmgr_t * );
int            EntryProc_rm_old_entries( struct entry_proc_op_t *, lmgr_t * );


pipeline_stage_t entry_proc_pipeline[] = {
    {STAGE_GET_FID, "STAGE_GET_FID", EntryProc_get_fid,
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC, 0},
    {STAGE_GET_INFO_DB, "STAGE_GET_INFO_DB", EntryProc_get_info_db,
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC | STAGE_FLAG_ID_CONSTRAINT, 0},
    {STAGE_GET_INFO_FS, "STAGE_GET_INFO_FS", EntryProc_get_info_fs,
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC, 0},
    {STAGE_REPORTING, "STAGE_REPORTING", EntryProc_reporting,
     STAGE_FLAG_PARALLEL | STAGE_FLAG_ASYNC, 0},
    {STAGE_DB_APPLY, "STAGE_DB_APPLY", EntryProc_db_apply,
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC, 0},

    /* only 1 thread here because commiting records must be sequential (in the same order as changelog) */
    {STAGE_CHGLOG_CLR, "STAGE_CHGLOG_CLR", EntryProc_chglog_clr,
     STAGE_FLAG_SEQUENTIAL | STAGE_FLAG_SYNC, 1},

    /* this step is for mass update / mass remove operations when starting/ending a FS scan. */
    {STAGE_RM_OLD_ENTRIES, "STAGE_RM_OLD_ENTRIES", EntryProc_rm_old_entries,
     STAGE_FLAG_SEQUENTIAL | STAGE_FLAG_SYNC, 0}
};


/* for entries from FS scan, we must get the entry ID associated to path */
int EntryProc_get_fid( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;
    entry_id_t     tmp_id;

    if ( !p_op->entry_attr_is_set || !ATTR_MASK_TEST( &p_op->entry_attr, fullpath ) )
    {
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                    "Error: entry full path is expected to be set in STAGE_GET_FID stage" );
    }

    /* perform path2fid */
    rc = Lustre_GetFidFromPath( ATTR( &p_op->entry_attr, fullpath ), &tmp_id ); 

    if ( rc )
    {
        /* remove the operation from pipeline */
        rc = EntryProcessor_Acknowledge( p_op, 0, TRUE );
        if ( rc )
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage STAGE_GET_FID.",
                        rc );
    }
    else
    {
        rc = EntryProcessor_SetEntryId( p_op, &tmp_id );

        if ( rc )
        {
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d setting entry id", rc );
            /* remove entry from pipeline */
            EntryProcessor_Acknowledge( p_op, 0, TRUE );
            return rc;
        }

        /* go to GET_INFO_DB stage */
        rc = EntryProcessor_Acknowledge( p_op, STAGE_GET_INFO_DB, FALSE );
        if ( rc )
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage STAGE_GET_FID.",
                        rc );
    }
    return rc;

}

/**
 * apply unlink policy on entry and move entry to the appropriate stage.
 */
static int  EntryProc_apply_hsm_remove( struct entry_proc_op_t *p_op, lmgr_t * lmgr, int archived )
{
    int rc;

    if ( policies.unlink_policy.no_hsm_remove || !archived )
    {
        /* just remove the entry from database, if it exists */
        if (  p_op->db_exists )
        {
            p_op->db_op_type = OP_TYPE_REMOVE;
            rc = EntryProcessor_Acknowledge( p_op, STAGE_DB_APPLY, FALSE );
        }
        else
        {
            /* skip the record (but ack it) */
            rc = EntryProcessor_Acknowledge( p_op, STAGE_CHGLOG_CLR, FALSE );
        }
    }
    else /* hsm removal enabled and file is archived */
    {
        /* If the entry exists in DB, this moves it from main table
         * to a remove queue, else, just insert it to remove queue.
         */
        p_op->db_op_type = OP_TYPE_SOFT_REMOVE;
        rc = EntryProcessor_Acknowledge( p_op, STAGE_DB_APPLY, FALSE );
    }

    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d checking for HSM removal.", rc );
    return rc;
}


/** check if the entry exists in the database and what info is to be retrieved */
int EntryProc_get_info_db( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;
    int            next_stage = STAGE_GET_INFO_FS;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];
    char          *basename;
    struct changelog_rec * logrec = p_op->extra_info.log_record.p_log_rec;

    /* does the entry exists ? */
    p_op->db_exists = ListMgr_Exists( lmgr, &p_op->entry_id );

    if ( !p_op->db_exists )
    {
        if ( p_op->extra_info.is_changelog_record
             && ( logrec->cr_type == CL_UNLINK ) )
        {
            /* unlink on unknown entry.
             * If nlink>0, the entry must be inserted to the DB.
             * Else, a REMOVE request must be sent to HSM if the entry was archived.
             * If entry is not archived, it is just removed from DB.
             */
            DisplayLog( LVL_DEBUG, ENTRYPROC_TAG, "UNLINK record on unknown entry "DFID": last=%s, archived=%s",
                PFID(&p_op->entry_id), bool2str( logrec->cr_flags & CLF_UNLINK_LAST ),
                bool2str( logrec->cr_flags & CLF_UNLINK_HSM_EXISTS ) );

            if ( (logrec->cr_flags & CLF_UNLINK_LAST) == 0 )
            {
                /* new unknown entry */
                p_op->db_op_type = OP_TYPE_INSERT;
                p_op->extra_info_is_set = TRUE;
                /* entry exists but it was unknown in the DB:
                 * must retrieve all info about it.
                 */
                p_op->extra_info.getattr_needed = TRUE;
                p_op->extra_info.getpath_needed = TRUE;
                p_op->extra_info.getstatus_needed = TRUE;
                p_op->extra_info.getstripe_needed = TRUE;
                next_stage = STAGE_GET_INFO_FS;
                goto next_step;
            }
            /* last unlink */
            else
            {
                /* CLF_UNLINK_ARCHIVED indicates if entry must be cleaned from HSM */
                rc = EntryProc_apply_hsm_remove( p_op, lmgr, logrec->cr_flags & CLF_UNLINK_HSM_EXISTS );
                return rc;
            }
        }

        p_op->db_op_type = OP_TYPE_INSERT;

        if ( p_op->extra_info.is_changelog_record )
        {
            /* Entry from changelog */
            p_op->entry_attr_is_set = TRUE;
            ATTR_MASK_SET( &p_op->entry_attr, creation_time );
            /* convert to epoch time */
            ATTR( &p_op->entry_attr, creation_time ) =
                cltime2sec( logrec->cr_time );

            /* successful HSM action */
            if ( (logrec->cr_type == CL_HSM) &&
                 (hsm_get_cl_error( logrec->cr_flags ) == CLF_HSM_SUCCESS) )
            {
               /* XXX we consider dirty copies as correct archive operation */
               /* @TODO take CLF_HSM_DIRTY flag into account */
               if ( hsm_get_cl_event( logrec->cr_flags ) == HE_ARCHIVE )
               {
                  ATTR_MASK_SET( &p_op->entry_attr, last_archive );
                  ATTR( &p_op->entry_attr, last_archive ) = cltime2sec( logrec->cr_time );
               }
               else if ( hsm_get_cl_event(logrec->cr_flags ) == HE_RESTORE )
               {
                  ATTR_MASK_SET( &p_op->entry_attr, last_restore );
                  ATTR( &p_op->entry_attr, last_restore ) = cltime2sec( logrec->cr_time );
               }
            }

            /* entry exists but it was unknown in the DB:
             * must retrieve all info about it that are not provided
             * in the changelog.
             */
            p_op->extra_info_is_set = TRUE;
            p_op->extra_info.getattr_needed = TRUE;
            p_op->extra_info.getpath_needed = TRUE;
        }
        else
        {
            /* Entry from FS scan */
            if ( !p_op->entry_attr_is_set || !ATTR_MASK_TEST( &p_op->entry_attr, fullpath ) )
            {
                /* should have already been retrieved when scanning */
                DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                            "Error: entry from FS scan should have entry_attr and fullpath set" );
            }

            p_op->entry_attr_is_set = TRUE;
            ATTR_MASK_SET( &p_op->entry_attr, creation_time );
            ATTR( &p_op->entry_attr, creation_time ) = time( NULL );
        }

        /* in any case, its a new entry so we must retrieve its stripe info and HSM flags */
        p_op->extra_info_is_set = TRUE;
        p_op->extra_info.getstripe_needed = TRUE;
        p_op->extra_info.getstatus_needed = TRUE; /** @TODO: not necessary if this is a CREATE event */

        next_stage = STAGE_GET_INFO_FS;

    }
    else if ( p_op->extra_info.is_changelog_record
              && (logrec->cr_type == CL_UNLINK)
              && (logrec->cr_flags & CLF_UNLINK_LAST) )
    {
        /* last unlink on known entry. */
        DisplayLog( LVL_DEBUG, ENTRYPROC_TAG, "last UNLINK record on entry "DFID" (in DB): archived=%s",
            PFID(&p_op->entry_id), bool2str( logrec->cr_flags & CLF_UNLINK_HSM_EXISTS ) );

       /* CLF_UNLINK_ARCHIVED indicates if entry must be cleaned from HSM */
        rc = EntryProc_apply_hsm_remove( p_op, lmgr, logrec->cr_flags & CLF_UNLINK_HSM_EXISTS );
        return rc;
    }
    else
    {
        p_op->db_op_type = OP_TYPE_UPDATE;
        p_op->extra_info_is_set = TRUE;

        /* Does file has stripe info ? */
        if ( ListMgr_CheckStripe( lmgr, &p_op->entry_id ) != DB_SUCCESS )
            p_op->extra_info.getstripe_needed = TRUE;

        if ( p_op->extra_info.is_changelog_record )
        {
            /* if we are processing log records, we get :
             * - previous path and name (to check if it would have been renamed)
             * - last md update time (to check if its metedata must be updated)
             */
            ATTR_MASK_INIT( &p_op->entry_attr );
            ATTR_MASK_SET( &p_op->entry_attr, fullpath );
            ATTR_MASK_SET( &p_op->entry_attr, name );
            ATTR_MASK_SET( &p_op->entry_attr, md_update );
            /* @TODO retrieve path_update too */

            /* get info */
            rc = ListMgr_Get( lmgr, &p_op->entry_id, &p_op->entry_attr );
            if ( rc )
            {
                DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d retrieving entry from DB", rc );
                p_op->entry_attr_is_set = FALSE;
            }
            else
            {
                p_op->entry_attr_is_set = TRUE;

                /* check if attributes are missing or are out of date */

                if ( !ATTR_MASK_TEST( &p_op->entry_attr, md_update )  /* never updated */
                     || ( entry_proc_conf.md_update_period == 0 )       /* always update */
                     || ( time( NULL ) - ATTR( &p_op->entry_attr, md_update )
                           >= entry_proc_conf.md_update_period ) ) /* md out of date */
                {
                    p_op->extra_info.getstatus_needed = TRUE; /* @TODO not necesary => depends on the record type */
                    p_op->extra_info.getattr_needed = TRUE;
                    p_op->extra_info.getpath_needed = TRUE; /* @TODO periodic path_update, or depending on record type */
                }
                else            /* check attributes independently */
                {

                    /* status: not necessary */
                    if ( !ATTR_MASK_TEST( &p_op->entry_attr, status ) )
                        p_op->extra_info.getstatus_needed = TRUE;

                    /* try to guess if path/filename changed */
                    if ( !ATTR_MASK_TEST( &p_op->entry_attr, fullpath ) )
                    {
                        DisplayLog( LVL_DEBUG, ENTRYPROC_TAG, "Getpath needed because fullpath or name not set in DB" );
                        p_op->extra_info.getpath_needed = TRUE;
                    }
                    else if ( ATTR_MASK_TEST( &p_op->entry_attr, name )
                              &&  (logrec->cr_namelen > 0)
                              && strcmp( logrec->cr_name, ATTR( &p_op->entry_attr, name ) ) )
                    {
                        DisplayLog( LVL_DEBUG, ENTRYPROC_TAG, "Getpath needed because name changed (cr_namelen=%u) '%s'->'%s'",
                                (unsigned int)logrec->cr_namelen, ATTR( &p_op->entry_attr, name ),
                                logrec->cr_name );
                        /* name changed */
                        p_op->extra_info.getpath_needed = TRUE;
                    }
                    else if ( (logrec->cr_type == CL_RENAME) || (logrec->cr_type == CL_EXT) )
                    {
                        DisplayLog( LVL_DEBUG, ENTRYPROC_TAG, "Getpath needed because it's a rename operation" );
                        /* file was renamed */
                        p_op->extra_info.getpath_needed = TRUE;
                    }
                    else if ( ATTR_MASK_TEST( &p_op->entry_attr, fullpath )
                              && (logrec->cr_namelen > 0) )
                    {
                        /* check that fullpath basename matches name from log */
                        basename = strrchr( ATTR( &p_op->entry_attr, fullpath ), '/' );
                        if ( basename )
                        {
                            basename++;
                            if ( strcmp( basename, logrec->cr_name ) )
                            {
                                p_op->extra_info.getpath_needed = TRUE;

                                DisplayLog( LVL_DEBUG, ENTRYPROC_TAG,
                                      "Getpath needed because name from log record '%s' doesn't match fullpath '%s'",
                                      logrec->cr_name , basename );
                            }
                        }
                    }

                    if ( ( logrec->cr_type == CL_TIME )
                         || ( logrec->cr_type == CL_TRUNC )
                         || ( logrec->cr_type == CL_HSM ) )
                    {
                        /* always update attributes and status if file changed or was modified */
                        p_op->extra_info.getattr_needed = TRUE;
                        p_op->extra_info.getstatus_needed = TRUE; /* @TODO base on HSM event type */
                    }
                    else if ( logrec->cr_type == CL_SETATTR )
                    {
                        /* just get attributes if an attribute changed */
                        p_op->extra_info.getattr_needed = TRUE;
                    }

                }               /* end if attribute out of date */

            }                   /* end if lmgr_get succeeded */

            /* successful HSM action */
            if ( (logrec->cr_type == CL_HSM) &&
                 (hsm_get_cl_error( logrec->cr_flags ) == CLF_HSM_SUCCESS) )
            {
               /* XXX we consider dirty copies as correct archive operation */
               /* @TODO take CLF_HSM_DIRTY flag into account */
               if ( hsm_get_cl_event( logrec->cr_flags ) == HE_ARCHIVE )
               {
                  ATTR_MASK_SET( &p_op->entry_attr, last_archive );
                  ATTR( &p_op->entry_attr, last_archive ) = cltime2sec( logrec->cr_time );
               }
               else if ( hsm_get_cl_event(logrec->cr_flags ) == HE_RESTORE )
               {
                  ATTR_MASK_SET( &p_op->entry_attr, last_restore );
                  ATTR( &p_op->entry_attr, last_restore ) = cltime2sec( logrec->cr_time );
               }
            }

        }                       /* end if log record */
        else
        {
            /* entry from FS scan */

            /* no getattr or getpath needed, because they are provided by FS scan */
            p_op->extra_info.getattr_needed = FALSE;
            p_op->extra_info.getpath_needed = FALSE;

            /* FS scan is used for resynchronizing DB, so we always get status */
            p_op->extra_info.getstatus_needed = TRUE;

            /* Does file has stripe info ? */
            if ( ListMgr_CheckStripe( lmgr, &p_op->entry_id ) != DB_SUCCESS )
                p_op->extra_info.getstripe_needed = TRUE;
        }

        next_stage = STAGE_GET_INFO_FS;
    }

  next_step:

    /* acknowledge the entry */
    rc = EntryProcessor_Acknowledge( p_op, next_stage, FALSE );
    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage %s.", rc,
                    stage_info->stage_name );
    return rc;
}


int EntryProc_get_info_fs( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;

    if ( p_op->extra_info_is_set )
    {
        char           path[MAXPATHLEN];

        BuildFidPath( &p_op->entry_id, path );

#ifdef _DEBUG_ENTRYPROC
        printf( "Getattr=%d, Getpath=%d, GetStatus=%d, Getstripe=%d\n",
                p_op->extra_info.getattr_needed,
                p_op->extra_info.getpath_needed,
                p_op->extra_info.getstatus_needed, p_op->extra_info.getstripe_needed );
#endif

        if ( p_op->extra_info.getattr_needed )
        {
            struct stat    entry_md;

            rc = errno = 0;
#if defined( _MDS_STAT_SUPPORT )
           if ( global_config.direct_mds_stat )
               rc = lustre_mds_stat_by_fid( &p_op->entry_id, &entry_md );
           else
#endif
           if ( lstat( path, &entry_md ) != 0 )
              rc = errno;

            /* get entry attributes */
            if ( rc != 0 )
            {
                if ( (rc == ENOENT) ||  (rc == ESTALE)  )
                {
                    DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Entry %s does not exist anymore", path );
                    /* in this case, an UNLINK event will be raised, so we ignore current record */
                    rc = EntryProcessor_Acknowledge( p_op, STAGE_CHGLOG_CLR, FALSE );
                    return rc;
                }
                else
                    DisplayLog( LVL_DEBUG, ENTRYPROC_TAG, "lstat() failed on %s: %s", path,
                                strerror( rc ) );
                /* If lstat returns an error, drop the log record */
                goto skip_record;
            }

            /* only files are considered (however, ack the record)*/
            if ( !S_ISREG( entry_md.st_mode ) )
            {
                rc = EntryProcessor_Acknowledge( p_op, STAGE_CHGLOG_CLR, FALSE );
                return rc;
            }

            /* convert them to internal structure */
#if defined( _MDS_STAT_SUPPORT )
            PosixStat2EntryAttr( &entry_md, &p_op->entry_attr, !global_config.direct_mds_stat );
#else
            PosixStat2EntryAttr( &entry_md, &p_op->entry_attr, TRUE );
#endif
            p_op->entry_attr_is_set = TRUE;

            ATTR_MASK_SET( &p_op->entry_attr, md_update );
            ATTR( &p_op->entry_attr, md_update ) = time( NULL );

        } /* getattr needed */

        if ( p_op->extra_info.getpath_needed )
        {
            /* get entry path (only for log records) */
            if ( p_op->extra_info.is_changelog_record )
            {
                rc = Lustre_GetFullPath( &p_op->entry_id,
                                         ATTR( &p_op->entry_attr, fullpath ), 1024 );

                if ( ( abs( rc ) == ENOENT ) || ( abs( rc ) == ESTALE ) )
                {
                    DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Entry "DFID" does not exist anymore", PFID(&p_op->entry_id) );
                    /* in this case, an UNLINK event will be raised, so we ignore current record */
                    goto skip_record;
                }
                else if ( rc == 0 )
                {
                    ATTR_MASK_SET( &p_op->entry_attr, fullpath );
                    ATTR_MASK_SET( &p_op->entry_attr, md_update ); /* @TODO replace by specific update timestamp */
                    ATTR( &p_op->entry_attr, md_update ) = time( NULL );
                }
#if 0
                else if ( !EMPTY_STRING( p_op->extra_info.log_record.p_log_rec->cr_name ) )
                {
                    /* @TODO can be useful for future path management */
                }
#endif

            }
        } /* getpath needed */

        if ( p_op->extra_info.getstripe_needed )
        {
            /* get entry stripe */
            rc = File_GetStripeByPath( path,
                                       &ATTR( &p_op->entry_attr, stripe_info ),
                                       &ATTR( &p_op->entry_attr, stripe_items ) );

            if ( ( abs( rc ) == ENOENT ) || ( abs( rc ) == ESTALE ) )
            {
                DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Entry "DFID" does not exist anymore", PFID(&p_op->entry_id) );
                /* in this case, an UNLINK event will be raised, so we ignore current record */
                goto skip_record;
            }
            else if ( rc == 0 )
            {
                ATTR_MASK_SET( &p_op->entry_attr, stripe_info );
                ATTR_MASK_SET( &p_op->entry_attr, stripe_items );
                p_op->entry_attr_is_set = TRUE;
            }
        } /* get_stripe needed */

        if ( p_op->extra_info.getstatus_needed )
        {
            /* get entry status */
            rc = LustreHSM_GetStatus( path, &ATTR( &p_op->entry_attr, status ),
                                      &ATTR( &p_op->entry_attr, no_purge ),
                                      &ATTR( &p_op->entry_attr, no_archive ) );
            if ( ( abs( rc ) == ENOENT ) || ( abs( rc ) == ESTALE ) )
            {
                DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Entry "DFID" does not exist anymore", PFID(&p_op->entry_id) );
                /* in this case, an UNLINK event will be raised, so we ignore current record */
                goto skip_record;
            }
            else if ( rc == 0 )
            {
                ATTR_MASK_SET( &p_op->entry_attr, status );
                ATTR_MASK_SET( &p_op->entry_attr, no_purge );
                ATTR_MASK_SET( &p_op->entry_attr, no_archive );

                /* if the file is released, it goes outside PolicyEngine working set */
                if ( ATTR( &p_op->entry_attr, status ) == STATUS_RELEASED )
                {
                    DisplayLog(LVL_DEBUG, ENTRYPROC_TAG,
                        "Entry "DFID" has status 'RELEASED': removing it from PolicyEngine working set (if it was in DB)",
                        PFID( &p_op->entry_id ) );
                    if ( p_op->db_exists )
                    {
                        p_op->db_op_type = OP_TYPE_REMOVE;
                    }
                    else
                    {
                        rc = EntryProcessor_Acknowledge( p_op, STAGE_CHGLOG_CLR, FALSE );
                        return rc;
                    }
                }
            }

        } /* get_status needed */

    }

    /* set other info */

    if ( p_op->extra_info.is_changelog_record )
    {
        p_op->entry_attr_is_set = TRUE;
        ATTR_MASK_SET( &p_op->entry_attr, last_op_index );
        ATTR( &p_op->entry_attr, last_op_index ) = p_op->extra_info.log_record.p_log_rec->cr_index;
    }

    /* XXX for now, directly apply to the db (no reporting step) */

    rc = EntryProcessor_Acknowledge( p_op, STAGE_DB_APPLY, FALSE );
    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage.", rc );
    return rc;

skip_record:

    /* remove the operation from pipeline */
    rc = EntryProcessor_Acknowledge( p_op, 0, TRUE );
    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage.", rc );
    return rc;

}


int EntryProc_reporting( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    /* TODO to be implemented */
    return 0;
}

/**
 * Perform an operation on database. 
 */
int EntryProc_db_apply( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];

    /* insert to DB */
    switch ( p_op->db_op_type )
    {
    case OP_TYPE_INSERT:
        p_op->entry_attr.attr_mask &= ~readonly_attr_set;
        DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Insert("DFID")", PFID(&p_op->entry_id) );
        rc = ListMgr_Insert( lmgr, &p_op->entry_id, &p_op->entry_attr );
        break;
    case OP_TYPE_UPDATE:
        p_op->entry_attr.attr_mask &= ~readonly_attr_set;
        DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Update("DFID")", PFID(&p_op->entry_id) );
        rc = ListMgr_Update( lmgr, &p_op->entry_id, &p_op->entry_attr );
        break;
    case OP_TYPE_REMOVE:
        DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Remove("DFID")", PFID(&p_op->entry_id) );
        rc = ListMgr_Remove( lmgr, &p_op->entry_id );
        break;
    case OP_TYPE_SOFT_REMOVE:
        DisplayLog( LVL_FULL, ENTRYPROC_TAG, "SoftRemove("DFID")", PFID(&p_op->entry_id) );
        rc = ListMgr_SoftRemove( lmgr, &p_op->entry_id,
                ATTR_MASK_TEST( &p_op->entry_attr, fullpath )?ATTR(&p_op->entry_attr, fullpath ):NULL,
                time(NULL) + policies.unlink_policy.deferred_remove_delay ) ;
        break;
    default:
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Unhandled DB operation type: %d", p_op->db_op_type );
        rc = -1;
    }

    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d performing database operation.", rc );

    /* Acknoledge the operation if there is a callback */
    if ( p_op->callback_func )
        rc = EntryProcessor_Acknowledge( p_op, STAGE_CHGLOG_CLR, FALSE );
    else
        rc = EntryProcessor_Acknowledge( p_op, 0, TRUE );

    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknoledging stage %s.", rc,
                    stage_info->stage_name );

    return rc;
}

int            EntryProc_chglog_clr( struct entry_proc_op_t * p_op, lmgr_t * lmgr )
{
    int            rc;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];
    struct changelog_rec * logrec = p_op->extra_info.log_record.p_log_rec;

    DisplayLog( LVL_FULL, ENTRYPROC_TAG, "stage %s - record #%llu - id="DFID"\n", stage_info->stage_name,
            logrec->cr_index, PFID(&p_op->entry_id) );

    if ( p_op->callback_func )
    {
        /* if operation was commited, Perform callback to info collector */
        rc = p_op->callback_func( p_op, p_op->callback_param );

        if ( rc )
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d performing callback at stage %s.", rc,
                        stage_info->stage_name );
    }

    /* Acknoledge the operation and remove it from pipeline */
    rc = EntryProcessor_Acknowledge( p_op, 0, TRUE );
    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknoledging stage %s.", rc,
                    stage_info->stage_name );

    return rc;
}

int EntryProc_rm_old_entries( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];
    char           timestamp[128];
    lmgr_filter_t  filter;
    filter_value_t val;

    lmgr_simple_filter_init( &filter );

    val.val_uint = ATTR( &p_op->entry_attr, md_update );
    lmgr_simple_filter_add( &filter, ATTR_INDEX_md_update, LESSTHAN_STRICT, val, 0 );

    /* force commit after this operation */
    ListMgr_ForceCommitFlag( lmgr, TRUE );

    /* remove entries listed in previous scans */
    rc = ListMgr_MassRemove( lmgr, &filter );

    lmgr_simple_filter_free( &filter );

    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                    "Error: ListMgr MassRemove operation failed with code %d.", rc );
    else if ( p_op->callback_func )
    {
        /* Perform callback to info collector */
        p_op->callback_func( p_op, p_op->callback_param );
    }

    /* also update the LastScan variable */
    sprintf( timestamp, "%lu", ( unsigned long ) time( NULL ) );
    ListMgr_SetVar( lmgr, LAST_SCAN_VAR, timestamp );

    /* unset force commit flag */
    ListMgr_ForceCommitFlag( lmgr, FALSE );

    rc = EntryProcessor_Acknowledge( p_op, 0, TRUE );

    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknoledging stage %s.", rc,
                    stage_info->stage_name );

    return rc;

}
