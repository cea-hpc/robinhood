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
/**
 * Pipeline functions for Tmp Filesystem Manager 
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

#define ERR_MISSING(_err) (((_err)==ENOENT)||((_err)==ESTALE))

/* forward declaration of EntryProc functions of pipeline */
int            EntryProc_get_fid( struct entry_proc_op_t *, lmgr_t * );
int            EntryProc_get_info_db( struct entry_proc_op_t *, lmgr_t * );
int            EntryProc_get_info_fs( struct entry_proc_op_t *, lmgr_t * );
int            EntryProc_infer_attrs( struct entry_proc_op_t *, lmgr_t * );
int            EntryProc_reporting( struct entry_proc_op_t *, lmgr_t * );
int            EntryProc_db_apply( struct entry_proc_op_t *, lmgr_t * );
#ifdef HAVE_CHANGELOGS
int            EntryProc_chglog_clr( struct entry_proc_op_t *, lmgr_t * );
#endif
int            EntryProc_rm_old_entries( struct entry_proc_op_t *, lmgr_t * );

/** pipeline stages definition */
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
#ifndef _SQLITE
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC, 0},
#else
    /* SQLite locks the whole file for modifications...
     * So, 1 single thread is enough.
     */
     STAGE_FLAG_MAX_THREADS | STAGE_FLAG_SYNC, 1},
#endif

#ifdef HAVE_CHANGELOGS
    /* only 1 thread here because commiting records must be sequential
     * (in the same order as changelog) */
    {STAGE_CHGLOG_CLR, "STAGE_CHGLOG_CLR", EntryProc_chglog_clr,
     STAGE_FLAG_SEQUENTIAL | STAGE_FLAG_SYNC, 1},

     /* acknowledging records must be sequential,
      * in the order of record ids
      * @TODO change this depending on the mode the program is started.
      */
#endif

    /* this step is for mass update / mass remove operations when starting/ending a FS scan. */
    {STAGE_RM_OLD_ENTRIES, "STAGE_RM_OLD_ENTRIES", EntryProc_rm_old_entries,
     STAGE_FLAG_SEQUENTIAL | STAGE_FLAG_SYNC, 0}
};

/* ignore tier transfer file */
static int entry_prefilter( struct entry_proc_op_t * p_op )
{
    if ( p_op->entry_attr_is_set || ATTR_MASK_TEST( &p_op->entry_attr, name ) )
    {
        if ( !strncmp( ATTR(&p_op->entry_attr, name ), SHERPA_PREFIXE_TRANSFERT,
                       strlen(SHERPA_PREFIXE_TRANSFERT)) )
        {
            /* skip the entry */
            return FALSE;
        }
    }
    return TRUE;
}

/* for entries from FS scan, we must get the entry ID associated to path */
int EntryProc_get_fid( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
#ifdef _HAVE_FID
    int            rc;
    entry_id_t     tmp_id;

    if ( !entry_prefilter( p_op ) )
    {
        DisplayLog( LVL_FULL, ENTRYPROC_TAG,
                    "Tier transfer file %s, skipped",
                    (ATTR_MASK_TEST( &p_op->entry_attr, fullpath )?
                     ATTR(&p_op->entry_attr, fullpath):
                     ATTR(&p_op->entry_attr, name)) );
        EntryProcessor_Acknowledge( p_op, 0, TRUE );
        return 0;
    }

    if ( !p_op->entry_attr_is_set || !ATTR_MASK_TEST( &p_op->entry_attr, fullpath ) )
    {
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                    "Error: entry full path is expected to be set in STAGE_GET_FID stage" );
        EntryProcessor_Acknowledge( p_op, 0, TRUE );
        return EINVAL;
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
#else
    DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error: unexpected stage in a filesystem with no fid: STAGE_GET_FID.");
    EntryProcessor_Acknowledge( p_op, 0, TRUE );
    return EINVAL;
#endif
}

#ifdef HAVE_CHANGELOGS
/**
 *  Infer information and determine needed information from a changelog record.
 *  \return the next pipeline step to be performed,
 *          -1 if entry must be dropped.
 */
static int EntryProc_ProcessLogRec( struct entry_proc_op_t *p_op )
{
    /* short alias */
    struct changelog_rec * logrec = p_op->extra_info.log_record.p_log_rec;

    /* allow event-driven update */
    int allow_md_updt = TRUE;
    int allow_path_updt = TRUE;

    if ( logrec->cr_type == CL_UNLINK )
    {
        DisplayLog( LVL_DEBUG, ENTRYPROC_TAG,
                    "UNLINK on %s entry "DFID": last=%s",
                    p_op->db_exists?"known":"unknown", PFID(&p_op->entry_id),
                    bool2str( logrec->cr_flags & CLF_UNLINK_LAST ) );

        /* it it the last reference to this file? */
        if ( logrec->cr_flags & CLF_UNLINK_LAST )
        {
            /* If the file was in DB: remove it, else skip the record. */
            if ( p_op->db_exists )
            {
                p_op->db_op_type = OP_TYPE_REMOVE;
                return STAGE_DB_APPLY;
            }
            else
                return STAGE_CHGLOG_CLR;
        }
        else if ( p_op->db_exists ) /* entry still exists and is known in DB */
        {
            /* Force updating the path, because the path we have may be
             * the removed one. */
            p_op->extra_info_is_set = TRUE;
            p_op->extra_info.getpath_needed = TRUE;
            /* then, check needed updates in the next part of the function */
        }
        /* else: is handled in the next part of the function */
    } /* end if UNLINK */
    else if ( logrec->cr_type == CL_RMDIR )
    {
        DisplayLog( LVL_DEBUG, ENTRYPROC_TAG,
                    "RMDIR on %s directory "DFID,
                    p_op->db_exists?"known":"unknown", PFID(&p_op->entry_id));

            /* If the directory was in DB: remove it, else skip the record. */
            if ( p_op->db_exists )
            {
                p_op->db_op_type = OP_TYPE_REMOVE;
                return STAGE_DB_APPLY;
            }
            else
                return STAGE_CHGLOG_CLR;
    }

    if ( !p_op->db_exists )
    {
        /* non-unlink (or non-destructive unlink) record on unknown entry:
         * insert entry to the DB */
        p_op->db_op_type = OP_TYPE_INSERT;

        /* we must get info that is not provided by the chglog */
        p_op->extra_info_is_set = TRUE;
        p_op->extra_info.getattr_needed = TRUE;
        p_op->extra_info.getpath_needed = TRUE;
        p_op->extra_info.getstripe_needed = TRUE;
    }
    else /* non-unlink record on known entry */
    {
        p_op->db_op_type = OP_TYPE_UPDATE;

        /* check what information must be updated */

        if ( !p_op->entry_attr_is_set )
        {
            /* we know nothing about it... */
            p_op->extra_info_is_set = TRUE;
            p_op->extra_info.getattr_needed = TRUE;
            p_op->extra_info.getpath_needed = TRUE;
            p_op->extra_info.getstripe_needed = TRUE;
        }
        else
        {
            p_op->extra_info_is_set = TRUE;

            /* get stripe info only if we don't already know it. */
            p_op->extra_info.getstripe_needed =
                ATTR_MASK_TEST( &p_op->entry_attr, stripe_info )? FALSE : TRUE;

            /* Check md_update policy */
            p_op->extra_info.getattr_needed
                = need_md_update( &p_op->entry_attr, &allow_md_updt );

            /* check if path update is needed */
            p_op->extra_info.getpath_needed
                = need_path_update( &p_op->entry_attr, &allow_path_updt );
        }
    }

    /* if the entry is already in DB, try to determine if something changed */
    if ( p_op->db_exists )
    {
        /* Sanity check: if the entry already exists in DB,
         * it could come from a previous filesystem that has been formatted.
         * In this case, force a full update of the entry.
         */
        if ( logrec->cr_type == CL_CREATE )
        {
            DisplayLog( LVL_MAJOR, ENTRYPROC_TAG,
                        "WARNING: CREATE record on already existing entry "
                        DFID" ?!", PFID(&p_op->entry_id) );

            p_op->extra_info_is_set = TRUE;
            p_op->extra_info.getattr_needed = TRUE;
            p_op->extra_info.getpath_needed = TRUE;
            p_op->extra_info.getstripe_needed = TRUE;
            return STAGE_GET_INFO_FS;
        }

        /* compare to the name in changelog */
        if ( allow_path_updt && (logrec->cr_namelen > 0) )
        {
            /* if name is set, compare to name */
            if ( ATTR_MASK_TEST( &p_op->entry_attr, name ) )
            {
                if ( strcmp(logrec->cr_name, ATTR( &p_op->entry_attr, name )) )
                {
                    p_op->extra_info.getpath_needed = TRUE;
                    DisplayLog( LVL_DEBUG, ENTRYPROC_TAG,
                             "Getpath needed because name changed: '%s'->'%s'",
                             ATTR( &p_op->entry_attr, name ), logrec->cr_name );
                }
            }
            else if ( ATTR_MASK_TEST( &p_op->entry_attr, fullpath ) )
            {
                /* else, compare using full path */
                char * basename;
                basename = strrchr( ATTR( &p_op->entry_attr, fullpath ), '/' );
                 if ( basename )
                 {
                    basename++;
                    if ( strcmp( basename, logrec->cr_name ) )
                    {
                        p_op->extra_info.getpath_needed = TRUE;
                        DisplayLog( LVL_DEBUG, ENTRYPROC_TAG,
                                    "Getpath needed because name from log record "
                                    "(%s) doesn't match fullpath '%s'",
                                    logrec->cr_name ,
                                    ATTR( &p_op->entry_attr, fullpath ) );
                    }
                 }
            }
        }

        /* in case of a rename, refresh the full path (after rename => rnm to only CL_EXT)*/
        if ( allow_path_updt && (logrec->cr_type == CL_EXT) )
        {
            /* file was renamed */
            DisplayLog( LVL_DEBUG, ENTRYPROC_TAG, "Getpath needed because it's "
                        "a rename operation" );
            p_op->extra_info.getpath_needed = TRUE;
        }

        /* get the new attributes, in case of a SATTR, TIME, HSM... */
        if ( allow_md_updt && (CL_TIME_NOACCESS(logrec->cr_type)
                               || ( logrec->cr_type == CL_TRUNC )
                               || ( logrec->cr_type == CL_HSM )
                               || ( logrec->cr_type == CL_SETATTR )) )
        {
            p_op->extra_info.getattr_needed = TRUE;
        }
    }

    return STAGE_GET_INFO_FS;
} /* EntryProc_ProcessLogRec */
#endif


/**
 * Check if entry already exist in database.
 * In the case of lustre, also check its stripe.
 */
int EntryProc_get_info_db( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;
    int            next_stage = -1; /* skip */
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];

    /* 1) get the name from changelog to pre-filter entry */
#ifdef HAVE_CHANGELOGS
    struct changelog_rec * logrec = p_op->extra_info.log_record.p_log_rec;

    /** if this is a changelog record, copy its name into the attr structure */
    if ( p_op->extra_info.is_changelog_record
         && (logrec->cr_namelen > 0)
         && !EMPTY_STRING(logrec->cr_name) )
    {
        strcpy( ATTR(&p_op->entry_attr, name), logrec->cr_name );
        ATTR_MASK_SET(&p_op->entry_attr, name);
        p_op->entry_attr_is_set = TRUE;
    }
#endif

    if ( !entry_prefilter( p_op ) )
    {
        DisplayLog( LVL_FULL, ENTRYPROC_TAG,
                    "Tier transfer file '%s', skipped",
                    (ATTR_MASK_TEST( &p_op->entry_attr, fullpath )?
                     ATTR(&p_op->entry_attr, fullpath):
                     ATTR(&p_op->entry_attr, name)) );
        EntryProcessor_Acknowledge( p_op, 0, TRUE );
        return 0;
    }

#ifdef HAVE_CHANGELOGS
    if ( p_op->extra_info.is_changelog_record )
    {
        struct changelog_rec * logrec = p_op->extra_info.log_record.p_log_rec;

        /* what do we know about this entry? */
        ATTR_MASK_INIT( &p_op->entry_attr );
        ATTR_MASK_SET( &p_op->entry_attr, fullpath );
        ATTR_MASK_SET( &p_op->entry_attr, name );
        ATTR_MASK_SET( &p_op->entry_attr, stripe_info );
        ATTR_MASK_SET( &p_op->entry_attr, md_update );
        ATTR_MASK_SET( &p_op->entry_attr, path_update );
        ATTR_MASK_SET( &p_op->entry_attr, status );

        if ( entry_proc_conf.match_classes )
        {
            /* get fileclasses update info to know if we must check it */
            ATTR_MASK_SET( &p_op->entry_attr, rel_cl_update );
            ATTR_MASK_SET( &p_op->entry_attr, release_class );
            ATTR_MASK_SET( &p_op->entry_attr, arch_cl_update );
            ATTR_MASK_SET( &p_op->entry_attr, archive_class );
            p_op->entry_attr.attr_mask |= policies.purge_policies.global_attr_mask;
            p_op->entry_attr.attr_mask |= policies.migr_policies.global_attr_mask;
        }

        rc = ListMgr_Get( lmgr, &p_op->entry_id, &p_op->entry_attr );

        if (rc == DB_SUCCESS )
        {
            p_op->db_exists = TRUE;
            p_op->entry_attr_is_set = TRUE;
        }
        else if (rc == DB_NOT_EXISTS )
        {
            p_op->db_exists = FALSE;
            ATTR_MASK_INIT( &p_op->entry_attr );
        }
        else
        {
            /* ERROR */
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                        "Error %d retrieving entry "DFID" from DB", rc,
                        PFID(&p_op->entry_id) );
            p_op->db_exists = FALSE;
            p_op->entry_attr_is_set = FALSE;
            ATTR_MASK_INIT( &p_op->entry_attr );
        }

        /* Retrieve info from the log record, and decide what info must be
         * retrieved from filesystem. */
        next_stage = EntryProc_ProcessLogRec( p_op );

        DisplayLog( LVL_DEBUG, ENTRYPROC_TAG, "RECORD: %s "DFID" %#x %s => "
                    "getstripe=%u, getattr=%u, getpath=%u",
                    changelog_type2str(logrec->cr_type), PFID(&p_op->entry_id),
                    logrec->cr_flags & CLF_FLAGMASK, logrec->cr_namelen>0?
                    logrec->cr_name:"<null>",
                    p_op->extra_info.getstripe_needed?1:0,
                    p_op->extra_info.getattr_needed?1:0,
                    p_op->extra_info.getpath_needed?1:0 );
    }
    else /* Entry from FS scan */
    {
#endif
        /* scan is expected to provide full path and attributes. */
        if ( !p_op->entry_attr_is_set
             || !ATTR_MASK_TEST( &p_op->entry_attr, fullpath ) )
        {
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                        "Error: missing info from FS scan" );
            /* skip the entry */
            next_stage = -1;
            goto next_step;
        }

        /* do the entry exists in DB? */
        p_op->db_exists = ListMgr_Exists( lmgr, &p_op->entry_id );

        if ( !p_op->db_exists )
        {
            /* new entry */
            p_op->db_op_type = OP_TYPE_INSERT;

            p_op->extra_info_is_set = TRUE;
#ifdef HAVE_CHANGELOGS
            p_op->extra_info.getattr_needed = FALSE;
            p_op->extra_info.getpath_needed = FALSE;
#endif

#ifdef _LUSTRE
            /* get entry stripe if this is a Lustre filesystem
             * and if the entry is a file.
             */
            if ( p_op->entry_attr_is_set
                 && ATTR_MASK_TEST( &p_op->entry_attr, type )
                 && !strcmp( ATTR( &p_op->entry_attr, type ), STR_TYPE_FILE ) 
                 && !strcmp( global_config.fs_type, "lustre" ) )
            {
                p_op->extra_info.getstripe_needed = TRUE;
            }
#endif

            next_stage = STAGE_GET_INFO_FS;
        }
        else
        {
            p_op->db_op_type = OP_TYPE_UPDATE;

            /* FS scan provides both path and attrs */
            p_op->extra_info_is_set = TRUE;
#ifdef HAVE_CHANGELOGS
            p_op->extra_info.getattr_needed = FALSE;
            p_op->extra_info.getpath_needed = FALSE;
#endif

#ifdef _LUSTRE
            /* validate entry stripe if this is a Lustre filesystem
             * and only if the entry is a file.
             */
            if ( p_op->entry_attr_is_set
                 && ATTR_MASK_TEST( &p_op->entry_attr, type )
                 && !strcmp( ATTR( &p_op->entry_attr, type ), STR_TYPE_FILE ) 
                 && !strcmp( global_config.fs_type, "lustre" ) )
            {
                /* Does file has stripe info ? */
                if ( ListMgr_CheckStripe( lmgr, &p_op->entry_id ) != DB_SUCCESS )
                    p_op->extra_info.getstripe_needed = TRUE;
            }
#endif
            next_stage = STAGE_GET_INFO_FS;
        }
#ifdef HAVE_CHANGELOGS
    }
#endif

next_step:
    if ( next_stage == -1 )
        /* drop the entry */
        rc = EntryProcessor_Acknowledge( p_op, 0, TRUE );
    else
        /* go to next pipeline step */
        rc = EntryProcessor_Acknowledge( p_op, next_stage, FALSE );

    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                    "Error %d acknowledging stage %s.", rc,
                    stage_info->stage_name );
    return rc;
}




/**
 * Get extra info and perform actions on entry.
 */
int EntryProc_get_info_fs( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];

    if ( p_op->extra_info_is_set )
    {
#ifdef _HAVE_FID
        char path[MAXPATHLEN];
        BuildFidPath( &p_op->entry_id, path );
#else
        char * path;
        if ( p_op->entry_attr_is_set && ATTR_MASK_TEST( &p_op->entry_attr, fullpath ) )
        {
            path = ATTR( &p_op->entry_attr, fullpath );
        }
        else
        {
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                        "Entry path is needed to get entry info" );
            goto skip;
        }
#endif


/* path only needed when the entry doesn't come from a fs scan */
#ifdef HAVE_CHANGELOGS
        if ( p_op->extra_info.getpath_needed )
        {
            /* get entry path (only for log records) */
            if ( p_op->extra_info.is_changelog_record )
            {
                rc = Lustre_GetFullPath( &p_op->entry_id,
                                         ATTR( &p_op->entry_attr, fullpath ),
                                         1024 );

                if ( ERR_MISSING(rc) )
                {
                    DisplayLog( LVL_FULL, ENTRYPROC_TAG,
                                "Entry "DFID" does not exist anymore",
                                PFID(&p_op->entry_id) );
                    /* if the entry was not is db, skip the record */
                    if ( !p_op->db_exists )
                        goto skip;
                    else /* else, remove it from db */
                        goto rm_record;
                }
                else if ( rc == 0 )
                {
                    ATTR_MASK_SET( &p_op->entry_attr, fullpath );
                    ATTR_MASK_SET( &p_op->entry_attr, path_update );
                    ATTR( &p_op->entry_attr, path_update ) = time( NULL );
                }
            }
        } /* getpath needed */
#endif

        if ( p_op->extra_info.getstripe_needed )
        {
            /* Get stripe info for this file */
    #ifdef _LUSTRE
            if ( File_GetStripeByPath( path,
                                       &ATTR( &p_op->entry_attr, stripe_info ),
                                       &ATTR( &p_op->entry_attr, stripe_items ) ) == 0 )
            {
                p_op->entry_attr_is_set = TRUE;
                ATTR_MASK_SET( &p_op->entry_attr, stripe_info );
                ATTR_MASK_SET( &p_op->entry_attr, stripe_items );
            }
    #endif
        }

#ifdef HAVE_CHANGELOGS /* for scan, always get it */
        if ( p_op->extra_info.getattr_needed )
        {
#endif
            /* get info about this entry and check policies about the entry. */
            switch( SherpaManageEntry( &p_op->entry_id, &p_op->entry_attr,
                                       entry_proc_conf.match_classes ) )
            {
                case do_skip:
                    goto skip;
                case do_rm:
                    /* remove entry if it was in db */
                    if ( p_op->db_exists )
                        goto rm_record;
                    else
                        goto skip;
                    break;
                case do_update:
                    p_op->entry_attr_is_set = TRUE;
                    /* OK, continue */
                    break; 
            }
#ifdef HAVE_CHANGELOGS
        }
#endif

    }


    /* acknowledge the stage and go to the next */
    rc = EntryProcessor_Acknowledge( p_op, STAGE_REPORTING, FALSE );
    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error acknowledging stage %s",
                    stage_info->stage_name );

    return rc;

skip:
    if ( EntryProcessor_Acknowledge( p_op, 0, TRUE ) != 0 )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error acknowledging stage %s",
                    stage_info->stage_name );
    return rc; 

rm_record:
    p_op->db_op_type = OP_TYPE_REMOVE;
    rc = EntryProcessor_Acknowledge( p_op, STAGE_DB_APPLY, FALSE );
    if ( rc )
       DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage.", rc );
    return rc;
}



/**
 *  Raise alert if the entry exceeds an admministrator defined limit.
 *  This operation can be made asynchronously.
 */
int EntryProc_reporting( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc, i;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];
    int            is_alert = FALSE;
    char           stralert[2048];
    char           strvalues[2048];
    char           strid[1024];
    char         * title = NULL;

    /* generate missing fields */
    ListMgr_GenerateFields( &p_op->entry_attr,
                            entry_proc_conf.alert_attr_mask );

    /* check alert criterias (synchronously) */
    for ( i = 0; i < entry_proc_conf.alert_count; i++ )
    {
        /* check entry attr mask (else, skip it) */
        if ( !p_op->entry_attr_is_set || !p_op->entry_id_is_set
             || ( ( p_op->entry_attr.attr_mask & entry_proc_conf.alert_list[i].attr_mask ) !=
                  entry_proc_conf.alert_list[i].attr_mask ) )
            continue;

        if ( EntryMatches
             ( &p_op->entry_id, &p_op->entry_attr,
               &entry_proc_conf.alert_list[i].boolexpr ) == POLICY_MATCH )
        {
            /* build alert string and break */
            if ( ATTR_MASK_TEST( &p_op->entry_attr, fullpath ) )
                snprintf( strid, 1024, "%s", ATTR( &p_op->entry_attr, fullpath ) );
            else
#ifdef _HAVE_FID
                snprintf( strid, 1024, "fid[seq, oid]=[%llu, %u]", p_op->entry_id.f_seq,
                          p_op->entry_id.f_oid );
#else
                snprintf( strid, 1024, "[inode, device]=[%llu, %llu]",
                          ( unsigned long long ) p_op->entry_id.inode,
                          ( unsigned long long ) p_op->entry_id.device );
#endif

            rc = BoolExpr2str( &entry_proc_conf.alert_list[i].boolexpr, stralert, 2048 );
            if ( rc < 0 )
                strcpy( stralert, "Error building alert string" );

            PrintAttrs( strvalues, 2048, &p_op->entry_attr,
                        entry_proc_conf.alert_list[i].attr_mask );

            if ( EMPTY_STRING(entry_proc_conf.alert_list[i].title) )
                title = NULL;
            else
                title = entry_proc_conf.alert_list[i].title;

            is_alert = TRUE;
            break;
        }
    }

    /* acknoledge now if the stage is asynchronous */
    if ( stage_info->stage_flags & STAGE_FLAG_ASYNC )
    {
        rc = EntryProcessor_Acknowledge( p_op, STAGE_DB_APPLY, FALSE );
        if ( rc )
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error acknowledging stage %s",
                        stage_info->stage_name );
    }

    if ( is_alert )
        RaiseEntryAlert(title, stralert, strid, strvalues );

    /* acknoledge now if the stage was synchronous */
    if ( !( stage_info->stage_flags & STAGE_FLAG_ASYNC ) )
    {
        rc = EntryProcessor_Acknowledge( p_op, STAGE_DB_APPLY, FALSE );
        if ( rc )
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error acknowledging stage %s",
                        stage_info->stage_name );
    }

    return 0;
}


/**
 * Perform an operation on database. 
 */
int EntryProc_db_apply( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];

#ifdef _DEBUG_ENTRYPROC
#ifdef _HAVE_FID
    printf( "stage %s - record #%u - id=[%llu,%u]\n", stage_info->stage_name,
            ( unsigned int ) p_op->entry_id.f_ver, p_op->entry_id.f_seq, p_op->entry_id.f_oid );
#else
    printf( "stage %s - entry %s - id=[%llu,%llu,%u]\n", stage_info->stage_name,
            ATTR( &p_op->entry_attr, fullpath ), ( unsigned long long ) p_op->entry_id.inode,
            ( unsigned long long ) p_op->entry_id.device, p_op->entry_id.validator );
#endif
#endif
    /* if stripe has not been retrieved, don't update it in the database */
    if ( !p_op->extra_info.getstripe_needed )
        ATTR_MASK_UNSET( &p_op->entry_attr, stripe_info );

    /* insert to DB */
    switch ( p_op->db_op_type )
    {
    case OP_TYPE_INSERT:
#ifdef _HAVE_FID
        DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Insert("DFID")", PFID(&p_op->entry_id) );
#endif
        p_op->entry_attr.attr_mask &= ~readonly_attr_set;
        rc = ListMgr_Insert( lmgr, &p_op->entry_id, &p_op->entry_attr );
        break;
    case OP_TYPE_UPDATE:
#ifdef _HAVE_FID
        DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Update("DFID")", PFID(&p_op->entry_id) );
#endif
        p_op->entry_attr.attr_mask &= ~readonly_attr_set;
        rc = ListMgr_Update( lmgr, &p_op->entry_id, &p_op->entry_attr );
        break;
    case OP_TYPE_REMOVE:
#ifdef _HAVE_FID
        DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Remove("DFID")", PFID(&p_op->entry_id) );
#endif
        rc = ListMgr_Remove( lmgr, &p_op->entry_id );
        break;
    default:
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Unhandled DB operation type: %d", p_op->db_op_type );
        rc = -1;
    }

    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d performing database operation.", rc );

#ifdef HAVE_CHANGELOGS
    if ( (rc == 0) && p_op->callback_func )
        rc = EntryProcessor_Acknowledge( p_op, STAGE_CHGLOG_CLR, FALSE );
    else /* if rc != 0, we must remove it from pipeline */
#endif
        rc = EntryProcessor_Acknowledge( p_op, 0, TRUE );

    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknoledging stage %s.", rc,
                    stage_info->stage_name );

    return rc;
}

#ifdef HAVE_CHANGELOGS
int            EntryProc_chglog_clr( struct entry_proc_op_t * p_op, lmgr_t * lmgr )
{
    int            rc;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];
    struct changelog_rec * logrec = p_op->extra_info.log_record.p_log_rec;

    DisplayLog( LVL_FULL, ENTRYPROC_TAG, "stage %s - record #%llu - id="DFID"\n",
                stage_info->stage_name, logrec->cr_index, PFID(&p_op->entry_id) );

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
#endif


/**
 * Special operation performed at the end of FS Scans
 */
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
        /* if operation was commited, Perform callback to info collector */
        if ( ListMgr_GetCommitStatus( lmgr ) )
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
