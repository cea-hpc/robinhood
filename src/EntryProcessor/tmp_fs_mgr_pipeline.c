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
    {STAGE_INFER_ATTRS, "STAGE_INFER_ATTRS", EntryProc_infer_attrs,
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC, 0},
    {STAGE_REPORTING, "STAGE_REPORTING", EntryProc_reporting,
     STAGE_FLAG_PARALLEL | STAGE_FLAG_ASYNC, 0},
    {STAGE_DB_APPLY, "STAGE_DB_APPLY", EntryProc_db_apply,
#if defined( _SQLITE )
     /* SQLite locks the whole file for modifications...
     * So, 2 threads at max is enough at this step.
     */
     STAGE_FLAG_MAX_THREADS | STAGE_FLAG_SYNC, 2},
#else
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC, 0},
#endif

#ifdef HAVE_CHANGELOGS
    /* only 1 thread here because commiting records must be sequential (in the same order as changelog) */
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

/* for entries from FS scan, we must get the entry ID associated to path */
int EntryProc_get_fid( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
#ifdef _HAVE_FID
    int            rc;
    entry_id_t     tmp_id;

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

/**
 * Check if entry already exist in database.
 * In the case of lustre, also check its stripe.
 */
int EntryProc_get_info_db( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];

    /* next stage by default */
    int            next_stage = STAGE_INFER_ATTRS;

    p_op->db_exists = ListMgr_Exists( lmgr, &p_op->entry_id );

    if ( !p_op->db_exists )
    {
        p_op->db_op_type = OP_TYPE_INSERT;

#ifdef HAVE_CHANGELOGS
        if ( p_op->extra_info.is_changelog_record )
        {
            /* Entry from changelog */
            next_stage = STAGE_GET_INFO_FS;
            p_op->extra_info_is_set = TRUE;
            p_op->extra_info.getattr_needed = TRUE;
            p_op->extra_info.getpath_needed = TRUE;
            p_op->extra_info.getstripe_needed = TRUE;
            goto next_step;
        }
#endif
    }
    else
    {
        p_op->db_op_type = OP_TYPE_UPDATE;

#ifdef HAVE_CHANGELOGS
        if ( p_op->extra_info_is_set && p_op->extra_info.is_changelog_record )
        {
            struct changelog_rec * logrec = p_op->extra_info.log_record.p_log_rec;

            /* entry from changelog: retrieve previous attrs */
            ATTR_MASK_INIT( &p_op->entry_attr );
            ATTR_MASK_SET( &p_op->entry_attr, fullpath );
            ATTR_MASK_SET( &p_op->entry_attr, name );
            ATTR_MASK_SET( &p_op->entry_attr, type ); /* to know if getstripe is needed */
            ATTR_MASK_SET( &p_op->entry_attr, md_update );

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
                    next_stage = STAGE_GET_INFO_FS;
                    p_op->extra_info.getattr_needed = TRUE;
                    p_op->extra_info.getpath_needed = TRUE;
                }
                else            /* check attributes independently */
                {
                    /* path/filename */
                    if ( !ATTR_MASK_TEST( &p_op->entry_attr, fullpath )
                         || !ATTR_MASK_TEST( &p_op->entry_attr, name ) )
                    {
                        printf( "Getpath needed because fullpath or name not set in DB\n" );
                        p_op->extra_info.getpath_needed = TRUE;
                        next_stage = STAGE_GET_INFO_FS;
                    }
                    else if ( ATTR_MASK_TEST( &p_op->entry_attr, name )
                              &&  (logrec->cr_namelen > 0)
                              && strcmp( logrec->cr_name, ATTR( &p_op->entry_attr, name ) ) )
                    {
                        printf( "Getpath needed because name changed '%s'->'%s'\n",
                                ATTR( &p_op->entry_attr, name ),
                                logrec->cr_name );
                        /* name changed */
                        p_op->extra_info.getpath_needed = TRUE;
                        next_stage = STAGE_GET_INFO_FS;
                    }
                    else if ( (logrec->cr_type == CL_RENAME) || (logrec->cr_type == CL_EXT) )
                    {
                        printf( "Getpath needed because it's a rename operation\n" );
                        /* file was renamed */
                        p_op->extra_info.getpath_needed = TRUE;
                        next_stage = STAGE_GET_INFO_FS;
                    }
                    else if ( ATTR_MASK_TEST( &p_op->entry_attr, fullpath )
                              && (logrec->cr_namelen > 0) )
                    {
                        char * basename;

                        /* check that fullpath basename matches name from log */
                        basename = strrchr( ATTR( &p_op->entry_attr, fullpath ), '/' );
                        if ( basename )
                        {
                            basename++;
                            if ( strcmp( basename, logrec->cr_name ) )
                            {
                                p_op->extra_info.getpath_needed = TRUE;
                                next_stage = STAGE_GET_INFO_FS;

                                printf
                                    ( "Getpath needed because name from log record '%s' doesn't match fullpath '%s'\n",
                                      logrec->cr_name , basename );
                            }
                        }
                    }

                    if ( ( logrec->cr_type == CL_TIME )
                         || ( logrec->cr_type == CL_TRUNC ) )
                    {
                        /* always update attributes and status if file changed or was modified */
                        p_op->extra_info.getattr_needed = TRUE;
                        next_stage = STAGE_GET_INFO_FS;
                    }
                    else if ( logrec->cr_type == CL_SETATTR )
                    {
                        /* just get attributes if an attribute changed */
                        p_op->extra_info.getattr_needed = TRUE;
                    }

                } /* end if attribute out of date */

            } /* end if lmgr_get succeeded */
        }
#endif
    }

    /* Only pass here if the entry is from FS scan,
     * or has previous metadata from DB.
     */
#ifdef _LUSTRE
    /* validate entry stripe in case of Lustre 
     * and only if the entry is a file.
     */
    if ( p_op->entry_attr_is_set && ATTR_MASK_TEST( &p_op->entry_attr, type )
         && !strcmp( ATTR( &p_op->entry_attr, type ), STR_TYPE_FILE ) )
    {
        if ( !strcmp( global_config.fs_type, "lustre" ) )
        {
            /* only get stripe for regular files */

            if ( p_op->db_op_type == OP_TYPE_INSERT )
            {
                /* always get stripe the first time it is inserted */
                next_stage = STAGE_GET_INFO_FS;
                p_op->extra_info_is_set = TRUE;
                p_op->extra_info.getstripe_needed = TRUE;
            }
            else                /* OP_TYPE_UPDATE */
            {
                /* in case of an update, validate current file stripe */
                rc = ListMgr_CheckStripe( lmgr, &p_op->entry_id );

                switch ( rc )
                {
                case DB_SUCCESS:
                    break;
                case DB_OUT_OF_DATE:
                case DB_NOT_EXISTS:
                    /* if stripe is missing or is out of date, retrieve it at next stage */
                    next_stage = STAGE_GET_INFO_FS;
                    p_op->extra_info_is_set = TRUE;
                    p_op->extra_info.getstripe_needed = TRUE;
                    break;
                default:
                    DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d checking file stripe", rc );
                }
            }
        }
    }
#endif

next_step:

    /* ack */
    rc = EntryProcessor_Acknowledge( p_op, next_stage, FALSE );
    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error acknowledging stage %s",
                    stage_info->stage_name );

    return rc;
}

/**
 * Fill extra info (stripe...) for entry
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
                        "Entry path is needed for retrieving stripe info" );
            return EINVAL;
        }
#endif

#ifdef HAVE_CHANGELOGS
        if ( p_op->extra_info.getattr_needed )
        {
            struct stat    entry_md;
            rc = errno = 0;
#if defined( _LUSTRE ) && defined( _HAVE_FID ) && defined( _MDS_STAT_SUPPORT )
           if ( global_config.direct_mds_stat )
               rc = lustre_mds_stat_by_fid( &p_op->entry_id, &entry_md );
           else
#endif
           if ( lstat( path, &entry_md ) != 0 )
              rc = errno;

            /* get entry attributes */
            if ( rc != 0 )
            {
                if ( ERR_MISSING(rc) )
                {
                    DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Entry %s does not exist anymore", path );
                    /* if the entry was not is db, skip the record */ 
                    if ( !p_op->db_exists )
                        goto skip_record;
                    else /* else, remove it from db */
                        goto rm_record;
                }
                else
                    DisplayLog( LVL_DEBUG, ENTRYPROC_TAG, "lstat() failed on %s: %s", path,
                                rc, strerror( rc ) );
                /* If lstat returns an error, drop the log record */
                goto skip_record;
            }

            /* convert them to internal structure */
#if defined( _LUSTRE ) && defined( _HAVE_FID ) && defined( _MDS_STAT_SUPPORT )
            PosixStat2EntryAttr( &entry_md, &p_op->entry_attr, !global_config.direct_mds_stat );
#else
            PosixStat2EntryAttr( &entry_md, &p_op->entry_attr, TRUE );
#endif
            p_op->entry_attr_is_set = TRUE;

            /* if the entry is not a file, not try to get stripe on it */
            if ( p_op->extra_info.getstripe_needed
                 && ATTR_MASK_TEST( &p_op->entry_attr, type )
                 && (strcmp( ATTR( &p_op->entry_attr, type ), STR_TYPE_FILE ) != 0) )
            {
                p_op->extra_info.getstripe_needed = FALSE;
            }

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

                if ( ERR_MISSING(rc) )
                {
                    DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Entry "DFID" does not exist anymore", PFID(&p_op->entry_id) );
                    /* if the entry was not is db, skip the record */
                    if ( !p_op->db_exists )
                        goto skip_record;
                    else /* else, remove it from db */
                        goto rm_record;
                }
                else if ( rc == 0 )
                {
                    ATTR_MASK_SET( &p_op->entry_attr, fullpath );
                    ATTR_MASK_SET( &p_op->entry_attr, md_update );
                    ATTR( &p_op->entry_attr, md_update ) = time( NULL );
                }

#if 0 /* could be used for future path management */
                else if ( !EMPTY_STRING( p_op->extra_info.log_record.p_log_rec->cr_name ) )
                {
                    strcpy( ATTR( &p_op->entry_attr, name ), p_op->extra_info.log_record.p_log_rec->cr_name  );
                    ATTR_MASK_SET( &p_op->entry_attr, name );
                    p_op->entry_attr_is_set = TRUE;
                }
#endif

            }
        } /* getpath needed */
#endif

        if ( p_op->extra_info.getstripe_needed )
        {
#ifdef _LUSTRE
            if ( File_GetStripeByPath( path,
                                       &ATTR( &p_op->entry_attr, stripe_info ),
                                       &ATTR( &p_op->entry_attr, stripe_items ) ) == 0 )
            {
                ATTR_MASK_SET( &p_op->entry_attr, stripe_info );
                ATTR_MASK_SET( &p_op->entry_attr, stripe_items );
            }
#endif
        }
    }

    /* acknowledge the stage and go to the next */
    rc = EntryProcessor_Acknowledge( p_op, STAGE_INFER_ATTRS, FALSE );
    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error acknowledging stage %s",
                    stage_info->stage_name );

    return rc;

#ifdef HAVE_CHANGELOGS
skip_record:
    /* do nothing on DB but ack the record */
    rc = EntryProcessor_Acknowledge( p_op, STAGE_CHGLOG_CLR, FALSE );
    if ( rc )
       DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage.", rc );
    return rc;

rm_record:
    p_op->db_op_type = OP_TYPE_REMOVE;
    rc = EntryProcessor_Acknowledge( p_op, STAGE_DB_APPLY, FALSE );
    if ( rc )
       DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage.", rc );
    return rc;
#endif
}

/**
 * Add extra information generated using current attributes
 * (whitelisted status...) 
 */
int EntryProc_infer_attrs( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];

    if ( p_op->entry_attr_is_set )
        check_policies( &p_op->entry_id, &p_op->entry_attr );

    /* acknowledge the stage and go to the next */
    rc = EntryProcessor_Acknowledge( p_op, STAGE_REPORTING, FALSE );
    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error acknowledging stage %s",
                    stage_info->stage_name );

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

        if ( EntryMatches( &p_op->entry_id, &p_op->entry_attr,
               &entry_proc_conf.alert_list[i].boolexpr ) == POLICY_MATCH )
        {
            /* build alert string and break */
            if ( ATTR_MASK_TEST( &p_op->entry_attr, fullpath ) )
                snprintf( strid, 1024, "'%s'", ATTR( &p_op->entry_attr, fullpath ) );
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
    {
        /* Send the alert */
        DisplayLog( LVL_EVENT, ENTRYPROC_TAG, "Raising alert about entry %s.", strid );
        DisplayAlert( "Robinhood alert: entry matches alert rule",
                      "Entry %s matches alert condition: %s.\n%s", strid, stralert, strvalues );
    }

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
