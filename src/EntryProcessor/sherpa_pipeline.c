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


/* forward declaration of EntryProc functions of pipeline */
int            EntryProc_get_fid( struct entry_proc_op_t *, lmgr_t * );
int            EntryProc_get_info_db( struct entry_proc_op_t *, lmgr_t * );
int            EntryProc_get_info_fs( struct entry_proc_op_t *, lmgr_t * );
int            EntryProc_infer_attrs( struct entry_proc_op_t *, lmgr_t * );
int            EntryProc_reporting( struct entry_proc_op_t *, lmgr_t * );
int            EntryProc_db_apply( struct entry_proc_op_t *, lmgr_t * );
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
#ifndef _SQLITE
    {STAGE_DB_APPLY, "STAGE_DB_APPLY", EntryProc_db_apply,
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC, 0},
#else
    /* SQLite locks the whole file for modifications...
     * So, 2 threads at max is enough at this step.
     */
    {STAGE_DB_APPLY, "STAGE_DB_APPLY", EntryProc_db_apply,
     STAGE_FLAG_MAX_THREADS | STAGE_FLAG_SYNC, 2},
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
        if ( !strncmp( ATTR(&p_op->entry_attr, name ), SHERPA_PREFIXE_TRANSFERT, strlen(SHERPA_PREFIXE_TRANSFERT) ) )
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

/**
 * Check if entry already exist in database.
 * In the case of lustre, also check its stripe.
 */
int EntryProc_get_info_db( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];
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

    p_op->db_exists = ListMgr_Exists( lmgr, &p_op->entry_id );

    if ( p_op->db_exists )
        p_op->db_op_type = OP_TYPE_UPDATE;
    else
        /* In case of an insert, initialize out_of_date to false */
        p_op->db_op_type = OP_TYPE_INSERT;

#ifdef _LUSTRE
    /* validate entry stripe in case of Lustre 
     * and only if the entry is a file 
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

    /* ack */
    rc = EntryProcessor_Acknowledge( p_op, STAGE_GET_INFO_FS, FALSE );
    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error acknowledging stage %s",
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

#ifdef _HAVE_FID
    char fid_path[1024];

    BuildFidPath( &p_op->entry_id, fid_path );

    /* get full path for this file */
    if ( !p_op->entry_attr_is_set || !ATTR_MASK_TEST( &p_op->entry_attr, fullpath ) )
    {
        rc = Lustre_GetFullPath( &p_op->entry_id, ATTR(&p_op->entry_attr, fullpath), 1024 );
        if ( rc )
        {
           DisplayLog( LVL_MAJOR, ENTRYPROC_TAG, "Can not get path for fid "DFID": (%d) %s",
                       PFID(&p_op->entry_id), rc, strerror(-rc) );
           goto skip;
        }
        ATTR_MASK_SET( &p_op->entry_attr, fullpath );
        p_op->entry_attr_is_set = TRUE;
    }
#else
    /* full path is mandatory */
    if ( !p_op->entry_attr_is_set || !ATTR_MASK_TEST( &p_op->entry_attr, fullpath ) )
    {
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                    "Entry path is needed to get entry info" );
        goto skip;
    }
#endif

    if ( p_op->extra_info_is_set && p_op->extra_info.getstripe_needed )
    {
        /* Get stripe info for this file */
#ifdef _LUSTRE
        if ( File_GetStripeByPath( 
#ifdef _HAVE_FID
                                   fid_path,
#else
                                   ATTR(&p_op->entry_attr, fullpath),
#endif
                                   &ATTR( &p_op->entry_attr, stripe_info ),
                                   &ATTR( &p_op->entry_attr, stripe_items ) ) == 0 )
        {
            p_op->entry_attr_is_set = TRUE;
            ATTR_MASK_SET( &p_op->entry_attr, stripe_info );
            ATTR_MASK_SET( &p_op->entry_attr, stripe_items );
        }
#endif
    }

    /* get info about this entry and check policies about the entry. */
    switch( SherpaManageEntry( &p_op->entry_id, &p_op->entry_attr ) )
    {
        case do_skip:
            goto skip;
        case do_rm:
            /* remove entry if it was in db */
            if ( p_op->db_exists )
            {
                rc = EntryProcessor_Acknowledge( p_op, STAGE_DB_APPLY, FALSE );
                if (rc)
                    DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error acknowledging stage %s",
                                stage_info->stage_name );
                return rc;
            }
            else
                goto skip;
            break;
        case do_update:
            /* OK, continue */
            break; 
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

        if ( EntryMatches
             ( &p_op->entry_id, &p_op->entry_attr,
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
        p_op->entry_attr.attr_mask &= ~readonly_attr_set;
        rc = ListMgr_Insert( lmgr, &p_op->entry_id, &p_op->entry_attr );
        break;
    case OP_TYPE_UPDATE:
        p_op->entry_attr.attr_mask &= ~readonly_attr_set;
        rc = ListMgr_Update( lmgr, &p_op->entry_id, &p_op->entry_attr );
        break;
    default:
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Unhandled DB operation type: %d", p_op->db_op_type );
        rc = -1;
    }

    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d performing database operation.", rc );
    else if ( p_op->callback_func )
    {
        /* if operation was commited, Perform callback to info collector */
        if ( ListMgr_GetCommitStatus( lmgr ) )
            p_op->callback_func( p_op, p_op->callback_param );
    }

    /* Acknoledge the operation and remove it from pipeline */
    rc = EntryProcessor_Acknowledge( p_op, 0, TRUE );
    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknoledging stage %s.", rc,
                    stage_info->stage_name );

    return rc;
}

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
