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

#define ERR_MISSING(_err) (((_err)==ENOENT)||((_err)==ESTALE))

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
#if defined( _SQLITE )
     /* SQLite locks the whole file for modifications...
     * So, 1 single threads is enough at this step.
     */
     STAGE_FLAG_MAX_THREADS | STAGE_FLAG_SYNC, 1},
#else
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC, 0},
#endif

    /* only 1 thread here because commiting records must be sequential
     * (in the same order as changelog) */
    {STAGE_CHGLOG_CLR, "STAGE_CHGLOG_CLR", EntryProc_chglog_clr,
     STAGE_FLAG_SEQUENTIAL | STAGE_FLAG_SYNC, 1},

    /* this step is for mass update / mass remove operations when
     * starting/ending a FS scan. */
    {STAGE_RM_OLD_ENTRIES, "STAGE_RM_OLD_ENTRIES", EntryProc_rm_old_entries,
     STAGE_FLAG_SEQUENTIAL | STAGE_FLAG_SYNC, 0}
};


/**
 * For entries from FS scan, we must get the associated entry ID.
 */
int EntryProc_get_fid( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;
    entry_id_t     tmp_id;

    if ( !p_op->entry_attr_is_set
         || !ATTR_MASK_TEST( &p_op->entry_attr, fullpath ) )
    {
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                    "Error: entry full path is expected to be set"
                    " in STAGE_GET_FID stage" );
        EntryProcessor_Acknowledge( p_op, -1, TRUE );
        return EINVAL;
    }

    /* perform path2fid */
    rc = Lustre_GetFidFromPath( ATTR( &p_op->entry_attr, fullpath ), &tmp_id );
    if ( rc )
    {
        /* remove the operation from pipeline */
        rc = EntryProcessor_Acknowledge( p_op, -1, TRUE );
        if ( rc )
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                        "Error %d acknowledging stage STAGE_GET_FID.", rc );
    }
    else
    {
        rc = EntryProcessor_SetEntryId( p_op, &tmp_id );
        if ( rc )
        {
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d setting entry id",
                        rc );
            /* remove entry from pipeline */
            EntryProcessor_Acknowledge( p_op, -1, TRUE );
            return rc;
        }

        /* go to GET_INFO_DB stage */
        rc = EntryProcessor_Acknowledge( p_op, STAGE_GET_INFO_DB, FALSE );
        if ( rc )
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                        "Error %d acknowledging stage STAGE_GET_FID.", rc );
    }
    return rc;
}

/**
 * Infer information from the changelog record (HSM status, ...).
 * \return next pipeline step to be perfomed.
 */
static int EntryProc_FillFromLogRec( struct entry_proc_op_t *p_op,
                                     int allow_md_updt, int allow_path_updt )
{
    /* alias to the log record */
    CL_REC_TYPE *logrec = p_op->extra_info.log_record.p_log_rec;

    /* if this is a CREATE record, we know that its status is NEW. */
    if ( logrec->cr_type == CL_CREATE )
    {
        /* Sanity check: if the entry already exists in DB,
         * it could come from a previous filesystem that has been formatted.
         * In this case, force a full update of the entry.
         */
        if ( p_op->db_exists )
        {
            if ( p_op->entry_attr_is_set
                 && ATTR_MASK_TEST( &p_op->entry_attr, fullpath ) )
                DisplayLog( LVL_MAJOR, ENTRYPROC_TAG,
                            "WARNING: CREATE record on already existing entry fid="
                            DFID", path=%s", PFID(&p_op->entry_id),
                            ATTR( &p_op->entry_attr, fullpath ) );
            else
                DisplayLog( LVL_MAJOR, ENTRYPROC_TAG,
                            "WARNING: CREATE record on already existing entry fid="
                            DFID, PFID(&p_op->entry_id) );

            /* set insertion time, like for a new entry */
            p_op->entry_attr_is_set = TRUE;
            ATTR_MASK_SET( &p_op->entry_attr, creation_time );
            ATTR( &p_op->entry_attr, creation_time )
                = cltime2sec(logrec->cr_time);

            p_op->extra_info_is_set = TRUE;
            p_op->extra_info.getattr_needed = TRUE;
            p_op->extra_info.getpath_needed = TRUE;
            p_op->extra_info.getstripe_needed = TRUE;
            p_op->extra_info.getstatus_needed = TRUE;
        }
        else /* new entry, its status is NEW */
        {
            p_op->entry_attr_is_set = TRUE;
            ATTR_MASK_SET( &p_op->entry_attr, status );
            ATTR( &p_op->entry_attr, status ) = STATUS_NEW;

            /* no flag is set for now */
            ATTR_MASK_SET( &p_op->entry_attr, no_archive );
            ATTR( &p_op->entry_attr, no_archive ) = 0;
            ATTR_MASK_SET( &p_op->entry_attr, no_release );
            ATTR( &p_op->entry_attr, no_release ) = 0;
            p_op->extra_info.getstatus_needed = FALSE;

            /* new entry: never archived or restored */
            ATTR( &p_op->entry_attr, last_archive ) = 0;
            ATTR_MASK_SET( &p_op->entry_attr, last_archive );
            ATTR( &p_op->entry_attr, last_restore ) = 0;
            ATTR_MASK_SET( &p_op->entry_attr, last_restore );
        }
    }
    else if ((logrec->cr_type == CL_MKDIR )
            || (logrec->cr_type == CL_RMDIR ))
    {
        /* entry is a directory */
        ATTR_MASK_SET( &p_op->entry_attr, type );
        strcpy( ATTR( &p_op->entry_attr, type ), STR_TYPE_DIR );
        p_op->extra_info.getstripe_needed = FALSE;
        p_op->extra_info.getstatus_needed = FALSE;
    }
    else if (logrec->cr_type == CL_UNLINK )
    {
        /* in any case, update the path because the stored path
         * may be the removed one. */
        p_op->extra_info_is_set = TRUE;
        p_op->extra_info.getpath_needed = TRUE;
        /* if the log record does not indicate if the entry still exists,
         * force performing "lstat()" after UNLINK, to verify if the entry
         * still exists.  Also need to get status to test if there is an
         * orphan entry in the backend.
         */
        p_op->extra_info.getattr_needed = TRUE;
        if ( policies.unlink_policy.hsm_remove )
            p_op->extra_info.getstatus_needed = TRUE;
    }
    else if ( logrec->cr_type == CL_HSM )
    {
        switch ( hsm_get_cl_event( logrec->cr_flags ) )
        {
            case HE_ARCHIVE:
               /* is it a successfull copy? */
               if ( hsm_get_cl_error( logrec->cr_flags ) == CLF_HSM_SUCCESS )
               {
                   /* remember last archive time */
                   p_op->entry_attr_is_set = TRUE;
                   ATTR_MASK_SET( &p_op->entry_attr, last_archive );
                   ATTR( &p_op->entry_attr, last_archive )
                        = cltime2sec( logrec->cr_time );

                   /* if dirty flag is set, the entry is dirty,
                    * else, it is up to date. */
                   ATTR_MASK_SET( &p_op->entry_attr, status );
                   ATTR( &p_op->entry_attr, status ) =
                        (hsm_get_cl_flags(logrec->cr_flags) & CLF_HSM_DIRTY) ?
                        STATUS_MODIFIED : STATUS_SYNCHRO;
                   p_op->extra_info.getstatus_needed = FALSE;
               }
               else /* archive failed */
               {
                   /* Entry is probably still dirty. If dirty flag is not set,
                    * we need to ask the actual status */
                   if ( hsm_get_cl_flags(logrec->cr_flags) & CLF_HSM_DIRTY )
                   {
                        p_op->entry_attr_is_set = TRUE;
                        ATTR_MASK_SET( &p_op->entry_attr, status );
                        ATTR( &p_op->entry_attr, status ) = STATUS_MODIFIED;
                        p_op->extra_info.getstatus_needed = FALSE;
                   }
                   else
                        p_op->extra_info.getstatus_needed = TRUE;
               }
               break;

            case HE_RESTORE:
                if ( hsm_get_cl_error( logrec->cr_flags ) == CLF_HSM_SUCCESS )
                {
                    /* remember last restore time */
                    p_op->entry_attr_is_set = TRUE;
                    ATTR_MASK_SET( &p_op->entry_attr, last_restore );
                    ATTR( &p_op->entry_attr, last_restore )
                        = cltime2sec( logrec->cr_time );

                    /* status is 'up-to-date' after a successful restore */
                    ATTR_MASK_SET( &p_op->entry_attr, status );
                    ATTR( &p_op->entry_attr, status ) = STATUS_SYNCHRO;
                    p_op->extra_info.getstatus_needed = FALSE;
                }
                else if ( p_op->db_exists )
                {
                    /* Entry status remains 'released':
                     * remove entry from PE working set */
                    p_op->db_op_type = OP_TYPE_REMOVE;
                    return STAGE_DB_APPLY;
                }
                else /* entry is not in PE working set */
                {
                    /* skip and clear the record */
                    return STAGE_CHGLOG_CLR;
                }
                break;

            case HE_RELEASE:
                if ( hsm_get_cl_error( logrec->cr_flags ) != CLF_HSM_SUCCESS )
                {
                    /* release records are not expected to be errorneous */
                    DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "ERROR: "
                         "Unexpected HSM release event with error %d",
                         hsm_get_cl_error(logrec->cr_flags) );
                    /* make sure of actual entry status */
                    p_op->extra_info.getstatus_needed = TRUE;
                }
                else if ( p_op->db_exists )
                {
                    /* Entry is now 'released': remove it from PE working set */
                    p_op->db_op_type = OP_TYPE_REMOVE;
                    return STAGE_DB_APPLY;
                }
                else /* entry is not in PE working set */
                {
                    /* skip and clear the record */
                    return STAGE_CHGLOG_CLR;
                }
                break;

            case HE_STATE:
                /* state changed: did it become dirty? */
                if ( hsm_get_cl_flags(logrec->cr_flags) & CLF_HSM_DIRTY )
                {
                    p_op->entry_attr_is_set = TRUE;
                    ATTR_MASK_SET( &p_op->entry_attr, status );
                    ATTR( &p_op->entry_attr, status ) = STATUS_MODIFIED;
                }
                else /* other status change: need to get it */
                    p_op->extra_info.getstatus_needed = TRUE;
                break;

            case HE_REMOVE:
            case HE_CANCEL:
                /* undetermined status after such an event */
                p_op->extra_info.getstatus_needed = TRUE;
                break;
            default:
                DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "ERROR: unknown HSM event:"
                            "bitfield=%#x, event=%u", logrec->cr_flags,
                            hsm_get_cl_event( logrec->cr_flags ) );
                /* skip */
                return -1;
        }
    }
    else if (CL_CHG_TIME(logrec->cr_type) || (logrec->cr_type == CL_SETATTR))
    {
        /* need to update attrs */
        p_op->extra_info_is_set = TRUE;
        p_op->extra_info.getattr_needed = TRUE;
        DisplayLog( LVL_DEBUG, ENTRYPROC_TAG,
                    "getattr needed because this is a CTIME or SATTR event" );
    }

    /* if the entry is already in DB, try to determine if something changed */
    if ( p_op->db_exists )
    {
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

        /* get the new attributes, in case of a SATTR, xTIME, HSM... */
        if ( allow_md_updt && (CL_MOD_TIME(logrec->cr_type)
                               || CL_CHG_TIME(logrec->cr_type)
                               || ( logrec->cr_type == CL_CLOSE )
                               || ( logrec->cr_type == CL_TRUNC )
                               || ( logrec->cr_type == CL_HSM )
                               || ( logrec->cr_type == CL_SETATTR )) )
        {
            DisplayLog( LVL_DEBUG, ENTRYPROC_TAG,
                        "Getattr needed because this is a TIME, TRUNC, SETATTR, HSM or CLOSE event, and "
                         "metadata has not been recently updated. event=%s",
                         changelog_type2str(logrec->cr_type) );

            p_op->extra_info.getattr_needed = TRUE;
        }
    }

    /* other records: keep default value for getstatus_needed */

    return STAGE_GET_INFO_FS;
}


/**
 *  Infer information and determine needed information from a changelog record.
 *  \return the next pipeline step to be performed,
 *          -1 if entry must be dropped.
 */
static int EntryProc_ProcessLogRec( struct entry_proc_op_t *p_op )
{
    /* short alias */
    CL_REC_TYPE *logrec = p_op->extra_info.log_record.p_log_rec;

    /* allow event-driven update */
    int md_allow_event_updt = TRUE;
    int path_allow_event_updt = TRUE;

    /* is there parent_id in log rec ? */
    if ( logrec->cr_namelen > 0 )
    {
        ATTR_MASK_SET( &p_op->entry_attr, parent_id );
        ATTR( &p_op->entry_attr, parent_id ) = logrec->cr_pfid;
    }

    if ( logrec->cr_type == CL_UNLINK )
    {
        DisplayLog( LVL_DEBUG, ENTRYPROC_TAG,
                    "UNLINK on %s entry "DFID": last=%s, archived=%s",
                    p_op->db_exists?"known":"unknown", PFID(&p_op->entry_id),
                    bool2str( logrec->cr_flags & CLF_UNLINK_LAST ),
                    bool2str( logrec->cr_flags & CLF_UNLINK_HSM_EXISTS ) );

        /* it it the last reference to this file? */
        if ( logrec->cr_flags & CLF_UNLINK_LAST )
        {
            if ( !policies.unlink_policy.hsm_remove ||
                 !(logrec->cr_flags & CLF_UNLINK_HSM_EXISTS) )
            {
                /*  hsm_remove is disabled or file doesn't exist in the HSM:
                 * If the file was in DB: remove it, else skip the record. */
                if ( p_op->db_exists )
                {
                    p_op->db_op_type = OP_TYPE_REMOVE;
                    return STAGE_DB_APPLY;
                }
                else
                    return STAGE_CHGLOG_CLR;
            }
            else /* hsm removal enabled and file is archived */
            {
                /* If the entry exists in DB, this moves it from main table
                 * to a remove queue, else, just insert it to remove queue. */
                p_op->db_op_type = OP_TYPE_SOFT_REMOVE;
                return STAGE_DB_APPLY;
            }
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

    if ( !p_op->db_exists )
    {
        /* non-unlink (or non-destructive unlink) record on unknown entry:
         * insert entry to the DB */
        p_op->db_op_type = OP_TYPE_INSERT;

        /* new entry, set insertion time */
        p_op->entry_attr_is_set = TRUE;
        ATTR_MASK_SET( &p_op->entry_attr, creation_time );
        ATTR( &p_op->entry_attr, creation_time ) = cltime2sec(logrec->cr_time);

        /* we must get info that is not provided by the chglog */
        p_op->extra_info_is_set = TRUE;
        p_op->extra_info.getattr_needed = TRUE;
        p_op->extra_info.getpath_needed = TRUE;
        p_op->extra_info.getstripe_needed = TRUE;
        /* By default, get status for a new record.
         * This is overwritten by EntryProc_FillFromLogRec if the status
         * can be infered from changelog rec. */
        p_op->extra_info.getstatus_needed = TRUE;
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
            p_op->extra_info.getstatus_needed = TRUE;
        }
        else
        {
            p_op->extra_info_is_set = TRUE;

            /* get stripe info only if we don't already know it. */
            p_op->extra_info.getstripe_needed =
                ATTR_MASK_TEST( &p_op->entry_attr, stripe_info )? FALSE : TRUE;

            /* get HSM status only if it is not already in the DB. */
            p_op->extra_info.getstatus_needed =
                ATTR_MASK_TEST( &p_op->entry_attr, status )? FALSE : TRUE;

            /* Check md_update policy */
            p_op->extra_info.getattr_needed
                = need_md_update( &p_op->entry_attr, &md_allow_event_updt );

            /* check if path update is needed */
            p_op->extra_info.getpath_needed
                = need_path_update( &p_op->entry_attr, &path_allow_event_updt );
        }
    }

    /* infer info from changelog record, then continue to next step */
    return EntryProc_FillFromLogRec( p_op, md_allow_event_updt,
                                     path_allow_event_updt );
}


/**
 * check if the entry exists in the database and what info
 * must be retrieved.
 */
int EntryProc_get_info_db( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;
    int            next_stage = -1; /* -1 = skip */
    const pipeline_stage_t
        *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];

    /* is this a changelog record? */
    if ( p_op->extra_info.is_changelog_record )
    {
        CL_REC_TYPE *logrec = p_op->extra_info.log_record.p_log_rec;

        /* what do we know about this entry? */
        ATTR_MASK_INIT( &p_op->entry_attr );
        ATTR_MASK_SET( &p_op->entry_attr, fullpath );
        ATTR_MASK_SET( &p_op->entry_attr, name );
        ATTR_MASK_SET( &p_op->entry_attr, type );
        ATTR_MASK_SET( &p_op->entry_attr, stripe_info );
        ATTR_MASK_SET( &p_op->entry_attr, md_update );
        ATTR_MASK_SET( &p_op->entry_attr, path_update );
        ATTR_MASK_SET( &p_op->entry_attr, status );
#ifdef ATTR_INDEX_creation_time
        if (entry_proc_conf.detect_fake_mtime)
            ATTR_MASK_SET( &p_op->entry_attr, creation_time);
#endif

        if ( entry_proc_conf.match_classes )
        {
            /* get fileclass update info to know if we must check it */
            ATTR_MASK_SET( &p_op->entry_attr, rel_cl_update );
            ATTR_MASK_SET( &p_op->entry_attr, release_class );
            ATTR_MASK_SET( &p_op->entry_attr, arch_cl_update );
            ATTR_MASK_SET( &p_op->entry_attr, archive_class );
            p_op->entry_attr.attr_mask |= policies.purge_policies.global_attr_mask;
            p_op->entry_attr.attr_mask |= policies.migr_policies.global_attr_mask;
        }
        p_op->entry_attr.attr_mask |= entry_proc_conf.alert_attr_mask;

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
                    "getstripe=%u, getattr=%u, getpath=%u, getstatus=%u",
                    changelog_type2str(logrec->cr_type), PFID(&p_op->entry_id),
                    logrec->cr_flags & CLF_FLAGMASK, logrec->cr_namelen>0?
                    logrec->cr_name:"<null>",
                    p_op->extra_info.getstripe_needed?1:0,
                    p_op->extra_info.getattr_needed?1:0,
                    p_op->extra_info.getpath_needed?1:0,
                    p_op->extra_info.getstatus_needed?1:0 );
    }
    else /* entry from FS scan */
    {
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

        /* check if the entry exists in DB */
        p_op->db_exists = ListMgr_Exists( lmgr, &p_op->entry_id );

        if ( p_op->db_exists )
        {
            int needed = entry_proc_conf.alert_attr_mask;

#ifdef ATTR_INDEX_creation_time
            if (entry_proc_conf.detect_fake_mtime)
                needed |= ATTR_MASK_creation_time;
#endif

            /* retrieve missing attrs, if needed */
            if ( entry_proc_conf.match_classes ||
                (needed & ~p_op->entry_attr.attr_mask) )
            {
                attr_set_t tmp_attr;

                ATTR_MASK_INIT( &tmp_attr );

                tmp_attr.attr_mask |= (needed & ~p_op->entry_attr.attr_mask);

                /* no class for directories */
                if ( ATTR_MASK_TEST(&p_op->entry_attr, type) &&
                     strcmp( ATTR(&p_op->entry_attr, type), STR_TYPE_DIR ) != 0 )
                {
                    if ( entry_proc_conf.match_classes )
                    {
                        /* get fileclass update info to know if we must check it */
                        ATTR_MASK_SET( &tmp_attr, rel_cl_update );
                        ATTR_MASK_SET( &tmp_attr, release_class );

                        tmp_attr.attr_mask |= (policies.purge_policies.global_attr_mask
                                               & ~p_op->entry_attr.attr_mask);

                        ATTR_MASK_SET( &tmp_attr, arch_cl_update );
                        ATTR_MASK_SET( &tmp_attr, archive_class );

                        tmp_attr.attr_mask |= (policies.migr_policies.global_attr_mask
                                               & ~p_op->entry_attr.attr_mask);
                    }
                    /* no dircount for files */
                    tmp_attr.attr_mask &= ~ATTR_MASK_dircount;
                }

                if( tmp_attr.attr_mask )
                {
                    rc = ListMgr_Get( lmgr, &p_op->entry_id, &tmp_attr );

                    if (rc == DB_SUCCESS )
                    {
                        p_op->entry_attr_is_set = TRUE;
                        /* merge with main attr set */
                        ListMgr_MergeAttrSets( &p_op->entry_attr, &tmp_attr, FALSE );
                    }
                    else if (rc == DB_NOT_EXISTS )
                    {
                        /* this kind of attributes do not apply to this type of entry */
                        DisplayLog( LVL_FULL, ENTRYPROC_TAG, "No such attribute found for this entry: type=%s, attr_mask=%#x",
                                    ATTR(&p_op->entry_attr, type), tmp_attr.attr_mask );
                    }
                    else
                    {
                        /* ERROR */
                        DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                                    "Error %d retrieving entry "DFID" from DB", rc,
                                    PFID(&p_op->entry_id) );
                    }
                }
            }
        } /* end if exist in db */

        if ( !p_op->db_exists )
        {
            /* new entry */
            p_op->db_op_type = OP_TYPE_INSERT;

            if (!ATTR_MASK_TEST(&p_op->entry_attr, creation_time))
            {
                ATTR_MASK_SET( &p_op->entry_attr, creation_time );
                ATTR( &p_op->entry_attr, creation_time ) = time( NULL );
            }

            /* defaults */
            p_op->extra_info_is_set = TRUE;
            p_op->extra_info.getstatus_needed = TRUE;
            p_op->extra_info.getstripe_needed = TRUE;
            p_op->extra_info.getattr_needed = FALSE;
            p_op->extra_info.getpath_needed = FALSE;

            if ( p_op->entry_attr_is_set
                 && ATTR_MASK_TEST( &p_op->entry_attr, type )
                 && strcmp( ATTR( &p_op->entry_attr, type ), STR_TYPE_FILE ) != 0 )
            {
                /* not a file, for sure */
                p_op->extra_info.getstatus_needed = FALSE;
                p_op->extra_info.getstripe_needed = FALSE;
            }

            next_stage = STAGE_GET_INFO_FS;
        }
        else
        {
            p_op->db_op_type = OP_TYPE_UPDATE;

            /* FS scan is used for resynchronizing DB, so we always get status */
            p_op->extra_info_is_set = TRUE;
            p_op->extra_info.getstatus_needed = FALSE;
            p_op->extra_info.getattr_needed = FALSE;
            p_op->extra_info.getpath_needed = FALSE;

            if ( p_op->entry_attr_is_set
                 && ATTR_MASK_TEST( &p_op->entry_attr, type )
                 && !strcmp( ATTR( &p_op->entry_attr, type ), STR_TYPE_FILE ) )
            {
                /* only get status for files */
                p_op->extra_info.getstatus_needed = TRUE;

                /* Does file has stripe info ? */
                if ( ListMgr_CheckStripe( lmgr, &p_op->entry_id ) != DB_SUCCESS )
                {
                    DisplayLog( LVL_DEBUG, ENTRYPROC_TAG, "Stripe information is missing" );
                    p_op->extra_info.getstripe_needed = TRUE;
                }
            }

            next_stage = STAGE_GET_INFO_FS;
        }
    } /* end if scan */

next_step:
    if ( next_stage == -1 )
        /* drop the entry */
        rc = EntryProcessor_Acknowledge( p_op, -1, TRUE );
    else
        /* go to next pipeline step */
        rc = EntryProcessor_Acknowledge( p_op, next_stage, FALSE );

    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                    "Error %d acknowledging stage %s.", rc,
                    stage_info->stage_name );
    return rc;
}



int EntryProc_get_info_fs( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;

    if ( p_op->extra_info_is_set )
    {
        char           path[RBH_PATH_MAX];

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
                if ( ERR_MISSING(rc) )
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

#ifdef ATTR_INDEX_creation_time
        if (entry_proc_conf.detect_fake_mtime)
        {
            if (ATTR_MASK_TEST(&p_op->entry_attr, creation_time)
                && ATTR_MASK_TEST(&p_op->entry_attr, last_mod)
                && ATTR(&p_op->entry_attr, last_mod) < ATTR(&p_op->entry_attr, creation_time))
            {
                time_t val;
                char mt[128];
                char ct[128];
                struct tm      t;
                val = ATTR(&p_op->entry_attr, last_mod);
                strftime(mt, 128, "%Y/%m/%d %T", localtime_r(&val, &t));
                val = ATTR(&p_op->entry_attr, creation_time);
                strftime(ct, 128, "%Y/%m/%d %T", localtime_r(&val, &t));

                if (ATTR_MASK_TEST(&p_op->entry_attr, fullpath))
                    DisplayLog(LVL_VERB, ENTRYPROC_TAG,
                               "Fake mtime detected for %s: mtime=%s, creation=%s",
                               ATTR(&p_op->entry_attr, fullpath), mt, ct);
                else
                    DisplayLog(LVL_VERB, ENTRYPROC_TAG,
                               "Fake mtime detected for "DFID": mtime=%s, creation=%s",
                               PFID(&p_op->entry_id), mt, ct);
            }
        }
#endif

        if ( p_op->extra_info.getpath_needed )
        {
            /* get entry path (only for log records) */
            if ( p_op->extra_info.is_changelog_record )
            {
                char pathnew[RBH_PATH_MAX];
                /* /!\ Lustre_GetFullPath modifies fullpath even on failure,
                 * so, first write to a tmp buffer */
                rc = Lustre_GetFullPath( &p_op->entry_id,
                                         pathnew, RBH_PATH_MAX );

                if ( ERR_MISSING( abs( rc )) )
                {
                    DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Entry "DFID" does not exist anymore", PFID(&p_op->entry_id) );
                    /* in this case, an UNLINK event will be raised, so we ignore current record */
                    goto skip_record;
                }
                else if ( rc == 0 )
                {
                    strcpy( ATTR( &p_op->entry_attr, fullpath ), pathnew );
                    ATTR_MASK_SET( &p_op->entry_attr, fullpath );
                    ATTR_MASK_SET( &p_op->entry_attr, path_update );
                    ATTR( &p_op->entry_attr, path_update ) = time( NULL );
                }
#if 0
                else if ( !EMPTY_STRING( p_op->extra_info.log_record.p_log_rec->cr_name ) )
                {
                    /* @TODO can be useful for future path management */
                }
#endif

            }
        } /* getpath needed */

        /* getstripe only for files */
        if ( p_op->extra_info.getstripe_needed
             && p_op->entry_attr_is_set
             && ATTR_MASK_TEST( &p_op->entry_attr, type )
             && strcmp( ATTR( &p_op->entry_attr, type ), STR_TYPE_FILE ) != 0 )
            p_op->extra_info.getstripe_needed = FALSE;

        if ( p_op->extra_info.getstripe_needed )
        {
            /* get entry stripe */
            rc = File_GetStripeByPath( path,
                                       &ATTR( &p_op->entry_attr, stripe_info ),
                                       &ATTR( &p_op->entry_attr, stripe_items ) );
            if (rc)
            {
                ATTR_MASK_UNSET( &p_op->entry_attr, stripe_info );
                ATTR_MASK_UNSET( &p_op->entry_attr, stripe_items );
            }

            if ( ERR_MISSING(abs(rc)) )
            {
                DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Entry "DFID" does not exist anymore",
                            PFID(&p_op->entry_id) );
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

        /* get status only for files */
        if ( p_op->extra_info.getstatus_needed
             && p_op->entry_attr_is_set
             && ATTR_MASK_TEST( &p_op->entry_attr, type )
             && strcmp( ATTR( &p_op->entry_attr, type ), STR_TYPE_FILE ) != 0 )
            p_op->extra_info.getstatus_needed = FALSE;

        if ( p_op->extra_info.getstatus_needed )
        {
            /* get entry status */
            rc = LustreHSM_GetStatus( path, &ATTR( &p_op->entry_attr, status ),
                                      &ATTR( &p_op->entry_attr, no_release ),
                                      &ATTR( &p_op->entry_attr, no_archive ) );
            if ( ERR_MISSING( abs( rc )) )
            {
                DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Entry "DFID" does not exist anymore",
                            PFID(&p_op->entry_id) );
                /* in this case, an UNLINK event will be raised, so we ignore current record */
                goto skip_record;
            }
            else if ( rc == 0 )
            {
                ATTR_MASK_SET( &p_op->entry_attr, status );
                ATTR_MASK_SET( &p_op->entry_attr, no_release );
                ATTR_MASK_SET( &p_op->entry_attr, no_archive );

                /* if the entry has no flags, the entry has never been archived or restored */
                if ( ATTR( &p_op->entry_attr, status ) == STATUS_NEW )
                {
                    ATTR_MASK_SET( &p_op->entry_attr, last_archive );
                    ATTR( &p_op->entry_attr, last_archive ) = 0;
                    ATTR_MASK_SET( &p_op->entry_attr, last_restore );
                    ATTR( &p_op->entry_attr, last_restore ) = 0;
                }
            }
        } /* get_status needed */
    }

    /* match fileclasses if specified in config */
    if ( entry_proc_conf.match_classes )
        check_policies( &p_op->entry_id, &p_op->entry_attr, TRUE );

    /* set other info */

    if ( p_op->extra_info.is_changelog_record )
    {
        p_op->entry_attr_is_set = TRUE;
        ATTR_MASK_SET( &p_op->entry_attr, last_op_index );
        ATTR( &p_op->entry_attr, last_op_index ) = p_op->extra_info.log_record.p_log_rec->cr_index;
    }


    rc = EntryProcessor_Acknowledge( p_op, STAGE_REPORTING, FALSE );
    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage.", rc );
    return rc;

skip_record:
    if ( p_op->extra_info.is_changelog_record )
    /* do nothing on DB but ack the record */
        rc = EntryProcessor_Acknowledge( p_op, STAGE_CHGLOG_CLR, FALSE );
    else
        /* remove the operation from pipeline */
        rc = EntryProcessor_Acknowledge( p_op, -1, TRUE );
    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage.", rc );
    return rc;

}


int EntryProc_reporting( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc, i;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];
    int            is_alert = FALSE;
    char           stralert[2*RBH_PATH_MAX];
    char           strvalues[2*RBH_PATH_MAX];
    char           strid[RBH_PATH_MAX];
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

        if ( EntryMatches( &p_op->entry_id, &p_op->entry_attr,
               &entry_proc_conf.alert_list[i].boolexpr, NULL ) == POLICY_MATCH )
        {
            /* build alert string and break */
            if ( ATTR_MASK_TEST( &p_op->entry_attr, fullpath ) )
                snprintf( strid, RBH_PATH_MAX, "%s", ATTR( &p_op->entry_attr, fullpath ) );
            else
                snprintf( strid, RBH_PATH_MAX, "fid[seq, oid]=[%llu, %u]", p_op->entry_id.f_seq,
                          p_op->entry_id.f_oid );

            rc = BoolExpr2str( &entry_proc_conf.alert_list[i].boolexpr, stralert, 2*RBH_PATH_MAX );
            if ( rc < 0 )
                strcpy( stralert, "Error building alert string" );

            PrintAttrs( strvalues, 2*RBH_PATH_MAX, &p_op->entry_attr,
                        entry_proc_conf.alert_list[i].attr_mask );

            if ( EMPTY_STRING(entry_proc_conf.alert_list[i].title) )
                title = NULL;
            else
                title = entry_proc_conf.alert_list[i].title;

            is_alert = TRUE;
            break;
        }
    }

    /* acknowledge now if the stage is asynchronous */
    if ( stage_info->stage_flags & STAGE_FLAG_ASYNC )
    {
        rc = EntryProcessor_Acknowledge( p_op, STAGE_DB_APPLY, FALSE );
        if ( rc )
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error acknowledging stage %s",
                        stage_info->stage_name );
    }

    if ( is_alert )
        RaiseEntryAlert(title, stralert, strid, strvalues );

    /* acknowledge now if the stage was synchronous */
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

    /* if stripe has not been retrieved, don't update it in the database */
    if ( !p_op->extra_info.getstripe_needed )
    {
        ATTR_MASK_UNSET( &p_op->entry_attr, stripe_info );
        ATTR_MASK_UNSET( &p_op->entry_attr, stripe_items );
    }

#ifdef ATTR_INDEX_creation_time
    /* never change creation time */
    if (p_op->db_op_type != OP_TYPE_INSERT)
        ATTR_MASK_UNSET( &p_op->entry_attr, creation_time );
#endif

    /* insert to DB */
    switch ( p_op->db_op_type )
    {
    case OP_TYPE_INSERT:
        p_op->entry_attr.attr_mask &= ~readonly_attr_set;
        DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Insert("DFID")", PFID(&p_op->entry_id) );
        rc = ListMgr_Insert( lmgr, &p_op->entry_id, &p_op->entry_attr, FALSE );
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

    /* Acknowledge the operation if there is a callback */
    if ( p_op->callback_func )
        rc = EntryProcessor_Acknowledge( p_op, STAGE_CHGLOG_CLR, FALSE );
    else
        rc = EntryProcessor_Acknowledge( p_op, -1, TRUE );

    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage %s.", rc,
                    stage_info->stage_name );

    return rc;
}

int            EntryProc_chglog_clr( struct entry_proc_op_t * p_op, lmgr_t * lmgr )
{
    int            rc;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];
    CL_REC_TYPE *logrec = p_op->extra_info.log_record.p_log_rec;

    if (p_op->extra_info.is_changelog_record)
        DisplayLog( LVL_FULL, ENTRYPROC_TAG, "stage %s - record #%llu - id="DFID"\n", stage_info->stage_name,
                logrec->cr_index, PFID(&p_op->entry_id) );

    if ( p_op->callback_func )
    {
        /* if operation was commited, Perform callback to info collector */
        rc = p_op->callback_func( lmgr, p_op, p_op->callback_param );

        if ( rc )
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d performing callback at stage %s.", rc,
                        stage_info->stage_name );
    }

    /* Acknowledge the operation and remove it from pipeline */
    rc = EntryProcessor_Acknowledge( p_op, -1, TRUE );
    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage %s.", rc,
                    stage_info->stage_name );

    return rc;
}

int EntryProc_rm_old_entries( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];
    lmgr_filter_t  filter;
    filter_value_t val;

    lmgr_simple_filter_init( &filter );

    val.val_uint = ATTR( &p_op->entry_attr, md_update );
    lmgr_simple_filter_add( &filter, ATTR_INDEX_md_update, LESSTHAN_STRICT, val, 0 );

    /* partial scan: remove non-updated entries from a subset of the namespace */
    if (ATTR_MASK_TEST( &p_op->entry_attr, fullpath ))
    {
        char tmp[RBH_PATH_MAX];
        strcpy(tmp, ATTR(&p_op->entry_attr, fullpath));
        strcat(tmp, "/*");
        val.val_str = tmp;
        lmgr_simple_filter_add( &filter, ATTR_INDEX_fullpath, LIKE, val, 0 );
    }

    /* force commit after this operation */
    ListMgr_ForceCommitFlag( lmgr, TRUE );

    /* remove entries listed in previous scans */
    if (policies.unlink_policy.hsm_remove)
        rc = ListMgr_MassSoftRemove( lmgr, &filter, time(NULL) + policies.unlink_policy.deferred_remove_delay );
    else
        rc = ListMgr_MassRemove( lmgr, &filter );

    lmgr_simple_filter_free( &filter );

    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                    "Error: ListMgr MassRemove operation failed with code %d.", rc );

    /* must call callback function in any case, to unblock the scan */
    if ( p_op->callback_func )
    {
        /* Perform callback to info collector */
        p_op->callback_func( lmgr, p_op, p_op->callback_param );
    }

    // update last scan end time moved to callback

    /* unset force commit flag */
    ListMgr_ForceCommitFlag( lmgr, FALSE );

    rc = EntryProcessor_Acknowledge( p_op, -1, TRUE );

    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage %s.", rc,
                    stage_info->stage_name );

    return rc;

}
