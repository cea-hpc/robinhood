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
 * Common pipeline functions
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "RobinhoodConfig.h"
#include "RobinhoodLogs.h"
#include "RobinhoodMisc.h"
#include "entry_processor.h"
#include "entry_proc_tools.h"
#include "Memory.h"
#ifdef _HSM_LITE
#include "backend_ext.h"
#endif
#include <errno.h>
#include <time.h>
#ifdef HAVE_SHOOK
#include <shook_svr.h>
#include <fnmatch.h>
#endif
#include <unistd.h>

#define ERR_MISSING(_err) (((_err)==ENOENT)||((_err)==ESTALE))

/* forward declaration of EntryProc functions of pipeline */
static int  EntryProc_get_fid( struct entry_proc_op_t *, lmgr_t * );
static int  EntryProc_get_info_db( struct entry_proc_op_t *, lmgr_t * );
static int  EntryProc_get_info_fs( struct entry_proc_op_t *, lmgr_t * );
static int  EntryProc_reporting( struct entry_proc_op_t *, lmgr_t * );
static int  EntryProc_pre_apply(struct entry_proc_op_t *, lmgr_t *);
static int  EntryProc_db_apply(struct entry_proc_op_t *, lmgr_t *);
static int  EntryProc_db_batch_apply(struct entry_proc_op_t **, int, lmgr_t *);
#ifdef HAVE_CHANGELOGS
static int  EntryProc_chglog_clr( struct entry_proc_op_t *, lmgr_t * );
#endif
static int  EntryProc_rm_old_entries( struct entry_proc_op_t *, lmgr_t * );

/* forward declaration to check batchable operations for db_apply stage */
static int  dbop_is_batchable(struct entry_proc_op_t *, struct entry_proc_op_t *, int *);

/* pipeline stages */
enum {
    STAGE_GET_FID = 0,
    STAGE_GET_INFO_DB,
    STAGE_GET_INFO_FS,
    STAGE_REPORTING,
    STAGE_PRE_APPLY,
    STAGE_DB_APPLY,
#ifdef HAVE_CHANGELOGS
    STAGE_CHGLOG_CLR,
#endif
    STAGE_RM_OLD_ENTRIES,   /* special stage at the end of FS scan */

    PIPELINE_STAGE_COUNT    /* keep last */
};

const pipeline_descr_t std_pipeline_descr =
{
    .stage_count    = PIPELINE_STAGE_COUNT,
    .GET_ID         = STAGE_GET_FID,
    .GET_INFO_DB    = STAGE_GET_INFO_DB,
    .GET_INFO_FS    = STAGE_GET_INFO_FS,
    .GC_OLDENT      = STAGE_RM_OLD_ENTRIES,
    .DB_APPLY       = STAGE_DB_APPLY,
};

/** pipeline stages definition */
pipeline_stage_t std_pipeline[] = {
    {STAGE_GET_FID, "STAGE_GET_FID", EntryProc_get_fid, NULL, NULL,
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC, 0},
    {STAGE_GET_INFO_DB, "STAGE_GET_INFO_DB", EntryProc_get_info_db, NULL, NULL,
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC | STAGE_FLAG_ID_CONSTRAINT, 0},
    {STAGE_GET_INFO_FS, "STAGE_GET_INFO_FS", EntryProc_get_info_fs, NULL, NULL,
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC, 0},
    {STAGE_REPORTING, "STAGE_REPORTING", EntryProc_reporting, NULL, NULL,
     STAGE_FLAG_PARALLEL | STAGE_FLAG_ASYNC, 0},
    {STAGE_PRE_APPLY, "STAGE_PRE_APPLY", EntryProc_pre_apply, NULL, NULL,
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC, 0},
    {STAGE_DB_APPLY, "STAGE_DB_APPLY", EntryProc_db_apply,
        EntryProc_db_batch_apply, dbop_is_batchable, /* batched ops management */
#if defined( _SQLITE )
     /* SQLite locks the whole file for modifications...
     * So, 1 single threads is enough at this step.
     */
     STAGE_FLAG_MAX_THREADS | STAGE_FLAG_SYNC, 1},
#else
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC, 0},
#endif

#ifdef HAVE_CHANGELOGS
    /* only 1 thread here because commiting records must be sequential
     * (in the same order as changelog) */
    {STAGE_CHGLOG_CLR, "STAGE_CHGLOG_CLR", EntryProc_chglog_clr, NULL, NULL, /* XXX could be batched? */
     STAGE_FLAG_SEQUENTIAL | STAGE_FLAG_SYNC, 1},

     /* acknowledging records must be sequential,
      * in the order of record ids
      * @TODO change this depending on the mode the program is started.
      */
#endif
    /* this step is for mass update / mass remove operations when
     * starting/ending a FS scan. */
    {STAGE_RM_OLD_ENTRIES, "STAGE_RM_OLD_ENTRIES", EntryProc_rm_old_entries, NULL, NULL,
     STAGE_FLAG_SEQUENTIAL | STAGE_FLAG_SYNC, 0}
};


/**
 * For entries from FS scan, we must get the associated entry ID.
 */
int EntryProc_get_fid( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
#ifdef _HAVE_FID
    int            rc;
    entry_id_t     tmp_id;
    char buff[RBH_PATH_MAX];
    char * path;

    /* 2 possible options: get fid using parent_fid/name or from fullpath */
    if (ATTR_MASK_TEST(&p_op->fs_attrs, parent_id)
        && ATTR_MASK_TEST(&p_op->fs_attrs, name))
    {
        BuildFidPath( &ATTR(&p_op->fs_attrs, parent_id), buff );
        long len = strlen(buff);
        sprintf(buff+len, "/%s", ATTR(&p_op->fs_attrs, name));
        path = buff;
    }
    else if (ATTR_MASK_TEST( &p_op->fs_attrs, fullpath))
    {
        path = ATTR(&p_op->fs_attrs, fullpath);
    }
    else
    {
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                    "Error: not enough information to get fid: parent_id/name or fullpath needed");
        EntryProcessor_Acknowledge( p_op, -1, TRUE );
        return EINVAL;
    }

    /* perform path2fid */
    rc = Lustre_GetFidFromPath( path, &tmp_id );

    /* Workaround for Lustre 2.3: if parent is root, llapi_path2fid returns EINVAL (see LU-3245).
     * In this case, get fid from full path.
     */
    if ((rc == -EINVAL) && ATTR_MASK_TEST( &p_op->fs_attrs, fullpath))
    {
        path = ATTR(&p_op->fs_attrs, fullpath);
        rc =  Lustre_GetFidFromPath( path, &tmp_id );
    }

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
        EntryProcessor_SetEntryId( p_op, &tmp_id );

        /* go to GET_INFO_DB stage */
        rc = EntryProcessor_Acknowledge( p_op, STAGE_GET_INFO_DB, FALSE );
        if ( rc )
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                        "Error %d acknowledging stage STAGE_GET_FID.", rc );
    }
    return rc;
#else
    DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error: unexpected stage in a filesystem with no fid: STAGE_GET_FID.");
    EntryProcessor_Acknowledge( p_op, -1, TRUE );
    return EINVAL;
#endif
}

#ifdef HAVE_RM_POLICY
static inline int soft_remove_filter(struct entry_proc_op_t *p_op)
{
    if (ATTR_FSorDB_TEST( p_op, type )
        && !strcmp( ATTR_FSorDB( p_op, type ), STR_TYPE_DIR ))
    {
        DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Removing directory entry (no rm in backend)");
        return FALSE;
    }
#ifdef ATTR_INDEX_status
    else if (ATTR_FSorDB_TEST(p_op, status)
        && (ATTR_FSorDB(p_op, status) == STATUS_NEW))
    {
        DisplayLog( LVL_DEBUG, ENTRYPROC_TAG, "Removing 'new' entry ("DFID"): no remove in backend",
                    PFID(&p_op->entry_id) );
        return FALSE;
    }
#endif
#ifdef HAVE_SHOOK
    /* if the removed entry is a restripe source,
     * we MUST NOT remove the backend entry
     * as it will be linked to the restripe target
     */
    else if ( (ATTR_FSorDB_TEST(p_op, fullpath)
               && !fnmatch("*/"RESTRIPE_DIR"/"RESTRIPE_SRC_PREFIX"*",
                     ATTR_FSorDB(p_op, fullpath), 0))
        ||
        (ATTR_FSorDB_TEST(p_op, name)
         && !strncmp(RESTRIPE_SRC_PREFIX, ATTR_FSorDB(p_op, name ),
                     strlen(RESTRIPE_SRC_PREFIX))))
    {
        DisplayLog( LVL_DEBUG, ENTRYPROC_TAG, "Removing shook stripe source %s: no removal in backend!",
                    ATTR_FSorDB_TEST(p_op, fullpath)?
                    ATTR_FSorDB(p_op, fullpath) : ATTR_FSorDB(p_op, name));
        return FALSE;
    }
#endif
    return TRUE;
}
#else
    /* no soft remove for this mode */
#define soft_remove_filter(_op) (FALSE)
#endif


#ifdef HAVE_CHANGELOGS

/* does the CL record gives a clue about object type? */
static obj_type_t cl2type_clue(CL_REC_TYPE *logrec)
{
    switch(logrec->cr_type)
    {
        case CL_CREATE:
        case CL_CLOSE:
        case CL_TRUNC:
#ifdef HAVE_CL_LAYOUT
        case CL_LAYOUT:
#endif
        case CL_HSM:
            return TYPE_FILE;
        case CL_MKDIR:
        case CL_RMDIR:
            return TYPE_DIR;
        case CL_SOFTLINK:
            return TYPE_LINK;
        case CL_MKNOD:
            return TYPE_CHR; /* or other special type */
        default:
            return TYPE_NONE;
    }
}

/** displays a warning if parent/name is missing whereas it should not */
static inline void check_path_info(struct entry_proc_op_t *p_op, const char *recname)
{
    /* name and parent should have been provided by the CREATE changelog record */
    if (!ATTR_MASK_TEST(&p_op->fs_attrs, parent_id)
        || !ATTR_MASK_TEST(&p_op->fs_attrs, name))
    {
        DisplayLog(LVL_MAJOR, ENTRYPROC_TAG, "WARNING: name and parent should be set by %s record",
                   recname);
        p_op->fs_attr_need |= ATTR_MASK_name | ATTR_MASK_parent_id;
    }
}

/**
 * Infer information from the changelog record (status, ...).
 * \return next pipeline step to be perfomed.
 */
static int EntryProc_FillFromLogRec( struct entry_proc_op_t *p_op, int allow_md_updt )
{
    /* alias to the log record */
    CL_REC_TYPE *logrec = p_op->extra_info.log_record.p_log_rec;

    /* if this is a CREATE record, we know that its status is NEW. */
    if ( logrec->cr_type == CL_CREATE )
    {
        /* not a symlink */
        p_op->fs_attr_need &= ~ATTR_MASK_link;

        /* Sanity check: if the entry already exists in DB,
         * it could come from a previous filesystem that has been reformatted.
         * In this case, force a full update of the entry.
         */
        if ( p_op->db_exists )
        {
                DisplayLog( LVL_EVENT, ENTRYPROC_TAG,
                            "CREATE record on already existing entry "DFID"%s%s."
                            " This is normal if you scanned it previously.",
                            PFID(&p_op->entry_id),
                            ATTR_MASK_TEST(&p_op->db_attrs, fullpath)?  ", path=":
                                (ATTR_MASK_TEST(&p_op->db_attrs, name)?", name=":""),
                            ATTR_MASK_TEST(&p_op->db_attrs, fullpath)? ATTR(&p_op->db_attrs, fullpath):
                                (ATTR_MASK_TEST(&p_op->db_attrs, name)?ATTR(&p_op->db_attrs,name):""));

#ifdef ATTR_INDEX_creation_time
            /* set insertion time, like for a new entry */
            ATTR_MASK_SET( &p_op->fs_attrs, creation_time );
            ATTR( &p_op->fs_attrs, creation_time )
                = cltime2sec(logrec->cr_time);
#endif

            /* force updating attributes */
            p_op->fs_attr_need |= POSIX_ATTR_MASK | ATTR_MASK_stripe_info;
            /* name and parent should have been provided by the CREATE changelog record */
            check_path_info(p_op, "CREATE");
#ifdef ATTR_INDEX_status
            p_op->fs_attr_need |= ATTR_MASK_status;
#endif
        }
#ifdef ATTR_INDEX_status
        else /* new entry */
        {
            ATTR_MASK_SET( &p_op->fs_attrs, status );
            ATTR( &p_op->fs_attrs, status ) = STATUS_NEW;

            /* new file, status is known */
            p_op->fs_attr_need &= ~ATTR_MASK_status;

#ifdef ATTR_INDEX_no_archive
            /* no flag is set for now */
            ATTR_MASK_SET( &p_op->fs_attrs, no_archive );
            ATTR( &p_op->fs_attrs, no_archive ) = 0;
#endif
#ifdef ATTR_INDEX_no_release
            /* no flag is set for now */
            ATTR_MASK_SET( &p_op->fs_attrs, no_release );
            ATTR( &p_op->fs_attrs, no_release ) = 0;
#endif

            /* new entry: never archived or restored */
            ATTR( &p_op->fs_attrs, last_archive ) = 0;
            ATTR_MASK_SET( &p_op->fs_attrs, last_archive );
#ifdef HAVE_PURGE_POLICY
            ATTR( &p_op->fs_attrs, last_restore ) = 0;
            ATTR_MASK_SET( &p_op->fs_attrs, last_restore );
#endif
        }
#endif
    }
    else if ( logrec->cr_type == CL_HARDLINK ) {
        /* The entry exists but not the name. We only have to
         * create the name. */

        /* name and parent should have been provided by the HARDLINK changelog record */
        check_path_info(p_op, "HARDLINK");
    }
#ifdef HAVE_SHOOK
    /* shook specific: xattrs on file indicate its current status */
    else if (logrec->cr_type == CL_XATTR)
    {
        /* need to update status */
        p_op->fs_attr_need |= ATTR_MASK_status;
    }
#endif
    else if ((logrec->cr_type == CL_MKDIR )
            || (logrec->cr_type == CL_RMDIR ))
    {
        /* entry is a directory */
        ATTR_MASK_SET( &p_op->fs_attrs, type );
        strcpy( ATTR( &p_op->fs_attrs, type ), STR_TYPE_DIR );

        /* not a link */
        p_op->fs_attr_need &= ~ATTR_MASK_link;

        /* no stripe info for dirs */
        p_op->fs_attr_need &= ~ATTR_MASK_stripe_info;
        p_op->fs_attr_need &= ~ATTR_MASK_stripe_items;

#ifdef ATTR_INDEX_status
        p_op->fs_attr_need &= ~ATTR_MASK_status; /* no status for directories (XXX all modes?) */
#endif
        /* path info should be set */
        check_path_info(p_op, changelog_type2str(logrec->cr_type));
    }
    else if (logrec->cr_type == CL_SOFTLINK)
    {
        /* entry is a symlink */
        ATTR_MASK_SET( &p_op->fs_attrs, type );
        strcpy( ATTR( &p_op->fs_attrs, type ), STR_TYPE_LINK );

        /* need to get symlink content */
        p_op->fs_attr_need |= ATTR_MASK_link;

        /* no stripe info for symlinks */
        p_op->fs_attr_need &= ~ATTR_MASK_stripe_info;
        p_op->fs_attr_need &= ~ATTR_MASK_stripe_items;
    }
#ifdef _LUSTRE_HSM
    else if ( logrec->cr_type == CL_HSM )
    {
        /* not a link */
        p_op->fs_attr_need &= ~ATTR_MASK_link;

        switch ( hsm_get_cl_event( logrec->cr_flags ) )
        {
            case HE_ARCHIVE:
               /* is it a successfull copy? */
               if ( hsm_get_cl_error( logrec->cr_flags ) == CLF_HSM_SUCCESS )
               {
                   /* remember last archive time */
                   ATTR_MASK_SET( &p_op->fs_attrs, last_archive );
                   ATTR( &p_op->fs_attrs, last_archive )
                        = cltime2sec( logrec->cr_time );

                   /* if dirty flag is set, the entry is dirty,
                    * else, it is up to date. */
                   ATTR_MASK_SET( &p_op->fs_attrs, status );
                   ATTR( &p_op->fs_attrs, status ) =
                        (hsm_get_cl_flags(logrec->cr_flags) & CLF_HSM_DIRTY) ?
                        STATUS_MODIFIED : STATUS_SYNCHRO;
                   p_op->fs_attr_need &= ~ATTR_MASK_status;
               }
               else /* archive failed */
               {
                   /* Entry is probably still dirty. If dirty flag is not set,
                    * we need to ask the actual status */
                   if ( hsm_get_cl_flags(logrec->cr_flags) & CLF_HSM_DIRTY )
                   {
                        ATTR_MASK_SET( &p_op->fs_attrs, status );
                        ATTR( &p_op->fs_attrs, status ) = STATUS_MODIFIED;
                        p_op->fs_attr_need &= ~ATTR_MASK_status;
                   }
                   else
                        p_op->fs_attr_need |= ATTR_MASK_status;
               }
               break;

            case HE_RESTORE:
                if ( hsm_get_cl_error( logrec->cr_flags ) == CLF_HSM_SUCCESS )
                {
                    /* remember last restore time */
                    ATTR_MASK_SET( &p_op->fs_attrs, last_restore );
                    ATTR( &p_op->fs_attrs, last_restore )
                        = cltime2sec( logrec->cr_time );

                    /* status is 'up-to-date' after a successful restore */
                    ATTR_MASK_SET( &p_op->fs_attrs, status );
                    ATTR( &p_op->fs_attrs, status ) = STATUS_SYNCHRO;
                    p_op->fs_attr_need &= ~ATTR_MASK_status;
                }
                else if ( p_op->db_exists )
                {
                    /* Entry status remains 'released' */
#ifdef _DROP_RELEASED
                    /* remove entry from PE working set */
                    p_op->db_op_type = OP_TYPE_REMOVE_LAST;
                    return STAGE_PRE_APPLY;
#else
                    ATTR_MASK_SET(&p_op->fs_attrs, status);
                    ATTR(&p_op->fs_attrs, status) = STATUS_RELEASED;
                    p_op->fs_attr_need &= ~ATTR_MASK_status;
#endif
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
                    /* release records are not expected to be erroneous */
                    DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "ERROR: "
                         "Unexpected HSM release event with error %d",
                         hsm_get_cl_error(logrec->cr_flags) );
                    /* make sure of actual entry status */
                    p_op->fs_attr_need |= ATTR_MASK_status;
                }
                else if ( p_op->db_exists )
                {
#ifdef _DROP_RELEASED
                    /* Entry is now 'released': remove it from PE working set */
                    p_op->db_op_type = OP_TYPE_REMOVE_LAST;
                    return STAGE_PRE_APPLY;
#else
                    ATTR_MASK_SET(&p_op->fs_attrs, status);
                    ATTR(&p_op->fs_attrs, status) = STATUS_RELEASED;
                    p_op->fs_attr_need &= ~ATTR_MASK_status;
#endif
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
                    ATTR_MASK_SET( &p_op->fs_attrs, status );
                    ATTR( &p_op->fs_attrs, status ) = STATUS_MODIFIED;
                    p_op->fs_attr_need &= ~ATTR_MASK_status;
                }
                else /* other status change: need to get it */
                    p_op->fs_attr_need |= ATTR_MASK_status;
                break;

            case HE_REMOVE:
            case HE_CANCEL:
                /* undetermined status after such an event */
                p_op->fs_attr_need |= ATTR_MASK_status;
                break;
            default:
                DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "ERROR: unknown HSM event:"
                            "bitfield=%#x, event=%u", logrec->cr_flags,
                            hsm_get_cl_event( logrec->cr_flags ) );
                /* skip */
                return -1;
        }
    }
#endif
    else if (logrec->cr_type == CL_UNLINK)
    {
        /* name and parent should have been provided by the UNLINK changelog record */
        check_path_info(p_op, "UNLINK");
    }
    else if ( logrec->cr_type == CL_MTIME || logrec->cr_type == CL_TRUNC ||
              (logrec->cr_type == CL_CLOSE))
    {
#ifdef ATTR_INDEX_status
        /* if file is modified or truncated, need to check its status
         * (probably modified) EXCEPT if its status is already 'modified' */
        if ( !ATTR_MASK_TEST( &p_op->db_attrs, status )
             || ((ATTR(&p_op->db_attrs, status) != STATUS_MODIFIED) &&
                 (ATTR(&p_op->db_attrs, status) != STATUS_NEW)) )
        {
            DisplayLog( LVL_DEBUG, ENTRYPROC_TAG,
                        "Getstatus needed because this is a MTIME, TRUNC or CLOSE event "
                        "and status is not already 'modified' or 'new': event=%s, status=%d",
                        changelog_type2str(logrec->cr_type),
                        ATTR_MASK_TEST( &p_op->db_attrs, status )?
                            ATTR(&p_op->db_attrs, status):-1 );
            p_op->fs_attr_need |= ATTR_MASK_status;
        }
#endif
    }
    else if (logrec->cr_type == CL_CTIME || (logrec->cr_type == CL_SETATTR))
    {
        /* need to update attrs */
        p_op->fs_attr_need |= POSIX_ATTR_MASK;
#ifdef ATTR_INDEX_status
#ifdef HAVE_SHOOK
        /* in Lustre v2.O, changing trusted xattr generates CTIME/SATTR event */
        p_op->fs_attr_need |= ATTR_MASK_status;

        DisplayLog( LVL_DEBUG, ENTRYPROC_TAG,
                    "getstatus and getattr needed because this is a CTIME or SATTR event" );
#else
        DisplayLog( LVL_DEBUG, ENTRYPROC_TAG,
                    "getattr needed because this is a CTIME or SATTR event" );
#endif
#endif
    }
#ifdef HAVE_CL_LAYOUT
    else if (logrec->cr_type == CL_LAYOUT)
    {
        p_op->fs_attr_need |= ATTR_MASK_stripe_info;
        p_op->fs_attr_need |= ATTR_MASK_stripe_items;
    }
#endif

    /* if the entry is already in DB, try to determine if something changed */
    if ( p_op->db_exists )
    {
#ifdef HAVE_SHOOK
        /* if the old name is a restripe file, update the status */
        if (!strncmp(RESTRIPE_TGT_PREFIX, ATTR( &p_op->db_attrs, name ),
                     strlen(RESTRIPE_TGT_PREFIX)))
        {
            p_op->fs_attr_need |= ATTR_MASK_status;
            DisplayLog( LVL_DEBUG, ENTRYPROC_TAG,
                        "Getstatus needed because entry was a restripe target");
        }
#endif

        if (logrec->cr_type == CL_EXT)
        {
            /* in case of a rename, the path info must be set */
            check_path_info(p_op, "RENAME");
        }

        /* get the new attributes, in case of a SATTR, HSM... */
        if ( allow_md_updt && (   ( logrec->cr_type == CL_MTIME )
                               || ( logrec->cr_type == CL_CTIME )
                               || ( logrec->cr_type == CL_CLOSE )
                               || ( logrec->cr_type == CL_TRUNC )
                               || ( logrec->cr_type == CL_HSM )
                               || ( logrec->cr_type == CL_SETATTR )) )
        {
            DisplayLog( LVL_DEBUG, ENTRYPROC_TAG,
                        "Getattr needed because this is a TIME, TRUNC, SETATTR, HSM or CLOSE event, and "
                         "metadata has not been recently updated. event=%s",
                         changelog_type2str(logrec->cr_type) );

            p_op->fs_attr_need |= POSIX_ATTR_MASK;
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
    CL_REC_TYPE * logrec = p_op->extra_info.log_record.p_log_rec;

    /* allow event-driven update */
    int md_allow_event_updt = TRUE;

    if ( logrec->cr_type == CL_UNLINK )
    {
        DisplayLog( LVL_DEBUG, ENTRYPROC_TAG,
#ifdef _LUSTRE_HSM
                   "UNLINK on %s entry "DFID": last=%s, archived=%s",
                    p_op->db_exists?"known":"unknown", PFID(&p_op->entry_id),
                    bool2str( logrec->cr_flags & CLF_UNLINK_LAST ),
                    bool2str( logrec->cr_flags & CLF_UNLINK_HSM_EXISTS ) );
#else
                    "UNLINK on %s entry "DFID": last=%s",
                    p_op->db_exists?"known":"unknown", PFID(&p_op->entry_id),
                    bool2str( logrec->cr_flags & CLF_UNLINK_LAST ) );
#endif

    #ifndef HAVE_RM_POLICY
        if (p_op->check_if_last_entry) {
            /* When inserting that entry, we didn't know whether the
             * entry was the last one or not, so use the nlink
             * attribute we requested earlier to determine. */
            if (ATTR_MASK_TEST(&p_op->db_attrs, nlink) && (ATTR(&p_op->db_attrs, nlink) <= 1))
            {
                DisplayLog(LVL_DEBUG, ENTRYPROC_TAG, "UNLINK record for entry with nlink=%u in DB => removing it",
                           ATTR(&p_op->db_attrs, nlink));
                logrec->cr_flags |= CLF_UNLINK_LAST;
            }
        }
    #else
            /* too dangerous */
    #endif

        /* is it the last reference to this file? */
        if ( logrec->cr_flags & CLF_UNLINK_LAST )
        {
#ifdef HAVE_RM_POLICY
            if ( !policies.unlink_policy.hsm_remove )
            {
#endif
                /* hsm_remove is disabled or file doesn't exist in the backend:
                 * If the file was in DB: remove it, else skip the record. */
                if ( p_op->db_exists )
                {
                    p_op->db_op_type = OP_TYPE_REMOVE_LAST;
                    return STAGE_PRE_APPLY;
                }
                else
                    /* ignore the record */
                    return STAGE_CHGLOG_CLR;

#ifdef HAVE_RM_POLICY
            }
            else /* hsm removal enabled: must check if there is some cleaning
                  * to be done in the backend */
            {
#ifdef _LUSTRE_HSM
                if (logrec->cr_flags & CLF_UNLINK_HSM_EXISTS)
                    /* if CLF_UNLINK_HSM_EXISTS is set, we must clean something in the backend */
                    p_op->db_op_type = OP_TYPE_SOFT_REMOVE;
                else if (p_op->db_exists)
                    /* nothing in the backend, just clean the entry in DB */
                    p_op->db_op_type = OP_TYPE_REMOVE_LAST;
                else
                    /* ignore the record */
                    return STAGE_CHGLOG_CLR;
#else
                /* If the entry exists in DB, this moves it from main table
                 * to a remove queue, else, ignore the record. */
                if (!p_op->db_exists)
                    /* ignore the record */
                    return STAGE_CHGLOG_CLR;
                else if (soft_remove_filter(p_op))
                    p_op->db_op_type = OP_TYPE_SOFT_REMOVE;
                else
                    p_op->db_op_type = OP_TYPE_REMOVE_LAST;
#endif
                return STAGE_PRE_APPLY;
            }
#endif
        }
        else if ( p_op->db_exists ) /* entry still exists and is known in DB */
        {
            /* Remove the name only. Keep the inode information since
             * there is more file names refering to it. */
            p_op->db_op_type = OP_TYPE_REMOVE_ONE;
            return STAGE_PRE_APPLY;
        }
        else {
            /* UNLINK with unknown file in database -> ignore the
             * record. This case can happen on systems without LU-543
             * when we insert a fake UNLINK (with an unknow FID at the
             * time), but an application has already issued an UNLINK
             * before the rename operation. */
            return STAGE_CHGLOG_CLR;
        }

    } /* end if UNLINK */
    else if ( logrec->cr_type == CL_RENAME)
    {
        /* this is a source name event */
        /* remove only the old name */
        /* TODO: could be OP_TYPE_REMOVE_ONE or OP_TYPE_REMOVE_LAST. */
        p_op->db_op_type = OP_TYPE_REMOVE_ONE;
        return STAGE_PRE_APPLY;
    }
    else if ( logrec->cr_type == CL_RMDIR )
    {
        if (p_op->db_exists)
        {
            p_op->db_op_type = OP_TYPE_REMOVE_LAST;
            return STAGE_PRE_APPLY;
        }
        else
        {
            /* ignore the record */
            return STAGE_CHGLOG_CLR;
        }
    } /* end if RMDIR */

    if ( !p_op->db_exists )
    {
        DisplayLog(LVL_FULL, ENTRYPROC_TAG, DFID"not in DB: INSERT", PFID(&p_op->entry_id));

        /* non-unlink (or non-destructive unlink) record on unknown entry:
         * insert entry to the DB */
        p_op->db_op_type = OP_TYPE_INSERT;

#ifdef ATTR_INDEX_creation_time
        /* new entry, set insertion time */
        ATTR_MASK_SET( &p_op->fs_attrs, creation_time );
        ATTR( &p_op->fs_attrs, creation_time ) = cltime2sec(logrec->cr_time);
#endif

        /* we must get info that is not provided by the chglog */
        p_op->fs_attr_need |= (POSIX_ATTR_MASK | ATTR_MASK_name | ATTR_MASK_parent_id
                               | ATTR_MASK_stripe_info | ATTR_MASK_stripe_items
                               | ATTR_MASK_link) & ~ p_op->fs_attrs.attr_mask;

        /* if we needed fullpath (e.g. for policies), set it */
        if ((p_op->db_attr_need & ATTR_MASK_fullpath) &&
            !ATTR_MASK_TEST(&p_op->fs_attrs, fullpath))
            p_op->fs_attr_need |= ATTR_MASK_fullpath;
#ifdef ATTR_INDEX_status
        /* By default, get status for a new record.
         * This is overwritten by EntryProc_FillFromLogRec if the status
         * can be infered from changelog rec. */
        p_op->fs_attr_need |= ATTR_MASK_status;
#endif
    }
    else /* non-unlink record on known entry */
    {
        DisplayLog(LVL_FULL, ENTRYPROC_TAG, DFID": UPDATE", PFID(&p_op->entry_id));

        p_op->db_op_type = OP_TYPE_UPDATE;

        /* check what information must be updated.
         * missing info = DB query - retrieved */
        int db_missing = p_op->db_attr_need & ~p_op->db_attrs.attr_mask;

        /* get attrs if some is missing */
        if ((db_missing & POSIX_ATTR_MASK) &&
            (p_op->fs_attrs.attr_mask & POSIX_ATTR_MASK) != POSIX_ATTR_MASK)
            p_op->fs_attr_need |= POSIX_ATTR_MASK;

        /* get stripe info if missing (file only) */
        if ((db_missing & ATTR_MASK_stripe_info) && !ATTR_MASK_TEST(&p_op->fs_attrs, stripe_info)
            && (!ATTR_FSorDB_TEST(p_op, type) || !strcmp(ATTR_FSorDB(p_op, type), STR_TYPE_FILE)))
        {
            p_op->fs_attr_need |= ATTR_MASK_stripe_info | ATTR_MASK_stripe_items;
        }

        /* get link content if missing (symlink only) */
        if ((db_missing & ATTR_MASK_link) && !ATTR_MASK_TEST(&p_op->fs_attrs, link)
            && (!ATTR_FSorDB_TEST(p_op, type) || !strcmp(ATTR_FSorDB(p_op, type), STR_TYPE_LINK)))
                p_op->fs_attr_need |= ATTR_MASK_link;

#ifdef ATTR_INDEX_status
        if ((db_missing & ATTR_MASK_status) && !ATTR_MASK_TEST(&p_op->fs_attrs, status))
            p_op->fs_attr_need |= ATTR_MASK_status;
#endif

        /* Check md_update policy */
        if (need_md_update(&p_op->db_attrs, &md_allow_event_updt))
            p_op->fs_attr_need |= POSIX_ATTR_MASK;

        /* check if path update is needed (only if it was not just updated) */
        if ((!ATTR_MASK_TEST(&p_op->fs_attrs, parent_id) || !ATTR_MASK_TEST(&p_op->fs_attrs, name))
            && (need_path_update(&p_op->db_attrs, NULL)
                || (db_missing & (ATTR_MASK_fullpath | ATTR_MASK_name | ATTR_MASK_parent_id))))
            p_op->fs_attr_need |= ATTR_MASK_name
                                  | ATTR_MASK_parent_id;
    }

    /* infer info from changelog record, then continue to next step */
    return EntryProc_FillFromLogRec( p_op, md_allow_event_updt );
}
#endif /* CHANGELOG support */

/* Ensure the fullpath from DB is consistent.
 * Set updt_mask according to the missing info.
 */
static void check_fullpath(attr_set_t *attrs, const entry_id_t *id, int *updt_mask)
{
#ifdef _HAVE_FID
    /* If the parent id from the changelog refers to a directory
     * that no longer exists, the path built from the DB may be partial.
     * If the current entry is the direct child of such a directory,
     * we must update the parent information.
     * Else, we should do it for every parent up to the unknown dir.
     */
    if (ATTR_MASK_TEST(attrs, fullpath) && ATTR(attrs, fullpath)[0] != '/')
    {
        char parent[RBH_NAME_MAX];
        char *next = strchr(ATTR(attrs, fullpath), '/');
        if (next != NULL)
        {
            entry_id_t parent_id;

            memset(parent,0, sizeof(parent));
            strncpy(parent, ATTR(attrs, fullpath), (ptrdiff_t)(next-ATTR(attrs, fullpath)));

            if (sscanf(parent, SFID, RFID(&parent_id)) != FID_SCAN_CNT) /* fid consists of 3 numbers */
            {
                DisplayLog(LVL_MAJOR, ENTRYPROC_TAG, "Entry "DFID" has an inconsistent relative path: %s",
                           PFID(id), ATTR(attrs, fullpath));
                /* fullpath is not consistent (should be <id>/name) */
                ATTR_MASK_UNSET(attrs, fullpath);
                /* update all path information */
                *updt_mask |= ATTR_MASK_parent_id | ATTR_MASK_name | ATTR_MASK_fullpath;
            }
            /* check if the entry is the direct child of the unknown directory */
            else if (strchr(next+1, '/') == NULL)
            {
                DisplayLog(LVL_EVENT, ENTRYPROC_TAG, "Parent dir for entry "DFID
                           " is unknown (parent: "DFID", child name: '%s'): updating entry path info",
                           PFID(id), PFID(&parent_id), next+1);
                ATTR_MASK_UNSET(attrs, fullpath);
                *updt_mask |= ATTR_MASK_parent_id | ATTR_MASK_name | ATTR_MASK_fullpath;
            }
            /* else: FIXME: We should update parent info for an upper entry. */
            else
            {
                ATTR_MASK_UNSET(attrs, fullpath);
                /* update path info anyhow, to try fixing the issue */
                *updt_mask |= ATTR_MASK_parent_id | ATTR_MASK_name | ATTR_MASK_fullpath;
            }
        }
        else
        {
            /* fullpath is not consistent (should be <pid>/name) */
            ATTR_MASK_UNSET(attrs, fullpath);
            /* update path info, to try fixing the issue */
            *updt_mask |= ATTR_MASK_parent_id | ATTR_MASK_name | ATTR_MASK_fullpath;
        }
    }
#else
    if (ATTR_MASK_TEST(attrs, fullpath)
        && ATTR(attrs, fullpath)[0] != '/')
    {
        /* fullpath is not pertinent */
        ATTR_MASK_UNSET(attrs, fullpath);
        /* update path info, to try fixing the issue */
        *updt_mask |= ATTR_MASK_parent_id | ATTR_MASK_name | ATTR_MASK_fullpath;
    }
#endif
}

/**
 * check if the entry exists in the database and what info
 * must be retrieved.
 */
int EntryProc_get_info_db( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc = 0;
    int            next_stage = -1; /* -1 = skip */
    unsigned int attr_allow_cached = 0;
#ifdef _HSM_LITE
    int rc_status_need = 0;
    unsigned int attr_need_fresh = 0;
#endif

    const pipeline_stage_t *stage_info =
        &entry_proc_pipeline[p_op->pipeline_stage];

    /* always ignore root */
    if (p_op->entry_id_is_set &&
        entry_id_equal(&p_op->entry_id, get_root_id()))
    {
        DisplayLog(LVL_DEBUG, ENTRYPROC_TAG, "Ignoring record for root directory");
        /* drop the entry */
        next_stage = -1;
        goto next_step;
    }

#ifdef HAVE_CHANGELOGS
    /* is this a changelog record? */
    if ( p_op->extra_info.is_changelog_record )
    {
        obj_type_t type_clue = TYPE_NONE;

        CL_REC_TYPE *logrec = p_op->extra_info.log_record.p_log_rec;

        /* does the log record gives a clue about entry_type */
        type_clue = cl2type_clue(logrec);

        p_op->db_attr_need = 0;

        if (type_clue == TYPE_NONE)
            /* type is a useful information to take decisions (about getstripe, readlink, ...) */
            p_op->db_attr_need |= ATTR_MASK_type;

        /* add diff mask for diff mode */
        p_op->db_attr_need |= entry_proc_conf.diff_mask;

#ifdef _HSM_LITE
        /* needed attributes to check special objects */
        p_op->db_attr_need |= rbhext_ignore_need();
#endif

        if (!chglog_reader_config.mds_has_lu543 &&
            logrec->cr_type == CL_UNLINK &&
            p_op->get_fid_from_db) {
            /* It is possible this unlink was inserted by the changelog
             * reader. Some Lustre server don't give the FID, so retrieve
             * it now from the NAMES table, given the parent FID and the
             * filename. */
            p_op->get_fid_from_db = 0;

            rc = ListMgr_Get_FID_from_Path(lmgr, &logrec->cr_pfid,
                                           rh_get_cl_cr_name(logrec),
                                           &p_op->entry_id);
            if (!rc) {
                if (!fid_is_sane(&logrec->cr_pfid))
                    DisplayLog(LVL_MAJOR, ENTRYPROC_TAG,
                               "Error: insane parent fid "DFID" from DB",
                               PFID(&logrec->cr_pfid));
                /* The FID is now set, so we can register it with the
                 * constraint engine. Since this operation is at the
                 * very top of the queue, we register it at the head
                 * of the constraint list, not at the tail. */
                p_op->entry_id_is_set = TRUE;
                id_constraint_register( p_op, TRUE );
            }

            /* Unblock the pipeline stage. */
            EntryProcessor_Unblock( STAGE_GET_INFO_DB );

            if (rc) {
                /* Not found. Skip the entry */
                DisplayLog( LVL_FULL, ENTRYPROC_TAG,
                            "Warning: parent/filename for UNLINK not found" );
                next_stage = -1;
                goto next_step;
            }

        }

        /* If this is an unlink and we don't know whether it is the
         * last entry, use nlink. */
        if (logrec->cr_type == CL_UNLINK && p_op->check_if_last_entry)
            p_op->db_attr_need |= ATTR_MASK_nlink;

        /* If it's a hard link, we will need the hlink so we can
         * increment it. Will override the fs value. */
        if ( logrec->cr_type == CL_HARDLINK ) {
            p_op->db_attr_need |= ATTR_MASK_nlink;
        }

        /* Only need to get md_update if the update policy != always */
        if (policies.updt_policy.md.policy != UPDT_ALWAYS)
            p_op->db_attr_need |= ATTR_MASK_md_update;

        /* Only need to get path_update if the update policy != always
         * and if it is not provided in logrec
         */
        if ((policies.updt_policy.path.policy != UPDT_ALWAYS)
            && (logrec->cr_namelen == 0))
            p_op->db_attr_need |= ATTR_MASK_path_update;

#if 0 /* XXX not to be done systematically: only on specific events? (CL_LAYOUT...) */
#ifdef _LUSTRE
        if ((type_clue == TYPE_NONE || type_clue == TYPE_FILE)
            && logrec->cr_type != CL_CREATE)
            /* to check if stripe_info is set for this entry (useless for CREATE)*/
            p_op->db_attr_need |= ATTR_MASK_stripe_info;
#endif
#endif
#ifdef ATTR_INDEX_status
        p_op->db_attr_need |= ATTR_MASK_status;
#endif
#ifdef ATTR_INDEX_creation_time
        if (entry_proc_conf.detect_fake_mtime)
             p_op->db_attr_need |= ATTR_MASK_creation_time;
#endif
        if (type_clue == TYPE_NONE || type_clue == TYPE_LINK)
            /* check if link content is set for this entry */
            p_op->db_attr_need |= ATTR_MASK_link;

        if ( entry_proc_conf.match_file_classes )
        {
            /* get fileclass update info to know if we must check it */
#ifdef HAVE_MIGR_POLICY
            if (policies.updt_policy.fileclass.policy != UPDT_ALWAYS)
                p_op->db_attr_need |= ATTR_MASK_arch_cl_update;
            p_op->db_attr_need |= ATTR_MASK_archive_class;
            p_op->db_attr_need |= policies.migr_policies.global_attr_mask;
#endif
#ifdef HAVE_PURGE_POLICY
            if (policies.updt_policy.fileclass.policy != UPDT_ALWAYS)
                p_op->db_attr_need |= ATTR_MASK_rel_cl_update;
            p_op->db_attr_need |= ATTR_MASK_release_class;
            p_op->db_attr_need |= policies.purge_policies.global_attr_mask;
#endif
        }
#ifdef HAVE_RMDIR_POLICY
        else if (entry_proc_conf.match_dir_classes)
        {
             if (policies.updt_policy.fileclass.policy != UPDT_ALWAYS)
                 p_op->db_attr_need |= ATTR_MASK_rel_cl_update;
            p_op->db_attr_need |= ATTR_MASK_release_class;
            p_op->db_attr_need |= policies.rmdir_policy.global_attr_mask;
        }
#endif
        p_op->db_attr_need |= entry_proc_conf.alert_attr_mask;

#ifdef _HSM_LITE
        /* what info is needed to check backend status? */
        rc = rbhext_status_needs( type_clue,
                                  &attr_allow_cached,
                                  &attr_need_fresh );
        if ( rc != 0 )
        {
            if ( rc == -ENOTSUP )
            {
                /* this type can't be backup'ed: skip the record */
                p_op->db_attr_need &= ~ATTR_MASK_status;
                p_op->fs_attr_need &= ~ATTR_MASK_status;
                p_op->extra_info.not_supp = TRUE;
            }
            else
                DisplayLog( LVL_MAJOR, ENTRYPROC_TAG,
                            "rbhext_status_needs() returned error %d", rc );
        }
        else
        {
            /* query needed (cached) info from the DB */
            p_op->db_attr_need |= attr_allow_cached ;
        }

        /* in case of unlink, we need the backend path */
        if ( p_op->extra_info.log_record.p_log_rec->cr_type == CL_UNLINK )
            p_op->db_attr_need |= ATTR_MASK_backendpath;
#endif

        /* In case of a RENAME, match the new name (not the one from the DB). */
        if ((logrec->cr_type == CL_EXT)
             && (p_op->db_attr_need & ATTR_MASK_fullpath))
        {
            rc = Lustre_GetFullPath(&p_op->entry_id,
                                    ATTR(&p_op->fs_attrs, fullpath),
                                    sizeof(ATTR(&p_op->fs_attrs, fullpath)));
            if (rc == 0)
            {
                ATTR_MASK_SET(&p_op->fs_attrs, fullpath);
                p_op->db_attr_need &= ~ATTR_MASK_fullpath;
            }
        }

        /* attributes to be retrieved */
        p_op->db_attrs.attr_mask = p_op->db_attr_need;
        rc = ListMgr_Get( lmgr, &p_op->entry_id, &p_op->db_attrs );

        if (rc == DB_SUCCESS )
        {
            p_op->db_exists = TRUE;
            /* attr mask has been set by ListMgr_Get */
        }
        else if (rc == DB_NOT_EXISTS )
        {
            p_op->db_exists = FALSE;
            /* no attrs from DB */
            ATTR_MASK_INIT( &p_op->db_attrs );
        }
        else
        {
            /* ERROR */
            DisplayLog(LVL_CRIT, ENTRYPROC_TAG,
                       "Error %d retrieving entry "DFID" from DB: %s.", rc,
                       PFID(&p_op->entry_id), lmgr_err2str(rc));
            p_op->db_exists = FALSE;
            /* no attrs from DB */
            ATTR_MASK_INIT( &p_op->db_attrs );
        }

        /* Retrieve info from the log record, and decide what info must be
         * retrieved from filesystem. */
        next_stage = EntryProc_ProcessLogRec( p_op );

#ifdef _HSM_LITE
        /* this check must be done after processing log record,
         * because it determined if status is needed */
        if (p_op->fs_attr_need & ATTR_MASK_status)
            p_op->fs_attr_need |= attr_need_fresh;
#endif

        DisplayLog( LVL_DEBUG, ENTRYPROC_TAG, "RECORD: %s "DFID" %#x %s => "
                    "getstripe=%u, getattr=%u, getpath=%u, readlink=%u"
#ifdef ATTR_INDEX_status
                    ", getstatus=%u"
#endif
                    , changelog_type2str(logrec->cr_type), PFID(&p_op->entry_id),
                    logrec->cr_flags & CLF_FLAGMASK,
                    logrec->cr_namelen ? rh_get_cl_cr_name(logrec) : "<null>",
                    NEED_GETSTRIPE(p_op)?1:0, NEED_GETATTR(p_op)?1:0,
                    NEED_GETPATH(p_op)?1:0, NEED_READLINK(p_op)?1:0
#ifdef ATTR_INDEX_status
                    , NEED_GETSTATUS(p_op)?1:0
#endif
                    );
    }
    else /* entry from FS scan */
    {
#endif
        /* scan is expected to provide full path and attributes. */
        if (!ATTR_MASK_TEST( &p_op->fs_attrs, fullpath ))
        {
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                        "Error: missing info from FS scan" );
            /* skip the entry */
            next_stage = -1;
            goto next_step;
        }

#ifdef _HSM_LITE
        /* what info is needed to check it?*/
        rc_status_need = rbhext_status_needs( ListMgr2PolicyType(ATTR(&p_op->fs_attrs, type)),
                                              &attr_allow_cached,
                                              &attr_need_fresh );
        if ( rc == -ENOTSUP )
        {
                /* this type can't be backup'ed: no status check */
                p_op->db_attr_need &= ~ATTR_MASK_status;
                p_op->fs_attr_need &= ~ATTR_MASK_status;
                p_op->extra_info.not_supp = TRUE;
        }
        else if (rc != 0)
        {
            DisplayLog( LVL_MAJOR, ENTRYPROC_TAG,
                        "rbhext_status_needs() returned error %d",
                        rc_status_need );
            attr_allow_cached = attr_need_fresh = 0;
        }
#endif

        p_op->db_attr_need |= entry_proc_conf.diff_mask;
        /* retrieve missing attributes for diff */
        p_op->fs_attr_need |= (entry_proc_conf.diff_mask & ~p_op->fs_attrs.attr_mask);

#ifdef ATTR_INDEX_creation_time
        if (entry_proc_conf.detect_fake_mtime)
             p_op->db_attr_need |= ATTR_MASK_creation_time;
#endif

        /* what must be retrieved from DB: */
        p_op->db_attr_need |= (attr_allow_cached
                               & ~p_op->fs_attrs.attr_mask);
        p_op->db_attr_need |= (entry_proc_conf.alert_attr_mask
                               & ~p_op->fs_attrs.attr_mask);

        /* no dircount for non-dirs */
        if (ATTR_MASK_TEST(&p_op->fs_attrs, type) &&
            strcmp(ATTR(&p_op->fs_attrs, type), STR_TYPE_DIR))
        {
            p_op->db_attr_need &= ~ATTR_MASK_dircount;
        }

        /* no readlink for non symlinks */
        if (ATTR_MASK_TEST(&p_op->fs_attrs, type)) /* likely */
        {
            if (!strcmp(ATTR(&p_op->fs_attrs, type), STR_TYPE_LINK))
            {
                p_op->db_attr_need |= ATTR_MASK_link; /* check if it is known */
                /* no stripe for symlinks */
                p_op->db_attr_need &= ~ (ATTR_MASK_stripe_info|ATTR_MASK_stripe_items);
            }
            else
                p_op->db_attr_need &= ~ATTR_MASK_link;
        }

        if ( entry_proc_conf.match_file_classes &&
             ATTR_MASK_TEST(&p_op->fs_attrs, type) &&
#ifdef _LUSTRE_HSM
             !strcmp( ATTR(&p_op->fs_attrs, type), STR_TYPE_FILE )) /* file */
#else
             strcmp( ATTR(&p_op->fs_attrs, type), STR_TYPE_DIR ) != 0 ) /* non-dir */
#endif
        {
#ifdef HAVE_MIGR_POLICY
            if (policies.updt_policy.fileclass.policy != UPDT_ALWAYS)
                p_op->db_attr_need |= ATTR_MASK_arch_cl_update;

            p_op->db_attr_need |= ATTR_MASK_archive_class;
            p_op->db_attr_need |= (policies.migr_policies.global_attr_mask
                                  & ~p_op->fs_attrs.attr_mask);
#endif

#ifdef HAVE_PURGE_POLICY
            if (policies.updt_policy.fileclass.policy != UPDT_ALWAYS)
                p_op->db_attr_need |= ATTR_MASK_rel_cl_update;

            p_op->db_attr_need |= ATTR_MASK_release_class;
            p_op->db_attr_need |= (policies.purge_policies.global_attr_mask
                                  & ~p_op->fs_attrs.attr_mask);
#endif
        }
#ifdef HAVE_RMDIR_POLICY
        else if ( entry_proc_conf.match_dir_classes &&
             ATTR_MASK_TEST(&p_op->fs_attrs, type) &&
             !strcmp( ATTR(&p_op->fs_attrs, type), STR_TYPE_DIR )) /* dir */
        {
            if (policies.updt_policy.fileclass.policy != UPDT_ALWAYS)
                p_op->db_attr_need |= ATTR_MASK_rel_cl_update;
            p_op->db_attr_need |= ATTR_MASK_release_class;
            p_op->db_attr_need |= (policies.rmdir_policy.global_attr_mask
                                  & ~p_op->fs_attrs.attr_mask);
        }
#endif

        if (p_op->db_attr_need)
        {
            p_op->db_attrs.attr_mask = p_op->db_attr_need;
            rc = ListMgr_Get(lmgr, &p_op->entry_id, &p_op->db_attrs);

            if (rc == DB_SUCCESS)
            {
                p_op->db_exists = TRUE;
            }
            else if (rc == DB_NOT_EXISTS )
            {
                p_op->db_exists = FALSE;
                ATTR_MASK_INIT( &p_op->db_attrs );
            }
            else
            {
                /* ERROR */
                DisplayLog(LVL_CRIT, ENTRYPROC_TAG,
                           "Error %d retrieving entry "DFID" from DB: %s.", rc,
                           PFID(&p_op->entry_id), lmgr_err2str(rc));
                p_op->db_exists = FALSE;
                ATTR_MASK_INIT( &p_op->db_attrs );
            }
        }
        else
        {
            p_op->db_exists = ListMgr_Exists( lmgr, &p_op->entry_id );
        }

        if ( !p_op->db_exists )
        {
            /* new entry */
            p_op->db_op_type = OP_TYPE_INSERT;

#ifdef ATTR_INDEX_creation_time
            /* set creation time if it was not set by scan module */
            if (!ATTR_MASK_TEST(&p_op->fs_attrs, creation_time))
            {
                ATTR_MASK_SET( &p_op->fs_attrs, creation_time );
                ATTR( &p_op->fs_attrs, creation_time ) = time(NULL); /* FIXME min(atime,mtime,ctime)? */
            }
#endif

#ifdef _LUSTRE
            /* get stripe for files */
            if (ATTR_MASK_TEST(&p_op->fs_attrs, type)
                && !strcmp(ATTR( &p_op->fs_attrs, type ), STR_TYPE_FILE )
                /* only if it was not retrieved during the scan */
                && !(ATTR_MASK_TEST(&p_op->fs_attrs, stripe_info)
                    && ATTR_MASK_TEST(&p_op->fs_attrs, stripe_items)))
                p_op->fs_attr_need |= ATTR_MASK_stripe_info | ATTR_MASK_stripe_items;
#endif

            /* readlink for symlinks (if not already known) */
            if (ATTR_MASK_TEST(&p_op->fs_attrs, type)
                && !strcmp(ATTR( &p_op->fs_attrs, type ), STR_TYPE_LINK)
                && !ATTR_MASK_TEST(&p_op->fs_attrs, link))
            {
                p_op->fs_attr_need |= ATTR_MASK_link;
            }
            else
            {
                p_op->fs_attr_need &= ~ATTR_MASK_link;
            }

            /* get status for archivable entries (file or non-dir, depending on the flavor) */
#ifdef ATTR_INDEX_status
            if (ATTR_MASK_TEST(&p_op->fs_attrs, type)
#ifdef _LUSTRE_HSM
                && !strcmp( ATTR(&p_op->fs_attrs, type), STR_TYPE_FILE ))
#elif defined (_HSM_LITE)
                && (strcmp( ATTR(&p_op->fs_attrs, type), STR_TYPE_DIR ) != 0)
                && !p_op->extra_info.not_supp)
#endif
            {
                p_op->fs_attr_need |= ATTR_MASK_status;
#ifdef _HSM_LITE
                p_op->fs_attr_need |= (attr_need_fresh & ~p_op->fs_attrs.attr_mask);
#endif
            }
            else
            {
                p_op->extra_info.not_supp = TRUE;
                p_op->fs_attr_need &= ~ATTR_MASK_status;
            }
#endif
            next_stage = STAGE_GET_INFO_FS;
        }
        else
        {
            p_op->db_op_type = OP_TYPE_UPDATE;

#ifdef ATTR_INDEX_status
            if (ATTR_MASK_TEST(&p_op->fs_attrs, type)
#ifdef _LUSTRE_HSM
                && !strcmp( ATTR(&p_op->fs_attrs, type), STR_TYPE_FILE ))
#elif defined (_HSM_LITE)
                && (strcmp( ATTR(&p_op->fs_attrs, type), STR_TYPE_DIR ) != 0)
                && !p_op->extra_info.not_supp)
#endif
            {
                p_op->fs_attr_need |= ATTR_MASK_status;
#ifdef _HSM_LITE
                p_op->fs_attr_need |= (attr_need_fresh & ~p_op->fs_attrs.attr_mask);
#endif
            }
            else
            {
                p_op->extra_info.not_supp = TRUE;
                p_op->fs_attr_need &= ~ATTR_MASK_status;
            }
#endif

            if (ATTR_MASK_TEST(&p_op->fs_attrs, type)) /* likely set */
            {
                if (strcmp(ATTR( &p_op->fs_attrs, type ), STR_TYPE_LINK))
                    /* non-link */
                    p_op->fs_attr_need &= ~ATTR_MASK_link;
                else
                {
                    /* link */
#ifdef _LUSTRE
                    /* already known (in DB or FS) */
                    if (ATTR_FSorDB_TEST(p_op, link))
                        p_op->fs_attr_need &= ~ATTR_MASK_link;
                    else /* not known */
                        p_op->fs_attr_need |= ATTR_MASK_link;
#else
                    /* For non-lustre filesystems, inodes may be recycled, so re-read link even if it is is DB */
                    if (ATTR_MASK_TEST(&p_op->fs_attrs, link))
                        p_op->fs_attr_need &= ~ATTR_MASK_link;
                    else
                        p_op->fs_attr_need |= ATTR_MASK_link;
#endif
                }
            }

            /* get parent_id+name, if not set during scan (eg. for root directory) */
            if (!ATTR_MASK_TEST( &p_op->fs_attrs, name))
                p_op->fs_attr_need |= ATTR_MASK_name;
            if (!ATTR_MASK_TEST( &p_op->fs_attrs, parent_id))
                p_op->fs_attr_need |= ATTR_MASK_parent_id;

#ifdef _LUSTRE
            /* check stripe only for files */
            if (ATTR_MASK_TEST(&p_op->fs_attrs, type)
                 && !strcmp(ATTR( &p_op->fs_attrs, type ), STR_TYPE_FILE)
                 && !strcmp( global_config.fs_type, "lustre"))
            {
                check_stripe_info(p_op, lmgr);
            }
#endif
            next_stage = STAGE_GET_INFO_FS;
        }

#ifdef HAVE_CHANGELOGS
    } /* end if entry from FS scan */
#endif

    check_fullpath(&p_op->db_attrs, &p_op->entry_id, &p_op->fs_attr_need);

    #ifdef _BENCH_DB
        /* don't get info from filesystem */
        next_stage = STAGE_PRE_APPLY;
    #endif

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
#ifdef _HAVE_FID
    char path[RBH_PATH_MAX];
    BuildFidPath( &p_op->entry_id, path );
#else
    char * path;
    if (ATTR_FSorDB_TEST( p_op, fullpath ))
    {
        path = ATTR_FSorDB( p_op, fullpath );
    }
    else
    {
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                    "Entry path is needed for retrieving file info" );
        return EINVAL;
    }
#endif

    DisplayLog(LVL_FULL, ENTRYPROC_TAG,
        DFID": Getattr=%u, Getpath=%u, Readlink=%u"
#ifdef ATTR_INDEX_status
        ", GetStatus=%u"
#endif
        ", Getstripe=%u",
         PFID(&p_op->entry_id), NEED_GETATTR(p_op)?1:0,
         NEED_GETPATH(p_op)?1:0, NEED_READLINK(p_op)?1:0,
#ifdef ATTR_INDEX_status
        NEED_GETSTATUS(p_op)?1:0,
#endif
        NEED_GETSTRIPE(p_op)?1:0);

    /* don't retrieve info which is already fresh */
    p_op->fs_attr_need &= ~p_op->fs_attrs.attr_mask;

#ifdef HAVE_CHANGELOGS /* never needed for scans */
    if (NEED_GETATTR(p_op) && (p_op->extra_info.is_changelog_record))
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
                DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Entry %s no longer exists", path );
                /* schedule rm in the backend, if enabled */
                if ((!p_op->db_exists)
#ifdef HAVE_RM_POLICY
                    && (!policies.unlink_policy.hsm_remove)
#endif
                )
                    goto skip_record;
                else /* else, remove it from db */
                    goto rm_record;
            }
            else
                DisplayLog( LVL_DEBUG, ENTRYPROC_TAG, "lstat() failed on %s: %s", path,
                            strerror( rc ) );
            /* If lstat returns an error, drop the log record */
            goto skip_record;
        }
        else if (entry_md.st_nlink == 0)
        {
            /* remove pending */
            DisplayLog(LVL_DEBUG, ENTRYPROC_TAG, "Entry %s has nlink=0: remove pending", path);
            /* schedule rm in the backend, if enabled */
            if ((!p_op->db_exists)
#ifdef HAVE_RM_POLICY
                && (!policies.unlink_policy.hsm_remove)
#endif
            )
                goto skip_record;
            else /* else, remove it from db */
                goto rm_record;
        }

        /* convert them to internal structure */
#if defined( _LUSTRE ) && defined( _HAVE_FID ) && defined( _MDS_STAT_SUPPORT )
        PosixStat2EntryAttr( &entry_md, &p_op->fs_attrs, !global_config.direct_mds_stat );
#else
        PosixStat2EntryAttr( &entry_md, &p_op->fs_attrs, TRUE );
#endif
        ATTR_MASK_SET( &p_op->fs_attrs, md_update );
        ATTR( &p_op->fs_attrs, md_update ) = time( NULL );

    } /* getattr needed */

    if (NEED_GETPATH(p_op))
        path_check_update(&p_op->entry_id, path, &p_op->fs_attrs, p_op->fs_attr_need);
#endif

#ifdef ATTR_INDEX_creation_time
    if (entry_proc_conf.detect_fake_mtime
        && ATTR_FSorDB_TEST(p_op, creation_time)
        && ATTR_MASK_TEST(&p_op->fs_attrs, last_mod))
    {
        check_and_warn_fake_mtime(p_op);
    }
#endif

#ifdef _LUSTRE
    /* getstripe only for files */
    if ( NEED_GETSTRIPE(p_op)
         && ATTR_FSorDB_TEST( p_op, type )
         && strcmp( ATTR_FSorDB( p_op, type), STR_TYPE_FILE ) != 0 )
        p_op->fs_attr_need &= ~(ATTR_MASK_stripe_info | ATTR_MASK_stripe_items);

    if (NEED_GETSTRIPE(p_op))
    {
        /* get entry stripe */
        rc = File_GetStripeByPath( path,
                                   &ATTR( &p_op->fs_attrs, stripe_info ),
                                   &ATTR( &p_op->fs_attrs, stripe_items ) );
        if (rc)
        {
            ATTR_MASK_UNSET( &p_op->fs_attrs, stripe_info );
            ATTR_MASK_UNSET( &p_op->fs_attrs, stripe_items );
        }
        else
        {
            ATTR_MASK_SET( &p_op->fs_attrs, stripe_info );
            ATTR_MASK_SET( &p_op->fs_attrs, stripe_items );
        }
    } /* get_stripe needed */
#endif

#ifdef _LUSTRE_HSM
    /* Lustre-HSM: get status only for files */
    /* (type may have been determined at this point) */
    if ( NEED_GETSTATUS(p_op)
         && ATTR_FSorDB_TEST( p_op, type )
         && strcmp( ATTR_FSorDB( p_op, type ), STR_TYPE_FILE ) != 0 )
    {
        p_op->fs_attr_need &= ~ATTR_MASK_status;
        p_op->extra_info.not_supp = TRUE;
    }
#endif

#ifdef ATTR_INDEX_status
    if (NEED_GETSTATUS(p_op))
    {
#ifdef _HSM_LITE
        attr_set_t merged_attrs; /* attrs from FS+DB */
        attr_set_t new_attrs; /* attrs from backend */

        ATTR_MASK_INIT(&merged_attrs);
        ATTR_MASK_INIT(&new_attrs);

        ListMgr_MergeAttrSets(&merged_attrs, &p_op->fs_attrs, 1);
        ListMgr_MergeAttrSets(&merged_attrs, &p_op->db_attrs, 0);

        /* get entry status */
        rc = rbhext_get_status( &p_op->entry_id, &merged_attrs, &new_attrs );
#elif defined(_LUSTRE_HSM)
        rc = LustreHSM_GetStatus( path, &ATTR( &p_op->fs_attrs, status ),
                                  &ATTR( &p_op->fs_attrs, no_release ),
                                  &ATTR( &p_op->fs_attrs, no_archive ) );
#endif
        if ( ERR_MISSING( abs( rc )) )
        {
            DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Entry %s no longer exists",
                        path );
            /* changelog: an UNLINK event will be raised, so we ignore current record
             * scan: entry will be garbage collected at the end of the scan */
            goto skip_record;
        }
        else if ( rc == 0 )
        {
#ifdef _HSM_LITE
            /* merge/update attributes */
            ListMgr_MergeAttrSets( &p_op->fs_attrs, &new_attrs, TRUE );
#elif defined(_LUSTRE_HSM)
            ATTR_MASK_SET( &p_op->fs_attrs, status );
            ATTR_MASK_SET( &p_op->fs_attrs, no_release );
            ATTR_MASK_SET( &p_op->fs_attrs, no_archive );
#endif

            /* if the entry has no flags, the entry has never been archived or restored */
            if ( ATTR( &p_op->fs_attrs, status ) == STATUS_NEW )
            {
                ATTR_MASK_SET( &p_op->fs_attrs, last_archive );
                ATTR( &p_op->fs_attrs, last_archive ) = 0;
#ifdef HAVE_PURGE_POLICY
                ATTR_MASK_SET( &p_op->fs_attrs, last_restore );
                ATTR( &p_op->fs_attrs, last_restore ) = 0;
#endif
            }

            /* NOTICE: keep released entries in the DB for stats purpose,
             * unless _DROP_RELEASED is defined */
#ifdef _DROP_RELEASED
            /* if the file is released, it goes outside PolicyEngine working set */
            if ( ATTR( &p_op->fs_attrs, status ) == STATUS_RELEASED )
            {
                DisplayLog(LVL_DEBUG, ENTRYPROC_TAG,
                    "Entry "DFID" has status 'RELEASED': removing it from PolicyEngine working set (if it was in DB)",
                    PFID( &p_op->entry_id ) );
                if ( p_op->db_exists )
                {
                    /* don't go to rm_record, as we don't want
                     * to remove the entry from backend! */
                    p_op->db_op_type = OP_TYPE_REMOVE;
                }
                else
                {
                    /* nothing to be removed in DB */
                    goto skip_record;
                }
            }
#endif
        }
        else if ( rc == -ENOTSUP )
        {
            /* this type of entry is not managed: ignored */
            p_op->extra_info.not_supp = TRUE;
            /* no status */
            ATTR_MASK_UNSET(&p_op->fs_attrs, status);
        }

    } /* get_status needed */
#endif

    /* readlink only for symlinks */
    if ( NEED_READLINK(p_op) && ATTR_FSorDB_TEST( p_op, type )
         && strcmp( ATTR_FSorDB( p_op, type), STR_TYPE_LINK ) != 0 )
        p_op->fs_attr_need &= ~ATTR_MASK_link;

    if (NEED_READLINK(p_op))
    {
        ssize_t len = readlink(path, ATTR(&p_op->fs_attrs, link), RBH_PATH_MAX);
        if (len >= 0)
        {
            ATTR_MASK_SET(&p_op->fs_attrs, link);

            /* add final '\0' on success */
            if (len >= RBH_PATH_MAX)
                ATTR(&p_op->fs_attrs, link)[len-1] = '\0';
            else
                ATTR(&p_op->fs_attrs, link)[len] = '\0';
        }
        else
            DisplayLog(LVL_MAJOR, ENTRYPROC_TAG, "readlink failed on %s: %s", path, strerror(errno));
    }

    /* check if the entry must be ignored */
    #ifdef _HSM_LITE
    {
        attr_set_t merged_attrs; /* attrs from FS+DB */
        ATTR_MASK_INIT(&merged_attrs);

        ListMgr_MergeAttrSets(&merged_attrs, &p_op->fs_attrs, 1);
        ListMgr_MergeAttrSets(&merged_attrs, &p_op->db_attrs, 0);

        /* ignored entries will always go there, as they are considered as new */
        /* use the same merged attributes to check the ignore condition */
        if (rbhext_ignore(&p_op->entry_id, &merged_attrs))
        {
            DisplayLog(LVL_DEBUG, ENTRYPROC_TAG,
                       "Special file or dir '%s' skipped",
                       (ATTR_FSorDB_TEST(p_op, fullpath )?
                        ATTR_FSorDB(p_op, fullpath):
                        ATTR_FSorDB(p_op, name)));
            goto skip_record;
        }
    }
    #endif

    /* match fileclasses if specified in config */

    /* sure it's a directory */
    if (ATTR_FSorDB_TEST( p_op, type) && !strcmp( ATTR_FSorDB( p_op, type ), STR_TYPE_DIR ))
    {
        if (entry_proc_conf.match_dir_classes)
            check_policies(&p_op->entry_id, &p_op->fs_attrs, &p_op->db_attrs, TRUE);
    }
    /* sure it is a supported type and not a dir */
    else if (!p_op->extra_info.not_supp)
    {
        if (entry_proc_conf.match_file_classes)
            check_policies(&p_op->entry_id, &p_op->fs_attrs, &p_op->db_attrs, TRUE);
    }
    /* non-dir or not supported (no check) */

    /* set other info */
    rc = EntryProcessor_Acknowledge( p_op, STAGE_REPORTING, FALSE );
    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage.", rc );
    return rc;

#ifdef HAVE_CHANGELOGS
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
#endif

#ifdef HAVE_CHANGELOGS
rm_record:
    /* soft remove the entry, except if it was 'new' (not in backend)
     * or not in DB.
     */
    if (!soft_remove_filter(p_op))
        /* Lustre 2 with changelog: we are here because lstat (by fid)
         * on the entry failed, which ensure the entry no longer
         * exists. Skip it. The entry will be removed by a subsequent
         * UNLINK record.
         *
         * On other posix filesystems, the entry disappeared between
         * its scanning and its processing... skip it so it will be
         * cleaned at the end of the scan. */
        goto skip_record;
    else if ( p_op->db_exists )
        p_op->db_op_type = OP_TYPE_SOFT_REMOVE;
    else
        /* drop the record */
        goto skip_record;

    rc = EntryProcessor_Acknowledge(p_op, STAGE_PRE_APPLY, FALSE);
    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage.", rc );
    return rc;
#endif
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

    /* tmp copy to be merged */
    attr_set_t  merged_attrs = p_op->fs_attrs;
    ListMgr_MergeAttrSets(&merged_attrs, &p_op->db_attrs, FALSE);

    /* generate missing fields */
    ListMgr_GenerateFields(&merged_attrs, entry_proc_conf.alert_attr_mask);

    /* check alert criterias (synchronously) */
    for ( i = 0; i < entry_proc_conf.alert_count; i++ )
    {
        /* check entry attr mask (else, skip it) */
        if (!merged_attrs.attr_mask || !p_op->entry_id_is_set
             || ( (merged_attrs.attr_mask & entry_proc_conf.alert_list[i].attr_mask ) !=
                  entry_proc_conf.alert_list[i].attr_mask ) )
            continue;

        if ( EntryMatches( &p_op->entry_id, &merged_attrs,
               &entry_proc_conf.alert_list[i].boolexpr, NULL ) == POLICY_MATCH )
        {
            /* build alert string and break */
            if ( ATTR_MASK_TEST( &merged_attrs, fullpath ) )
                snprintf( strid, RBH_PATH_MAX, "%s", ATTR(&merged_attrs, fullpath ) );
            else
                snprintf( strid, RBH_PATH_MAX, DFID, PFID(&p_op->entry_id));

            rc = BoolExpr2str( &entry_proc_conf.alert_list[i].boolexpr, stralert, 2*RBH_PATH_MAX );
            if ( rc < 0 )
                strcpy( stralert, "Error building alert string" );

            PrintAttrs( strvalues, 2*RBH_PATH_MAX, &merged_attrs,
                        entry_proc_conf.alert_list[i].attr_mask, 0 );

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
        rc = EntryProcessor_Acknowledge(p_op, STAGE_PRE_APPLY, FALSE);
        if ( rc )
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error acknowledging stage %s",
                        stage_info->stage_name );
    }

    if ( is_alert )
        RaiseEntryAlert(title, stralert, strid, strvalues );

    /* acknowledge now if the stage was synchronous */
    if ( !( stage_info->stage_flags & STAGE_FLAG_ASYNC ) )
    {
        rc = EntryProcessor_Acknowledge(p_op, STAGE_PRE_APPLY, FALSE);
        if ( rc )
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error acknowledging stage %s",
                        stage_info->stage_name );
    }

    return 0;
}

static int  dbop_is_batchable(struct entry_proc_op_t *first,
                              struct entry_proc_op_t *next,
                              int *full_attr_mask)
{
    if (first->db_op_type != OP_TYPE_INSERT
        && first->db_op_type != OP_TYPE_UPDATE
        && first->db_op_type != OP_TYPE_NONE)
        return FALSE;
    else if (first->db_op_type != next->db_op_type)
        return FALSE;
    /* starting from here, db_op_type is the same for the 2 operations */
    /* all NOOP operations can be batched */
    else if (first->db_op_type == OP_TYPE_NONE)
        return TRUE;
    /* different masks can be mixed, as long as attributes for each table are
     * the same or 0. Ask the list manager about that. */
    else if (lmgr_batch_compat(*full_attr_mask, next->fs_attrs.attr_mask))
    {
        *full_attr_mask |= next->fs_attrs.attr_mask;
        return TRUE;
    }
    else
        return FALSE;
}

/** operation cleaning before the db_apply step */
int EntryProc_pre_apply(struct entry_proc_op_t *p_op, lmgr_t * lmgr)
{
    int            rc;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];

#ifdef ATTR_INDEX_creation_time
    /* once set, never change creation time */
    if (p_op->db_op_type != OP_TYPE_INSERT)
        ATTR_MASK_UNSET(&p_op->fs_attrs, creation_time);
#endif

#ifdef HAVE_CHANGELOGS
    /* handle nlink. We don't want the values from the filesystem if
     * we're not doing a scan. */
    if (p_op->extra_info.is_changelog_record) {
        CL_REC_TYPE *logrec = p_op->extra_info.log_record.p_log_rec;

        if (logrec->cr_type == CL_CREATE) {
            /* New file. Hardlink is always 1. */
            ATTR_MASK_SET(&p_op->fs_attrs, nlink);
            ATTR(&p_op->fs_attrs, nlink) = 1;
        }
#ifdef ATTR_INDEX_nlink
        else if ((logrec->cr_type == CL_HARDLINK) &&
                 (ATTR_MASK_TEST(&p_op->db_attrs, nlink))) {
            /* New hardlink. Add 1 to existing value. Ignore what came
             * from the FS, since it can be out of sync by now. */
            ATTR_MASK_SET(&p_op->fs_attrs, nlink);
            ATTR(&p_op->fs_attrs, nlink) = ATTR(&p_op->db_attrs, nlink) + 1;
        }
#endif
    }
#endif

    /* Only update fields that changed */
    if (p_op->db_op_type == OP_TYPE_UPDATE)
    {
        int diff_mask = ListMgr_WhatDiff(&p_op->fs_attrs, &p_op->db_attrs);

        /* In scan mode, always keep md_update and path_update,
         * to avoid their cleaning at the end of the scan.
         * Also keep name and parent as they are keys in DNAMES table.
         */
        int to_keep = ATTR_MASK_parent_id | ATTR_MASK_name;

        /* the mask to be displayed > diff_mask (include to_keep flags) */
        int display_mask = entry_proc_conf.diff_mask & diff_mask;

        /* keep fullpath if parent or name changed (friendly display) */
        if (diff_mask & (ATTR_MASK_parent_id | ATTR_MASK_name)) {
            to_keep |= ATTR_MASK_fullpath;
            display_mask |= ATTR_MASK_fullpath;
        }
#ifdef HAVE_CHANGELOGS
        if (!p_op->extra_info.is_changelog_record)
#endif
            to_keep |= (ATTR_MASK_md_update | ATTR_MASK_path_update);

        /* remove other unchanged attrs + attrs not in db mask */
        p_op->fs_attrs.attr_mask &= (diff_mask | to_keep | ~p_op->db_attrs.attr_mask);

        /* FIXME: free cleared attributes */

        /* SQL req optimizations:
         * if update policy == always and fileclass is not changed,
         * don't set update timestamp.
         */
#ifdef HAVE_PURGE_POLICY
        if ((policies.updt_policy.fileclass.policy == UPDT_ALWAYS)
            && !ATTR_MASK_TEST( &p_op->fs_attrs, release_class))
            ATTR_MASK_UNSET(&p_op->fs_attrs, rel_cl_update);
#endif

#ifdef HAVE_MIGR_POLICY
        if ((policies.updt_policy.fileclass.policy == UPDT_ALWAYS)
            && !ATTR_MASK_TEST( &p_op->fs_attrs, archive_class))
            ATTR_MASK_UNSET(&p_op->fs_attrs, arch_cl_update);
#endif

        /* nothing changed => noop */
        if (p_op->fs_attrs.attr_mask == 0)
        {
            /* no op */
            p_op->db_op_type = OP_TYPE_NONE;
        }
        /* something changed in diffmask */
        else if (diff_mask & entry_proc_conf.diff_mask)
        {
            char attrchg[RBH_PATH_MAX] = "";

            /* attr from DB */
            if (display_mask == 0)
                attrchg[0] = '\0';
            else
                PrintAttrs(attrchg, RBH_PATH_MAX, &p_op->db_attrs, display_mask, 1);

            printf("-"DFID" %s\n", PFID(&p_op->entry_id), attrchg);

            /* attr from FS */
            PrintAttrs(attrchg, RBH_PATH_MAX, &p_op->fs_attrs, display_mask, 1);
            printf("+"DFID" %s\n", PFID(&p_op->entry_id), attrchg);
        }
    }
    else if (entry_proc_conf.diff_mask)
    {
        if (p_op->db_op_type == OP_TYPE_INSERT)
        {
            char attrnew[RBH_PATH_MAX];
            PrintAttrs(attrnew, RBH_PATH_MAX, &p_op->fs_attrs,
                p_op->fs_attrs.attr_mask & entry_proc_conf.diff_mask, 1);
            printf("++"DFID" %s\n", PFID(&p_op->entry_id), attrnew);
        }
        else if ((p_op->db_op_type == OP_TYPE_REMOVE_LAST) || (p_op->db_op_type == OP_TYPE_REMOVE_ONE) || (p_op->db_op_type == OP_TYPE_SOFT_REMOVE))
        {
            if (ATTR_FSorDB_TEST(p_op, fullpath))
                printf("--"DFID" path=%s\n", PFID(&p_op->entry_id), ATTR_FSorDB(p_op, fullpath));
            else
                printf("--"DFID"\n", PFID(&p_op->entry_id));
        }
    }

    p_op->fs_attrs.attr_mask &= ~readonly_attr_set;

    rc = EntryProcessor_Acknowledge(p_op, STAGE_DB_APPLY, FALSE);
    if (rc)
        DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Error acknowledging stage %s",
                   stage_info->stage_name);
    return rc;

#ifdef HAVE_SHOOK
skip_record:
#ifdef HAVE_CHANGELOGS
    if (p_op->extra_info.is_changelog_record)
    /* do nothing on DB but ack the record */
        rc = EntryProcessor_Acknowledge(p_op, STAGE_CHGLOG_CLR, FALSE);
    else
#endif
    /* remove the operation from pipeline */
    rc = EntryProcessor_Acknowledge(p_op, -1, TRUE);

    if (rc)
        DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage.", rc);
    return rc;
#endif
}

/**
 * Perform a single operation on the database.
 */
int EntryProc_db_apply(struct entry_proc_op_t *p_op, lmgr_t * lmgr)
{
    int            rc;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];

    /* insert to DB */
    switch ( p_op->db_op_type )
    {
    case OP_TYPE_NONE:
        /* noop */
        DisplayLog( LVL_FULL, ENTRYPROC_TAG, "NoOp("DFID")", PFID(&p_op->entry_id) );
        rc = 0;
        break;
    case OP_TYPE_INSERT:
        DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Insert("DFID")", PFID(&p_op->entry_id) );
        rc = ListMgr_Insert( lmgr, &p_op->entry_id, &p_op->fs_attrs, FALSE );
        break;
    case OP_TYPE_UPDATE:
        DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Update("DFID")", PFID(&p_op->entry_id) );
        rc = ListMgr_Update( lmgr, &p_op->entry_id, &p_op->fs_attrs );
        break;
    case OP_TYPE_REMOVE_ONE:
        DisplayLog( LVL_FULL, ENTRYPROC_TAG, "RemoveOne("DFID")", PFID(&p_op->entry_id) );
        rc = ListMgr_Remove( lmgr, &p_op->entry_id, &p_op->fs_attrs, FALSE );
        break;
    case OP_TYPE_REMOVE_LAST:
        DisplayLog( LVL_FULL, ENTRYPROC_TAG, "RemoveLast("DFID")", PFID(&p_op->entry_id) );
        rc = ListMgr_Remove( lmgr, &p_op->entry_id, &p_op->fs_attrs, TRUE );
        break;
#ifdef HAVE_RM_POLICY
    case OP_TYPE_SOFT_REMOVE:

        if (log_config.debug_level >= LVL_DEBUG) {
            char buff[2*RBH_PATH_MAX];
            PrintAttrs(buff, 2*RBH_PATH_MAX, &p_op->fs_attrs,
                       ATTR_MASK_fullpath | ATTR_MASK_parent_id | ATTR_MASK_name
#ifdef _HSM_LITE
                       | ATTR_MASK_backendpath
#endif
                    , 1);
            DisplayLog( LVL_DEBUG, ENTRYPROC_TAG, "SoftRemove("DFID",%s)",
                        PFID(&p_op->entry_id), buff);
        }

        rc = ListMgr_SoftRemove( lmgr, &p_op->entry_id, &p_op->fs_attrs,
                                 time(NULL) + policies.unlink_policy.deferred_remove_delay );
        break;
#endif
    default:
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Unhandled DB operation type: %d", p_op->db_op_type );
        rc = -1;
    }

    if ( rc )
        DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Error %d performing database operation: %s.",
                   rc, lmgr_err2str(rc));

    /* Acknowledge the operation if there is a callback */
#ifdef HAVE_CHANGELOGS
    if ( p_op->callback_func )
        rc = EntryProcessor_Acknowledge( p_op, STAGE_CHGLOG_CLR, FALSE );
    else
#endif
        rc = EntryProcessor_Acknowledge( p_op, -1, TRUE );

    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage %s.", rc,
                    stage_info->stage_name );

    return rc;
}

/**
 * Perform a batch of operations on the database.
 */
int EntryProc_db_batch_apply(struct entry_proc_op_t **ops, int count,
                             lmgr_t *lmgr)
{
    int            i, rc = 0;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[ops[0]->pipeline_stage];
    entry_id_t **ids = NULL;
    attr_set_t **attrs = NULL;

    /* allocate arrays of ids and attrs */
    ids = MemCalloc(count, sizeof(*ids));
    if (!ids)
        return -ENOMEM;
    attrs = MemCalloc(count, sizeof(*attrs));
    if (!attrs)
    {
        rc = -ENOMEM;
        goto free_ids;
    }
    for (i = 0; i < count; i++)
    {
        ids[i] = &ops[i]->entry_id;
        attrs[i] = &ops[i]->fs_attrs;
    }

    /* insert to DB */
    switch (ops[0]->db_op_type)
    {
    case OP_TYPE_NONE:
        /* noop */
        DisplayLog(LVL_FULL, ENTRYPROC_TAG, "NoOp(%u ops: "DFID"...)", count,
                   PFID(ids[0]));
        rc = 0;
        break;
    case OP_TYPE_INSERT:
        DisplayLog(LVL_FULL, ENTRYPROC_TAG, "BatchInsert(%u ops: "DFID"...)",
                   count, PFID(ids[0]));
        rc = ListMgr_BatchInsert(lmgr, ids, attrs, count, FALSE);
        break;
    case OP_TYPE_UPDATE:
        DisplayLog(LVL_FULL, ENTRYPROC_TAG, "BatchUpdate(%u ops: "DFID"...)",
                   count, PFID(ids[0]));
        rc = ListMgr_BatchInsert(lmgr, ids, attrs, count, TRUE);
        break;
    default:
        DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Unexpected operation for batch op: %d",
                   ops[0]->db_op_type);
        rc = -1;
    }

    if (rc)
        DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Error %d performing batch database operation: %s.",
                   rc, lmgr_err2str(rc));

    /* Acknowledge the operation if there is a callback */
#ifdef HAVE_CHANGELOGS
    if (ops[0]->callback_func)
        rc = EntryProcessor_AcknowledgeBatch(ops, count, STAGE_CHGLOG_CLR, FALSE);
    else
#endif
        rc = EntryProcessor_AcknowledgeBatch(ops, count, -1, TRUE);

    if (rc)
        DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage %s.", rc,
                   stage_info->stage_name);

    MemFree(attrs);
free_ids:
    MemFree(ids);
    return rc;
}


#ifdef HAVE_CHANGELOGS
int            EntryProc_chglog_clr( struct entry_proc_op_t * p_op, lmgr_t * lmgr )
{
    int            rc;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];
    CL_REC_TYPE * logrec = p_op->extra_info.log_record.p_log_rec;

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
#endif

static void mass_rm_cb(const entry_id_t *p_id)
{
    printf("--"DFID"\n", PFID(p_id));
}

int EntryProc_rm_old_entries( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];
    lmgr_filter_t  filter;
    filter_value_t val;
    rm_cb_func_t cb = NULL;

    /* callback func for diff display */
    if (entry_proc_conf.diff_mask)
        cb = mass_rm_cb;

    /* If gc_entries or gc_names are not set,
     * this is just a special op to wait for pipeline flush.
     * => don't clean old entries */
    if (p_op->gc_entries || p_op->gc_names)
    {
        lmgr_simple_filter_init( &filter );

        if (p_op->gc_entries)
        {
            /* remove entries from all tables that have not been seen during the scan */
            val.value.val_uint = ATTR( &p_op->fs_attrs, md_update );
            lmgr_simple_filter_add( &filter, ATTR_INDEX_md_update, LESSTHAN_STRICT, val, 0 );
        }

        if (p_op->gc_names)
        {
            /* use the same timestamp for cleaning paths that have not been seen during the scan */
            val.value.val_uint = ATTR( &p_op->fs_attrs, md_update );
            lmgr_simple_filter_add( &filter, ATTR_INDEX_path_update, LESSTHAN_STRICT, val, 0 );
        }

        /* partial scan: remove non-updated entries from a subset of the namespace */
        if (ATTR_MASK_TEST( &p_op->fs_attrs, fullpath ))
        {
            char tmp[RBH_PATH_MAX];
            strcpy(tmp, ATTR(&p_op->fs_attrs, fullpath));
            strcat(tmp, "/*");
            val.value.val_str = tmp;
            lmgr_simple_filter_add(&filter, ATTR_INDEX_fullpath, LIKE, val, 0);
        }

        /* force commit after this operation */
        ListMgr_ForceCommitFlag( lmgr, TRUE );

        /* remove entries listed in previous scans */
    #ifdef HAVE_RM_POLICY
        if (policies.unlink_policy.hsm_remove)
            /* @TODO fix for dirs */
            rc = ListMgr_MassSoftRemove(lmgr, &filter,
                     time(NULL) + policies.unlink_policy.deferred_remove_delay, cb);
        else
    #endif
            rc = ListMgr_MassRemove(lmgr, &filter, cb);

        lmgr_simple_filter_free( &filter );

        if ( rc )
            DisplayLog(LVL_CRIT, ENTRYPROC_TAG,
                       "Error: ListMgr MassRemove operation failed with code %d: %s",
                       rc,lmgr_err2str(rc));
    }

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
