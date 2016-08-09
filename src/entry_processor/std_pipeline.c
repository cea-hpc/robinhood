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

#include "rbh_cfg.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "entry_processor.h"
#include "entry_proc_tools.h"
#include "Memory.h"
#include "policy_rules.h"
#include "update_params.h"
#include "status_manager.h"
#include <errno.h>
#include <time.h>
#include <unistd.h>

#define ERR_MISSING(_err) (((_err)==ENOENT)||((_err)==ESTALE))

#define diff_mask *((attr_mask_t*)entry_proc_arg)

/* forward declaration of EntryProc functions of pipeline */
static int  EntryProc_get_fid( struct entry_proc_op_t *, lmgr_t * );
static int  EntryProc_get_info_db( struct entry_proc_op_t *, lmgr_t * );
static int  EntryProc_get_info_fs( struct entry_proc_op_t *, lmgr_t * );
static int  EntryProc_pre_apply(struct entry_proc_op_t *, lmgr_t *);
static int  EntryProc_db_apply(struct entry_proc_op_t *, lmgr_t *);
static int  EntryProc_db_batch_apply(struct entry_proc_op_t **, int, lmgr_t *);
#ifdef HAVE_CHANGELOGS
static int  EntryProc_chglog_clr( struct entry_proc_op_t *, lmgr_t * );
#endif
static int  EntryProc_rm_old_entries( struct entry_proc_op_t *, lmgr_t * );

/* forward declaration to check batchable operations for db_apply stage */
static bool dbop_is_batchable(struct entry_proc_op_t *, struct entry_proc_op_t *, attr_mask_t *);

/** pipeline stages */
enum {
    STAGE_GET_FID = 0,
    STAGE_GET_INFO_DB,
    STAGE_GET_INFO_FS,
    STAGE_PRE_APPLY,
    STAGE_DB_APPLY,
#ifdef HAVE_CHANGELOGS
    STAGE_CHGLOG_CLR,
#endif
    STAGE_RM_OLD_ENTRIES,   /* special stage at the end of FS scan */

    PIPELINE_STAGE_COUNT    /* keep last */
};

/** std_pipeline entry points */
const pipeline_descr_t std_pipeline_descr =
{
    .stage_count    = PIPELINE_STAGE_COUNT,
    .GET_ID         = STAGE_GET_FID,
    .GET_INFO_DB    = STAGE_GET_INFO_DB,
    .GET_INFO_FS    = STAGE_GET_INFO_FS,
    .GC_OLDENT      = STAGE_RM_OLD_ENTRIES,
    .DB_APPLY       = STAGE_DB_APPLY,
};

/** pipeline stages definition and parameters */
pipeline_stage_t std_pipeline[] = {
    {STAGE_GET_FID, "STAGE_GET_FID", EntryProc_get_fid, NULL, NULL,
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC, 0},
    {STAGE_GET_INFO_DB, "STAGE_GET_INFO_DB", EntryProc_get_info_db, NULL, NULL,
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC | STAGE_FLAG_ID_CONSTRAINT, 0},
    {STAGE_GET_INFO_FS, "STAGE_GET_INFO_FS", EntryProc_get_info_fs, NULL, NULL,
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC, 0},
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
    /* only 1 thread here because committing records must be sequential
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
        DisplayLog(LVL_CRIT, ENTRYPROC_TAG,
                   "Error: not enough information to get fid: "
                   "parent_id/name or fullpath needed");
        EntryProcessor_Acknowledge(p_op, -1, true);
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
        rc = EntryProcessor_Acknowledge(p_op, -1, true);
        if ( rc )
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                        "Error %d acknowledging stage STAGE_GET_FID.", rc );
    }
    else
    {
        EntryProcessor_SetEntryId( p_op, &tmp_id );

        /* go to GET_INFO_DB stage */
        rc = EntryProcessor_Acknowledge(p_op, STAGE_GET_INFO_DB, false);
        if ( rc )
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                        "Error %d acknowledging stage STAGE_GET_FID.", rc );
    }
    return rc;
#else
    DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error: unexpected stage in a filesystem with no fid: STAGE_GET_FID.");
    EntryProcessor_Acknowledge(p_op, -1, true);
    return EINVAL;
#endif
}

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
        attr_mask_set_index(&p_op->fs_attr_need, ATTR_INDEX_name);
        attr_mask_set_index(&p_op->fs_attr_need, ATTR_INDEX_parent_id);
    }
}

/**
 * Infer information from the changelog record (status, ...).
 * \return next pipeline step to be perfomed.
 */
static int EntryProc_FillFromLogRec(struct entry_proc_op_t *p_op,
                                    bool allow_md_updt)
{
    /* status mask to call changelog callbacks */
    uint32_t cl_cb_status_mask = 0;
    bool status_mask_set = false;

    /* alias to the log record */
    CL_REC_TYPE *logrec = p_op->extra_info.log_record.p_log_rec;
    attr_mask_t status_mask_need = null_mask;
    proc_action_e rec_action = PROC_ACT_NONE;

    /* if this is a CREATE record, we know that its status is NEW. */
    if (logrec->cr_type == CL_CREATE)
    {
        /* not a symlink */
        attr_mask_unset_index(&p_op->fs_attr_need, ATTR_INDEX_link);

        /* name and parent should have been provided by the CREATE changelog record */
        check_path_info(p_op, "CREATE");

        /* Sanity check: if the entry already exists in DB,
         * it could come from a previous filesystem that has been reformatted.
         * In this case, force a full update of the entry.
         */
        if (p_op->db_exists)
        {
                DisplayLog(LVL_EVENT, ENTRYPROC_TAG,
                           "CREATE record on already existing entry "DFID"%s%s."
                           " This is normal if you scanned it previously.",
                           PFID(&p_op->entry_id),
                           ATTR_MASK_TEST(&p_op->db_attrs, fullpath)?", path=":
                               (ATTR_MASK_TEST(&p_op->db_attrs, name)?", name=":""),
                           ATTR_MASK_TEST(&p_op->db_attrs, fullpath)?
                                ATTR(&p_op->db_attrs, fullpath):
                                (ATTR_MASK_TEST(&p_op->db_attrs, name)?
                                    ATTR(&p_op->db_attrs,name):""));

            /* set insertion time, like for a new entry */
            ATTR_MASK_SET(&p_op->fs_attrs, creation_time);
            ATTR(&p_op->fs_attrs, creation_time)
                = cltime2sec(logrec->cr_time);

            /* force updating attributes */
            p_op->fs_attr_need.std |= POSIX_ATTR_MASK | ATTR_MASK_stripe_info;
            /* get status for all policies with a matching scope */
            add_matching_scopes_mask(&p_op->entry_id, &p_op->fs_attrs, true,
                                     &p_op->fs_attr_need.status);

            /* will use the same mask for calling changelog callbacks */
            cl_cb_status_mask = p_op->fs_attr_need.status;
            status_mask_set = true;
        }
    }
    else if (logrec->cr_type == CL_HARDLINK)
    {
        /* The entry exists but not the name. We only have to
         * create the name. */

        /* name and parent should have been provided by the HARDLINK changelog record */
        check_path_info(p_op, "HARDLINK");
    }
    else if ((logrec->cr_type == CL_MKDIR) || (logrec->cr_type == CL_RMDIR))
    {
        /* entry is a directory */
        ATTR_MASK_SET(&p_op->fs_attrs, type);
        strcpy(ATTR(&p_op->fs_attrs, type), STR_TYPE_DIR);

        /* not a link */
        attr_mask_unset_index(&p_op->fs_attr_need, ATTR_INDEX_link);

        /* no stripe info for dirs */
        attr_mask_unset_index(&p_op->fs_attr_need, ATTR_INDEX_stripe_info);
        attr_mask_unset_index(&p_op->fs_attr_need, ATTR_INDEX_stripe_items);

        /* when a directory is created or deleted, directory is empty */
        ATTR_MASK_SET(&p_op->fs_attrs, dircount);
        ATTR(&p_op->fs_attrs, dircount) = 0;

        /* path info should be set */
        check_path_info(p_op, changelog_type2str(logrec->cr_type));
    }
    else if (logrec->cr_type == CL_SOFTLINK)
    {
        /* entry is a symlink */
        ATTR_MASK_SET(&p_op->fs_attrs, type);
        strcpy(ATTR(&p_op->fs_attrs, type), STR_TYPE_LINK);

        /* need to get symlink content */
        attr_mask_set_index(&p_op->fs_attr_need, ATTR_INDEX_link);

        /* no stripe info for symlinks */
        attr_mask_unset_index(&p_op->fs_attr_need, ATTR_INDEX_stripe_info);
        attr_mask_unset_index(&p_op->fs_attr_need, ATTR_INDEX_stripe_items);
    }
#ifdef _LUSTRE_HSM
    else if (logrec->cr_type == CL_HSM)
    {
        /* not a link */
        attr_mask_unset_index(&p_op->fs_attr_need, ATTR_INDEX_link);
    }
#endif
    else if (logrec->cr_type == CL_UNLINK)
    {
        /* name and parent should have been provided by the UNLINK changelog record */
        check_path_info(p_op, "UNLINK");
    }
#ifdef HAVE_CL_LAYOUT
    else if (logrec->cr_type == CL_LAYOUT)
    {
        attr_mask_set_index(&p_op->fs_attr_need, ATTR_INDEX_stripe_info);
        attr_mask_set_index(&p_op->fs_attr_need, ATTR_INDEX_stripe_items);
    }
#endif

    /* if the entry is already in DB, try to determine if something changed */
    if (p_op->db_exists)
    {
        if (logrec->cr_type == CL_EXT)
        {
            /* in case of a rename, the path info must be set */
            check_path_info(p_op, "RENAME");
        }

        /* get the new attributes, in case of a SATTR, HSM... */
        if (allow_md_updt && ((logrec->cr_type == CL_MTIME)
                              || (logrec->cr_type == CL_CTIME)
                              || (logrec->cr_type == CL_CLOSE)
                              || (logrec->cr_type == CL_TRUNC)
                              || (logrec->cr_type == CL_HSM)
                              || (logrec->cr_type == CL_SETATTR)))
        {
            DisplayLog(LVL_DEBUG, ENTRYPROC_TAG,
                       "Getattr needed because this is a %s event, and "
                       "metadata has not been recently updated.",
                       changelog_type2str(logrec->cr_type));

            p_op->fs_attr_need.std |= POSIX_ATTR_MASK;
        }
    }

    if (!status_mask_set)
    {
        /* status mask has not been already computed */
        /* Warning: do not use cached DB information to check the scope
         * because some information may have changed (e.g. entry status)
         * so the entry may now match the scope, and using an outdated status
         * may result in an invalid matching. */
        add_matching_scopes_mask(&p_op->entry_id, &p_op->fs_attrs, true,
                                 &cl_cb_status_mask);
    }
    else
    {
        cl_cb_status_mask = p_op->fs_attr_need.status;
    }
    /* call changelog callback for policies with a matching scope */
    run_all_cl_cb(logrec, &p_op->entry_id, &p_op->db_attrs, &p_op->fs_attrs,
                  &status_mask_need, cl_cb_status_mask, &rec_action);
    p_op->fs_attr_need = attr_mask_or(&p_op->fs_attr_need, &status_mask_need);

    /* process the value of rec_action */
    switch (rec_action)
    {
        case PROC_ACT_NONE: /* nothing particular */
            DisplayLog(LVL_DEBUG, ENTRYPROC_TAG,
                       DFID": run_all_cl_cb=none",
                       PFID(&p_op->entry_id));
            break;
        case PROC_ACT_RM_ALL: /* remove the entry if it exists */
            DisplayLog(LVL_DEBUG, ENTRYPROC_TAG,
                       DFID": run_all_cl_cb=rm from DB",
                       PFID(&p_op->entry_id));
            if (p_op->db_exists)
            {
                p_op->db_op_type = OP_TYPE_REMOVE_LAST;
                return STAGE_PRE_APPLY;
            }
            else /* ignore the record */
                return STAGE_CHGLOG_CLR;

        case PROC_ACT_SOFTRM_IF_EXISTS:
            DisplayLog(LVL_DEBUG, ENTRYPROC_TAG,
                       DFID": run_all_cl_cb=softrm if exists in DB",
                       PFID(&p_op->entry_id));
            /* if no policy manages deleted entries, don't SOFTRM */
            if (!has_deletion_policy())
            {
                p_op->db_op_type = OP_TYPE_REMOVE_LAST;
                return STAGE_PRE_APPLY;
            }
            else if (p_op->db_exists)
            {
                /* soft remove when it exists */
                p_op->db_op_type = OP_TYPE_SOFT_REMOVE;
                return STAGE_PRE_APPLY;
            }
            else /* ignore the record */
                return STAGE_CHGLOG_CLR;

        case PROC_ACT_SOFTRM_ALWAYS:
            DisplayLog(LVL_DEBUG, ENTRYPROC_TAG,
                       DFID": run_all_cl_cb=always softrm",
                       PFID(&p_op->entry_id));
            /* if no policy manages deleted entries, don't SOFTRM */
            if (!has_deletion_policy())
                p_op->db_op_type = OP_TYPE_REMOVE_LAST;
            else
                /* always soft remove */
                p_op->db_op_type = OP_TYPE_SOFT_REMOVE;

            return STAGE_PRE_APPLY;
    }

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
    bool md_allow_event_updt = true;

    if ( logrec->cr_type == CL_UNLINK )
    {
#ifdef _LUSTRE_HSM
        DisplayLog(LVL_DEBUG, ENTRYPROC_TAG,
                   "UNLINK on %s entry "DFID": last=%s, archived=%s",
                   p_op->db_exists?"known":"unknown", PFID(&p_op->entry_id),
                   bool2str(logrec->cr_flags & CLF_UNLINK_LAST),
                   bool2str(logrec->cr_flags & CLF_UNLINK_HSM_EXISTS));
#else
        DisplayLog(LVL_DEBUG, ENTRYPROC_TAG,
                   "UNLINK on %s entry "DFID": last=%s",
                   p_op->db_exists?"known":"unknown", PFID(&p_op->entry_id),
                   bool2str(logrec->cr_flags & CLF_UNLINK_LAST));
#endif

        if (!has_deletion_policy() && p_op->check_if_last_entry) {
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
        /* else: too dangerous : we risk to clean an entry in backend
         * that we should not */

        /* is it the last reference to this file? */
        if ( logrec->cr_flags & CLF_UNLINK_LAST )
        {
            if (!has_deletion_policy())
            {
                /* no policy manages deleted entries, just drop removed entries
                 * from DB */
                if (p_op->db_exists)
                {
                    p_op->db_op_type = OP_TYPE_REMOVE_LAST;
                    return STAGE_PRE_APPLY;
                }
                else
                    /* ignore the record */
                    return STAGE_CHGLOG_CLR;
            }
            /* policy-specific cases are managed in changelog callbacks */
        }
        else if ( p_op->db_exists ) /* entry still exists and is known in DB */
        {
            /* Remove the name only. Keep the inode information since
             * there is more file names referring to it. */
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
        /* XXX this piece of code suppose policies do not perform
         * softrm on directories. This must be modified if
         * some status managers care about directory removal.
         */
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

    /* not a removal */
    if (logrec->cr_type != CL_UNLINK && logrec->cr_type != CL_RMDIR) /* not a removal */
    {
        if (!p_op->db_exists)
        {
            attr_mask_t tmp;

            DisplayLog(LVL_FULL, ENTRYPROC_TAG, DFID"not in DB: INSERT", PFID(&p_op->entry_id));

            /* non-unlink (or non-destructive unlink) record on unknown entry:
             * insert entry to the DB */
            p_op->db_op_type = OP_TYPE_INSERT;

            /* new entry, set insertion time */
            ATTR_MASK_SET( &p_op->fs_attrs, creation_time );
            ATTR( &p_op->fs_attrs, creation_time ) = cltime2sec(logrec->cr_time);

            /* we must get info that is not provided by the chglog:
             * fs_attr_need |=  <needed attributes> AND NOT in fs_attrs.
             */
            tmp.std = (POSIX_ATTR_MASK | ATTR_MASK_name | ATTR_MASK_parent_id
                       | ATTR_MASK_stripe_info | ATTR_MASK_stripe_items
                       | ATTR_MASK_link);
            tmp = attr_mask_and_not(&tmp, &p_op->fs_attrs.attr_mask);
            p_op->fs_attr_need = attr_mask_or(&p_op->fs_attr_need, &tmp);

            /* if we needed fullpath (e.g. for policies), set it */
            if (attr_mask_test_index(&p_op->db_attr_need, ATTR_INDEX_fullpath) &&
                !ATTR_MASK_TEST(&p_op->fs_attrs, fullpath))
                attr_mask_set_index(&p_op->fs_attr_need, ATTR_INDEX_fullpath);

            /* EntryProc_FillFromLogRec() will determine
             * what status are to be retrieved.
             */
        }
        else /* non-unlink record on known entry */
        {
            attr_mask_t db_missing;

            p_op->db_op_type = OP_TYPE_UPDATE;

            /* check what information must be updated.
             * missing info = DB query - retrieved */
            db_missing = attr_mask_and_not(&p_op->db_attr_need,
                                           &p_op->db_attrs.attr_mask);

            /* get attrs if some is missing (all std attrs) */
            if ((db_missing.std & POSIX_ATTR_MASK) &&
                ((p_op->fs_attrs.attr_mask.std & POSIX_ATTR_MASK) != POSIX_ATTR_MASK))
                p_op->fs_attr_need.std |= POSIX_ATTR_MASK;

            /* get stripe info if missing (file only) */
            if ((db_missing.std & ATTR_MASK_stripe_info) && !ATTR_MASK_TEST(&p_op->fs_attrs, stripe_info)
                && (!ATTR_FSorDB_TEST(p_op, type) || !strcmp(ATTR_FSorDB(p_op, type), STR_TYPE_FILE)))
            {
                p_op->fs_attr_need.std |= ATTR_MASK_stripe_info | ATTR_MASK_stripe_items;
            }

            /* get link content if missing (symlink only) */
            if ((db_missing.std & ATTR_MASK_link) && !ATTR_MASK_TEST(&p_op->fs_attrs, link)
                && (!ATTR_FSorDB_TEST(p_op, type) || !strcmp(ATTR_FSorDB(p_op, type), STR_TYPE_LINK)))
                    p_op->fs_attr_need.std |= ATTR_MASK_link;

            /* EntryProc_FillFromLogRec() will determine
             * what status are to be retrieved. */

            /* Check md_update policy */
            if (need_md_update(&p_op->db_attrs, &md_allow_event_updt))
                p_op->fs_attr_need.std |= POSIX_ATTR_MASK;

            /* check if path update is needed (only if it was not just updated) */
            if ((!ATTR_MASK_TEST(&p_op->fs_attrs, parent_id) || !ATTR_MASK_TEST(&p_op->fs_attrs, name))
                && (need_path_update(&p_op->db_attrs, NULL)
                    || (db_missing.std & (ATTR_MASK_fullpath | ATTR_MASK_name | ATTR_MASK_parent_id))))
                p_op->fs_attr_need.std |= ATTR_MASK_name
                                      | ATTR_MASK_parent_id;
        }
    }

    /* infer info from changelog record, then continue to next step */
    return EntryProc_FillFromLogRec(p_op, md_allow_event_updt);
}
#endif /* CHANGELOG support */

/* Ensure the fullpath from DB is consistent.
 * Set updt_mask according to the missing info.
 */
static void check_fullpath(attr_set_t *attrs, const entry_id_t *id, attr_mask_t *updt_mask)
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
                updt_mask->std |= ATTR_MASK_parent_id | ATTR_MASK_name | ATTR_MASK_fullpath;
            }
            /* check if the entry is the direct child of the unknown directory */
            else if (strchr(next+1, '/') == NULL)
            {
                DisplayLog(LVL_EVENT, ENTRYPROC_TAG, "Parent dir for entry "DFID
                           " is unknown (parent: "DFID", child name: '%s'): updating entry path info",
                           PFID(id), PFID(&parent_id), next+1);
                ATTR_MASK_UNSET(attrs, fullpath);
                updt_mask->std |= ATTR_MASK_parent_id | ATTR_MASK_name | ATTR_MASK_fullpath;
            }
            /* else: FIXME: We should update parent info for an upper entry. */
            else
            {
                ATTR_MASK_UNSET(attrs, fullpath);
                /* update path info anyhow, to try fixing the issue */
                updt_mask->std |= ATTR_MASK_parent_id | ATTR_MASK_name | ATTR_MASK_fullpath;
            }
        }
        else
        {
            /* fullpath is not consistent (should be <pid>/name) */
            ATTR_MASK_UNSET(attrs, fullpath);
            /* update path info, to try fixing the issue */
            updt_mask->std |= ATTR_MASK_parent_id | ATTR_MASK_name | ATTR_MASK_fullpath;
        }
    }
#else
    if (ATTR_MASK_TEST(attrs, fullpath)
        && ATTR(attrs, fullpath)[0] != '/')
    {
        /* fullpath is not pertinent */
        ATTR_MASK_UNSET(attrs, fullpath);
        /* update path info, to try fixing the issue */
        updt_mask->std |= ATTR_MASK_parent_id | ATTR_MASK_name | ATTR_MASK_fullpath;
    }
#endif
}

static bool is_lustre_special(const struct entry_proc_op_t *p_op)
{
#ifdef _HAVE_FID
    if (p_op->entry_id_is_set)
    {
        /* check if id is the same as '<root>/.lustre' or '<root>/.lustre/fid' */
        if (entry_id_equal(&p_op->entry_id, get_dot_lustre_fid())
            || entry_id_equal(&p_op->entry_id, get_fid_fid()))
        {
            DisplayLog(LVL_DEBUG, ENTRYPROC_TAG, "Ignoring special lustre directory "DFID,
                       PFID(&p_op->entry_id));
            return true;
        }
    }
    else /* id is not known, check other criteria (name, path, ...) */
    {
        const char       *path;
        const entry_id_t *root_id;

        root_id = get_root_id();

        /* check if parent_id is root dir and name is '.lustre' */
        if (root_id != NULL && ATTR_MASK_TEST(&p_op->fs_attrs, parent_id)
            && ATTR_MASK_TEST(&p_op->fs_attrs, name)
            && entry_id_equal(root_id, &ATTR(&p_op->fs_attrs, parent_id))
            && strcmp(ATTR(&p_op->fs_attrs, name), dot_lustre_name) == 0)
        {
            DisplayLog(LVL_DEBUG, ENTRYPROC_TAG, "Ignoring special lustre directory "DFID"/%s",
                       PFID(root_id), ATTR(&p_op->fs_attrs, name));
            return true;
        }

        path = get_dot_lustre_dir();

        /* check the whole '<mnt_dir>/.lustre' path */
        if (path != NULL && ATTR_FSorDB_TEST(p_op, fullpath) &&
            strncmp(ATTR_FSorDB(p_op, fullpath), path, strlen(path) + 1) == 0)
        {
            DisplayLog(LVL_DEBUG, ENTRYPROC_TAG, "Ignoring special lustre directory <%s>",
                       ATTR_FSorDB(p_op, fullpath));
            return true;
        }
    }
#endif
    return false;
}


/**
 * check if the entry exists in the database and what info
 * must be retrieved.
 */
int EntryProc_get_info_db( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int      rc = 0;
    int      next_stage = -1; /* -1 = skip */
    attr_mask_t attr_allow_cached = null_mask;
    attr_mask_t attr_need_fresh = null_mask;
    uint32_t status_scope = 0; /* status mask */
    attr_mask_t tmp;

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

    /* ignore special files */
    if (is_lustre_special(p_op)) {
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

        p_op->db_attr_need = null_mask;

        if (type_clue == TYPE_NONE) {
            /* type is a useful information to take decisions (about getstripe, readlink, ...) */
            attr_mask_set_index(&p_op->db_attr_need, ATTR_INDEX_type);
        } else {
            ATTR_MASK_SET(&p_op->fs_attrs, type);
            strcpy(ATTR(&p_op->fs_attrs, type), type2db(type_clue));
        }

        /* add diff mask for diff mode */
        p_op->db_attr_need = attr_mask_or(&p_op->db_attr_need, &diff_mask);

        /* chglog_reader_config.mds_has_lu543 has already been tested
         * in changelog reader before pushing the entry. */
        if (logrec->cr_type == CL_UNLINK && p_op->get_fid_from_db)
        {
            /* It is possible this unlink was inserted by the changelog
             * reader. Some Lustre server don't give the FID, so retrieve
             * it now from the NAMES table, given the parent FID and the
             * filename. */
            p_op->get_fid_from_db = 0;
            rc = ListMgr_Get_FID_from_Path(lmgr, &logrec->cr_pfid,
                                           rh_get_cl_cr_name(logrec),
                                           &p_op->entry_id);

            if (!rc)
            {
                if (!fid_is_sane(&logrec->cr_pfid))
                    DisplayLog(LVL_MAJOR, ENTRYPROC_TAG,
                               "Error: insane parent fid "DFID" from DB",
                               PFID(&logrec->cr_pfid));
                /* The FID is now set, so we can register it with the
                 * constraint engine. Since this operation is at the
                 * very top of the queue, we register it at the head
                 * of the constraint list, not at the tail. */
                p_op->entry_id_is_set = 1;
                id_constraint_register(p_op, true);
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
            attr_mask_set_index(&p_op->db_attr_need, ATTR_INDEX_nlink);
        /* If it's a hard link, we will need the hlink so we can
         * increment it. Will override the fs value. */
        else if (logrec->cr_type == CL_HARDLINK)
            attr_mask_set_index(&p_op->db_attr_need, ATTR_INDEX_nlink);

        /* Only need to get md_update if the update policy != always */
        if (updt_params.md.when != UPDT_ALWAYS)
            attr_mask_set_index(&p_op->db_attr_need, ATTR_INDEX_md_update);

        /* Only need to get path_update if the update policy != always
         * and if it is not provided in logrec
         */
        if ((updt_params.path.when != UPDT_ALWAYS)
            && (logrec->cr_namelen == 0))
            attr_mask_set_index(&p_op->db_attr_need, ATTR_INDEX_path_update);

#if 0 /* XXX not to be done systematically: only on specific events? (CL_LAYOUT...) */
#ifdef _LUSTRE
        if ((type_clue == TYPE_NONE || type_clue == TYPE_FILE)
            && logrec->cr_type != CL_CREATE)
            /* to check if stripe_info is set for this entry (useless for CREATE)*/
            p_op->db_attr_need |= ATTR_MASK_stripe_info;
#endif
#endif

        if (entry_proc_conf.detect_fake_mtime)
            attr_mask_set_index(&p_op->db_attr_need, ATTR_INDEX_creation_time);

        if (type_clue == TYPE_NONE || type_clue == TYPE_LINK)
            /* check if link content is set for this entry */
            attr_mask_set_index(&p_op->db_attr_need, ATTR_INDEX_link);

        if (entry_proc_conf.match_classes)
        {
            if (updt_params.fileclass.when != UPDT_ALWAYS)
                attr_mask_set_index(&p_op->db_attr_need, ATTR_INDEX_class_update);

            tmp = attr_mask_and_not(&policies.global_fileset_mask, &p_op->fs_attrs.attr_mask);
            p_op->db_attr_need = attr_mask_or(&p_op->db_attr_need, &tmp);
        }

        /* check if entry is in policies scope */
        add_matching_scopes_mask(&p_op->entry_id, &p_op->fs_attrs, true, &status_scope);

        /* get missing attributes to check the scopes:
         * db_attr_need |= <attrs_for_status> and not <fs_attrs>
         */
        tmp = attrs_for_status_mask(status_scope, false);
        tmp = attr_mask_and_not(&tmp, &p_op->fs_attrs.attr_mask);
        p_op->db_attr_need = attr_mask_or(&p_op->db_attr_need, &tmp);

        /* in case of unlink, we need softrm filter masks */
        if (p_op->extra_info.log_record.p_log_rec->cr_type == CL_UNLINK)
        {
            tmp = sm_softrm_mask();

            p_op->db_attr_need = attr_mask_or(&p_op->db_attr_need, &tmp);
        }

        /* In case of a RENAME, match the new name (not the one from the DB). */
        if ((logrec->cr_type == CL_EXT)
             && (p_op->db_attr_need.std & ATTR_MASK_fullpath))
        {
            rc = Lustre_GetFullPath(&p_op->entry_id,
                                    ATTR(&p_op->fs_attrs, fullpath),
                                    sizeof(ATTR(&p_op->fs_attrs, fullpath)));
            if (rc == 0)
            {
                ATTR_MASK_SET(&p_op->fs_attrs, fullpath);
                p_op->db_attr_need.std &= ~ATTR_MASK_fullpath;
            }
        }

        /* attributes to be retrieved */
        p_op->db_attrs.attr_mask = p_op->db_attr_need;

        rc = ListMgr_Get( lmgr, &p_op->entry_id, &p_op->db_attrs );

        if (rc == DB_SUCCESS )
        {
            p_op->db_exists = 1;
            /* attr mask has been set by ListMgr_Get */
        }
        else if (rc == DB_NOT_EXISTS )
        {
            p_op->db_exists = 0;
            /* no attrs from DB */
            ATTR_MASK_INIT( &p_op->db_attrs );
        }
        else
        {
            /* ERROR */
            DisplayLog(LVL_CRIT, ENTRYPROC_TAG,
                       "Error %d retrieving entry "DFID" from DB: %s.", rc,
                       PFID(&p_op->entry_id), lmgr_err2str(rc));
            p_op->db_exists = 0;
            /* no attrs from DB */
            ATTR_MASK_INIT( &p_op->db_attrs );
        }

        /* Retrieve info from the log record, and decide what info must be
         * retrieved from filesystem. */
        next_stage = EntryProc_ProcessLogRec( p_op );

        /* Note: this check must be done after processing log record,
         * because it can determine if status is needed */
        tmp = attrs_for_status_mask(p_op->fs_attr_need.status, true);
        p_op->fs_attr_need = attr_mask_or(&p_op->fs_attr_need, &tmp);

        char tmp_buf[RBH_NAME_MAX];
        DisplayLog(LVL_DEBUG, ENTRYPROC_TAG, "RECORD: %s "DFID" %#x %s => "
                   "getstripe=%u, getattr=%u, getpath=%u, readlink=%u"
                   ", getstatus(%s)",
                   changelog_type2str(logrec->cr_type), PFID(&p_op->entry_id),
                   logrec->cr_flags & CLF_FLAGMASK,
                   logrec->cr_namelen ? rh_get_cl_cr_name(logrec) : "<null>",
                   NEED_GETSTRIPE(p_op)?1:0, NEED_GETATTR(p_op)?1:0,
                   NEED_GETPATH(p_op)?1:0, NEED_READLINK(p_op)?1:0,
                   name_status_mask(p_op->fs_attr_need.status, tmp_buf, sizeof(tmp_buf)));
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

        /* check if entry is in policies scope */
        add_matching_scopes_mask(&p_op->entry_id, &p_op->fs_attrs, true, &status_scope);

        p_op->db_attr_need = attr_mask_or(&p_op->db_attr_need, &diff_mask);
        /* retrieve missing attributes for diff */
        tmp = attr_mask_and_not(&diff_mask, &p_op->fs_attrs.attr_mask);
        p_op->fs_attr_need = attr_mask_or(&p_op->fs_attr_need, &tmp);

        if (entry_proc_conf.detect_fake_mtime)
            attr_mask_set_index(&p_op->db_attr_need, ATTR_INDEX_creation_time);

        /* get all needed attributes for status */
        attr_allow_cached = attrs_for_status_mask(status_scope, false);
        attr_need_fresh = attrs_for_status_mask(status_scope, true);

        /* what must be retrieved from DB: */
        tmp = attr_mask_and_not(&attr_allow_cached, &p_op->fs_attrs.attr_mask);
        p_op->db_attr_need = attr_mask_or(&p_op->db_attr_need, &tmp);

        /* no dircount for non-dirs */
        if (ATTR_MASK_TEST(&p_op->fs_attrs, type) &&
            strcmp(ATTR(&p_op->fs_attrs, type), STR_TYPE_DIR))
            attr_mask_unset_index(&p_op->db_attr_need, ATTR_INDEX_dircount);

        /* no readlink for non symlinks */
        if (ATTR_MASK_TEST(&p_op->fs_attrs, type)) /* likely */
        {
            if (!strcmp(ATTR(&p_op->fs_attrs, type), STR_TYPE_LINK))
            {
                attr_mask_set_index(&p_op->db_attr_need, ATTR_INDEX_link); /* check if it is known */
                /* no stripe for symlinks */
                attr_mask_unset_index(&p_op->db_attr_need, ATTR_INDEX_stripe_info);
                attr_mask_unset_index(&p_op->db_attr_need, ATTR_INDEX_stripe_items);
            }
            else
                attr_mask_unset_index(&p_op->db_attr_need, ATTR_INDEX_link);
        }

        if (entry_proc_conf.match_classes)
        {
            if (updt_params.fileclass.when != UPDT_ALWAYS)
                attr_mask_set_index(&p_op->db_attr_need, ATTR_INDEX_class_update);

            tmp = attr_mask_and_not(&policies.global_fileset_mask,
                                    &p_op->fs_attrs.attr_mask);
            p_op->db_attr_need = attr_mask_or(&p_op->db_attr_need, &tmp);
        }

        if (!attr_mask_is_null(p_op->db_attr_need))
        {
            p_op->db_attrs.attr_mask = p_op->db_attr_need;
            rc = ListMgr_Get(lmgr, &p_op->entry_id, &p_op->db_attrs);

            if (rc == DB_SUCCESS)
            {
                p_op->db_exists = 1;
                tmp = attr_mask_and_not(&p_op->db_attr_need, &p_op->db_attrs.attr_mask);
                p_op->fs_attr_need = attr_mask_or(&p_op->fs_attr_need, &tmp);
            }
            else if (rc == DB_NOT_EXISTS )
            {
                p_op->db_exists = 0;
                ATTR_MASK_INIT( &p_op->db_attrs );
            }
            else
            {
                /* ERROR */
                DisplayLog(LVL_CRIT, ENTRYPROC_TAG,
                           "Error %d retrieving entry "DFID" from DB: %s.", rc,
                           PFID(&p_op->entry_id), lmgr_err2str(rc));
                p_op->db_exists = 0;
                ATTR_MASK_INIT( &p_op->db_attrs );
            }
        }
        else
        {
            p_op->db_exists = ListMgr_Exists( lmgr, &p_op->entry_id );
        }

        /* get status for all policies */
        p_op->fs_attr_need.status |= all_status_mask();
        tmp = attr_mask_and_not(&attr_need_fresh, &p_op->fs_attrs.attr_mask);
        p_op->fs_attr_need = attr_mask_or(&p_op->fs_attr_need, &tmp);

        if ( !p_op->db_exists )
        {
            /* new entry */
            p_op->db_op_type = OP_TYPE_INSERT;

            /* set creation time if it was not set by scan module */
            if (!ATTR_MASK_TEST(&p_op->fs_attrs, creation_time))
            {
                ATTR_MASK_SET( &p_op->fs_attrs, creation_time );
                ATTR( &p_op->fs_attrs, creation_time ) = time(NULL); /* FIXME min(atime,mtime,ctime)? */
            }

#ifdef _LUSTRE
            /* get stripe for files */
            if (ATTR_MASK_TEST(&p_op->fs_attrs, type)
                && !strcmp(ATTR( &p_op->fs_attrs, type ), STR_TYPE_FILE )
                /* only if it was not retrieved during the scan */
                && !(ATTR_MASK_TEST(&p_op->fs_attrs, stripe_info)
                    && ATTR_MASK_TEST(&p_op->fs_attrs, stripe_items)))
            {
                attr_mask_set_index(&p_op->fs_attr_need, ATTR_INDEX_stripe_info);
                attr_mask_set_index(&p_op->fs_attr_need, ATTR_INDEX_stripe_items);
            }
#endif

            /* readlink for symlinks (if not already known) */
            if (ATTR_MASK_TEST(&p_op->fs_attrs, type)
                && !strcmp(ATTR( &p_op->fs_attrs, type ), STR_TYPE_LINK)
                && !ATTR_MASK_TEST(&p_op->fs_attrs, link))
            {
                attr_mask_set_index(&p_op->fs_attr_need, ATTR_INDEX_link);
            }
            else
            {
                attr_mask_unset_index(&p_op->fs_attr_need, ATTR_INDEX_link);
            }

            next_stage = STAGE_GET_INFO_FS;
        }
        else
        {
            p_op->db_op_type = OP_TYPE_UPDATE;

            if (ATTR_MASK_TEST(&p_op->fs_attrs, type)) /* likely set */
            {
                if (strcmp(ATTR( &p_op->fs_attrs, type ), STR_TYPE_LINK))
                    /* non-link */
                    attr_mask_unset_index(&p_op->fs_attr_need, ATTR_INDEX_link);
                else
                {
                    /* link */
#ifdef _LUSTRE
                    /* already known (in DB or FS) */
                    if (ATTR_FSorDB_TEST(p_op, link))
                        attr_mask_unset_index(&p_op->fs_attr_need, ATTR_INDEX_link);
                    else /* not known */
                        attr_mask_set_index(&p_op->fs_attr_need, ATTR_INDEX_link);
#else
                    /* For non-lustre filesystems, inodes may be recycled, so re-read link even if it is is DB */
                    if (ATTR_MASK_TEST(&p_op->fs_attrs, link))
                        attr_mask_unset_index(&p_op->fs_attr_need, ATTR_INDEX_link);
                    else
                        attr_mask_set_index(&p_op->fs_attr_need, ATTR_INDEX_link);
#endif
                }
            }

            /* get parent_id+name, if not set during scan (eg. for root directory) */
            if (!ATTR_MASK_TEST( &p_op->fs_attrs, name))
                attr_mask_set_index(&p_op->fs_attr_need, ATTR_INDEX_name);
            if (!ATTR_MASK_TEST( &p_op->fs_attrs, parent_id))
                attr_mask_set_index(&p_op->fs_attr_need, ATTR_INDEX_parent_id);

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
        rc = EntryProcessor_Acknowledge(p_op, -1, true);
    else
        /* go to next pipeline step */
        rc = EntryProcessor_Acknowledge(p_op, next_stage, false);

    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                    "Error %d acknowledging stage %s.", rc,
                    stage_info->stage_name );
    return rc;
}

/** skip_record a record by acknowledging current operation */
static int skip_record(struct entry_proc_op_t *p_op)
{
    int rc;

#ifdef HAVE_CHANGELOGS
    if (p_op->extra_info.is_changelog_record)
        /* do nothing on DB but ack the record */
        rc = EntryProcessor_Acknowledge(p_op, STAGE_CHGLOG_CLR, false);
    else
#endif
        /* remove the operation from processing pipeline */
        rc = EntryProcessor_Acknowledge(p_op, -1, true);

    if (rc)
        DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage.", rc);
    return rc;
}

#ifdef HAVE_CHANGELOGS
/** take DB removal decision when an entry no longer exists in the filesystem */
static int rm_record(struct entry_proc_op_t *p_op)
{
    attr_set_t merged_attrs = ATTR_SET_INIT;
    proc_action_e pa;
    int rc;

    /* if the entry is no in DB, an no deletion policy is defined,
     * just drop the log record */
    if (!p_op->db_exists && !has_deletion_policy())
        return skip_record(p_op);

    if (p_op->extra_info.is_changelog_record
        && (p_op->extra_info.log_record.p_log_rec->cr_type != CL_UNLINK
            && p_op->extra_info.log_record.p_log_rec->cr_type != CL_RMDIR))
    {
        /* Lustre 2 with changelog: we are here because lstat (by fid)
         * on the entry failed, which ensure the entry no longer
         * exists. Skip it. The entry will be removed by a subsequent
         * UNLINK record.
         *
         * On other posix filesystems, the entry disappeared between
         * its scanning and its processing... skip it so it will be
         * cleaned at the end of the scan. */
         return skip_record(p_op);
    }

    ATTR_MASK_INIT(&merged_attrs);

    ListMgr_MergeAttrSets(&merged_attrs, &p_op->fs_attrs, 1);
    ListMgr_MergeAttrSets(&merged_attrs, &p_op->db_attrs, 0);

    pa =  match_all_softrm_filters(&p_op->entry_id, &merged_attrs);

    /* free allocated structs in merged attributes */
    ListMgr_FreeAttrs(&merged_attrs);

    switch (pa)
    {
        case PROC_ACT_NONE:
            DisplayLog(LVL_DEBUG, ENTRYPROC_TAG,
                       DFID": match_all_softrm_filters=none",
                       PFID(&p_op->entry_id));
            /* keep the current db_op_type */
            break;

        case PROC_ACT_RM_ALL:
            DisplayLog(LVL_DEBUG, ENTRYPROC_TAG,
                       DFID": match_all_softrm_filters=rm from DB",
                       PFID(&p_op->entry_id));
            return rm_record(p_op);

        case PROC_ACT_SOFTRM_IF_EXISTS:
            DisplayLog(LVL_DEBUG, ENTRYPROC_TAG,
                       DFID": match_all_softrm_filters=softrm if exists in DB (exist=%d)",
                       PFID(&p_op->entry_id), p_op->db_exists ? 1 : 0);
            if (p_op->db_exists)
                p_op->db_op_type = OP_TYPE_SOFT_REMOVE;
            else
                /* drop the record */
                 return skip_record(p_op);
            break;

        case PROC_ACT_SOFTRM_ALWAYS:
            DisplayLog(LVL_DEBUG, ENTRYPROC_TAG,
                       DFID": match_all_softrm_filters=always softrm",
                       PFID(&p_op->entry_id));
            p_op->db_op_type = OP_TYPE_SOFT_REMOVE;
            break;
    }

    rc = EntryProcessor_Acknowledge(p_op, STAGE_PRE_APPLY, false);
    if (rc)
        DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage.", rc);
    return rc;
}
#endif


int EntryProc_get_info_fs( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;
    char tmp_buf[RBH_NAME_MAX];
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
        ", Getstatus(%s), Getstripe=%u",
         PFID(&p_op->entry_id), NEED_GETATTR(p_op)?1:0,
         NEED_GETPATH(p_op)?1:0, NEED_READLINK(p_op)?1:0,
         name_status_mask(p_op->fs_attr_need.status, tmp_buf, sizeof(tmp_buf)),
         NEED_GETSTRIPE(p_op)?1:0);

    /* don't retrieve info which is already fresh */
    p_op->fs_attr_need = attr_mask_and_not(&p_op->fs_attr_need, &p_op->fs_attrs.attr_mask);

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
        if (rc != 0)
        {
            if (ERR_MISSING(rc))
            {
                DisplayLog(LVL_DEBUG, ENTRYPROC_TAG, "Entry %s no longer exists", path);
                return rm_record(p_op);
            }
            else
                DisplayLog(LVL_DEBUG, ENTRYPROC_TAG, "lstat() failed on %s: %s", path,
                           strerror(rc));

            /* If lstat returns an error, drop the log record */
            return skip_record(p_op);
        }
        else if (entry_md.st_nlink == 0)
        {
            /* remove pending */
            DisplayLog(LVL_DEBUG, ENTRYPROC_TAG, "Entry %s has nlink=0: remove pending", path);
            return rm_record(p_op);
        }

        /* convert them to internal structure */
#if defined( _LUSTRE ) && defined( _HAVE_FID ) && defined( _MDS_STAT_SUPPORT )
        stat2rbh_attrs(&entry_md, &p_op->fs_attrs, !global_config.direct_mds_stat);
#else
        stat2rbh_attrs(&entry_md, &p_op->fs_attrs, true);
#endif
        ATTR_MASK_SET( &p_op->fs_attrs, md_update );
        ATTR( &p_op->fs_attrs, md_update ) = time( NULL );

    } /* getattr needed */

    if (NEED_GETPATH(p_op))
        path_check_update(&p_op->entry_id, path, &p_op->fs_attrs, p_op->fs_attr_need);
#endif

    if (entry_proc_conf.detect_fake_mtime
        && ATTR_FSorDB_TEST(p_op, creation_time)
        && ATTR_MASK_TEST(&p_op->fs_attrs, last_mod))
    {
        check_and_warn_fake_mtime(p_op);
    }

#ifdef _LUSTRE
    /* getstripe only for files */
    if ( NEED_GETSTRIPE(p_op)
         && ATTR_FSorDB_TEST( p_op, type )
         && strcmp( ATTR_FSorDB( p_op, type), STR_TYPE_FILE ) != 0 )
    {
        attr_mask_unset_index(&p_op->fs_attr_need, ATTR_INDEX_stripe_info);
        attr_mask_unset_index(&p_op->fs_attr_need, ATTR_INDEX_stripe_items);
    }

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

    if (NEED_ANYSTATUS(p_op))
    {
        int i;
        sm_instance_t *smi;
        attr_set_t merged_attrs = ATTR_SET_INIT; /* attrs from FS+DB */
        attr_set_t new_attrs = ATTR_SET_INIT; /* attributes + status */

        ATTR_MASK_INIT(&merged_attrs);

        ListMgr_MergeAttrSets(&merged_attrs, &p_op->fs_attrs, 1);
        ListMgr_MergeAttrSets(&merged_attrs, &p_op->db_attrs, 0);

        /* match policy scopes according to newly set information:
         * remove needed status from mask and append the updated one. */
        p_op->fs_attr_need.status &= ~all_status_mask();
        /* XXX this fails if scope attributes are missing */
        add_matching_scopes_mask(&p_op->entry_id, &p_op->fs_attrs, true,
                                 &p_op->fs_attr_need.status);

        i = 0;
        while ((smi = get_sm_instance(i)) != NULL)
        {
            ATTR_MASK_INIT(&new_attrs); /* clean the mask without freeing sm_status */

            if (NEED_GETSTATUS(p_op, i))
            {
                if (smi->sm->get_status_func != NULL)
                {
                    DisplayLog(LVL_FULL, ENTRYPROC_TAG, DFID": retrieving status for policy '%s'",
                               PFID(&p_op->entry_id), smi->sm->name);
                    /* this also check if entry is ignored for this policy */
                    rc =  smi->sm->get_status_func(smi, &p_op->entry_id,
                                                   &merged_attrs, &new_attrs);
                    if (ERR_MISSING(abs(rc)))
                    {
                        DisplayLog(LVL_DEBUG, ENTRYPROC_TAG, "Entry %s no longer exists",
                                   path);
                        /* changelog: an UNLINK event will be raised, so we ignore current record
                         * scan: entry will be garbage collected at the end of the scan */
                        return skip_record(p_op);
                    }
                    else if (rc != 0)
                    {
                        DisplayLog(LVL_MAJOR, ENTRYPROC_TAG, "Failed to get status for %s (%s status manager): error %d",
                                   path, smi->sm->name, rc);
                    }
                    else
                    {
                        /* merge/update attributes */
                        ListMgr_MergeAttrSets(&p_op->fs_attrs, &new_attrs, true);

                        /** @FIXME RBHv3 if entry is not supported: set extra_info.not_supp?  */
                    }
                    /* free allocated resources, once merged */
                    ListMgr_FreeAttrs(&new_attrs);
                }
            }
            i++;
        }
        /* free allocated structs in merged attributes */
        ListMgr_FreeAttrs(&merged_attrs);
    }

    /* readlink only for symlinks */
    if (NEED_READLINK(p_op) && ATTR_FSorDB_TEST(p_op, type)
        && strcmp(ATTR_FSorDB(p_op, type), STR_TYPE_LINK) != 0)
        attr_mask_unset_index(&p_op->fs_attr_need, ATTR_INDEX_link);

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

    /** FIXME some special files should be ignored i.e. not inserted in DB. */
    if (is_lustre_special(p_op))
        /* drop the entry */
        return skip_record(p_op);

    /* match fileclasses if specified in config */
    /* FIXME: check fileclass update parameters */
    if (entry_proc_conf.match_classes)
        match_classes(&p_op->entry_id, &p_op->fs_attrs, &p_op->db_attrs);

    /* go to next step */
    rc = EntryProcessor_Acknowledge(p_op, STAGE_PRE_APPLY, false);
    if (rc)
        DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage.", rc);
    return rc;
}

static bool dbop_is_batchable(struct entry_proc_op_t *first,
                              struct entry_proc_op_t *next,
                              attr_mask_t *full_attr_mask)
{
    if (first->db_op_type != OP_TYPE_INSERT
        && first->db_op_type != OP_TYPE_UPDATE
        && first->db_op_type != OP_TYPE_NONE)
        return false;
    else if (first->db_op_type != next->db_op_type)
        return false;
    /* starting from here, db_op_type is the same for the 2 operations */
    /* all NOOP operations can be batched */
    else if (first->db_op_type == OP_TYPE_NONE)
        return true;
    /* different masks can be mixed, as long as attributes for each table are
     * the same or 0. Ask the list manager about that. */
    else if (lmgr_batch_compat(*full_attr_mask, next->fs_attrs.attr_mask))
    {
        *full_attr_mask =  attr_mask_or(full_attr_mask, &next->fs_attrs.attr_mask);
        return true;
    }
    else
        return false;
}

/** operation cleaning before the db_apply step */
int EntryProc_pre_apply(struct entry_proc_op_t *p_op, lmgr_t * lmgr)
{
    int            rc;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];

    /* once set, never change creation time */
    if (p_op->db_op_type != OP_TYPE_INSERT)
        ATTR_MASK_UNSET(&p_op->fs_attrs, creation_time);

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
        else if ((logrec->cr_type == CL_HARDLINK) &&
                 (ATTR_MASK_TEST(&p_op->db_attrs, nlink)))
        {
            /* New hardlink. Add 1 to existing value. Ignore what came
             * from the FS, since it can be out of sync by now. */
            ATTR_MASK_SET(&p_op->fs_attrs, nlink);
            ATTR(&p_op->fs_attrs, nlink) = ATTR(&p_op->db_attrs, nlink) + 1;
        }
    }
#endif

    /* Only update fields that changed */
    if (p_op->db_op_type == OP_TYPE_UPDATE)
    {
        attr_mask_t tmp;
        attr_mask_t loc_diff_mask = ListMgr_WhatDiff(&p_op->fs_attrs, &p_op->db_attrs);

        /* In scan mode, always keep md_update and path_update,
         * to avoid their cleaning at the end of the scan.
         * Also keep name and parent as they are keys in DNAMES table.
         */
        attr_mask_t to_keep = {.std = ATTR_MASK_parent_id | ATTR_MASK_name, 0, 0LL };

        /* the mask to be displayed > diff_mask (include to_keep flags) */
        attr_mask_t display_mask = attr_mask_and(&diff_mask, &loc_diff_mask);

        /* keep fullpath if parent or name changed (friendly display) */
        if (loc_diff_mask.std & (ATTR_MASK_parent_id | ATTR_MASK_name)) {
            to_keep.std |= ATTR_MASK_fullpath;
            display_mask.std |= ATTR_MASK_fullpath;
        }
#ifdef HAVE_CHANGELOGS
        if (!p_op->extra_info.is_changelog_record)
#endif
            to_keep.std |= (ATTR_MASK_md_update | ATTR_MASK_path_update);

        /* remove other unchanged attrs + attrs not in db mask */
        tmp = attr_mask_or(&loc_diff_mask, &to_keep);
        tmp = attr_mask_or_not(&tmp, &p_op->db_attrs.attr_mask);
        p_op->fs_attrs.attr_mask = attr_mask_and(&p_op->fs_attrs.attr_mask, &tmp);

        /* FIXME: free cleared attributes */

        /* SQL req optimizations:
         * if update policy == always and fileclass is not changed,
         * don't set update timestamp.
         */
        if ((updt_params.fileclass.when == UPDT_ALWAYS)
            && !ATTR_MASK_TEST(&p_op->fs_attrs, fileclass))
            ATTR_MASK_UNSET(&p_op->fs_attrs, class_update);

        /* nothing changed => noop */
        if (attr_mask_is_null(p_op->fs_attrs.attr_mask))
        {
            /* no op */
            p_op->db_op_type = OP_TYPE_NONE;
        }
        /* something changed in diffmask */
        else if (!attr_mask_is_null(attr_mask_and(&loc_diff_mask, &diff_mask)))
        {
            GString *attrchg = g_string_new("");

            /* attr from DB */
            if (!attr_mask_is_null(display_mask))
                print_attrs(attrchg, &p_op->db_attrs, display_mask, 1);

            printf("-"DFID" %s\n", PFID(&p_op->entry_id), attrchg->str);

            /* attr from FS */
            print_attrs(attrchg, &p_op->fs_attrs, display_mask, 1);
            printf("+"DFID" %s\n", PFID(&p_op->entry_id), attrchg->str);

            g_string_free(attrchg, TRUE);
        }
    }
    else if (!attr_mask_is_null(diff_mask))
    {
        if (p_op->db_op_type == OP_TYPE_INSERT)
        {
            GString *attrnew = g_string_new(NULL);

            print_attrs(attrnew, &p_op->fs_attrs,
                attr_mask_and(&p_op->fs_attrs.attr_mask, &diff_mask), 1);

            printf("++"DFID" %s\n", PFID(&p_op->entry_id), attrnew->str);
            g_string_free(attrnew, TRUE);
        }
        else if ((p_op->db_op_type == OP_TYPE_REMOVE_LAST)
                || (p_op->db_op_type == OP_TYPE_REMOVE_ONE)
                || (p_op->db_op_type == OP_TYPE_SOFT_REMOVE))
        {
            if (ATTR_FSorDB_TEST(p_op, fullpath))
                printf("--"DFID" path=%s\n", PFID(&p_op->entry_id), ATTR_FSorDB(p_op, fullpath));
            else
                printf("--"DFID"\n", PFID(&p_op->entry_id));
        }
    }
    attr_mask_unset_readonly(&p_op->fs_attrs.attr_mask);

    rc = EntryProcessor_Acknowledge(p_op, STAGE_DB_APPLY, false);
    if (rc)
        DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Error acknowledging stage %s",
                   stage_info->stage_name);
    return rc;

#ifdef HAVE_SHOOK
skip_record:
#ifdef HAVE_CHANGELOGS
    if (p_op->extra_info.is_changelog_record)
    /* do nothing on DB but ack the record */
        rc = EntryProcessor_Acknowledge(p_op, STAGE_CHGLOG_CLR, false);
    else
#endif
    /* remove the operation from pipeline */
    rc = EntryProcessor_Acknowledge(p_op, -1, true);

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
    switch (p_op->db_op_type)
    {
    case OP_TYPE_NONE:
        /* noop */
        DisplayLog(LVL_FULL, ENTRYPROC_TAG, "NoOp("DFID")", PFID(&p_op->entry_id));
        rc = 0;
        break;

    case OP_TYPE_INSERT:
        DisplayLog(LVL_FULL, ENTRYPROC_TAG, "Insert("DFID")", PFID(&p_op->entry_id));
        rc = ListMgr_Insert(lmgr, &p_op->entry_id, &p_op->fs_attrs, false);
        break;

    case OP_TYPE_UPDATE:
        DisplayLog(LVL_FULL, ENTRYPROC_TAG, "Update("DFID")", PFID(&p_op->entry_id));
        rc = ListMgr_Update(lmgr, &p_op->entry_id, &p_op->fs_attrs);
        break;

    case OP_TYPE_REMOVE_ONE:
        DisplayLog(LVL_FULL, ENTRYPROC_TAG, "RemoveOne("DFID")", PFID(&p_op->entry_id));
        rc = ListMgr_Remove(lmgr, &p_op->entry_id, &p_op->fs_attrs, false);
        break;

    case OP_TYPE_REMOVE_LAST:
        DisplayLog(LVL_FULL, ENTRYPROC_TAG, "RemoveLast("DFID")", PFID(&p_op->entry_id));
        rc = ListMgr_Remove(lmgr, &p_op->entry_id, &p_op->fs_attrs, true);
        break;

    case OP_TYPE_SOFT_REMOVE:

        if (log_config.debug_level >= LVL_DEBUG)
        {
            attr_mask_t tmp = null_mask;
            attr_mask_t tmp2 = null_mask;
            GString *gs = g_string_new(NULL);

            tmp.std = ATTR_MASK_fullpath | ATTR_MASK_parent_id | ATTR_MASK_name;
            tmp2 = sm_softrm_mask();
            tmp = attr_mask_or(&tmp, &tmp2);

            print_attrs(gs, &p_op->fs_attrs, tmp, true);
            DisplayLog(LVL_DEBUG, ENTRYPROC_TAG, "SoftRemove("DFID",%s)",
                       PFID(&p_op->entry_id), gs->str);
            g_string_free(gs, TRUE);
        }

        /* FIXME get remove time from changelog */
        ATTR_MASK_SET(&p_op->fs_attrs, rm_time);
        ATTR(&p_op->fs_attrs, rm_time) = time(NULL);
        rc = ListMgr_SoftRemove(lmgr, &p_op->entry_id, &p_op->fs_attrs);
        break;

    default:
        DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Unhandled DB operation type: %d", p_op->db_op_type);
        rc = -1;
    }

    if (rc)
        DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Error %d performing database operation: %s.",
                   rc, lmgr_err2str(rc));

    /* Acknowledge the operation if there is a callback */
#ifdef HAVE_CHANGELOGS
    if (p_op->callback_func != NULL)
        rc = EntryProcessor_Acknowledge(p_op, STAGE_CHGLOG_CLR, false);
    else
#endif
        rc = EntryProcessor_Acknowledge(p_op, -1, true);

    if (rc)
        DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage %s.", rc,
                    stage_info->stage_name);

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
        rc = ListMgr_BatchInsert(lmgr, ids, attrs, count, false);
        break;
    case OP_TYPE_UPDATE:
        DisplayLog(LVL_FULL, ENTRYPROC_TAG, "BatchUpdate(%u ops: "DFID"...)",
                   count, PFID(ids[0]));
        rc = ListMgr_BatchInsert(lmgr, ids, attrs, count, true);
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
    if (ops[0]->callback_func != NULL)
        rc = EntryProcessor_AcknowledgeBatch(ops, count, STAGE_CHGLOG_CLR, false);
    else
#endif
        rc = EntryProcessor_AcknowledgeBatch(ops, count, -1, true);

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
        DisplayLog(LVL_FULL, ENTRYPROC_TAG, "stage %s - record #%llu - id="DFID,
                   stage_info->stage_name, logrec->cr_index, PFID(&p_op->entry_id));

    if ( p_op->callback_func )
    {
        /* if operation was committed, Perform callback to info collector */
        rc = p_op->callback_func( lmgr, p_op, p_op->callback_param );

        if ( rc )
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d performing callback at stage %s.", rc,
                        stage_info->stage_name );
    }

    /* Acknowledge the operation and remove it from pipeline */
    rc = EntryProcessor_Acknowledge(p_op, -1, true);
    if (rc)
        DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage %s.", rc,
                   stage_info->stage_name);

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
    if (!attr_mask_is_null(diff_mask))
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
        ListMgr_ForceCommitFlag(lmgr, true);

        /* remove entries listed in previous scans */
        if (has_deletion_policy())
            /* @TODO fix for dirs */
            rc = ListMgr_MassSoftRemove(lmgr, &filter, time(NULL), cb);
        else
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
    ListMgr_ForceCommitFlag(lmgr, false);

    rc = EntryProcessor_Acknowledge(p_op, -1, true);

    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage %s.", rc,
                    stage_info->stage_name );

    return rc;

}
