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
#include "status_manager.h"
#include <errno.h>
#include <time.h>
#include <unistd.h>

#define ERR_MISSING(_err) (((_err)==ENOENT)||((_err)==ESTALE))

#define diff_arg ((diff_arg_t*)entry_proc_arg)
#define diff_mask (diff_arg->diff_mask)

/* forward declaration of EntryProc functions of pipeline */
static int  EntryProc_get_fid( struct entry_proc_op_t *, lmgr_t * );
static int  EntryProc_get_info_db( struct entry_proc_op_t *, lmgr_t * );
static int  EntryProc_get_info_fs( struct entry_proc_op_t *, lmgr_t * );
static int  EntryProc_report_diff( struct entry_proc_op_t *, lmgr_t * );
static int  EntryProc_apply( struct entry_proc_op_t *, lmgr_t * );
static int  EntryProc_batch_apply(struct entry_proc_op_t **, int, lmgr_t *);
static int  EntryProc_report_rm( struct entry_proc_op_t *, lmgr_t * );

/* forward declaration to check batchable operations for db_apply stage */
static bool dbop_is_batchable(struct entry_proc_op_t *, struct entry_proc_op_t *, attr_mask_t *);

/* pipeline stages */
enum {
    STAGE_GET_FID = 0,
    STAGE_GET_INFO_DB,
    STAGE_GET_INFO_FS,
    STAGE_REPORT_DIFF,
    STAGE_APPLY,
    STAGE_REPORT_RM,

    PIPELINE_STAGE_COUNT /* keep it at last */
};

const pipeline_descr_t diff_pipeline_descr =
{
    .stage_count    = PIPELINE_STAGE_COUNT,
    .GET_ID         = STAGE_GET_FID,
    .GET_INFO_DB    = STAGE_GET_INFO_DB,
    .GET_INFO_FS    = STAGE_GET_INFO_FS,
    .GC_OLDENT      = STAGE_REPORT_RM,
    .DB_APPLY       = STAGE_APPLY
};

/** pipeline stages definition */
pipeline_stage_t diff_pipeline[] = {
    {STAGE_GET_FID, "STAGE_GET_FID", EntryProc_get_fid, NULL, NULL,
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC, 0},
    {STAGE_GET_INFO_DB, "STAGE_GET_INFO_DB", EntryProc_get_info_db, NULL, NULL,
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC | STAGE_FLAG_ID_CONSTRAINT, 0},
    {STAGE_GET_INFO_FS, "STAGE_GET_INFO_FS", EntryProc_get_info_fs, NULL, NULL,
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC, 0},
    /* must be sequential to avoid line interlacing */
    {STAGE_REPORT_DIFF, "STAGE_REPORT_DIFF", EntryProc_report_diff, NULL, NULL,
     STAGE_FLAG_SEQUENTIAL | STAGE_FLAG_SYNC, 1},
    {STAGE_APPLY, "STAGE_APPLY", EntryProc_apply,
        EntryProc_batch_apply, dbop_is_batchable, /* batched ops management */
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC, 0},
    /* this step is for displaying removed entries when
     * starting/ending a FS scan. */
    {STAGE_REPORT_RM, "STAGE_REPORT_RM", EntryProc_report_rm, NULL, NULL,
     STAGE_FLAG_SEQUENTIAL | STAGE_FLAG_SYNC, 1}
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
    uint32_t status_scope = 0; /* status mask only */
    attr_mask_t tmp;

    const pipeline_stage_t *stage_info =
        &entry_proc_pipeline[p_op->pipeline_stage];

    /* check if entry is in policies scope */
    add_matching_scopes_mask(&p_op->entry_id, &p_op->fs_attrs, true,
                             &status_scope);

    /* XXX also retrieve needed attributes to check the scope? */

    /* get diff attributes from DB and FS (to allow comparison) */
    p_op->db_attr_need = attr_mask_or(&p_op->db_attr_need, &diff_mask);

    tmp = attr_mask_and_not(&diff_mask, &p_op->fs_attrs.attr_mask);
    p_op->fs_attr_need = attr_mask_or(&p_op->fs_attr_need, &tmp);

    if (entry_proc_conf.detect_fake_mtime)
        attr_mask_set_index(&p_op->db_attr_need, ATTR_INDEX_creation_time);

    attr_allow_cached = attrs_for_status_mask(status_scope, false);
    attr_need_fresh = attrs_for_status_mask(status_scope, true);
    /* XXX check if entry is in policy scope? */

    /* what must be retrieved from DB: */
    tmp = attr_mask_and_not(&attr_allow_cached, &p_op->fs_attrs.attr_mask);
    p_op->db_attr_need = attr_mask_or(&p_op->db_attr_need, &tmp);

    /* no dircount for non-dirs */
    if (ATTR_MASK_TEST(&p_op->fs_attrs, type) &&
        !strcmp(ATTR(&p_op->fs_attrs, type), STR_TYPE_DIR))
    {
        attr_mask_unset_index(&p_op->db_attr_need, ATTR_INDEX_dircount);
    }

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

    if (!attr_mask_is_null(p_op->db_attr_need))
    {
        p_op->db_attrs.attr_mask = p_op->db_attr_need;
        rc = ListMgr_Get(lmgr, &p_op->entry_id, &p_op->db_attrs);

        if (rc == DB_SUCCESS)
        {
            p_op->db_exists = 1;
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

    /* get status for all policies with a matching scope */
    add_matching_scopes_mask(&p_op->entry_id, &p_op->fs_attrs, true,
                             &p_op->fs_attr_need.status);
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
            ATTR( &p_op->fs_attrs, creation_time ) = time(NULL); /* XXX min(atime,mtime,ctime)? */
        }

#ifdef _LUSTRE
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

#ifdef ATTR_INDEX_status /** @FIXME RBHv3 drop old-style status reference */
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
            p_op->extra_info.not_supp = 1;
            p_op->fs_attr_need &= ~ATTR_MASK_status;
        }
#endif
        next_stage = STAGE_GET_INFO_FS;
    }
    else
    {
        p_op->db_op_type = OP_TYPE_UPDATE;

#ifdef ATTR_INDEX_status /** @FIXME RBHv3 drop old-style status reference */
        /* only if status is in diff_mask */
        if (diff_mask & ATTR_MASK_status)
        {
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
                p_op->extra_info.not_supp = 1;
                p_op->fs_attr_need &= ~ATTR_MASK_status;
            }
        }
#endif

        if (attr_mask_test_index(&diff_mask, ATTR_INDEX_link))
        {
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
        }

        /* get parent_id+name, if not set during scan (eg. for root directory) */
        if (!ATTR_MASK_TEST( &p_op->fs_attrs, name))
            attr_mask_set_index(&p_op->fs_attr_need, ATTR_INDEX_name);
        if (!ATTR_MASK_TEST( &p_op->fs_attrs, parent_id))
            attr_mask_set_index(&p_op->fs_attr_need, ATTR_INDEX_parent_id);

#ifdef _LUSTRE
        /* only if stripe is in diff_mask || db_apply */
        if ((diff_mask.std & (ATTR_MASK_stripe_info | ATTR_MASK_stripe_items))
            || (diff_arg->apply == APPLY_DB))
        {
            /* get stripe only for files */
            if ( ATTR_MASK_TEST( &p_op->fs_attrs, type )
                 && !strcmp(ATTR( &p_op->fs_attrs, type ), STR_TYPE_FILE)
                 && !strcmp( global_config.fs_type, "lustre" ))
            {
                check_stripe_info(p_op, lmgr);
            }
        }
#endif
        next_stage = STAGE_GET_INFO_FS;
    }

    if (next_stage == -1)
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



int EntryProc_get_info_fs( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;
    char           tmp_buf[RBH_NAME_MAX];

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

    /* scans: never need to get attr (provided in operation) */

#if defined( _LUSTRE ) && defined( _HAVE_FID )
    /* may be needed if parent information is missing */
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
        /** attrs from FS+DB */
        attr_set_t merged_attrs = ATTR_SET_INIT;
        /** attributes + status */
        attr_set_t new_attrs = ATTR_SET_INIT;

        ListMgr_MergeAttrSets(&merged_attrs, &p_op->fs_attrs, 1);
        ListMgr_MergeAttrSets(&merged_attrs, &p_op->db_attrs, 0);

        /* match policy scopes according to newly set information:
         * remove needed status from mask and append the updated one. */
        p_op->fs_attr_need.status &= ~all_status_mask();
        /* FIXME this fails if scope attributes are missing */
        add_matching_scopes_mask(&p_op->entry_id, &merged_attrs, false,
                                 &p_op->fs_attr_need.status);

        i = 0;
        while ((smi = get_sm_instance(i)) != NULL)
        {
            ATTR_MASK_INIT(&new_attrs);

            if (NEED_GETSTATUS(p_op, i))
            {
                if (smi->sm->get_status_func != NULL)
                {
                    /* this also check if entry is ignored for this policy */
                    rc =  smi->sm->get_status_func(smi, &p_op->entry_id,
                                                   &merged_attrs, &new_attrs);
                    if (ERR_MISSING(abs(rc)))
                    {
                        DisplayLog(LVL_DEBUG, ENTRYPROC_TAG, "Entry %s no longer exists",
                                   path);
                        /* changelog: an UNLINK event will be raised, so we ignore current record
                         * scan: entry will be garbage collected at the end of the scan */
                        goto skip_record;
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

                        /** @TODO RBHv3
                         * manage no_archive, no_release
                         * manage last_archive, last_restore (init: 0 if status is 'new')
                         * if entry is not supported: set extra_info.not_supp
                         */
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

    /* print diff */
    rc = EntryProcessor_Acknowledge(p_op, STAGE_REPORT_DIFF, false);
    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage.", rc );
    return rc;

skip_record:
    /* remove the operation from pipeline */
    rc = EntryProcessor_Acknowledge(p_op, -1, true);

    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage.", rc );
    return rc;
}


/* report diff and clean unchenged attributes */
int EntryProc_report_diff( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];
    int rc;

    /* once set, never change creation time */
    if (p_op->db_op_type != OP_TYPE_INSERT)
        ATTR_MASK_UNSET( &p_op->fs_attrs, creation_time );

    /* Only keep fields that changed */
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

        /* remove other unchanged attrs or attrs not in db mask */
        tmp = attr_mask_or(&loc_diff_mask, &to_keep);
        tmp = attr_mask_or_not(&tmp, &p_op->db_attrs.attr_mask);
        p_op->fs_attrs.attr_mask = attr_mask_and(&p_op->fs_attrs.attr_mask, &tmp);

        /* nothing changed => noop */
        if (attr_mask_is_null(p_op->fs_attrs.attr_mask))
        {
            /* no op */
            p_op->db_op_type = OP_TYPE_NONE;
        }
        else if (!attr_mask_is_null(attr_mask_and(&loc_diff_mask, &diff_mask))
                 && !attr_mask_is_null(display_mask))
        {
            GString *attrchg = g_string_new(NULL);

            /* revert change: reverse display */
            if (diff_arg->apply == APPLY_FS)
            {
                /* attr from FS */
                print_attrs(attrchg, &p_op->fs_attrs, display_mask, 1);
                printf("-"DFID" %s\n", PFID(&p_op->entry_id), attrchg->str);

                /* attr from DB */
                print_attrs(attrchg, &p_op->db_attrs, display_mask, 1);
                printf("+"DFID" %s\n", PFID(&p_op->entry_id), attrchg->str);
            }
            else
            {
                /* attr from DB */
                print_attrs(attrchg, &p_op->db_attrs, display_mask, 1);
                printf("-"DFID" %s\n", PFID(&p_op->entry_id), attrchg->str);

                /* attr from FS */
                print_attrs(attrchg, &p_op->fs_attrs, display_mask, 1);
                printf("+"DFID" %s\n", PFID(&p_op->entry_id), attrchg->str);
            }
            g_string_free(attrchg, TRUE);
        }
    }
    else if (!attr_mask_is_null(diff_mask))
    {
        if (p_op->db_op_type == OP_TYPE_INSERT)
        {
            if (diff_arg->apply == APPLY_FS)
            {
                /* revert change: reverse display */
                if (ATTR_FSorDB_TEST(p_op, fullpath))
                    printf("--"DFID" path=%s\n", PFID(&p_op->entry_id), ATTR_FSorDB(p_op, fullpath));
                else
                    printf("--"DFID"\n", PFID(&p_op->entry_id));
            }
            else
            {
                GString *attrnew = g_string_new(NULL);

                print_attrs(attrnew, &p_op->fs_attrs, p_op->fs_attrs.attr_mask,
                            1);
                printf("++"DFID" %s\n", PFID(&p_op->entry_id), attrnew->str);

                g_string_free(attrnew, TRUE);
            }
        }
        else if ((p_op->db_op_type == OP_TYPE_REMOVE_LAST) ||
                 (p_op->db_op_type == OP_TYPE_REMOVE_ONE)
                 || (p_op->db_op_type == OP_TYPE_SOFT_REMOVE))
        {
            /* actually: never happens */

            if (diff_arg->apply == APPLY_FS)
            {
                GString *attrnew = g_string_new(NULL);

                /* revert change: reverse display */
                print_attrs(attrnew, &p_op->db_attrs, p_op->db_attrs.attr_mask,
                            1);
                printf("++"DFID" %s\n", PFID(&p_op->entry_id), attrnew->str);

                g_string_free(attrnew, TRUE);
            }
            else
            {
                if (ATTR_FSorDB_TEST(p_op, fullpath))
                    printf("--"DFID" path=%s\n", PFID(&p_op->entry_id), ATTR_FSorDB(p_op, fullpath));
                else
                    printf("--"DFID"\n", PFID(&p_op->entry_id));
            }
        }
    }

    if (diff_arg->apply == APPLY_DB)
        attr_mask_unset_readonly(&p_op->fs_attrs.attr_mask);

    /* always go to APPLY step, at least to tag the entry */
    rc = EntryProcessor_Acknowledge(p_op, STAGE_APPLY, false);
    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error acknowledging stage %s",
                    stage_info->stage_name );
    return rc;
}

/* forward declaration to check batchable operations for db_apply stage */
static bool dbop_is_batchable(struct entry_proc_op_t *first, struct
                              entry_proc_op_t *next, attr_mask_t *full_attr_mask)
{
    /* batch nothing if not applying to DB */
    if ((diff_arg->apply != APPLY_DB) || (pipeline_flags & RUNFLG_DRY_RUN))
        return false;

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
        *full_attr_mask = attr_mask_or(full_attr_mask, &next->fs_attrs.attr_mask);
        return true;
    }
    else
        return false;
}



/**
 * Perform an operation on database.
 */
int EntryProc_apply( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];

    if ((diff_arg->apply == APPLY_DB) && !(pipeline_flags & RUNFLG_DRY_RUN))
    {
        /* insert to DB */
        switch (p_op->db_op_type)
        {
        case OP_TYPE_NONE:
            /* noop */
            DisplayLog(LVL_FULL, ENTRYPROC_TAG, "NoOp("DFID")", PFID(&p_op->entry_id));
            rc = 0;
            break;

        case OP_TYPE_INSERT:
    #ifdef _HAVE_FID
            DisplayLog(LVL_FULL, ENTRYPROC_TAG, "Insert("DFID")", PFID(&p_op->entry_id));
    #endif
            rc = ListMgr_Insert(lmgr, &p_op->entry_id, &p_op->fs_attrs, false);
            break;

        case OP_TYPE_UPDATE:
    #ifdef _HAVE_FID
            DisplayLog(LVL_FULL, ENTRYPROC_TAG, "Update("DFID")", PFID(&p_op->entry_id));
    #endif
            rc = ListMgr_Update(lmgr, &p_op->entry_id, &p_op->fs_attrs);
            break;

        case OP_TYPE_REMOVE_ONE:
    #ifdef _HAVE_FID
            DisplayLog(LVL_FULL, ENTRYPROC_TAG, "Remove("DFID")", PFID(&p_op->entry_id));
    #endif
            rc = ListMgr_Remove(lmgr, &p_op->entry_id, &p_op->fs_attrs, false);
            break;

        case OP_TYPE_REMOVE_LAST:
    #ifdef _HAVE_FID
            DisplayLog(LVL_FULL, ENTRYPROC_TAG, "Remove("DFID")", PFID(&p_op->entry_id));
    #endif
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
    }
    else if (diff_arg->db_tag)
    {
        /* tag the entry in the DB */
        rc = ListMgr_TagEntry(lmgr, diff_arg->db_tag, &p_op->entry_id);
        if ( rc )
            DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Error %d performing database operation: %s.",
                       rc, lmgr_err2str(rc));
    }

    if (diff_arg->apply == APPLY_FS)
    {
        attr_mask_t tmp;

        /* all changes must be reverted. So, insert=>rm, rm=>create, ... */
        /* FIXME as this step is parallel, how to manage file creation while
         * parent directory is not created?
         * Same issue for unlink & rmdir */
        switch ( p_op->db_op_type )
        {
            case OP_TYPE_INSERT:

#ifdef _HAVE_FID
                /* if fullpath is not set, but parent and name are set, use parent/name
                 * as the fullpath (for fids only) */
                if (!ATTR_MASK_TEST(&p_op->fs_attrs, fullpath)
                    && ATTR_MASK_TEST(&p_op->fs_attrs, parent_id)
                    && ATTR_MASK_TEST(&p_op->fs_attrs, name))
                {
                    char *str = ATTR(&p_op->fs_attrs, fullpath);
                    BuildFidPath(&ATTR(&p_op->fs_attrs, parent_id), str);
                    long len = strlen(str);
                    sprintf(str+len, "/%s", ATTR(&p_op->fs_attrs, name));
                    ATTR_MASK_SET(&p_op->fs_attrs, fullpath);
                }
#endif

                /* unlink or rmdir */
                if ( ATTR_MASK_TEST(&p_op->fs_attrs, type)
                     &&  ATTR_MASK_TEST(&p_op->fs_attrs, fullpath) )
                {
                    if (!strcmp(ATTR(&p_op->fs_attrs, type), STR_TYPE_DIR))
                    {
                        /* rmdir */
                        DisplayReport("%srmdir(%s)",
                                      (pipeline_flags & RUNFLG_DRY_RUN)?"(dry-run) ":"",
                                      ATTR(&p_op->fs_attrs, fullpath));
                        if (!(pipeline_flags & RUNFLG_DRY_RUN))
                        {
                            if (rmdir(ATTR(&p_op->fs_attrs, fullpath)))
                                DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "rmdir(%s) failed: %s",
                                    ATTR(&p_op->fs_attrs, fullpath), strerror(errno));
                        }
                    }
                    else
                    {
                        /* unlink */
                        DisplayReport("%sunlink(%s)", (pipeline_flags & RUNFLG_DRY_RUN)?"(dry-run) ":"",
                                      ATTR(&p_op->fs_attrs, fullpath));
                        if (!(pipeline_flags & RUNFLG_DRY_RUN))
                        {
                            if (unlink(ATTR(&p_op->fs_attrs, fullpath)))
                                DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "unlink(%s) failed: %s",
                                    ATTR(&p_op->fs_attrs, fullpath), strerror(errno));
                        }
                    }
                }
                else
                {
                    DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Cannot remove entry: type or path is unknown" );
                }
                break;
            case OP_TYPE_UPDATE:
                tmp = attr_mask_and(&p_op->db_attrs.attr_mask, &p_op->fs_attrs.attr_mask);
                tmp = attr_mask_and(&tmp, &diff_mask);

                /*attributes to be changed: p_op->db_attrs.attr_mask & p_op->fs_attrs.attr_mask & diff_mask */
                rc = ApplyAttrs(&p_op->entry_id, &p_op->db_attrs, &p_op->fs_attrs, tmp,
                                pipeline_flags & RUNFLG_DRY_RUN);
                break;


            default:
                /* no attr update: insert or remove */
                ;
        }
    }

    rc = EntryProcessor_Acknowledge(p_op, -1, true);
    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage %s.", rc,
                    stage_info->stage_name );
    return rc;
}

/**
 * Perform a batch of operations on the database.
 */
int EntryProc_batch_apply(struct entry_proc_op_t **ops, int count,
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

    rc = EntryProcessor_AcknowledgeBatch(ops, count, -1, true);
    if (rc)
        DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage %s.", rc,
                   stage_info->stage_name);

    MemFree(attrs);
free_ids:
    MemFree(ids);
    return rc;
}

/* called for each untagged entry */
static void no_tag_cb(const entry_id_t *p_id)
{
    if (diff_arg->apply == APPLY_FS)
        /* XXX no rm callback is supposed to be called for FS apply */
        printf("++"DFID"\n", PFID(p_id));
    else
        printf("--"DFID"\n", PFID(p_id));
}

#ifdef _HSM_LITE
static int hsm_recover(lmgr_t * lmgr,
                       entry_id_t *p_id,
                       attr_set_t *p_oldattr)
{
    recov_status_t st;
    entry_id_t new_id;
    attr_set_t new_attrs;
    int rc;
    const char * status_str;

    /* try to recover from backend */

    /** FIXME use undelete function from a status manager */
    st = RS_ERROR;
    //st = rbhext_recover(p_id, p_oldattr, &new_id, &new_attrs, NULL);
    switch (st)
    {
        case RS_FILE_OK:
        case RS_FILE_EMPTY:
        case RS_NON_FILE:
        case RS_FILE_DELTA:

            attr_mask_unset_readonly(&new_attrs.attr_mask);
            rc = ListMgr_Replace(lmgr, p_id, p_oldattr, &new_id, &new_attrs,
                                 true, true);
            if (rc)
            {
                DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Failed to replace entry "
                           DFID" with "DFID" (%s) in DB.",
                           PFID(p_id), PFID(&new_id), ATTR(&new_attrs, fullpath));
                goto clean_entry;
            }

            status_str = "?";
            if (st == RS_FILE_OK)
                status_str = "up-to-date file";
            else if (st == RS_FILE_EMPTY)
                status_str = "empty file";
            else if (st == RS_FILE_DELTA)
                status_str = "old file data";
            else if (st == RS_NON_FILE)
                status_str = "non-file";

            DisplayReport("%s successfully recovered (%s)", ATTR(&new_attrs, fullpath), status_str);
            return 0;

        case RS_NOBACKUP:
            DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "No backup available for entry '%s'",
                       ATTR(p_oldattr, fullpath));
            goto clean_entry;
        case RS_ERROR:
        default:
            DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Failed to restore entry '%s' (status=%d)",
                       ATTR(p_oldattr, fullpath), st);
            goto clean_entry;
    }

clean_entry:
    /* clean new entry (inconsistent) */
    if (!strcmp(ATTR(p_oldattr, type), STR_TYPE_DIR))
        rc = rmdir(ATTR(p_oldattr, fullpath));
    else
        rc = unlink(ATTR(p_oldattr, fullpath));
    if (rc)
        DisplayLog(LVL_EVENT, ENTRYPROC_TAG, "cleanup: unlink/rmdir failed: %s",
                   strerror(errno));

    /* failure */
    return -1;
}
#endif

static int std_recover(lmgr_t * lmgr,
                       entry_id_t *p_id,
                       attr_set_t *p_oldattr)
{
    entry_id_t new_id;
    attr_set_t new_attrs;
    int rc;

    rc = create_from_attrs(p_oldattr, &new_attrs, &new_id, false, false);
    if (rc)
    {
        DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Failed to create entry '%s' (status=%d)",
                       ATTR(p_oldattr, fullpath), rc);
        goto clean_entry;
    }

#ifdef _LUSTRE
#ifndef _MDT_SPECIFIC_LOVEA
    if (diff_arg->lovea_file)
    {
        if (!ATTR_MASK_TEST(&new_attrs, fullpath))
        {
            DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Fullpath needed to write into lovea_file");
        }
        else
        {
            /* associate old stripe objects to new object id */
            char buff[4096];
            ssize_t sz = BuildLovEA(&new_id, p_oldattr, buff, 4096);
            if (sz > 0)
            {
                int i;
                char output[4096];
                char relpath[RBH_PATH_MAX];
                char * curr = output;

                if (relative_path(ATTR(&new_attrs, fullpath), global_config.fs_path,
                              relpath) == 0)
                {

                    /* write as a single line to avoid mixing them */
                    curr += sprintf(curr, "%s ", relpath);

                    /* write output for set_lovea tool */
                    for (i = 0 ; i < sz; i++ )
                        curr += sprintf(curr, "%02hhx", buff[i]);
                    sprintf(curr, "\n");

                    fprintf(diff_arg->lovea_file, "%s", output);

                    /* XXX overwrite stripe info in new attrs? */
        //            ATTR(&new_attrs, stripe_info) = ATTR(p_oldattr, stripe_info);
        //            ATTR(&new_attrs, stripe_items) = ATTR(p_oldattr, stripe_items);
                }
            }
        }
    }
    if (diff_arg->fid_remap_file)
    {
        /* print for each stripe: ost index, stripe_number, object id, old fid, new fid */
        if (ATTR_MASK_TEST(p_oldattr, stripe_items))
        {
            int i;
            stripe_items_t *pstripe = &ATTR(p_oldattr, stripe_items);
            for (i = 0; i < pstripe->count; i++)
            {
                fprintf(diff_arg->fid_remap_file, "%u %u %"PRIu64" "DFID" "DFID"\n",
                        pstripe->stripe[i].ost_idx, i,
                        pstripe->stripe[i].obj_id,
                        PFID(p_id), PFID(&new_id));
            }
        }
    }
#endif
#endif

    /* insert the new entry to the DB */
    attr_mask_unset_readonly(&new_attrs.attr_mask);
    rc = ListMgr_Replace(lmgr, p_id, p_oldattr, &new_id, &new_attrs,
                         true, true);
    if (rc)
    {
        DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Failed to replace entry "
                   DFID" with "DFID" (%s) in DB.",
                   PFID(p_id), PFID(&new_id), ATTR(&new_attrs, fullpath));
        goto clean_entry;
    }

    return 0;

clean_entry:
    /* clean new entry (inconsistent) */
    if (!strcmp(ATTR(p_oldattr, type), STR_TYPE_DIR))
        rc = rmdir(ATTR(p_oldattr, fullpath));
    else
        rc = unlink(ATTR(p_oldattr, fullpath));
    if (rc)
        DisplayLog(LVL_EVENT, ENTRYPROC_TAG, "cleanup: unlink/rmdir failed: %s",
                   strerror(errno));

    /* failure */
    return -1;
}


static int EntryProc_report_rm(struct entry_proc_op_t *p_op, lmgr_t * lmgr)
{
    int            rc;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];
    lmgr_filter_t  filter;
    filter_value_t val;
    rm_cb_func_t cb = NULL;

    /* callback func for diff display */
    if (!attr_mask_is_null(diff_mask))
        cb = no_tag_cb;

    /* If gc_entries or gc_names are not set,
     * this is just a special op to wait for pipeline flush.
     * => don't clean old entries */
    if (p_op->gc_entries || p_op->gc_names)
    {
        /* call MassRemove only if APPLY_DB is set */
        if ((diff_arg->apply == APPLY_DB) && !(pipeline_flags & RUNFLG_DRY_RUN))
        {
            lmgr_simple_filter_init(&filter);

            if (p_op->gc_entries)
            {
                val.value.val_uint = ATTR(&p_op->fs_attrs, md_update);
                lmgr_simple_filter_add(&filter, ATTR_INDEX_md_update,
                                       LESSTHAN_STRICT, val, 0);
            }

            if (p_op->gc_names)
            {
                /* use the same timestamp for cleaning paths that have not been seen during the scan */
                val.value.val_uint = ATTR(&p_op->fs_attrs, md_update);
                lmgr_simple_filter_add(&filter, ATTR_INDEX_path_update,
                                       LESSTHAN_STRICT, val, 0);
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
                /* @TODO fix for dirs, symlinks, ... */
                rc = ListMgr_MassSoftRemove(lmgr, &filter, time(NULL), cb);
            else
                rc = ListMgr_MassRemove(lmgr, &filter, cb);

            /* /!\ TODO : entries must be removed from backend too */

            lmgr_simple_filter_free( &filter );

            if ( rc )
                DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                            "Error: ListMgr MassRemove operation failed with code %d.", rc );
        }
        else if (diff_arg->db_tag)
        {
            /* list untagged entries (likely removed from filesystem) */
            struct lmgr_iterator_t *it;
            entry_id_t id;
            attr_set_t attrs;

            it = ListMgr_ListUntagged(lmgr, diff_arg->db_tag, NULL);

            if (it == NULL)
            {
                DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                            "Error: ListMgr_ListUntagged operation failed." );
            }
            else
            {
                attr_mask_t getattr_mask = {0};

                if (diff_arg->apply == APPLY_FS)
                {
                    /* all possible info */
                    getattr_mask.std = ~0;
                    getattr_mask.status = ~0;
                    getattr_mask.sm_info = ~0LL;
                }
                else
                    getattr_mask.std = ATTR_MASK_fullpath;

                attrs.attr_mask = getattr_mask;
                while ((rc = ListMgr_GetNext(it, &id, &attrs )) == DB_SUCCESS)
                {
                    if (diff_arg->apply == APPLY_FS)
                    {
                        GString *attrnew = g_string_new(NULL);

                        /* FS apply: reverse display */
                        print_attrs(attrnew, &attrs, null_mask, 1);
                        printf("++"DFID" %s\n", PFID(&id), attrnew->str);

                        g_string_free(attrnew, TRUE);

                        /* create or recover it (even without HSM mode) */
#ifdef _HSM_LITE
                        if (diff_arg->recov_from_backend)
                        {
                            /* try to recover the entry from the backend */
                            DisplayReport("%srecover(%s)",
                                          (pipeline_flags & RUNFLG_DRY_RUN)?"(dry-run) ":"",
                                          ATTR(&attrs, fullpath));
                            /** FIXME use undelete function from status manager */
                            if (!(pipeline_flags & RUNFLG_DRY_RUN))
                                hsm_recover(lmgr, &id, &attrs);
                        }
                        else
#endif
                        {
                            /* create the file with no stripe and generate lovea information to be set on MDT */
                            DisplayReport("%screate(%s)",
                                          (pipeline_flags & RUNFLG_DRY_RUN)?"(dry-run) ":"",
                                          ATTR(&attrs, fullpath));
                            if (!(pipeline_flags & RUNFLG_DRY_RUN))
                                std_recover(lmgr, &id, &attrs);
                        }
                    }
                    else /* apply=db */
                    {
                        if (ATTR_MASK_TEST(&attrs, fullpath))
                            printf("--"DFID" path=%s\n", PFID(&id), ATTR(&attrs, fullpath));
                        else
                            printf("--"DFID"\n", PFID(&id));
                    }

                    ListMgr_FreeAttrs( &attrs );

                    /* prepare next call */
                    attrs.attr_mask = getattr_mask;
                }

                ListMgr_CloseIterator( it );
            }

            /* can now destroy the tag */
            rc = ListMgr_DestroyTag(lmgr, diff_arg->db_tag);
            if (rc)
                DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error: ListMgr_DestroyTag operation failed (rc=%d)", rc);
        }
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
