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

#define diff_arg ((diff_arg_t*)(entry_proc_arg))

/* forward declaration of EntryProc functions of pipeline */
static int  EntryProc_get_fid( struct entry_proc_op_t *, lmgr_t * );
static int  EntryProc_get_info_db( struct entry_proc_op_t *, lmgr_t * );
static int  EntryProc_get_info_fs( struct entry_proc_op_t *, lmgr_t * );
static int  EntryProc_report_diff( struct entry_proc_op_t *, lmgr_t * );
static int  EntryProc_apply( struct entry_proc_op_t *, lmgr_t * );
static int  EntryProc_batch_apply(struct entry_proc_op_t **, int, lmgr_t *);
static int  EntryProc_report_rm( struct entry_proc_op_t *, lmgr_t * );

/* forward declaration to check batchable operations for db_apply stage */
static int  dbop_is_batchable(struct entry_proc_op_t *, struct entry_proc_op_t *, int *);

/* pipeline stages */
#define STAGE_GET_FID       0
#define STAGE_GET_INFO_DB   1
#define STAGE_GET_INFO_FS   2
#define STAGE_REPORT_DIFF   3
#define STAGE_APPLY         4
#define STAGE_REPORT_RM     5 /* special stage at the end of FS scan */

#define PIPELINE_STAGE_COUNT (STAGE_REPORT_RM+1)

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
        !strcmp(ATTR(&p_op->fs_attrs, type), STR_TYPE_DIR))
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
            ATTR( &p_op->fs_attrs, creation_time ) = time(NULL); /* XXX min(atime,mtime,ctime)? */
        }
#endif

#ifdef _LUSTRE
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
        /* only if status is in diff_mask */
        if (entry_proc_conf.diff_mask & ATTR_MASK_status)
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
                p_op->extra_info.not_supp = TRUE;
                p_op->fs_attr_need &= ~ATTR_MASK_status;
            }
        }
#endif

        if (entry_proc_conf.diff_mask & ATTR_MASK_link)
        {
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
        }

        /* get parent_id+name, if not set during scan (eg. for root directory) */
        if (!ATTR_MASK_TEST( &p_op->fs_attrs, name))
            p_op->fs_attr_need |= ATTR_MASK_name;
        if (!ATTR_MASK_TEST( &p_op->fs_attrs, parent_id))
            p_op->fs_attr_need |= ATTR_MASK_parent_id;

#ifdef _LUSTRE
        /* only if stripe is in diff_mask || db_apply */
        if ((entry_proc_conf.diff_mask & (ATTR_MASK_stripe_info | ATTR_MASK_stripe_items))
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

    DisplayLog( LVL_FULL, ENTRYPROC_TAG,
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

    /* scans: never need to get attr (provided in operation) */

#if defined( _LUSTRE ) && defined( _HAVE_FID )
    /* may be needed if parent information is missing */
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
            DisplayLog(LVL_FULL, ENTRYPROC_TAG, "Entry %s no longer exists",
                       path);
             /* entry will be garbage collected at the end of the scan */
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

    /* print diff */
    rc = EntryProcessor_Acknowledge( p_op, STAGE_REPORT_DIFF, FALSE );
    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage.", rc );
    return rc;

#ifdef ATTR_INDEX_status
skip_record:
    /* remove the operation from pipeline */
    rc = EntryProcessor_Acknowledge( p_op, -1, TRUE );

    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage.", rc );
    return rc;
#endif
}


/* report diff and clean unchenged attributes */
int EntryProc_report_diff( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];
    int rc;

#ifdef ATTR_INDEX_creation_time
    /* once set, never change creation time */
    if (p_op->db_op_type != OP_TYPE_INSERT)
        ATTR_MASK_UNSET( &p_op->fs_attrs, creation_time );
#endif
    /* Only keep fields that changed */
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

        /* remove other unchanged attrs or attrs not in db mask */
        p_op->fs_attrs.attr_mask &= (diff_mask | to_keep | ~p_op->db_attrs.attr_mask);

        /* nothing changed => noop */
        if (p_op->fs_attrs.attr_mask == 0)
        {
            /* no op */
            p_op->db_op_type = OP_TYPE_NONE;
        }
        else if ((diff_mask & entry_proc_conf.diff_mask) && (display_mask != 0))
        {
            char attrchg[RBH_PATH_MAX] = "";

            /* revert change: reverse display */
            if (diff_arg->apply == APPLY_FS)
            {
                /* attr from FS */
                PrintAttrs(attrchg, RBH_PATH_MAX, &p_op->fs_attrs, display_mask, 1);
                printf("-"DFID" %s\n", PFID(&p_op->entry_id), attrchg);

                /* attr from DB */
                PrintAttrs(attrchg, RBH_PATH_MAX, &p_op->db_attrs,
                           display_mask, 1);
                printf("+"DFID" %s\n", PFID(&p_op->entry_id), attrchg);
            }
            else
            {
                /* attr from DB */
                PrintAttrs(attrchg, RBH_PATH_MAX, &p_op->db_attrs, display_mask, 1);
                printf("-"DFID" %s\n", PFID(&p_op->entry_id), attrchg);

                /* attr from FS */
                PrintAttrs(attrchg, RBH_PATH_MAX, &p_op->fs_attrs, display_mask, 1);
                printf("+"DFID" %s\n", PFID(&p_op->entry_id), attrchg);
            }
        }
    }
    else if (entry_proc_conf.diff_mask)
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
                char attrnew[RBH_PATH_MAX];
                PrintAttrs(attrnew, RBH_PATH_MAX, &p_op->fs_attrs,
                    p_op->fs_attrs.attr_mask, 1);

                printf("++"DFID" %s\n", PFID(&p_op->entry_id), attrnew);
            }
        }
        else if ((p_op->db_op_type == OP_TYPE_REMOVE_LAST) ||
                 (p_op->db_op_type == OP_TYPE_REMOVE_ONE)
                 || (p_op->db_op_type == OP_TYPE_SOFT_REMOVE))
        {
            /* actually: never happens */

            if (diff_arg->apply == APPLY_FS)
            {
                /* revert change: reverse display */
                char attrnew[RBH_PATH_MAX];
                PrintAttrs(attrnew, RBH_PATH_MAX, &p_op->db_attrs,
                    p_op->db_attrs.attr_mask, 1);

                printf("++"DFID" %s\n", PFID(&p_op->entry_id), attrnew);
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
        p_op->fs_attrs.attr_mask &= ~readonly_attr_set;

    /* always go to APPLY step, at least to tag the entry */
    rc = EntryProcessor_Acknowledge( p_op, STAGE_APPLY, FALSE );
    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error acknowledging stage %s",
                    stage_info->stage_name );
    return rc;
}

/* forward declaration to check batchable operations for db_apply stage */
static int  dbop_is_batchable(struct entry_proc_op_t *first, struct entry_proc_op_t *next, int *full_attr_mask)
{
    /* batch nothing if not applying to DB */
    if ((diff_arg->apply != APPLY_DB) || (pipeline_flags & FLAG_DRY_RUN))
        return FALSE;

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



/**
 * Perform an operation on database.
 */
int EntryProc_apply( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];

    if ((diff_arg->apply == APPLY_DB) && !(pipeline_flags & FLAG_DRY_RUN))
    {
        /* insert to DB */
        switch ( p_op->db_op_type )
        {
        case OP_TYPE_NONE:
            /* noop */
            DisplayLog( LVL_FULL, ENTRYPROC_TAG, "NoOp("DFID")", PFID(&p_op->entry_id) );
            rc = 0;
            break;
        case OP_TYPE_INSERT:
    #ifdef _HAVE_FID
            DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Insert("DFID")", PFID(&p_op->entry_id) );
    #endif
            rc = ListMgr_Insert( lmgr, &p_op->entry_id, &p_op->fs_attrs, FALSE );
            break;
        case OP_TYPE_UPDATE:
    #ifdef _HAVE_FID
            DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Update("DFID")", PFID(&p_op->entry_id) );
    #endif
            rc = ListMgr_Update( lmgr, &p_op->entry_id, &p_op->fs_attrs );
            break;
    case OP_TYPE_REMOVE_ONE:
#ifdef _HAVE_FID
        DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Remove("DFID")", PFID(&p_op->entry_id) );
#endif
        rc = ListMgr_Remove( lmgr, &p_op->entry_id, &p_op->fs_attrs, FALSE );
        break;
    case OP_TYPE_REMOVE_LAST:
#ifdef _HAVE_FID
        DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Remove("DFID")", PFID(&p_op->entry_id) );
#endif
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
                        DisplayReport("%srmdir(%s)", (pipeline_flags & FLAG_DRY_RUN)?"(dry-run) ":"",
                                      ATTR(&p_op->fs_attrs, fullpath));
                        if (!(pipeline_flags & FLAG_DRY_RUN))
                        {
                            if (rmdir(ATTR(&p_op->fs_attrs, fullpath)))
                                DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "rmdir(%s) failed: %s",
                                    ATTR(&p_op->fs_attrs, fullpath), strerror(errno));
                        }
                    }
                    else
                    {
                        /* unlink */
                        DisplayReport("%sunlink(%s)", (pipeline_flags & FLAG_DRY_RUN)?"(dry-run) ":"",
                                      ATTR(&p_op->fs_attrs, fullpath));
                        if (!(pipeline_flags & FLAG_DRY_RUN))
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
                /*attributes to be changed: p_op->db_attrs.attr_mask & p_op->fs_attrs.attr_mask & entry_proc_conf.diff_mask */
                rc = ApplyAttrs(&p_op->entry_id, &p_op->db_attrs, &p_op->fs_attrs,
                                p_op->db_attrs.attr_mask & p_op->fs_attrs.attr_mask & entry_proc_conf.diff_mask,
                                pipeline_flags & FLAG_DRY_RUN);
                break;


            default:
                /* no attr update: insert or remove */
                ;
        }
    }

    rc = EntryProcessor_Acknowledge( p_op, -1, TRUE );
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

    rc = EntryProcessor_AcknowledgeBatch(ops, count, -1, TRUE);
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
    st = rbhext_recover(p_id, p_oldattr, &new_id, &new_attrs, NULL);
    switch (st)
    {
        case RS_FILE_OK:
        case RS_FILE_EMPTY:
        case RS_NON_FILE:
        case RS_FILE_DELTA:

            new_attrs.attr_mask &= ~readonly_attr_set;
            rc = ListMgr_Replace(lmgr, p_id, p_oldattr, &new_id, &new_attrs,
                                 TRUE, TRUE);
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

    rc = create_from_attrs(p_oldattr, &new_attrs, &new_id, FALSE, FALSE);
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
    new_attrs.attr_mask &= ~readonly_attr_set;
    rc = ListMgr_Replace(lmgr, p_id, p_oldattr, &new_id, &new_attrs,
                         TRUE, TRUE);
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
    if (entry_proc_conf.diff_mask)
        cb = no_tag_cb;

    /* If gc_entries or gc_names are not set,
     * this is just a special op to wait for pipeline flush.
     * => don't clean old entries */
    if (p_op->gc_entries || p_op->gc_names)
    {
        /* call MassRemove only if APPLY_DB is set */
        if ((diff_arg->apply == APPLY_DB) && !(pipeline_flags & FLAG_DRY_RUN))
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
            ListMgr_ForceCommitFlag( lmgr, TRUE );

            /* remove entries listed in previous scans */
        #ifdef HAVE_RM_POLICY
            if (policies.unlink_policy.hsm_remove)
                /* @TODO fix for dirs, symlinks, ... */
                rc = ListMgr_MassSoftRemove(lmgr, &filter,
                         time(NULL) + policies.unlink_policy.deferred_remove_delay, cb);
            else
        #endif
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
                int getattr_mask;

                if (diff_arg->apply == APPLY_FS)
                    getattr_mask = 0xFFFFFFFF; /* all possible info */
                else
                    getattr_mask = ATTR_MASK_fullpath;

                attrs.attr_mask = getattr_mask;
                while ((rc = ListMgr_GetNext(it, &id, &attrs )) == DB_SUCCESS)
                {
                    if (diff_arg->apply == APPLY_FS)
                    {
                        /* FS apply: reverse display */
                        char attrnew[RBH_PATH_MAX];
                        PrintAttrs(attrnew, RBH_PATH_MAX, &attrs, 0, 1);

                        printf("++"DFID" %s\n", PFID(&id), attrnew);
                        /* create or recover it (even without HSM mode) */
#ifdef _HSM_LITE
                        if (diff_arg->recov_from_backend)
                        {
                            /* try to recover the entry from the backend */
                            DisplayReport("%srecover(%s)", (pipeline_flags & FLAG_DRY_RUN)?"(dry-run) ":"",
                                          ATTR(&attrs, fullpath));
                            if (!(pipeline_flags & FLAG_DRY_RUN))
                                hsm_recover(lmgr, &id, &attrs);
                        }
                        else
#endif
                        {
                            /* create the file with no stripe and generate lovea information to be set on MDT */
                            DisplayReport("%screate(%s)", (pipeline_flags & FLAG_DRY_RUN)?"(dry-run) ":"",
                                          ATTR(&attrs, fullpath));
                            if (!(pipeline_flags & FLAG_DRY_RUN))
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
    ListMgr_ForceCommitFlag( lmgr, FALSE );

    rc = EntryProcessor_Acknowledge( p_op, -1, TRUE );

    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage %s.", rc,
                    stage_info->stage_name );

    return rc;

}
