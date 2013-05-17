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
static int  EntryProc_report_rm( struct entry_proc_op_t *, lmgr_t * );

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
    .GC_OLDENT      = STAGE_REPORT_RM
};

/** pipeline stages definition */
pipeline_stage_t diff_pipeline[] = {
    {STAGE_GET_FID, "STAGE_GET_FID", EntryProc_get_fid,
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC, 0},
    {STAGE_GET_INFO_DB, "STAGE_GET_INFO_DB", EntryProc_get_info_db,
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC | STAGE_FLAG_ID_CONSTRAINT, 0},
    {STAGE_GET_INFO_FS, "STAGE_GET_INFO_FS", EntryProc_get_info_fs,
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC, 0},
    /* must be sequential to avoid line interlacing */
    {STAGE_REPORT_DIFF, "STAGE_REPORT_DIFF", EntryProc_report_diff,
     STAGE_FLAG_SEQUENTIAL | STAGE_FLAG_SYNC, 1},
    {STAGE_APPLY, "STAGE_APPLY", EntryProc_apply,
     STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC, 0},
    /* this step is for displaying removed entries when
     * starting/ending a FS scan. */
    {STAGE_REPORT_RM, "STAGE_REPORT_RM", EntryProc_report_rm,
     STAGE_FLAG_SEQUENTIAL | STAGE_FLAG_SYNC, 1}
};

#ifdef HAVE_SHOOK
static int shook_special_obj( struct entry_proc_op_t *p_op )
{
    if (ATTR_FSorDB_TEST( p_op, fullpath )
        && ATTR_FSorDB_TEST( p_op, type))
    {
        if ( !strcmp(STR_TYPE_FILE, ATTR_FSorDB(p_op, type)) )
        {
            /* is it a lock file? */
            if (!fnmatch("*/"LOCK_DIR"/"SHOOK_LOCK_PREFIX"*", ATTR_FSorDB(p_op, fullpath ), 0))
            {
                /* skip the entry */
                DisplayLog(LVL_DEBUG, ENTRYPROC_TAG, "%s is a shook lock",
                           ATTR_FSorDB(p_op, fullpath));
                /** XXX raise special event for the file: LOCK/UNLOCK? */
                return TRUE;
            }
        }
        else if (!strcmp(STR_TYPE_DIR, ATTR_FSorDB(p_op, type)))
        {
            if (!fnmatch("*/"LOCK_DIR, ATTR_FSorDB(p_op, fullpath ), 0))
            {
                /* skip the entry */
                DisplayLog(LVL_DEBUG, ENTRYPROC_TAG, "%s is a shook lock dir",
                           ATTR_FSorDB(p_op, fullpath));
                return TRUE;
            }
            else if (!fnmatch("*/"RESTRIPE_DIR, ATTR_FSorDB(p_op, fullpath ), 0))
            {
                /* skip the entry */
                DisplayLog(LVL_DEBUG, ENTRYPROC_TAG, "%s is a shook restripe dir",
                           ATTR_FSorDB(p_op, fullpath));
                return TRUE;
            }
        }
    }

    /* set name from path */
    if (ATTR_MASK_TEST(&p_op->db_attrs, fullpath))
        ListMgr_GenerateFields( &p_op->db_attrs, ATTR_MASK_name );
    if (ATTR_MASK_TEST(&p_op->fs_attrs, fullpath))
        ListMgr_GenerateFields( &p_op->fs_attrs, ATTR_MASK_name );

    /* also match '.shook' directory */
    if (p_op && ATTR_FSorDB_TEST( p_op, name )
        && ATTR_FSorDB_TEST( p_op, type))
    {
        if ( !strcmp(STR_TYPE_DIR, ATTR_FSorDB(p_op, type)) &&
             !strcmp(SHOOK_DIR, ATTR_FSorDB(p_op, name)) )
        {
            /* skip the entry */
            DisplayLog(LVL_DEBUG, ENTRYPROC_TAG, "\"%s\" is a shook dir",
                       ATTR_FSorDB(p_op, name));
            return TRUE;
        }
    }

    return FALSE;
}

#define SKIP_SPECIAL_OBJ(_pop, _goto) do {                              \
        if (shook_special_obj( _pop )) {                                \
                    DisplayLog( LVL_DEBUG, ENTRYPROC_TAG,               \
                        "Shook special file or dir '%s', skipped",      \
                        (ATTR_FSorDB_TEST( _pop, fullpath )?            \
                         ATTR_FSorDB(_pop, fullpath):                   \
                         ATTR_FSorDB(_pop, name)) );                    \
            goto _goto;                                                 \
        }                                                               \
    } while(0)
#else
#define SKIP_SPECIAL_OBJ(_pop, _goto) /* do nothing */
#endif

/**
 * For entries from FS scan, we must get the associated entry ID.
 */
int EntryProc_get_fid( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
#ifdef _HAVE_FID
    int            rc;
    entry_id_t     tmp_id;

    if ( !ATTR_MASK_TEST( &p_op->fs_attrs, fullpath ) )
    {
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                    "Error: entry full path is expected to be set"
                    " in STAGE_GET_FID stage" );
        EntryProcessor_Acknowledge( p_op, -1, TRUE );
        return EINVAL;
    }

    /* perform path2fid */
    rc = Lustre_GetFidFromPath( ATTR( &p_op->fs_attrs, fullpath ), &tmp_id );
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
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                        "Error %d retrieving entry "DFID" from DB", rc,
                        PFID(&p_op->entry_id) );
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

#ifdef _LUSTRE
        /* only if stripe is in diff_mask */
        if (entry_proc_conf.diff_mask & (ATTR_MASK_stripe_info | ATTR_MASK_stripe_items))
        {
            /* get stripe only for files */
            if ( ATTR_MASK_TEST( &p_op->fs_attrs, type )
                 && !strcmp( ATTR( &p_op->fs_attrs, type ), STR_TYPE_FILE ))
            {
                /* only get stripe for files */
                if (!strcmp( global_config.fs_type, "lustre" ))
                {
                    /* Does file has stripe info ? */
                    if ( ListMgr_CheckStripe( lmgr, &p_op->entry_id ) != DB_SUCCESS )
                    {
                        DisplayLog( LVL_DEBUG, ENTRYPROC_TAG, "Stripe information is missing/invalid in DB" );

                        /* don't need to get stripe if we already have fresh stripe info from FS */
                        if (!(ATTR_MASK_TEST(&p_op->fs_attrs, stripe_info)
                              && ATTR_MASK_TEST(&p_op->fs_attrs, stripe_items)))
                        {
                                p_op->fs_attr_need |= ATTR_MASK_stripe_info | ATTR_MASK_stripe_items;
                        }
                    }
                    else
                    {
                        /* don't update stripe info (it is still valid) */
                        ATTR_MASK_UNSET(&p_op->fs_attrs, stripe_info);
                        if (ATTR_MASK_TEST(&p_op->fs_attrs, stripe_items))
                        {
                            /* free stripe structure */
                            if (ATTR(&p_op->fs_attrs, stripe_items).stripe)
                                MemFree(ATTR(&p_op->fs_attrs, stripe_items).stripe);
                            ATTR_MASK_UNSET(&p_op->fs_attrs, stripe_items);
                        }
                    }
                }
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

    /* early check using DB info */
    SKIP_SPECIAL_OBJ(p_op, skip_record);

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

    /* scans: never need to get attr or path (provided in operation) */

#ifdef ATTR_INDEX_creation_time
    if (entry_proc_conf.detect_fake_mtime)
    {
        if (ATTR_FSorDB_TEST(p_op, creation_time)
            && ATTR_MASK_TEST(&p_op->fs_attrs, last_mod)
            && ATTR(&p_op->fs_attrs, last_mod) < ATTR_FSorDB(p_op, creation_time))
        {
            time_t val;
            char mt[128];
            char ct[128];
            struct tm      t;
            val = ATTR(&p_op->fs_attrs, last_mod);
            strftime(mt, 128, "%Y/%m/%d %T", localtime_r(&val, &t));
            val = ATTR_FSorDB(p_op, creation_time);
            strftime(ct, 128, "%Y/%m/%d %T", localtime_r(&val, &t));

            if (ATTR_FSorDB_TEST(p_op, fullpath))
                DisplayLog(LVL_VERB, ENTRYPROC_TAG,
                           "Fake mtime detected for %s: mtime=%s, creation=%s",
                           ATTR_FSorDB(p_op, fullpath), mt, ct);
            else
                DisplayLog(LVL_VERB, ENTRYPROC_TAG,
                           "Fake mtime detected for "DFID": mtime=%s, creation=%s",
                           PFID(&p_op->entry_id), mt, ct);
        }
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
        attr_set_t new_attrs;
        ATTR_MASK_INIT(&new_attrs);

        /* get entry status */
        rc = rbhext_get_status( &p_op->entry_id, &p_op->fs_attrs,
                                &new_attrs );
#elif defined(_LUSTRE_HSM)
        rc = LustreHSM_GetStatus( path, &ATTR( &p_op->fs_attrs, status ),
                                  &ATTR( &p_op->fs_attrs, no_release ),
                                  &ATTR( &p_op->fs_attrs, no_archive ) );
#endif
        if ( ERR_MISSING( abs( rc )) )
        {
            DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Entry %s does not exist anymore",
                        path );
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

    /* later check using DB+FS info */
    SKIP_SPECIAL_OBJ(p_op, skip_record);

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


int EntryProc_report_diff( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];
    int rc;

    /* final check before displaying diff */
    SKIP_SPECIAL_OBJ(p_op, skip_record);

#ifdef ATTR_INDEX_creation_time
    /* once set, never change creation time */
    if (p_op->db_op_type != OP_TYPE_INSERT)
        ATTR_MASK_UNSET( &p_op->fs_attrs, creation_time );
#endif
    p_op->fs_attrs.attr_mask &= ~readonly_attr_set;

    /* Only keep fields that changed */
    if (p_op->db_op_type == OP_TYPE_UPDATE)
    {
        /* XXX keep md_update to avoid removing the entry from DB:
         * use DB tag instead?
         */
        int to_keep = ATTR_MASK_md_update;
        /* keep path_update if fullpath is in diff_mask */
        if (entry_proc_conf.diff_mask & ATTR_MASK_fullpath)
            to_keep |= ATTR_MASK_path_update;

        ListMgr_KeepDiff(&p_op->fs_attrs, &p_op->db_attrs);

        /* keep only fields in diff_mask or to_keep */
        p_op->fs_attrs.attr_mask &= (entry_proc_conf.diff_mask | to_keep);

        /* nothing changed => noop */
        if (p_op->fs_attrs.attr_mask == 0)
        {
            /* no op */
            p_op->db_op_type = OP_TYPE_NONE;
        }
        else if (p_op->fs_attrs.attr_mask & entry_proc_conf.diff_mask)
        {
            char attrchg[RBH_PATH_MAX] = "";

            /* revert change: reverse display */
            if (diff_arg->apply == APPLY_FS)
            {
                /* attr from FS */
                PrintAttrs(attrchg, RBH_PATH_MAX, &p_op->fs_attrs,
                    p_op->fs_attrs.attr_mask & p_op->db_attrs.attr_mask
                    & entry_proc_conf.diff_mask, 1);

                printf("-"DFID" %s\n", PFID(&p_op->entry_id), attrchg);

                /* attr from DB */
                if ((p_op->db_attrs.attr_mask & p_op->fs_attrs.attr_mask
                    & entry_proc_conf.diff_mask) == 0)
                    attrchg[0] = '\0';
                else
                    PrintAttrs(attrchg, RBH_PATH_MAX, &p_op->db_attrs,
                        p_op->db_attrs.attr_mask & p_op->fs_attrs.attr_mask
                        & entry_proc_conf.diff_mask, 1);

                printf("+"DFID" %s\n", PFID(&p_op->entry_id), attrchg);
            }
            else
            {
                /* attr from DB */
                if ((p_op->db_attrs.attr_mask & p_op->fs_attrs.attr_mask
                    & entry_proc_conf.diff_mask) == 0)
                    attrchg[0] = '\0';
                else
                    PrintAttrs(attrchg, RBH_PATH_MAX, &p_op->db_attrs,
                        p_op->db_attrs.attr_mask & p_op->fs_attrs.attr_mask
                        & entry_proc_conf.diff_mask, 1);

                printf("-"DFID" %s\n", PFID(&p_op->entry_id), attrchg);

                /* attr from FS */
                PrintAttrs(attrchg, RBH_PATH_MAX, &p_op->fs_attrs,
                    p_op->fs_attrs.attr_mask & entry_proc_conf.diff_mask, 1);

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
        else if ((p_op->db_op_type == OP_TYPE_REMOVE)
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

    /* always go to APPLY step, at least to tag the entry */
    rc = EntryProcessor_Acknowledge( p_op, STAGE_APPLY, FALSE );
    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error acknowledging stage %s",
                    stage_info->stage_name );
    return rc;

#ifdef HAVE_SHOOK
skip_record:
    /* remove the operation from pipeline */
    rc = EntryProcessor_Acknowledge( p_op, -1, TRUE );

    if ( rc )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d acknowledging stage.", rc );
    return rc;
#endif
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
        case OP_TYPE_REMOVE:
    #ifdef _HAVE_FID
            DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Remove("DFID")", PFID(&p_op->entry_id) );
    #endif
            rc = ListMgr_Remove( lmgr, &p_op->entry_id );
            break;
        case OP_TYPE_SOFT_REMOVE:
    #ifdef _HSM_LITE
            DisplayLog( LVL_DEBUG, ENTRYPROC_TAG, "SoftRemove("DFID", path=%s, bkpath=%s)",
                        PFID(&p_op->entry_id),
                        ATTR_FSorDB_TEST( p_op, fullpath )?ATTR_FSorDB(p_op, fullpath):"",
                        ATTR_FSorDB_TEST( p_op, backendpath )?ATTR_FSorDB(p_op, backendpath):"" );
            rc = ListMgr_SoftRemove( lmgr, &p_op->entry_id,
                    ATTR_FSorDB_TEST( p_op, fullpath )?ATTR_FSorDB(p_op, fullpath):NULL,
                    ATTR_FSorDB_TEST( p_op, backendpath )?ATTR_FSorDB(p_op, backendpath):NULL,
                    time(NULL) + policies.unlink_policy.deferred_remove_delay ) ;
    #elif defined(_LUSTRE_HSM) /* Lustre-HSM */
            DisplayLog( LVL_DEBUG, ENTRYPROC_TAG, "SoftRemove("DFID", path=%s)",
                        PFID(&p_op->entry_id),
                        ATTR_FSorDB_TEST( p_op, fullpath )?ATTR_FSorDB(p_op, fullpath):"");
            rc = ListMgr_SoftRemove( lmgr, &p_op->entry_id,
                    ATTR_FSorDB_TEST( p_op, fullpath )?ATTR_FSorDB(p_op, fullpath):NULL,
                    time(NULL) + policies.unlink_policy.deferred_remove_delay ) ;
    #else
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "SoftRemove operation not supported in this mode!" );
            rc = -1;
    #endif
            break;
        default:
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Unhandled DB operation type: %d", p_op->db_op_type );
            rc = -1;
        }

        if ( rc )
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d performing database operation.", rc );
    }
    else if (diff_arg->db_tag)
    {
        /* tag the entry in the DB */
        rc = ListMgr_TagEntry(lmgr, diff_arg->db_tag, &p_op->entry_id);
        if ( rc )
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error %d performing database operation.", rc );
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
                       const entry_id_t *p_id,
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

            /* insert the new entry to the DB */
            rc = ListMgr_Insert(lmgr, &new_id, &new_attrs, TRUE);
            if (rc)
            {
                DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Failed to insert new entry '%s' ("DFID") to the database",
                           ATTR(&new_attrs, fullpath), PFID(&new_id));
                goto clean_entry;
            }

            rc = ListMgr_Remove(lmgr, p_id);
            if (rc)
            {
                DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Failed to remove old reference "DFID" from the database",
                           PFID(p_id));
                goto clean_db;
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

clean_db:
    rc = ListMgr_Remove(lmgr, &new_id);
    if (rc)
        DisplayLog(LVL_EVENT, ENTRYPROC_TAG, "db cleanup: remove failed: error %d", rc);

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
                       const entry_id_t *p_id,
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
    if (diff_arg->lovea_file)
    {
        if (!ATTR_MASK_TEST(&new_attrs, fullpath))
        {
            DisplayLog(LVL_MAJOR, ENTRYPROC_TAG, "Fullpath needed to write into lovea_file");
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

                    fprintf(diff_arg->lovea_file, output);

                    /* XXX overwrite stripe info in new attrs? */
        //            ATTR(&new_attrs, stripe_info) = ATTR(p_oldattr, stripe_info);
        //            ATTR(&new_attrs, stripe_items) = ATTR(p_oldattr, stripe_items);
                }
            }
        }
    }
#endif

    /* insert the new entry to the DB */
    rc = ListMgr_Insert(lmgr, &new_id, &new_attrs, TRUE);
    if (rc)
    {
        DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Failed to insert new entry '%s' ("DFID") to the database",
                   ATTR(&new_attrs, fullpath), PFID(&new_id));
        goto clean_entry;
    }

    rc = ListMgr_Remove(lmgr, p_id);
    if (rc)
    {
        DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Failed to remove old reference "DFID" from the database",
                   PFID(p_id));
        goto clean_db;
    }

    return 0;

clean_db:
    rc = ListMgr_Remove(lmgr, &new_id);
    if (rc)
        DisplayLog(LVL_EVENT, ENTRYPROC_TAG, "db cleanup: remove failed: error %d", rc);

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


int EntryProc_report_rm( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];
    lmgr_filter_t  filter;
    filter_value_t val;
    rm_cb_func_t cb = NULL;

    /* callback func for diff display */
    if (entry_proc_conf.diff_mask)
        cb = no_tag_cb;

    /* if md_update is not set, this is just an empty op to wait for
     * pipeline flush => don't rm old entries */
    if (ATTR_MASK_TEST(&p_op->fs_attrs, md_update))
    {
        /* call MassRemove only if APPLY_DB is set */
        if ((diff_arg->apply == APPLY_DB) && !(pipeline_flags & FLAG_DRY_RUN))
        {
            lmgr_simple_filter_init( &filter );

            val.value.val_uint = ATTR( &p_op->fs_attrs, md_update );
            lmgr_simple_filter_add( &filter, ATTR_INDEX_md_update, LESSTHAN_STRICT, val, 0 );

            /* partial scan: remove non-updated entries from a subset of the namespace */
            if (ATTR_MASK_TEST( &p_op->fs_attrs, fullpath ))
            {
                char tmp[RBH_PATH_MAX];
                strcpy(tmp, ATTR(&p_op->fs_attrs, fullpath));
                strcat(tmp, "/*");
                val.value.val_str = tmp;
                lmgr_simple_filter_add( &filter, ATTR_INDEX_fullpath, LIKE, val, 0 );
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
