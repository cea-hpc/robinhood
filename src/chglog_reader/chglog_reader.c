/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009, 2010 CEA/DAM
 * Copyright 2013 Cray Inc. All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */


/**
 * \file    chglog_reader.c
 * \author  Th. Leibovici
 * \brief   Lustre MDT Changelog processing.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "Memory.h"
#include "RobinhoodLogs.h"
#include "entry_processor.h"
#include "entry_proc_hash.h"
#include "RobinhoodMisc.h"
#include "global_config.h"
#include "RobinhoodConfig.h"

#include <pthread.h>
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include "lustre_extended_types.h"

#ifdef _LLAPI_FORKS
#include <signal.h>
#include <sys/wait.h>
#endif

/* for logs */
#define CHGLOG_TAG  "ChangeLog"

/* reader thread info, one per MDT */
typedef struct reader_thr_info_t
{
    /** reader thread index */
    unsigned int thr_index;

    /** thread id */
    pthread_t thr_id;

    /** nbr of records read by this thread */
    unsigned long long nb_read;

    /** number of records of interest (ie. not MARK, IOCTL, ...) */
    unsigned long long interesting_records;

    /** number of suppressed/merged records */
    unsigned long long suppressed_records;

    /** time when the last line was read */
    time_t  last_read_time;

    /** time of the last read record */
    struct timeval last_read_record_time;

    /** last read record id */
    unsigned long long last_read_record;

    /** last record id committed to database */
    unsigned long long last_committed_record;

    /** last record id cleared with changelog */
    unsigned long long last_cleared_record;

    /* number of times the changelog has been reopened */
    unsigned int nb_reopen;

    /** thread was asked to stop */
    unsigned int force_stop : 1;

    /** log handler */
    void * chglog_hdlr;

    /** Queue of pending changelogs to push to the pipeline. */
    struct list_head op_queue;
    unsigned int op_queue_count;

    /** Store the ops for easier access. Each element in the hash
     * table is also in the op_queue list. This hash table doesn't
     * need a lock per slot since there is only one reader. The
     * slot counts won't be used either. */
    struct id_hash * id_hash;

    unsigned long long cl_counters[CL_LAST]; /* since program start time */
    unsigned long long cl_reported[CL_LAST]; /* last reported stat (for incremental diff) */
    time_t last_report;

    /* to compute relative changelog speed (timeframe of read changelog since the last report) */
    struct timeval last_report_record_time;
    unsigned long long last_report_record_id;
    unsigned int last_reopen;

    /** On pre LU-1331 versions of Lustre, a CL_RENAME is always
     * followed by a CL_EXT, however these may not be
     * contiguous. Temporarily store the CL_RENAME changelog until we
     * get the CL_EXT. */
    CL_REC_TYPE * cl_rename;

} reader_thr_info_t;

/* Number of entries in each readers' op hash table. */
#define ID_CHGLOG_HASH_SIZE 7919

chglog_reader_config_t chglog_reader_config;
static int behavior_flags = 0;

/* stop reading logs when reaching end of file? */
#define one_shot ( behavior_flags & FLAG_ONCE )

/** array of reader info */
static reader_thr_info_t  * reader_info = NULL;


/** Reload configuration for changelog readers */
int            ChgLogRdr_ReloadConfig( void *module_config )
{
    /** @TODO Reload ChangeLog reader config dynamically */
    return 0;
}

/**
 * Close the changelog for a thread.
 */
static int log_close( reader_thr_info_t * p_info )
{
    int rc;

    /* close the log and clear input buffers */
    rc = llapi_changelog_fini(&p_info->chglog_hdlr);

    if ( rc )
        DisplayLog(LVL_CRIT, CHGLOG_TAG, "Error %d closing changelog: %s",
            rc, strerror(abs(rc)) );

    return abs(rc);
}

/**
 * Free allocated structures in op_extra_info_t field.
 */
static void free_extra_info( void * ptr )
{
    op_extra_info_t * p_info = (op_extra_info_t*)ptr;

    if ( p_info->is_changelog_record && p_info->log_record.p_log_rec )
    {
        llapi_changelog_free( &p_info->log_record.p_log_rec );
    }
}

static void free_extra_info2( void * ptr )
{
    op_extra_info_t * p_info = (op_extra_info_t*)ptr;

    if ( p_info->is_changelog_record && p_info->log_record.p_log_rec )
    {
        /* if this is a locally allocated record, just "free" it */
        free(p_info->log_record.p_log_rec);
        p_info->log_record.p_log_rec = NULL;
    }
}

/**
 * Clear the changelogs up to the last commited number seen.
 */
static int clear_changelog_records(reader_thr_info_t * p_info)
{
    int rc;
    const char * mount_point = get_mount_point(NULL);

    if (p_info->last_committed_record == 0) {
        /* No record was ever commited. Stop here because calling
         * llapi_changelog_clear() with record 0 will clear all
         * records, leading to a potential record loss. */
        return 0;
    }

    DisplayLog( LVL_DEBUG, CHGLOG_TAG, "Acknowledging ChangeLog records up to #%llu",
                p_info->last_committed_record );

    DisplayLog( LVL_FULL, CHGLOG_TAG, "llapi_changelog_clear('%s', '%s', %llu)",
                mount_point,
                chglog_reader_config.mdt_def[p_info->thr_index].reader_id,
                p_info->last_committed_record );

    rc = llapi_changelog_clear( mount_point,
                    chglog_reader_config.mdt_def[p_info->thr_index].reader_id,
                    p_info->last_committed_record );

    if (rc)
    {
            DisplayLog( LVL_CRIT, CHGLOG_TAG,
                        "ERROR: llapi_changelog_clear(\"%s\", \"%s\", %llu) returned %d",
                        mount_point,
                        chglog_reader_config.mdt_def[p_info->thr_index].reader_id,
                        p_info->last_committed_record, rc );
    } else {
        p_info->last_cleared_record = p_info->last_committed_record;
    }

    return rc;

}

/**
 * DB callback function: this is called when a given ChangeLog record
 * has been successfully applied to the database.
 */
static int log_record_callback( lmgr_t *lmgr, struct entry_proc_op_t * pop, void * param )
{
    int rc;
    reader_thr_info_t * p_info = (reader_thr_info_t *) param;
    CL_REC_TYPE * logrec = pop->extra_info.log_record.p_log_rec;

    /** Check that a log record is set for this entry
     * (should always be the case).
     */
    if ( !pop->extra_info.is_changelog_record || (logrec == NULL ) )
    {
        DisplayLog( LVL_CRIT, CHGLOG_TAG, "Error: log record callback function"
                    " has been called for a non-changelog entry" );
        return EINVAL;
    }

    /* New highest commited record so far. */
    p_info->last_committed_record = logrec->cr_index;

    /* batching llapi_changelog_clear() calls.
     * clear the record in any of those cases:
     *      - batch_ack_count = 1 (i.e. acknowledge every record).
     *      - we reached the last read record.
     *      - if the delta to last cleared record is high enough.
     * do nothing in all other cases:
     */
    if ((chglog_reader_config.batch_ack_count > 1)
         && (logrec->cr_index < p_info->last_read_record)
         && ((logrec->cr_index - p_info->last_cleared_record)
             < chglog_reader_config.batch_ack_count))
    {
        DisplayLog(LVL_FULL, CHGLOG_TAG, "callback - %llu %llu\n", logrec->cr_index, p_info->last_cleared_record);
        /* do nothing, don't clear log now */
        return 0;
    }

    rc = clear_changelog_records(p_info);

    return rc;

}

#ifdef _LUSTRE_HSM

static const char * event_name[] = {
    "archive", "restore", "cancel", "release", "remove", "state"
};
#define CL_EVENT_MAX 5
#endif

/* Push the oldest (all=FALSE) or all (all=TRUE) entries into the pipeline. */
static void process_op_queue(reader_thr_info_t *p_info, const int push_all)
{
    time_t oldest = time(NULL) - chglog_reader_config.queue_max_age;

    while(!rh_list_empty(&p_info->op_queue)) {
        entry_proc_op_t *op = rh_list_first_entry(&p_info->op_queue, entry_proc_op_t, list);

        /* Stop when the queue is below our limit, and when the oldest
         * element is still new enough. */
        if (push_all == FALSE &&
            p_info->op_queue_count < chglog_reader_config.queue_max_size &&
            op->changelog_inserted > oldest)
            break;

        rh_list_del(&op->list);
        rh_list_del(&op->hash_list);

        /* Push the entry to the pipeline */
        EntryProcessor_Push( op );

        p_info->op_queue_count --;
    }
}

/* Flags to insert_into_hash. */
#define PLR_FLG_FREE2       0x0001 /* must free changelog record on completion */
#define CHECK_IF_LAST_ENTRY 0x0002 /* check whether the unlinked file is the last one. */
#define GET_FID_FROM_DB     0x0004 /* fid is not valid, get it from DB */

/* Insert the operation into the internal hash table. */
static int insert_into_hash( reader_thr_info_t * p_info, CL_REC_TYPE * p_rec, unsigned int flags )
{
    entry_proc_op_t *op;
    struct id_hash_slot *slot;

    op = EntryProcessor_Get( );
    if (!op) {
        DisplayLog( LVL_CRIT, CHGLOG_TAG,
                    "CRITICAL ERROR: EntryProcessor_Get failed to allocate a new op" );
        return -1;
    }

    /* first, it will check if it already exists in database */
    op->pipeline_stage = entry_proc_descr.GET_INFO_DB;

    /* set log record */
    op->extra_info_is_set = TRUE;
    op->extra_info.is_changelog_record = TRUE;
    op->extra_info.log_record.p_log_rec = p_rec;

    /* set mdt name */
    op->extra_info.log_record.mdt =
        chglog_reader_config.mdt_def[p_info->thr_index].mdt_name;

    if (flags & PLR_FLG_FREE2)
        op->extra_info_free_func = free_extra_info2;
    else
        op->extra_info_free_func = free_extra_info;

    op->check_if_last_entry = !!(flags & CHECK_IF_LAST_ENTRY);
    op->get_fid_from_db = !!(flags & GET_FID_FROM_DB);

    /* set callback function + args */
    op->callback_func = log_record_callback;
    op->callback_param = p_info;

    /* Set entry ID */
    if (!op->get_fid_from_db)
        EntryProcessor_SetEntryId( op, &p_rec->cr_tfid );

    /* Add the entry on the pending queue ... */
    op->changelog_inserted = time(NULL);
    rh_list_add_tail(&op->list, &p_info->op_queue);
    p_info->op_queue_count ++;

    /* ... and the hash table. */
    slot = get_hash_slot(p_info->id_hash, &op->entry_id);
    rh_list_add_tail(&op->hash_list, &slot->list);

    return 0;
}

/* Describes which records can be safely ignored. By default a record
 * is never ignored. It is only necessary to add an entry in this
 * table if the record may be skipped (and thus has a mask defined) or
 * if it can be skipped altogether. */
static const struct {
    enum { IGNORE_NEVER = 0,    /* default */
           IGNORE_MASK,         /* mask must be set, and record has a FID */
           IGNORE_ALWAYS
    } ignore;
    unsigned int ignore_mask;
} record_filters[CL_LAST] = {

    /* Record we don't care about. */
    [CL_MARK] = { .ignore = IGNORE_ALWAYS },
    [CL_IOCTL] = { .ignore = IGNORE_ALWAYS },
#ifndef HAVE_SHOOK
    [CL_XATTR] = { .ignore = IGNORE_ALWAYS },
#endif

    /* Similar operation (data changes). For instance, if the current
     * operation is a CLOSE, drop it if we find a previous
     * TRUNC/CLOSE/MTIME or CREATE for the same FID. */
    [CL_TRUNC] = { IGNORE_MASK, 1<<CL_TRUNC | 1<<CL_CLOSE | 1<<CL_MTIME | 1<<CL_CREATE },
    [CL_CLOSE] = { IGNORE_MASK, 1<<CL_TRUNC | 1<<CL_CLOSE | 1<<CL_MTIME | 1<<CL_CREATE },
    [CL_MTIME] = { IGNORE_MASK, 1<<CL_TRUNC | 1<<CL_CLOSE | 1<<CL_MTIME | 1<<CL_CREATE | 1<<CL_MKNOD | 1<<CL_MKDIR },

    /* Similar operations (metadata changes). */
    [CL_CTIME] = { IGNORE_MASK, 1<<CL_CTIME | 1<<CL_SETATTR | 1<<CL_CREATE | 1<<CL_MKNOD | 1<<CL_MKDIR },
    [CL_SETATTR] = { IGNORE_MASK, 1<<CL_CTIME | 1<<CL_SETATTR | 1<<CL_CREATE | 1<<CL_MKNOD | 1<<CL_MKDIR },
};

/* Decides whether a new changelog record can be ignored. Ignoring a
 * record should not impact the database state, however the gain is to:
 *  - reduce contention on pipeline stages with constraints,
 *  - reduce the number of DB and FS requests.
 *
 * Returns TRUE or FALSE.
 */
static int can_ignore_record(const reader_thr_info_t *p_info,
                             const CL_REC_TYPE *logrec_in)
{
    entry_proc_op_t *op, *t1;
    unsigned int ignore_mask;
    struct id_hash_slot *slot;

    if (record_filters[logrec_in->cr_type].ignore == IGNORE_NEVER)
        return FALSE;

    if (record_filters[logrec_in->cr_type].ignore == IGNORE_ALWAYS)
        return TRUE;

    /* The ignore field is IGNORE_MASK. At that point, the FID in the
     * changelog record must be set. All the changelog record with the
     * same FID will go into the same bucket, so parse that slot
     * instead of the whole op_queue list. */
    slot = get_hash_slot(p_info->id_hash, &logrec_in->cr_tfid);
    ignore_mask = record_filters[logrec_in->cr_type].ignore_mask;

    rh_list_for_each_entry_safe_reverse(op, t1, &slot->list, hash_list)
    {
        CL_REC_TYPE *logrec = op->extra_info.log_record.p_log_rec;

        /* If the type of record matches what we're looking for, and
         * it's for the same FID, then we can ignore the new
         * record. */
        if ((ignore_mask & (1<<logrec->cr_type)) &&
            entry_id_equal(&logrec->cr_tfid, &logrec_in->cr_tfid)) {
            return TRUE;
        }
    }

    return FALSE;
}

/**
 * Create a fake unlink changelog record that will be used to remove a
 * file that is overriden during a rename operation.
 *
 * This is only available if LU-543 or LU-1331 are present on the
 * Lustre server.
 */
static CL_REC_TYPE * create_fake_unlink_record(const reader_thr_info_t *p_info,
                                               CL_REC_TYPE *rec_in,
                                               unsigned int *insert_flags)
{
    CL_REC_TYPE *rec;

    /* rename overwriting target entry */
    rec = MemAlloc(sizeof(CL_REC_TYPE) + rec_in->cr_namelen + 1);
    if (rec) {
        /* Copy the structure and fix a few fields. */
        memcpy(rec, rec_in, sizeof(CL_REC_TYPE) + rec_in->cr_namelen);

        /* TODO: It is unclear whether cr_name is actually 0
         * terminated, so add one just in case. */
        rec->cr_name[rec->cr_namelen] = 0;

        *insert_flags = PLR_FLG_FREE2;

        if (!chglog_reader_config.mds_has_lu543) {
            /* The server doesn't tell whether the rename operation will
             * remove a file. */
            *insert_flags |= GET_FID_FROM_DB;
        }

#ifdef CLF_RENAME_LAST
        /* The client support LU-1331 (since CLF_RENAME_LAST is
         * defined) but that may not be the case of the server. */
        if (chglog_reader_config.mds_has_lu1331) {
            rec->cr_flags = (rec_in->cr_flags & CLF_RENAME_LAST)? CLF_UNLINK_LAST : 0;
        } else
#endif
        {
            /* CLF_RENAME_LAST is not supported in this version of the
             * client and/or the server. The pipeline will have to
             * decide whether this is the last entry or not. */
            rec->cr_flags = 0;
            *insert_flags |= CHECK_IF_LAST_ENTRY;
        }

        rec->cr_type = CL_UNLINK;
        rec->cr_index = rec_in->cr_index - 1;

        DisplayLog( LVL_DEBUG, CHGLOG_TAG,
                    "Unlink: object="DFID", name=%.*s",
                    PFID(&rec->cr_tfid), rec->cr_namelen,
                    rec->cr_name );
    }

    return rec;

}

#if defined(HAVE_CHANGELOG_EXTEND_REC)
/**
 * Create a fake rename record to ensure compatibility with older
 * Lustre records.
 *
 * This is only available LU-1331 is present on the Lustre server.
 */
static CL_REC_TYPE * create_fake_rename_record(const reader_thr_info_t *p_info,
                                               CL_REC_TYPE *rec_in)
{
    CL_REC_TYPE *rec;

    /* rename overwriting target entry */
    rec = MemAlloc(sizeof(CL_REC_TYPE) + rec_in->cr_namelen + 1);
    if (rec) {
        /* Copy the structure and fix a few fields. */
        memcpy(rec, rec_in, sizeof(CL_REC_TYPE) + rec_in->cr_namelen);

        /* copy source name to RNMFRM */
        rec->cr_namelen = changelog_rec_snamelen(rec_in);
        strncpy(rec->cr_name, changelog_rec_sname(rec_in), rec->cr_namelen);
        rec->cr_name[rec->cr_namelen] = '\0';

        rec->cr_flags = 0; /* not used for RNMFRM */
        rec->cr_type = CL_RENAME;

        /* we don't want to acknowledge this record as long as the 2
         * records are not processed. acknowledge n-1 instead */
        rec->cr_index = rec_in->cr_index - 1;

        rec->cr_tfid = rec_in->cr_sfid; /* the renamed fid */
        rec->cr_pfid = rec_in->cr_spfid; /* the source parent */
    }

    return rec;

}
#endif

/**
 * This handles a single log record.
 */
static int process_log_rec( reader_thr_info_t * p_info, CL_REC_TYPE * p_rec )
{
    unsigned int opnum;
    char flag_buff[256] = "";

#ifdef _LUSTRE_HSM
    if ( p_rec->cr_type == CL_HSM )
    {
        const char * event = NULL;
        if ( hsm_get_cl_event( p_rec->cr_flags ) > CL_EVENT_MAX )
            event = "unknown";
        else
            event = event_name[hsm_get_cl_event( p_rec->cr_flags )];

        snprintf(flag_buff, 256, "(%s%s,rc=%d)", event,
                 hsm_get_cl_flags( p_rec->cr_flags ) & CLF_HSM_DIRTY? ",dirty":"",
                 hsm_get_cl_error( p_rec->cr_flags ));
    }
#endif

    /* display the log record in debug mode */
    if (p_rec->cr_namelen)
    {
        /* this record has a 'name' field. */
        DisplayLog( LVL_DEBUG, CHGLOG_TAG, "%llu %02d%-5s %u.%09u 0x%x%s t="DFID" p="DFID" %.*s",
                    p_rec->cr_index, p_rec->cr_type,
                    changelog_type2str(p_rec->cr_type),
                    (uint32_t)cltime2sec(p_rec->cr_time),
                    cltime2nsec(p_rec->cr_time),
                    p_rec->cr_flags & CLF_FLAGMASK, flag_buff,
                    PFID(&p_rec->cr_tfid),
                    PFID(&p_rec->cr_pfid),
                    p_rec->cr_namelen, p_rec->cr_name);
    }
    else
    {
        /* no 'name' field. */
        DisplayLog( LVL_DEBUG, CHGLOG_TAG, "%llu %02d%-5s %u.%09u 0x%x%s t="DFID,
                    p_rec->cr_index, p_rec->cr_type,
                    changelog_type2str(p_rec->cr_type),
                    (uint32_t)cltime2sec(p_rec->cr_time),
                    cltime2nsec(p_rec->cr_time),
                    p_rec->cr_flags & CLF_FLAGMASK, flag_buff,
                    PFID(&p_rec->cr_tfid) );
    }

    /* update stats */
    opnum = p_rec->cr_type ;
    if ((opnum >= 0) && (opnum < CL_LAST))
        p_info->cl_counters[opnum] ++;
    else {
        DisplayLog( LVL_CRIT, CHGLOG_TAG,
                    "Log record type %d out of bounds.",
                    opnum );
        return EINVAL;
    }

    /* This record might be of interest. But try to check whether it
     * might create a duplicate operation anyway. */
    if (can_ignore_record(p_info, p_rec)) {
        DisplayLog( LVL_FULL, CHGLOG_TAG, "Ignoring event %s", changelog_type2str(opnum) );
        p_info->suppressed_records ++;
        llapi_changelog_free( &p_rec );
        goto done;
    }

    p_info->interesting_records ++;

    if (p_rec->cr_type == CL_RENAME) {
        /* Ensure there is no pending rename. */
        if (p_info->cl_rename) {
            /* Should never happen. */
            DisplayLog( LVL_CRIT, CHGLOG_TAG,
                        "Got 2 CL_RENAME in a row without a CL_EXT." );

            /* Discarding bogus entry. */
            llapi_changelog_free( &p_info->cl_rename );
            p_info->cl_rename = NULL;
        }

#ifdef HAVE_CHANGELOG_EXTEND_REC
        /* extended record: 1 single RENAME record per rename op;
         * there is no EXT. */
        if (CHANGELOG_REC_EXTENDED(p_rec))
        {
            struct changelog_ext_rec * p_rec2;

            /* The MDS sent an extended record, so we have both LU-543
             * and LU-1331. */
            if (!chglog_reader_config.mds_has_lu543 ||
                !chglog_reader_config.mds_has_lu1331) {
                DisplayLog( LVL_EVENT, CHGLOG_TAG,
                            "Detected LU-1331." );

                chglog_reader_config.mds_has_lu543 = 1;
                chglog_reader_config.mds_has_lu1331 = 1;
            }

            if (!FID_IS_ZERO(&p_rec->cr_tfid))
            {
                CL_REC_TYPE * unlink;
                unsigned int insert_flags;

                unlink = create_fake_unlink_record(p_info,
                                                   p_rec,
                                                   &insert_flags);
                if (unlink) {
                    insert_into_hash(p_info, unlink, insert_flags);
                } else {
                    DisplayLog( LVL_CRIT, CHGLOG_TAG,
                                "Could not allocate an UNLINK record." );
                }
            }

            DisplayLog( LVL_DEBUG, CHGLOG_TAG,
                        "Rename: object="DFID", old parent/name="DFID"/%s, new parent/name="DFID"/%.*s",
                        PFID(&p_rec->cr_sfid), PFID(&p_rec->cr_spfid),changelog_rec_sname(p_rec),
                        PFID(&p_rec->cr_pfid), p_rec->cr_namelen, p_rec->cr_name );

            /* Ensure compatibility with older Lustre versions:
             * push RNMFRM to remove the old path from NAMES table.
             * push RNMTO to add target path information.
             */
            /* 1) build & push RNMFRM */
            p_rec2 = create_fake_rename_record(p_info, p_rec);
            insert_into_hash(p_info, p_rec2, PLR_FLG_FREE2);

            /* 2) update RNMTO */
            p_rec->cr_type = CL_EXT; /* CL_RENAME -> CL_RNMTO */
            p_rec->cr_tfid = p_rec->cr_sfid; /* removed fid -> renamed fid */
            insert_into_hash(p_info, p_rec, 0);
        }
        else
#endif
        {
            /* This CL_RENAME is followed by CL_EXT, so keep it until
             * then. */
            p_info->cl_rename = p_rec;
        }
    }
    else if (p_rec->cr_type == CL_EXT) {

        if (!p_info->cl_rename) {
            /* Should never happen. */
            DisplayLog( LVL_CRIT, CHGLOG_TAG,
                        "Got CL_EXT without a CL_RENAME." );

            /* Discarding bogus entry. */
            llapi_changelog_free( &p_rec );

            goto done;
        }

        if (!chglog_reader_config.mds_has_lu543 &&
            (FID_IS_ZERO(&p_rec->cr_tfid) ||
             !entry_id_equal(&p_info->cl_rename->cr_tfid, &p_rec->cr_tfid))) {
            /* tfid if 0, or the two fids are different, so we have LU-543. */
            chglog_reader_config.mds_has_lu543 = 1;
            DisplayLog( LVL_EVENT, CHGLOG_TAG,
                        "Detected LU-543." );
        }

        /* We now have a CL_RENAME and a CL_EXT. */
        /* If target fid is not zero: unlink the target.
         * e.g. "mv a b" and b exists => rm b.
         */
        if (!FID_IS_ZERO(&p_rec->cr_tfid)) {
            CL_REC_TYPE * unlink;
            unsigned int insert_flags;

            /* Push an unlink. */
            unlink = create_fake_unlink_record(p_info, p_rec, &insert_flags);

            if (unlink) {
                insert_into_hash(p_info, unlink, insert_flags);
            } else {
                DisplayLog( LVL_CRIT, CHGLOG_TAG,
                            "Could not allocate an UNLINK record." );
            }
        }

        /* Push the rename and the ext.
         *
         * TODO: we should be able to push only one RENAME/EXT now.
         *
         * This is a little racy if CL_RENAME and CL_EXT were not
         * consecutive, because we are re-ordering the
         * CL_RENAME. Clearing one of the record in the middle will
         * also clear the RENAME with Lustre, however the RENAME
         * hasn't been processed yet. To hit the race, that
         * non-contiguous case should also happen while the changelog
         * is shutting down. The chance of that happening in the real
         * world should be rather slim to non-existent. */

        /* indicate the target fid as the renamed entry */
        p_rec->cr_tfid = p_info->cl_rename->cr_tfid;

        insert_into_hash(p_info, p_info->cl_rename, 0);
        p_info->cl_rename = NULL;
        insert_into_hash(p_info, p_rec, 0);
    }
    else {
        /* build the record to be processed in the pipeline */
        insert_into_hash(p_info, p_rec, 0);
    }

done:
    return 0;
}

static inline void cl_update_stats(reader_thr_info_t * info, CL_REC_TYPE * p_rec)
{
        /* update thread info */
        info->last_read_time = time(NULL);
        info->nb_read ++;
        info->last_read_record =  p_rec->cr_index;
        info->last_read_record_time.tv_sec = (time_t)cltime2sec(p_rec->cr_time);
        info->last_read_record_time.tv_usec = cltime2nsec(p_rec->cr_time)/1000;

        /* if no record has been read, save it as the previous last */
        if (info->last_report_record_id == 0)
        {
            info->last_report_record_id = info->last_read_record - 1;
            info->last_report_record_time = info->last_read_record_time;
        }
}


/* get a changelog line (with retries) */
typedef enum {cl_ok, cl_continue, cl_stop} cl_status_e;

static cl_status_e cl_get_one(reader_thr_info_t * info,  CL_REC_TYPE ** pp_rec)
{
    int rc;
    /* get next record */
    rc = llapi_changelog_recv( info->chglog_hdlr, pp_rec);

    /* is it EOF ? */
    if ( rc == 1 )
    {
        /* do we exit at EOF? */
        if ( !one_shot )
        {
            /* reopen */
            char mdtdevice[128];
            int flags;

            snprintf( mdtdevice, 128, "%s-%s", get_fsname(),
                      chglog_reader_config.mdt_def[info->thr_index].mdt_name );
            log_close(info);

            if ( chglog_reader_config.force_polling )
            {
                DisplayLog( LVL_FULL, CHGLOG_TAG,
                    "EOF reached on changelog, reopening in %d sec",
                    chglog_reader_config.polling_interval);
                    /* sleep during polling interval */
                    rh_sleep( chglog_reader_config.polling_interval );
                    flags = CHANGELOG_FLAG_BLOCK;
            }
            else
            {
                DisplayLog( LVL_EVENT, CHGLOG_TAG,
                    "WARNING: EOF reached on ChangeLog whereas FOLLOW flag "
                    "was specified. Re-opening in 1 sec..." );
                    rh_sleep( 1 );
                    flags = CHANGELOG_FLAG_BLOCK | CHANGELOG_FLAG_FOLLOW;
            }
            info->nb_reopen ++;
            /* opening the log again (from last_read_record + 1) */
            rc = llapi_changelog_start( &info->chglog_hdlr, flags,
                                        mdtdevice, info->last_read_record + 1 );
            if (rc==0)
                return cl_continue;
            else
                return cl_stop;
        }
        else
            return cl_stop;
    }
    else if ( rc == -EINTR )
    {
        DisplayLog( LVL_EVENT, CHGLOG_TAG,
                    "llapi_changelog_recv() interrupted. Retrying." );
        return cl_continue;
    }

    if ( rc )
    {
        DisplayLog( LVL_CRIT, CHGLOG_TAG,
                    "Error in llapi_changelog_recv(): %d: %s",
                    rc, strerror(abs(rc)));
        rh_sleep(1);
        /* try to recover from this error */
        return cl_continue;
    }
    else if ( *pp_rec == NULL )
    {
        DisplayLog( LVL_CRIT, CHGLOG_TAG,
                    "Error in llapi_changelog_recv(): NULL pointer returned" );
        rh_sleep(1);
        /* try to recover from this error */
        return cl_continue;
    }

    /* successfully retrieved a record */

    /* update thread info */
    cl_update_stats(info, *pp_rec);

    return cl_ok;
}


/** a thread that reads lines from a given changelog */
static void * chglog_reader_thr( void *  arg )
{
    reader_thr_info_t * info = (reader_thr_info_t*) arg;
    CL_REC_TYPE * p_rec = NULL;
    cl_status_e st;
    time_t next_push_time = time(NULL) + chglog_reader_config.queue_check_interval; /* Next time we will have to push. */

    /* loop until a TERM signal is caught */
    while ( !info->force_stop )
    {
        /* Is it time to flush? */
        if (info->op_queue_count >= chglog_reader_config.queue_max_size ||
            next_push_time <= time(NULL)) {
            process_op_queue(info, FALSE);

            next_push_time = time(NULL) + chglog_reader_config.queue_check_interval;
        }

        st = cl_get_one(info, &p_rec);
        if (st == cl_continue )
            continue;
        else if (st == cl_stop)
            break;

        /* handle the line and push it to the pipeline */
        process_log_rec( info, p_rec );
    }

    /* Stopping. Flush the internal queue. */
    process_op_queue(info, TRUE);

    DisplayLog(LVL_CRIT, CHGLOG_TAG, "Changelog reader thread terminating");
    return NULL;

}

#ifdef _LLAPI_FORKS
/* In early Lustre 2.0 releases, llapi_changelog_start() forks a process
 * that keeps in <defunc> state.
 * So we work around this issue by trapping SIGCHILD signals.
 */
static void action_sigchld( int sig )
{
    pid_t child ;
    do
    {
        /* wait for all terminated children
         * and stop on end of list or error.
         */
        child = waitpid( -1, NULL, WNOHANG ) ;
    } while ( child > 0 );

}
#endif


/** start ChangeLog Reader module */
int            ChgLogRdr_Start( chglog_reader_config_t * p_config, int flags )
{
    int i, rc;
    char mdtdevice[128];
#ifdef _LLAPI_FORKS
    struct sigaction act_sigchld ;
#endif

    /* check parameters */
#ifdef _DEBUG_CHGLOG
    printf( "Parameters: mdt_count = %u, mdt_def = %p\n",
            p_config->mdt_count, p_config->mdt_def );
#endif

    if ( (p_config->mdt_count == 0) || (p_config->mdt_def == NULL) )
    {
        DisplayLog( LVL_CRIT, CHGLOG_TAG,
                    "ERROR: no MDT ChangeLog has been defined in configuration" );
        return EINVAL;
    }
    else if ( p_config->mdt_count > 1 )
    {
        DisplayLog(LVL_CRIT, CHGLOG_TAG,
                   "ERROR: multi-MDT filesystems are not supported in the current version");
          return ENOTSUP;
    }

    /* saves the current config and parameter flags */
    chglog_reader_config = *p_config;
    behavior_flags = flags;

    /* create thread params */
    reader_info = (reader_thr_info_t*)MemCalloc(p_config->mdt_count,
                                                sizeof( reader_thr_info_t ));
    if ( reader_info == NULL )
        return ENOMEM;

#ifdef _LLAPI_FORKS
    /* initialize sigchild handler */
    memset( &act_sigchld, 0, sizeof( act_sigchld ) ) ;
    act_sigchld.sa_flags = 0 ;
    act_sigchld.sa_handler = action_sigchld ;
    if( sigaction( SIGCHLD, &act_sigchld, NULL ) == -1 )
    {
        DisplayLog(LVL_CRIT, CHGLOG_TAG,
                   "ERROR: Could not initialize SIGCHLD handler: %s",
                   strerror(errno) );
        return errno;
    }
    DisplayLog(LVL_DEBUG, CHGLOG_TAG,
               "Ready to trap SIGCHLD from liblustreapi child process" );
#endif

    Alert_StartBatching();

    /* create one reader per MDT */
    for ( i = 0; i < p_config->mdt_count ; i++ )
    {

        /* retrieve from the first unacknowledged record */
        unsigned long long last_rec = 0;

        memset(&reader_info[i], 0, sizeof(sizeof( reader_thr_info_t )));
        reader_info[i].thr_index = i;
        rh_list_init(&reader_info[i].op_queue);
        reader_info[i].last_report = time(NULL);
        reader_info->id_hash = id_hash_init( ID_CHGLOG_HASH_SIZE, FALSE );

        snprintf( mdtdevice, 128, "%s-%s", get_fsname(),
                  p_config->mdt_def[i].mdt_name );

        DisplayLog( LVL_DEBUG, CHGLOG_TAG, "Opening chglog for %s", mdtdevice );

        /* open the changelog (if we are in one_shot mode,
         * don't use the CHANGELOG_FLAG_FOLLOW flag)
         */
        rc = llapi_changelog_start( &reader_info[i].chglog_hdlr,
                                    ((one_shot || p_config->force_polling)?0:CHANGELOG_FLAG_FOLLOW)
                                    | CHANGELOG_FLAG_BLOCK,
                                    mdtdevice, last_rec );

        if ( rc )
        {
                DisplayLog(LVL_CRIT, CHGLOG_TAG,
                    "ERROR %d opening changelog for MDT '%s': %s",
                    rc, mdtdevice, strerror(abs(rc)) );
                return abs(rc);
        }

        /* then create the thread that manages it */
        if ( pthread_create(&reader_info[i].thr_id, NULL, chglog_reader_thr,
                            (void*)&reader_info[i]) )
        {
            int err = errno;
            DisplayLog(LVL_CRIT, CHGLOG_TAG,
                "ERROR creating ChangeLog reader thread: %s",
                strerror(err) );
            return err;
        }

    }

    return 0;
}


/** terminate ChangeLog Readers */
int            ChgLogRdr_Terminate( void )
{
    unsigned int i;

    /* ask threads to stop */
    for ( i = 0; i < chglog_reader_config.mdt_count; i++ )
    {
        reader_info[i].force_stop = TRUE;
    }

    DisplayLog( LVL_EVENT, CHGLOG_TAG,
                "Stop request has been sent to all ChangeLog reader threads" );

    ChgLogRdr_Wait(  );

    return 0;
}


/** wait for ChangeLog Readers termination */
int            ChgLogRdr_Wait( void )
{
    int i;
    void * ret;

    for ( i = 0; i < chglog_reader_config.mdt_count; i++ )
    {
        pthread_join( reader_info[i].thr_id, &ret );
    }

    Alert_EndBatching();

    return 0;
}

/** Release last changelog records, and dump the final stats. */
int            ChgLogRdr_Done( void )
{
    int i;

    for ( i = 0; i < chglog_reader_config.mdt_count; i++ )
    {
        reader_thr_info_t * info = &reader_info[i];

        /* Clear the records that are still batched for clearing. */
        clear_changelog_records(info);

        log_close(info);
    }

    ChgLogRdr_DumpStats();

    return 0;
}

/** dump changelog processing stats */
int            ChgLogRdr_DumpStats( void )
{
    unsigned int i,j;
    char tmp_buff[256];
    char * ptr;
    struct tm paramtm;

    /* ask threads to stop */

    for ( i = 0; i < chglog_reader_config.mdt_count; i++ )
    {
        double speed, speed2;
        unsigned int interval, interval2 = 0;

        DisplayLog( LVL_MAJOR, "STATS", "ChangeLog reader #%u:", i );

        DisplayLog( LVL_MAJOR, "STATS", "   fs_name    =   %s",
                    get_fsname() );
        DisplayLog( LVL_MAJOR, "STATS", "   mdt_name   =   %s",
                    chglog_reader_config.mdt_def[i].mdt_name );
        DisplayLog( LVL_MAJOR, "STATS", "   reader_id  =   %s",
                    chglog_reader_config.mdt_def[i].reader_id );
        DisplayLog( LVL_MAJOR, "STATS", "   records read        = %llu",
                    reader_info[i].nb_read );
        DisplayLog( LVL_MAJOR, "STATS", "   interesting records = %llu",
                    reader_info[i].interesting_records );
        DisplayLog( LVL_MAJOR, "STATS", "   suppressed records  = %llu",
                    reader_info[i].suppressed_records );
        DisplayLog( LVL_MAJOR, "STATS", "   records pending     = %u",
                    reader_info[i].op_queue_count );

        if (reader_info[i].nb_read)
        {
            time_t now = time(NULL);

            strftime( tmp_buff, 256, "%Y/%m/%d %T",
                      localtime_r( &reader_info[i].last_read_time, &paramtm ) );
            DisplayLog( LVL_MAJOR, "STATS", "   last received            = %s", tmp_buff );

            strftime( tmp_buff, 256, "%Y/%m/%d %T",
                      localtime_r( &reader_info[i].last_read_record_time.tv_sec, &paramtm ) );
            DisplayLog( LVL_MAJOR, "STATS", "   last read record time    = %s.%06u",
                        tmp_buff, (unsigned int)reader_info[i].last_read_record_time.tv_usec );

            DisplayLog( LVL_MAJOR, "STATS", "   last read record id      = %llu",
                        reader_info[i].last_read_record );
            DisplayLog( LVL_MAJOR, "STATS", "   last committed record id = %llu",
                        reader_info[i].last_committed_record );
            DisplayLog( LVL_MAJOR, "STATS", "   last cleared record id   = %llu",
                        reader_info[i].last_cleared_record );

            if (reader_info[i].last_report_record_id && (now > reader_info[i].last_report))
            {
                interval = now - reader_info[i].last_report;
                /* interval except time for reopening */
                interval2 = interval - (reader_info[i].nb_reopen - reader_info[i].last_reopen)
                            * chglog_reader_config.polling_interval;

                /* compute speed (rec/sec) */
                speed = (double)(reader_info[i].last_read_record - reader_info[i].last_report_record_id)/(double)interval;
                if ((interval2 != 0) && (interval2 != interval))
                {
                    speed2 = (double)(reader_info[i].last_read_record - reader_info[i].last_report_record_id)/(double)interval2;
                    DisplayLog( LVL_MAJOR, "STATS", "   read speed               = %.2f record/sec (%.2f incl. idle time)", speed2, speed );
                }
                else
                    DisplayLog( LVL_MAJOR, "STATS", "   read speed               = %.2f record/sec", speed );

                /* compute relative speed (sec/sec) or (h/h) or (d/d) */
                speed = (double)(reader_info[i].last_read_record_time.tv_sec + reader_info[i].last_read_record_time.tv_usec*0.000001
                                 - reader_info[i].last_report_record_time.tv_sec - reader_info[i].last_report_record_time.tv_usec*0.000001)/(double)interval;
                DisplayLog( LVL_MAJOR, "STATS", "   processing speed ratio   = %.2f", speed );
            }

        }

        if ( reader_info[i].force_stop )
            DisplayLog( LVL_MAJOR, "STATS", "   status                   = terminating");
        else if (interval2 == 0) /* spends its time polling */
        {
            /* more than a single record read? */
            if (reader_info[i].last_read_record - reader_info[i].last_report_record_id > 1)
                DisplayLog( LVL_MAJOR, "STATS", "   status                   = almost idle");
            else
                DisplayLog( LVL_MAJOR, "STATS", "   status                   = idle");
        }
        else if (reader_info[i].nb_reopen == reader_info[i].last_reopen) /* no reopen: it is busy reading changelogs */
            DisplayLog( LVL_MAJOR, "STATS", "   status                   = busy");

        DisplayLog( LVL_MAJOR, "STATS", "   ChangeLog stats:");

        tmp_buff[0] = '\0';
        ptr = tmp_buff;
        for (j = 0; j < CL_LAST; j++)
        {
            /* flush full line */
            if (ptr - tmp_buff >= 80)
            {
                DisplayLog( LVL_MAJOR, "STATS", "   %s", tmp_buff );
                tmp_buff[0] = '\0';
                ptr = tmp_buff;
            }
            if (ptr != tmp_buff)
                ptr += sprintf( ptr, ", ");

            ptr += sprintf( ptr, "%s: %llu", changelog_type2str(j),
                            reader_info[i].cl_counters[j] );
        }
        /* last unflushed line */
        if (ptr != tmp_buff)
            DisplayLog( LVL_MAJOR, "STATS", "   %s", tmp_buff );
    }

    return 0;
}

/** store changelog stats to the database */
int            ChgLogRdr_StoreStats( lmgr_t * lmgr )
{
    unsigned int i;
    char tmp_buff[256];
    struct tm paramtm;

    /* ask threads to stop */

    if ( chglog_reader_config.mdt_count > 1 )
        DisplayLog( LVL_MAJOR, CHGLOG_TAG, "WARNING: more than 1 MDT changelog reader, only 1st reader stats will be stored in DB" );
    else if ( chglog_reader_config.mdt_count < 1 )
        return ENOENT; /* nothing to be stored */

    sprintf( tmp_buff, "%llu", reader_info[0].last_read_record );
    ListMgr_SetVar( lmgr, CL_LAST_READ_REC_ID, tmp_buff );
    reader_info[0].last_report_record_id = reader_info[0].last_read_record;

    strftime( tmp_buff, 256, "%Y/%m/%d %T",
              localtime_r( &reader_info[0].last_read_record_time.tv_sec, &paramtm ) );
    sprintf( tmp_buff, "%s.%06u", tmp_buff, (unsigned int)reader_info[0].last_read_record_time.tv_usec );
    ListMgr_SetVar( lmgr, CL_LAST_READ_REC_TIME, tmp_buff );
    reader_info[0].last_report_record_time = reader_info[0].last_read_record_time;

    strftime( tmp_buff, 256, "%Y/%m/%d %T",
              localtime_r( &reader_info[0].last_read_time, &paramtm ) );
    ListMgr_SetVar( lmgr, CL_LAST_READ_TIME, tmp_buff );

    sprintf( tmp_buff, "%llu", reader_info[0].last_committed_record );
    ListMgr_SetVar( lmgr, CL_LAST_COMMITTED, tmp_buff );


    for (i = 0; i < CL_LAST; i++)
    {
        /* get and set (increment) */
        char varname[256];
        char last_val[256];
        unsigned long long last, current, diff;
        sprintf( varname, "%s_%s", CL_COUNT_PREFIX, changelog_type2str(i) );
        if ( ListMgr_GetVar( lmgr, varname, last_val ) != DB_SUCCESS )
            last = 0;
        else
            last = str2bigint(last_val);

        /* diff = current - last_reported */
        current = reader_info[0].cl_counters[i];
        diff = current - reader_info[0].cl_reported[i];

        /* new value = last + diff */
        sprintf( tmp_buff, "%llu", last + diff );
        if ( ListMgr_SetVar( lmgr, varname, tmp_buff ) == DB_SUCCESS )
            /* last_reported is now current */
            reader_info[0].cl_reported[i] = current;

        /* save diff */
        sprintf( varname, "%s_%s", CL_DIFF_PREFIX, changelog_type2str(i) );
        sprintf( tmp_buff, "%llu", diff );
        ListMgr_SetVar( lmgr, varname, tmp_buff );
    }

    /* indicate diff interval */
    sprintf( tmp_buff, "%lu", time(NULL) - reader_info[0].last_report );
    ListMgr_SetVar( lmgr, CL_DIFF_INTERVAL, tmp_buff );
    reader_info[0].last_report = time(NULL);

    reader_info[0].last_reopen = reader_info[0].nb_reopen;

    return 0;
}

