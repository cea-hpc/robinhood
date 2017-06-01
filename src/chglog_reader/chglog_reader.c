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
#include "rbh_logs.h"
#include "entry_processor.h"
#include "entry_proc_hash.h"
#include "rbh_misc.h"
#include "global_config.h"
#include "rbh_cfg_helpers.h"
#include "chglog_reader.h"

#include <pthread.h>
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <glib.h>
#include "lustre_extended_types.h"

#ifdef _LLAPI_FORKS
#include <signal.h>
#include <sys/wait.h>
#endif

/* for logs */
#define CHGLOG_TAG  "ChangeLog"

struct rec_stats {
    /** index of the record */
    uint64_t        rec_id;
    /** timestamp of the record */
    struct timeval  rec_time;
    /** when the record reached the current processing step */
    struct timeval  step_time;

    /* to compute speed between reports */
    uint64_t        last_report_rec_id;
    struct timeval  last_report_rec_time;
};

/** convert changelog record stats to timeval */
static void timeval_from_rec(struct timeval *tv, CL_REC_TYPE *logrec)
{
    tv->tv_sec = (time_t)cltime2sec(logrec->cr_time);
    tv->tv_usec = (time_t)cltime2nsec(logrec->cr_time) / 1000;
}

/** update record stats */
static void update_rec_stats(struct rec_stats *rs, CL_REC_TYPE *logrec)
{
    rs->rec_id = logrec->cr_index;
    timeval_from_rec(&rs->rec_time, logrec);
    gettimeofday(&rs->step_time, NULL);

    /* if no record has been reported, save this one - 1 as the previous last */
    if (rs->last_report_rec_id == 0) {
        rs->last_report_rec_id = rs->rec_id - 1;
        rs->last_report_rec_time = rs->rec_time;
    }
}

/* reader thread info, one per MDT */
typedef struct reader_thr_info_t {
    /** reader thread index */
    unsigned int thr_index;

    /** thread id */
    pthread_t thr_id;

    /** open information */
    char *mdtdevice;
    int flags;

    /** nbr of records read by this thread */
    unsigned long long nb_read;

    /** number of records of interest (ie. not MARK, IOCTL, ...) */
    unsigned long long interesting_records;

    /** number of suppressed/merged records */
    unsigned long long suppressed_records;

    /** last record read from the changelog */
    struct rec_stats last_read;
    /** last record pushed to the pipeline */
    struct rec_stats last_push;
    /** last record commited to the DB */
    struct rec_stats last_commit;
    /** last commit id saved to the DB */
    struct rec_stats last_commit_update;
    /** last record cleared from the changelog */
    struct rec_stats last_clear;

    /* number of times the changelog has been reopened */
    unsigned int nb_reopen;

    /** thread was asked to stop */
    unsigned int force_stop:1;

    /** log handler */
    void *chglog_hdlr;

    /** Queue of pending changelogs to push to the pipeline. */
    struct rh_list_head op_queue;
    unsigned int op_queue_count;

    /** Store the ops for easier access. Each element in the hash
     * table is also in the op_queue list. This hash table doesn't
     * need a lock per slot since there is only one reader. The
     * slot counts won't be used either. */
    struct id_hash *id_hash;

    ull_t cl_counters[CL_LAST]; /* since program start time */
    ull_t cl_reported[CL_LAST]; /* last reported stat (for incremental diff) */
    time_t last_report;

    unsigned int last_reopen;

    /** On pre LU-1331 versions of Lustre, a CL_RENAME is always
     * followed by a CL_EXT, however these may not be
     * contiguous. Temporarily store the CL_RENAME changelog until we
     * get the CL_EXT. */
    CL_REC_TYPE *cl_rename;

} reader_thr_info_t;

/* Number of entries in each readers' op hash table. */
#define ID_CHGLOG_HASH_SIZE 7919

extern chglog_reader_config_t cl_reader_config;
static run_flags_t behavior_flags = 0;

/* stop reading logs when reaching end of file? */
#define one_shot (behavior_flags & RUNFLG_ONCE)

/** array of reader info */
static reader_thr_info_t *reader_info = NULL;

/**
 * Close the changelog for a thread.
 */
static int log_close(reader_thr_info_t *p_info)
{
    int rc;

    /* close the log and clear input buffers */
    rc = llapi_changelog_fini(&p_info->chglog_hdlr);

    if (rc)
        DisplayLog(LVL_CRIT, CHGLOG_TAG, "Error %d closing changelog: %s",
                   rc, strerror(abs(rc)));

    return abs(rc);
}

/**
 * Free allocated structures in op_extra_info_t field.
 */
static void free_extra_info(void *ptr)
{
    op_extra_info_t *p_info = (op_extra_info_t *)ptr;

    if (p_info->is_changelog_record && p_info->log_record.p_log_rec) {
        llapi_changelog_free(&p_info->log_record.p_log_rec);
    }
}

static void free_extra_info2(void *ptr)
{
    op_extra_info_t *p_info = (op_extra_info_t *)ptr;

    if (p_info->is_changelog_record && p_info->log_record.p_log_rec) {
        /* if this is a locally allocated record, just "free" it */
        free(p_info->log_record.p_log_rec);
        p_info->log_record.p_log_rec = NULL;
    }
}

/**
 * Clear the changelogs up to the last committed number seen.
 */
static int clear_changelog_records(reader_thr_info_t *p_info)
{
    int rc;
    const char *reader_id;

    if (p_info->last_commit.rec_id == 0) {
        /* No record was ever committed. Stop here because calling
         * llapi_changelog_clear() with record 0 will clear all
         * records, leading to a potential record loss. */
        return 0;
    }

    reader_id = cl_reader_config.mdt_def[p_info->thr_index].reader_id;

    DisplayLog(LVL_DEBUG, CHGLOG_TAG,
               "%s: acknowledging ChangeLog records up to #%"PRIu64,
               p_info->mdtdevice, p_info->last_commit.rec_id);

    DisplayLog(LVL_FULL, CHGLOG_TAG, "llapi_changelog_clear('%s', '%s', %"PRIu64")",
               p_info->mdtdevice, reader_id,
               p_info->last_commit.rec_id);

    rc = llapi_changelog_clear(p_info->mdtdevice, reader_id,
                               p_info->last_commit.rec_id);

    if (rc) {
        DisplayLog(LVL_CRIT, CHGLOG_TAG,
                   "ERROR: llapi_changelog_clear(\"%s\", \"%s\", %"PRIu64") "
                   "returned %d", p_info->mdtdevice, reader_id,
                   p_info->last_commit.rec_id, rc);
        return rc;
    }

    /* update info about last cleared record */
    p_info->last_clear.rec_id = p_info->last_commit.rec_id;
    p_info->last_clear.rec_time =  p_info->last_commit.rec_time;
    gettimeofday(&p_info->last_clear.step_time, NULL);

    return 0;
}

/**
 * Store the information about changelog processing
 */
static int store_rec_stats(lmgr_t *lmgr, const reader_thr_info_t *info,
                           const char *var_prefix, const struct rec_stats *rs)
{
    const char *mdt = cl_reader_config.mdt_def[info->thr_index].mdt_name;
    char *var = NULL;
    char *val = NULL;
    int rc;

    /* Don't override previous values in the DB if no record has been
     * processed by current robinhood instance. */
    if (rs->rec_id == 0)
        return 0;

    if (asprintf(&var, "%s_%s", var_prefix, mdt) == -1 || var == NULL)
        return errno ? -errno : -ENOMEM;

    /* Format of value is rec_id:record_time(epoch.us):step_time(epoch.us) */
    if (asprintf(&val, "%"PRIu64":%lu.%lu:%lu.%lu", rs->rec_id,
                 rs->rec_time.tv_sec, rs->rec_time.tv_usec,
                 rs->step_time.tv_sec, rs->step_time.tv_usec)  == -1
        || val == NULL) {
        rc = errno ? -errno : -ENOMEM;
        goto out;
    }

    if (ListMgr_SetVar(lmgr, var, val)) {
        DisplayLog(LVL_MAJOR, CHGLOG_TAG,
                   "Failed to save %s record stats for %s", var_prefix, mdt);
        rc = -EIO;
        goto out;
    }

    rc = 0;

out:
    free(var);
    free(val);
    return rc;
}

/**
 * Store the last processed record (i.e. commited to the DB)
 * at regular interval.
 * @return true if the record id was saved, false in other cases.
 */
static bool store_last_commit(lmgr_t *lmgr, reader_thr_info_t *info, bool force)
{
    int64_t delta_id = info->last_commit.rec_id
                       - info->last_commit_update.rec_id;
    time_t delta_sec = info->last_commit.step_time.tv_sec
                       - info->last_commit_update.step_time.tv_sec;

    /** check update delays */
    if (!force && delta_id < cl_reader_config.commit_update_max_delta
        && delta_sec < cl_reader_config.commit_update_max_delay)
        return false;

    if (store_rec_stats(lmgr, info, CL_LAST_COMMITTED_REC, &info->last_commit))
        return false;

    info->last_commit_update.rec_id = info->last_commit.rec_id;
    info->last_commit_update.rec_time = info->last_commit.rec_time;
    gettimeofday(&info->last_commit_update.step_time, NULL);

    return true;
}

/** drop all old changelog stats */
static void drop_deprecated_changelog_vars(lmgr_t *lmgr, const char *mdt)
{
    char *var = NULL;
    int i;

    if (asprintf(&var, "%s_%s", CL_LAST_COMMITTED_OLD, mdt) == -1
        || var == NULL)
        return;
    ListMgr_SetVar(lmgr, var, NULL);
    free(var);

    ListMgr_SetVar(lmgr, CL_LAST_READ_REC_ID_OLD, NULL);
    ListMgr_SetVar(lmgr, CL_LAST_READ_REC_TIME_OLD, NULL);
    ListMgr_SetVar(lmgr, CL_LAST_READ_TIME_OLD, NULL);
    ListMgr_SetVar(lmgr, CL_LAST_COMMITTED_OLD, NULL);
    ListMgr_SetVar(lmgr, CL_DIFF_INTERVAL_OLD, NULL);

    for (i = 0; i < CL_LAST; i++) {
        if (asprintf(&var, "%s_%s", CL_COUNT_PREFIX_OLD, changelog_type2str(i))
            == -1 || var == NULL)
            continue;
        ListMgr_SetVar(lmgr, var, NULL);
        free(var);

        if (asprintf(&var, "%s_%s", CL_DIFF_PREFIX_OLD, changelog_type2str(i))
            == -1 || var == NULL)
            continue;
        ListMgr_SetVar(lmgr, var, NULL);
        free(var);
    }
}


/**
 * Retrieve the old variable of last committed changelog record,
 * and store it as the new name.
 * @return 0 if the information is not available.
 */
static uint64_t retrieve_old_commit(lmgr_t *lmgr, const reader_thr_info_t *info)
{
    const char *mdt = cl_reader_config.mdt_def[info->thr_index].mdt_name;
    char *var = NULL;
    char val_str[128];
    struct rec_stats rs = {0};
    uint64_t rec_id;

    if (asprintf(&var, "%s_%s", CL_LAST_COMMITTED_OLD, mdt) == -1
        || var == NULL)
        return 0;

    if (ListMgr_GetVar(lmgr, var, val_str, sizeof(val_str)) != DB_SUCCESS) {
        free(var);
        return 0;
    }

    rec_id = str2bigint(val_str);
    if (rec_id == -1LL)
        rec_id = 0;

    DisplayLog(LVL_EVENT, CHGLOG_TAG, "Old variable '%s' detected: replacing "
               "it by '%s_%s'", var, CL_LAST_COMMITTED_REC, mdt);
    free(var);
    rs.rec_id = rec_id;

    if (store_rec_stats(lmgr, info, CL_LAST_COMMITTED_REC, &rs) == 0)
        /* don't drop old variables if the new one could not be set */
        drop_deprecated_changelog_vars(lmgr, mdt);

    return rec_id;
}

/**
 * Retrieve last committed record for the given reader.
 * @return 0 if the information is not available.
 */
static uint64_t retrieve_last_commit(lmgr_t *lmgr,
                                     const reader_thr_info_t *info)
{
    const char *mdt = cl_reader_config.mdt_def[info->thr_index].mdt_name;
    char val_str[MAX_VAR_LEN];
    int64_t last_rec;
    char *var = NULL;
    char *sv = NULL;
    char *tok;

    if (asprintf(&var, "%s_%s", CL_LAST_COMMITTED_REC, mdt) == -1
        || var == NULL)
        return 0;

    if (ListMgr_GetVar(lmgr, var, val_str, sizeof(val_str)) != DB_SUCCESS) {
        free(var);
        /* try with the old name */
        return retrieve_old_commit(lmgr, info);
    }
    free(var);

    tok = strtok_r(val_str, ":", &sv);
    if (tok == NULL)
        return 0;

    last_rec = str2bigint(tok);
    if (last_rec == -1LL)
        last_rec = 0;

    return last_rec;
}

/**
 * DB callback function: this is called when a given ChangeLog record
 * has been successfully applied to the database.
 */
static int log_record_callback(lmgr_t *lmgr, struct entry_proc_op_t *pop,
                               void *param)
{
    reader_thr_info_t *info = (reader_thr_info_t *)param;
    CL_REC_TYPE *logrec = pop->extra_info.log_record.p_log_rec;
    bool saved;
    int rc;

    /** Check that a log record is set for this entry
     * (should always be the case).
     */
    if (!pop->extra_info.is_changelog_record || (logrec == NULL)) {
        DisplayLog(LVL_CRIT, CHGLOG_TAG, "Error: log record callback function"
                   " has been called for a non-changelog entry");
        return EINVAL;
    }

    /* update info about the last committed record */
    update_rec_stats(&info->last_commit, logrec);

    /* Save the last committed record so robinhood doesn't get old records
     * when restarting (especially if there are multiple changelog readers). */
    saved = store_last_commit(lmgr, info, false);

    /* batching llapi_changelog_clear() calls.
     * clear the record in any of those cases:
     *      - batch_ack_count = 1 (i.e. acknowledge every record).
     *      - we reached the last pushed record.
     *      - if the delta to last cleared record is high enough.
     * do nothing in all other cases:
     */
    if ((cl_reader_config.batch_ack_count > 1)
        && (logrec->cr_index < info->last_push.rec_id)
        && ((logrec->cr_index - info->last_clear.rec_id)
            < cl_reader_config.batch_ack_count)) {
        DisplayLog(LVL_FULL, CHGLOG_TAG, "callback - %s cl_record: %llu, "
                   "last_cleared: %"PRIu64", last_pushed: %"PRIu64,
                   info->mdtdevice, logrec->cr_index,
                   info->last_clear.rec_id, info->last_push.rec_id);
        /* do nothing, don't clear log now */
        return 0;
    }

    rc = clear_changelog_records(info);

    /* Always save the last commit after clearing records. This avoids
     * clearing records twice. */
    if (!saved)
        store_last_commit(lmgr, info, true);

    return rc;
}

#ifdef _LUSTRE_HSM
static const char *get_event_name(unsigned int cl_event)
{
    static const char * const event_name[] = {
        "archive", "restore", "cancel", "release", "remove", "state",
    };

    if (cl_event >= G_N_ELEMENTS(event_name))
        return "unknown";
    else
        return event_name[cl_event];
}
#endif

#define CL_BASE_FORMAT "%s: %llu %02d%-5s %u.%09u 0x%x%s t="DFID
#define CL_BASE_ARG(_mdt, _rec_) (_mdt), (_rec_)->cr_index, (_rec_)->cr_type, \
                                 changelog_type2str((_rec_)->cr_type),        \
                                 (uint32_t)cltime2sec((_rec_)->cr_time),      \
                                 cltime2nsec((_rec_)->cr_time),               \
                                 (_rec_)->cr_flags & CLF_FLAGMASK, flag_buff, \
                                 PFID(&(_rec_)->cr_tfid)
#define CL_NAME_FORMAT "p="DFID" %.*s"
#define CL_NAME_ARG(_rec_) PFID(&(_rec_)->cr_pfid), (_rec_)->cr_namelen, \
        rh_get_cl_cr_name(_rec_)

#if defined(HAVE_CHANGELOG_EXTEND_REC) || defined(HAVE_FLEX_CL)
#define CL_EXT_FORMAT   "s="DFID" sp="DFID" %.*s"
#endif

/* Dump a single record. */
static void dump_record(int debug_level, const char *mdt,
                        const CL_REC_TYPE *rec)
{
    char flag_buff[256] = "";
    char record_str[RBH_PATH_MAX] = "";
    char *curr = record_str;
    int len;
    int left = sizeof(record_str);

    /* No need to go further if the log level is not right. */
    if (EMPTY_STRING(log_config.changelogs_file) &&
        log_config.debug_level < debug_level)
        return;

#ifdef _LUSTRE_HSM
    if (rec->cr_type == CL_HSM)
        g_snprintf(flag_buff, sizeof(flag_buff), "(%s%s,rc=%d)",
                   get_event_name(hsm_get_cl_event(rec->cr_flags)),
                   hsm_get_cl_flags(rec->
                                    cr_flags) & CLF_HSM_DIRTY ? ",dirty" : "",
                   hsm_get_cl_error(rec->cr_flags));
#endif

    len = snprintf(curr, left, CL_BASE_FORMAT, CL_BASE_ARG(mdt, rec));
    curr += len;
    left -= len;
    if (left > 0 && rec->cr_namelen) {
        /* this record has a 'name' field. */
        len = snprintf(curr, left, " " CL_NAME_FORMAT, CL_NAME_ARG(rec));
        curr += len;
        left -= len;
    }

    if (left > 0) {
#if defined(HAVE_FLEX_CL)
        /* Newer versions. The cr_sfid is not directly in the
         * changelog record anymore. CLF_RENAME is always present for
         * backward compatibility; it describes the format of the
         * record, but the rename extension will be zero'ed for
         * non-rename records...
         */
        if (rec->cr_flags & CLF_RENAME) {
            struct changelog_ext_rename *cr_rename;

            cr_rename = changelog_rec_rename((CL_REC_TYPE *)rec);
            if (fid_is_sane(&cr_rename->cr_sfid)) {
                len = snprintf(curr, left, " " CL_EXT_FORMAT,
                               PFID(&cr_rename->cr_sfid),
                               PFID(&cr_rename->cr_spfid),
                               (int)changelog_rec_snamelen((CL_REC_TYPE *)rec),
                               changelog_rec_sname((CL_REC_TYPE *)rec));
                curr += len;
                left -= len;
            }
        }
        if (rec->cr_flags & CLF_JOBID) {
            struct changelog_ext_jobid *jobid =
                changelog_rec_jobid((CL_REC_TYPE *)rec);

            len = snprintf(curr, left, " J=%s", jobid->cr_jobid);
            curr += len;
            left -= len;
        }
#elif defined(HAVE_CHANGELOG_EXTEND_REC)
        if (fid_is_sane(&rec->cr_sfid)) {
            len = snprintf(curr, left, " " CL_EXT_FORMAT,
                           PFID(&rec->cr_sfid),
                           PFID(&rec->cr_spfid),
                           changelog_rec_snamelen((CL_REC_TYPE *)rec),
                           changelog_rec_sname((CL_REC_TYPE *)rec));
            curr += len;
            left -= len;
        }
#endif
    }

    if (left <= 0)
        record_str[RBH_PATH_MAX - 1] = '\0';

    DisplayLog(debug_level, CHGLOG_TAG, "%s", record_str);
    DisplayChangelogs("%s", record_str);
}

/* Dumps the nth most recent entries in the queue. If -1, dump them
 * all. */
static void dump_op_queue(reader_thr_info_t *p_info, int debug_level, int num)
{
    entry_proc_op_t *op;

    if (log_config.debug_level < debug_level || num == 0)
        return;

    rh_list_for_each_entry_reverse(op, &p_info->op_queue, list) {
        dump_record(debug_level, op->extra_info.log_record.mdt,
                    op->extra_info.log_record.p_log_rec);

        if (num != -1) {
            num--;
            if (num == 0)
                return;
        }
    }
}

/** extract parent_id and name attributes from the changelog record */
static void set_name(CL_REC_TYPE *logrec, entry_proc_op_t *p_op)
{
    /* is there entry name in log rec? */
    if (logrec->cr_namelen == 0)
        return;
    ATTR_MASK_SET(&p_op->fs_attrs, name);
    rh_strncpy(ATTR(&p_op->fs_attrs, name), rh_get_cl_cr_name(logrec),
               sizeof(ATTR(&p_op->fs_attrs, name)));

    /* parent id is always set when name is (Cf. comment in lfs.c) */
    if (fid_is_sane(&logrec->cr_pfid)) {
        ATTR_MASK_SET(&p_op->fs_attrs, parent_id);
        ATTR(&p_op->fs_attrs, parent_id) = logrec->cr_pfid;

        ATTR_MASK_SET(&p_op->fs_attrs, path_update);
        ATTR(&p_op->fs_attrs, path_update) = time(NULL);
    } else {
        DisplayLog(LVL_MAJOR, CHGLOG_TAG, "Error: insane parent fid " DFID
                   " in %s changelog record (namelen=%u)",
                   PFID(&logrec->cr_pfid),
                   changelog_type2str(logrec->cr_type), logrec->cr_namelen);
    }
}

/* Push the oldest (all=FALSE) or all (all=TRUE) entries into the pipeline. */
static void process_op_queue(reader_thr_info_t *p_info, bool push_all)
{
    time_t oldest = time(NULL) - cl_reader_config.queue_max_age;
    CL_REC_TYPE *rec;

    DisplayLog(LVL_FULL, CHGLOG_TAG, "processing changelog queue");

    while (!rh_list_empty(&p_info->op_queue)) {
        entry_proc_op_t *op =
            rh_list_first_entry(&p_info->op_queue, entry_proc_op_t, list);

        /* Stop when the queue is below our limit, and when the oldest
         * element is still new enough. */
        if (!push_all &&
            (p_info->op_queue_count < cl_reader_config.queue_max_size) &&
            (op->timestamp.changelog_inserted > oldest))
            break;

        rh_list_del(&op->list);
        rh_list_del(&op->id_hash_list);

        rec = op->extra_info.log_record.p_log_rec;
        DisplayLog(LVL_FULL, CHGLOG_TAG, "pushing cl record #%llu: age=%ld",
                   rec->cr_index,
                   time(NULL) - op->timestamp.changelog_inserted);

        /* Set parent_id+name from changelog record info, as they are used
         * in pipeline for stage locking. */
        set_name(rec, op);
        /* Push the entry to the pipeline */
        EntryProcessor_Push(op);

        update_rec_stats(&p_info->last_push, rec);
        p_info->op_queue_count--;
    }
}

/* Flags to insert_into_hash. */
#define PLR_FLG_FREE2       0x0001  /* must free changelog record
                                       on completion */
#define CHECK_IF_LAST_ENTRY 0x0002  /* check whether the unlinked file is
                                       the last one. */
#define GET_FID_FROM_DB     0x0004  /* fid is not valid, get it from DB */

/* Insert the operation into the internal hash table. */
static int insert_into_hash(reader_thr_info_t *p_info, CL_REC_TYPE *p_rec,
                            unsigned int flags)
{
    entry_proc_op_t *op;
    struct id_hash_slot *slot;

    op = EntryProcessor_Get();
    if (!op) {
        DisplayLog(LVL_CRIT, CHGLOG_TAG,
                   "CRITICAL ERROR: EntryProcessor_Get failed to allocate a new op");
        return -1;
    }

    /* first, it will check if it already exists in database */
    op->pipeline_stage = entry_proc_descr.GET_INFO_DB;

    /* set log record */
    op->extra_info_is_set = 1;
    op->extra_info.is_changelog_record = 1;
    op->extra_info.log_record.p_log_rec = p_rec;

    /* set mdt name */
    op->extra_info.log_record.mdt =
        cl_reader_config.mdt_def[p_info->thr_index].mdt_name;

    if (flags & PLR_FLG_FREE2)
        op->extra_info_free_func = free_extra_info2;
    else
        op->extra_info_free_func = free_extra_info;

    /* if the unlink record is not tagged as last unlink,
     * always check the previous value of nlink in DB */
    op->check_if_last_entry = (p_rec->cr_type == CL_UNLINK)
        && !(p_rec->cr_flags & CLF_UNLINK_LAST);
    op->get_fid_from_db = !!(flags & GET_FID_FROM_DB);

    /* set callback function + args */
    op->callback_func = log_record_callback;
    op->callback_param = p_info;

    /* Set entry ID */
    if (!op->get_fid_from_db)
        EntryProcessor_SetEntryId(op, &p_rec->cr_tfid);

    /* Add the entry on the pending queue ... */
    op->timestamp.changelog_inserted = time(NULL);
    rh_list_add_tail(&op->list, &p_info->op_queue);
    p_info->op_queue_count++;

    /* ... and the hash table. */
    slot = get_hash_slot(p_info->id_hash, &op->entry_id);
    rh_list_add_tail(&op->id_hash_list, &slot->list);

    return 0;
}

/* Describes which records can be safely ignored. By default a record
 * is never ignored. It is only necessary to add an entry in this
 * table if the record may be skipped (and thus has a mask defined) or
 * if it can be skipped altogether. */
static const struct {
    enum { IGNORE_NEVER = 0,    /* default */
        IGNORE_MASK,    /* mask must be set, and record has a FID */
        IGNORE_ALWAYS
    } ignore;
    unsigned int ignore_mask;
} record_filters[CL_LAST] = {

    /* Record we don't care about. */
    [CL_MARK] = { .ignore = IGNORE_ALWAYS },
#ifdef _HAVE_CL_IOCTL /* replaced by CL_LAYOUT in Lustre 2.5 */
    [CL_IOCTL] = { .ignore = IGNORE_ALWAYS },
#endif

    /* Similar operation (data changes). For instance, if the current
     * operation is a CLOSE, drop it if we find a previous
     * TRUNC/CLOSE/MTIME or CREATE for the same FID. */
    [CL_TRUNC] = { IGNORE_MASK, 1<<CL_TRUNC | 1<<CL_CLOSE | 1<<CL_MTIME
                   | 1<<CL_CREATE },
    [CL_CLOSE] = { IGNORE_MASK, 1<<CL_TRUNC | 1<<CL_CLOSE | 1<<CL_MTIME
                   | 1<<CL_CREATE },
    [CL_MTIME] = { IGNORE_MASK, 1<<CL_TRUNC | 1<<CL_CLOSE | 1<<CL_MTIME
                   | 1<<CL_CREATE | 1<<CL_MKNOD | 1<<CL_MKDIR },

    /* Similar operations (metadata changes). */
    [CL_CTIME] = { IGNORE_MASK, 1<<CL_CTIME | 1<<CL_SETATTR | 1<<CL_CREATE
                   | 1<<CL_MKNOD | 1<<CL_MKDIR },
    [CL_SETATTR] = { IGNORE_MASK, 1<<CL_CTIME | 1<<CL_SETATTR | 1<<CL_CREATE
                   | 1<<CL_MKNOD | 1<<CL_MKDIR },
};

/* Decides whether a new changelog record can be ignored. Ignoring a
 * record should not impact the database state, however the gain is to:
 *  - reduce contention on pipeline stages with constraints,
 *  - reduce the number of DB and FS requests.
 *
 * Returns TRUE or FALSE.
 */
static bool can_ignore_record(const reader_thr_info_t *p_info,
                              const CL_REC_TYPE *logrec_in)
{
    entry_proc_op_t *op, *t1;
    unsigned int ignore_mask;
    struct id_hash_slot *slot;

    if (record_filters[logrec_in->cr_type].ignore == IGNORE_NEVER)
        return false;

    if (record_filters[logrec_in->cr_type].ignore == IGNORE_ALWAYS)
        return true;

    /* The ignore field is IGNORE_MASK. At that point, the FID in the
     * changelog record must be set. All the changelog record with the
     * same FID will go into the same bucket, so parse that slot
     * instead of the whole op_queue list. */
    slot = get_hash_slot(p_info->id_hash, &logrec_in->cr_tfid);
    ignore_mask = record_filters[logrec_in->cr_type].ignore_mask;

    rh_list_for_each_entry_safe_reverse(op, t1, &slot->list, id_hash_list) {
        CL_REC_TYPE *logrec = op->extra_info.log_record.p_log_rec;

        /* If the type of record matches what we're looking for, and
         * it's for the same FID, then we can ignore the new
         * record. */
        if ((ignore_mask & (1 << logrec->cr_type)) &&
            entry_id_equal(&logrec->cr_tfid, &logrec_in->cr_tfid)) {

            /* if the matching record is n, and ignored record is n+1,
             * acknownledging(n) can also acknownledge(n+1),
             * as they refer to the same entry.
             */
            if (logrec_in->cr_index == logrec->cr_index + 1) {
                DisplayLog(LVL_FULL, CHGLOG_TAG,
                           "acknowledging %llu will acknowledge %llu too",
                           logrec->cr_index, logrec_in->cr_index);
                logrec->cr_index++;
            }
            return true;
        }
    }

    return false;
}

/**
 * Convert rename flags to unlink flags, depending on Lustre client/server
 * versions.
 * @param[in]     flags            cr_flags from rename changelog record.
 * @param[in,out] pipeline_flags   indicate if specific processing is needed
 *                                 in pipeline.
 */
static uint16_t cl_rename2unlink_flags(uint16_t flags,
                                       unsigned int *pipeline_flags)
{
    uint16_t retflg = 0;

#ifdef CLF_RENAME_LAST
    /* The client support LU-1331 (since CLF_RENAME_LAST is
     * defined) but that may not be the case of the server. */
    if (cl_reader_config.mds_has_lu1331) {
        if (flags & CLF_RENAME_LAST)
            retflg |= CLF_UNLINK_LAST;
#ifdef CLF_RENAME_LAST_EXISTS
        if (flags & CLF_RENAME_LAST_EXISTS)
            retflg |= CLF_UNLINK_HSM_EXISTS;
#endif

    } else
#endif
    {
        /* CLF_RENAME_LAST is not supported in this version of the
         * client and/or the server. The pipeline will have to
         * decide whether this is the last entry or not. */
        *pipeline_flags |= CHECK_IF_LAST_ENTRY;
    }

    if (!cl_reader_config.mds_has_lu543) {
        /* The server doesn't tell whether the rename operation will
         * remove a file. */
        *pipeline_flags |= GET_FID_FROM_DB;
    }

    return retflg;
}

/**
 * Create a fake unlink changelog record that will be used to remove a
 * file that is overriden during a rename operation.
 *
 * rec_in is a changelog of type CL_RENAME (if rename is recorded with
 * one changelog record) or CL_EXT (if rename is recorded with
 * CL_RENAME+CL_EXT). This function is called because the rename
 * operation is deleting the destination, so we need to insert a fake
 * CL_UNLINK into the pipeline for that operation.
 */
static CL_REC_TYPE *create_fake_unlink_record(const reader_thr_info_t *p_info,
                                              CL_REC_TYPE *rec_in,
                                              unsigned int *insert_flags)
{
    CL_REC_TYPE *rec;
    size_t name_len;

    /* Build a simple changelog record with no extension (jobid, rename...).
     * So, just allocate enough space for the record and the source name. */
    name_len = strlen(rh_get_cl_cr_name(rec_in));
    rec = MemAlloc(sizeof(CL_REC_TYPE) + name_len + 1);
    if (rec == NULL)
        return NULL;

    /* Copy the fix part of the changelog structure */
    memcpy(rec, rec_in, sizeof(CL_REC_TYPE));

    /* set target flags before using any accessor on it */
    rec->cr_flags = cl_rename2unlink_flags(rec_in->cr_flags, insert_flags);

    /* record has to be freed */
    *insert_flags |= PLR_FLG_FREE2;

    /* unlinked entry is the target name */
    memcpy(rh_get_cl_cr_name(rec), rh_get_cl_cr_name(rec_in), name_len);
    rh_get_cl_cr_name(rec)[name_len] = 0;   /* terminate string */
    rec->cr_namelen = name_len + 1;

    rec->cr_type = CL_UNLINK;
    rec->cr_index = rec_in->cr_index - 1;

    DisplayLog(LVL_DEBUG, CHGLOG_TAG,
               "Unlink: object=" DFID ", name=%.*s, flags=%#x",
               PFID(&rec->cr_tfid), rec->cr_namelen,
               rh_get_cl_cr_name(rec), rec->cr_flags);

    return rec;
}

#if defined(HAVE_CHANGELOG_EXTEND_REC) || defined(HAVE_FLEX_CL)
/**
 * Create a fake rename record to ensure compatibility with older
 * Lustre records.
 *
 * rec_in is a single rename record of type CL_RENAME; Lustre won't
 * issue a CL_EXT record for this rename. But RH's pipeline expects a
 * CL_RENAME followed by a CL_EXT record. So this function creates an
 * old fashion CL_RENAME that will be followed by a CL_EXT.
 *
 * This is only used if LU-1331 fix is present on the Lustre server.
 */
static CL_REC_TYPE *create_fake_rename_record(const reader_thr_info_t *p_info,
                                              CL_REC_TYPE *rec_in)
{
    CL_REC_TYPE *rec;
    size_t sname_len;

    /* Build a simple changelog record with no extension (jobid, rename...).
     * So, just allocate enough space for the record and the source name. */
    sname_len = changelog_rec_snamelen(rec_in);
    rec = MemAlloc(sizeof(CL_REC_TYPE) + sname_len + 1);
    if (rec == NULL)
        return NULL;

    /* Copy the fix part of the changelog structure */
    memcpy(rec, rec_in, sizeof(CL_REC_TYPE));

    /* set target flags before using any accessor on it */
    rec->cr_flags = 0;  /* simplest record */

    rec->cr_namelen = sname_len + 1;    /* add 1 for final NULL-byte */
    memcpy(rh_get_cl_cr_name(rec), changelog_rec_sname(rec_in), sname_len);
    rh_get_cl_cr_name(rec)[sname_len] = 0;  /* terminate string */

    /* we don't want to acknowledge this record as long as the 2
     * records are not processed. acknowledge n-1 instead */
    rec->cr_index = rec_in->cr_index - 1;

#ifdef HAVE_FLEX_CL
    {
        const struct changelog_ext_rename *cr_ren_in =
            changelog_rec_rename(rec_in);

        rec->cr_tfid = cr_ren_in->cr_sfid;  /* the renamed fid */
        rec->cr_pfid = cr_ren_in->cr_spfid; /* the source parent */
    }
#else
    rec->cr_tfid = rec_in->cr_sfid; /* the renamed fid */
    rec->cr_pfid = rec_in->cr_spfid;    /* the source parent */
#endif

    return rec;
}
#endif

#define mdtname(_info) (cl_reader_config.mdt_def[(_info)->thr_index].mdt_name)

/**
 * This handles a single log record.
 */
static int process_log_rec(reader_thr_info_t *p_info, CL_REC_TYPE *p_rec)
{
    unsigned int opnum;

    /* display the log record in debug mode */
    dump_record(LVL_DEBUG, mdtname(p_info), p_rec);

    /* update stats */
    opnum = p_rec->cr_type;
    if ((opnum >= 0) && (opnum < CL_LAST))
        p_info->cl_counters[opnum]++;
    else {
        DisplayLog(LVL_CRIT, CHGLOG_TAG,
                   "Log record type %d out of bounds.", opnum);
        return EINVAL;
    }

    /* This record might be of interest. But try to check whether it
     * might create a duplicate operation anyway. */
    if (can_ignore_record(p_info, p_rec)) {
        DisplayLog(LVL_FULL, CHGLOG_TAG, "Ignoring event %s",
                   changelog_type2str(opnum));
        DisplayChangelogs("(ignored redundant record %s:%llu)", mdtname(p_info),
                          p_rec->cr_index);
        p_info->suppressed_records++;
        llapi_changelog_free(&p_rec);
        goto done;
    }

    p_info->interesting_records++;

    if (p_rec->cr_type == CL_RENAME) {
        /* Ensure there is no pending rename. */
        if (p_info->cl_rename) {
            /* Should never happen. */
            DisplayLog(LVL_CRIT, CHGLOG_TAG,
                       "Got 2 CL_RENAME in a row without a CL_EXT.");
            dump_record(LVL_CRIT, mdtname(p_info), p_rec);
            dump_op_queue(p_info, LVL_CRIT, 32);

            /* Discarding bogus entry. */
            llapi_changelog_free(&p_info->cl_rename);
            p_info->cl_rename = NULL;
        }
#if defined(HAVE_CHANGELOG_EXTEND_REC) || defined(HAVE_FLEX_CL)
        /* extended record: 1 single RENAME record per rename op;
         * there is no EXT. */
        if (rh_is_rename_one_record(p_rec)) {
            CL_REC_TYPE *p_rec2;
#ifdef HAVE_FLEX_CL
            struct changelog_ext_rename *cr_ren;
#endif

            /* The MDS sent an extended record, so we have both LU-543
             * and LU-1331. */
            if (!cl_reader_config.mds_has_lu543 ||
                !cl_reader_config.mds_has_lu1331) {
                DisplayLog(LVL_EVENT, CHGLOG_TAG,
                           "LU-1331 is fixed in this version of Lustre.");

                cl_reader_config.mds_has_lu543 = true;
                cl_reader_config.mds_has_lu1331 = true;
            }

            if (!FID_IS_ZERO(&p_rec->cr_tfid)) {
                CL_REC_TYPE *unlink;
                unsigned int insert_flags;

                unlink = create_fake_unlink_record(p_info,
                                                   p_rec, &insert_flags);
                if (unlink) {
                    insert_into_hash(p_info, unlink, insert_flags);
                } else {
                    DisplayLog(LVL_CRIT, CHGLOG_TAG,
                               "Could not allocate an UNLINK record.");
                }
            }
#ifdef HAVE_FLEX_CL
            cr_ren = changelog_rec_rename(p_rec);
            DisplayLog(LVL_DEBUG, CHGLOG_TAG,
                       "Rename: object=" DFID ", old parent/name=" DFID
                       "/%.*s, new parent/name=" DFID "/%.*s",
                       PFID(&cr_ren->cr_sfid), PFID(&cr_ren->cr_spfid),
                       (int)changelog_rec_snamelen(p_rec),
                       changelog_rec_sname(p_rec), PFID(&p_rec->cr_pfid),
                       p_rec->cr_namelen, rh_get_cl_cr_name(p_rec));
#else
            DisplayLog(LVL_DEBUG, CHGLOG_TAG,
                       "Rename: object=" DFID ", old parent/name=" DFID
                       "/%s, new parent/name=" DFID "/%.*s",
                       PFID(&p_rec->cr_sfid), PFID(&p_rec->cr_spfid),
                       changelog_rec_sname(p_rec), PFID(&p_rec->cr_pfid),
                       p_rec->cr_namelen, rh_get_cl_cr_name(p_rec));
#endif

            /* Ensure compatibility with older Lustre versions:
             * push RNMFRM to remove the old path from NAMES table.
             * push RNMTO to add target path information.
             */
            /* 1) build & push RNMFRM */
            p_rec2 = create_fake_rename_record(p_info, p_rec);
            insert_into_hash(p_info, p_rec2, PLR_FLG_FREE2);

            /* 2) update RNMTO */
            p_rec->cr_type = CL_EXT;    /* CL_RENAME -> CL_RNMTO */
#ifdef HAVE_FLEX_CL
            p_rec->cr_tfid = cr_ren->cr_sfid;   /* removed fid -> renamed fid */
#else
            p_rec->cr_tfid = p_rec->cr_sfid;    /* removed fid -> renamed fid */
#endif
            insert_into_hash(p_info, p_rec, 0);
        } else
#endif
        {
            /* This CL_RENAME is followed by CL_EXT, so keep it until
             * then. */
            p_info->cl_rename = p_rec;
        }
    } else if (p_rec->cr_type == CL_EXT) {

        if (!p_info->cl_rename) {
            /* Should never happen. */
            DisplayLog(LVL_CRIT, CHGLOG_TAG, "Got CL_EXT without a CL_RENAME.");
            dump_record(LVL_CRIT, mdtname(p_info), p_rec);
            dump_op_queue(p_info, LVL_CRIT, 32);

            /* Discarding bogus entry. */
            llapi_changelog_free(&p_rec);

            goto done;
        }

        if (!cl_reader_config.mds_has_lu543 &&
            (FID_IS_ZERO(&p_rec->cr_tfid) ||
             !entry_id_equal(&p_info->cl_rename->cr_tfid, &p_rec->cr_tfid))) {
            /* tfid if 0, or the two fids are different, so we have LU-543. */
            cl_reader_config.mds_has_lu543 = true;
            DisplayLog(LVL_EVENT, CHGLOG_TAG,
                       "LU-543 is fixed in this version of Lustre.");
        }

        /* We now have a CL_RENAME and a CL_EXT. */
        /* If target fid is not zero: unlink the target.
         * e.g. "mv a b" and b exists => rm b.
         */
        if (!FID_IS_ZERO(&p_rec->cr_tfid)) {
            CL_REC_TYPE *unlink;
            unsigned int insert_flags;

            /* Push an unlink. */
            unlink = create_fake_unlink_record(p_info, p_rec, &insert_flags);

            if (unlink) {
                insert_into_hash(p_info, unlink, insert_flags);
            } else {
                DisplayLog(LVL_CRIT, CHGLOG_TAG,
                           "Could not allocate an UNLINK record.");
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
    } else {
        /* build the record to be processed in the pipeline */
        insert_into_hash(p_info, p_rec, 0);
    }

 done:
    return 0;
}

/* get a changelog line (with retries) */
typedef enum { cl_ok, cl_continue, cl_stop } cl_status_e;

static cl_status_e cl_get_one(reader_thr_info_t *info, CL_REC_TYPE **pp_rec)
{
    int rc;

    /* get next record */
    rc = llapi_changelog_recv(info->chglog_hdlr, pp_rec);

    if (!EMPTY_STRING(log_config.changelogs_file) && rc != 0 && rc != 1) {
        DisplayChangelogs(">>> llapi_changelog_recv returned error %d "
                          "(last record = %"PRIu64")", rc,
                          info->last_read.rec_id);
        FlushLogs();
    }

    switch (rc) {
    case 0:
        /* Successfully retrieved a record. Update last read record. */
        update_rec_stats(&info->last_read, *pp_rec);
        info->nb_read++;
        return cl_ok;

    case 1:    /* EOF */
    case -EINVAL:  /* FS unmounted */
    case -EPROTO:  /* error in KUC channel */

        /* warn if it is an error */
        if (rc != 1)
            DisplayLog(LVL_EVENT, CHGLOG_TAG,
                       "Error %d in llapi_changelog_recv(): %s. "
                       "Trying to reopen it.", rc, strerror(-rc));

        if (one_shot)
            return cl_stop;

        /* Close, wait and open the log again (from last_read_record + 1) */
        log_close(info);

        if (cl_reader_config.force_polling) {
            DisplayLog(LVL_FULL, CHGLOG_TAG,
                       "EOF reached on changelog from %s, reopening in %ld sec",
                       info->mdtdevice, cl_reader_config.polling_interval);
            /* sleep during polling interval */
            rh_sleep(cl_reader_config.polling_interval);
        } else {
            DisplayLog(LVL_EVENT, CHGLOG_TAG,
                       "WARNING: EOF reached on ChangeLog whereas FOLLOW flag "
                       "was specified. Re-opening in 1 sec...");
            rh_sleep(1);
        }

        info->nb_reopen++;

        rc = llapi_changelog_start(&info->chglog_hdlr, info->flags,
                                   info->mdtdevice, info->last_read.rec_id + 1);
        if (rc) {
            /* will try to recover from this error */
            rh_sleep(1);
        }

        return cl_continue;

    case -EINTR:
        DisplayLog(LVL_EVENT, CHGLOG_TAG,
                   "llapi_changelog_recv() interrupted. Retrying.");
        return cl_continue;

    default:
        DisplayLog(LVL_CRIT, CHGLOG_TAG,
                   "Error in llapi_changelog_recv(): %d: %s",
                   rc, strerror(abs(rc)));

        /* will try to recover from this error */
        rh_sleep(1);

        return cl_continue;
    }

    /* Unreachable */
    return cl_continue;
}

/** a thread that reads lines from a given changelog */
static void *chglog_reader_thr(void *arg)
{
    reader_thr_info_t *info = (reader_thr_info_t *)arg;
    CL_REC_TYPE *p_rec = NULL;
    cl_status_e st;
    /* Next time we will have to push. */
    time_t next_push_time = time(NULL) + cl_reader_config.queue_check_interval;

    /* loop until a TERM signal is caught */
    while (!info->force_stop) {
        /* Is it time to flush? */
        if (info->op_queue_count >= cl_reader_config.queue_max_size ||
            next_push_time <= time(NULL)) {
            process_op_queue(info, false);

            next_push_time = time(NULL) + cl_reader_config.queue_check_interval;

            if (!EMPTY_STRING(log_config.changelogs_file))
                FlushLogs();
        }

        st = cl_get_one(info, &p_rec);
        if (st == cl_continue)
            continue;
        else if (st == cl_stop)
            break;

        /* handle the line and push it to the pipeline */
        process_log_rec(info, p_rec);
    }

    /* Stopping. Flush the internal queue. */
    process_op_queue(info, true);

    DisplayLog(LVL_CRIT, CHGLOG_TAG, "Changelog reader thread terminating");
    FlushLogs();
    return NULL;

}

#ifdef _LLAPI_FORKS
/* In early Lustre 2.0 releases, llapi_changelog_start() forks a process
 * that keeps in <defunc> state.
 * So we work around this issue by trapping SIGCHILD signals.
 */
static void action_sigchld(int sig)
{
    pid_t child;
    do {
        /* wait for all terminated children
         * and stop on end of list or error.
         */
        child = waitpid(-1, NULL, WNOHANG);
    } while (child > 0);

}
#endif

/** start ChangeLog Reader module */
int cl_reader_start(run_flags_t flags, int mdt_index)
{
    int i, rc;
    char mdtdevice[128];
#ifdef _LLAPI_FORKS
    struct sigaction act_sigchld;
#endif

    for (i = 0; i < cl_reader_config.mdt_count; i++) {
        if (mdt_index == -1 || mdt_index == i)
            DisplayLog(LVL_FULL, CHGLOG_TAG, "mdt[%u] = %s", i,
                       cl_reader_config.mdt_def[i].mdt_name);
    }

    /* check parameters */
    if ((cl_reader_config.mdt_count == 0)
        || (cl_reader_config.mdt_def == NULL)) {
        DisplayLog(LVL_CRIT, CHGLOG_TAG,
                   "ERROR: no MDT ChangeLog has been defined in configuration");
        return EINVAL;
    }
#ifndef HAVE_DNE
    else if ((cl_reader_config.mdt_count > 1) || (mdt_index > 0)) {
        DisplayLog(LVL_CRIT, CHGLOG_TAG,
                   "ERROR: multiple MDTs are not supported with this version of Lustre");
        return ENOTSUP;
    }
#endif
    else if (mdt_index >= (int)cl_reader_config.mdt_count) {
        DisplayLog(LVL_CRIT, CHGLOG_TAG,
                   "The specified mdt_index (%d) exceeds the MDT count in configuration file (%u)",
                   mdt_index, cl_reader_config.mdt_count);
        return EINVAL;
    }

    if (mdt_index != -1) {
        /* hack the configuration structure to keep only the specified MDT */
        if (mdt_index != 0)
            cl_reader_config.mdt_def[0] = cl_reader_config.mdt_def[mdt_index];
        cl_reader_config.mdt_count = 1;
        DisplayLog(LVL_MAJOR, CHGLOG_TAG,
                   "Starting changelog reader only for %s, as specified by command line",
                   cl_reader_config.mdt_def[0].mdt_name);
    }

    /* saves the current config and parameter flags */
    behavior_flags = flags;

    /* create thread params */
    reader_info = (reader_thr_info_t *)MemCalloc(cl_reader_config.mdt_count,
                                                  sizeof(reader_thr_info_t));

    if (reader_info == NULL)
        return ENOMEM;

#ifdef _LLAPI_FORKS
    /* initialize sigchild handler */
    memset(&act_sigchld, 0, sizeof(act_sigchld));
    act_sigchld.sa_flags = 0;
    act_sigchld.sa_handler = action_sigchld;
    if (sigaction(SIGCHLD, &act_sigchld, NULL) == -1) {
        DisplayLog(LVL_CRIT, CHGLOG_TAG,
                   "ERROR: Could not initialize SIGCHLD handler: %s",
                   strerror(errno));
        return errno;
    }
    DisplayLog(LVL_DEBUG, CHGLOG_TAG,
               "Ready to trap SIGCHLD from liblustreapi child process");
#endif

    Alert_StartBatching();

    // need a connection to get last committed record
    lmgr_t lmgr;
    int dbget = 1;
    rc = ListMgr_InitAccess(&lmgr);
    if (rc)
        dbget = 0;

    /* create one reader per MDT */
    for (i = 0; i < cl_reader_config.mdt_count; i++) {
        reader_thr_info_t *info = &reader_info[i];

        /* retrieve from the first unacknowledged record */
        unsigned long long last_rec = 0;

        memset(info, 0, sizeof(reader_thr_info_t));
        info->thr_index = i;
        rh_list_init(&info->op_queue);
        info->last_report = time(NULL);
        info->id_hash = id_hash_init(ID_CHGLOG_HASH_SIZE, false);

        snprintf(mdtdevice, 128, "%s-%s", get_fsname(),
                 cl_reader_config.mdt_def[i].mdt_name);

        info->mdtdevice = strdup(mdtdevice);
        info->flags =
            ((one_shot
              || cl_reader_config.force_polling) ? 0 : CHANGELOG_FLAG_FOLLOW)
            | CHANGELOG_FLAG_BLOCK;

        if (dbget) {
            last_rec = retrieve_last_commit(&lmgr, info);
            if (last_rec != 0)
                /* start rec = last rec + 1 */
                last_rec++;
        }
        DisplayLog(LVL_DEBUG, CHGLOG_TAG,
                   "Opening chglog for %s (start_rec=%llu)", mdtdevice,
                   last_rec);

        /* open the changelog (if we are in one_shot mode,
         * don't use the CHANGELOG_FLAG_FOLLOW flag)
         */
        rc = llapi_changelog_start(&info->chglog_hdlr,
                                   info->flags, info->mdtdevice, last_rec);

        if (rc) {
            DisplayLog(LVL_CRIT, CHGLOG_TAG,
                       "ERROR %d opening changelog for MDT '%s': %s",
                       rc, mdtdevice, strerror(abs(rc)));
            return abs(rc);
        }

        /* then create the thread that manages it */
        if (pthread_create(&info->thr_id, NULL, chglog_reader_thr, info)) {
            int err = errno;
            DisplayLog(LVL_CRIT, CHGLOG_TAG,
                       "ERROR creating ChangeLog reader thread: %s",
                       strerror(err));
            return err;
        }

    }

    if (dbget)
        ListMgr_CloseAccess(&lmgr);

    return 0;
}

/** terminate ChangeLog Readers */
int cl_reader_terminate(void)
{
    unsigned int i;

    /* ask threads to stop */
    for (i = 0; i < cl_reader_config.mdt_count; i++) {
        reader_info[i].force_stop = true;
    }

    DisplayLog(LVL_EVENT, CHGLOG_TAG,
               "Stop request has been sent to all ChangeLog reader threads");

    cl_reader_wait();

    return 0;
}

/** wait for ChangeLog Readers termination */
int cl_reader_wait(void)
{
    int i;
    void *ret;

    for (i = 0; i < cl_reader_config.mdt_count; i++) {
        pthread_join(reader_info[i].thr_id, &ret);
    }

    Alert_EndBatching();

    return 0;
}

/** Release last changelog records, and dump the final stats. */
int cl_reader_done(void)
{
    lmgr_t lmgr;
    int rc;
    int i;

    for (i = 0; i < cl_reader_config.mdt_count; i++) {
        reader_thr_info_t *info = &reader_info[i];

        /* Clear the records that are still batched for clearing. */
        clear_changelog_records(info);

        log_close(info);
    }

    cl_reader_dump_stats();

    /* need DB access to save changelog stats */
    rc = ListMgr_InitAccess(&lmgr);
    if (rc != DB_SUCCESS)
        return 0;
    cl_reader_store_stats(&lmgr);
    ListMgr_CloseAccess(&lmgr);

    return 0;
}

/** Display record stats */
static void show_rec_stats(const char *verb, const char *verb_ed,
                           struct rec_stats *rs, time_t last_report)
{
    char rectime_str[256];
    char steptime_str[256];
    struct tm paramtm;
    time_t now = time(NULL);

    /* nothing processed, nothing to report */
    if (rs->rec_id == 0)
        return;

    /* first convert tv_sec */
    strftime(rectime_str, sizeof(rectime_str), "%Y/%m/%d %T",
             localtime_r(&rs->rec_time.tv_sec, &paramtm));
    strftime(steptime_str, sizeof(steptime_str), "%Y/%m/%d %T",
             localtime_r(&rs->step_time.tv_sec, &paramtm));

    /* then %06u appends microseconds (strftime only supports struct tm) */
    DisplayLog(LVL_MAJOR, "STATS", "   last %s: rec_id=%"PRIu64", "
               "rec_time=%s.%06lu, %s at %s.%06lu", verb_ed, rs->rec_id,
               rectime_str, rs->rec_time.tv_usec, verb_ed, steptime_str,
               rs->step_time.tv_usec);

    /* compute speeds */
    if (rs->last_report_rec_id != 0 && now > last_report) {
        double interval = now - last_report;
        double speed, ratio;

        speed = (double)(rs->rec_id - rs->last_report_rec_id) / interval;

        ratio = (double)((rs->rec_time.tv_sec + rs->rec_time.tv_usec * 0.000001)
                         - (rs->last_report_rec_time.tv_sec
                            + rs->last_report_rec_time.tv_usec * 0.000001))
                / interval;
        DisplayLog(LVL_MAJOR, "STATS", "       %s speed: %.2f rec/sec, "
                   "log/real time ratio: %.2f", verb, speed, ratio);
    }

    rs->last_report_rec_id = rs->rec_id;
    rs->last_report_rec_time = rs->rec_time;
}

/** dump changelog processing stats */
int cl_reader_dump_stats(void)
{
    unsigned int i, j;
    char tmp_buff[256];
    char *ptr;

    for (i = 0; i < cl_reader_config.mdt_count; i++) {
        DisplayLog(LVL_MAJOR, "STATS", "ChangeLog reader #%u:", i);

        DisplayLog(LVL_MAJOR, "STATS", "   fs_name    =   %s", get_fsname());
        DisplayLog(LVL_MAJOR, "STATS", "   mdt_name   =   %s",
                   cl_reader_config.mdt_def[i].mdt_name);
        DisplayLog(LVL_MAJOR, "STATS", "   reader_id  =   %s",
                   cl_reader_config.mdt_def[i].reader_id);
        DisplayLog(LVL_MAJOR, "STATS", "   records read        = %llu",
                   reader_info[i].nb_read);
        DisplayLog(LVL_MAJOR, "STATS", "   interesting records = %llu",
                   reader_info[i].interesting_records);
        DisplayLog(LVL_MAJOR, "STATS", "   suppressed records  = %llu",
                   reader_info[i].suppressed_records);
        DisplayLog(LVL_MAJOR, "STATS", "   records pending     = %u",
                   reader_info[i].op_queue_count);

        if (reader_info[i].force_stop)
            DisplayLog(LVL_MAJOR, "STATS",
                       "   status              = terminating");
        else if (reader_info[i].nb_reopen == reader_info[i].last_reopen)
            /* no reopen: it is busy reading changelogs */
            DisplayLog(LVL_MAJOR, "STATS",
                       "   status              = busy");
        else if (time(NULL) - reader_info[i].last_report
                 == (reader_info[i].nb_reopen - reader_info[i].last_reopen)
                    * cl_reader_config.polling_interval) {
            /* if the whole interval is the reopen time => it spends it time
             * polling */
            /* more than a single record read? */
            if (reader_info[i].last_read.rec_id -
                reader_info[i].last_read.last_report_rec_id > 1)
                DisplayLog(LVL_MAJOR, "STATS",
                           "   status              = almost idle");
            else
                DisplayLog(LVL_MAJOR, "STATS",
                           "   status              = idle");
        }

        if (reader_info[i].nb_read > 0) {
            show_rec_stats("receive", "received", &reader_info[i].last_read,
                           reader_info[i].last_report);
            show_rec_stats("push", "pushed", &reader_info[i].last_push,
                           reader_info[i].last_report);
            show_rec_stats("commit", "committed", &reader_info[i].last_commit,
                           reader_info[i].last_report);
            show_rec_stats("clear", "cleared", &reader_info[i].last_clear,
                           reader_info[i].last_report);
        }
        /* last_report is updated by cl_reader_store_stats */

        DisplayLog(LVL_MAJOR, "STATS", "   ChangeLog stats:");

        tmp_buff[0] = '\0';
        ptr = tmp_buff;
        for (j = 0; j < CL_LAST; j++) {
            /* flush full line */
            if (ptr - tmp_buff >= 80) {
                DisplayLog(LVL_MAJOR, "STATS", "   %s", tmp_buff);
                tmp_buff[0] = '\0';
                ptr = tmp_buff;
            }
            if (ptr != tmp_buff)
                ptr += sprintf(ptr, ", ");

            ptr += sprintf(ptr, "%s: %llu", changelog_type2str(j),
                           reader_info[i].cl_counters[j]);
        }
        /* last unflushed line */
        if (ptr != tmp_buff)
            DisplayLog(LVL_MAJOR, "STATS", "   %s", tmp_buff);
    }

    return 0;
}

static void store_thread_info(lmgr_t *lmgr, reader_thr_info_t *info)
{
    const char *mdt = cl_reader_config.mdt_def[info->thr_index].mdt_name;
    char *varname = NULL;
    char tmp_buff[256];
    int i;

    store_rec_stats(lmgr, info, CL_LAST_READ_REC, &info->last_read);
    store_rec_stats(lmgr, info, CL_LAST_PUSHED_REC, &info->last_push);
    store_rec_stats(lmgr, info, CL_LAST_CLEARED_REC, &info->last_clear);
    /* CL_LAST_COMMITTED_REC is updated by entry processor callbacks */

    for (i = 0; i < CL_LAST; i++) {
        char last_val[256];
        unsigned long long last, current, diff;

        /* CL counters format:  <prefix>_<mdt_name>_<event_name> */
        if (asprintf(&varname, "%s_%s_%s", CL_COUNT_PREFIX, mdt,
                     changelog_type2str(i)) == -1 || varname == NULL)
            continue;

        /* get and set (increment) */
        if (ListMgr_GetVar(lmgr, varname, last_val, sizeof(last_val)) !=
            DB_SUCCESS)
            last = 0;
        else
            last = str2bigint(last_val);

        /* diff = current - last_reported */
        current = info->cl_counters[i];
        diff = current - info->cl_reported[i];

        /* new value = last + diff */
        snprintf(tmp_buff, sizeof(tmp_buff), "%llu", last + diff);
        if (ListMgr_SetVar(lmgr, varname, tmp_buff) == DB_SUCCESS)
            /* last_reported is now current */
            info->cl_reported[i] = current;
        free(varname);

        /* save diff */
        if (asprintf(&varname, "%s_%s_%s", CL_DIFF_PREFIX, mdt,
                     changelog_type2str(i)) == -1 || varname == NULL)
            continue;
        snprintf(tmp_buff, sizeof(tmp_buff), "%llu", diff);
        ListMgr_SetVar(lmgr, varname, tmp_buff);
        free(varname);
    }

    if (asprintf(&varname, "%s_%s", CL_DIFF_INTERVAL, mdt) == -1
        || varname == NULL)
        return;

    /* indicate diff interval */
    snprintf(tmp_buff, sizeof(tmp_buff), "%lu", time(NULL) - info->last_report);
    ListMgr_SetVar(lmgr, varname, tmp_buff);
    free(varname);

    info->last_report = time(NULL);
    info->last_reopen = info->nb_reopen;
}

/** store changelog stats to the database */
void cl_reader_store_stats(lmgr_t *lmgr)
{
    int i;

    if (cl_reader_config.mdt_count < 1)
        /* nothing to be stored */
        return;

    for (i = 0; i < cl_reader_config.mdt_count; i++)
        store_thread_info(lmgr, &reader_info[i]);
}
