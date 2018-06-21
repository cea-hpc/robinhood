/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/* TODO CASTOR-1998 Update header */
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
 * \file lhsm.c
 * \brief implements Lustre/HSM status manager and functions.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "mod_internal.h"
#include "status_manager.h"
#include "entry_processor.h"
#include "rbh_logs.h"
#include "rbh_misc.h"

/* for logs */
#define COLLAPSE_TAG  "cpp_collapse"

bool collapse(struct rh_list_head *op_queue, unsigned int *op_queue_count,
	      void *cpp_instance_data);

const char *mod_get_name(void)
{
    return "collapse";
}

status_manager_t *mod_get_status_manager(void)
{
    return NULL;
}

static chglog_postproc_t collapse_cpp = {
	.name = "collapse",
	.action = collapse,
	.instance_data = NULL
};

chglog_postproc_t *mod_get_changelog_postproc(void)
{
    return &collapse_cpp;
}

action_func_t mod_get_action_by_name(const char *action_name)
{
	return NULL;
}

#define LUSTRE_FIDS_EQUAL(_fid1, _fid2) \
    (  (_fid1).f_seq == (_fid2).f_seq   \
    && (_fid1).f_oid == (_fid2).f_oid   \
    && (_fid1).f_ver == (_fid2).f_ver)

#ifdef _LUSTRE_HSM
static const char *get_event_name(unsigned int cl_event)
{
    static const char *event_name[] = {
        "archive", "restore", "cancel", "release", "remove", "state",
    };

    if (cl_event >= G_N_ELEMENTS(event_name))
        return "unknown";
    else
        return event_name[cl_event];
}
#endif

#define CL_BASE_FORMAT "%s: %llu %02d%-5s %u.%09u 0x%x%s t="DFID
#define CL_BASE_ARG(_mdt, _rec_) \
    (_mdt), (_rec_)->cr_index, \
    (_rec_)->cr_type, changelog_type2str((_rec_)->cr_type), \
    (uint32_t)cltime2sec((_rec_)->cr_time), cltime2nsec((_rec_)->cr_time), \
    (_rec_)->cr_flags & CLF_FLAGMASK, flag_buff, PFID(&(_rec_)->cr_tfid)
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

#ifdef _LUSTRE_HSM
    if (rec->cr_type == CL_HSM)
        g_snprintf(flag_buff, sizeof(flag_buff), "(%s%s,rc=%d)",
                   get_event_name(hsm_get_cl_event(rec->cr_flags)),
                   hsm_get_cl_flags(rec->cr_flags) & CLF_HSM_DIRTY? ",dirty":"",
                   hsm_get_cl_error(rec->cr_flags));
#endif

    len = snprintf(curr, left, CL_BASE_FORMAT, CL_BASE_ARG(mdt, rec));
    curr += len;
    left -= len;
    if (left > 0 && rec->cr_namelen)
    {
        /* this record has a 'name' field. */
        len = snprintf(curr, left, " "CL_NAME_FORMAT, CL_NAME_ARG(rec));
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
                len = snprintf(curr, left, " "CL_EXT_FORMAT,
                               PFID(&cr_rename->cr_sfid),
                               PFID(&cr_rename->cr_spfid),
                               (int)changelog_rec_snamelen((CL_REC_TYPE *)rec),
                               changelog_rec_sname((CL_REC_TYPE *)rec));
                curr += len;
                left -= len;
            }
        }
        if (rec->cr_flags & CLF_JOBID) {
            struct changelog_ext_jobid *jobid;
            jobid = changelog_rec_jobid((CL_REC_TYPE *)rec);

            len = snprintf(curr, left, " J=%s", jobid->cr_jobid);
            curr += len;
            left -= len;
        }
#elif defined(HAVE_CHANGELOG_EXTEND_REC)
        if (fid_is_sane(&rec->cr_sfid)) {
            len = snprintf(curr, left, " "CL_EXT_FORMAT,
                           PFID(&rec->cr_sfid),
                           PFID(&rec->cr_spfid),
                           (int)changelog_rec_snamelen(rec),
                           changelog_rec_sname(rec));
            curr += len;
            left -= len;
        }
#endif
    }

    if (left <= 0)
        record_str[RBH_PATH_MAX-1] = '\0';

    DisplayLog(debug_level, COLLAPSE_TAG, "%s", record_str);
}

static void drop_operation(entry_proc_op_t *operation)
{
    DisplayLog(LVL_DEBUG, COLLAPSE_TAG, "Dropping operation");
    dump_record(LVL_DEBUG, operation->extra_info.log_record.mdt,
                operation->extra_info.log_record.p_log_rec);
    rh_list_del(&operation->list);
    rh_list_del(&operation->id_hash_list);
    EntryProcessor_Release(operation);
}

bool collapse(struct rh_list_head *op_queue, unsigned int *op_queue_count,
	      void *cpp_instance_data)
{
    entry_proc_op_t *op;
    entry_proc_op_t *tmp;
    entry_proc_op_t *to_del;
    CL_REC_TYPE     *rec;
    CL_REC_TYPE     *td_rec;
    bool             deleted = false;
    bool             remove_unlink = false;

    /* Lookup starting from the last operation in the slot. */
    op = rh_list_last_entry(op_queue, entry_proc_op_t, list);
    while (   !rh_list_empty(op_queue)
           && op != rh_list_first_entry(op_queue, entry_proc_op_t, list)) {
        if (op->extra_info_is_set == 0) {
            op = rh_list_entry(op->list.prev, entry_proc_op_t, list);
            continue;
        }
        rec = op->extra_info.log_record.p_log_rec;
        if (   rec->cr_type != CL_UNLINK
            && rec->cr_type != CL_RENAME
            && rec->cr_type != CL_RMDIR) {
            op = rh_list_entry(op->list.prev, entry_proc_op_t, list);
            continue;
        }

        tmp = op;
        if (   rec->cr_type == CL_UNLINK
            && (rec->cr_flags & CLF_UNLINK_LAST) != 0) {
            /* Remove all records before for same target FID, but this one. */
            while (   tmp
                   != rh_list_first_entry(op_queue, entry_proc_op_t, list)) {
                to_del = rh_list_entry(tmp->list.prev, entry_proc_op_t, list);
                td_rec = to_del->extra_info.log_record.p_log_rec;
                if (!LUSTRE_FIDS_EQUAL(rec->cr_tfid, td_rec->cr_tfid)) {
                    tmp = to_del;
                } else {
                    drop_operation(to_del);
                    --*op_queue_count;
                    deleted = true;
                }
            }
        } else {
            /* Remove records before for same tfid, pfid, and name. If removed
             * CL_CREATE, CL_MKDIR, or CL_EXT (RNMTO), remove this too. */
            while (   tmp
                   != rh_list_first_entry(op_queue, entry_proc_op_t, list)) {
                to_del = rh_list_entry(tmp->list.prev, entry_proc_op_t, list);
                td_rec = to_del->extra_info.log_record.p_log_rec;
                if (   !LUSTRE_FIDS_EQUAL(rec->cr_tfid, td_rec->cr_tfid)
                    || !LUSTRE_FIDS_EQUAL(rec->cr_pfid, td_rec->cr_pfid)
                    || strcmp(rh_get_cl_cr_name(rec),
                              rh_get_cl_cr_name(td_rec)) != 0) {
                    tmp = to_del;
                    continue;
                }

                /* if create, remove unlink record too. If unlink followed
                 * by corresponding CL_RENAME, remove it too.
                 */
                remove_unlink =    td_rec->cr_type == CL_CREATE
                                || td_rec->cr_type == CL_HARDLINK
                                || td_rec->cr_type == CL_SOFTLINK
                                || td_rec->cr_type == CL_EXT
                                || td_rec->cr_type == CL_MKDIR;
                drop_operation(to_del);
                --*op_queue_count;
                deleted = true;

                if (remove_unlink)
                    break;
            }
        }
        if (remove_unlink) {
            /* Special processing: we'll move to previous operation as result of
             * unlink record removal.
             */
            to_del = op;
            if (op != rh_list_first_entry(op_queue, entry_proc_op_t, list))
                op = rh_list_entry(op->list.prev, entry_proc_op_t, list);
            drop_operation(to_del);
            --*op_queue_count;
            deleted = true;
            remove_unlink = false;
            continue;
        }

        if (!!rh_list_empty(op_queue))
            break;
        if (op != rh_list_first_entry(op_queue, entry_proc_op_t, list))
            op = rh_list_entry(op->list.prev, entry_proc_op_t, list);
    }

    return deleted;
}
