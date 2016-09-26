/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2008-2015 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
/**
 * Misc tools for managing entry processor pipeline
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "entry_proc_tools.h"
#include "entry_proc_hash.h"
#include "Memory.h"
#include "rbh_logs.h"
#include "rbh_cfg_helpers.h"
#include "rbh_misc.h"
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>

/* configuration for this module */
entry_proc_config_t entry_proc_conf;
int pipeline_flags = 0;

/* default max pending operation is 10k */
#define ID_HASH_SIZE 16007
/* hash table for storing references to ids */
static struct id_hash *id_hash;
/* hash table for storing references to parent_id/name */
static struct id_hash *name_hash;

/** initialize id constraint manager */
int id_constraint_init(void)
{
    id_hash = id_hash_init(ID_HASH_SIZE, true);
    name_hash = id_hash_init(ID_HASH_SIZE, true);
    /* exiting the process releases hash resources */
    return (id_hash == NULL || name_hash == NULL) ? -1 : 0;
}

/**
 * This is called to register the operation (with the ordering of pipeline)
 * Normal operation is to register at the tail.
 * @return ID_OK if the entry can be processed.
 *         ID_MISSING if the ID is not set in p_op structure
 *         ID_ALREADY if the op_structure has already been registered
 */
int id_constraint_register(entry_proc_op_t *p_op, int at_head)
{
    struct id_hash_slot *slot;

    if (!p_op->entry_id_is_set)
        return ID_MISSING;

    /* compute id hash value */
    slot = get_hash_slot(id_hash, &p_op->entry_id);

    P(slot->lock);

    if (at_head)
        rh_list_add(&p_op->id_hash_list, &slot->list);
    else
        rh_list_add_tail(&p_op->id_hash_list, &slot->list);

    slot->count++;
    p_op->id_is_referenced = 1;

    V(slot->lock);

    /* also lock parent_id/name */
    if (ATTR_MASK_TEST(&p_op->fs_attrs, parent_id) &&
        ATTR_MASK_TEST(&p_op->fs_attrs, name)) {
        slot = get_name_hash_slot(name_hash, &ATTR(&p_op->fs_attrs, parent_id),
                                  ATTR(&p_op->fs_attrs, name));
        P(slot->lock);

        if (at_head)
            rh_list_add(&p_op->name_hash_list, &slot->list);
        else
            rh_list_add_tail(&p_op->name_hash_list, &slot->list);

        slot->count++;
        p_op->name_is_referenced = 1;

        V(slot->lock);
    }

    return ID_OK;
}

#ifdef HAVE_CHANGELOGS
#define op_name(_op)    ((_op)->extra_info.is_changelog_record ? \
                              changelog_type2str((_op)\
                              ->extra_info.log_record.p_log_rec->cr_type) : \
                              "scan_op")
#else
#define op_name(_op)    "scan_op"
#endif

/**
 * Test if a given operation is the first to be processed.
 */
bool id_constraint_is_first_op(entry_proc_op_t *p_op_in)
{
    entry_proc_op_t *op;
    struct id_hash_slot *slot;
    int is_first = -1;  /* not set */

    /* compute id hash value */
    slot = get_hash_slot(id_hash, &p_op_in->entry_id);

    P(slot->lock);
    rh_list_for_each_entry(op, &slot->list, id_hash_list) {
        if (entry_id_equal(&p_op_in->entry_id, &op->entry_id)) {
            /* found an operation with the given id */
            if (op == p_op_in)
                is_first = 1;
            else {
                is_first = 0;
                DisplayLog(LVL_FULL, "IdConstraint",
                           "Pending operation with the same id: " DFID
                           " (%s). next op: %s", PFID(&op->entry_id),
                           op_name(op), op_name(p_op_in));
            }
            break;
        }
    }
    V(slot->lock);

    if (is_first == 0)
        /* for sure, there is another operation on the same id before
         * this one */
        return false;

    /* sanity check: registered operation was not found??? */
    if ((is_first == -1) && (p_op_in->id_is_referenced))
        RBH_BUG("Registered operation was not found in id_constraint hash");

    /* Entry may be the first (or is not registered).
     * Additional check of parent/name constraint: */
    if (ATTR_MASK_TEST(&p_op_in->fs_attrs, parent_id) &&
        ATTR_MASK_TEST(&p_op_in->fs_attrs, name)) {
        slot =
            get_name_hash_slot(name_hash, &ATTR(&p_op_in->fs_attrs, parent_id),
                               ATTR(&p_op_in->fs_attrs, name));
        P(slot->lock);
        rh_list_for_each_entry(op, &slot->list, name_hash_list) {
            if (entry_id_equal
                (&ATTR(&p_op_in->fs_attrs, parent_id),
                 &ATTR(&op->fs_attrs, parent_id))
                && !strcmp(ATTR(&p_op_in->fs_attrs, name),
                           ATTR(&op->fs_attrs, name))) {
                if (op == p_op_in)
                    is_first = 1;
                else {
                    is_first = 0;
                    DisplayLog(LVL_FULL, "IdConstraint",
                               "Pending operation with the same parent/name: "
                               DFID "/%s (%s). next op: %s",
                               PFID(&ATTR(&p_op_in->fs_attrs, parent_id)),
                               ATTR(&p_op_in->fs_attrs, name), op_name(op),
                               op_name(p_op_in));
                }
                break;
            }
        }
        V(slot->lock);
    }

    /* if is_first = 0 => not first */
    /* if is_first = -1: not found => not first */
    /* just return TRUE if is_first = 1 */
    return (is_first == 1);
}

/**
 * This removes the current reference to an id when the operation is removed.
 */
int id_constraint_unregister(entry_proc_op_t *p_op)
{
    struct id_hash_slot *slot;

    if (!p_op->entry_id_is_set)
        return ID_MISSING;

    if (!p_op->id_is_referenced)
        return ID_NOT_EXISTS;

    slot = get_hash_slot(id_hash, &p_op->entry_id);

    /* Remove the entry */
    P(slot->lock);

    rh_list_del(&p_op->id_hash_list);
    p_op->id_is_referenced = 0;
    slot->count--;

    V(slot->lock);

    if (p_op->name_is_referenced) {
        if (ATTR_MASK_TEST(&p_op->fs_attrs, parent_id) &&
            ATTR_MASK_TEST(&p_op->fs_attrs, name)) {
            slot =
                get_name_hash_slot(name_hash, &ATTR(&p_op->fs_attrs, parent_id),
                                   ATTR(&p_op->fs_attrs, name));
            /* Remove the entry */
            P(slot->lock);

            rh_list_del(&p_op->name_hash_list);
            p_op->name_is_referenced = 0;
            slot->count--;

            V(slot->lock);
        } else {
            DisplayLog(LVL_MAJOR, "IdConstraint", "WARNING: cannot unregister "
                       "entry with no parent/name but with a registered name!");
        }
    }

    return ID_OK;
}

void id_constraint_stats(void)
{
    id_hash_stats(id_hash, "Id constraints count");
    id_hash_stats(name_hash, "Name constraints count");
}

void id_constraint_dump(void)
{
    id_hash_dump(id_hash, false);
    id_hash_dump(name_hash, true);
}

/* ------------ Config management functions --------------- */

#define ENTRYPROC_CONFIG_BLOCK  "EntryProcessor"
#define ALERT_BLOCK "Alert"

static void entry_proc_cfg_set_default(void *module_config)
{
    entry_proc_config_t *conf = (entry_proc_config_t *) module_config;

    if (lmgr_parallel_batches())
        conf->nb_thread = 16;
    else
        conf->nb_thread = 10;

    /* for efficient batching of 1000 ops */
    conf->max_pending_operations = 10000;
    conf->max_batch_size = 1000;
    conf->match_classes = true;

    conf->detect_fake_mtime = false;
}

static void entry_proc_cfg_write_default(FILE *output)
{
    print_begin_block(output, 0, ENTRYPROC_CONFIG_BLOCK, NULL);

    if (lmgr_parallel_batches())
        print_line(output, 1, "nb_threads             :  16");
    else
        print_line(output, 1, "nb_threads             :  10");

    print_line(output, 1, "max_pending_operations :  10000");
    print_line(output, 1, "max_batch_size         :  1000");
    print_line(output, 1, "match_classes          :  yes");
    print_line(output, 1, "detect_fake_mtime      :  no");
    print_end_block(output, 0);
}

#define CRITICAL_ERR_CHECK(_ptr_, _blkname_) do { if (!_ptr_) {\
            sprintf(msg_out, "Internal error reading %s block in config file", \
                    _blkname_); \
            return EFAULT; \
         }\
    } while (0)

/** set expected values for the std pipeline
 * \return the number of variables added to array
 */
static int std_pipeline_arg_names(char **list, char *buffer)
{
    int i, c;
    char *curr_buf = buffer;
    unsigned int w;
    c = 0;
    for (i = 0; i < std_pipeline_descr.stage_count; i++) {
        w = sprintf(curr_buf, "%s_threads_max", std_pipeline[i].stage_name);
        list[i] = curr_buf;
        curr_buf += w + 1;  /* written bytes + final null char */
        c++;
    }
    return c;
}

static int load_pipeline_config(const pipeline_descr_t *descr,
                                pipeline_stage_t *p,
                                const entry_proc_config_t *conf,
                                config_item_t entryproc_block, char *msg_out)
{
    int i, rc, tmpval;

    for (i = 0; i < descr->stage_count; i++) {
        char varname[256];

        snprintf(varname, 256, "%s_threads_max", p[i].stage_name);

        rc = GetIntParam(entryproc_block, ENTRYPROC_CONFIG_BLOCK, varname,
                         PFLG_POSITIVE, &tmpval, NULL, NULL, msg_out);

        if ((rc != 0) && (rc != ENOENT))
            return rc;
        else if ((rc != ENOENT) && (tmpval > 0)) {  /* 0: keep default */

            if (p[i].stage_flags & STAGE_FLAG_PARALLEL) {
                /* the stage is no longer parallel, it has a limited number of
                 * threads */
                p[i].stage_flags &= ~STAGE_FLAG_PARALLEL;
                p[i].stage_flags |= STAGE_FLAG_MAX_THREADS;
                p[i].max_thread_count = conf->nb_thread;
            }
            if (p[i].stage_flags & STAGE_FLAG_MAX_THREADS) {
                /* if batching is enabled and simultaneous batches are not
                 * allowed: ERROR */
                if (!lmgr_parallel_batches() && (i == descr->DB_APPLY)
                    && (conf->max_batch_size != 1) && (tmpval > 1)) {
                    sprintf(msg_out,
                            "Wrong value for '%s': Parallelizing batched DB operations "
                            "is not allowed when accounting is ON.\n"
                            "Remove this tuning, disable accounting (accounting = no)"
                            " or disable batching (max_batch_size=1) to parallelize this stage.",
                            varname);
                    return EINVAL;
                }

                if ((i == descr->DB_APPLY) && (conf->nb_thread > 1))
                    /* don't starve other steps: max is nb_thread-1
                     * (except if nb_thread = 1) */
                    p[i].max_thread_count = MIN2(conf->nb_thread - 1, tmpval);
                else
                    /* nb_thread at most */
                    p[i].max_thread_count = MIN2(conf->nb_thread, tmpval);
            } else if ((p[i].stage_flags & STAGE_FLAG_SEQUENTIAL)
                       && (tmpval != 1)) {
                sprintf(msg_out,
                        "%s is sequential. Cannot use %u threads at this stage.",
                        p[i].stage_name, tmpval);
                return EINVAL;
            }
        }
    }

    return 0;
}

static void set_default_pipeline_config(const pipeline_descr_t *descr,
                                        pipeline_stage_t *p,
                                        const entry_proc_config_t *conf)
{
    int i = descr->DB_APPLY;

    if (p[i].stage_flags & STAGE_FLAG_PARALLEL) {
        p[i].stage_flags &= ~STAGE_FLAG_PARALLEL;
        p[i].stage_flags |= STAGE_FLAG_MAX_THREADS;

        /* mode batching + parallel: 50% of threads at most for DB apply */
        if (lmgr_parallel_batches() && conf->max_batch_size > 1) {
            /* 10 => 4, 20 => 8... => remove 1/5, then /2 => *2/5 */
            /* if nb thread < 4 => 1 */
            if (conf->nb_thread < 4)
                p[i].max_thread_count = 1;
            else
                p[i].max_thread_count = 2 * conf->nb_thread / 5;
        } else {    /* not // + batching */

            /* if nb thread = 1 or 2 => set the limit to 1
             *                3      =>                  2
             *                4-7    =>                  n-2
             *                7+     =>                  80%
             */
            if (conf->nb_thread < 4)
                p[i].max_thread_count = MAX2(conf->nb_thread - 1, 1);
            else if (conf->nb_thread < 8)
                p[i].max_thread_count = conf->nb_thread - 2;
            else
                p[i].max_thread_count = (8 * conf->nb_thread) / 10;
        }
    } else if (p[i].stage_flags & STAGE_FLAG_MAX_THREADS) {
        /* ensure DB_APPLY threads <= nbthread - 1 */
        if (p[i].max_thread_count > conf->nb_thread - 1)
            p[i].max_thread_count = conf->nb_thread - 1;
    }

    /* if batching is enabled, DB_APPLY_THREAD_MAX = 1 */
    if (!lmgr_parallel_batches() && conf->max_batch_size != 1) {
        if (p[i].stage_flags & STAGE_FLAG_PARALLEL)
            RBH_BUG("step should no big tagged as 'PARALLEL' at this point");
        else if (p[i].stage_flags & STAGE_FLAG_MAX_THREADS)
            p[i].max_thread_count = 1;
    }
}

static int entry_proc_cfg_read(config_file_t config, void *module_config,
                               char *msg_out)
{
    int rc, blc_index;
    entry_proc_config_t *conf = (entry_proc_config_t *) module_config;
    unsigned int next_idx = 0;
    config_item_t entryproc_block;

    /* buffer to store arg names */
    char *pipeline_names = NULL;
    /* max size is max pipeline steps (<10) + other args (<6) */
#define MAX_ENTRYPROC_ARGS 16
    char *entry_proc_allowed[MAX_ENTRYPROC_ARGS] = { 0 };

    const cfg_param_t cfg_params[] = {
        {"nb_threads", PT_INT, PFLG_POSITIVE | PFLG_NOT_NULL, &conf->nb_thread,
         0},
        {"max_pending_operations", PT_INT, PFLG_POSITIVE | PFLG_NOT_NULL,
         &conf->max_pending_operations, 0},
        {"max_batch_size", PT_INT, PFLG_POSITIVE | PFLG_NOT_NULL,
         &conf->max_batch_size, 0},
        {"match_classes", PT_BOOL, 0, &conf->match_classes, 0},
        {"detect_fake_mtime", PT_BOOL, 0, &conf->detect_fake_mtime, 0},

        END_OF_PARAMS
    };

    /* get EntryProcessor block */
    rc = get_cfg_block(config, ENTRYPROC_CONFIG_BLOCK, &entryproc_block,
                       msg_out);
    if (rc == ENOENT) {
        /* set default pipeline config */
        set_default_pipeline_config(&std_pipeline_descr, std_pipeline, conf);
        /* No error because no parameter is mandatory */
        return 0;
    }
    if (rc)
        return rc;

    /* read std params */
    rc = read_scalar_params(entryproc_block, ENTRYPROC_CONFIG_BLOCK, cfg_params,
                            msg_out);
    if (rc)
        return rc;

    /* should have at least 2 threads! */
    if (conf->nb_thread == 1)
        DisplayLog(LVL_MAJOR, "EntryProc_Config", "WARNING: "
                   ENTRYPROC_CONFIG_BLOCK " should have at least 2 threads to "
                   "avoid pipeline step starvation!");

    /* look for '<stage>_thread_max' parameters (for all pipelines) */

    /* Set default pipeline config according to EntryProc config
     * FIXME this modifies the global config variable, even when reloading!
     */
    set_default_pipeline_config(&std_pipeline_descr, std_pipeline, conf);

    rc = load_pipeline_config(&std_pipeline_descr, std_pipeline, conf,
                              entryproc_block, msg_out);
    if (rc)
        return rc;

    // TODO load_pipeline_config(&diff_pipeline_descr, &diff_pipeline);

    /* TODO Check consistency of performance strategy:
     * batching vs. multithread DB operations */

    /* Warn about deprecated "Alert" blocks */
    for (blc_index = 0; blc_index < rh_config_GetNbItems(entryproc_block);
         blc_index++) {
        char *block_name;
        config_item_t curr_item;

        curr_item = rh_config_GetItemByIndex(entryproc_block, blc_index);
        CRITICAL_ERR_CHECK(curr_item, ENTRYPROC_CONFIG_BLOCK);

        if (rh_config_ItemType(curr_item) != CONFIG_ITEM_BLOCK)
            continue;

        block_name = rh_config_GetBlockName(curr_item);
        CRITICAL_ERR_CHECK(curr_item, ENTRYPROC_CONFIG_BLOCK);

        if (!strcasecmp(block_name, ALERT_BLOCK)) {
            DisplayLog(LVL_MAJOR, "EntryProc_Config",
                       "WARNING: %s blocks are deprecated. "
                       "Configure an alert policy instead (include 'alerts.inc').",
                       ALERT_BLOCK);
        }
    }

    next_idx = 0;
    entry_proc_allowed[next_idx++] = "nb_threads";
    entry_proc_allowed[next_idx++] = "max_pending_operations";
    entry_proc_allowed[next_idx++] = "max_batch_size";
    entry_proc_allowed[next_idx++] = "match_classes";
    entry_proc_allowed[next_idx++] = "detect_fake_mtime";

    pipeline_names = malloc(16 * 256);  /* max 16 strings of 256 (oversized) */
    if (!pipeline_names)
        return ENOMEM;

    /* fill arg list with pipeline step names */
    next_idx +=
        std_pipeline_arg_names(entry_proc_allowed + next_idx, pipeline_names);
    //TODO
    //next_idx += diff_pipeline_arg_names(entry_proc_allowed + next_idx,
    //                                    pipeline_names + XXX?);

    CheckUnknownParameters(entryproc_block, ENTRYPROC_CONFIG_BLOCK,
                           (const char **)entry_proc_allowed);
    free(pipeline_names);

    return 0;
}

static int entry_proc_cfg_reload(entry_proc_config_t *conf)
{
    if (conf->nb_thread != entry_proc_conf.nb_thread)
        DisplayLog(LVL_MAJOR, "EntryProc_Config",
                   ENTRYPROC_CONFIG_BLOCK
                   "::nb_threads changed in config file, but cannot be modified dynamically");

    if (conf->max_pending_operations != entry_proc_conf.max_pending_operations)
        DisplayLog(LVL_MAJOR, "EntryProc_Config",
                   ENTRYPROC_CONFIG_BLOCK
                   "::max_pending_operations changed in config file, but cannot be modified dynamically");

    if (conf->max_batch_size != entry_proc_conf.max_batch_size) {
        DisplayLog(LVL_MAJOR, "EntryProc_Config",
                   ENTRYPROC_CONFIG_BLOCK
                   "::max_batch_size updated: '%u'->'%u'",
                   entry_proc_conf.max_batch_size, conf->max_batch_size);
        entry_proc_conf.max_batch_size = conf->max_batch_size;
    }

    if (conf->match_classes != entry_proc_conf.match_classes) {
        DisplayLog(LVL_MAJOR, "EntryProc_Config",
                   ENTRYPROC_CONFIG_BLOCK "::match_classes updated: '%s'->'%s'",
                   bool2str(entry_proc_conf.match_classes),
                   bool2str(conf->match_classes));
        entry_proc_conf.match_classes = conf->match_classes;
    }

    if (conf->detect_fake_mtime != entry_proc_conf.detect_fake_mtime) {
        DisplayLog(LVL_MAJOR, "EntryProc_Config",
                   ENTRYPROC_CONFIG_BLOCK
                   "::detect_fake_mtime updated: '%s'->'%s'",
                   bool2str(entry_proc_conf.detect_fake_mtime),
                   bool2str(conf->detect_fake_mtime));
        entry_proc_conf.detect_fake_mtime = conf->detect_fake_mtime;
    }

    if (entry_proc_conf.match_classes && (policies.fileset_count == 0)) {
        DisplayLog(LVL_EVENT, "EntryProc_Config",
                   "No fileclass defined in configuration, disabling fileclass matching.");
        entry_proc_conf.match_classes = false;
    }

    return 0;
}

static int entry_proc_cfg_set(void *cfg, bool reload)
{
    entry_proc_config_t *config = cfg;

    if (reload)
        return entry_proc_cfg_reload(config);

    entry_proc_conf = *config;
    return 0;
}

static void entry_proc_cfg_write_template(FILE *output)
{
    int i;

    print_begin_block(output, 0, ENTRYPROC_CONFIG_BLOCK, NULL);

    print_line(output, 1,
               "# nbr of worker threads for processing pipeline tasks");
    print_line(output, 1, "nb_threads = 16 ;");
    fprintf(output, "\n");
    print_line(output, 1,
               "# Max number of operations in the Entry Processor pipeline.");
    print_line(output, 1,
               "# If the number of pending operations exceeds this limit, ");
    print_line(output, 1,
               "# info collectors are suspended until this count decreases");
#ifdef _SQLITE
    print_line(output, 1, "max_pending_operations = 500 ;");
#else
    print_line(output, 1, "max_pending_operations = 10000 ;");
#endif
    fprintf(output, "\n");
    print_line(output, 1, "# max batched DB operations (1=no batching)");
    print_line(output, 1, "max_batch_size = 1000;");
    fprintf(output, "\n");

    print_line(output, 1,
               "# Optionnaly specify a maximum thread count for each stage of the pipeline:");
    print_line(output, 1, "# <stagename>_threads_max = <n> (0: use default)");
    for (i = 0; i < std_pipeline_descr.stage_count; i++) {
        if (i == std_pipeline_descr.DB_APPLY) {
            print_line(output, 1,
                       "# Disable batching (max_batch_size=1) or accounting (accounting=no)");
            print_line(output, 1,
                       "# to allow parallelizing the following step:");
        }

        if (std_pipeline[i].stage_flags & STAGE_FLAG_PARALLEL)
            print_line(output, 1, "# %s_threads_max\t= 4 ;",
                       std_pipeline[i].stage_name);
        else if (std_pipeline[i].stage_flags & STAGE_FLAG_MAX_THREADS)
            print_line(output, 1, "%s_threads_max\t= %u ;",
                       std_pipeline[i].stage_name,
                       std_pipeline[i].max_thread_count);
    }
    fprintf(output, "\n");

    print_line(output, 1, "# if set to 'no', classes will only be matched");
    print_line(output, 1,
               "# at policy application time (not during a scan or reading changelog)");
    print_line(output, 1, "match_classes = yes;");

    fprintf(output, "\n");
    print_line(output, 1,
               "# Faking mtime to an old time causes the file to be migrated");
    print_line(output, 1,
               "# with top priority. Enabling this parameter detect this behavior");
    print_line(output, 1, "# and doesn't allow  mtime < creation_time");
    print_line(output, 1, "detect_fake_mtime = no;");

    print_end_block(output, 0);
}

/** try to convert a time_t to a human readable form */
void time2human_helper(time_t t, const char *attr_name, char *str,
                       size_t size, const struct entry_proc_op_t *p_op)
{
    struct tm res;

    /* initialize as 'out of range' */
    strncpy(str, "<out of range>", size);

    if (localtime_r(&t, &res) != NULL)
        strftime(str, size, "%Y/%m/%d %T", &res);
    else if (ATTR_FSorDB_TEST(p_op, fullpath))
        DisplayLog(LVL_MAJOR, ENTRYPROC_TAG,
                   "Invalid or corrupted %s detected for %s: %lu", attr_name,
                   ATTR_FSorDB(p_op, fullpath), t);
    else
        DisplayLog(LVL_MAJOR, ENTRYPROC_TAG,
                   "Invalid or corrupted %s detected for " DFID ": %lu",
                   attr_name, PFID(&p_op->entry_id), t);
}

void check_and_warn_fake_mtime(const struct entry_proc_op_t *p_op)
{
    char mt[128];
    char ct[128];

    /* check if mtime is before estimated creation time */
    if (ATTR(&p_op->fs_attrs, last_mod) < ATTR_FSorDB(p_op, creation_time)) {
        time2human_helper(ATTR(&p_op->fs_attrs, last_mod), "mtime", mt,
                          sizeof(mt), p_op);

        time2human_helper(ATTR(&p_op->fs_attrs, creation_time), "crtime", ct,
                          sizeof(ct), p_op);

        if (ATTR_FSorDB_TEST(p_op, fullpath))
            DisplayLog(LVL_VERB, ENTRYPROC_TAG,
                       "Fake mtime detected for '%s': mtime=%s, creation=%s",
                       ATTR_FSorDB(p_op, fullpath), mt, ct);
        else
            DisplayLog(LVL_VERB, ENTRYPROC_TAG,
                       "Fake mtime detected for " DFID
                       ": mtime=%s, creation=%s", PFID(&p_op->entry_id), mt,
                       ct);
    }
    /* a 24h delay can be explained by different timezones */
    else if (ATTR(&p_op->fs_attrs, last_mod) > time(NULL) + 86400) {
        time2human_helper(ATTR(&p_op->fs_attrs, last_mod), "mtime", mt,
                          sizeof(mt), p_op);

        if (ATTR_FSorDB_TEST(p_op, fullpath))
            DisplayLog(LVL_EVENT, ENTRYPROC_TAG,
                       "Fake mtime detected for '%s': mtime=%s is in the future",
                       ATTR_FSorDB(p_op, fullpath), mt);
        else
            DisplayLog(LVL_EVENT, ENTRYPROC_TAG,
                       "Fake mtime detected for " DFID
                       ": mtime=%s is in the future", PFID(&p_op->entry_id),
                       mt);
    }
}

#ifdef _LUSTRE
static void clear_stripe_info(attr_set_t *attrs)
{
    ATTR_MASK_UNSET(attrs, stripe_info);
    if (ATTR_MASK_TEST(attrs, stripe_items)) {
        /* free stripe structure */
        if (ATTR(attrs, stripe_items).stripe)
            MemFree(ATTR(attrs, stripe_items).stripe);
        ATTR_MASK_UNSET(attrs, stripe_items);
    }
}

void check_stripe_info(struct entry_proc_op_t *p_op, lmgr_t *lmgr)
{
#ifdef HAVE_LLAPI_FSWAP_LAYOUTS
    /* Since Lustre2.4, entry striping can change (lfs swap_layouts,
     * lfs hsm_release...) so scanning must update file stripe information. */
    /* Possible cases:
     * - File striping is not set in fs_attrs: check it exists in DB
     *      If not, get stripe info from filesystem
     * - File striping is set in fs_attrs:
     *      - Check stripe validator in DB: if OK, don't update DB info
     *      - if an error is reported, update with the new values.
     */
    if (!ATTR_MASK_TEST(&p_op->fs_attrs, stripe_info)) {
#endif
        /* check it exists in DB */
        if (ListMgr_CheckStripe(lmgr, &p_op->entry_id, VALID_EXISTS) !=
            DB_SUCCESS) {
            DisplayLog(LVL_DEBUG, ENTRYPROC_TAG,
                       DFID ": stripe information is missing/invalid in DB",
                       PFID(&p_op->entry_id));

            /* don't need to get stripe if we already have fresh stripe info
             * from FS */
            if (!(ATTR_MASK_TEST(&p_op->fs_attrs, stripe_info)
                  && ATTR_MASK_TEST(&p_op->fs_attrs, stripe_items))) {
                attr_mask_set_index(&p_op->fs_attr_need,
                                    ATTR_INDEX_stripe_info);
                attr_mask_set_index(&p_op->fs_attr_need,
                                    ATTR_INDEX_stripe_items);
            }
        } else  /* stripe is OK, don't update stripe items */
            clear_stripe_info(&p_op->fs_attrs);

#ifdef HAVE_LLAPI_FSWAP_LAYOUTS
    } else if (ListMgr_CheckStripe(lmgr, &p_op->entry_id,
                                   ATTR(&p_op->fs_attrs, stripe_info).validator)
               == DB_SUCCESS) {
        /* don't update */
        clear_stripe_info(&p_op->fs_attrs);
    } else  /* keep stripe info in fs_attrs, as it must be updated */
        DisplayLog(LVL_DEBUG, ENTRYPROC_TAG,
                   DFID ": stripe information has changed",
                   PFID(&p_op->entry_id));
#endif
}
#endif

static void *entry_proc_cfg_new(void)
{
    return calloc(1, sizeof(entry_proc_config_t));
}

static void entry_proc_cfg_free(void *cfg)
{
    free(cfg);
}

/* export config functions */
mod_cfg_funcs_t entry_proc_cfg_hdlr = {
    .module_name = "entry processor",
    .new = entry_proc_cfg_new,
    .free = entry_proc_cfg_free,
    .set_default = entry_proc_cfg_set_default,
    .read = entry_proc_cfg_read,
    .set_config = entry_proc_cfg_set,
    .write_default = entry_proc_cfg_write_default,
    .write_template = entry_proc_cfg_write_template
};
