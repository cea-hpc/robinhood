/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009-2016 CEA/DAM
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

#include "policy_run.h"
#include "run_policies.h"
#include "list_mgr.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "Memory.h"
#include "xplatform_print.h"
#include "update_params.h"
#include "status_manager.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>

#define CHECK_QUEUE_INTERVAL    1

#define ignore_policies(_p) ((_p)->flags & RUNFLG_IGNORE_POL)
#define dry_run(_p)         ((_p)->flags & RUNFLG_DRY_RUN)
#define aborted(_p)         ((_p)->aborted)
#define no_limit(_p)        ((_p)->flags & RUNFLG_NO_LIMIT)
#define force_run(_p)       ((_p)->flags & RUNFLG_FORCE_RUN)
#define tag(_p)             ((_p)->descr->name)

#define TAG "PolicyRun"

typedef struct queue_item__ {
    entry_id_t entry_id;
    attr_set_t entry_attr;
    unsigned long targeted;
} queue_item_t;

/**
 *  alloc a new worker item so it can be pushed to the worker queue.
 */
static queue_item_t *entry2queue_item(entry_id_t *p_entry_id,
                                      attr_set_t *p_attr_set,
                                      unsigned long targeted)
{
    queue_item_t *new_entry;

    new_entry = (queue_item_t *) MemAlloc(sizeof(queue_item_t));
    if (!new_entry)
        return NULL;

    new_entry->entry_id = *p_entry_id;
    new_entry->entry_attr = *p_attr_set;
    new_entry->targeted = targeted;

    return new_entry;
}

/**
 * Free a queue Item (and the resources of its entry_attr).
 */
static void free_queue_item(queue_item_t *item)
{
    ListMgr_FreeAttrs(&item->entry_attr);
    MemFree(item);
}

typedef struct subst_args {
    action_params_t *params;
    const entry_id_t *id;
    const attr_set_t *attrs;
    const char **subst_array;
    const sm_instance_t *smi;
} subst_args_t;

/** substitute placeholders in a param value */
static int subst_one_param(const char *key, const char *val, void *udata)
{
    subst_args_t *args = (subst_args_t *) udata;
    gchar *new_val;
    char *descr = NULL;
    int rc;

    asprintf(&descr, "parameter %s='%s'", key, val);
    new_val = subst_params(val, descr, args->id, args->attrs, args->params,
                           args->subst_array, args->smi, false, false);
    free(descr);

    if (!new_val)
        return -EINVAL;

    rc = rbh_param_set(args->params, key, new_val, true);

    g_free(new_val);

    return rc;
}

static void set_addl_params(const char *addl_params[], unsigned int size,
                            const rule_item_t *rule,
                            const fileset_item_t *fileset)
{
    int last_param_idx = 0;

    if (rule != NULL) {
        if (unlikely(size < last_param_idx + 2))
            RBH_BUG("set_addl_params: array parameter too small");

        addl_params[last_param_idx] = "rule";
        addl_params[last_param_idx + 1] = rule->rule_id;
        last_param_idx += 2;
    }

    /* add params from fileclass (possibly override previous params) */
    if (fileset != NULL) {
        if (unlikely(size < last_param_idx + 2))
            RBH_BUG("set_addl_params: array parameter too small");

        addl_params[last_param_idx] = "fileclass";
        addl_params[last_param_idx + 1] = fileset->fileset_id;
        last_param_idx += 2;
    }

    if (unlikely(size < last_param_idx + 1))
        RBH_BUG("set_addl_params: array parameter too small");

    /* terminate the list of addl params */
    addl_params[last_param_idx] = NULL;
}

/**
 * Build action parameters according to: (in growing priority)
 *  - policy action_params
 *  - policy rule action_params
 *  - action_params of the matched fileclass
 * @param(out)  params  the params struct to be set
 * @param(in)   policy  the policy to build params for
 * @param(in)   rule    the matched policy rule
 * @param(in)   fileset the matched fileset for the policy rule
 * @return  0 on success, a negative value on error.
 */
static int build_action_params(action_params_t *params,
                               const entry_id_t *id,
                               const attr_set_t *attrs,
                               const policy_info_t *policy,
                               const rule_item_t *rule,
                               const fileset_item_t *fileset)
{
    int rc = 0;
    char const *addl_params[5]; /* 5 max: "fileclass" + its name,
                                  "rule" + its name, NULL */
    subst_args_t subst_param_args = {
        .params = params,
        .id = id,
        .attrs = attrs,
        .smi = policy->descr->status_mgr
    };

    if (params == NULL)
        return -EINVAL;

    /* Merging parameters from:
     * 1) policy
     * 2) trigger
     * 2) policy rule
     * 3) fileclass
     */
    /* params from policy */
    if (likely(policy->config != NULL)) {
        rc = rbh_params_copy(params, &policy->config->action_params);
        if (rc)
            goto err;
    }

    /* add params from trigger (possibly override previous params) */
    if (policy->trigger_action_params != NULL) {
        rc = rbh_params_copy(params, policy->trigger_action_params);
        if (rc)
            goto err;
    }

    /* add params from rule (possibly override previous params) */
    if (rule != NULL) {
        rc = rbh_params_copy(params, &rule->action_params);
        if (rc)
            goto err;
    }

    /* add params from fileclass (possibly override previous params) */
    if (fileset != NULL) {
        const action_params_t *fileset_params;

        /* check if there are parameters for the given policy */
        fileset_params = get_fileset_policy_params(fileset,
                                                   policy->descr->name);
        if (fileset_params != NULL) {
            rc = rbh_params_copy(params, fileset_params);
            if (rc)
                goto err;
        }
    }

    set_addl_params(addl_params, sizeof(addl_params) / sizeof(char *), rule,
                    fileset);
    subst_param_args.subst_array = addl_params;

    /* replace placeholders in action params */
    rc = rbh_params_foreach(params, subst_one_param, &subst_param_args);
    if (rc)
        goto err;

    return 0;

 err:
    rbh_params_free(params);
    return rc;
}

/** Execute a policy action. */
static int policy_action(policy_info_t *policy,
                         const rule_item_t *rule,
                         const fileset_item_t *fileset, const entry_id_t *id,
                         attr_set_t *p_attr_set,
                         const action_params_t *params, post_action_e *after)
{
    int rc = 0;
    sm_instance_t *smi = policy->descr->status_mgr;
    const policy_action_t *actionp = NULL;

    /* Get the action from policy rule, if defined.
     * Else, get the default action for the policy. */
    if (rule != NULL && rule->action.type != ACTION_UNSET)
        actionp = &rule->action;
    else
        /* defaults to default_action from */
        actionp = &policy->config->action;

    /* log as DEBUG level if 'report_actions' is disabled */
    DisplayLog(policy->config->report_actions ? LVL_EVENT : LVL_DEBUG,
               tag(policy),
               "%sExecuting policy action on: " DFID_NOBRACE " (%s)",
               dry_run(policy) ? "(dry-run) " : "", PFID(id),
               ATTR(p_attr_set, fullpath));
    if (log_config.debug_level >= LVL_DEBUG) {
        GString *str = g_string_new("");
        rc = rbh_params_serialize(params, str, NULL, RBH_PARAM_CSV);
        if (rc == 0)
            DisplayLog(LVL_DEBUG, tag(policy), DFID ": action_params: %s",
                       PFID(id), str->str);
        g_string_free(str, TRUE);
    }

    if (dry_run(policy))
        return 0;

    /* If the status manager has an 'executor', make it run the action.
     * Else, run directly the action function. */
    if (smi != NULL && smi->sm->executor != NULL) {
        /* @TODO provide a DB callback */
        rc = smi->sm->executor(smi, policy->descr->implements, actionp,
                               id, p_attr_set, params, after, NULL, NULL);
    } else {
        switch (actionp->type) {
        case ACTION_FUNCTION:
            /* @TODO provide a DB callback */
            DisplayLog(LVL_DEBUG, tag(policy), DFID ": action: %s",
                       PFID(id), actionp->action_u.func.name);
            rc = actionp->action_u.func.call(id, p_attr_set, params, after,
                                             NULL, NULL);
            break;
        case ACTION_COMMAND:   /* execute custom action */
            {
                char *descr = NULL;
                char **cmd;
                char const *addl_params[5];

                set_addl_params(addl_params,
                                sizeof(addl_params) / sizeof(char *), rule,
                                fileset);

                asprintf(&descr, "action command '%s'",
                         actionp->action_u.command[0]);

                /* replaces placeholders in command */
                rc = subst_shell_params(actionp->action_u.command, descr,
                                        id, p_attr_set, params, addl_params,
                                        smi, true, &cmd);
                free(descr);
                if (rc == 0) {
                    /* call custom command */
                    if (log_config.debug_level >= LVL_DEBUG) {
                        char *log_cmd = concat_cmd(cmd);
                        DisplayLog(LVL_DEBUG, tag(policy),
                                   DFID ": action: cmd(%s)", PFID(id), log_cmd);
                        free(log_cmd);
                    }

                    rc = execute_shell_command(cmd, cb_stderr_to_log,
                                               (void *)LVL_DEBUG);
                    g_strfreev(cmd);
                    /* @TODO handle other hardlinks to the same entry */
                }

                /* external commands can't set 'after': default to update */
                *after = PA_UPDATE;

                break;
            }
        case ACTION_UNSET:
        case ACTION_NONE:
            rc = 0;
            break;
        }

        /* call action callback if there is no status manager executor to wrap
         * actions */
        if (smi != NULL && smi->sm->action_cb != NULL) {
            int tmp_rc = smi->sm->action_cb(smi, policy->descr->implements, rc,
                                            id, p_attr_set, after);
            if (tmp_rc)
                DisplayLog(LVL_MAJOR, tag(policy),
                           "Action callback failed for action '%s': rc=%d",
                           policy->descr->implements ? policy->descr->
                           implements : "<null>", tmp_rc);
        }
    }

    return rc;
}

/**
 * Return the value of the current attribute for sorting the policy LRU.
 */
static inline int get_sort_attr(policy_info_t *p, const attr_set_t *p_attrs)
{
    if (p->config->lru_sort_attr == LRU_ATTR_NONE)
        return -1;

    if (!attr_mask_test_index(&p_attrs->attr_mask, p->config->lru_sort_attr))
        return -1;

    if (is_sm_info(p->config->lru_sort_attr)) {
        unsigned int idx = attr2sminfo_index(p->config->lru_sort_attr);

        return *((unsigned int *)p_attrs->attr_values.sm_info[idx]);
    }

    switch (p->config->lru_sort_attr) {
    case ATTR_INDEX_creation_time:
        return ATTR(p_attrs, creation_time);
    case ATTR_INDEX_last_mod:
        return ATTR(p_attrs, last_mod);
    case ATTR_INDEX_last_access:
        return ATTR(p_attrs, last_access);
    case ATTR_INDEX_rm_time:
        return ATTR(p_attrs, rm_time);
    default:
        return -1;
    }
}

/** set dummy time attributes, to check 'end of list' criteria */
static inline void set_max_time_attrs(policy_info_t *p, attr_set_t *p_attrs,
                                      time_t value)
{
    switch (p->config->lru_sort_attr) {
    case ATTR_INDEX_last_mod:
        ATTR_MASK_SET(p_attrs, last_mod);
        ATTR(p_attrs, last_mod) = value;
        /* cr_time always <=  last_mod */
        ATTR_MASK_SET(p_attrs, creation_time);
        ATTR(p_attrs, creation_time) = value;
        break;

    case ATTR_INDEX_last_access:
        ATTR_MASK_SET(p_attrs, last_access);
        ATTR(p_attrs, last_access) = value;
        /* in robinhood, lastmod <= last_access as
         * last_access=MAX(atime,mtime) */
        ATTR_MASK_SET(p_attrs, last_mod);
        ATTR(p_attrs, last_mod) = value;
        /* cr_time always <= last_mod */
        ATTR_MASK_SET(p_attrs, creation_time);
        ATTR(p_attrs, creation_time) = value;
        break;

    case ATTR_INDEX_creation_time:
        ATTR_MASK_SET(p_attrs, creation_time);
        ATTR(p_attrs, creation_time) = value;
        break;

    default:
        if (is_sm_info(p->config->lru_sort_attr)) {
            int *dup = malloc(sizeof(int));

            if (!dup)
                return;
            *dup = (int)value;

            /* Don't know the implications of this attribute
             * on other time attributes.
             * So, just set its value and return. */
            if (set_sm_info(p->descr->status_mgr, p_attrs,
                            attr2sminfo_index(p->config->lru_sort_attr)
                            - p->descr->status_mgr->sm_info_offset, dup))
                free(dup);
            return;
        }

        /* unsupported */
        RBH_BUG("Unsupported LRU sort attribute");
    }

#if 0   /* FIXME RBHv3: guess other times, depending on status scope? */
    /* If entry is dirty (migration):
     *      creation_time <= last_archive <= last mod (entry is dirty)
     *                    <= last_access
     * If entry is synchro (purge):
     *      creation_time <= last_mod <= last_access
     *                    <= last_archive (entry is synchro)
     *      creation_time <= last_restore
     *                    <= last_access (entry still not purged)
     */

    /* what about other times??? */
    if entry is dirty: last_archive < last_mod < last_access
#endif
}

/** return attribute name from its index. */
static const char *pol_attrindex2name(int index)
{
    if (is_std_attr(index))
        return field_infos[index].field_name;
    else if (is_status(index))
        return get_sm_instance(attr2status_index(index))->db_field;
    else if (is_sm_info(index))
        return sm_attr_info[attr2sminfo_index(index)].user_attr_name;

    return "?";
}

/** return the name of the lru_sort_attr of the policy */
static inline const char *sort_attr_name(const policy_info_t *pol)
{
    int attr = pol->config->lru_sort_attr;

    if (attr == LRU_ATTR_NONE)
        return "none";

    return pol_attrindex2name(attr);
}

/**
 * Given the timestamp of the last processed entry, this tries to guess
 * if next entries still have a chance to match the policy.
 * Example: if entries are sorted by last access (from the older to the newer)
 * and if the last listed entry is too recent to match any policy,
 * then next entries won't match too.
 */
static bool heuristic_end_of_list(policy_info_t *policy, time_t last_time)
{
    entry_id_t void_id;
    attr_set_t void_attr = ATTR_SET_INIT;
    bool rb = false;

    /* list all files if policies are ignored */
    if (ignore_policies(policy))
        return false;

    /* don't rely on fake times (0, 1 or in the future...) */
    if (last_time <= 1 || last_time > time(NULL))
        return false;

    /* Optimization:
     * we build a void entry with time attr = current sort attr
     * If it doesn't match any policy, next entries won't match too
     * because entries are sorted by this attribute, so it is not necessary
     * to continue. */
    memset(&void_id, 0, sizeof(entry_id_t));
    memset(&void_attr, 0, sizeof(attr_set_t));
    ATTR_MASK_INIT(&void_attr);

    /* We set a max value for time conditions.
     * In this case, if an entry matches age > x with
     * its times attributes = max_value (the latest),
     * it would also match for older times.
     * So, set all times <= sort order of policy (depends on status scope?)
     */
    set_max_time_attrs(policy, &void_attr, last_time);

    if (policy_match_all(policy->descr, &void_id, &void_attr,
                         policy->time_modifier, NULL) == POLICY_NO_MATCH) {
        DisplayLog(LVL_DEBUG, tag(policy),
                   "Optimization: entries with %s later than %lu cannot match "
                   "any policy condition. Stop retrieving DB entries.",
                   sort_attr_name(policy), last_time);
        rb = true;
    } else {
        rb = false;
    }

    ListMgr_FreeAttrs(&void_attr);

    return rb;
}

/**
 *  Sum the number of acks from a status tab
 */
static inline unsigned int ack_count(const unsigned int *status_tab)
{
    unsigned int i, sum;
    sum = 0;

    for (i = 0; i < AS_ENUM_COUNT; i++)
        sum += status_tab[i];

    return sum;
}

/**
*  Sum the number of skipped entries from a status tab
*/
static inline unsigned int skipped_count(const unsigned int *status_tab)
{
    int i;
    unsigned int nb = 0;

    /* skipped if it has been accessed, has changed, is whitelisted,
     * matches no policy, is in use, already running, type not supported...
     * i.e. status in AS_ACCESSED to AS_ALREADY
     */
    for (i = AS_ACCESSED; i <= AS_ALREADY; i++)
        nb += status_tab[i];

    return nb;
}

/**
 *  Sum the number of errors from a status tab
 */
static inline unsigned int error_count(const unsigned int *status_tab)
{
    int i;
    unsigned int nb = 0;

    /* next status are errors */
    for (i = AS_MISSING_MD; i <= AS_ERROR; i++)
        nb += status_tab[i];

    return nb;
}

/**
 * Convert queue statistics to counters
 */
static void queue_stats2counters(const unsigned long long *feedback_before,
                                 const unsigned long long *feedback_after,
                                 const unsigned int *status_tab_before,
                                 const unsigned int *status_tab_after,
                                 counters_t *ctr_ok, counters_t *ctr_nok,
                                 unsigned int *ack, unsigned int *errors,
                                 unsigned int *skipped)
{
    if (ctr_ok)
        memset(ctr_ok, 0, sizeof(*ctr_ok));
    if (ctr_nok)
        memset(ctr_nok, 0, sizeof(*ctr_nok));
    *ack = 0;
    *errors = 0;
    *skipped = 0;

    if (feedback_before != NULL && feedback_after != NULL) {
        if (ctr_ok) {
            ctr_ok->count =
                feedback_after[AF_NBR_OK] - feedback_before[AF_NBR_OK];
            ctr_ok->vol =
                feedback_after[AF_VOL_OK] - feedback_before[AF_VOL_OK];
            ctr_ok->blocks =
                feedback_after[AF_BLOCKS_OK] -
                feedback_before[AF_BLOCKS_OK];
            ctr_ok->targeted =
                feedback_after[AF_TARGETED_OK] -
                feedback_before[AF_TARGETED_OK];
        }
        if (ctr_nok) {
            ctr_nok->count =
                feedback_after[AF_NBR_NOK] - feedback_before[AF_NBR_NOK];
            ctr_nok->vol =
                feedback_after[AF_VOL_NOK] - feedback_before[AF_VOL_NOK];
            ctr_nok->blocks =
                feedback_after[AF_BLOCKS_NOK] -
                feedback_before[AF_BLOCKS_NOK];
            ctr_nok->targeted =
                feedback_after[AF_TARGETED_NOK] -
                feedback_before[AF_TARGETED_NOK];
        }
    }
    if (status_tab_before != NULL && status_tab_after != NULL) {
        *ack = ack_count(status_tab_after) - ack_count(status_tab_before);
        *skipped =
            skipped_count(status_tab_after) -
            skipped_count(status_tab_before);
        *errors =
            error_count(status_tab_after) - error_count(status_tab_before);
    }
}

/**
 * Test if the policy run limit has been reached,
 * or if the max error rate is reached.
 */
static bool check_limit(policy_info_t *policy,
                        const counters_t *ctr_ok,
                        unsigned int errors, const counters_t *limit)
{
    unsigned int total;

    /* --no-limit option specified? */
    if (no_limit(policy))
        return false;

    /* counter of successful actions reached the limit? */
    if (counter_reached_limit(ctr_ok, limit))
        return true;

    total = ctr_ok->count + errors;
    if (total == 0)
        return false;

    /* stop if too many error occurred */
    if ((policy->config->suspend_error_pct > 0.0)
        && (policy->config->suspend_error_min > 0)
        && (errors >= policy->config->suspend_error_min)) {
        /* total >= errors >= suspend_error_min  > 0
         * => total != 0 */
        double pct = 100.0 * (float)errors / (float)total;
        if (pct >= policy->config->suspend_error_pct) {
            DisplayLog(LVL_EVENT, tag(policy),
                       "error count %u >= %u, error rate %.2f%% >= %.2f => suspending policy run",
                       errors, policy->config->suspend_error_min,
                       pct, policy->config->suspend_error_pct);
            return true;
        }
    }
    return false;
}

/**
 * Compute an adaptive delay (in microseconds) to check if pending requests exceed the limit.
 * The computed value is 10% of the estimated time to process the current queue.
 */
#define USEC_PER_MSEC 1000  /* 1ms */
#define USEC_PER_SEC  1000000   /* 1s */
#define MIN_CHECK_DELAY   (10 * USEC_PER_MSEC)  /* 10ms */
#define MAX_CHECK_DELAY   USEC_PER_SEC
static unsigned long adaptive_check_delay_us(time_t policy_start,
                                             unsigned long long
                                             nb_processed,
                                             unsigned long long nb_pending)
{
    unsigned long check_delay;
    unsigned long spent_us = USEC_PER_SEC * (time(NULL) - policy_start);
    unsigned long us_per_ent;

    /* compute check_delay depending on past processing speed */
    if (spent_us == 0)
        spent_us = 100 * USEC_PER_MSEC; /* default to 100ms */

    if (nb_processed > 0) {
        /* how much time to process these entries? */
        us_per_ent = spent_us / nb_processed;
        /* how much to process 10% of current queue? */
        check_delay = (us_per_ent * nb_pending) / 10;
        DisplayLog(LVL_FULL, __func__,
                   "%llu entries processed @ %.2f ms/ent, "
                   "%llu pending => check delay = 10%% x %llu ms = %lu ms",
                   nb_processed, (float)us_per_ent / USEC_PER_MSEC,
                   nb_pending, (us_per_ent * nb_pending) / USEC_PER_MSEC,
                   check_delay / USEC_PER_MSEC);
    } else {
        /* nothing was done so far (check again in 10% x spent) */
        check_delay = spent_us / 10;
        DisplayLog(LVL_FULL, __func__,
                   "No entry processed, %llu pending => check delay = 10%% x %lu ms",
                   nb_pending, spent_us / USEC_PER_MSEC);
    }

    if (check_delay > MAX_CHECK_DELAY)
        check_delay = MAX_CHECK_DELAY;
    else if (check_delay < MIN_CHECK_DELAY)
        check_delay = MIN_CHECK_DELAY;

    return check_delay;
}

/**
 * Check if enqueued entries reach the limit.
 * If so, wait a while to recheck after some entries have been processed.
 * return if no more entries are pending,
 *     or if the limit is not reached
 *     or if the limit is definitely reached.
 * \retval true if policy run must stop
 * \retval false if policy run can continue
 */
static bool check_queue_limit(policy_info_t *pol,
                              const counters_t *pushed,
                              const unsigned long long *feedback_before,
                              const unsigned int *status_before,
                              const counters_t *target_ctr)
{
    unsigned long long feedback_after[AF_ENUM_COUNT];
    unsigned int status_after[AS_ENUM_COUNT];

    do {
        counters_t ctr_ok, ctr_nok, ctr_pending, ctr_pot;
        unsigned int errors, skipped, ack;

        RetrieveQueueStats(&pol->queue, NULL, NULL, NULL, NULL, NULL,
                           status_after, feedback_after);

        queue_stats2counters(feedback_before, feedback_after,
                             status_before, status_after,
                             &ctr_ok, &ctr_nok, &ack, &errors, &skipped);

        /* compute pending conters (pushed - done) */
        ctr_pending = *pushed;
        ctr_pending.count -= ack;
        ctr_pending.vol -= ctr_ok.vol + ctr_nok.vol;
        ctr_pending.blocks -= ctr_ok.blocks + ctr_nok.blocks;
        ctr_pending.targeted -= ctr_ok.targeted + ctr_nok.targeted;

        /* compute total counter */
        counters_add(&ctr_ok, &pol->progress.action_ctr);
        skipped += pol->progress.skipped;
        errors += pol->progress.errors;

        /* check the limit of all acknowledged status */
        if (check_limit(pol, &ctr_ok, errors, target_ctr))
            return true;

        /* 2) queue is empty and limit is not reached */
        if (ctr_pending.count == 0) {
            DisplayLog(LVL_FULL, tag(pol),
                       "queue is empty => continue enqueuing");
            return false;
        }

        /* check the potential limit of successful + pending */
        ctr_pot = ctr_pending;
        counters_add(&ctr_pot, &ctr_ok);
        DisplayLog(LVL_FULL, tag(pol), "requests: OK + pending = %llu",
                   ctr_pot.count);

        if (check_limit(pol, &ctr_pot, errors, target_ctr)) {
            unsigned long check_delay =
                adaptive_check_delay_us(pol->progress.policy_start,
                                        ctr_ok.count + errors + skipped,
                                        ctr_pending.count);
            DisplayLog(LVL_DEBUG, tag(pol),
                       "Limit potentially reached (%llu requests successful, "
                       "%llu requests in queue, volume: %llu done, %llu pending), "
                       "waiting %lums before re-checking.", ctr_ok.count,
                       ctr_pending.count, ctr_ok.vol, ctr_pending.vol,
                       check_delay / USEC_PER_MSEC);
            rh_usleep(check_delay);
            continue;
        } else {
            return false;
        }
    } while (1);

    RBH_BUG("This line should not be reached");
}

/**
 * build a filter from policies, to optimize DB queries.
 */
static int set_optimization_filters(policy_info_t *policy,
                                    lmgr_filter_t *p_filter)
{
    policy_rules_t *rules = &policy->descr->rules;
/** @TODO build a filter for getting the union of all filesets/conditions */

    /* If there is a single policy, try to convert its condition
     * to a simple filter.
     */
    if (rules->rule_count == 1) // TODO won't apply to LUA scripts
    {
        if (convert_boolexpr_to_simple_filter
            (&rules->rules[0].condition, p_filter,
             policy->descr->status_mgr, policy->time_modifier,
             policy->descr->manage_deleted ? FILTER_FLAG_ALLOW_NULL : 0)) {
            DisplayLog(LVL_FULL, tag(policy),
                       "Could not convert purge rule '%s' to simple filter.",
                       rules->rules[0].rule_id);
        }
    }

    if (!policy->config->recheck_ignored_entries) {
        int i;

        /* don't select files in ignored classes */
        for (i = 0; i < rules->ignore_count; i++) {
            filter_value_t fval;
            int flags = 0;

            fval.value.val_str = rules->ignore_list[i]->fileset_id;
            if (i == 0)
                flags = FILTER_FLAG_NOT | FILTER_FLAG_ALLOW_NULL;
            else
                flags = FILTER_FLAG_NOT;
            lmgr_simple_filter_add(p_filter, ATTR_INDEX_fileclass, EQUAL,
                                   fval, flags);
        }

        /* don't select entries maching 'ignore' statements */
        for (i = 0; i < rules->whitelist_count; i++) {
            if (convert_boolexpr_to_simple_filter
                (&rules->whitelist_rules[i].bool_expr, p_filter,
                 policy->descr->status_mgr, policy->time_modifier,
                 policy->descr->
                 manage_deleted ? FILTER_FLAG_ALLOW_NULL | FILTER_FLAG_NOT :
                 FILTER_FLAG_NOT)) {
                DisplayLog(LVL_DEBUG, tag(policy),
                           "Could not convert 'ignore' rule to simple filter.");
                DisplayLog(LVL_EVENT, tag(policy),
                           "Warning: 'ignore' rule is too complex and may "
                           "affect policy run performance");
            }
        }
    }

    /* avoid re-checking all old whitelisted entries at the beginning
     * of the list, so start from the first non-whitelisted file.
     * restart from initial file when no migration could be done. */
    if ((policy->config->lru_sort_attr != LRU_ATTR_NONE)
        && policy->first_eligible) {
        filter_value_t fval;
        char datestr[128];
        struct tm ts;

        fval.value.val_uint = policy->first_eligible;
        lmgr_simple_filter_add(p_filter, policy->config->lru_sort_attr,
                               MORETHAN, fval, 0);
        strftime(datestr, 128, "%Y/%m/%d %T",
                 localtime_r(&policy->first_eligible, &ts));
        DisplayLog(LVL_EVENT, tag(policy),
                   "Optimization: considering entries with %s newer than %s",
                   sort_attr_name(policy), datestr);
    }

    return 0;
}

/**
 * report the current policy run progress at regular interval.
 */
static void report_progress(policy_info_t *policy,
                            const unsigned long long *pass_begin,
                            const unsigned long long *pass_current,
                            const unsigned int *status_tab_begin,
                            const unsigned int *status_tab_current)
{
    counters_t curr_ctr;
    unsigned int ack, nb_errors, nb_skipped;

    /* get current pass counters */
    queue_stats2counters(pass_begin, pass_current, status_tab_begin,
                         status_tab_current, &curr_ctr, NULL, &ack,
                         &nb_errors, &nb_skipped);
    /* add counters of previous passes */
    counters_add(&curr_ctr, &policy->progress.action_ctr);
    nb_skipped += policy->progress.skipped;
    nb_errors += policy->progress.errors;

    /* say hello every runtime interval */
    if (time(NULL) - policy->progress.last_report >=
        policy->config->report_interval) {
        char buf1[128];
        char buf2[128];
        char buf3[128];
        unsigned int spent = time(NULL) - policy->progress.policy_start;
        if (spent == 0)
            return;
        FormatDuration(buf1, 128, spent);
        FormatFileSize(buf2, 128, curr_ctr.vol);
        FormatFileSize(buf3, 128, curr_ctr.vol / spent);

        DisplayLog(LVL_EVENT, tag(policy),
                   "Policy is running (started %s ago): "
                   "%llu actions succeeded (%.2f/sec); volume: %s (%s/sec); "
                   "skipped: %u; errors: %u", buf1, curr_ctr.count,
                   (float)curr_ctr.count / (float)spent, buf2, buf3,
                   nb_skipped, nb_errors);
        policy->progress.last_report = time(NULL);
    }
}

/**
 * Wait until the queue is empty or migrations timed-out.
 * \retval 0 when the queue is empty
 * \retval ETIME on timeout.
 */
static int wait_queue_empty(policy_info_t *policy,
                            unsigned int nb_submitted,
                            const unsigned long long *feedback_init,
                            const unsigned int *status_tab_init,
                            unsigned long long *feedback_after,
                            unsigned int *status_tab_after,
                            bool long_sleep)
{
    unsigned int nb_in_queue, nb_action_pending;

    /* Wait for end of purge pass */
    do {
        time_t last_push, last_pop, last_ack, last_activity;

        last_push = last_pop = last_ack = last_activity = 0;

        RetrieveQueueStats(&policy->queue, NULL, &nb_in_queue,
                           &last_push, &last_pop, &last_ack,
                           status_tab_after, feedback_after);

        /* the last time a request was pushed/poped/acknowledged */
        last_activity = MAX3(last_push, last_pop, last_ack);

        /* nb of operation pending
           = nb_enqueued - (nb ack after - nb ack before) */
        nb_action_pending = nb_submitted + ack_count(status_tab_init)
                            - ack_count(status_tab_after);

        if ((nb_in_queue > 0) || (nb_action_pending > 0)) {
            /* abort this pass if the last action was done a too long time
             * ago */
            if ((policy->config->action_timeout != 0) &&
                (time(NULL) - last_activity >
                 policy->config->action_timeout)) {
                DisplayLog(LVL_MAJOR, tag(policy), "Policy run time-out: "
                           "%u actions inactive for %us", nb_action_pending,
                           (unsigned int)(time(NULL) - last_activity));
                /* don't wait for current actions to end, continue with
                 * other entries */
                return ETIME;
            }

            report_progress(policy, feedback_init, feedback_after,
                            status_tab_init, status_tab_after);

            DisplayLog(LVL_DEBUG, tag(policy),
                       "Waiting for the end of current pass: "
                       "still %u entries pending (%u in queue, %u being processed). "
                       "Last action %us ago.",
                       nb_action_pending, nb_in_queue,
                       nb_action_pending - nb_in_queue,
                       (unsigned int)(time(NULL) - last_activity));

            if (long_sleep)
                rh_sleep(CHECK_QUEUE_INTERVAL);
            else
                rh_usleep(1000);
        } else
            DisplayLog(LVL_DEBUG, tag(policy), "End of current pass");
    }
    while ((nb_in_queue != 0) || (nb_action_pending != 0));

    return 0;
}

/** set the mask of attributes to be retrieved from db */
static attr_mask_t db_attr_mask(policy_info_t *policy,
                                const policy_param_t *param)
{
    attr_mask_t mask = { 0 };
    attr_mask_t tmp;

/* TODO depends on the prototype of the action to be taken + fileset mask
*       + condition mask... */

    /* needed for ListMgr_Remove() operations */
#ifdef _HAVE_FID
    mask.std |= ATTR_MASK_name | ATTR_MASK_parent_id;
#endif
    /* needed for posix operations, and for display */
    mask.std |= ATTR_MASK_fullpath;

    /* needed if update params != never */
    if (updt_params.md.when != UPDT_NEVER &&
        updt_params.md.when != UPDT_ALWAYS)
        mask.std |= ATTR_MASK_md_update;

#ifdef _HAVE_FID
    if (updt_params.path.when != UPDT_NEVER &&
        updt_params.path.when != UPDT_ALWAYS)
        mask.std |= ATTR_MASK_path_update;
#endif
    /* needed to check the entry order didn't change */
    if (policy->config->lru_sort_attr != LRU_ATTR_NONE)
        attr_mask_set_index(&mask, policy->config->lru_sort_attr);

    /* needed for size counters and logging, or to verify the entry
     * didn't change */
    mask.std |= ATTR_MASK_size;
    /* depends on policy params (limits) */
    if (param->target_ctr.blocks != 0 || param->target_ctr.targeted != 0)
        mask.std |= ATTR_MASK_blocks;
#ifdef _LUSTRE
    if (param->target == TGT_POOL || param->target == TGT_OST) {
        mask.std |= ATTR_MASK_stripe_info | ATTR_MASK_stripe_items;
    }
#endif

    /* Get attrs to match policy scope */
    mask = attr_mask_or(&mask, &policy->descr->scope_mask);

    /* needed (cached) attributes to check status from scope */
    tmp = attrs_for_status_mask(mask.status, false);
    mask = attr_mask_or(&mask, &tmp);

    /* needed attributes to check policy rules */
    mask = attr_mask_or(&mask, &policy->descr->rules.run_attr_mask);

    /* if the policy manages deleted entries, get all
     * SOFTRM attributes for the current status manager */
    if (policy->descr->manage_deleted
        && (policy->descr->status_mgr != NULL))
        mask =
            attr_mask_or(&mask,
                         &policy->descr->status_mgr->softrm_table_mask);

    // TODO class management?

    return mask;
}

/**
 * Compute the target amount for an entry.
 */
static int entry2tgt_amount(const policy_param_t *p_param,
                            const attr_set_t *attrs, counters_t *p_ctr)
{
    memset(p_ctr, 0, sizeof(*p_ctr));

    p_ctr->count = 1;
    if (ATTR_MASK_TEST(attrs, size))
        p_ctr->vol = ATTR(attrs, size);
    else if (ATTR_MASK_TEST(attrs, blocks))
        p_ctr->vol = ATTR(attrs, blocks) * DEV_BSIZE;

    if (ATTR_MASK_TEST(attrs, blocks))
        p_ctr->blocks = ATTR(attrs, blocks);

    if (p_param->target_ctr.targeted != 0) {
        /* When the target amount is not count, vol or blocks
         * This is the case for OST: the target is only a subset of the
         * blocks.
         */
#ifdef _LUSTRE
        if (p_param->target != TGT_OST && p_param->target)
        /* FIXME what about pool? */
#else
        if (p_param->target)
#endif
        {
            DisplayLog(LVL_CRIT, "PolicyRun",
                       "unsupported targeted limit != OST");
            return -1;
        }
#ifdef _LUSTRE
        p_ctr->targeted =
            BlocksOnOST(p_ctr->blocks, p_param->optarg_u.index,
                        &ATTR(attrs, stripe_info), &ATTR(attrs,
                                                         stripe_items));
#endif
    }

    return 0;
}

#ifdef HAVE_CHANGELOGS
#define BUILD_LIST_MSG "Building policy list - last full FS Scan:"
#else
#define BUILD_LIST_MSG "Building policy list from last full FS Scan:"
#endif

/**
 * Check if a filesystem scan has ever been done.
 * \retval ENOENT if no scan has been done (no complete filesystem list is
 *          available).
 */
static int check_scan_done(const policy_info_t *pol, lmgr_t *lmgr)
{
    char timestamp[1024];

    if (force_run(pol)) /* no check in that case */
        return 0;

    if (ListMgr_GetVar(lmgr, LAST_SCAN_END_TIME, timestamp,
                       sizeof(timestamp)) != DB_SUCCESS) {
        DisplayLog(LVL_MAJOR, tag(pol),
                   "Full FS Scan has never been done. Policy ordering would be done on a partial list "
                   "(use --force to apply the policy anyway).");
        return ENOENT;
    } else {
        time_t last_scan = atoi(timestamp);
        struct tm date;

        localtime_r(&last_scan, &date);
        DisplayLog(LVL_EVENT, tag(pol),
                   BUILD_LIST_MSG " %.4d/%.2d/%.2d %.2d:%.2d:%.2d",
                   1900 + date.tm_year, date.tm_mon + 1, date.tm_mday,
                   date.tm_hour, date.tm_min, date.tm_sec);
    }
    return 0;
}

/**
 * Set a DB filter and attr mask depending on the specified target.
 * By the way, log target information for this run.
 */
static int set_target_filter(const policy_info_t *pol,
                             const policy_param_t *p_param,
                             lmgr_filter_t *filter,
                             attr_mask_t *attr_mask)
{
    filter_value_t fval;

    switch (p_param->target) {
    case TGT_FS:   /* apply policies to the filesystem */
        DisplayLog(LVL_MAJOR, tag(pol), "Starting policy run");
        return 0;

#ifdef _LUSTRE
    case TGT_OST:  /* apply policies to the specified OST */
        DisplayLog(LVL_MAJOR, tag(pol), "Starting policy run on OST #%d",
                   p_param->optarg_u.index);

        /* retrieve stripe info and stripe items */
        attr_mask->std |= ATTR_MASK_stripe_info | ATTR_MASK_stripe_items;

        /* retrieve files from this OST */
        fval.value.val_uint = p_param->optarg_u.index;
        return lmgr_simple_filter_add(filter, ATTR_INDEX_stripe_items,
                                      EQUAL, fval, 0);

    case TGT_POOL: /* apply policies to the specified pool of OSTs */
        DisplayLog(LVL_MAJOR, tag(pol), "Starting policy run on pool '%s'",
                   p_param->optarg_u.name);

        attr_mask->std |= ATTR_MASK_stripe_info | ATTR_MASK_stripe_items;

        /* retrieve files from this pool */
        fval.value.val_str = p_param->optarg_u.name;
        return lmgr_simple_filter_add(filter, ATTR_INDEX_stripe_info,
                                      WILDCARDS_IN(fval.value.
                                                   val_str) ? LIKE : EQUAL,
                                      fval, 0);
#endif

    case TGT_USER: /* apply policies to the specified user */
        DisplayLog(LVL_MAJOR, tag(pol),
                   "Starting policy run on '%s' user files",
                   p_param->optarg_u.name);

        attr_mask->std |= ATTR_MASK_uid;

        /* retrieve files for this owner */
        if (set_uid_val(p_param->optarg_u.name, &fval.value))
            return EINVAL;
        return lmgr_simple_filter_add(filter, ATTR_INDEX_uid,
                                      WILDCARDS_IN(p_param->optarg_u.
                                                   name) ? LIKE : EQUAL,
                                      fval, 0);

    case TGT_GROUP:    /* apply policies to the specified group */
        DisplayLog(LVL_MAJOR, tag(pol),
                   "Starting policy run on '%s' group files",
                   p_param->optarg_u.name);

        attr_mask->std |= ATTR_MASK_gid;

        /* retrieve files for this group */
        if (set_gid_val(p_param->optarg_u.name, &fval.value))
            return EINVAL;
        return lmgr_simple_filter_add(filter, ATTR_INDEX_gid,
                                      WILDCARDS_IN(p_param->optarg_u.
                                                   name) ? LIKE : EQUAL,
                                      fval, 0);

    case TGT_CLASS:    /* apply policies to the specified fileclass */
        DisplayLog(LVL_MAJOR, tag(pol),
                   "Starting policy run on fileclass '%s'",
                   p_param->optarg_u.name);

        attr_mask->std |= ATTR_MASK_fileclass;

        fval.value.val_str = p_param->optarg_u.name;
        return lmgr_simple_filter_add(filter, ATTR_INDEX_fileclass,
                                      WILDCARDS_IN(fval.value.
                                                   val_str) ? LIKE : EQUAL,
                                      fval, 0);

    case TGT_FILE:
        /* this is supposed to be handled by specific code:
         * single_file_run() */
        RBH_BUG("ERROR: file target type is supposed to be handled in "
                "a different function");
        return ENOTSUP;

    default:
        DisplayLog(LVL_CRIT, tag(pol), "ERROR: unhandled target type %u\n",
                   p_param->target);
        return EINVAL;
    }
}

/** reset stats before a policy pass */
static void init_pass_stats(policy_info_t *pol, counters_t *pushed_ctr,
                            unsigned int *status_tab_before,
                            unsigned int *status_tab_after,
                            unsigned long long *feedback_before,
                            unsigned long long *feedback_after)
{
    /* Retrieve stats before starting policy,
     * for computing a delta later.
     */
    RetrieveQueueStats(&pol->queue, NULL, NULL, NULL, NULL, NULL,
                       status_tab_before, feedback_before);

    /* set pushed entries counter = 0 */
    memset(pushed_ctr, 0, sizeof(*pushed_ctr));

    /* reset after's */
    memset(feedback_after, 0, AF_ENUM_COUNT * sizeof(*feedback_after));
    memset(status_tab_after, 0, AS_ENUM_COUNT * sizeof(*status_tab_after));
}

static void update_pass_stats(policy_info_t *pol,
                              unsigned int *status_tab_before,
                              unsigned int *status_tab_after,
                              unsigned long long *feedback_before,
                              unsigned long long *feedback_after)
{
    /* how much has been processed, errors, skipped... */
    pol->progress.action_ctr.count += feedback_after[AF_NBR_OK]
        - feedback_before[AF_NBR_OK];
    pol->progress.action_ctr.vol += feedback_after[AF_VOL_OK]
        - feedback_before[AF_VOL_OK];
    pol->progress.action_ctr.blocks += feedback_after[AF_BLOCKS_OK]
        - feedback_before[AF_BLOCKS_OK];
    pol->progress.action_ctr.targeted += feedback_after[AF_TARGETED_OK]
        - feedback_before[AF_TARGETED_OK];
    pol->progress.skipped += skipped_count(status_tab_after)
        - skipped_count(status_tab_before);
    pol->progress.errors += error_count(status_tab_after)
        - error_count(status_tab_before);
}

/* these types allow generic iteration on std entries or removed entries */

typedef enum { IT_LIST, IT_RMD } it_type_e;

struct policy_iter {
    it_type_e it_type;
    union {
        struct lmgr_iterator_t *std_iter;
        struct lmgr_rm_list_t *rmd_iter;
    } it;
};

static inline int iter_next(struct policy_iter *it, entry_id_t *p_id,
                            attr_set_t *p_attrs)
{
    switch (it->it_type) {
    case IT_LIST:
        return ListMgr_GetNext(it->it.std_iter, p_id, p_attrs);
    case IT_RMD:
        return ListMgr_GetNextRmEntry(it->it.rmd_iter, p_id, p_attrs);
    }
    return DB_INVALID_ARG;
}

static inline void iter_close(struct policy_iter *it)
{
    switch (it->it_type) {
    case IT_LIST:
        if (it->it.std_iter == NULL)
            return;
        ListMgr_CloseIterator(it->it.std_iter);
        it->it.std_iter = NULL;
        break;
    case IT_RMD:
        if (it->it.rmd_iter == NULL)
            return;
        ListMgr_CloseRmList(it->it.rmd_iter);
        it->it.rmd_iter = NULL;
        break;
    }
}

static inline int iter_open(lmgr_t *lmgr,
                            it_type_e type,
                            struct policy_iter *it,
                            lmgr_filter_t *filter,
                            const lmgr_sort_type_t *sort_type,
                            const lmgr_iter_opt_t *opt)
{
    it->it_type = type;
    switch (type) {
    case IT_LIST:
        it->it.std_iter = ListMgr_Iterator(lmgr, filter, sort_type, opt);
        if (it->it.std_iter == NULL)
            return DB_REQUEST_FAILED;
        break;

    case IT_RMD:
        it->it.rmd_iter = ListMgr_RmList(lmgr, filter, sort_type);
        if (it->it.rmd_iter == NULL)
            return DB_REQUEST_FAILED;
        break;
    }
    return DB_SUCCESS;
}

/** return codes of fill_workers_queue() */
typedef enum {
    PASS_EOL,
    PASS_LIMIT,
    PASS_ABORTED,
    PASS_ERROR,
} pass_status_e;

/**
* Get entries from the DB and push them to the workers queue until:
* - end of list is reached
* or:
* - the policy limit is potentially reached.
* @param attr_mask  Mask of attrs to be retrieved from the DB,
*                   to be able to match policy rules, scope...
*/
static pass_status_e fill_workers_queue(policy_info_t *pol,
                                        const policy_param_t *p_param,
                                        lmgr_t *lmgr,
                                        struct policy_iter *it,
                                        const lmgr_iter_opt_t *req_opt,
                                        const lmgr_sort_type_t *sort_type,
                                        lmgr_filter_t *filter,
                                        attr_mask_t attr_mask,
                                        int *last_sort_time,
                                        unsigned int *db_current_list_count,
                                        unsigned int *db_total_list_count)
{
    int rc;
    pass_status_e st;
    attr_set_t attr_set;
    entry_id_t entry_id;
    counters_t pushed_ctr;
    filter_value_t fval;
    unsigned long long feedback_before[AF_ENUM_COUNT];
    unsigned long long feedback_after[AF_ENUM_COUNT];
    unsigned int status_tab_before[AS_ENUM_COUNT];
    unsigned int status_tab_after[AS_ENUM_COUNT];

    init_pass_stats(pol, &pushed_ctr, status_tab_before, status_tab_after,
                    feedback_before, feedback_after);

    /* by default, exit the loop when check_queue_limit is true */
    st = PASS_LIMIT;

    /* List entries for policy */
    do {
        counters_t entry_amount;

        /* reset attr_mask, if it was altered by last ListMgr_GetNext() */
        memset(&attr_set, 0, sizeof(attr_set_t));
        attr_set.attr_mask = attr_mask;

        memset(&entry_id, 0, sizeof(entry_id_t));

        rc = iter_next(it, &entry_id, &attr_set);

        if (aborted(pol)) {
            /* free the last returned entry */
            if (rc == 0)
                ListMgr_FreeAttrs(&attr_set);

            DisplayLog(LVL_MAJOR, tag(pol),
                       "Policy run aborted, stop enqueuing requests.");
            st = PASS_ABORTED;
            break;
        } else if (rc == DB_END_OF_LIST) {
            *db_total_list_count += *db_current_list_count;

            /* if limit = inifinite => END OF LIST */
            if ((*db_current_list_count == 0)
                || ((req_opt->list_count_max > 0) &&
                    (*db_current_list_count < req_opt->list_count_max))) {
                DisplayLog(LVL_FULL, tag(pol), "End of list "
                           "(%u entries returned)", *db_total_list_count);
                st = PASS_EOL;
                break;
            }

            /* no new useless request when entries are sorted
             * and the max time is reached */
            if ((pol->config->lru_sort_attr != LRU_ATTR_NONE)
                && heuristic_end_of_list(pol, *last_sort_time)) {
                st = PASS_EOL;
                break;
            }

            /* Free previous iterator */
            iter_close(it);

            /* we must wait that migr. queue is empty,
             * to prevent from processing the same entry twice
             * (not safe until their md_update has not been updated).
             */
            wait_queue_empty(pol, pushed_ctr.count, feedback_before,
                             status_tab_before, feedback_after,
                             status_tab_after, false);

            /* perform a new request with next entries */

            /* /!\ if there is already a filter on <sort_attr> or md_update
             * only replace it, do not add a new filter.
             */

            /* don't retrieve just-updated entries
             * (update>=first_request_time) */
            fval.value.val_int = pol->progress.policy_start;
            rc = lmgr_simple_filter_add_or_replace(filter,
                                                   ATTR_INDEX_md_update,
                                                   LESSTHAN_STRICT, fval,
                                                   FILTER_FLAG_ALLOW_NULL);
            if (rc)
                return PASS_ERROR;

            /* filter on <sort_time> */
            if (pol->config->lru_sort_attr != LRU_ATTR_NONE) {
                fval.value.val_int = *last_sort_time;
                rc = lmgr_simple_filter_add_or_replace(filter,
                                               pol->config->lru_sort_attr,
                                               MORETHAN, fval,
                                               FILTER_FLAG_ALLOW_NULL);
                if (rc)
                    return PASS_ERROR;

                DisplayLog(LVL_DEBUG, tag(pol),
                           "Performing new request with a limit of %u entries"
                           " and %s >= %d and md_update < %ld ",
                           req_opt->list_count_max, sort_attr_name(pol),
                           *last_sort_time, pol->progress.policy_start);
            } else {
                DisplayLog(LVL_DEBUG, tag(pol),
                           "Performing new request with a limit of %u entries"
                           " and md_update < %ld ", req_opt->list_count_max,
                           pol->progress.policy_start);
            }

            *db_current_list_count = 0;
            rc = iter_open(lmgr, it->it_type, it, filter, sort_type,
                           req_opt);
            if (rc != DB_SUCCESS) {
                DisplayLog(LVL_CRIT, tag(pol),
                           "Error %d retrieving list of candidates from "
                           "database. Policy run cancelled.", rc);
                return PASS_ERROR;
            }
            continue;
        } else if (rc != 0) {
            DisplayLog(LVL_CRIT, tag(pol),
                       "Error %d getting next entry of iterator", rc);
            st = PASS_ERROR;
            break;
        }

        (*db_current_list_count)++;

        rc = get_sort_attr(pol, &attr_set);
        if (rc != -1)
            *last_sort_time = rc;

        rc = entry2tgt_amount(p_param, &attr_set, &entry_amount);
        if (rc == -1) {
            DisplayLog(LVL_MAJOR, tag(pol),
                       "Failed to determine target amount for entry " DFID,
                       PFID(&entry_id));
            /* handle next entries */
            continue;
        }

        /* Insert candidate to workers queue */
        rc = Queue_Insert(&pol->queue,
                          entry2queue_item(&entry_id, &attr_set,
                                           entry_amount.targeted));
        if (rc)
            return PASS_ERROR;

        counters_add(&pushed_ctr, &entry_amount);

    /* Enqueue entries to workers queue as long as the specified limit is
     * not reached */
    } while (!check_queue_limit(pol, &pushed_ctr, feedback_before,
                                status_tab_before, &p_param->target_ctr));

    /* Make sure the processing queue is empty. */
    wait_queue_empty(pol, pushed_ctr.count, feedback_before,
                     status_tab_before, feedback_after, status_tab_after,
                     true);

    update_pass_stats(pol, status_tab_before, status_tab_after,
                      feedback_before, feedback_after);

    return st;
}

/* forward declaration */
static void process_entry(policy_info_t *pol, lmgr_t *lmgr,
                          queue_item_t *p_item, bool free_item);
/**
* Apply policy to a single file.
*/
static int single_file_run(policy_info_t *pol, lmgr_t *lmgr,
                           const policy_param_t *p_param,
                           action_summary_t *p_summary)
{
    queue_item_t item;
    int rc;
    unsigned int status_before[AS_ENUM_COUNT];
    unsigned int status_after[AS_ENUM_COUNT];
    unsigned long long feedback_before[AF_ENUM_COUNT];
    unsigned long long feedback_after[AF_ENUM_COUNT];

    memset(&item, 0, sizeof(item));

    RetrieveQueueStats(&pol->queue, NULL, NULL, NULL, NULL, NULL,
                       status_before, feedback_before);

    pol->progress.policy_start = pol->progress.last_report = time(NULL);

    /* resolve the fid of the target */
    rc = path2id(p_param->optarg_u.name, &item.entry_id, NULL);
    if (rc)
        return rc;

    /* needed attributes to apply the policy */
    item.entry_attr.attr_mask = db_attr_mask(pol, p_param);

    /* get fid from DB */
    rc = ListMgr_Get(lmgr, &item.entry_id, &item.entry_attr);
    if (rc) {
        if (rc == DB_NOT_EXISTS)
            DisplayLog(LVL_MAJOR, tag(pol),
                       "%s: this entry is not known in database",
                       p_param->optarg_u.name);
        /* expect a posix error code */
        rc = EINVAL;
        return rc;
    }

    /* apply the policy to the entry */
    process_entry(pol, lmgr, &item, false);

    ListMgr_FreeAttrs(&item.entry_attr);

    RetrieveQueueStats(&pol->queue, NULL, NULL, NULL, NULL, NULL,
                       status_after, feedback_after);
    update_pass_stats(pol, status_before, status_after,
                      feedback_before, feedback_after);

    if (p_summary)
        *p_summary = pol->progress;

    return 0;
}

/**
* This is called by triggers (or manual policy runs) to run a pass of a policy.
* @param[in,out] p_pol_info   policy information and resources
* @param[in]     p_param      parameters of this run (target, limit, ...)
* @param[out]    p_summary    summary of the policy run
* @param[in]     lmgr         connection to the database
*  \return 0 on success, a POSIX error code else, -1 for internal failure.
*  \retval ENOENT if no file list is available.
*/
int run_policy(policy_info_t *p_pol_info, const policy_param_t *p_param,
               action_summary_t *p_summary, lmgr_t *lmgr)
{
    struct policy_iter it = { 0 };
    int rc;
    pass_status_e st;
    lmgr_filter_t filter;
    filter_value_t fval;
    lmgr_sort_type_t sort_type;
    int last_sort_time = 0;
    /* XXX first_request_start = policy_start */
    attr_mask_t attr_mask;
    unsigned int nb_returned, total_returned;

    lmgr_iter_opt_t opt = LMGR_ITER_OPT_INIT;

    if (!p_pol_info)
        RBH_BUG("p_pol_info argument is NULL");
    if (!p_param)
        RBH_BUG("p_param argument is NULL");

    p_pol_info->time_modifier = p_param->time_mod;
    p_pol_info->trigger_action_params = p_param->action_params;

    memset(&p_pol_info->progress, 0, sizeof(p_pol_info->progress));
    if (p_summary)
        memset(p_summary, 0, sizeof(*p_summary));

    /* XXX previously here: interpreting target type and amount */

    /* special case: apply policy on a single file */
    if (p_param->target == TGT_FILE)
        return single_file_run(p_pol_info, lmgr, p_param, p_summary);

    /* Do nothing if no previous scan was done
     * (except if --force is specified). */
    rc = check_scan_done(p_pol_info, lmgr);
    if (rc)
        return rc;

    /* set attributes to be retrieved from DB */
    attr_mask = db_attr_mask(p_pol_info, p_param);

    /* sort by last access */
    // TODO manage random
    sort_type.attr_index = p_pol_info->config->lru_sort_attr;
    sort_type.order = p_pol_info->config->lru_sort_attr == LRU_ATTR_NONE ?
        SORT_NONE : SORT_ASC;

    rc = lmgr_simple_filter_init(&filter);
    if (rc)
        return rc;

    /* filter entries in the policy scope */
    DisplayLog(LVL_FULL, tag(p_pol_info),
               "Converting scope to DB filter...");
    if (convert_boolexpr_to_simple_filter
        (&p_pol_info->descr->scope, &filter, p_pol_info->descr->status_mgr,
         p_pol_info->time_modifier,
         p_pol_info->descr->manage_deleted ? FILTER_FLAG_ALLOW_NULL : 0)) {
        DisplayLog(LVL_DEBUG, tag(p_pol_info),
                   "Could not convert policy scope to simple filter.");
        DisplayLog(LVL_EVENT, tag(p_pol_info),
                   "Warning: scope definition is too complex and may affect policy run performance");
    }

    if (!p_pol_info->descr->manage_deleted) {
        /* do not retrieve 'invalid' entries */
        fval.value.val_bool = false;
        rc = lmgr_simple_filter_add(&filter, ATTR_INDEX_invalid, EQUAL,
                                    fval, FILTER_FLAG_ALLOW_NULL);
        if (rc)
            return rc;
    }

    /* set target filter and attr mask */
    rc = set_target_filter(p_pol_info, p_param, &filter, &attr_mask);
    if (rc)
        return rc;

    /* Flushing messages before performing the long DB sort query */
    FlushLogs();

    /* add optimisation filters based on policies */
    if (!ignore_policies(p_pol_info))
        set_optimization_filters(p_pol_info, &filter);

    /* Do not retrieve all entries at once, as the result may exceed
     * the client memory! */
    opt.list_count_max = p_pol_info->config->db_request_limit;
    nb_returned = 0;
    total_returned = 0;

    rc = iter_open(lmgr,
                   p_pol_info->descr->manage_deleted ? IT_RMD : IT_LIST,
                   &it, &filter, &sort_type, &opt);
    if (rc != DB_SUCCESS) {
        lmgr_simple_filter_free(&filter);
        DisplayLog(LVL_CRIT, tag(p_pol_info),
                   "Error retrieving list of candidates from database. "
                   "Policy run cancelled.");
        return rc;
    }

    p_pol_info->progress.policy_start = p_pol_info->progress.last_report
        = time(NULL);

    /* start alert batching in case the policy trigger alerts */
    Alert_StartBatching();

    /* loop on all policy passes */
    do {
        /* check if progress must be reported  */
        report_progress(p_pol_info, NULL, NULL, NULL, NULL);

        /* feed workers until the specified limit is reached or
         * end of list is reached */
        st = fill_workers_queue(p_pol_info, p_param, lmgr, &it, &opt,
                                &sort_type, &filter, attr_mask,
                                &last_sort_time, &nb_returned,
                                &total_returned);
        switch (st) {
        case PASS_EOL:
            rc = 0;
            break;
        case PASS_ABORTED:
            rc = ECANCELED;
            break;
        case PASS_LIMIT:
            rc = 0;
            break;
        case PASS_ERROR:
            rc = -1;
            break;
        }

    /* exit in all cases except pass_limit (double check the limit in this
     * case): check the real amount of performed actions
     * (progress.action_ctr) */
    } while ((st == PASS_LIMIT) &&
             !check_limit(p_pol_info, &p_pol_info->progress.action_ctr,
                          p_pol_info->progress.errors,
                          &p_param->target_ctr));

    lmgr_simple_filter_free(&filter);
    /* iterator may have been closed in fill_workers_queue() */
    iter_close(&it);

    /* flush pending alerts */
    Alert_EndBatching();

    if (p_summary)
        *p_summary = p_pol_info->progress;

    return rc;
}

#ifndef _HAVE_FID
/* If entries are accessed by FID, we can always get their status.
* This is not the case for POSIX, because they may have moved.
* In this case, the entry is tagged as 'invalid' in the DB
* until we find it again during a next scan.
*/
static inline int invalidate_entry(const policy_info_t *pol, lmgr_t *lmgr,
                                   entry_id_t *p_entry_id)
{
    attr_set_t new_attr_set = ATTR_SET_INIT;
    int rc;

    ATTR_MASK_INIT(&new_attr_set);
    ATTR_MASK_SET(&new_attr_set, invalid);
    ATTR(&new_attr_set, invalid) = true;

    /* update the entry */
    rc = ListMgr_Update(lmgr, p_entry_id, &new_attr_set);
    if (rc)
        DisplayLog(LVL_CRIT, tag(pol),
                   "Error %d tagging entry as invalid in database.", rc);
    return rc;
}
#endif

static inline int update_entry(lmgr_t *lmgr, const entry_id_t *p_entry_id,
                               const attr_set_t *p_attr_set)
{
    int rc;
    attr_set_t tmp_attrset = *p_attr_set;

    /* update classes according to new attributes */
    match_classes(p_entry_id, &tmp_attrset, NULL);

    /* /!\ do not update stripe info */
    /* @TODO actually, the best operation would be to update only
     * attributes that changed */
    ATTR_MASK_UNSET(&tmp_attrset, stripe_info);
    ATTR_MASK_UNSET(&tmp_attrset, stripe_items);

    /* also unset read only attrs */
    attr_mask_unset_readonly(&tmp_attrset.attr_mask);

    /* never update creation time */
    ATTR_MASK_UNSET(&tmp_attrset, creation_time);

    /* update DB and skip the entry */
    rc = ListMgr_Update(lmgr, p_entry_id, &tmp_attrset);
    if (rc)
        DisplayLog(LVL_CRIT, TAG, "Error %d updating entry in database.",
                   rc);

    return rc;
}

#ifdef _HAVE_FID
/**
* Check that entry still exists
* @param fill entry MD if entry is valid
*/
static int check_entry(const policy_info_t *policy, lmgr_t *lmgr,
                       queue_item_t *p_item, attr_set_t *new_attr_set)
{
    char fid_path[RBH_PATH_MAX];
    struct stat entry_md;
    int rc;
    sm_instance_t *smi = policy->descr->status_mgr;

    DisplayLog(LVL_FULL, tag(policy), "Considering entry " DFID,
               PFID(&p_item->entry_id));

    /* 1) Build fid path */
    BuildFidPath(&p_item->entry_id, fid_path);

    /* 2) Perform lstat on entry (size is needed!):
     * important to respect LRU sort order (TODO: not if sort order is not
     * in POSIX attrs)
     */
    if (lstat(fid_path, &entry_md) != 0) {
        rc = errno;

        /* If lstat returns an error, skip the entry */
        DisplayLog(LVL_DEBUG, tag(policy),
                   "lstat() failed on %s. Skipping it.", fid_path);

        /* This entry has been processed and has probably removed */
        if (rc == ENOENT)
        /** @TODO remove entry from DB if errno = ENOENT ? */
            return AS_MOVED;
        else
            return AS_STAT_FAILURE;
    }

    /* creation time from DB has the priority on filesystem stat */
    if (ATTR_MASK_TEST(&p_item->entry_attr, creation_time)) {
        ATTR_MASK_SET(new_attr_set, creation_time);
        ATTR(new_attr_set, creation_time) =
            ATTR(&p_item->entry_attr, creation_time);
    }

    /* convert posix attributes to attr structure */
    stat2rbh_attrs(&entry_md, new_attr_set, true);

    /* set update time of the structure */
    ATTR_MASK_SET(new_attr_set, md_update);
    ATTR(new_attr_set, md_update) = time(NULL);

    /* get fullpath or name, if they are needed for applying policy */
    if (policy->descr->rules.run_attr_mask.
        std & (ATTR_MASK_fullpath | ATTR_MASK_name)) {
        path_check_update(&p_item->entry_id, fid_path, new_attr_set,
                          policy->descr->rules.run_attr_mask);
    }

    /* retrieve up-to-date status from status manager if the scope reliesk
     * on it */
    if (smi != NULL && smi->sm->get_status_func != NULL
        && (policy->descr->scope_mask.status & SMI_MASK(smi->smi_index))) {
        rc = smi->sm->get_status_func(smi, &p_item->entry_id,
                                      new_attr_set, new_attr_set);
        if (rc != 0) {
            DisplayLog(LVL_MAJOR, tag(policy),
                       "Failed to get status for " DFID
                       " (%s status manager): error %d",
                       PFID(&p_item->entry_id), smi->sm->name, rc);
            return AS_ERROR;
        }
    }

    /* entry is valid */
    return AS_OK;
}
#else /* no FID */
/**
* Check that entry exists with the good path and its id is consistent.
* @param fill entry MD if entry is valid
*/
static int check_entry(const policy_info_t *policy, lmgr_t *lmgr,
                       queue_item_t *p_item, attr_set_t *new_attr_set)
{
    struct stat entry_md;
    char *stat_path;
    sm_instance_t *smi = policy->descr->status_mgr;

    /* 1) Check if fullpath is set (if no fid support) */
    if (!ATTR_MASK_TEST(&p_item->entry_attr, fullpath)) {
        DisplayLog(LVL_DEBUG, tag(policy),
                   "Warning: entry fullpath is not set. Tagging it invalid.");
        invalidate_entry(policy, lmgr, &p_item->entry_id);

        /* not enough metadata */
        return AS_MISSING_MD;
    }
    stat_path = ATTR(&p_item->entry_attr, fullpath);

    if (ATTR_MASK_TEST(&p_item->entry_attr, fullpath))
        DisplayLog(LVL_FULL, tag(policy), "Considering entry %s",
                   ATTR(&p_item->entry_attr, fullpath));

    /* 2) Perform lstat on entry */
    if (lstat(stat_path, &entry_md) != 0) {
        /* If lstat returns an error, invalidate the entry */
        DisplayLog(LVL_DEBUG, tag(policy),
                   "lstat() failed on %s. Tagging it invalid.", stat_path);
        invalidate_entry(policy, lmgr, &p_item->entry_id);

        /* This entry has been processed and has probably moved */
        return AS_MOVED;
    }

    /* 3) check entry id and fskey */
    if ((entry_md.st_ino != p_item->entry_id.inode)
        || (get_fskey() != p_item->entry_id.fs_key)) {
        /* If it has changed, invalidate the entry (fullpath does not match
         * entry_id, it will be updated or removed at next FS scan). */
        DisplayLog(LVL_DEBUG, tag(policy),
                   "Inode of %s changed: old=<%llu,%llu>, "
                   "new=<%llu,%llu>. Tagging it invalid.",
                   ATTR(&p_item->entry_attr, fullpath),
                   (unsigned long long)p_item->entry_id.inode,
                   (unsigned long long)p_item->entry_id.fs_key,
                   (unsigned long long)entry_md.st_ino,
                   (unsigned long long)get_fskey());

        invalidate_entry(policy, lmgr, &p_item->entry_id);

        /* This entry has been processed and has probably moved */
        return AS_MOVED;
    }

    /* convert posix attributes to attr structure */
    stat2rbh_attrs(&entry_md, new_attr_set, true);

    /* set update time of the structure */
    ATTR_MASK_SET(new_attr_set, md_update);
    ATTR(new_attr_set, md_update) = time(NULL);

    /* retrieve up-to-date status from status manager if the scope relies
     * on it */
    if (smi != NULL && smi->sm->get_status_func != NULL
        && (policy->descr->scope_mask.status & SMI_MASK(smi->smi_index))) {
        int rc;

        rc = smi->sm->get_status_func(smi, &p_item->entry_id,
                                      new_attr_set, new_attr_set);
        if (rc != 0) {
            DisplayLog(LVL_MAJOR, tag(policy),
                       "Failed to get status for " DFID
                       " (%s status manager): error %d",
                       PFID(&p_item->entry_id), smi->sm->name, rc);
            return AS_ERROR;
        }
    }

    /* entry is valid */
    return AS_OK;
}
#endif

/** check that time ordering did not change and that time attributes
* are consistent. */
static action_status_t check_entry_times(policy_info_t *pol, lmgr_t *lmgr,
                                         const entry_id_t *p_id,
                                         const attr_set_t *p_attrs_old,
                                         const attr_set_t *p_attrs_new)
{
    if (pol->descr->manage_deleted) {
        /* deleted entry: no new attrs */

        /* if lru sort order is rmtime and rmtime is not set: missing MD */
        if ((pol->config->lru_sort_attr == ATTR_INDEX_rm_time)
            && !ATTR_MASK_TEST(p_attrs_old, rm_time)) {
            /* cannot determine if sort criteria has changed */
            DisplayLog(LVL_VERB, tag(pol),
                       "rm_time attribute is not set for deleted entry: skipping it");
            return AS_MISSING_MD;
        }
        return AS_OK;
    }

    /* If the policy application is ordered, make sure the value used for
     * ordering did not change. If so, update the entry so it will be
     * correctly ordered for the next pass. */
    if (pol->config->lru_sort_attr != LRU_ATTR_NONE) {
        int val1, val2;

        val1 = get_sort_attr(pol, p_attrs_old);
        val2 = get_sort_attr(pol, p_attrs_new);

        if ((val1 == -1) || (val2 == -1)) {
            /* cannot determine if sort criteria has changed */
            DisplayLog(LVL_VERB, tag(pol),
                       "Cannot determine if sort criteria value"
                       " changed (missing attribute '%s'): skipping entry.",
                       sort_attr_name(pol));
            if (!pol->descr->manage_deleted)
                update_entry(lmgr, p_id, p_attrs_new);
            return AS_MISSING_MD;
        } else if (val1 != val2) {
            DisplayLog(LVL_DEBUG, tag(pol),
                       "%s has been accessed/modified since last md update. Skipping entry.",
                       ATTR(p_attrs_old, fullpath));
            if (!pol->descr->manage_deleted)
                update_entry(lmgr, p_id, p_attrs_new);
            return AS_ACCESSED;
        }

        /* LRU on access/modification: size change detected? */
        if ((pol->config->lru_sort_attr == ATTR_INDEX_last_access
             || pol->config->lru_sort_attr == ATTR_INDEX_last_mod)
            && ATTR_MASK_TEST(p_attrs_old, size)
            && ATTR_MASK_TEST(p_attrs_new, size)
            && (ATTR(p_attrs_old, size) != ATTR(p_attrs_new, size))) {
            DisplayLog(LVL_DEBUG, tag(pol),
                       "%s has been modified since last md update (size changed). Skipping entry.",
                       ATTR(p_attrs_old, fullpath));
            if (!pol->descr->manage_deleted)
                update_entry(lmgr, p_id, p_attrs_new);
            return AS_ACCESSED;
        }
    }
    return AS_OK;
}

/** Display action success to log and report. */
static void log_action_success(const policy_info_t *pol,
                               const attr_set_t *attrs,
                               const rule_item_t *rule,
                               const fileset_item_t *fileset,
                               int sort_time)
{
    GString *str = NULL;
    GString *str_stripe = NULL;
    char strsize[256];

    /* display needed? */
    if (log_config.debug_level < LVL_DEBUG && !pol->config->report_actions)
        return;

    str = g_string_new(NULL);
    g_string_printf(str, "%s success for '%s', matching rule '%s'",
                    tag(pol), ATTR(attrs, fullpath), rule->rule_id);

    if (fileset)
        g_string_append_printf(str, " (fileset=%s)", fileset->fileset_id);

    if (pol->config->lru_sort_attr != LRU_ATTR_NONE) {
        if (sort_time > 0) {
            char strtime[256];

            FormatDurationFloat(strtime, sizeof(strtime),
                                time(NULL) - sort_time);
            g_string_append_printf(str, ", %s %s ago", sort_attr_name(pol),
                                   strtime);
        } else
            g_string_append_printf(str, ", %s <none>", sort_attr_name(pol));
    }

    FormatFileSize(strsize, sizeof(strsize), ATTR(attrs, size));

#ifdef _LUSTRE
    /* Only needed if trace level is DEBUG or if report_action
     * is enabled */
    if ((log_config.debug_level >= LVL_DEBUG || pol->config->report_actions)
        && ATTR_MASK_TEST(attrs, stripe_items)) {
        str_stripe = g_string_new("");
        append_stripe_list(str_stripe, &ATTR(attrs, stripe_items), false);
    }
#endif

    DisplayLog(LVL_DEBUG, tag(pol), "%s, size=%s%s%s", str->str, strsize,
               str_stripe ? " stored on " : "",
               str_stripe ? str_stripe->str : "");

    if (pol->config->report_actions) {
        g_string_append_printf(str, " | size=%" PRI_SZ, ATTR(attrs, size));

        if (pol->config->lru_sort_attr != LRU_ATTR_NONE)
            g_string_append_printf(str, ", %s=%u", sort_attr_name(pol),
                                   sort_time);

        if (str_stripe)
            g_string_append_printf(str, ", stripes=%s", str_stripe->str);

        DisplayReport("%s", str->str);
    }
    if (str_stripe != NULL)
        g_string_free(str_stripe, TRUE);

    g_string_free(str, TRUE);
}

/* acknowledging helper */
#define policy_ack(_q, _status, _pattrs, _tgt)  do {                 \
            unsigned long long feedback[AF_ENUM_COUNT];              \
            memset(feedback, 0, sizeof(feedback));   \
            if (_status == AS_OK) {             \
                feedback[AF_NBR_OK] = 1;        \
                feedback[AF_VOL_OK] = ATTR_MASK_TEST(_pattrs, size) ? \
                                      ATTR(_pattrs, size) : 0;        \
                feedback[AF_TARGETED_OK] = _tgt;\
                feedback[AF_BLOCKS_OK] = ATTR_MASK_TEST(_pattrs, blocks) ? \
                                         ATTR(_pattrs, blocks) : 0; \
            } else {                            \
                feedback[AF_NBR_NOK] = 1;        \
                feedback[AF_VOL_NOK] = ATTR_MASK_TEST(_pattrs, size) ? \
                                       ATTR(_pattrs, size) : 0;        \
                feedback[AF_TARGETED_NOK] = _tgt;\
                feedback[AF_BLOCKS_NOK] = ATTR_MASK_TEST(_pattrs, blocks) ? \
                                          ATTR(_pattrs, blocks) : 0; \
            }                                   \
            Queue_Acknowledge(_q, _status, feedback, AF_ENUM_COUNT); \
       } while (0)

/**
* Manage an entry by path or by fid, depending on FS
*/
static void process_entry(policy_info_t *pol, lmgr_t *lmgr,
                          queue_item_t *p_item, bool free_item)
{
    attr_set_t new_attr_set = ATTR_SET_INIT;
    int rc;

    policy_match_t match;
    rule_item_t *rule;
    fileset_item_t *p_fileset;
    attr_set_t attr_sav;
    int lastrm;
    post_action_e after_action = PA_NONE;
    action_params_t params = { 0 };

    if (aborted(pol)) {
        /* migration aborted by a signal, doesn't submit new migrations */
        DisplayLog(LVL_FULL, tag(pol),
                   "Policy run aborted: skipping pending requests");
        policy_ack(&pol->queue, AS_ABORT, &p_item->entry_attr,
                   p_item->targeted);
        rc = AS_ABORT;
        goto end;
    }

    DisplayLog(LVL_FULL, tag(pol),
               "Checking if entry %s matches policy rules",
               ATTR(&p_item->entry_attr, fullpath));

    if (!pol->descr->manage_deleted) {
        rc = check_entry(pol, lmgr, p_item, &new_attr_set);
        if (rc != AS_OK) {
            policy_ack(&pol->queue, rc, &p_item->entry_attr,
                       p_item->targeted);
            goto end;
        }
    }
    /* In any case, complete with missing attrs from database */
    ListMgr_MergeAttrSets(&new_attr_set, &p_item->entry_attr, false);

#ifdef ATTR_INDEX_invalid
    /* From here, assume that entry is valid */
    ATTR_MASK_SET(&new_attr_set, invalid);
    ATTR(&new_attr_set, invalid) = false;
#endif

    /* check the entry still matches the policy scope */
    switch (match_scope(pol->descr, &p_item->entry_id, &new_attr_set,
                        !pol->descr->manage_deleted)) {
    case POLICY_MATCH:
        /* OK */
        break;
    case POLICY_NO_MATCH:
        DisplayLog(LVL_DEBUG, tag(pol),
                   "Entry %s doesn't match scope of policy '%s'.",
                   ATTR(&new_attr_set, fullpath), tag(pol));
        if (!pol->descr->manage_deleted)
            update_entry(lmgr, &p_item->entry_id, &new_attr_set);
        policy_ack(&pol->queue, AS_OUT_OF_SCOPE, &p_item->entry_attr,
                   p_item->targeted);
        goto end;
        break;
    default:
        if (!pol->descr->manage_deleted) {
            DisplayLog(LVL_MAJOR, tag(pol),
                       "Warning: cannot determine if entry %s matches the "
                       "scope of policy '%s': skipping it.",
                       ATTR(&new_attr_set, fullpath), tag(pol));

            update_entry(lmgr, &p_item->entry_id, &new_attr_set);
            policy_ack(&pol->queue, AS_MISSING_MD, &p_item->entry_attr,
                       p_item->targeted);
            goto end;
        } else {
            /* For deleted entries, we expect missing attributes.
             * so, continue anyway. */
            DisplayLog(LVL_DEBUG, tag(pol),
                       "Cannot determine if entry %s matches the "
                       "scope of policy '%s'. Continuing anyway.",
                       ATTR(&new_attr_set, fullpath), tag(pol));
        }
    }

    /* if ignore-policies flag is specified:
     * - don't check rules
     * - don't care about recent atime etc...
     */
    if (!ignore_policies(pol)) {
        /* 4) check whitelist rules */
        match =
            is_whitelisted(pol->descr, &p_item->entry_id, &new_attr_set,
                           &p_fileset);

        if (match == POLICY_MATCH) {
            DisplayLog(LVL_DEBUG, tag(pol),
                       "Entry %s matches ignored target %s.",
                       ATTR(&p_item->entry_attr, fullpath),
                       p_fileset ? p_fileset->fileset_id : "(ignore rule)");

            if (!pol->descr->manage_deleted)
                update_entry(lmgr, &p_item->entry_id, &new_attr_set);
            policy_ack(&pol->queue, AS_WHITELISTED, &p_item->entry_attr,
                       p_item->targeted);
            goto end;
        } else if (match != POLICY_NO_MATCH) {
            /* Cannot determine if entry is whitelisted: skip it
             * (do nothing in database) */
            DisplayLog(LVL_MAJOR, tag(pol),
                       "Warning: cannot determine if entry %s is whitelisted: skipping it.",
                       ATTR(&p_item->entry_attr, fullpath));

            if (!pol->descr->manage_deleted)
                update_entry(lmgr, &p_item->entry_id, &new_attr_set);
            policy_ack(&pol->queue, AS_MISSING_MD, &p_item->entry_attr,
                       p_item->targeted);
            goto end;
        }

        /* check that time ordering did not change and that time attributes
         * are consistent. */
        rc = check_entry_times(pol, lmgr, &p_item->entry_id,
                               &p_item->entry_attr, &new_attr_set);
        if (rc != AS_OK) {
            /* check_entry_times already updates the entry */
            policy_ack(&pol->queue, rc, &p_item->entry_attr,
                       p_item->targeted);
            goto end;
        }
    }

    /* end if 'don't ignore policies' */
    /* get policy rule for the entry */
    rule = policy_case(pol->descr, &p_item->entry_id, &new_attr_set,
                       &p_fileset);
    if (!rule) {
        DisplayLog(LVL_DEBUG, tag(pol), "Entry %s matches no policy rule",
                   ATTR(&p_item->entry_attr, fullpath));

        if (!pol->descr->manage_deleted)
            update_entry(lmgr, &p_item->entry_id, &new_attr_set);

        policy_ack(&pol->queue, AS_NO_POLICY, &p_item->entry_attr,
                   p_item->targeted);
        goto end;
    }

    /* don't care about policy condition if 'ignore-policies' flag is
     * specified */
    if (!ignore_policies(pol)) {
        /* check if the entry matches the policy condition */
        switch (entry_matches
                (&p_item->entry_id, &new_attr_set, &rule->condition,
                 pol->time_modifier, pol->descr->status_mgr)) {
        case POLICY_NO_MATCH:
            /* entry is not eligible now */
            DisplayLog(LVL_DEBUG, tag(pol),
                       "Entry %s doesn't match condition for policy rule '%s'",
                       ATTR(&p_item->entry_attr, fullpath), rule->rule_id);

            if (!pol->descr->manage_deleted)
                update_entry(lmgr, &p_item->entry_id, &new_attr_set);

            policy_ack(&pol->queue, AS_WHITELISTED, &p_item->entry_attr,
                       p_item->targeted);

            goto end;
            break;
        case POLICY_MATCH:
            /* OK, can be purged */
            DisplayLog(LVL_DEBUG, tag(pol),
                       "Entry %s matches the condition for policy rule '%s'.",
                       ATTR(&p_item->entry_attr, fullpath), rule->rule_id);
            break;
        default:
            /* Cannot determine if entry matches the policy condition */
            DisplayLog(LVL_MAJOR, tag(pol),
                       "Warning: cannot determine if entry %s matches the "
                       "condition for policy rule '%s': skipping it.",
                       ATTR(&p_item->entry_attr, fullpath), rule->rule_id);

            if (!pol->descr->manage_deleted)
                update_entry(lmgr, &p_item->entry_id, &new_attr_set);
            policy_ack(&pol->queue, AS_MISSING_MD, &p_item->entry_attr,
                       p_item->targeted);
            goto end;
        }
    }

    /* it is the first matching entry? */
    rc = get_sort_attr(pol, &p_item->entry_attr);
    if (rc != -1 && (!pol->first_eligible || (rc < pol->first_eligible)))
        pol->first_eligible = rc;

    int time_save = rc;

    /* build action parameters */
    rc = build_action_params(&params, &p_item->entry_id, &new_attr_set, pol,
                             rule, p_fileset);
    if (rc) {
        if (!pol->descr->manage_deleted)
            update_entry(lmgr, &p_item->entry_id, &new_attr_set);
        policy_ack(&pol->queue, AS_ERROR, &p_item->entry_attr,
                   p_item->targeted);
        goto end;
    }

    /* save attributes before doing the action */
    /* @FIXME this only save scalar value, not values in allocated structures */
    attr_sav = new_attr_set;

    /* apply action to the entry! */
    /* TODO RBHv3: action must indicate what to do with the entry
     * => db update, rm from filesystem etc... */
    rc = policy_action(pol, rule, p_fileset, &p_item->entry_id,
                       &new_attr_set, &params, &after_action);
    rbh_params_free(&params);

    if (rc != 0) {
        const char *err_str;

        if (rc < 0)
            err_str = strerror(-rc);
        else
            err_str = "command execution failed";

        DisplayLog(LVL_DEBUG, tag(pol),
                   "Error applying action on entry %s: %s",
                   ATTR(&new_attr_set, fullpath), err_str);

        /* no update for deleted entries */
        if (!pol->descr->manage_deleted)
            update_entry(lmgr, &p_item->entry_id, &new_attr_set);

        policy_ack(&pol->queue, AS_ERROR, &p_item->entry_attr,
                   p_item->targeted);
    } else {
        log_action_success(pol, &attr_sav, rule, p_fileset, time_save);

        if (pol->descr->manage_deleted &&
            (after_action == PA_RM_ONE || after_action == PA_RM_ALL)) {
            rc = ListMgr_SoftRemove_Discard(lmgr, &p_item->entry_id);
            if (rc)
                DisplayLog(LVL_CRIT, tag(pol),
                           "Error %d removing entry from database.", rc);
        } else if (after_action == PA_UPDATE) {
            if (!pol->descr->manage_deleted)
                update_entry(lmgr, &p_item->entry_id, &new_attr_set);
        } else if (after_action == PA_RM_ONE) {
            lastrm = ATTR_MASK_TEST(&attr_sav, nlink) ?
                (ATTR(&attr_sav, nlink) <= 1) : 0;

            /* must be based on the DB content = old attrs */
            rc = ListMgr_Remove(lmgr, &p_item->entry_id,
                                &p_item->entry_attr, lastrm);
            if (rc)
                DisplayLog(LVL_CRIT, tag(pol),
                           "Error %d removing entry from database.", rc);
        } else if (after_action == PA_RM_ALL) {
            /* must be based on the DB content = old attrs */
            rc = ListMgr_Remove(lmgr, &p_item->entry_id,
                                &p_item->entry_attr, 1);
            if (rc)
                DisplayLog(LVL_CRIT, tag(pol),
                           "Error %d removing entry from database.", rc);
        }

        /* TODO update target info */
        policy_ack(&pol->queue, AS_OK, &new_attr_set, p_item->targeted);
    }

end:
    ListMgr_FreeAttrs(&new_attr_set);

    if (free_item)
        free_queue_item(p_item);
    return;
}

/**
*  Main routine of policy thread
*/
static void *thr_policy_run(void *arg)
{
    int rc;
    lmgr_t lmgr;
    void *p_queue_entry;
    policy_info_t *pol = (policy_info_t *) arg;

    rc = ListMgr_InitAccess(&lmgr);
    if (rc) {
        DisplayLog(LVL_CRIT, tag(pol),
                   "Could not connect to database (error %d). Exiting.",
                   rc);
        exit(rc);
    }

    while (Queue_Get(&pol->queue, &p_queue_entry) == 0)
        process_entry(pol, &lmgr, (queue_item_t *) p_queue_entry, true);

    /* Error occurred in purge queue management... */
    DisplayLog(LVL_CRIT, tag(pol),
               "An error occurred in policy run queue management. Exiting.");
    exit(-1);
    return NULL;    /* for avoiding compiler warnings */
}

int start_worker_threads(policy_info_t *pol)
{
    unsigned int i;

    pol->threads = (pthread_t *) MemCalloc(pol->config->nb_threads,
                                           sizeof(pthread_t));
    if (!pol->threads) {
        DisplayLog(LVL_CRIT, tag(pol), "Memory error in %s", __func__);
        return ENOMEM;
    }

    for (i = 0; i < pol->config->nb_threads; i++) {
        if (pthread_create(&pol->threads[i], NULL, thr_policy_run, pol) !=
            0) {
            int rc = errno;
            DisplayLog(LVL_CRIT, tag(pol),
                       "Error %d creating policy threads in %s: %s", rc,
                       __func__, strerror(rc));
            return rc;
        }
    }
    return 0;
}

/**
* Update the status of outstanding actions
* \param lmgr          [IN] connexion to database
* \param p_nb_reset    [OUT] number of purge reset
* \param p_nb_total    [OUT] total number of purge checked
*
* Note:   the timeout is in pol->config
*/
int check_current_actions(policy_info_t *pol, lmgr_t *lmgr,
                          unsigned int *p_nb_reset,
                          unsigned int *p_nb_total)
{
    int rc;
    struct lmgr_iterator_t *it = NULL;

    lmgr_filter_t filter;
    filter_value_t fval;

    queue_item_t q_item;

    unsigned int nb_returned = 0;
    unsigned int nb_aborted = 0;
    attr_mask_t attr_mask_sav = { 0 };
    attr_mask_t tmp;

    /* do nothing if this policy applies to deleted entries */
    if (pol->descr->manage_deleted)
        return 0;

    /* attributes to be retrieved */
    ATTR_MASK_INIT(&q_item.entry_attr);
    ATTR_MASK_SET(&q_item.entry_attr, fullpath);
    ATTR_MASK_SET(&q_item.entry_attr, path_update);

    /* Add attrs to match policy scope */
    q_item.entry_attr.attr_mask = attr_mask_or(&q_item.entry_attr.attr_mask,
                                               &pol->descr->scope_mask);

    /* needed attributes from DB */
    tmp = attrs_for_status_mask(q_item.entry_attr.attr_mask.status, false);
    q_item.entry_attr.attr_mask =
        attr_mask_or(&q_item.entry_attr.attr_mask, &tmp);

    attr_mask_sav = q_item.entry_attr.attr_mask;

    rc = lmgr_simple_filter_init(&filter);
    if (rc)
        return rc;

    /* if timeout is > 0, only select entries whose last update
     * is old enough (last_update <= now - timeout) or NULL*/
    if (pol->config->action_timeout > 0) {
        fval.value.val_int = time(NULL) - pol->config->action_timeout;
        rc = lmgr_simple_filter_add(&filter, ATTR_INDEX_md_update, LESSTHAN,
                                    fval, FILTER_FLAG_ALLOW_NULL);
        if (rc)
            return rc;
    }

    /* filter by status of current actions */
    fval.value.val_str = pol->descr->status_current;
    rc = lmgr_simple_filter_add(&filter,
                                smi_status_index(pol->descr->status_mgr),
                                EQUAL, fval, 0);
    if (rc)
        return rc;

#ifdef ATTR_INDEX_invalid
    /* don't retrieve invalid entries (allow entries with invalid == NULL) */
    fval.value.val_int = true;
    rc = lmgr_simple_filter_add(&filter, ATTR_INDEX_invalid, NOTEQUAL, fval,
                                FILTER_FLAG_ALLOW_NULL);
    if (rc)
        return rc;
#endif

    it = ListMgr_Iterator(lmgr, &filter, NULL, NULL);

    if (it == NULL) {
        lmgr_simple_filter_free(&filter);
        DisplayLog(LVL_CRIT, tag(pol),
                   "Error retrieving the list of current actions. Recovery cancelled.");
        return -1;
    }

    memset(&q_item, 0, sizeof(queue_item_t));
    q_item.entry_attr.attr_mask = attr_mask_sav;

    while ((rc = ListMgr_GetNext(it, &q_item.entry_id, &q_item.entry_attr))
           == DB_SUCCESS) {
        nb_returned++;

        if (ATTR_MASK_TEST(&q_item.entry_attr, fullpath))
            DisplayLog(LVL_VERB, tag(pol), "Updating status of '%s'...",
                       ATTR(&q_item.entry_attr, fullpath));

        /* check entry */
        if (check_entry(pol, lmgr, &q_item, &q_item.entry_attr) == AS_OK) {
            int smi_index = pol->descr->status_mgr->smi_index;

            if (ATTR_MASK_STATUS_TEST(&q_item.entry_attr, smi_index)) {
                if (strcmp(STATUS_ATTR(&q_item.entry_attr, smi_index),
                           pol->descr->status_current)) {
                    DisplayLog(LVL_EVENT, tag(pol),
                               "status of '%s' changed: now '%s'",
                               ATTR(&q_item.entry_attr, fullpath),
                               STATUS_ATTR(&q_item.entry_attr, smi_index));
                    nb_aborted++;
                } else
                    DisplayLog(LVL_EVENT, tag(pol),
                               "status of '%s' is still '%s'",
                               ATTR(&q_item.entry_attr, fullpath),
                               STATUS_ATTR(&q_item.entry_attr, smi_index));
            }

            /* update entry status */
            update_entry(lmgr, &q_item.entry_id, &q_item.entry_attr);
        }

        /* reset attr_mask, if it was altered by last ListMgr_GetNext() call */
        memset(&q_item, 0, sizeof(queue_item_t));
        q_item.entry_attr.attr_mask = attr_mask_sav;
    }

    lmgr_simple_filter_free(&filter);
    ListMgr_CloseIterator(it);

    if (p_nb_total)
        *p_nb_total = nb_returned;

    if (p_nb_reset)
        *p_nb_reset = nb_aborted;

    /* check rc */
    if (rc != DB_END_OF_LIST) {
        DisplayLog(LVL_CRIT, tag(pol),
                   "Error %d getting next entry of iterator", rc);
        return -1;
    }

    return 0;
}
