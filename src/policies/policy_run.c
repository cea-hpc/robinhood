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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "policy_run.h"
#include "run_policies.h"
#include "list_mgr.h"
#include "RobinhoodLogs.h"
#include "RobinhoodMisc.h"
#include "Memory.h"
#include "xplatform_print.h"
#include "RobinhoodConfig.h"

#ifdef _HSM_LITE
#include "backend_mgr.h"
#include "backend_ext.h"
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>

#define CHECK_QUEUE_INTERVAL    1

#define ignore_policies(_p) ((_p)->flags & FLAG_IGNORE_POL)
#define dry_run(_p)         ((_p)->flags & FLAG_DRY_RUN)
#define aborted(_p)         ((_p)->aborted)
#define no_limit(_p)        ((_p)->flags & FLAG_NO_LIMIT)
#define force_run(_p)       ((_p)->flags & FLAG_FORCE_RUN)
#define tag(_p)             ((_p)->descr->name)

#define TAG "PolicyRun"

typedef struct queue_item__
{
    entry_id_t     entry_id;
    attr_set_t     entry_attr;
    unsigned long  targeted;
} queue_item_t;

/**
 *  alloc a new worker item so it can be pushed to the worker queue.
 */
static queue_item_t *entry2queue_item(entry_id_t *p_entry_id,
                                      attr_set_t *p_attr_set,
                                      unsigned long targeted)
{
    queue_item_t  *new_entry;

    new_entry = (queue_item_t *)MemAlloc(sizeof(queue_item_t));
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

static int policy_action(policy_info_t *policy,
                         const entry_id_t *id, attr_set_t *p_attr_set,
                         const char *hints, post_action_e *after)
{
    int rc = 0;
    DisplayLog(LVL_EVENT, tag(policy),
               "Applying policy action to: " DFID_NOBRACE ", hints='%s'",
               PFID(id), hints?hints:"<none>");
    if (dry_run(policy))
        return 0;

    switch(policy->config->action_type)
    {
        case ACTION_FUNCTION:
            rc = policy->config->action_u.function(id, p_attr_set, hints, after);
            break;
        case ACTION_COMMAND:
        {
            /* execute custom action */
            char strfid[128];
            sprintf(strfid, DFID, PFID(id));

            /* @TODO to be documented */
            const char *vars[] = {
                "path", ATTR_MASK_TEST(p_attr_set, fullpath)?ATTR(p_attr_set, fullpath):"",
                "fsname", get_fsname(),
                "fid", strfid,
                "hints", hints,
                "policy", tag(policy),
                NULL, NULL
            };

            char *cmd = replace_cmd_parameters(policy->config->action_u.command, vars);
            if (cmd)
            {
                int rc = 0;
                /* call custom purge command instead of unlink() */
                DisplayLog(LVL_DEBUG, tag(policy), "%scmd(%s)",
                           dry_run(policy)?"(dry-run)":"", cmd);
                if (!dry_run(policy))
                    rc =  execute_shell_command(TRUE, cmd, 0);
                free(cmd);
                /* @TODO handle other hardlinks to the same entry */
            }
            else
                rc = errno;
            break;
        }
        case ACTION_NONE:
            rc = 0;
            break;
    }
//    if (rc == 0)
//        // FIXME set post_action_e in case of external command
//        *after = PA_RM_ONE;
//    else
//        *after = PA_UPDATE;

    return rc;
}

static inline int get_sort_attr(policy_info_t *p, const attr_set_t *p_attrs)
{
    switch(p->config->lru_sort_attr)
    {
        case ATTR_INDEX_creation_time:
            return (ATTR_MASK_TEST(p_attrs, creation_time)?
                    ATTR(p_attrs, creation_time) : -1);
        case ATTR_INDEX_last_mod:
            return (ATTR_MASK_TEST(p_attrs, last_mod)?
                    ATTR(p_attrs, last_mod) : -1);
        case ATTR_INDEX_last_access:
            return (ATTR_MASK_TEST(p_attrs, last_access)?
                    ATTR(p_attrs, last_access) : -1);
        case ATTR_INDEX_last_archive:
            return (ATTR_MASK_TEST(p_attrs, last_archive)?
                    ATTR(p_attrs, last_archive) : -1);
        case ATTR_INDEX_last_restore:
            return (ATTR_MASK_TEST(p_attrs, last_restore)?
                    ATTR(p_attrs, last_restore) : -1);
        default:
            return -1;
    }
}

/* set dummy time attributes, to check 'end of list' criteria */
static inline void set_max_time_attrs(policy_info_t *p, attr_set_t *p_attrs,
                                      time_t value)
{
    switch(p->config->lru_sort_attr)
    {
        case ATTR_INDEX_last_mod:
            ATTR_MASK_SET(p_attrs, last_mod);
            ATTR(p_attrs, last_mod) = value;
            break;

        case ATTR_INDEX_last_access:
            ATTR_MASK_SET(p_attrs, last_access);
            ATTR(p_attrs, last_access) = value;
            /* in robinhood, lastmod <= last_access as last_access=MAX(atime,mtime) */
            ATTR_MASK_SET(p_attrs, last_mod);
            ATTR(p_attrs, last_mod) = value;
            break;

        case ATTR_INDEX_last_archive:
            ATTR_MASK_SET(p_attrs, last_archive);
            ATTR(p_attrs, last_archive) = value;
            break;

        case ATTR_INDEX_last_restore:
            ATTR_MASK_SET(p_attrs, last_restore);
            ATTR(p_attrs, last_restore) = value;
            break;

        default:
            /* unsupported */
            RBH_BUG("Unsupported LRU sort attribute");
    }

    /* always set create time criteria (always <= any time attribute) */
    ATTR_MASK_SET(p_attrs, creation_time);
    ATTR(p_attrs, creation_time) = value;

    /* FIXME: guess other times, depending on status scope? */
#if 0
    /* If entry is dirty (migration):
     *      creation_time <= last_archive <= last mod (entry is dirty) <= last_access
     * If entry is synchro (purge):
     *      creation_time <= last_mod <= last_access <= last_archive (entry is synchro)
     *      creation_time <= last_restore <= last_access (entry still not purged)
     */

    /* what about other times??? */
    if entry is dirty: last_archive < last_mod < last_access

    /* last_mod <= last_access: set last_mod if sort_time is last_access */
    /* unless if entry is released, */
    last_archive
    last_access
    last_mod
    last_restore
#endif
}

#define sort_attr_name(_p)  (field_infos[(_p)->config->lru_sort_attr].field_name)

static int heuristic_end_of_list(policy_info_t *policy, time_t last_time)
{
    entry_id_t     void_id;
    attr_set_t     void_attr = {0};

    /* list all files if policies are ignored */
    if (ignore_policies(policy))
        return FALSE;

    /* don't rely on fake times (0, 1 or in the future...) */
    if (last_time <= 1 || last_time > time(NULL))
        return FALSE;

    /* Optimization: we build a void entry with time attr = current sort attr
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

    if (policy_match_all(policy->descr,  &void_id, &void_attr,
            policy->time_modifier, NULL) == POLICY_NO_MATCH)
    {
        DisplayLog(LVL_DEBUG, tag(policy),
                   "Optimization: entries with %s later than %lu cannot match "
                   "any policy condition. Stop retrieving DB entries.",
                   sort_attr_name(policy), last_time);
        return TRUE;
    }
    else
        return FALSE;
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

static inline unsigned int skipped_count(const unsigned int *status_tab)
{
    int i;
    unsigned int nb = 0;

    /* skipped if it has been accessed, has changed, is whitelisted, matches no policy,
     * is in use, already running, type not supported...
     * i.e. status in AS_ACCESSED to AS_ALREADY
     */
    for (i = AS_ACCESSED ; i <= AS_ALREADY; i++)
        nb += status_tab[i];

    return nb;
}

static inline unsigned int error_count(const unsigned int *status_tab)
{
    int i;
    unsigned int nb = 0;

    /* next status are errors */
    for (i = AS_MISSING_MD; i <= AS_ERROR; i++)
        nb += status_tab[i];

    return nb;
}

/* return 0 if limit is not reached, a non null value else */
static inline int check_limit(policy_info_t *policy,
                              unsigned int count, unsigned long long vol,
                              unsigned int errors, int verbose)
{
    unsigned int total = count + errors;

    if (no_limit(policy))
        return 0;

    if ((policy->config->max_action_nbr > 0) &&
        (count >= policy->config->max_action_nbr))
    {
        DisplayLog(verbose ? LVL_EVENT : LVL_DEBUG, tag(policy),
                   "max count %u is reached for current run.",
                   policy->config->max_action_nbr);
        return 1;
    }
    if ((policy->config->max_action_vol > 0) &&
        (vol >= policy->config->max_action_vol))
    {
        DisplayLog(verbose ? LVL_EVENT : LVL_DEBUG, tag(policy),
                   "max volume %llu is reached for current run.",
                   policy->config->max_action_vol);
        return 1;
    }

    if ((policy->config->suspend_error_pct > 0.0)
        && (policy->config->suspend_error_min > 0)
        && (errors >= policy->config->suspend_error_min))
    {
        /* total >= errors >= suspend_error_min  > 0 */
        double pct = 100.0 * (float)errors/(float)total;
        if (pct >= policy->config->suspend_error_pct)
        {
            DisplayLog(verbose ? LVL_EVENT : LVL_DEBUG, tag(policy),
                       "error count %u >= %u, error rate %.2f%% >= %.2f => suspending policy run",
                       errors, policy->config->suspend_error_min,
                       pct, policy->config->suspend_error_pct);
            return 1;
        }
    }

    return 0;
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
        if (convert_boolexpr_to_simple_filter(&rules->rules[0].condition,
                                              p_filter))
        {
            DisplayLog(LVL_FULL, tag(policy),
                       "Could not convert purge rule '%s' to simple filter.",
                       rules->rules[0].rule_id);
        }
    }

    if (!policy->config->recheck_ignored_classes)
    {
        int i;
        filter_value_t fval;

        /* don't select files in ignored classes */
        for (i = 0; i < rules->ignore_count; i++)
        {
            int flags = 0;
            fval.value.val_str = rules->ignore_list[i]->fileset_id;
            if (i == 0)
                flags = FILTER_FLAG_NOT | FILTER_FLAG_ALLOW_NULL;
            else
                flags = FILTER_FLAG_NOT;
            lmgr_simple_filter_add(p_filter, ATTR_INDEX_fileclass, EQUAL,
                                   fval, flags);
        }
    }

    /* avoid re-checking all old whitelisted entries at the beginning of the list,
     * so start from the first non-whitelisted file.
     * restart from initial file when no migration could be done. */
    if (policy->first_eligible)
    {
        filter_value_t fval;
        char datestr[128];
        struct tm ts;

        fval.value.val_uint = policy->first_eligible;
        lmgr_simple_filter_add(p_filter, policy->config->lru_sort_attr, MORETHAN,
                               fval, 0);
        strftime(datestr, 128, "%Y/%m/%d %T",
                 localtime_r(&policy->first_eligible, &ts));
        DisplayLog(LVL_EVENT, tag(policy),
                   "Optimization: considering entries with %s newer than %s",
                   sort_attr_name(policy), datestr);
    }

    return 0;
}

static void report_progress(policy_info_t *policy,
                            const unsigned long long *pass_begin,
                            const unsigned long long *pass_current,
                            const unsigned int *status_tab_begin,
                            const unsigned int *status_tab_current)
{
    /* policy->progress.last_report contains the stats for the last pass */
    unsigned int count = policy->progress.action_cpt.count;
    unsigned long long vol = policy->progress.action_cpt.vol;
    unsigned long long targeted = policy->progress.action_cpt.targeted;
    unsigned int nb_skipped = policy->progress.skipped;
    unsigned int nb_errors = policy->progress.errors;

    /* add stats for the current pass */
    if (pass_begin && pass_current)
    {
        count += pass_current[AF_NBR_OK] - pass_begin[AF_NBR_OK];
        vol += pass_current[AF_VOL_OK] - pass_begin[AF_VOL_OK];
        targeted += pass_current[AF_TARGETED_OK] - pass_begin[AF_TARGETED_OK];
    }
    if (status_tab_begin && status_tab_current)
    {
        nb_errors = error_count(status_tab_current)
                     - error_count(status_tab_begin);
        nb_skipped = skipped_count(status_tab_current)
                     - skipped_count(status_tab_begin);
    }

    /* say hello every runtime interval */
    if (time(NULL) - policy->progress.last_report >=
            policy->config->report_interval)
    {
        char buf1[128];
        char buf2[128];
        char buf3[128];
        unsigned int spent = time(NULL) - policy->progress.policy_start;
        if (spent == 0)
            return;
        FormatDuration(buf1, 128, spent);
        FormatFileSize(buf2, 128, vol);
        FormatFileSize(buf3, 128, vol/spent);

        DisplayLog(LVL_EVENT, tag(policy), "Policy is running (started %s ago): "
                   "%u actions succeeded (%.2f/sec); volume: %s (%s/sec); "
                   "skipped: %u; errors: %u",
                   buf1, count, (float)count/(float)spent, buf2, buf3,
                   nb_skipped, nb_errors);
        policy->progress.last_report = time(NULL);
    }
}


/** wait until the queue is empty or migrations timed-out.
 * \return 0 when the queue is empty
 *         ETIME on timeout.
 */
static int wait_queue_empty(policy_info_t *policy, unsigned int nb_submitted,
                            const unsigned long long * feedback_init,
                            const unsigned int * status_tab_init,
                            unsigned long long * feedback_after,
                            unsigned int * status_tab_after,
                            int long_sleep)
{
    unsigned int nb_in_queue, nb_action_pending;

    /* Wait for end of purge pass */
    do
    {
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

        if ((nb_in_queue != 0) || (nb_action_pending != 0))
        {
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
        }
        else
            DisplayLog(LVL_DEBUG, tag(policy), "End of current pass");
    }
    while ((nb_in_queue != 0) || (nb_action_pending != 0));

    return 0;
}

/* set attributes to be retrieved from db */
static int init_db_attr_mask(policy_info_t *policy, const policy_param_t *param,
                             attr_set_t *p_attr_set)
{
#ifdef _HSM_LITE
    int rc;
    uint64_t allow_cached_attrs = 0;
    uint64_t need_fresh_attrs = 0;
#endif

    ATTR_MASK_INIT(p_attr_set);

    /* Retrieve at least: fullpath, last_access, size, blcks
     * because they are used for checking if info changed.
     * Retrieve last_mod and stripe_info for logs and reports.
     * Also retrieve info needed for blacklist/whitelist rules.
     */
/* need parent_id and name for ListMgr_Remove() prototype */

/* TODO depends on the prototype of the action to be taken + fileset mask + condition mask... */

/* needed for remove operations */
#ifdef HAVE_FID
    ATTR_MASK_SET(p_attr_set, name);
    ATTR_MASK_SET(p_attr_set, parent_id);
#endif
    /* needed for posix operations, and for display */
    ATTR_MASK_SET(p_attr_set, fullpath);

/* needed if update params != never */
    if (updt_params.md.when != UPDT_NEVER &&
        updt_params.md.when != UPDT_ALWAYS )
        ATTR_MASK_SET(p_attr_set, md_update);

#ifdef HAVE_FID
    if (updt_params.path.when != UPDT_NEVER &&
        updt_params.path.when != UPDT_ALWAYS)
        ATTR_MASK_SET(p_attr_set, path_update);
#endif
    p_attr_set->attr_mask |= (1LL << policy->config->lru_sort_attr);
    /* needed for display */
    ATTR_MASK_SET(p_attr_set, size);
    /* depends on policy params (limits) */
    if (param->target_cpt.blocks != 0 || param->target_cpt.targeted != 0)
        ATTR_MASK_SET(p_attr_set, blocks);
    if (param->target == TGT_POOL || param->target == TGT_OST)
    {
        ATTR_MASK_SET(p_attr_set, stripe_info);
        ATTR_MASK_SET(p_attr_set, stripe_items);
    }

    /* FIXME only if needed by scope? */
    ATTR_MASK_STATUS_SET(p_attr_set, policy->descr->status_mgr->smi_index);

    // TODO class management
    // ATTR_MASK_SET(p_attr_set, release_class);
    // ATTR_MASK_SET(p_attr_set, rel_cl_update);
    p_attr_set->attr_mask |= policy->descr->rules.run_attr_mask;

    /* TODO based on scope */
    //ATTR_MASK_SET(p_attr_set, type);

    /* FIXME only if needed by scope? */
    p_attr_set->attr_mask |= policy->descr->status_mgr->sm->status_needs_attrs_cached;

    return 0;
}

/* function to check limits:
    -> trigger type, limits, stats
*/

static int entry2tgt_amount(const policy_param_t *p_param,
                            const attr_set_t *attrs,
                            counters_t *pcpt)
{
    memset(pcpt, 0, sizeof(*pcpt));

    pcpt->count = 1;
    if (ATTR_MASK_TEST(attrs, size))
        pcpt->vol = ATTR(attrs, size);
    else if (ATTR_MASK_TEST(attrs, blocks))
        pcpt->vol = ATTR(attrs, blocks) * DEV_BSIZE;

    if (ATTR_MASK_TEST(attrs, blocks))
        pcpt->blocks = ATTR(attrs, blocks);

    if (p_param->target_cpt.targeted != 0)
    {
        /* When the target amount is not count, vol or blocks
         * This is the case for OST: the target is only a subset of the blocks.
         */
        if (p_param->target != TGT_OST && p_param->target) /* FIXME or pool? */
        {
            DisplayLog(LVL_CRIT, "PolicyRun", "unsupported targeted limit != OST");
            return -1;
        }

        pcpt->targeted = BlocksOnOST(pcpt->blocks, p_param->optarg_u.index,
                                     &ATTR(attrs, stripe_info), &ATTR(attrs, stripe_items));
    }

    return 0;
}

/** check if a counter (or the sum of 2 counters) exceed the specifed limits */
static int tgt_amount_reached(const counters_t *pcpt1, const counters_t *pcpt2, const counters_t *plimit)
{
    /* no counter? */
    if (pcpt1 == NULL && pcpt2 == NULL)
        return FALSE;
    /* no limit? */
    if (plimit== NULL)
        return FALSE;
    if (plimit->count == 0 && plimit->vol == 0 && plimit->blocks == 0 && plimit->targeted == 0)
        return FALSE;

    /* single counter check */
    if (pcpt2 == NULL)
        return counter_reached_limit(pcpt1, plimit);

    /* double counter check */
    return (((plimit->count != 0) && (pcpt1->count + pcpt2->count >= plimit->count)) ||
            ((plimit->vol != 0) && (pcpt1->vol + pcpt2->vol >= plimit->vol)) ||
            ((plimit->blocks != 0) && (pcpt1->blocks + pcpt2->blocks >= plimit->blocks)) ||
            ((plimit->targeted != 0) && (pcpt1->targeted + pcpt2->targeted >= plimit->targeted)));
}


/*
 *  Return ENOENT if no list is available
 */
int run_policy(policy_info_t *p_pol_info, const policy_param_t *p_param,
               action_summary_t *p_summary, lmgr_t *lmgr)
{
    int            rc;
    struct lmgr_iterator_t *it = NULL;

    lmgr_filter_t  filter;
    filter_value_t fval;
    lmgr_sort_type_t sort_type;

    attr_set_t     attr_set = {0};
    entry_id_t     entry_id;

    unsigned long long feedback_before[AF_ENUM_COUNT];
    unsigned long long feedback_after[AF_ENUM_COUNT];
    unsigned int   status_tab_before[AS_ENUM_COUNT];
    unsigned int   status_tab_after[AS_ENUM_COUNT];

    counters_t submitted;

    int            last_sort_time = 0;
    /* XXX first_request_start = policy_start */
    int            attr_mask_sav;
    int            end_of_list = FALSE;

    unsigned int   nb_returned, total_returned;

    lmgr_iter_opt_t opt = LMGR_ITER_OPT_INIT;

    if (!p_pol_info)
        RBH_BUG("p_pol_info argument is NULL");
    if (!p_param)
        RBH_BUG("p_param argument is NULL");

    p_pol_info->time_modifier = p_param->time_mod;

    memset(&p_pol_info->progress, 0, sizeof(p_pol_info->progress));
    if (p_summary)
        memset(p_summary, 0, sizeof(*p_summary));

    memset(feedback_before, 0, sizeof(feedback_before));
    memset(feedback_after, 0, sizeof(feedback_after));
    memset(status_tab_before, 0, sizeof(status_tab_before));
    memset(status_tab_after, 0, sizeof(status_tab_after));

    /* XXX previously here: interpreting target type and amount */

    /**
     * Do nothing if no previous scan was done.
     */
    if (!force_run(p_pol_info))
    {
        char timestamp[1024];
        if (ListMgr_GetVar(lmgr, LAST_SCAN_END_TIME, timestamp) != DB_SUCCESS)
        {
            DisplayLog(LVL_MAJOR, tag(p_pol_info),
                        "Full FS Scan has never been done. Policy ordering would be done on a partial list "
                        "(use --force to apply the policy anyway).");
            return ENOENT;
        }
        else
        {
            time_t         last_scan = atoi(timestamp);
            struct tm      date;

            localtime_r(&last_scan, &date);
            DisplayLog(LVL_EVENT, tag(p_pol_info),
    #ifdef HAVE_CHANGELOGS
                        "Building policy list - last full FS Scan: "
    #else
                        "Building policy list from last full FS Scan: "
    #endif
                        "%.4d/%.2d/%.2d %.2d:%.2d:%.2d",
                        1900 + date.tm_year, date.tm_mon + 1, date.tm_mday,
                        date.tm_hour, date.tm_min, date.tm_sec);
        }
    }

    rc = init_db_attr_mask(p_pol_info, p_param, &attr_set);
    if (rc)
        return rc;

    /* sort by last access */
    sort_type.attr_index = p_pol_info->config->lru_sort_attr; // TODO manage random
    sort_type.order = SORT_ASC;

    rc = lmgr_simple_filter_init(&filter);
    if (rc)
        return rc;

    // TODO list entries in scope */

#if 0
#ifdef ATTR_INDEX_invalid
    /* do not retrieve 'invalid' entries */
    fval.value.val_bool = FALSE;
    rc = lmgr_simple_filter_add(&filter, ATTR_INDEX_invalid, EQUAL, fval,
                                 FILTER_FLAG_ALLOW_NULL);
    if (rc)
        return rc;
#endif

#ifdef ATTR_INDEX_no_release // no_archive etc...
    /* do not retrieve entries with 'no_release' tag = 1 */
    fval.value.val_bool = TRUE;
    rc = lmgr_simple_filter_add(&filter, ATTR_INDEX_no_release, NOTEQUAL,
                                 fval, FILTER_FLAG_ALLOW_NULL);
    if (rc)
        return rc;
#endif

#ifdef ATTR_INDEX_status
    /* only get entries with HSM state SYNCHRO */
    fval.value.val_int = STATUS_SYNCHRO;
    rc = lmgr_simple_filter_add(&filter, ATTR_INDEX_status, EQUAL, fval, 0);
    if (rc)
        return rc;
#endif

    /* do not consider directories, if they are stored in DB */
#ifdef ATTR_INDEX_type
    if ((field_infos[ATTR_INDEX_type].flags & GENERATED) == 0)
    {

#if defined(_LUSTRE_HSM) || defined(_HSM_LITE)
        /* only retrieve files */
        fval.value.val_str = STR_TYPE_FILE;
        rc = lmgr_simple_filter_add(&filter, ATTR_INDEX_type, EQUAL, fval, 0);
#else
        /* do not retrieve directories */
        fval.value.val_str = STR_TYPE_DIR;
        rc = lmgr_simple_filter_add(&filter, ATTR_INDEX_type, NOTEQUAL, fval, 0);
#endif
        if (rc)
            return rc;
    }
#endif
#endif

    switch (p_param->target)
    {
        case TGT_FS:    /* apply policies to the filesystem */
            DisplayLog(LVL_MAJOR, tag(p_pol_info), "Starting policy run");
            break;

        case TGT_OST:   /* apply policies to the specified OST */
            DisplayLog(LVL_MAJOR, tag(p_pol_info), "Starting policy run on OST #%u",
                       p_param->optarg_u.index);

            /* retrieve stripe info and stripe items */
            ATTR_MASK_SET(&attr_set, stripe_info);
            ATTR_MASK_SET(&attr_set, stripe_items);

            /* retrieve files from this OST */
            fval.value.val_uint = p_param->optarg_u.index;
            rc = lmgr_simple_filter_add(&filter, ATTR_INDEX_stripe_items,
                                        EQUAL, fval, 0);
            if (rc)
                return rc;
            break;

        case TGT_FILE:  /* check/apply policies to the specified file */
            /* TODO: special case? */
            break;

        case TGT_POOL:  /* apply policies to the specified pool of OSTs */
            DisplayLog(LVL_MAJOR, tag(p_pol_info), "Starting policy run on pool '%s'",
                       p_param->optarg_u.name);
            ATTR_MASK_SET(&attr_set, stripe_info);
            ATTR_MASK_SET(&attr_set, stripe_items);
             /* retrieve files from this pool */
            fval.value.val_str = p_param->optarg_u.name;
            rc = lmgr_simple_filter_add(&filter, ATTR_INDEX_stripe_info,
                                        WILDCARDS_IN(fval.value.val_str)?LIKE:EQUAL,
                                        fval, 0);
            if (rc)
                return rc;
            break;

        case TGT_USER:  /* apply policies to the specified user */
            DisplayLog(LVL_MAJOR, tag(p_pol_info), "Starting policy run on '%s' user files",
                       p_param->optarg_u.name);

            ATTR_MASK_SET(&attr_set, owner);

            /* retrieve files for this owner */
            fval.value.val_str = p_param->optarg_u.name;
            rc = lmgr_simple_filter_add(&filter, ATTR_INDEX_owner,
                                        WILDCARDS_IN(fval.value.val_str)?LIKE:EQUAL,
                                        fval, 0);
            if (rc)
                return rc;
            break;

        case TGT_GROUP: /* apply policies to the specified group */
            DisplayLog(LVL_MAJOR, tag(p_pol_info), "Starting policy run on '%s' group files",
                       p_param->optarg_u.name);

            ATTR_MASK_SET(&attr_set, gr_name);

            /* retrieve files for this group */
            fval.value.val_str = p_param->optarg_u.name;
            rc = lmgr_simple_filter_add(&filter, ATTR_INDEX_gr_name,
                                        WILDCARDS_IN(fval.value.val_str)?LIKE:EQUAL,
                                        fval, 0);
            if (rc)
                return rc;
            break;

        case TGT_CLASS: /* apply policies to the specified fileclass */
            DisplayLog(LVL_MAJOR, tag(p_pol_info), "Starting policy run on fileclass '%s'",
                       p_param->optarg_u.name);

            ATTR_MASK_SET(&attr_set, fileclass);

            fval.value.val_str = p_param->optarg_u.name;
            rc = lmgr_simple_filter_add(&filter, ATTR_INDEX_fileclass,
                                        WILDCARDS_IN(fval.value.val_str)?LIKE:EQUAL,
                                        fval, 0);
            if (rc)
                return rc;
            break;

        default:
            DisplayLog(LVL_CRIT, tag(p_pol_info), "ERROR: unhandled target type %u\n",
                       p_param->target);
            return EINVAL;
    }

    /* Flushing messages before performing DB query */
    FlushLogs();

    /* add optimisation filters based on policies */
    if (!ignore_policies(p_pol_info))
        set_optimization_filters(p_pol_info, &filter);

    /* optimization: do not retrieve whitelisted entries
    * that do not need fileclass update. Skip this optimization
     * if there is no 'ignore' statement.
     */
#if 0 // FIXME implement with new fileclass management
    if (policies.purge_policies.whitelist_count > 0)
    {
        if (policies.updt_policy.fileclass.policy == UPDT_NEVER)
        {
            /* filter: release class != ignored */
            fval.value.val_str = CLASS_IGNORED;
            rc = lmgr_simple_filter_add(&filter, ATTR_INDEX_release_class,
                                         NOTEQUAL, fval, FILTER_FLAG_ALLOW_NULL);
            if (rc)
                return rc;
        }
        else if (policies.updt_policy.fileclass.policy == UPDT_PERIODIC)
        {
            /* filter: release class != ignored OR update <= now - period */
            fval.value.val_str = CLASS_IGNORED;
            rc = lmgr_simple_filter_add(&filter, ATTR_INDEX_release_class,
                                         NOTEQUAL, fval, FILTER_FLAG_ALLOW_NULL
                                                        | FILTER_FLAG_BEGIN);
            fval.value.val_uint = time(NULL) - policies.updt_policy.fileclass.period_max;
            rc = lmgr_simple_filter_add(&filter, ATTR_INDEX_rel_cl_update,
                                         LESSTHAN, fval, FILTER_FLAG_ALLOW_NULL
                                                        | FILTER_FLAG_OR
                                                        | FILTER_FLAG_END);
        }
    }
#endif

    /* start with a limited count of entries, to save memory */
    opt.list_count_max = p_pol_info->config->db_request_limit;
    nb_returned = 0;
    total_returned = 0;

    it = ListMgr_Iterator(lmgr, &filter, &sort_type, &opt);
    if (it == NULL)
    {
        lmgr_simple_filter_free(&filter);
        DisplayLog(LVL_CRIT, tag(p_pol_info),
                    "Error retrieving list of candidates from database. "
                    "Policy run cancelled.");
        return -1;
    }

    attr_mask_sav = attr_set.attr_mask;
    p_pol_info->progress.policy_start = p_pol_info->progress.last_report
        = time(NULL);

    /* loop on all policy passes */
    do
    {
        /* check if progress must be reported.
         * NULL => only report about p_pol_info->progress contents
         */
        report_progress(p_pol_info, NULL, NULL, NULL, NULL);

        /* Retrieve stats before starting policy,
         * for computing a delta later.
         */
        RetrieveQueueStats(&p_pol_info->queue, NULL, NULL, NULL, NULL, NULL,
                           status_tab_before, feedback_before);

        memset(&submitted, 0, sizeof(submitted));

        /* reset after's */
        memset(feedback_after, 0, sizeof(feedback_after));
        memset(status_tab_after, 0, sizeof(status_tab_after));

        /* List entries for policy */
        do
        {
            counters_t  entry_amount;

            /* reset attr_mask, if it was altered by last ListMgr_GetNext() call */
            memset(&attr_set, 0, sizeof(attr_set_t));
            attr_set.attr_mask = attr_mask_sav;

            memset(&entry_id, 0, sizeof(entry_id_t));
            rc = ListMgr_GetNext(it, &entry_id, &attr_set);

            if (aborted(p_pol_info))
            {
                /* free the last returned entry */
                if (rc == 0)
                    ListMgr_FreeAttrs(&attr_set);

                DisplayLog(LVL_MAJOR, tag(p_pol_info), "Policy run aborted, stop enqueuing requests.");
                end_of_list = TRUE;
                break;
            }
            else if (rc == DB_END_OF_LIST)
            {
                total_returned += nb_returned;

                /* if limit = inifinite => END OF LIST */
                if ((nb_returned == 0)
                     || ((opt.list_count_max > 0) &&
                        (nb_returned < opt.list_count_max)))
                {
                    DisplayLog(LVL_FULL, tag(p_pol_info), "End of list "
                                "(%u entries returned)", total_returned);
                    end_of_list = TRUE;
                    break;
                }

                /* no new useless request */
                if (heuristic_end_of_list(p_pol_info, last_sort_time))
                {
                    end_of_list = TRUE;
                    break;
                }

                /* Free previous iterator */
                ListMgr_CloseIterator(it);

                /* we must wait that migr. queue is empty,
                 * to prevent from processing the same entry twice
                 * (not safe until their md_update has not been updated).
                 */
                wait_queue_empty(p_pol_info, submitted.count, feedback_before,
                                 status_tab_before, feedback_after,
                                 status_tab_after, FALSE);

                /* perform a new request with next entries */

                /* /!\ if there is already a filter on <sort_attr> or md_update
                 * only replace it, do not add a new filter.
                 */

                /* don't retrieve just-updated entries
                 * (update>=first_request_time) */
                fval.value.val_int = p_pol_info->progress.policy_start;
                rc = lmgr_simple_filter_add_or_replace(&filter,
                                                        ATTR_INDEX_md_update,
                                                        LESSTHAN_STRICT, fval,
                                                        FILTER_FLAG_ALLOW_NULL);
                if (rc)
                    return rc;

                /* filter on <sort_time> */
                fval.value.val_int = last_sort_time;
                rc = lmgr_simple_filter_add_or_replace(&filter,
                                                       p_pol_info->config->lru_sort_attr,
                                                       MORETHAN, fval,
                                                       FILTER_FLAG_ALLOW_NULL);
                if (rc)
                    return rc;

                DisplayLog(LVL_DEBUG, tag(p_pol_info),
                            "Performing new request with a limit of %u entries"
                            " and %s >= %d and md_update < %ld ",
                            opt.list_count_max, sort_attr_name(p_pol_info),
                            last_sort_time, p_pol_info->progress.policy_start);

                nb_returned = 0;
                it = ListMgr_Iterator(lmgr, &filter, &sort_type, &opt);

                if (it == NULL)
                {
                    lmgr_simple_filter_free(&filter);
                    DisplayLog(LVL_CRIT, tag(p_pol_info),
                                "Error retrieving list of candidates from "
                                "database. Policy run cancelled.");
                    return -1;
                }
                continue;
            }
            else if (rc != 0)
            {
                DisplayLog(LVL_CRIT, tag(p_pol_info), "Error %d getting next entry of iterator", rc);
                break;
            }

            nb_returned++;

            rc = get_sort_attr(p_pol_info, &attr_set);
            if (rc != -1)
                last_sort_time = rc;

            rc = entry2tgt_amount(p_param, &attr_set, &entry_amount);
            if (rc == -1)
            {
                DisplayLog(LVL_MAJOR, tag(p_pol_info), "Failed to determine target amount for entry "DFID,
                           PFID(&entry_id));
                continue;
            }

            /* Insert candidate */
            rc = Queue_Insert(&p_pol_info->queue,
                    entry2queue_item(&entry_id, &attr_set, entry_amount.targeted));
            if (rc)
                return rc;

            counters_add(&submitted, &entry_amount);

        } while (!tgt_amount_reached(&submitted,
                                     &p_pol_info->progress.action_cpt,
                                     &p_param->target_cpt));

        /* Wait for end of purge pass */
        wait_queue_empty(p_pol_info, submitted.count, feedback_before,
                         status_tab_before, feedback_after, status_tab_after, TRUE);

        /* how much has been processed? errors? skipped? */
        p_pol_info->progress.action_cpt.count += feedback_after[AF_NBR_OK] - feedback_before[AF_NBR_OK];
        p_pol_info->progress.action_cpt.vol += feedback_after[AF_VOL_OK] - feedback_before[AF_VOL_OK];
        p_pol_info->progress.action_cpt.blocks += feedback_after[AF_BLOCKS_OK] - feedback_before[AF_BLOCKS_OK];
        p_pol_info->progress.action_cpt.targeted += feedback_after[AF_TARGETED_OK] - feedback_before[AF_TARGETED_OK];
        p_pol_info->progress.skipped += skipped_count(status_tab_after) - skipped_count(status_tab_before);
        p_pol_info->progress.errors += error_count(status_tab_after) - error_count(status_tab_before);

        /* if getnext returned an error */
        if (rc)
            break;
    }
    while ((!end_of_list) &&
           (!tgt_amount_reached(&p_pol_info->progress.action_cpt, NULL, &p_param->target_cpt)));

    lmgr_simple_filter_free(&filter);
    ListMgr_CloseIterator(it);

    if (p_summary)
        *p_summary = p_pol_info->progress;

    return (aborted(p_pol_info)?ECANCELED:0);
}

#ifndef _HAVE_FID               /* if entries are accessed by FID, we can always get their status */
inline static int invalidate_entry(lmgr_t * lmgr, entry_id_t * p_entry_id)
{
    attr_set_t     new_attr_set = {0};
    int            rc;

    ATTR_MASK_INIT(&new_attr_set);
    ATTR_MASK_SET(&new_attr_set, invalid);
    ATTR(&new_attr_set, invalid) = TRUE;

    /* update the entry */
    rc = ListMgr_Update(lmgr, p_entry_id, &new_attr_set);
    if (rc)
        DisplayLog(LVL_CRIT, PURGE_TAG, "Error %d tagging entry as invalid in database.", rc);

    return rc;
}
#endif

/* declaration from listmgr_common.c */
extern uint64_t     readonly_attr_set;

inline static int update_entry(lmgr_t * lmgr, entry_id_t * p_entry_id, attr_set_t * p_attr_set)
{
    int            rc;
    attr_set_t     tmp_attrset = *p_attr_set;

    /* /!\ do not update stripe info */
    /* @TODO actually, the best operation would be to update only attributes that changed */
    ATTR_MASK_UNSET(&tmp_attrset, stripe_info);
    ATTR_MASK_UNSET(&tmp_attrset, stripe_items);

    /* also unset read only attrs */
    tmp_attrset.attr_mask &= ~readonly_attr_set;
#ifdef ATTR_INDEX_creation_time
    /* never update creation time */
    ATTR_MASK_UNSET(&tmp_attrset, creation_time);
#endif

    /* update DB and skip the entry */
    rc = ListMgr_Update(lmgr, p_entry_id, &tmp_attrset);
    if (rc)
        DisplayLog(LVL_CRIT, TAG, "Error %d updating entry in database.", rc);

    return rc;
}

#ifdef _HAVE_FID
/**
 * Check that entry still exists
 * @param fill entry MD if entry is valid
 */
static int check_entry(policy_info_t *policy, lmgr_t *lmgr, queue_item_t *p_item, attr_set_t *new_attr_set)
{
    char           fid_path[RBH_PATH_MAX];
    struct stat    entry_md;
    int            rc;

    DisplayLog(LVL_FULL, tag(policy), "Considering entry " DFID, PFID(&p_item->entry_id));

    /* 1) Build fid path */
    BuildFidPath(&p_item->entry_id, fid_path);

    /* 2) Perform lstat on entry (size is need!):
     * important to respect LRU sort order (TODO: not if sort order is not in POSIX attrs)
     */
    if (lstat(fid_path, &entry_md) != 0)
    {
        rc = errno;

        /* If lstat returns an error, skip the entry */
        DisplayLog(LVL_DEBUG, tag(policy), "lstat() failed on %s. Skipping it.", fid_path);

        /* This entry has been processed and has probably removed */
        if (rc == ENOENT)
            /** @TODO remove entry from DB if errno = ENOENT ? */
            return AS_MOVED;
        else
            return AS_STAT_FAILURE;
    }

    /* convert posix attributes to attr structure */
    PosixStat2EntryAttr(&entry_md, new_attr_set, TRUE);

    /* set update time of the stucture */
    ATTR_MASK_SET(new_attr_set, md_update);
    ATTR(new_attr_set, md_update) = time(NULL);

    /* get fullpath or name, if they are needed for applying policy */

    if (policy->descr->rules.run_attr_mask & (ATTR_MASK_fullpath | ATTR_MASK_name))
    {
        path_check_update(&p_item->entry_id, fid_path, new_attr_set,
                          policy->descr->rules.run_attr_mask);
    }

#if 0 // TODO status management (only if scope is about status)
#ifdef ATTR_INDEX_status
#ifdef _LUSTRE_HSM
    /* For Lustre-HSM, don't care about fresh status because 'release'
     * is safe (Lustre locking + check of open/modified files)
     */
    /* is status known? */
    if (!ATTR_MASK_TEST(&p_item->entry_attr, status)
        || ATTR(&p_item->entry_attr, status) == STATUS_UNKNOWN)
    {
        DisplayLog(LVL_FULL, PURGE_TAG, "Update of HSM state (not known in DB)");
        rc = LustreHSM_GetStatus(fid_path, &ATTR(new_attr_set, status),
                                  &ATTR(new_attr_set, no_release),
                                  &ATTR(new_attr_set, no_archive));
        if (!rc)
        {
            ATTR_MASK_SET(new_attr_set, status);
            ATTR_MASK_SET(new_attr_set, no_release);
            ATTR_MASK_SET(new_attr_set, no_archive);
        }
    }

#elif defined(_HSM_LITE)
    /* always update status for _HSM_LITE */
    rc = rbhext_get_status(&p_item->entry_id, new_attr_set, new_attr_set);
    if (rc)
        return PURGE_ERROR;
#endif
#endif
#endif

    /* entry is valid */
    return AS_OK;
}
#else /* no FID */
/**
 * Check that entry exists with the good path and its id is consistent.
 * @param fill entry MD if entry is valid
 */
static int check_entry(lmgr_t * lmgr, purge_item_t * p_item, attr_set_t * new_attr_set)
{
#error "TODO: new check_entry prototype"
    struct stat    entry_md;
    char * stat_path;
#ifdef _HAVE_FID
    char fid_path[1024];
#endif

#ifndef _HAVE_FID
    /* 1) Check if fullpath is set (if no fid support) */
    if (!ATTR_MASK_TEST(&p_item->entry_attr, fullpath))
    {
        DisplayLog(LVL_DEBUG, PURGE_TAG, "Warning: entry fullpath is not set. Tagging it invalid.");
        invalidate_entry(lmgr, &p_item->entry_id);

        /* not enough metadata */
        return PURGE_PARTIAL_MD;
    }
    stat_path = ATTR(&p_item->entry_attr, fullpath);
#else
    BuildFidPath(&p_item->entry_id, fid_path);
    stat_path = fid_path;
#endif

if (ATTR_MASK_TEST(&p_item->entry_attr, fullpath))
    DisplayLog(LVL_FULL, PURGE_TAG, "Considering entry %s", ATTR(&p_item->entry_attr, fullpath));
#ifdef _HAVE_FID
else
    DisplayLog(LVL_FULL, PURGE_TAG, "Considering entry with fid="DFID, PFID(&p_item->entry_id));
#endif

    /* 2) Perform lstat on entry */

    if (lstat(stat_path, &entry_md) != 0)
    {
        /* If lstat returns an error, invalidate the entry */
        DisplayLog(LVL_DEBUG, PURGE_TAG, "lstat() failed on %s. Tagging it invalid.",
                    stat_path);
        invalidate_entry(lmgr, &p_item->entry_id);

        /* This entry has been processed and has probably moved */
        return PURGE_ENTRY_MOVED;
    }

    /* 3) check entry id and fskey */

    if ((entry_md.st_ino != p_item->entry_id.inode)
         || (get_fskey() != p_item->entry_id.fs_key))
    {
        /* If it has changed, invalidate the entry (fullpath does not match entry_id, it will be updated or removed at next FS scan). */
        DisplayLog(LVL_DEBUG, PURGE_TAG, "Inode of %s changed: old=<%llu,%llu>, "
                    "new=<%llu,%llu>. Tagging it invalid.",
                    ATTR(&p_item->entry_attr, fullpath),
                    (unsigned long long) p_item->entry_id.inode,
                    (unsigned long long) p_item->entry_id.fs_key,
                    (unsigned long long) entry_md.st_ino,
                    (unsigned long long) get_fskey());

        invalidate_entry(lmgr, &p_item->entry_id);

        /* This entry has been processed and has probably moved */
        return PURGE_ENTRY_MOVED;
    }

    /* convert posix attributes to attr structure */
    PosixStat2EntryAttr(&entry_md, new_attr_set, TRUE);

    /* set update time of the stucture */
    ATTR_MASK_SET(new_attr_set, md_update);
    ATTR(new_attr_set, md_update) = time(NULL);

    /* entry is valid */
    return PURGE_OK;
}
#endif


/**
 * Manage an entry by path or by fid, depending on FS
 */
static void process_entry(policy_info_t *pol, lmgr_t * lmgr,
                          queue_item_t * p_item)
{
    attr_set_t     new_attr_set = {0};
    int            rc;

    policy_match_t   match;
    rule_item_t     *rule;
    fileset_item_t  *p_fileset;
    attr_set_t      attr_sav;
    char            *hints = NULL;
    int lastrm;
    post_action_e   after_action = PA_NONE;

/* acknowledging helper */
#define policy_ack(_q, _status, _pattrs, _tgt)  do {                    \
                               unsigned long long feedback[AF_ENUM_COUNT]; \
                               memset(feedback, 0, sizeof(feedback));   \
                                    if (_status == AS_OK) {             \
                                        feedback[AF_NBR_OK] = 1;        \
                                        feedback[AF_VOL_OK] = ATTR_MASK_TEST(_pattrs,size)?ATTR(_pattrs,size):0;     \
                                        feedback[AF_TARGETED_OK] = _tgt;\
                                        feedback[AF_BLOCKS_OK] = ATTR_MASK_TEST(_pattrs,blocks)?ATTR(_pattrs,blocks):0; \
                                    } else {                            \
                                        feedback[AF_NBR_NOK] = 1;        \
                                        feedback[AF_VOL_NOK] = ATTR_MASK_TEST(_pattrs,size)?ATTR(_pattrs,size):0;     \
                                        feedback[AF_TARGETED_NOK] = _tgt;\
                                        feedback[AF_BLOCKS_NOK] = ATTR_MASK_TEST(_pattrs,blocks)?ATTR(_pattrs,blocks):0; \
                                    }                                   \
                                    Queue_Acknowledge(_q, _status, feedback, AF_ENUM_COUNT); \
                                } while(0)


    if (aborted(pol))
    {
       /* migration aborted by a signal, doesn't submit new migrations */
       DisplayLog(LVL_FULL, tag(pol), "Policy run aborted: skipping pending requests");
       policy_ack(&pol->queue, AS_ABORT, &p_item->entry_attr, p_item->targeted);
       rc = AS_ABORT;
       goto end;
    }

    DisplayLog(LVL_FULL, tag(pol),
               "Checking if entry %s matches policy rules",
               ATTR(&p_item->entry_attr, fullpath));

    ATTR_MASK_INIT(&new_attr_set);

    rc = check_entry(pol, lmgr, p_item, &new_attr_set);
    if (rc != AS_OK)
    {
        policy_ack(&pol->queue, rc, &p_item->entry_attr, p_item->targeted);
        goto end;
    }

    /* Merge with missing attrs from database */
    ListMgr_MergeAttrSets(&new_attr_set, &p_item->entry_attr, FALSE);

#ifdef ATTR_INDEX_invalid
    /* From here, assume that entry is valid */
    ATTR_MASK_SET(&new_attr_set, invalid);
    ATTR(&new_attr_set, invalid) = FALSE;
#endif

#if 0 // TODO manage no_release, no_archive etc.
#ifdef ATTR_INDEX_no_release
    /* check that the entry has the expected status */
    if (ATTR_MASK_TEST(&new_attr_set, no_release) && ATTR(&new_attr_set, no_release))
    {
        /* this entry is now tagged 'no_release' */
        DisplayLog(LVL_MAJOR, PURGE_TAG,
                    "Entry " DFID_NOBRACE " is now tagged 'no_release', skipping it.",
                    PFID(&p_item->entry_id));

        update_entry(lmgr, &p_item->entry_id, &new_attr_set);
        Acknowledge(&purge_queue, PURGE_ENTRY_WHITELISTED, 0, 0);

        goto end;
    }
#endif
#endif

#if 0 // TODO status management
#ifdef ATTR_INDEX_status
    if (!ATTR_MASK_TEST(&new_attr_set, status))
    {
        DisplayLog(LVL_MAJOR, PURGE_TAG, "Warning: entry status should be set at this point");
        Acknowledge(&purge_queue, PURGE_PARTIAL_MD, 0, 0);
        goto end;
    }
    else if (ATTR(&new_attr_set, status) != STATUS_SYNCHRO)
    {
        /* status changed */
        DisplayLog(LVL_MAJOR, PURGE_TAG,
                    "%s: entry status recently changed (%s): skipping entry.",
                    ATTR(&new_attr_set,fullpath),
                    db_status2str(ATTR(&new_attr_set, status),1));

        /* update DB and skip the entry */
        update_entry(lmgr, &p_item->entry_id, &new_attr_set);

        /* Notify that this entry is whitelisted */
        Acknowledge(&purge_queue, PURGE_STATUS_CHGD, 0, 0);
        goto end;
    }
#endif
#endif

    /* if ignore-policies flag is specified:
     * - don't check rules
     * - don't care about recent atime etc...
     */
    if (!ignore_policies(pol))
    {
        /* 4) check whitelist rules */
        match = is_whitelisted(pol->descr, &p_item->entry_id, &new_attr_set, &p_fileset);

        if (match == POLICY_MATCH)
        {
            update_entry(lmgr, &p_item->entry_id, &new_attr_set);
            policy_ack(&pol->queue, AS_WHITELISTED, &p_item->entry_attr, p_item->targeted);
            goto end;
        }
        else if (match != POLICY_NO_MATCH)
        {
           /* Cannot determine if entry is whitelisted: skip it (do nothing in database) */
            DisplayLog(LVL_MAJOR, tag(pol),
                       "Warning: cannot determine if entry %s is whitelisted: skipping it.",
                       ATTR(&p_item->entry_attr, fullpath));
            policy_ack(&pol->queue, AS_MISSING_MD, &p_item->entry_attr, p_item->targeted);
            goto end;
        }

        /* 5) check that entry has not been accessed.
         *   - If is has been accessed, update its info.
         *   - Else, perform purge, and remove entry from database /!\ nlink ?.
         */

#if 0 // TODO how to handle atime check with generic policies?
        int atime_check = TRUE;

        /* for directories or links, don't check access time as it is modified
         * by robinhood itself will collecting info about entry.
         */
        if (ATTR_MASK_TEST(&p_item->entry_attr, type) &&
            (!strcmp(ATTR(&p_item->entry_attr, type), STR_TYPE_LINK)
             || !strcmp(ATTR(&p_item->entry_attr, type), STR_TYPE_DIR)))
            atime_check = FALSE;

        if ((atime_check && !ATTR_MASK_TEST(&p_item->entry_attr, last_access))
             || !ATTR_MASK_TEST(&p_item->entry_attr, size))
        {
            /* cannot determine if entry has been accessed: update and skip it */
            DisplayLog(LVL_MAJOR, PURGE_TAG,
                        "Warning: previous value of 'last_access' or 'size' "
                        "is not available: cannot determine if entry has been "
                        "accessed. Skipping entry.");
            update_entry(lmgr, &p_item->entry_id, &new_attr_set);

            /* Notify error */
            Acknowledge(&purge_queue, PURGE_PARTIAL_MD, 0, 0);

            goto end;
        }

        if ((atime_check && (ATTR(&p_item->entry_attr, last_access) < ATTR(&new_attr_set, last_access)))
             || (ATTR(&p_item->entry_attr, size) != ATTR(&new_attr_set, size)))
        {
            DisplayLog(LVL_DEBUG, PURGE_TAG,
                        "%s has been accessed or modified since it was updated. Skipping entry.",
                        ATTR(&p_item->entry_attr, fullpath));
            DisplayLog(LVL_FULL, PURGE_TAG,
                        "atime before=%d, after=%d | size before=%llu, after=%llu",
                        (int) ATTR(&p_item->entry_attr, last_access),
                        (int) ATTR(&new_attr_set, last_access),
                        (unsigned long long) ATTR(&p_item->entry_attr, size),
                        (unsigned long long) ATTR(&new_attr_set, size));

            update_entry(lmgr, &p_item->entry_id, &new_attr_set);

            Acknowledge(&purge_queue, PURGE_ENTRY_ACCESSED, 0, 0);

            goto end;
        }
#endif

    } /* end if 'don't ignore policies' */

    /* get policy rule for the entry */
    rule = policy_case(pol->descr, &p_item->entry_id, &new_attr_set,
                       &p_fileset);
    if (!rule)
    {
        update_entry(lmgr, &p_item->entry_id, &new_attr_set);
        policy_ack(&pol->queue, AS_NO_POLICY, &p_item->entry_attr,
                   p_item->targeted);
        goto end;
    }

    /* don't care about policy condition if 'ignore-policies' flag is specified */
    if (!ignore_policies(pol))
    {
        /* check if the entry matches the policy condition */
        switch(entry_matches(&p_item->entry_id, &new_attr_set, &rule->condition,
                             pol->time_modifier))
        {
        case POLICY_NO_MATCH:
            /* entry is not eligible now */
            update_entry(lmgr, &p_item->entry_id, &new_attr_set);
            policy_ack(&pol->queue, AS_WHITELISTED, &p_item->entry_attr, p_item->targeted);
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
            policy_ack(&pol->queue, AS_MISSING_MD, &p_item->entry_attr,
                       p_item->targeted);
            goto end;
        }
    }

    /* it is the first matching entry? */
    rc = get_sort_attr(pol, &p_item->entry_attr);
    if (rc != -1 && (!pol->first_eligible || (rc < pol->first_eligible)))
        pol->first_eligible = rc;

    /* build hints */
    hints = build_action_hints(pol->descr, rule, p_fileset, &p_item->entry_id,
                               &new_attr_set);

    /* save attributes before doing the action */
    attr_sav = new_attr_set;

    /* apply action to the entry! */
    /* TODO action must indicate what to do with the entry
     * => db update, rm from filesystem etc... */
    rc = policy_action(pol, &p_item->entry_id, &new_attr_set, hints, &after_action);

#if 0 // TODO handle status update (in action? using status manager?)
#ifdef _LUSTRE_HSM
    rc = PurgeEntry_ByFid(&p_item->entry_id, &new_attr_set);

    if (rc == 0)
    {
        /* new status is release pending */
        ATTR_MASK_SET(&new_attr_set, status);
        ATTR(&new_attr_set, status) = STATUS_RELEASE_PENDING;
    }
    else
    {
        char fid_path[RBH_PATH_MAX];
        BuildFidPath(&p_item->entry_id, fid_path);

        /* we probably have a wrong status for this entry: refresh it */

        rc = LustreHSM_GetStatus(fid_path, &ATTR(&new_attr_set, status),
                                  &ATTR(&new_attr_set, no_release),
                                  &ATTR(&new_attr_set, no_archive));
        if (!rc)
        {
            ATTR_MASK_SET(&new_attr_set, status);
            ATTR_MASK_SET(&new_attr_set, no_release);
            ATTR_MASK_SET(&new_attr_set, no_archive);
        }
    }
#elif defined(_HSM_LITE)
    rc = PurgeEntry_ByFid(&p_item->entry_id, &new_attr_set);

    if (rc == 0)
    {
        /* new status is released */
        ATTR_MASK_SET(&new_attr_set, status);
        ATTR(&new_attr_set, status) = STATUS_RELEASED;
    }
#else
    /* FIXME should remove all paths to the object */
    rc = PurgeEntry(&p_item->entry_id, ATTR(&new_attr_set, fullpath));
#endif
#endif
    if (hints)
        free_action_hints(hints);

    if (rc != 0)
    {
        const char *err_str;

        if (abs(rc) < 126)
            err_str = strerror(abs(rc));
        else
            err_str = "command execution failed";

        DisplayLog(LVL_DEBUG, tag(pol), "Error applying action on entry %s: %s",
                   ATTR(&new_attr_set, fullpath), err_str);
        update_entry(lmgr, &p_item->entry_id, &new_attr_set);

        policy_ack(&pol->queue, AS_ERROR, &p_item->entry_attr,
                   p_item->targeted);
    }
    else
    {
        char           strtime[256];
        char           strsize[256];
        char           strfileset[FILESET_ID_LEN+128] = "";
        char           strstorage[24576]="";
        int            is_stor = FALSE;
        time_t         t;

        /* Action was sucessful */
        /* display attribute values just before the action,
         * as we used them to match the policy.
         */
        t = get_sort_attr(pol, &attr_sav);

        /* report messages */
        if (t > 0)
            FormatDurationFloat(strtime, 256, time( NULL ) - t);
        else
            strcpy(strtime, "<none>");
        FormatFileSize(strsize, 256, ATTR(&attr_sav, size));

#ifdef _LUSTRE
        if (ATTR_MASK_TEST(&p_item->entry_attr, stripe_items))
        {
            FormatStripeList(strstorage, sizeof(strstorage),
                &ATTR(&p_item->entry_attr, stripe_items), 0);
            is_stor = TRUE;
        }
        else
            strcpy(strstorage, "<none>");
#endif
        if (p_fileset)
            sprintf(strfileset, " (fileset=%s)", p_fileset->fileset_id);
        else
            strcpy(strfileset,"");

        DisplayLog(LVL_DEBUG, tag(pol),
                   "%s success for '%s', matching rule '%s'%s, %s %s%s, size=%s%s%s",
                   tag(pol), ATTR(&p_item->entry_attr, fullpath),
                   rule->rule_id, strfileset, sort_attr_name(pol),
                   strtime, ((t > 0) ? " ago" : ""),  strsize,
                   (is_stor ? " stored on " : ""), (is_stor ? strstorage : ""));

        DisplayReport("%s success for '%s', matching rule '%s'%s, %s %s%s | size=%"
                      PRI_SZ ", %s=%" PRI_TT "%s%s",
                      tag(pol), ATTR(&p_item->entry_attr, fullpath),
                      rule->rule_id, strfileset, sort_attr_name(pol),
                      strtime, ((t > 0) ? " ago" : ""), ATTR(&attr_sav, size),
                      sort_attr_name(pol), t, (is_stor ? ", stripes=" : ""),
                      (is_stor ? strstorage : ""));

        if (after_action == PA_UPDATE)
            update_entry(lmgr, &p_item->entry_id, &new_attr_set);
        else if (after_action ==  PA_RM_ONE)
        {
            lastrm = ATTR_MASK_TEST(&attr_sav, nlink)?
                        (ATTR(&attr_sav, nlink) <= 1):0;

            rc = ListMgr_Remove(lmgr, &p_item->entry_id,
                                /* must be based on the DB content = old attrs */
                                &p_item->entry_attr,
                                lastrm);
            if (rc)
                DisplayLog(LVL_CRIT, tag(pol), "Error %d removing entry from database.", rc);
        }
        else if (after_action == PA_RM_ALL)
        {
            rc = ListMgr_Remove(lmgr, &p_item->entry_id,
                                /* must be based on the DB content = old attrs */
                                &p_item->entry_attr,
                                1);
            if (rc)
                DisplayLog(LVL_CRIT, tag(pol), "Error %d removing entry from database.", rc);
        }

        policy_ack(&pol->queue, AS_OK, &new_attr_set, p_item->targeted); /* TODO update target info */
    }

  end:
    free_queue_item(p_item);
    return;
}


/**
 *  Main routine of policy thread
 */
static void   *thr_policy_run(void *arg)
{
    int            rc;
    lmgr_t         lmgr;
    void          *p_queue_entry;
    policy_info_t *pol = (policy_info_t*)arg;

    rc = ListMgr_InitAccess(&lmgr);
    if (rc)
    {
        DisplayLog(LVL_CRIT, tag(pol), "Could not connect to database (error %d). Exiting.", rc);
        exit(rc);
    }

    while (Queue_Get(&pol->queue, &p_queue_entry) == 0)
        process_entry(pol, &lmgr, (queue_item_t *)p_queue_entry);

    /* Error occured in purge queue management... */
    DisplayLog(LVL_CRIT, tag(pol), "An error occured in policy run queue management. Exiting.");
    exit(-1);
    return NULL;                /* for avoiding compiler warnings */
}

int start_worker_threads(policy_info_t *pol)
{
    unsigned int i;

    pol->threads = (pthread_t *)MemCalloc(pol->config->nb_threads,
                                          sizeof(pthread_t));
    if (!pol->threads)
    {
        DisplayLog(LVL_CRIT, tag(pol), "Memory error in %s", __FUNCTION__);
        return ENOMEM;
    }

    for (i = 0; i < pol->config->nb_threads; i++)
    {
        if (pthread_create(&pol->threads[i], NULL, thr_policy_run, pol) != 0)
        {
            int            rc = errno;
            DisplayLog(LVL_CRIT, tag(pol),
                       "Error %d creating policy threads in %s: %s", rc,
                       __FUNCTION__, strerror(rc));
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
 */
int check_current_actions(policy_info_t *pol, lmgr_t *lmgr, /* the timeout is in pol->config */
                          unsigned int *p_nb_reset, unsigned int *p_nb_total)
{
    int            rc;
    struct lmgr_iterator_t *it = NULL;

    lmgr_filter_t  filter;
    filter_value_t fval;

    queue_item_t q_item;

    unsigned int nb_returned = 0;
    unsigned int nb_aborted = 0;
    uint64_t     attr_mask_sav = 0;
#ifdef _HSM_LITE
    uint64_t     allow_cached_attrs = 0;
    uint64_t     need_fresh_attrs = 0;
#endif

    /* attributes to be retrieved */
    ATTR_MASK_INIT(&q_item.entry_attr);
    ATTR_MASK_SET(&q_item.entry_attr, fullpath);
    ATTR_MASK_SET(&q_item.entry_attr, path_update);
    /* /!\ don't retrieve status, to force getting it from the filesystem */

#ifdef _HSM_LITE
    ATTR_MASK_SET(&q_item.entry_attr, type);

    /* what information the backend needs from DB? */
    rc = rbhext_status_needs(TYPE_NONE, &allow_cached_attrs, &need_fresh_attrs);
    if (rc != 0)
    {
        DisplayLog(LVL_MAJOR, tag(pol), "Unexpected error from rbhext_status_needs(), in %s line %u: %d",
                   __FUNCTION__, __LINE__, rc);
        return rc;
    }
    q_item.entry_attr.attr_mask |= allow_cached_attrs;
#endif

    attr_mask_sav = q_item.entry_attr.attr_mask;

    rc = lmgr_simple_filter_init(&filter);
    if (rc)
        return rc;

    /* if timeout is > 0, only select entries whose last update
     * is old enough (last_update <= now - timeout) or NULL*/
    if (pol->config->action_timeout > 0)
    {
        fval.value.val_int = time(NULL) - pol->config->action_timeout;
        rc = lmgr_simple_filter_add(&filter, ATTR_INDEX_md_update, LESSTHAN,
                fval, FILTER_FLAG_ALLOW_NULL);
        if (rc)
            return rc;
    }

#if 0 // TODO manage status
    /* filter on current status RESTORE_RUNNING or RELEASE_PENDING
     * also check values with NULL status */

     /* '(status = RELEASE_PENDING' ... */
    fval.value.val_int = STATUS_RELEASE_PENDING;
    rc = lmgr_simple_filter_add(&filter, ATTR_INDEX_status, EQUAL, fval,
                FILTER_FLAG_BEGIN);
    if (rc)
        return rc;

    /* ...' OR status = RESTORE_RUNNING OR status is NULL)' */
    fval.value.val_int = STATUS_RESTORE_RUNNING;
    rc = lmgr_simple_filter_add(&filter, ATTR_INDEX_status, EQUAL, fval,
                FILTER_FLAG_OR | FILTER_FLAG_ALLOW_NULL | FILTER_FLAG_END);
    if (rc)
        return rc;
#endif

#ifdef ATTR_INDEX_invalid
    /* don't retrieve invalid entries (allow entries with invalid == NULL) */
    fval.value.val_int = TRUE;
    rc = lmgr_simple_filter_add(&filter, ATTR_INDEX_invalid, NOTEQUAL, fval,
            FILTER_FLAG_ALLOW_NULL);
    if (rc)
        return rc;
#endif

    it = ListMgr_Iterator(lmgr, &filter, NULL, NULL);

    if (it == NULL)
    {
        lmgr_simple_filter_free(&filter);
        DisplayLog(LVL_CRIT, tag(pol),
                    "Error retrieving the list of current actions. Recovery cancelled.");
        return -1;
    }

    memset(&q_item, 0, sizeof(queue_item_t));
    q_item.entry_attr.attr_mask = attr_mask_sav;

    while ((rc = ListMgr_GetNext(it, &q_item.entry_id, &q_item.entry_attr))
                == DB_SUCCESS)
    {
        nb_returned ++;

        if (ATTR_MASK_TEST(&q_item.entry_attr, fullpath))
            DisplayLog(LVL_VERB, tag(pol), "Updating status of '%s'...",
                ATTR(&q_item.entry_attr, fullpath));

        /* check entry */
        if (check_entry(pol, lmgr, &q_item, &q_item.entry_attr) == AS_OK)
        {
#if 0 // TODO manage status
            /* check new status */
            if (ATTR_MASK_TEST(&q_item.entry_attr, status)
                 && (ATTR(&q_item.entry_attr, status) == STATUS_RESTORE_RUNNING))
            {
               DisplayLog(LVL_EVENT, PURGE_TAG, "%s: restore still running",
                           ATTR(&q_item.entry_attr, fullpath));
            }
            else if (ATTR_MASK_TEST(&q_item.entry_attr, status)
                 && (ATTR(&q_item.entry_attr, status) == STATUS_RELEASE_PENDING))
            {
               DisplayLog(LVL_EVENT, PURGE_TAG, "%s: release still in progress",
                           ATTR(&q_item.entry_attr, fullpath));
            }
            else
            {
               DisplayLog(LVL_EVENT, PURGE_TAG, "%s: operation finished\n",
                           ATTR(&q_item.entry_attr, fullpath));
               nb_aborted++;
            }
#endif
            /* update entry status */
            update_entry(lmgr, &q_item.entry_id,  &q_item.entry_attr);
        }
        else
            nb_aborted ++;

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
    if (rc != DB_END_OF_LIST)
    {
        DisplayLog(LVL_CRIT, tag(pol), "Error %d getting next entry of iterator", rc);
        return -1;
    }

    return 0;
}
