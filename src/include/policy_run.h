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

/**
 * \file policy_run.h
 * \brief This module tr/iggers migrations to HSM or external storage.
 */
#ifndef _GENERIC_POLICIES_H
#define _GENERIC_POLICIES_H

#include "rbh_const.h"
#include "config_parsing.h"
#include "policy_rules.h"
#include "queue.h"
#include <sys/types.h>
#include "rbh_logs.h"

/**
 *  Trigger type
 */
typedef enum {
    TRIG_ALWAYS = 0, /**< always trigger policy at scheduled interval */
    TRIG_CONDITION, /**<  check a condition at scheduled interval */
    /* TODO 2.6 add conditions: rbh idle, host under a given load, external command or module ... */
} trigger_type_t;

/** target for a policy run */
typedef enum {
    TGT_NONE = 0, /* if specified as a module parameter: check all triggers. */
    TGT_FS,     /* apply policies to the filesystem */
#ifdef _LUSTRE
    TGT_OST,    /* apply policies to the specified OST */
    TGT_POOL,   /* apply policies to the specified pool of OSTs */
    TGT_PROJID, /* apply policies to the specified project id */
#endif
    TGT_USER,   /* apply policies to the specified user */
    TGT_GROUP,  /* apply policies to the specified group */
    TGT_FILE,   /* check/apply policies to the specified file */
    TGT_CLASS,  /* apply policies to the specified fileclass */
} policy_target_t;

/* TODO actions can be:
 *   1) policy wide (always the same for a given policy)
 *   2) trigger wide: specified for each trigger (?)
 *   3) policy_case wide: different for each policy case
 */

typedef enum {
    PCT_THRESHOLD,    /**< threshold is specified as a percentage */
    VOL_THRESHOLD,    /**< threshold is specified as a (used) size (in Bytes) */
    COUNT_THRESHOLD,  /**< threshold is specified as a number of entries */
    CNTPCT_THRESHOLD  /**< threshold is specified as a percentage of
                           the number of available inodes */
} trigger_value_type_t;

typedef union {
    ull_t   volume;
    ull_t   count;
    double  percent;
} threshold_u;

typedef struct trigger_item_t {
    trigger_type_t      trigger_type;
    policy_target_t     target_type;

    char              **list; /**< list of groups or user for user/group
                                   triggers. NULL=> apply to all. */
    unsigned int        list_size;
    time_t              check_interval;

    unsigned int        max_action_nbr;
    ull_t               max_action_vol;

    trigger_value_type_t hw_type;
    /* volume, percent, or count */
    threshold_u         hw_u;

    trigger_value_type_t lw_type;
    /* volume, percent, or count */
    threshold_u         lw_u;

    /* min time to wait between 2 trigger applications */
    time_t              post_trigger_wait;

    /* trigger options: */
    /* raise alert when it is triggered */
    bool                alert_hw;

    /* raise alert when it cannot reach low threshold */
    bool                alert_lw;

    /* action params (overrides policy action params) */
    action_params_t     action_params;
    attr_mask_t         params_mask;

} trigger_item_t;

static inline char *trigger2str(const trigger_item_t *trig)
{
    if (trig->trigger_type == TRIG_ALWAYS)
        return "scheduled";
    /* else: condition to be checked at scheduled interval */

    switch (trig->target_type) {
    case TGT_FS:
        return "global_usage";
#ifdef _LUSTRE
    case TGT_OST:
        return "ost_usage";
    case TGT_POOL:
        return "pool_usage";
    case TGT_PROJID:
        return "projid_usage";
#endif
    case TGT_USER:
        return "user_usage";
    case TGT_GROUP:
        return "group_usage";

    case TGT_NONE:
        RBH_BUG("TGT_NONE: not an expected trigger");
    case TGT_CLASS:    /* only for manual actions */
    case TGT_FILE: /* only for manual actions */
        RBH_BUG("No trigger expected on files or fileclass: "
                "only for manual actions");
    }
    return NULL;
}

typedef enum {
    MS_INVALID = -1,  /**< Wrong value */
    MS_NONE    = 0,   /**< No matching */
    MS_CACHE_ONLY,    /**< Use only cached information for matching */
    MS_AUTO_ATTRS,    /**< Complete cached information by retrieving missing
                           attributes and attributes that are required to be
                           up to date (path excluded) */
    MS_AUTO_ALL,      /**< Complete cached information by retrieving missing
                           attributes and attributes that are required to be
                           up to date (including path) */
    MS_FORCE_UPDT,    /**< Force using up to date information */
} match_source_t;


typedef struct policy_run_config_t {
    unsigned int        nb_threads;
    unsigned int        queue_size;
    unsigned int        db_request_limit;

    unsigned int        max_action_nbr; /**< can also be specified in each
                                             trigger */
    ull_t               max_action_vol; /**< can also be specified in each
                                             trigger */
    trigger_item_t     *trigger_list;
    unsigned int        trigger_count;

    time_t              check_action_status_delay;
    time_t              action_timeout;

    /** interval for reporting progress of current policy run */
    time_t              report_interval;

    /* maintenance related option */
    /** is this policy influenced by maintenance mecanism */
    bool                maintenance_sensitive;
    /** time window to start modifying time conditions */
    time_t              pre_maintenance_window;
    /** minimal time condition */
    time_t              maint_min_apply_delay;

    /** min error percentage to suspend current policy (0=disable) */
    double              suspend_error_pct;
    /** min error count to suspend current policy (0=disable) */
    unsigned int        suspend_error_min;

    /** attr index of the sort order (e.g. last_mod, creation_time, ...).
     * overrides default_lru_sort_attr (from policy descr). */
    unsigned int        lru_sort_attr;
    /* overrides default_lru_sort_order */
    sort_order_t        lru_sort_order;

    /** if specified, overrides default_action from the policy descriptor.
     * Can then be overriden by rules. */
    policy_action_t     action;

    /** default action parameters for the policy.
     *  They can be overriden by action params from rule and fileset. */
    action_params_t     action_params;

    /** attributes used in action and action_params */
    attr_mask_t         run_attr_mask;

    bool                check_action_status_on_startup;
    bool                recheck_ignored_entries;

    /** report policy actions in report file? */
    bool                report_actions;

    /** Delay (in milliseconds) for resubmitting entries
     * to a scheduler after it delayed an entry. */
    unsigned int        reschedule_delay_ms;

    /** number of action schedulers */
    int                      sched_count;
    /** list of actions schedulers */
    const struct action_scheduler **schedulers;
    /** configuration of action schedulers */
    void                   **sched_cfg;

    /** source information for checking policy rules before scheduling */
    match_source_t       pre_sched_match;
    /** source information for checking policy rules before action
     * (after scheduling) */
    match_source_t       post_sched_match;

    /** command to execute before each policy run */
    char          **pre_run_command;
    /** command to execute after each policy run */
    char          **post_run_command;

} policy_run_config_t;

typedef struct counters_t {
    ull_t count;
    ull_t vol;
    ull_t blocks;
    ull_t targeted;
} counters_t;

/** add counters together */
static inline void counters_add(counters_t *dst, const counters_t *src)
{
    dst->count += src->count;
    dst->vol += src->vol;
    dst->blocks += src->blocks;
    dst->targeted += src->targeted;
}

/** test if a counter is zero */
static inline bool counter_is_set(const counters_t *c)
{
    return (c->count != 0 || c->vol != 0 || c->blocks != 0 || c->targeted != 0);
}

/** test if any of the counter fields reached a limit.
 * @param c the counter
 * @param l the limits
 */
static inline bool counter_reached_limit(const counters_t *c,
                                         const counters_t *l)
{
    return (((l->count != 0) && (c->count >= l->count))
            || ((l->vol != 0) && (c->vol >= l->vol))
            || ((l->blocks != 0) && (c->blocks >= l->blocks))
            || ((l->targeted != 0) && (c->targeted >= l->targeted)));
}

/** test if a specified target has been reached */
static inline bool counter_not_reached(const counters_t *c,
                                       const counters_t *t)
{
    return (((t->count != 0) && (c->count < t->count))
            || ((t->vol != 0) && (c->vol < t->vol))
            || ((t->blocks != 0) && (c->blocks < t->blocks))
            || ((t->targeted != 0) && (c->targeted < t->targeted)));
}

typedef struct __action_summary {
    time_t          policy_start;
    time_t          last_report;
    counters_t      action_ctr;
    unsigned int    skipped;
    unsigned int    errors;
} action_summary_t;

typedef enum {
    TRIG_NOT_CHECKED,   /* not checked yet */
    TRIG_BEING_CHECKED, /* currently beeing checked */
    TRIG_RUNNING,       /* current policy run for this trigger */
    TRIG_OK,            /* no run is needed */
    TRIG_NO_LIST,       /* no file list available */
    TRIG_NOT_ENOUGH,    /* not enough candidates */
    TRIG_CHECK_ERROR,   /* Misc Error */
    TRIG_ABORTED,       /* aborted purge */
    TRIG_UNSUPPORTED    /* Trigger not supported in this mode */
} trigger_status_t;

/* Info about each trigger */
typedef struct trigger_status__ {
    time_t              last_check;  /* the last time this trigger was tested */
    trigger_status_t    status;

    /* total of triggered actions since startup */
    counters_t          total_ctr;
    /* last triggered actions */
    counters_t          last_ctr;

    /* its usage, the last time it was checked for OST and global FS triggers */
    double              last_usage;
    /* for inode based thresholds there is also percentage */
    double              last_count;
} trigger_info_t;

/* policy runtime information */
typedef struct policy_info_t {
    policy_descr_t         *descr;        /**< point to policy descriptor */
    policy_run_config_t    *config;       /**< policy run configuration */
    const action_params_t  *trigger_action_params;  /**< action parameters from
                                                         trigger */
    entry_queue_t           queue;        /**< processing queue */
    pthread_t              *threads;      /**< worker threads array (size in config) */
    pthread_t               trigger_thr;  /**< trigger checker thread */
    lmgr_t                  lmgr;         /**< db connexion for triggers */
    trigger_info_t         *trigger_info; /**< stats about policy triggers */
    dev_t                   fs_dev;       /**< to check if filesystem is
                                               unmounted */
    struct sched_res_t     *sched_res;    /**< internal state of schedulers
                                           *   (see config to known their count) */
    action_summary_t        progress;
    time_t                  first_eligible;
    time_modifier_t        *time_modifier;
    time_t                  gcd_interval; /**< gcd of check intervals
                                               (gcd(policy triggers)) */
    run_flags_t             flags;        /**< from policy_opt */
    bool                    aborted;      /**< abort status */
    bool                    stopping;     /**< current run is stopping */
    volatile bool           waiting;      /**< a thread is already trying to
                                               join the trigger thread */
} policy_info_t;

/**
 * Scheduler init function prototype.
 * @param[in]   config          Scheduler configuration created by its config handlers.
 * @param[out]  p_sched_data    Private context allocated by this function.
 */
typedef int (*sched_init_func_t)(void *config, void **p_sched_data);

/**
 *  Reset a scheduler.
 *  Empty all its internal queues and reset its state for a new policy run.
 */
typedef int (*sched_reset_func_t)(void *sched_data);

typedef enum {
    SCHED_OK            = 0,    /**< Entry taken into account by scheduler */
    SCHED_DELAY         = 1,    /**< Wait before submitting new entries */
    SCHED_SKIP_ENTRY    = 2,    /**< Skip the entry for the  current run */
    SCHED_STOP_RUN      = 3,    /**< Stop submitting entries for the current
                                 *   policy run. Already submitted tasks
                                 *   keep running. Queued entries in previous
                                 *   schedulers are canceled.
                                 */
    SCHED_KILL_RUN      = 4,    /**< Abort the policy run and cancel in-flight
                                     tasks in next schedulers. */
} sched_status_e;

/**
 * Function to be called by an action scheduler to trigger an action,
 * or skip an entry.
 * @param st    SCHED_OK if the action is to be performed.
 *              Another status in other cases (stop run, etc.)
 */
typedef void (*sched_cb_t)(void *udata, sched_status_e st);


/**
 * Scheduler function prototype.
 * @param[in,out] sched_data   Scheduler private context created by
 *                             sched_init.
 * @param[in]     id           Entry id for the action to be scheduled.
 * @param[in]     attrs        Entry attributes for the action to be scheduled.
 * @param[in]     cb           Function to be called by the scheduler to trigger
 *                             an action, or skip an entry.
 * @param[in]     udata        Argument to be passed to cb function.
 *
 * @return  A positive code (sched_status_e) for policy workflow control.
 *          A negative value (-errno) on error.
 */
typedef int (*sched_func_t)(void *sched_data,
                            const entry_id_t *id,
                            const attr_set_t *attrs,
                            sched_cb_t cb,
                            void *udata);

/** Action scheduler (implemented by plugins) */
typedef struct action_scheduler {
    const char            *sched_name;      /**< Scheduler name */
    const ctx_cfg_funcs_t *sched_cfg_funcs; /**< Configuration handlers */
    sched_init_func_t      sched_init_func; /**< Scheduler initialization
                                              function */
    sched_reset_func_t     sched_reset_func;/**< Scheduler reset function */
    attr_mask_t            sched_attr_mask; /**< Needed attributes to make the
                                              scheduling decision */
    sched_func_t           sched_schedule;  /**< Function to invoke the
                                              scheduler */
} action_scheduler_t;

/** policies runtime config */
typedef struct policy_run_config_list_t {
    policy_run_config_t *configs;
    unsigned int         count;
} policy_run_config_list_t;

/** defined in policies/policy_run_cfg.c */
extern policy_run_config_list_t run_cfgs;

/** config handlers */
extern mod_cfg_funcs_t policy_run_cfg_hdlr;

typedef union {
    int         index;
    const char *name;
} target_u;

typedef struct policy_opt_t {
    policy_target_t     target;
    run_flags_t         flags;
    target_u            optarg_u;
    double              usage_pct;   /**< target pct for purges */

    /* limits from command line */
    unsigned int        max_action_nbr;
    ull_t               max_action_vol;
} policy_opt_t;

/**
 * Start a policy module instance (workers, triggers...).
 * @param[out] policy   This structure is filled with all policy run information
 *                      and resources.
 * @param[in] policy_descr Describes the policy to be managed.
 * @param[in] p_config     Policy run configuration.
 * @param[in] options      Run options from command line.
 */
int policy_module_start(policy_info_t *policy, /* out */
                        policy_descr_t *policy_descr,  /* in */
                        policy_run_config_t *p_config, /* in */
                        const policy_opt_t *options);  /* in */
int policy_module_stop(policy_info_t *policy);
int policy_module_wait(policy_info_t *policy);
void policy_module_dump_stats(policy_info_t *policy);

/* update trigger intervals,
 * update gcd_interval
 */
void policy_module_update_check_interval(policy_info_t *policy);

#endif
