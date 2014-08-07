/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009-2014 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

/**
 * \file generic_policies.h
 * \brief This module triggers migrations to HSM or external storage.
 */
#ifndef _GENERIC_POLICIES_H
#define _GENERIC_POLICIES_H

#include "rbh_const.h"
#include "config_parsing.h"
#include "list_mgr.h"
#include "queue.h"
#include <sys/types.h>

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
    TGT_NONE = 0,  /* if specified as a module parameter: check all triggers. */
    TGT_FS,    /* apply policies to the filesystem */
    TGT_OST,   /* apply policies to the specified OST */
    TGT_POOL,  /* apply policies to the specified pool of OSTs */
    TGT_USER,  /* apply policies to the specified user */
    TGT_GROUP, /* apply policies to the specified group */
    TGT_FILE,  /* check/apply policies to the specified file */
    TGT_CLASS, /* apply policies to the specified fileclass */
} policy_target_t;


/* TODO actions can be:
 *   1) policy wide (always the same for a given policy)
 *   2) trigger wide: specified for each trigger (?)
 *   3) policy_case wide: different for each policy case
 */

typedef enum {
    PCT_THRESHOLD,          /**< threshold is specified as a percentage */
    VOL_THRESHOLD,          /**< threshold is specified as a (used) size (in Bytes) */
    COUNT_THRESHOLD         /**< threshold is specified as a number of entries */
} trigger_value_type_t;

typedef struct trigger_item_t
{
    trigger_type_t  trigger_type;
    policy_target_t target_type;

    char       **list;       /**< list of groups or user for user/group triggers. NULL=> apply to all. */
    unsigned int list_size;
    time_t       check_interval;

    unsigned int   max_action_nbr; /* overrides global config */
    unsigned long long max_action_vol; /* overrides global config */

    trigger_value_type_t hw_type;
    union
    {
        unsigned long long volume;
        unsigned long long count;
        double             percent;
    } hw_u;

#define hw_volume    hw_u.volume
#define hw_percent   hw_u.percent
#define hw_count     hw_u.count

    trigger_value_type_t lw_type;
    union
    {
        unsigned long long volume;
        unsigned long long count;
        double             percent;
    } lw_u;

#define lw_volume   lw_u.volume
#define lw_percent  lw_u.percent
#define lw_count    lw_u.count

    /* min time to wait between 2 trigger applications */
    time_t         post_trigger_wait;

    /* trigger options: */
    /* raise alert when it is triggered */
    int alert_hw;

    /* raise alert when it cannot reach low threshold */
    int alert_lw;

} trigger_item_t;

static inline char *trigger2str(const trigger_item_t *trig)
{
    if (trig->trigger_type == TRIG_ALWAYS)
        return "scheduled";
    /* else: condition to be checked at scheduled interval */

    switch(trig->target_type)
    {
        case TGT_FS:
            return "global_usage";
        case TGT_OST:
            return "ost_usage";
        case TGT_POOL:
            return "pool_usage";
        case TGT_USER:
            return "user_usage";
        case TGT_GROUP:
            return "group_usage";

        case TGT_NONE:
            RBH_BUG("TGT_NONE: not an expected trigger");
        case TGT_CLASS: /* only for manual actions */
        case TGT_FILE: /* only for manual actions */
            RBH_BUG("No trigger expected on files or fileclass: only for manual actions");
    }
    return NULL;
}

typedef enum action_type_t
{
    ACTION_NONE,
    ACTION_FUNCTION,
    ACTION_COMMAND
} action_type_t;

/* what to do with the entry after the policy action.
 * returned by action_function */
typedef enum {
    PA_NONE,
    PA_RM_ONE,
    PA_RM_ALL,
    PA_UPDATE
} post_action_e;

typedef  int (*action_func_t)(const entry_id_t *,attr_set_t *, const char *,
                              post_action_e *after); /* hints */

action_func_t action_name2function(const char *fname);

typedef struct policy_run_config_t
{
    unsigned int   nb_threads;
    unsigned int   queue_size;
    unsigned int   db_request_limit;

    unsigned int   max_action_nbr; /* can also be specified in each trigger */
    unsigned long long max_action_vol; /* can also be specified in each trigger */

    trigger_item_t *trigger_list;
    unsigned int   trigger_count;

	action_type_t  action_type;
    union {
	    char            command[RBH_PATH_MAX];
        action_func_t   function;
    } action_u; /* command for ACTION_COMMAND, function for ACTION_FUNCTION, ... */

    time_t         check_action_status_delay;
    time_t         action_timeout;

    /* interval for reporting progress of current policy run */
    time_t         report_interval;

    time_t         pre_maintenance_window;
    time_t         maint_min_apply_delay;

    /* min error percentage to suspend current policy (0=disable) */
    double         suspend_error_pct;
    /* min error count to suspend current policy (0=disable) */
    unsigned int   suspend_error_min;

    /* attr index of the sort order (e.g. last_mod, creation_time, ...) */
    unsigned int   lru_sort_attr;

    int   check_action_status_on_startup;
    int   recheck_ignored_classes;

} policy_run_config_t;

typedef struct counters_t {
    unsigned long long  count;
    unsigned long long  vol;
    unsigned long long  blocks;
    unsigned long long  targeted;
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
    return (c->count != 0 || c->vol != 0 ||
            c->blocks != 0 || c->targeted != 0);
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

typedef struct __action_summary {
    time_t  policy_start;
    time_t  last_report;
    counters_t  action_ctr;
    unsigned int skipped;
    unsigned int errors;
} action_summary_t;

typedef enum
{
    TRIG_NOT_CHECKED,                            /* not checked yet */
    TRIG_BEING_CHECKED,                          /* currently beeing checked */
    TRIG_RUNNING,                                /* current policy run for this trigger */
    TRIG_OK,                                     /* no run is needed */
    TRIG_NO_LIST,                                /* no file list available */
    TRIG_NOT_ENOUGH,                             /* not enough candidates */
    TRIG_CHECK_ERROR,                            /* Misc Error */
    TRIG_ABORTED,                                /* aborted purge */
    TRIG_UNSUPPORTED                             /* Trigger not supported in this mode */
} trigger_status_t;

/* Info about each trigger */
typedef struct trigger_status__
{
    time_t         last_check;                   /* the last time this trigger was tested */
    trigger_status_t status;

    /* total of triggered actions since startup */
    counters_t     total_ctr;
    /* last triggered actions */
    counters_t     last_ctr;

    /* its usage, the last time it was checked for OST and global FS triggers */
    double         last_usage;
    /* for inode based thresholds */
    unsigned long long last_count;
} trigger_info_t;


/* policy runtime information */
typedef struct policy_info_t
{
    policy_descr_t  *descr;  /* point to policy descriptor */
    policy_run_config_t *config; /* policy run configuration */
    entry_queue_t    queue;  /* processing queue */
    pthread_t       *threads; /* worker threads array (size in config) */
    pthread_t        trigger_thr; /* trigger checker thread */
    lmgr_t           lmgr; /* db connexion for triggers */
    trigger_info_t  *trigger_info; /* stats about policy triggers */
    dev_t            fs_dev; /* to check if filesystem is unmounted */
    action_summary_t progress;
    time_t           first_eligible;
    time_modifier_t *time_modifier;
    time_t           gcd_interval; /* gcd of check intervals (gcd(triggers))*/
    int              flags; /* from policy_opt */
    unsigned int     aborted:1; /* abort status */
    volatile unsigned int waiting:1; /* a thread is already trying to join the trigger thread */
} policy_info_t;

/** policies runtime config */
typedef struct policy_run_configs_t {
    policy_run_config_t *configs;
    unsigned int count;
} policy_run_configs_t;
/** defined in policies/policy_run_cfg.c */
extern policy_run_configs_t run_cfgs;

/* set default for all policies */
int policy_run_cfg_set_default(void *module_config, char *msg_out);

/* load cfg for all policies */
int policy_run_cfg_read(config_file_t config, void *module_config, char *msg_out,
                        int for_reload);
/* reload cfg for all policies */
int policy_run_cfg_reload(void *module_config);

int policy_run_cfg_write_template(FILE *output);
int policy_run_cfg_write_defaults(FILE *output);

typedef union {
    unsigned int index;
    const char *name;
} target_u;

typedef struct policy_opt_t
{
    policy_target_t target;
    int             flags;
    target_u        optarg_u;
    union
    {
        double  usage_pct; /* target pct used for purges */
    } target_value_u;
} policy_opt_t;

/**
 * Start a policy module instance (workers, triggers...).
 * @param[out] policy this structure is filled with all policy run information and resources.
 * @param[in] policy_descr describes the policy to be managed.
 * @param[in] p_config policy run configuration.
 * @param[in] options run options from command line.
 */
int policy_module_start(policy_info_t *policy,         /* out */
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
