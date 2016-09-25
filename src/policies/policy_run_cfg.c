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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "rbh_cfg_helpers.h"
#include "run_policies.h"
#include "rbh_misc.h"
#include "Memory.h"
#include <errno.h>

#define PARAM_SUFFIX   "_parameters"
#define TRIGGER_SUFFIX       "_trigger"

#define TAG       "PolicyRunCfg"

/* contains all run configs */
policy_run_config_list_t run_cfgs = { NULL, 0 };

static int polrun_set_default(const policy_descr_t *pol,
                              policy_run_config_t *cfg)
{
    memset(cfg, 0, sizeof(*cfg));

    cfg->nb_threads = 4;
    cfg->queue_size = 4096;
    cfg->db_request_limit = 100000;
    cfg->max_action_nbr = 0;    /* unlimited */
    cfg->max_action_vol = 0;    /* unlimited */

    cfg->trigger_list = NULL;
    cfg->trigger_count = 0;

    cfg->report_interval = 10 * 60; /* 10 min */

    cfg->pre_maintenance_window = 0;    /* disabled */
    cfg->maint_min_apply_delay = 30 * 60;   /* 30 min */

    cfg->suspend_error_pct = 0.0;   /* disabled */
    cfg->suspend_error_min = 0; /* disabled */

    /* attr index of the sort order (e.g. last_mod, creation_time, ...) */
    cfg->lru_sort_attr = pol->default_lru_sort_attr;

    cfg->action = pol->default_action;
    cfg->action_params.param_set = NULL;
    cfg->run_attr_mask = null_mask;

    cfg->check_action_status_on_startup = false;
    cfg->check_action_status_delay = 0; /* no check */

    cfg->action_timeout = 2 * 3600; /* 2h */

    cfg->recheck_ignored_entries = false;
    cfg->report_actions = true;

    return 0;
}

static void policy_run_cfg_set_default(void *module_config)
{
    int i;
    policy_run_config_list_t *cfg = (policy_run_config_list_t *) module_config;

    for (i = 0; i < cfg->count; i++)
        polrun_set_default(&policies.policy_list[i], &cfg->configs[i]);
}

static void *policy_run_cfg_new(void)
{
    policy_run_config_list_t *cfg;

    cfg = calloc(1, sizeof(policy_run_config_list_t));
    if (cfg == NULL)
        return NULL;

    /* safe because policies configuration is always parsed before
     * policy run config */
    cfg->count = policies.policy_count;
    cfg->configs =
        (policy_run_config_t *) calloc(cfg->count, sizeof(policy_run_config_t));
    if (cfg->configs == NULL)
        return NULL;

    return cfg;
}

static void policy_run_cfg_write_default(FILE *output)
{
    print_begin_block(output, 0, "<policy>" PARAM_SUFFIX, NULL);
    print_line(output, 1,
               "lru_sort_attr           : default_lru_sort_attr (from 'define_policy' block)");
    print_line(output, 1, "max_action_count        : 0 (unlimited)");
    print_line(output, 1, "max_action_volume       : 0 (unlimited)");
    print_line(output, 1, "suspend_error_pct       : disabled (0)");
    print_line(output, 1, "suspend_error_min       : disabled (0)");
    print_line(output, 1, "report_interval         : 10min");
    print_line(output, 1, "check_actions_on_startup: no");
    print_line(output, 1, "check_actions_interval  : 0 (disabled)");
    print_line(output, 1, "action_timeout          : 2h");
    print_line(output, 1, "recheck_ignored_entries : no");
    print_line(output, 1, "report_actions          : yes");
    print_line(output, 1, "nb_threads              : 4");
    print_line(output, 1, "queue_size              : 4096");
    print_line(output, 1, "db_result_size_max      : 100000");
    print_line(output, 1, "pre_maintenance_window  : 0 (disabled)");
    print_line(output, 1, "maint_min_apply_delay   : 30min");
    print_end_block(output, 0);
    fprintf(output, "\n");
}

static void policy_run_cfg_write_template(FILE *output)
{
    print_line(output, 0, "#<policy>" PARAM_SUFFIX " {");
    print_line(output, 1, "# sort order for applying the policy (overrides ");
    print_line(output, 1, "# default_lru_sort_attr from policy definition)");
    print_line(output, 1, "#lru_sort_attr = last_access ;");
    fprintf(output, "\n");
    print_line(output, 1,
               "# maximum number of actions per policy run (default: no limit)");
    print_line(output, 1, "#max_action_count = 100000 ;");
    print_line(output, 1,
               "# maximum volume of processed files per policy run (default: no limit)");
    print_line(output, 1, "#max_action_volume = 10TB ;");
    fprintf(output, "\n");
    print_line(output, 1, "# nbr of threads to execute policy actions");
    print_line(output, 1, "#nb_threads = 8;");
    fprintf(output, "\n");
    print_line(output, 1,
               "# suspend current run if 50%% of actions fail (after 100 errors):");
    print_line(output, 1, "#suspend_error_pct = 50%% ;");
    print_line(output, 1, "#suspend_error_min = 100 ;");
    print_line(output, 1, "# interval to report policy run progress:");
    print_line(output, 1, "#report_interval = 10min;");
    print_line(output, 1, "# cancel an action after a given time:");
    print_line(output, 1, "#action_timeout = 2h;");
    print_line(output, 1, "# interval to check the status of started actions");
    print_line(output, 1, "#check_actions_interval = 30min;");
    print_line(output, 1,
               "# check the status of previously started actions on startup:");
    print_line(output, 1, "#check_actions_on_startup = yes;");
    fprintf(output, "\n");
    print_line(output, 1,
               "# When applying policies, recheck entries that were previously");
    print_line(output, 1,
               "# ignored. Enable it after changing fileclass definitions,");
    print_line(output, 1, "# or if entries move from one class to another.");
    print_line(output, 1,
               "# This can significantly slow down policy application.");
    print_line(output, 1, "#recheck_ignored_entries = no;");
    fprintf(output, "\n");
    print_line(output, 1, "# report actions to report log file?");
    print_line(output, 1, "# report_actions = yes;");
    fprintf(output, "\n");
    print_line(output, 1, "# pre-maintenance feature parameters");
    print_line(output, 1, "# 0 to disable this feature");
    print_line(output, 1, "#pre_maintenance_window = 24h;");
    print_line(output, 1, "#maint_min_apply_delay = 30min;");
    fprintf(output, "\n");
    print_line(output, 1, "# internal/tuning parameters");
    print_line(output, 1, "#queue_size = 4096;");
    print_line(output, 1, "#db_result_size_max = 100000;");
    print_line(output, 0, "#}");
    fprintf(output, "\n");

    /* @TODO Add template triggers */
}

# if 0  // @TODO RBHv3: write template triggers

fprintf(output, "\n");
#ifdef _LUSTRE
print_line(output, 0, "# Trigger purge on individual OST usage");
print_begin_block(output, 0, TRIGGER_BLOCK, NULL);
print_line(output, 1, "trigger_on         = OST_usage ;");
print_line(output, 1, "high_threshold_pct = 85%% ;");
print_line(output, 1, "low_threshold_pct  = 80%% ;");
print_line(output, 1, "check_interval     = 5min ;");
print_end_block(output, 0);
fprintf(output, "\n");
#endif

print_line(output, 0, "# Trigger purge on filesystem usage");
print_begin_block(output, 0, TRIGGER_BLOCK, NULL);
print_line(output, 1, "trigger_on         = global_usage ;");
print_line(output, 1, "high_threshold_pct = 90%% ;");
print_line(output, 1, "low_threshold_pct  = 85%% ;");
print_line(output, 1, "check_interval     = 5min ;");
print_line(output, 1, "# raise an alert when the high threshold is reached");
print_line(output, 1, "alert_high         = yes ;");
print_line(output, 1, "# raise an alert if not enough data can be purged");
print_line(output, 1, "# to reach the low threshold");
print_line(output, 1, "alert_low          = yes ;");
print_end_block(output, 0);

fprintf(output, "\n");

/** @TODO not implemented yet */

#if 0
#ifdef _LUSTRE
print_line(output, 1, "# Trigger purge on pool usage");
print_begin_block(output, 1, TRIGGER_BLOCK);
print_line(output, 2, "trigger_on         = pool_usage(pool1,pool2) ;");
print_line(output, 2, "high_threshold_pct = 85%% ;");
print_line(output, 2, "low_threshold_pct  = 80%% ;");
print_line(output, 2, "check_interval     = 5min ;");
print_end_block(output, 1);
fprintf(output, "\n");
#endif

#endif

print_line(output, 0, "# Trigger purge of charlie's or foo's files");
print_line(output, 0, "# if they use more than a TB (check twice a day)");
print_begin_block(output, 0, TRIGGER_BLOCK, NULL);
print_line(output, 1, "trigger_on         = user_usage(charlie,foo) ;");
print_line(output, 1, "high_threshold_vol = 1TB ;");
print_line(output, 1, "low_threshold_vol  = 950GB ;");
print_line(output, 1, "check_interval     = 12h ;");
print_line(output, 1, "# send an alert when the quota is reached");
print_line(output, 1, "alert_high         = yes ;");
print_end_block(output, 0);

#endif

struct trig_target_def {
    const char *name;
    trigger_type_t type;
    policy_target_t target;
    bool allow_args;
};

/** list of possible triggers and their caracteristics */
static const struct trig_target_def target_defs[] = {
    /* periodic and scheduled are synonymes */
    {"periodic", TRIG_ALWAYS, TGT_FS, false},
    {"scheduled", TRIG_ALWAYS, TGT_FS, false},

    {"global_usage", TRIG_CONDITION, TGT_FS, false},
    {"user_usage", TRIG_CONDITION, TGT_USER, true},
    {"group_usage", TRIG_CONDITION, TGT_GROUP, true},
#ifdef _LUSTRE
    {"ost_usage", TRIG_CONDITION, TGT_OST, false},
    {"pool_usage", TRIG_CONDITION, TGT_POOL, true},
#endif

    {NULL, 0, 0, false} /* terminal element */
};

static const struct trig_target_def *str2trigger_def(const char *str)
{
    const struct trig_target_def *def;

    for (def = target_defs; def->name != NULL; def++) {
        if (!strcasecmp(def->name, str))
            return def;
    }
    return NULL;
}

/** fills the target fields of a trigger item */
static int set_trigger_target(trigger_item_t *p_trigger,
                              const struct trig_target_def *def,
                              char **args, unsigned int arg_count,
                              char *msg_out)
{
    int i;

    p_trigger->trigger_type = def->type;
    p_trigger->target_type = def->target;

    /* default: alert enabled if LW cannot be reached.
     * No thresholds for TRIG_ALWAYS. */
    p_trigger->alert_lw = ((def->type == TRIG_ALWAYS) ? false : true);

    /* optional arguments: target list (user list, group list, pool list) */
    if (arg_count > 0) {
        if (!def->allow_args) {
            sprintf(msg_out, "No extra argument expected for trigger type"
                    " '%s': %u argument(s) found.", def->name, arg_count);
            return EINVAL;
        }

        p_trigger->list = (char **)calloc(arg_count, sizeof(char *));
        p_trigger->list_size = arg_count;
        for (i = 0; i < arg_count; i++) {
            p_trigger->list[i] = strdup(args[i]);
            if (p_trigger->list[i] == NULL) {
                sprintf(msg_out, "Failed to allocate memory: %s",
                        strerror(errno));
                return ENOMEM;
            }
        }
    }
    return 0;
}

static inline const char *mk_threshold_param(const char *prefix,
                                             const char *suffix, char *buff)
{
    sprintf(buff, "%s_threshold_%s", prefix, suffix);
    return buff;
}

/** read thresholds params, check their consistency and fills the trigger item.
 *  @param[in] prefix "high" or "low"
 *  @param[in] p_trigger to check compatibility with trigger type and target.
 */
static int read_threshold_params(config_item_t config_blk,
                                 const char *block_name, const char *prefix,
                                 const trigger_item_t *p_trigger,
                                 trigger_value_type_t *type, threshold_u *val,
                                 char *msg_out)
{
    unsigned int cnt = 0;
    uint64_t tmpval;
    int rc;
    char buff[128]; /* oversized for param name */

    rc = GetFloatParam(config_blk, block_name,
                       mk_threshold_param(prefix, "pct", buff),
                       PFLG_POSITIVE | PFLG_ALLOW_PCT_SIGN, &val->percent,
                       NULL, NULL, msg_out);
    if ((rc != 0) && (rc != ENOENT))    /* real error */
        return rc;
    if (rc == 0) {
        *type = PCT_THRESHOLD;
        cnt++;
    }

    rc = GetSizeParam(config_blk, block_name,
                      mk_threshold_param(prefix, "vol", buff),
                      PFLG_POSITIVE, &val->volume, NULL, NULL, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    if (rc == 0) {
        *type = VOL_THRESHOLD;
        cnt++;
    }

    rc = GetInt64Param(config_blk, block_name,
                       mk_threshold_param(prefix, "cnt", buff),
                       PFLG_POSITIVE, &tmpval, NULL, NULL, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    if (rc == 0) {
        *type = COUNT_THRESHOLD;
        /* unsigned long long to uint64_t */
        val->count = (unsigned long long)tmpval;
        cnt++;
    }

    /* check params consistency */
    if (p_trigger->trigger_type == TRIG_ALWAYS) {
        if (cnt > 0) {
            /* in case of 'periodic' triggers, no thresholds are expected */
            strcpy(msg_out,
                   "No high/low threshold expected for 'periodic' trigger");
            return EINVAL;
        } else  /* no extra check needed */
            return 0;
    }

    if (cnt > 1) {
        sprintf(msg_out, "Multiple %s_threshold parameters in trigger", prefix);
        return EINVAL;
    }

    if (cnt == 0) {
        sprintf(msg_out, "No %s_threshold found in trigger (mandatory): "
                " '%s_threshold_pct', '%s_threshold_vol'"
                "or '%s_threshold_cnt' expected", prefix, prefix,
                prefix, prefix);
        return ENOENT;
    }

    /* count threshold is only support for global FS usage */
    if ((*type == COUNT_THRESHOLD)
        && (p_trigger->target_type != TGT_FS)
        && (p_trigger->target_type != TGT_USER)
        && (p_trigger->target_type != TGT_GROUP)) {
        strcpy(msg_out, "Threshold on entry count is only supported "
               "for 'global_usage', 'user_usage' and 'group_usage' triggers");
        return EINVAL;
    }

    return 0;
}

/** parse a trigger block from configuration and fills a trigger item */
static int parse_trigger_block(config_item_t config_blk, const char *block_name,
                               trigger_item_t *p_trigger_item, char *msg_out)
{
    int rc;
    char tmpstr[1024];
    char **arg_tab;
    unsigned int arg_count;
    config_item_t params_block;

    const struct trig_target_def *def;

    static const char *trigger_expect[] = {
        "trigger_on", "check_interval",
        "high_threshold_pct", "low_threshold_pct",
        "high_threshold_vol", "low_threshold_vol",
        "high_threshold_cnt", "low_threshold_cnt",
        "alert_high", "alert_low", "post_trigger_wait",
        "action_params", "max_action_count", "max_action_volume",
        NULL
    };

    const cfg_param_t cfg_params[] = {
        {"max_action_count", PT_INT, PFLG_POSITIVE,
         &p_trigger_item->max_action_nbr, 0},
        {"max_action_volume", PT_SIZE, PFLG_POSITIVE,
         &p_trigger_item->max_action_vol, 0},
        {"check_interval", PT_DURATION,
         PFLG_POSITIVE | PFLG_NOT_NULL | PFLG_MANDATORY,
         &p_trigger_item->check_interval, 0},
        {"alert_high", PT_BOOL, 0,
         &p_trigger_item->alert_hw, 0},
        {"alert_low", PT_BOOL, 0,
         &p_trigger_item->alert_lw, 0},
        {"post_trigger_wait", PT_DURATION, 0,
         &p_trigger_item->post_trigger_wait, 0},
        END_OF_PARAMS
    };

    memset(p_trigger_item, 0, sizeof(*p_trigger_item));

    /* retrieve special parameters */
    rc = GetStringParam(config_blk, block_name, "trigger_on",
                        PFLG_MANDATORY | PFLG_NO_WILDCARDS, tmpstr,
                        sizeof(tmpstr), &arg_tab, &arg_count, msg_out);
    if (rc) /* even ENOENT retruns an error because trigger_on is mandatory */
        return rc;

    /* initialize list of optional args */
    p_trigger_item->list = NULL;
    p_trigger_item->list_size = 0;

    def = str2trigger_def(tmpstr);
    if (def == NULL) {
        sprintf(msg_out, "Unexpected value for 'trigger_on' parameter: %s.",
                tmpstr);
        return EINVAL;
    }
    rc = set_trigger_target(p_trigger_item, def, arg_tab, arg_count, msg_out);
    if (rc)
        return rc;

    /* retrieve high and low thresholds params and check their compatibility */
    rc = read_threshold_params(config_blk, block_name, "high", p_trigger_item,
                               &p_trigger_item->hw_type, &p_trigger_item->hw_u,
                               msg_out);
    if (rc)
        return rc;

    rc = read_threshold_params(config_blk, block_name, "low", p_trigger_item,
                               &p_trigger_item->lw_type, &p_trigger_item->lw_u,
                               msg_out);
    if (rc)
        return rc;

    if ((p_trigger_item->trigger_type != TRIG_ALWAYS)
        && (p_trigger_item->hw_type != p_trigger_item->lw_type)) {
        strcpy(msg_out, "Incompatible high/low threshold types");
        return EINVAL;
    }
    /** FIXME RBHv3 count threshold for HSM systems should only match online
     *  files (not released) */

    /* retrieve other scalar parameters */
    rc = read_scalar_params(config_blk, block_name, cfg_params, msg_out);
    if (rc)
        return rc;

    /* get action_params subblock */
    bool unique = true;
    params_block =
        rh_config_GetItemByName(config_blk, "action_params", &unique);

    if (params_block != NULL) {
        if (!unique) {
            sprintf(msg_out, "Found duplicate block '%s' in '%s' line %d.",
                    "action_params", block_name,
                    rh_config_GetItemLine(params_block));
            return EEXIST;
        }
        if (rh_config_ItemType(params_block) != CONFIG_ITEM_BLOCK) {
            sprintf(msg_out,
                    "A block is expected for configuration item '%s::action_params', line %d.",
                    block_name, rh_config_GetItemLine(params_block));
            return EINVAL;
        }
#ifdef _DEBUG_POLICIES
        fprintf(stderr, "processing parameters for trigger '%s'\n", block_name);
#endif
        rc = read_action_params(params_block, &p_trigger_item->action_params,
                                &p_trigger_item->params_mask, msg_out);
        if (rc)
            return rc;
    }

    CheckUnknownParameters(config_blk, block_name, trigger_expect);

    return 0;
}

#define critical_err_check(_ptr_, _blkname_) do { if (!(_ptr_)) {\
          sprintf(msg_out, "Internal error reading %s block in config file",\
                  (_blkname_)); \
          return EFAULT; \
        }\
    } while (0)

static int polrun_read_config(config_file_t config, const char *policy_name,
                              const struct sm_instance *smi,
                              policy_run_config_t *conf, char *msg_out)
{
    int rc;
    char block_name[1024];
    char tmp[1024];
    config_item_t param_block, action_params_block;
    char **extra = NULL;
    unsigned int extra_cnt = 0;

    /* parameter for CheckUnknownParams() */
    static const char *allowed[] = {
        "lru_sort_attr", "max_action_count",
        "max_action_volume", "nb_threads", "suspend_error_pct",
        "suspend_error_min", "report_interval", "action_timeout",
        "check_actions_interval", "check_actions_on_startup",
        "recheck_ignored_entries", "report_actions",
        "pre_maintenance_window", "maint_min_apply_delay", "queue_size",
        "db_result_size_max", "action_params", "action",
        "recheck_ignored_classes",  /* for compat */
        NULL
    };

    /* parameter for read_scalar_params() */
    const cfg_param_t cfg_params[] = {
        {"max_action_count", PT_INT, PFLG_POSITIVE,
         &conf->max_action_nbr, 0},
        {"max_action_volume", PT_SIZE, PFLG_POSITIVE,
         &conf->max_action_vol, 0},
        {"nb_threads", PT_INT, PFLG_POSITIVE | PFLG_NOT_NULL,
         &conf->nb_threads, 0},
        {"suspend_error_pct", PT_FLOAT, PFLG_POSITIVE | PFLG_ALLOW_PCT_SIGN,
         &conf->suspend_error_pct, 0},
        {"suspend_error_min", PT_INT, PFLG_POSITIVE,
         &conf->suspend_error_min, 0},
        {"report_interval", PT_DURATION, PFLG_POSITIVE | PFLG_NOT_NULL,
         &conf->report_interval, 0},
        {"action_timeout", PT_DURATION, PFLG_POSITIVE,
         &conf->action_timeout, 0},
        {"check_actions_interval", PT_DURATION, PFLG_POSITIVE,
         &conf->check_action_status_delay, 0},
        {"check_actions_on_startup", PT_BOOL, 0,
         &conf->check_action_status_on_startup, 0},
        {"recheck_ignored_entries", PT_BOOL, 0,
         &conf->recheck_ignored_entries, 0},
        {"report_actions", PT_BOOL, 0, &conf->report_actions, 0},
        {"pre_maintenance_window", PT_DURATION, PFLG_POSITIVE,
         &conf->pre_maintenance_window, 0},
        {"maint_min_apply_delay", PT_DURATION, PFLG_POSITIVE,
         &conf->maint_min_apply_delay, 0},
        {"queue_size", PT_INT, PFLG_POSITIVE | PFLG_NOT_NULL,
         &conf->queue_size, 0},
        {"db_result_size_max", PT_INT, PFLG_POSITIVE,
         &conf->db_request_limit, 0},

        {NULL, 0, 0, NULL, 0}
    };
    snprintf(block_name, sizeof(block_name), "%s" PARAM_SUFFIX, policy_name);

    /* get <policy>_parameters block */
    rc = get_cfg_block(config, block_name, &param_block, msg_out);
    if (rc)
        return rc == ENOENT ? 0 : rc;   /* not mandatory */

    /* check deprecated parameters */
    rc = GetBoolParam(param_block, block_name, "recheck_ignored_classes", 0,
                      &conf->recheck_ignored_entries, NULL, NULL, msg_out);
    if (rc == 0)
        DisplayLog(LVL_CRIT, TAG, "WARNING: parameter %s::%s' is deprecated. "
                   "Use 'recheck_ignored_entries' instead.",
                   block_name, "recheck_ignored_classes");

    /* read all scalar params */
    rc = read_scalar_params(param_block, block_name, cfg_params, msg_out);
    if (rc)
        return rc;

    /* read specific parameters */

    /* 'lru_sort_attr' overrides 'default_lru_sort_attr' from 'define_policy' */
    rc = GetStringParam(param_block, block_name, "lru_sort_attr",
                        PFLG_NO_WILDCARDS, tmp, sizeof(tmp), NULL, NULL,
                        msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    else if (rc != ENOENT) {
        /* is it a time attribute? */
        rc = str2lru_attr(tmp, smi);
        if (rc == LRU_ATTR_INVAL) {
            strcpy(msg_out, "time attribute expected for 'lru_sort_attr': "
                   ALLOWED_LRU_ATTRS_STR "...");
            return EINVAL;
        } else
            conf->lru_sort_attr = rc;
    }

    /* 'action' overrides 'default_action' from 'define_policy' */
    rc = GetStringParam(param_block, block_name, "action",
                        0, tmp, sizeof(tmp), &extra, &extra_cnt, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    else if (rc != ENOENT) {
        rc = parse_policy_action("action", tmp, extra, extra_cnt,
                                 &conf->action, &conf->run_attr_mask, msg_out);
        if (rc)
            return rc;
    }

    /* get subblock */
    bool unique = true;
    action_params_block =
        rh_config_GetItemByName(param_block, "action_params", &unique);
    if (action_params_block != NULL) {
        if (!unique) {
            sprintf(msg_out, "Found duplicate block '%s' in '%s' line %d.",
                    "action_params", block_name,
                    rh_config_GetItemLine(action_params_block));
            return EEXIST;
        }
        if (rh_config_ItemType(action_params_block) != CONFIG_ITEM_BLOCK) {
            sprintf(msg_out,
                    "A block is expected for configuration item '%s::action_params', line %d.",
                    block_name, rh_config_GetItemLine(action_params_block));
            return EINVAL;
        }
#ifdef _DEBUG_POLICIES
        fprintf(stderr, "processing parameters in '%s'\n", block_name);
#endif
        rc = read_action_params(action_params_block, &conf->action_params,
                                &conf->run_attr_mask, msg_out);
        if (rc)
            return rc;
    }

    /* warn for unknown parameters */
    CheckUnknownParameters(param_block, block_name, allowed);

    return 0;
}

static int polrun_read_triggers(config_file_t config, const char *policy_name,
                                policy_run_config_t *conf, char *msg_out)
{
    int rc;
    unsigned int blk_index;
    char block_name[1024];

    /* get TRIGGER blocks */
    snprintf(block_name, sizeof(block_name), "%s" TRIGGER_SUFFIX, policy_name);

    for (blk_index = 0; blk_index < rh_config_GetNbBlocks(config);
         blk_index++) {
        char *curr_bname;

        config_item_t curr_item = rh_config_GetBlockByIndex(config, blk_index);
        critical_err_check(curr_item, "root");

        if (rh_config_ItemType(curr_item) != CONFIG_ITEM_BLOCK)
            continue;

        curr_bname = rh_config_GetBlockName(curr_item);
        critical_err_check(curr_bname, "root");

        if (!strcasecmp(curr_bname, block_name)) {
            conf->trigger_count++;
            /* realloc behaves as malloc when trigger_list is NULL */
            conf->trigger_list = (trigger_item_t *) realloc(conf->trigger_list,
                                                            conf->
                                                            trigger_count *
                                                            sizeof
                                                            (trigger_item_t));
            if (conf->trigger_list == NULL)
                return ENOMEM;

            /* analyze trigger block */
            rc = parse_trigger_block(curr_item, curr_bname,
                                     &conf->trigger_list[conf->trigger_count -
                                                         1], msg_out);
            if (rc)
                return rc;
        }
    }

    return 0;
}

/* read the run cfg for all policies */
static int policy_run_cfg_read(config_file_t config, void *module_config,
                               char *msg_out)
{
    int i, rc = 0;
    policy_run_config_list_t *allconf =
        (policy_run_config_list_t *) module_config;

    /* allconf->count is supposed to be set by set_default and configs must
     * be allocated.
     * double check by comparing policy count and policy_run count */
    if (allconf->count != policies.policy_count)
        RBH_BUG("Unexpected policy_run_cfg count != policy count");

    for (i = 0; i < allconf->count; i++) {
        rc = polrun_read_config(config, policies.policy_list[i].name,
                                policies.policy_list[i].status_mgr,
                                &allconf->configs[i], msg_out);
        if (rc)
            return rc;

        rc = polrun_read_triggers(config, policies.policy_list[i].name,
                                  &allconf->configs[i], msg_out);
        if (rc)
            return rc;
    }
    return 0;
}

static inline void no_trig_updt_msg(const char *what)
{
    DisplayLog(LVL_MAJOR, TAG, "%s changed in config file but cannot be "
               "modified dynamically: trigger update cancelled", what);
}

static void update_triggers(trigger_item_t *trigger_tgt,
                            unsigned int count_tgt,
                            trigger_item_t *trigger_new,
                            unsigned int count_new, bool *check_interval_chgd)
{
    unsigned int i;
    *check_interval_chgd = false;

    if (count_new != count_tgt) {
        /* skip trigger checking & update */
        no_trig_updt_msg("Trigger count");
        return;
    }

    /* check trigger types */
    for (i = 0; i < count_new; i++) {
        if (trigger_new[i].trigger_type != trigger_tgt[i].trigger_type
            || trigger_new[i].target_type != trigger_tgt[i].target_type) {
            no_trig_updt_msg("Trigger type");
            return;
        } else if ((trigger_new[i].trigger_type != TRIG_ALWAYS) &&
                   (trigger_new[i].hw_type != trigger_tgt[i].hw_type)) {
            no_trig_updt_msg("High threshold type");
            return;
        } else if ((trigger_new[i].trigger_type != TRIG_ALWAYS) &&
                   (trigger_new[i].lw_type != trigger_tgt[i].lw_type)) {
            no_trig_updt_msg("Low threshold type");
            return;
        }
    }

    /* triggers have the same type: update simple parameters:
     * max_action_count, max_action_volume, check_interval, alert_high,
     * alert_low, post_trigger_wait */
    for (i = 0; i < count_new; i++) {
        char tname[256];

        snprintf(tname, sizeof(tname), "#%u (%s): ", i,
                 trigger2str(&trigger_tgt[i]));

        if (trigger_new[i].check_interval != trigger_tgt[i].check_interval) {
            DisplayLog(LVL_EVENT, TAG,
                       "check_interval updated for trigger %s: %lu->%lu", tname,
                       trigger_tgt[i].check_interval,
                       trigger_new[i].check_interval);
            trigger_tgt[i].check_interval = trigger_new[i].check_interval;
            *check_interval_chgd = true;
        }

        if (trigger_new[i].max_action_nbr != trigger_tgt[i].max_action_nbr) {
            DisplayLog(LVL_EVENT, TAG,
                       "max_action_count updated for trigger %s: %u entries ->%u entries",
                       tname, trigger_tgt[i].max_action_nbr,
                       trigger_new[i].max_action_nbr);
            trigger_tgt[i].max_action_nbr = trigger_new[i].max_action_nbr;
        }

        if (trigger_new[i].max_action_vol != trigger_tgt[i].max_action_vol) {
            DisplayLog(LVL_EVENT, TAG,
                       "max_action_volume updated for trigger %s: %llu bytes->%llu bytes",
                       tname, trigger_tgt[i].max_action_vol,
                       trigger_new[i].max_action_vol);
            trigger_tgt[i].max_action_vol = trigger_new[i].max_action_vol;
        }

        if (trigger_new[i].post_trigger_wait !=
            trigger_tgt[i].post_trigger_wait) {
            DisplayLog(LVL_EVENT, TAG,
                       "post_trigger_wait updated for trigger %s: %lu->%lu",
                       tname, trigger_tgt[i].post_trigger_wait,
                       trigger_new[i].post_trigger_wait);
            trigger_tgt[i].post_trigger_wait = trigger_new[i].post_trigger_wait;
        }

        if (trigger_new[i].alert_hw != trigger_tgt[i].alert_hw) {
            DisplayLog(LVL_EVENT, TAG,
                       "alert_high updated for trigger %s: %s->%s", tname,
                       bool2str(trigger_tgt[i].alert_hw),
                       bool2str(trigger_new[i].alert_hw));
            trigger_tgt[i].alert_hw = trigger_new[i].alert_hw;
        }

        if (trigger_new[i].alert_lw != trigger_tgt[i].alert_lw) {
            DisplayLog(LVL_EVENT, TAG,
                       "alert_low updated for trigger %s: %s->%s", tname,
                       bool2str(trigger_tgt[i].alert_lw),
                       bool2str(trigger_new[i].alert_lw));
            trigger_tgt[i].alert_lw = trigger_new[i].alert_lw;
        }

        if (trigger_new[i].trigger_type == TRIG_ALWAYS)
            /* no threshold for 'periodic' triggers */
            continue;

        switch (trigger_new[i].hw_type) {
        case PCT_THRESHOLD:
            if (trigger_new[i].hw_percent != trigger_tgt[i].hw_percent) {
                DisplayLog(LVL_EVENT, TAG,
                           "High threshold updated for trigger %s: "
                           "%.2f%%->%.2f%%", tname, trigger_tgt[i].hw_percent,
                           trigger_new[i].hw_percent);
                trigger_tgt[i].hw_percent = trigger_new[i].hw_percent;
            }
            break;

        case VOL_THRESHOLD:
            if (trigger_new[i].hw_volume != trigger_tgt[i].hw_volume) {
                DisplayLog(LVL_EVENT, TAG,
                           "High threshold updated for trigger %s: %llu bytes->%llu bytes",
                           tname, trigger_tgt[i].hw_volume,
                           trigger_new[i].hw_volume);
                trigger_tgt[i].hw_volume = trigger_new[i].hw_volume;
            }
            break;

        case COUNT_THRESHOLD:
            if (trigger_new[i].hw_count != trigger_tgt[i].hw_count) {
                DisplayLog(LVL_EVENT, TAG,
                           "High threshold updated for trigger %s: %llu entries ->%llu entries",
                           tname, trigger_tgt[i].hw_count,
                           trigger_new[i].hw_count);
                trigger_tgt[i].hw_count = trigger_new[i].hw_count;
            }
            break;
        }

        switch (trigger_new[i].lw_type) {
        case PCT_THRESHOLD:
            if (trigger_new[i].lw_percent != trigger_tgt[i].lw_percent) {
                DisplayLog(LVL_EVENT, TAG,
                           "Low threshold updated for trigger %s: %.2f%%->%.2f%%",
                           tname, trigger_tgt[i].lw_percent,
                           trigger_new[i].lw_percent);
                trigger_tgt[i].lw_percent = trigger_new[i].lw_percent;
            }
            break;

        case VOL_THRESHOLD:
            if (trigger_new[i].lw_volume != trigger_tgt[i].lw_volume) {
                DisplayLog(LVL_EVENT, TAG,
                           "Low threshold updated for trigger %s: %llu bytes->%llu bytes",
                           tname, trigger_tgt[i].lw_volume,
                           trigger_new[i].lw_volume);
                trigger_tgt[i].lw_volume = trigger_new[i].lw_volume;
            }
            break;

        case COUNT_THRESHOLD:
            if (trigger_new[i].lw_count != trigger_tgt[i].lw_count) {
                DisplayLog(LVL_EVENT, TAG,
                           "Low threshold updated for trigger %s: %llu entries->%llu entries",
                           tname, trigger_tgt[i].lw_count,
                           trigger_new[i].lw_count);
                trigger_tgt[i].lw_count = trigger_new[i].lw_count;
            }
            break;
        }
    }

    /* update global interval check (GCD of all check intervals) if one of
     * them changed */
// TODO move to caller
//    if (check_interval_chgd)
//        ResMon_UpdateCheckInterval();

    /* triggers have been updated */
    return;
}

static void free_triggers(trigger_item_t *p_triggers, unsigned int count)
{
    unsigned int i, j;
    for (i = 0; i < count; i++) {
        if ((p_triggers[i].list_size > 0) && (p_triggers[i].list != NULL)) {
            /* free the strings */
            for (j = 0; j < p_triggers[i].list_size; j++) {
                if (p_triggers[i].list[j] != NULL)
                    free(p_triggers[i].list[j]);
            }

            /* free the arg list */
            free(p_triggers[i].list);

            /* free action_params */
            rbh_params_free(&p_triggers[i].action_params);
        }
    }

    /* free the trigger list */
    if ((count > 0) && (p_triggers != NULL))
        free(p_triggers);
}

static inline void no_param_updt_msg(const char *blk, const char *name)
{
    DisplayLog(LVL_MAJOR, TAG, "%s::%s changed in config file, but cannot be "
               "modified dynamically", blk, name);
}

#define PARAM_UPDT_MSG(_blk, _name, _format, _v1, _v2) \
        DisplayLog(LVL_EVENT, TAG, "%s::%s updated: "_format"->"_format, \
                   _blk, _name, _v1, _v2)

/** reload parameters for a single policy */
static int polrun_reload(const char *blkname, policy_run_config_t *cfg_tgt,
                         policy_run_config_t *cfg_new,
                         bool *recompute_interval)
{
    /* parameters that can't be modified dynamically */
    if (cfg_tgt->nb_threads != cfg_new->nb_threads)
        no_param_updt_msg(blkname, "nb_threads");

    if (cfg_tgt->queue_size != cfg_new->queue_size)
        no_param_updt_msg(blkname, "queue_size");

// FIXME can change action functions, but not cmd string
//    if (strcmp(cfg_new->default_action, cfg_tgt->default_action))
//        no_param_updt_msg(blkname, "default_action");
//  TODO parse action and action_params
//       and set params_attr_mask accordingly.

    if (cfg_tgt->lru_sort_attr != cfg_new->lru_sort_attr)
        no_param_updt_msg(blkname, "lru_sort_attr");

    /* dynamic parameters */
    if (cfg_tgt->max_action_nbr != cfg_new->max_action_nbr) {
        PARAM_UPDT_MSG(blkname, "max_action_count", "%u",
                       cfg_tgt->max_action_nbr, cfg_new->max_action_nbr);
        cfg_tgt->max_action_nbr = cfg_new->max_action_nbr;
    }

    if (cfg_tgt->max_action_vol != cfg_new->max_action_vol) {
        PARAM_UPDT_MSG(blkname, "max_action_volume", "%llu",
                       cfg_tgt->max_action_vol, cfg_new->max_action_vol);
        cfg_tgt->max_action_vol = cfg_new->max_action_vol;
    }

    if (cfg_tgt->suspend_error_pct != cfg_new->suspend_error_pct) {
        PARAM_UPDT_MSG(blkname, "suspend_error_pct", "%.2f%%",
                       cfg_tgt->suspend_error_pct, cfg_new->suspend_error_pct);
        cfg_tgt->suspend_error_pct = cfg_new->suspend_error_pct;
    }

    if (cfg_tgt->suspend_error_min != cfg_new->suspend_error_min) {
        PARAM_UPDT_MSG(blkname, "suspend_error_min", "%u",
                       cfg_tgt->suspend_error_min, cfg_new->suspend_error_min);
        cfg_tgt->suspend_error_min = cfg_new->suspend_error_min;
    }

    if (cfg_tgt->report_interval != cfg_new->report_interval) {
        PARAM_UPDT_MSG(blkname, "report_interval", "%lu",
                       cfg_tgt->report_interval, cfg_new->report_interval);
        cfg_tgt->report_interval = cfg_new->report_interval;
    }

    if (cfg_tgt->action_timeout != cfg_new->action_timeout) {
        PARAM_UPDT_MSG(blkname, "action_timeout", "%lu",
                       cfg_tgt->action_timeout, cfg_new->action_timeout);
        cfg_tgt->action_timeout = cfg_new->action_timeout;
    }

    if (cfg_tgt->check_action_status_delay !=
        cfg_new->check_action_status_delay) {
        PARAM_UPDT_MSG(blkname, "check_actions_interval", "%lu",
                       cfg_tgt->check_action_status_delay,
                       cfg_new->check_action_status_delay);
        cfg_tgt->check_action_status_delay = cfg_new->check_action_status_delay;
    }

    if (cfg_tgt->db_request_limit != cfg_new->db_request_limit) {
        PARAM_UPDT_MSG(blkname, "db_result_size_max", "%u",
                       cfg_tgt->db_request_limit, cfg_new->db_request_limit);
        cfg_tgt->db_request_limit = cfg_new->db_request_limit;
    }

    if (cfg_tgt->pre_maintenance_window != cfg_new->pre_maintenance_window) {
        PARAM_UPDT_MSG(blkname, "pre_maintenance_window", "%lu",
                       cfg_tgt->pre_maintenance_window,
                       cfg_new->pre_maintenance_window);
        cfg_tgt->pre_maintenance_window = cfg_new->pre_maintenance_window;
    }

    if (cfg_tgt->maint_min_apply_delay != cfg_new->maint_min_apply_delay) {
        PARAM_UPDT_MSG(blkname, "maint_min_apply_delay", "%lu",
                       cfg_tgt->maint_min_apply_delay,
                       cfg_new->maint_min_apply_delay);
        cfg_tgt->maint_min_apply_delay = cfg_new->maint_min_apply_delay;
    }

    if (cfg_tgt->check_action_status_on_startup !=
        cfg_new->check_action_status_on_startup) {
        PARAM_UPDT_MSG(blkname, "check_actions_on_startup", "%s",
                       bool2str(cfg_tgt->check_action_status_on_startup),
                       bool2str(cfg_new->check_action_status_on_startup));
        cfg_tgt->check_action_status_on_startup =
            cfg_new->check_action_status_on_startup;
    }

    if (cfg_tgt->recheck_ignored_entries != cfg_new->recheck_ignored_entries) {
        PARAM_UPDT_MSG(blkname, "recheck_ignored_entries", "%s",
                       bool2str(cfg_tgt->recheck_ignored_entries),
                       bool2str(cfg_new->recheck_ignored_entries));
        cfg_tgt->recheck_ignored_entries = cfg_new->recheck_ignored_entries;
    }

    if (cfg_tgt->report_actions != cfg_new->report_actions) {
        PARAM_UPDT_MSG(blkname, "report_actions", "%s",
                       bool2str(cfg_tgt->report_actions),
                       bool2str(cfg_new->report_actions));
        cfg_tgt->report_actions = cfg_new->report_actions;
    }

    update_triggers(cfg_tgt->trigger_list, cfg_tgt->trigger_count,
                    cfg_new->trigger_list, cfg_new->trigger_count,
                    recompute_interval);
    return 0;
}

/** reload cfg for all policies */
static int policy_run_cfg_reload(policy_run_config_list_t *conf)
{
    int i, rc;
    int err = 0;

    if (conf->count != run_cfgs.count) {
        DisplayLog(LVL_MAJOR, TAG, "New policy count doesn't match previous "
                   "policy count (%u vs %u): skipping config update.",
                   conf->count, run_cfgs.count);
        return 0;
    }
    for (i = 0; i < conf->count; i++) {
        bool chgd = false;
        char block_name[256];
        const char *pname = policies.policy_list[i].name;
        snprintf(block_name, sizeof(block_name), "%s" PARAM_SUFFIX, pname);

        rc = polrun_reload(block_name, &run_cfgs.configs[i], &conf->configs[i],
                           &chgd);
        if (rc) {
            DisplayLog(LVL_MAJOR, TAG,
                       "Failed to reload parameters for policy %s (rc=%d)",
                       pname, rc);
            if (rc > err)
                err = rc;
        } else {
            DisplayLog(LVL_DEBUG, TAG,
                       "Successfully reloaded config for policy %s", pname);
        }
    }

    /* policy runs may not be in the same order as policies and run_cfgs */
//    FIXME RBHv3
//    if (chgd && policy_runs.runs != NULL)
//        policy_module_update_check_interval(&policy_runs.runs[i]);

    return err;
}

static int policy_run_cfg_set(void *config, bool reload)
{
    policy_run_config_list_t *cfg = (policy_run_config_list_t *) config;

    if (reload)
        return policy_run_cfg_reload(cfg);

    run_cfgs = *cfg;
    return 0;
}

static void policy_run_cfg_free(void *config)
{
    policy_run_config_list_t *cfg = (policy_run_config_list_t *) config;

    if (cfg != NULL) {
        int i;

        if (cfg->configs != NULL) {
            for (i = 0; i < cfg->count; i++) {
                if (cfg->configs[i].trigger_list != NULL)
                    free_triggers(cfg->configs[i].trigger_list,
                                  cfg->configs[i].trigger_count);
                rbh_params_free(&cfg->configs[i].action_params);
            }
            free(cfg->configs);
        }
        free(cfg);
    }
}

mod_cfg_funcs_t policy_run_cfg_hdlr = {
    .module_name = "policy run",
    .new = policy_run_cfg_new,
    .free = policy_run_cfg_free,
    .set_default = policy_run_cfg_set_default,
    .read = policy_run_cfg_read,
    .set_config = policy_run_cfg_set,
    .write_default = policy_run_cfg_write_default,
    .write_template = policy_run_cfg_write_template
};
