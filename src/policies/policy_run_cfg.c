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

#include "rbh_cfg.h"
#include "run_policies.h"
#include "rbh_misc.h"
#include "Memory.h"
#include <errno.h>

#define PARAM_SUFFIX   "_parameters"
#define TRIGGER_SUFFIX       "_trigger"

#define TAG       "PolicyRunCfg"

/* contains all run configs */
policy_run_configs_t run_cfgs = { NULL, 0 };

static int polrun_set_default(policy_run_config_t *cfg)
{
    memset(cfg, 0, sizeof(*cfg));
    cfg->nb_threads = 4;
    cfg->queue_size = 4096;
    cfg->db_request_limit = 100000;
    cfg->max_action_nbr = 0; /* unlimited */
    cfg->max_action_vol = 0; /* unlimited */

    cfg->trigger_list = NULL;
    cfg->trigger_count = 0;

    /* TODO move to policy declaration? */
    cfg->action_type = ACTION_NONE; /* mandatory */

    cfg->check_action_status_delay = 30 * 60; /* 30 min */
    cfg->action_timeout = 2 * 3600; /* 2h */
    cfg->report_interval = 10 * 60; /* 10 min */

    cfg->pre_maintenance_window = 24 * 3600; /* 24h */
    cfg->maint_min_apply_delay = 30 * 60; /* 30 min */

    cfg->suspend_error_pct = 0.0; /* disabled */
    cfg->suspend_error_min = 0; /* disabled */

    /* attr index of the sort order (e.g. last_mod, creation_time, ...) */
    cfg->lru_sort_attr =  ATTR_INDEX_last_access;

    cfg->check_action_status_on_startup = true;
    cfg->recheck_ignored_classes = true;

    return 0;
}

static inline policies_t* runcfg2policies(void *module_config)
{
    /* hack to  get the policy list */
    return (policies_t *)((ptrdiff_t)module_config
        - offsetof(robinhood_config_t, policy_run_cfgs)
        + offsetof(robinhood_config_t, policies));
}

int policy_run_cfg_set_default(void *module_config, char *msg_out)
{
    int i;
    policy_run_configs_t *cfg = (policy_run_configs_t *)module_config;
    policies_t *parsed_policies = runcfg2policies(module_config);

    cfg->count = parsed_policies->policy_count;
    cfg->configs = (policy_run_config_t *)MemCalloc(cfg->count, sizeof(policy_run_config_t));
    if (cfg->configs == NULL)
    {
        strcpy(msg_out, "Memory allocation failed");
        return ENOMEM;
    }
    for (i = 0; i < cfg->count; i++)
        polrun_set_default(&cfg->configs[i]);
    return 0;
}

int policy_run_cfg_write_defaults(FILE *output)
{
    print_begin_block(output, 0, "<policy>"PARAM_SUFFIX, NULL);
    print_line(output, 1, "lru_sort_attr           : last_access");
    print_line(output, 1, "default_action          : <none> (mandatory parameter)");
    print_line(output, 1, "max_action_count        : 0 (unlimited)");
    print_line(output, 1, "max_action_volume       : 0 (unlimited)");
    print_line(output, 1, "suspend_error_pct       : disabled (0)");
    print_line(output, 1, "suspend_error_min       : disabled (0)");
    print_line(output, 1, "report_interval         : 10min");
    print_line(output, 1, "action_timeout          : 2h");
    print_line(output, 1, "check_actions_interval  : 30min");
    print_line(output, 1, "check_actions_on_startup: yes");
    print_line(output, 1, "recheck_ignored_classes : yes" );
    print_line(output, 1, "nb_threads              : 4");
    print_line(output, 1, "queue_size              : 4096");
    print_line(output, 1, "db_result_size_max      : 100000");
    print_line(output, 1, "pre_maintenance_window  : 24h");
    print_line(output, 1, "maint_min_apply_delay   : 30min");
    print_end_block(output, 0);
    fprintf(output, "\n");

    return 0;
}

int policy_run_cfg_write_template(FILE *output)
{
    print_begin_block(output, 0, TEMPL_POLICY_NAME PARAM_SUFFIX, NULL);
    print_line(output, 1, "# sort order for applying the policy");
    print_line(output, 1, "lru_sort_attr = last_access ;");
    fprintf(output, "\n");
    print_line(output, 1, "# Default action for this policy.");
    print_line(output, 1, "# The syntax to call built-in functions is <module_name>.<action_name>");
    print_line(output, 1, "# e.g. common.copy, common.unlink, lhsm.archive, lhsm.release...");
    print_line(output, 1, "default_action = common.unlink ;");
    print_line(output, 1, "# To call a custom script instead, use the following syntax:");
    print_line(output, 1, "# default_action = cmd(\"/usr/bin/move_to_trash.sh {path}\") ;");
    print_line(output, 1, "# Special parameters can passed to the command:");
    print_line(output, 1, "#    {path}: posix path to the entry");
#ifdef _LUSTRE
#   ifdef _HAVE_FID
    print_line(output, 1, "#    {fid}: fid of the entry");
#   endif
    print_line(output, 1, "#    {fsname}: Lustre fsname");
#endif
    print_line(output, 1, "#    {hints}: pass action_hints to the command");
    fprintf(output, "\n");
    print_line(output, 1, "# maximum number of actions per policy run (default: no limit)");
    print_line(output, 1, "#max_action_count = 100000 ;");
    print_line(output, 1, "# maximum volume of processed files per policy run (default: no limit)");
    print_line(output, 1, "#max_action_volume = 10TB ;");
    fprintf(output, "\n");
    print_line(output, 1, "# nbr of threads to execute policy actions" );
    print_line(output, 1, "nb_threads = 8;");
    fprintf(output, "\n");
    print_line(output, 1, "# suspend current run if 50%% of actions fail (after 100 errors):");
    print_line(output, 1, "#suspend_error_pct = 50%% ;");
    print_line(output, 1, "#suspend_error_min = 100 ;");
    print_line(output, 1, "# interval to report policy run progress:");
    print_line(output, 1, "#report_interval = 10min;");
    print_line(output, 1, "# cancel an action after a given time:");
    print_line(output, 1, "#action_timeout = 2h;");
    print_line(output, 1, "# interval to check the status of started actions");
    print_line(output, 1, "#check_actions_interval = 30min;");
    print_line(output, 1, "# check the status of previously started actions on startup:");
    print_line(output, 1, "#check_actions_on_startup = yes;");
    fprintf(output, "\n");
    print_line(output, 1, "# When applying policies, recheck entries that previously");
    print_line(output, 1, "# matched ignored classes.");
    print_line(output, 1, "# Enable it after changing fileclass definitions");
    print_line(output, 1, "# or if entries move from one class to another.");
    print_line(output, 1, "# This can significantly slow down policy application.");
    print_line(output, 1, "#recheck_ignored_classes = yes" );
    fprintf(output, "\n");
    print_line(output, 1, "# pre-maintenance feature parameters");
    print_line(output, 1, "#pre_maintenance_window = 24h;");
    print_line(output, 1, "#maint_min_apply_delay = 30min;");
    fprintf(output, "\n");
    print_line(output, 1, "# internal/tuning parameters");
    print_line(output, 1, "#queue_size = 4096;");
    print_line(output, 1, "#db_result_size_max = 100000;");
    print_end_block(output, 0);
    fprintf(output, "\n");

    /* @TODO Add template triggers */

    return 0;
}

# if 0 // @TODO RBHv3: write template triggers
    
    fprintf( output, "\n" );
#ifdef _LUSTRE
    print_line( output, 0, "# Trigger purge on individual OST usage" );
    print_begin_block( output, 0, TRIGGER_BLOCK, NULL );
    print_line( output, 1, "trigger_on         = OST_usage ;" );
    print_line( output, 1, "high_threshold_pct = 85%% ;" );
    print_line( output, 1, "low_threshold_pct  = 80%% ;" );
    print_line( output, 1, "check_interval     = 5min ;" );
    print_end_block( output, 0 );
    fprintf( output, "\n" );
#endif

    print_line( output, 0, "# Trigger purge on filesystem usage" );
    print_begin_block( output, 0, TRIGGER_BLOCK, NULL );
    print_line( output, 1, "trigger_on         = global_usage ;" );
    print_line( output, 1, "high_threshold_pct = 90%% ;" );
    print_line( output, 1, "low_threshold_pct  = 85%% ;" );
    print_line( output, 1, "check_interval     = 5min ;" );
    print_line( output, 1, "# raise an alert when the high threshold is reached" );
    print_line( output, 1, "alert_high         = yes ;" );
    print_line( output, 1, "# raise an alert if not enough data can be purged");
    print_line( output, 1, "# to reach the low threshold");
    print_line( output, 1, "alert_low          = yes ;" );
    print_end_block( output, 0 );

    fprintf( output, "\n" );

/** @TODO not implemented yet */

#if 0
#ifdef _LUSTRE
    print_line( output, 1, "# Trigger purge on pool usage" );
    print_begin_block( output, 1, TRIGGER_BLOCK );
    print_line( output, 2, "trigger_on         = pool_usage(pool1,pool2) ;" );
    print_line( output, 2, "high_threshold_pct = 85%% ;" );
    print_line( output, 2, "low_threshold_pct  = 80%% ;" );
    print_line( output, 2, "check_interval     = 5min ;" );
    print_end_block( output, 1 );
    fprintf( output, "\n" );
#endif

#endif

    print_line( output, 0, "# Trigger purge of charlie's or foo's files" );
    print_line( output, 0, "# if they use more than a TB (check twice a day)" );
    print_begin_block( output, 0, TRIGGER_BLOCK, NULL );
    print_line( output, 1, "trigger_on         = user_usage(charlie,foo) ;" );
    print_line( output, 1, "high_threshold_vol = 1TB ;" );
    print_line( output, 1, "low_threshold_vol  = 950GB ;" );
    print_line( output, 1, "check_interval     = 12h ;" );
    print_line( output, 1, "# send an alert when the quota is reached" );
    print_line( output, 1, "alert_high         = yes ;" );
    print_end_block( output, 0 );

#endif

static int parse_trigger_block(config_item_t config_blk, const char *block_name,
                               trigger_item_t * p_trigger_item, char *msg_out)
{
    int            rc;
    int            rc_hp, rc_lp, rc_hv, rc_lv, rc_hc, rc_lc;
    unsigned int   high_count = 0;
    unsigned int   low_count = 0;
    double         h_pct, l_pct;
    unsigned long long h_vol, l_vol;
    uint64_t       h_cnt, l_cnt;
    int            i;
    char           tmpstr[1024];
    char         **arg_tab;
    unsigned int   arg_count;

    static const char *trigger_expect[] =
    {
        "trigger_on", "check_interval",
        "high_threshold_pct", "low_threshold_pct",
        "high_threshold_vol", "low_threshold_vol",
        "high_threshold_cnt", "low_threshold_cnt",
        "alert_high", "alert_low",
        NULL
    };

    const cfg_param_t cfg_params[] = {
        {"max_action_count",    PT_INT,     PFLG_POSITIVE,
            &p_trigger_item->max_action_nbr, 0},
        {"max_action_volume",   PT_SIZE,    PFLG_POSITIVE,
            &p_trigger_item->max_action_vol, 0},
        {"check_interval", PT_DURATION, PFLG_POSITIVE | PFLG_NOT_NULL | PFLG_MANDATORY,
            &p_trigger_item->check_interval, 0},
        {"alert_high", PT_BOOL, 0, &p_trigger_item->alert_hw, 0},
        {"alert_low", PT_BOOL, 0, &p_trigger_item->alert_lw, 0},
        {"post_trigger_wait", PT_DURATION, 0, &p_trigger_item->post_trigger_wait, 0},
        END_OF_PARAMS
    };

    memset(p_trigger_item, 0, sizeof(*p_trigger_item));

    /* retrieve special parameters */
    rc = GetStringParam(config_blk, block_name, "trigger_on",
                        PFLG_MANDATORY | PFLG_NO_WILDCARDS, tmpstr,
                        sizeof(tmpstr), &arg_tab, &arg_count, msg_out);
    if (rc)                   /* even ENOENT retruns an error because trigger_on is mandatory */
        return rc;

    /* initialize list of optional args */
    p_trigger_item->list = NULL;
    p_trigger_item->list_size = 0;

    /* analyze trigger_on parameter */
    if (!strcasecmp(tmpstr, "periodic") || !strcasecmp(tmpstr, "scheduled"))
    {
        p_trigger_item->trigger_type = TRIG_ALWAYS;
        p_trigger_item->target_type = TGT_FS; /* TODO allow other targets (user, group, fileclass...) */

        /* default: alert enabled if LW cannot be reached */
        p_trigger_item->alert_lw = false;

        /* no arg expected */
        if (arg_count > 0)
        {
            sprintf(msg_out,
                    "No extra argument expected for trigger type '%s': %u argument(s) found.",
                    tmpstr, arg_count);
            return EINVAL;
        }
    }
    else if (!strcasecmp(tmpstr, "global_usage"))
    {
        p_trigger_item->trigger_type = TRIG_CONDITION;
        p_trigger_item->target_type = TGT_FS;

        /* default: alert enabled if LW cannot be reached */
        p_trigger_item->alert_lw = true;

        /* no arg expected */
        if (arg_count > 0)
        {
            sprintf(msg_out,
                     "No extra argument expected for trigger type '%s': %u argument(s) found.",
                     tmpstr, arg_count);
            return EINVAL;
        }
    }
    else if (!strcasecmp(tmpstr, "OST_usage"))
    {
        p_trigger_item->trigger_type = TRIG_CONDITION;
        p_trigger_item->target_type = TGT_OST;

        /* default: alert enabled if LW cannot be reached */
        p_trigger_item->alert_lw = true;

        /* no arg expected */
        if (arg_count > 0)
        {
            sprintf(msg_out,
                     "No extra argument expected for trigger type '%s': %u argument(s) found.",
                     tmpstr, arg_count);
            return EINVAL;
        }
    }
    else if (!strcasecmp(tmpstr, "user_usage"))
    {
        p_trigger_item->trigger_type = TRIG_CONDITION;
        p_trigger_item->target_type = TGT_USER;

        /* default: alert enabled if LW cannot be reached */
        p_trigger_item->alert_lw = true;

        /* optional arguments: user list */
        if (arg_count > 0)
        {
            p_trigger_item->list = (char **) calloc(arg_count, sizeof(char *));
            p_trigger_item->list_size = arg_count;
            for (i = 0; i < arg_count; i++)
            {
                p_trigger_item->list[i] = (char *) malloc(strlen(arg_tab[i]) + 1);
                strcpy(p_trigger_item->list[i], arg_tab[i]);
            }
        }
    }
    else if (!strcasecmp(tmpstr, "group_usage"))
    {
        p_trigger_item->trigger_type = TRIG_CONDITION;
        p_trigger_item->target_type = TGT_GROUP;

        /* default: alert enabled if LW cannot be reached */
        p_trigger_item->alert_lw = true;

        /* optional argument: group list */
        if (arg_count > 0)
        {
            p_trigger_item->list = (char **) calloc(arg_count, sizeof(char *));
            p_trigger_item->list_size = arg_count;
            for (i = 0; i < arg_count; i++)
            {
                p_trigger_item->list[i] = (char *) malloc(strlen(arg_tab[i]) + 1);
                strcpy(p_trigger_item->list[i], arg_tab[i]);
            }
        }
    }
    else if (!strcasecmp(tmpstr, "pool_usage"))
    {
        p_trigger_item->trigger_type = TRIG_CONDITION;
        p_trigger_item->target_type = TGT_POOL;

        /* default: alert enabled if LW cannot be reached */
        p_trigger_item->alert_lw = true;

        /* optional arguments: user list */
        if (arg_count > 0)
        {
            p_trigger_item->list = (char **) calloc(arg_count, sizeof(char *));
            p_trigger_item->list_size = arg_count;
            for (i = 0; i < arg_count; i++)
            {
                p_trigger_item->list[i] = (char *) malloc(strlen(arg_tab[i]) + 1);
                strcpy(p_trigger_item->list[i], arg_tab[i]);
            }
        }
    }
    else
    {
        sprintf(msg_out, "Unexpected value for 'trigger_on' parameter: %s.", tmpstr);
        return EINVAL;
    }


    /* retrieve all threshold params and check their compatibility */
    high_count = low_count = 0;

    rc_hp = GetFloatParam(config_blk, block_name, "high_threshold_pct",
                          PFLG_POSITIVE | PFLG_ALLOW_PCT_SIGN, &h_pct,
                          NULL, NULL, msg_out);
    if ((rc_hp != 0) && (rc_hp != ENOENT))
        return rc_hp;
    else if (rc_hp != ENOENT)
        high_count++;

    rc_hv = GetSizeParam(config_blk, block_name, "high_threshold_vol",
                        PFLG_POSITIVE, &h_vol, NULL, NULL, msg_out);
    if ((rc_hv != 0) && (rc_hv != ENOENT))
        return rc_hv;
    else if (rc_hv != ENOENT)
        high_count++;

    rc_hc = GetInt64Param(config_blk, block_name, "high_threshold_cnt",
                          PFLG_POSITIVE, &h_cnt, NULL, NULL, msg_out);
    if ((rc_hc != 0) && (rc_hc != ENOENT))
        return rc_hc;
    else if (rc_hc != ENOENT)
        high_count++;

    rc_lp = GetFloatParam(config_blk, block_name, "low_threshold_pct",
                         PFLG_POSITIVE | PFLG_ALLOW_PCT_SIGN, &l_pct,
                         NULL, NULL, msg_out);
    if ((rc_lp != 0) && (rc_lp != ENOENT))
        return rc_lp;
    else if (rc_lp != ENOENT)
        low_count++;

    rc_lv = GetSizeParam(config_blk, block_name, "low_threshold_vol",
                        PFLG_POSITIVE, &l_vol, NULL, NULL, msg_out);
    if ((rc_lv != 0) && (rc_lv != ENOENT))
        return rc_lv;
    else if (rc_lv != ENOENT)
        low_count++;

    rc_lc = GetInt64Param(config_blk, block_name, "low_threshold_cnt",
                           PFLG_POSITIVE, &l_cnt, NULL, NULL, msg_out);
    if ((rc_lc != 0) && (rc_lc != ENOENT))
        return rc_lc;
    else if (rc_lc != ENOENT)
        low_count++;

    if (p_trigger_item->trigger_type == TRIG_ALWAYS)
    {
        /* in case of 'periodic' trigger, no thresholds are expected */
        if ((high_count > 0) || (low_count > 0))
        {
            strcpy(msg_out,
                    "No high/low threshold expected for trigger type 'periodic'");
            return EINVAL;
        }
    }
    else if (high_count > 1)
    {
        strcpy(msg_out, "Multiple purge start conditions in trigger.");
        return EINVAL;
    }
    else if (low_count > 1)
    {
        strcpy(msg_out, "Multiple purge stop conditions in trigger.");
        return EINVAL;
    }
    else if (high_count == 0)
    {
        strcpy(msg_out, "No purge start condition found in trigger "
                         "(mandatory). 'high_threshold_pct', 'high_threshold_vol'"
                         "or 'high_threshold_cnt' expected");
        return ENOENT;
    }
    else if (low_count == 0)
    {
        strcpy(msg_out, "No purge stop condition found in trigger "
                         "(mandatory). 'low_threshold_pct', 'low_threshold_vol'"
                         "or 'low_threshold_cnt' expected");
        return ENOENT;
    }
    else if (rc_hc != rc_lc) /* both 0 or both ENOENT */
    {
        strcpy(msg_out, "Incompatible threshold types: 'high_threshold_cnt' "
                         "must be used with 'low_threshold_cnt'");
        return ENOENT;
    }

    /* NOTE: count threshold for HSM systems only match online files (not released)*/

    /* count threshold is only on global usage */
    if ((p_trigger_item->trigger_type != TGT_FS)
         && (p_trigger_item->target_type != TGT_USER)
         && (p_trigger_item->target_type != TGT_GROUP)
         && ((rc_hc == 0) || (rc_lc == 0)))
    {
        strcpy(msg_out, "Threshold on entry count is only supported "
                         "for 'global_usage', 'user_usage' and 'group_usage' triggers");
        return EINVAL;
    }

    if (rc_hp == 0)
    {
        p_trigger_item->hw_type = PCT_THRESHOLD;
        p_trigger_item->hw_percent = h_pct;
    }
    else if (rc_hv == 0)
    {
        p_trigger_item->hw_type = VOL_THRESHOLD;
        p_trigger_item->hw_volume = h_vol;
    }
    else if (rc_hc == 0)
    {
        p_trigger_item->hw_type = COUNT_THRESHOLD;
        p_trigger_item->hw_count = h_cnt;
    }

    if (rc_lp == 0)
    {
        p_trigger_item->lw_type = PCT_THRESHOLD;
        p_trigger_item->lw_percent = l_pct;
    }
    else if (rc_lv == 0)
    {
        p_trigger_item->lw_type = VOL_THRESHOLD;
        p_trigger_item->lw_volume = l_vol;
    }
    else if (rc_lc == 0)
    {
        p_trigger_item->lw_type = COUNT_THRESHOLD;
        p_trigger_item->lw_count = l_cnt;
    }

    /* retrieve scalar parameters */
    rc = read_scalar_params(config_blk, block_name, cfg_params, msg_out);
    if (rc)
        return rc;

    CheckUnknownParameters(config_blk, block_name, trigger_expect);

    return 0;
}

#define critical_err_check(_ptr_, _blkname_) do { if (!_ptr_) {\
                                        sprintf( msg_out, "Internal error reading %s block in config file", _blkname_); \
                                        return EFAULT; \
                                    }\
                                } while (0)

static int polrun_read_config(config_file_t config, const char *policy_name,
                              policy_run_config_t *conf,
                              char *msg_out, int for_reload)
{
    int            rc;
    unsigned int   blc_index;
    char           block_name[1024];
    char           tmp[1024];
    config_item_t  param_block;

    /* parameter for CheckUnknownParams() */
    static const char *allowed[] = {
        "lru_sort_attr", "default_action", "max_action_count",
        "max_action_volume", "nb_threads", "suspend_error_pct",
        "suspend_error_min", "report_interval", "action_timeout",
        "check_actions_interval", "check_actions_on_startup",
        "recheck_ignored_classes",
        "pre_maintenance_window", "maint_min_apply_delay", "queue_size",
        "db_result_size_max",
        NULL
    };

    /* parameter for read_scalar_params() */
    const cfg_param_t cfg_params[] = {
        {"max_action_count",    PT_INT,     PFLG_POSITIVE,
            &conf->max_action_nbr, 0},
        {"max_action_volume",   PT_SIZE,    PFLG_POSITIVE,
            &conf->max_action_vol, 0},
        {"nb_threads",          PT_INT,     PFLG_POSITIVE | PFLG_NOT_NULL,
            &conf->nb_threads, 0},
        {"suspend_error_pct",   PT_FLOAT,   PFLG_POSITIVE | PFLG_ALLOW_PCT_SIGN,
            &conf->suspend_error_pct, 0},
        {"suspend_error_min",   PT_INT,      PFLG_POSITIVE,
            &conf->suspend_error_min, 0},
        {"report_interval",     PT_DURATION, PFLG_POSITIVE | PFLG_NOT_NULL,
            &conf->report_interval, 0},
        {"action_timeout",      PT_DURATION, PFLG_POSITIVE,
            &conf->action_timeout, 0},
        {"check_actions_interval",      PT_DURATION, PFLG_POSITIVE,
            &conf->check_action_status_delay, 0},
        {"check_actions_on_startup",    PT_BOOL,     0,
            &conf->check_action_status_on_startup, 0},
        {"recheck_ignored_classes",     PT_BOOL,     0,
            &conf->recheck_ignored_classes, 0},
        {"pre_maintenance_window",      PT_DURATION, PFLG_POSITIVE | PFLG_NOT_NULL,
            &conf->pre_maintenance_window, 0},
        {"maint_min_apply_delay",       PT_DURATION, PFLG_POSITIVE,
            &conf->maint_min_apply_delay, 0},
        {"queue_size",          PT_INT, PFLG_POSITIVE | PFLG_NOT_NULL,
            &conf->queue_size, 0},
        {"db_result_size_max",  PT_INT, PFLG_POSITIVE,
            &conf->db_request_limit, 0},

        {NULL, 0, 0, NULL, 0}
    };
    snprintf(block_name, sizeof(block_name), "%s"PARAM_SUFFIX, policy_name);

    /* get <policy>_parameters block */
    /* @TODO RBHv3: be able to aggregate multiple blocks with the same name */
    param_block = rh_config_FindItemByName(config, block_name);

    /* default_action is mandatory */
    if (param_block == NULL)
    {
        sprintf(msg_out, "Missing mandatory block '%s' (in particular, "
                "'default_action' parameter must be specified)", block_name);
        return ENOENT;
    }

    /* check this is a block... */
    if (rh_config_ItemType(param_block) != CONFIG_ITEM_BLOCK)
    {
        sprintf(msg_out, "A block is expected for configuration item '%s'", block_name);
        return EINVAL;
    }

    /* read all scalar params */
    rc = read_scalar_params(param_block, block_name, cfg_params, msg_out);
    if (rc)
        return rc;

    /* read specific parameters */
    rc = GetStringParam(param_block, block_name, "lru_sort_attr",
                        PFLG_NO_WILDCARDS, tmp, sizeof(tmp), NULL, NULL,
                        msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    else if (rc != ENOENT) {
        /* is it a time attribute? */
#ifdef ATTR_INDEX_last_archive
        if (!strcasecmp(tmp, criteria2str(CRITERIA_LAST_ARCHIVE)))
            conf->lru_sort_attr = ATTR_INDEX_last_archive;
        else
#endif
        if (!strcasecmp(tmp, criteria2str(CRITERIA_LAST_ACCESS)))
            conf->lru_sort_attr = ATTR_INDEX_last_access;
        else if (!strcasecmp(tmp, criteria2str(CRITERIA_LAST_MOD)))
            conf->lru_sort_attr = ATTR_INDEX_last_mod;
#ifdef ATTR_INDEX_creation_time
        else if (!strcasecmp(tmp, criteria2str(CRITERIA_CREATION)))
            conf->lru_sort_attr = ATTR_INDEX_creation_time;
#endif
        else {
            strcpy(msg_out, "time attribute expected for 'lru_sort_attr': creation, last_access, last_mod, last_archive...");
            return EINVAL;
        }
    }

    char ** extra = NULL;
    unsigned int extra_cnt = 0;
    rc = GetStringParam(param_block, block_name, "default_action",
                        PFLG_NO_WILDCARDS | PFLG_MANDATORY,
                        tmp, sizeof(tmp), &extra, &extra_cnt, msg_out);
    if (rc)
        return rc;
    if (!strcasecmp(tmp, "cmd"))
    {
        /* external command */
        /* 1 single argument expected */
        if (extra_cnt != 1)
        {
            strcpy(msg_out, "A single argument is expected for cmd. E.g.: default_action = cmd(\"myscript.sh\");");
            return EINVAL;
        }
        /* absolute path expected */
        else if (extra[0][0] != '/')
        {
            strcpy(msg_out, "An absolute path is expected for default_action::cmd");
            return EINVAL;
        }
        rh_strncpy(conf->action_u.command, extra[0], sizeof(conf->action_u.command));
        conf->action_type = ACTION_COMMAND;
    }
    else
    {
        if (extra_cnt != 0)
        {
            strcpy(msg_out, "No extra argument is expected for default_action");
            return EINVAL;
        }
        conf->action_u.function = action_name2function(tmp);
        if (conf->action_u.function == NULL)
        {
            sprintf(msg_out, "default_action: unknown function '%s'", tmp);
            return EINVAL;
        }
        conf->action_type = ACTION_FUNCTION; 
    }

    /* warn for unknown parameters */
    CheckUnknownParameters(param_block, block_name, allowed);

    /* get TRIGGER blocks */
    snprintf(block_name, sizeof(block_name), "%s"TRIGGER_SUFFIX, policy_name);

    for (blc_index = 0; blc_index < rh_config_GetNbBlocks(config); blc_index++)
    {
        char          *curr_bname;
        config_item_t  curr_item = rh_config_GetBlockByIndex(config, blc_index);
        critical_err_check(curr_item, "root");

        if (rh_config_ItemType(curr_item) != CONFIG_ITEM_BLOCK)
            continue;

        curr_bname = rh_config_GetBlockName(curr_item);
        critical_err_check(curr_bname, "root");

        if (!strcasecmp(curr_bname, block_name))
        {
            if (conf->trigger_count == 0)
                conf->trigger_list = (trigger_item_t *)malloc(sizeof(trigger_item_t));
            else
                conf->trigger_list =
                    (trigger_item_t *)realloc(conf->trigger_list,
                                              (conf->trigger_count + 1)
                                              * sizeof(trigger_item_t));
            conf->trigger_count++;

            /* analyze trigger block */
            rc = parse_trigger_block(curr_item, curr_bname,
                                   &conf->trigger_list[conf->trigger_count - 1],
                                   msg_out);
            if (rc)
                return rc;
        }
    }
    return 0;
}

/* read the run cfg for all policies */
int policy_run_cfg_read(config_file_t config, void *module_config, char *msg_out,
                        bool for_reload)
{
    int i, rc = 0;
    policy_run_configs_t *allconf = (policy_run_configs_t *)module_config;
    policies_t *parsed_policies = runcfg2policies(module_config);

    /* allconf->count is supposed to be set by set_default and configs must be allocated.
     * double check by comparing policy count and policy_run count */
    if (allconf->count != parsed_policies->policy_count)
        RBH_BUG("Unexpected policy_run_cfg count != policy count");

    for (i = 0; i < allconf->count; i++)
    {
        rc = polrun_read_config(config, parsed_policies->policy_list[i].name,
                                &allconf->configs[i], msg_out, for_reload);
        if (rc)
            return rc;
    }
    return rc;
}

#define NO_TRIG_UPDT_MSG(_what) DisplayLog(LVL_MAJOR, TAG, _what \
        " changed in config file but cannot be modified dynamically: trigger update cancelled")

static void update_triggers(trigger_item_t *trigger_tgt, unsigned int count_tgt,
                            trigger_item_t *trigger_new, unsigned int count_new,
                            bool *check_interval_chgd)
{
    unsigned int   i;
    *check_interval_chgd = false;

    if (count_new != count_tgt)
    {
        /* skip trigger checking & update */
        NO_TRIG_UPDT_MSG("Trigger count");
        return;
    }

    /* check trigger types */
    for (i = 0; i < count_new; i++)
    {
        if (trigger_new[i].trigger_type != trigger_tgt[i].trigger_type
            || trigger_new[i].target_type != trigger_tgt[i].target_type)
        {
            NO_TRIG_UPDT_MSG("Trigger type");
            return;
        }
        else if ((trigger_new[i].trigger_type != TRIG_ALWAYS) &&
                  (trigger_new[i].hw_type != trigger_tgt[i].hw_type))
        {
            NO_TRIG_UPDT_MSG("High threshold type");
            return;
        }
        else if ((trigger_new[i].trigger_type != TRIG_ALWAYS) &&
                  (trigger_new[i].lw_type != trigger_tgt[i].lw_type))
        {
            NO_TRIG_UPDT_MSG("Low threshold type");
            return;
        }
    }

    /* triggers have the same type: update simple parameters:
     * max_action_count, max_action_volume, check_interval, alert_high, alert_low, post_trigger_wait */
    for (i = 0; i < count_new; i++)
    {
        char tname[256];
        snprintf(tname, sizeof(tname), "#%u (%s): ", i, trigger2str(&trigger_tgt[i]));

        if (trigger_new[i].check_interval != trigger_tgt[i].check_interval)
        {
            DisplayLog(LVL_EVENT, TAG, "check_interval updated for trigger %s: %lu->%lu",
                       tname, trigger_tgt[i].check_interval, trigger_new[i].check_interval);
            trigger_tgt[i].check_interval = trigger_new[i].check_interval;
            *check_interval_chgd = true;
        }

        if (trigger_new[i].max_action_nbr != trigger_tgt[i].max_action_nbr)
        {
            DisplayLog(LVL_EVENT, TAG, "max_action_count updated for trigger %s: %u entries ->%u entries",
                       tname, trigger_tgt[i].max_action_nbr, trigger_new[i].max_action_nbr);
            trigger_tgt[i].max_action_nbr = trigger_new[i].max_action_nbr;
        }

        if (trigger_new[i].max_action_vol != trigger_tgt[i].max_action_vol)
        {
            DisplayLog(LVL_EVENT, TAG, "max_action_volume updated for trigger %s: %llu bytes->%llu bytes",
                       tname, trigger_tgt[i].max_action_vol, trigger_new[i].max_action_vol);
            trigger_tgt[i].max_action_vol = trigger_new[i].max_action_vol;
        }

        if (trigger_new[i].post_trigger_wait != trigger_tgt[i].post_trigger_wait)
        {
            DisplayLog(LVL_EVENT, TAG, "post_trigger_wait updated for trigger %s: %lu->%lu",
                       tname, trigger_tgt[i].post_trigger_wait, trigger_new[i].post_trigger_wait);
            trigger_tgt[i].post_trigger_wait = trigger_new[i].post_trigger_wait;
        }

        if (trigger_new[i].alert_hw != trigger_tgt[i].alert_hw)
        {
            DisplayLog(LVL_EVENT, TAG, "alert_high updated for trigger %s: %s->%s",
                       tname, bool2str(trigger_tgt[i].alert_hw),
                       bool2str(trigger_new[i].alert_hw));
            trigger_tgt[i].alert_hw = trigger_new[i].alert_hw;
        }

        if (trigger_new[i].alert_lw != trigger_tgt[i].alert_lw)
        {
            DisplayLog(LVL_EVENT, TAG, "alert_low updated for trigger %s: %s->%s",
                       tname, bool2str(trigger_tgt[i].alert_lw),
                       bool2str(trigger_new[i].alert_lw));
            trigger_tgt[i].alert_lw = trigger_new[i].alert_lw;
        }

        if (trigger_new[i].trigger_type == TRIG_ALWAYS)
            /* no threshold for 'periodic' triggers */
            continue;

        switch (trigger_new[i].hw_type)
        {
        case PCT_THRESHOLD:
            if (trigger_new[i].hw_percent != trigger_tgt[i].hw_percent)
            {
                DisplayLog(LVL_EVENT, TAG, "High threshold updated for trigger %s: "
                           "%.2f%%->%.2f%%", tname, trigger_tgt[i].hw_percent, trigger_new[i].hw_percent);
                trigger_tgt[i].hw_percent = trigger_new[i].hw_percent;
            }
            break;

        case VOL_THRESHOLD:
            if (trigger_new[i].hw_volume != trigger_tgt[i].hw_volume)
            {
                DisplayLog(LVL_EVENT, TAG,
                           "High threshold updated for trigger %s: %llu bytes->%llu bytes",
                           tname, trigger_tgt[i].hw_volume, trigger_new[i].hw_volume);
                trigger_tgt[i].hw_volume = trigger_new[i].hw_volume;
            }
            break;

        case COUNT_THRESHOLD:
            if (trigger_new[i].hw_count != trigger_tgt[i].hw_count)
            {
                DisplayLog(LVL_EVENT, TAG,
                           "High threshold updated for trigger %s: %llu entries ->%llu entries",
                           tname, trigger_tgt[i].hw_count, trigger_new[i].hw_count);
                trigger_tgt[i].hw_count = trigger_new[i].hw_count;
            }
            break;
        }

        switch (trigger_new[i].lw_type)
        {
        case PCT_THRESHOLD:
            if (trigger_new[i].lw_percent != trigger_tgt[i].lw_percent)
            {
                DisplayLog(LVL_EVENT, TAG,
                           "Low threshold updated for trigger %s: %.2f%%->%.2f%%", tname,
                           trigger_tgt[i].lw_percent, trigger_new[i].lw_percent);
                trigger_tgt[i].lw_percent = trigger_new[i].lw_percent;
            }
            break;

        case VOL_THRESHOLD:
            if (trigger_new[i].lw_volume != trigger_tgt[i].lw_volume)
            {
                DisplayLog(LVL_EVENT, TAG,
                            "Low threshold updated for trigger %s: %llu bytes->%llu bytes", tname,
                            trigger_tgt[i].lw_volume, trigger_new[i].lw_volume);
                trigger_tgt[i].lw_volume = trigger_new[i].lw_volume;
            }
            break;

        case COUNT_THRESHOLD:
            if (trigger_new[i].lw_count != trigger_tgt[i].lw_count)
            {
                DisplayLog(LVL_EVENT, TAG,
                           "Low threshold updated for trigger %s: %llu entries->%llu entries", tname,
                           trigger_tgt[i].lw_count, trigger_new[i].lw_count);
                trigger_tgt[i].lw_count = trigger_new[i].lw_count;
            }
            break;
        }
    }

    /* update global interval check (GCD of all check intervals) if one of them changed */
// TODO move to caller
//    if (check_interval_chgd)
//        ResMon_UpdateCheckInterval();

    /* triggers have been updated */
    return;
}

static void free_triggers(trigger_item_t * p_triggers, unsigned int count)
{
    unsigned int   i, j;
    for (i = 0; i < count; i++)
    {
        if ((p_triggers[i].list_size > 0) && (p_triggers[i].list != NULL))
        {
            /* free the strings */
            for (j = 0; j < p_triggers[i].list_size; j++)
            {
                if (p_triggers[i].list[j] != NULL)
                    free(p_triggers[i].list[j]);
            }

            /* free the arg list */
            free(p_triggers[i].list);
        }
    }

    /* free the trigger list */
    if ((count > 0) && (p_triggers != NULL))
        free(p_triggers);
}

#define NO_PARAM_UPDT_MSG(_blk, _name) DisplayLog(LVL_MAJOR, TAG, "%s::%s"    \
                " changed in config file, but cannot be modified dynamically", \
                 _blk, _name)
#define PARAM_UPDT_MSG(_blk, _name, _format, _v1, _v2) DisplayLog(LVL_EVENT,  \
                TAG, "%s::%s updated: "_format"->"_format, _blk, _name, _v1, _v2)

/** reload parameters for a single policy */
static int polrun_reload(const char *blkname, policy_run_config_t *cfg_tgt,
                         policy_run_config_t *cfg_new, bool *recompute_interval)
{
    /* parameters that can't be modified dynamically */
    if (cfg_tgt->nb_threads != cfg_new->nb_threads)
        NO_PARAM_UPDT_MSG(blkname, "nb_threads");

    if (cfg_tgt->queue_size != cfg_new->queue_size)
        NO_PARAM_UPDT_MSG(blkname, "queue_size");

// FIXME can change action functions, but not cmd string
//    if (strcmp(cfg_new->default_action, cfg_tgt->default_action))
//        NO_PARAM_UPDT_MSG(blkname, "default_action");

    if (cfg_tgt->lru_sort_attr != cfg_new->lru_sort_attr)
        NO_PARAM_UPDT_MSG(blkname, "lru_sort_attr");

    /* dynamic parameters */
    if (cfg_tgt->max_action_nbr != cfg_new->max_action_nbr)
    {
        PARAM_UPDT_MSG(blkname, "max_action_count", "%u",
                       cfg_tgt->max_action_nbr, cfg_new->max_action_nbr);
        cfg_tgt->max_action_nbr = cfg_new->max_action_nbr;
    }

    if (cfg_tgt->max_action_vol != cfg_new->max_action_vol)
    {
        PARAM_UPDT_MSG(blkname, "max_action_volume", "%llu",
                       cfg_tgt->max_action_vol, cfg_new->max_action_vol);
        cfg_tgt->max_action_vol = cfg_new->max_action_vol;
    }

    if (cfg_tgt->suspend_error_pct != cfg_new->suspend_error_pct)
    {
        PARAM_UPDT_MSG(blkname, "suspend_error_pct", "%.2f%%",
                       cfg_tgt->suspend_error_pct, cfg_new->suspend_error_pct);
        cfg_tgt->suspend_error_pct = cfg_new->suspend_error_pct;
    }

    if (cfg_tgt->suspend_error_min != cfg_new->suspend_error_min)
    {
        PARAM_UPDT_MSG(blkname, "suspend_error_min", "%u",
                       cfg_tgt->suspend_error_min, cfg_new->suspend_error_min);
        cfg_tgt->suspend_error_min = cfg_new->suspend_error_min;
    }

    if (cfg_tgt->report_interval != cfg_new->report_interval)
    {
        PARAM_UPDT_MSG(blkname, "report_interval", "%lu",
                       cfg_tgt->report_interval, cfg_new->report_interval);
        cfg_tgt->report_interval = cfg_new->report_interval;
    }

    if (cfg_tgt->action_timeout != cfg_new->action_timeout)
    {
        PARAM_UPDT_MSG(blkname, "action_timeout", "%lu",
                       cfg_tgt->action_timeout, cfg_new->action_timeout);
        cfg_tgt->action_timeout = cfg_new->action_timeout;
    }

    if (cfg_tgt->check_action_status_delay != cfg_new->check_action_status_delay)
    {
        PARAM_UPDT_MSG(blkname, "check_actions_interval", "%lu",
                       cfg_tgt->check_action_status_delay,
                       cfg_new->check_action_status_delay);
        cfg_tgt->check_action_status_delay = cfg_new->check_action_status_delay;
    }

    if (cfg_tgt->db_request_limit != cfg_new->db_request_limit)
    {
        PARAM_UPDT_MSG(blkname, "db_result_size_max", "%u",
                       cfg_tgt->db_request_limit, cfg_new->db_request_limit);
        cfg_tgt->db_request_limit = cfg_new->db_request_limit;
    }

    if (cfg_tgt->pre_maintenance_window != cfg_new->pre_maintenance_window)
    {
        PARAM_UPDT_MSG(blkname, "pre_maintenance_window", "%lu",
                       cfg_tgt->pre_maintenance_window, cfg_new->pre_maintenance_window);
        cfg_tgt->pre_maintenance_window = cfg_new->pre_maintenance_window;
    }

    if (cfg_tgt->maint_min_apply_delay != cfg_new->maint_min_apply_delay)
    {
        PARAM_UPDT_MSG(blkname, "maint_min_apply_delay", "%lu",
                       cfg_tgt->maint_min_apply_delay, cfg_new->maint_min_apply_delay);
        cfg_tgt->maint_min_apply_delay = cfg_new->maint_min_apply_delay;
    }

    if (cfg_tgt->check_action_status_on_startup != cfg_new->check_action_status_on_startup)
    {
        PARAM_UPDT_MSG(blkname, "check_actions_on_startup", "%s",
                       bool2str(cfg_tgt->check_action_status_on_startup),
                       bool2str(cfg_new->check_action_status_on_startup));
        cfg_tgt->check_action_status_on_startup = cfg_new->check_action_status_on_startup;
    }

    if (cfg_tgt->recheck_ignored_classes != cfg_new->recheck_ignored_classes)
    {
        PARAM_UPDT_MSG(blkname, "recheck_ignored_classes", "%s",
                       bool2str(cfg_tgt->recheck_ignored_classes),
                       bool2str(cfg_new->recheck_ignored_classes));
        cfg_tgt->recheck_ignored_classes = cfg_new->recheck_ignored_classes;
    }

    update_triggers(cfg_tgt->trigger_list, cfg_tgt->trigger_count,
                    cfg_new->trigger_list, cfg_new->trigger_count,
                    recompute_interval);
    free_triggers(cfg_new->trigger_list, cfg_new->trigger_count);

    return 0;
}

/** reload cfg for all policies */
int policy_run_cfg_reload(void *module_config)
{
    int i, rc;
    int err = 0;
    policy_run_configs_t *allconf = (policy_run_configs_t *)module_config;

    if (allconf->count != run_cfgs.count)
    {
        DisplayLog(LVL_MAJOR, TAG, "New policy count doesn't match previous "
                   "policy count (%u vs %u): skipping config update.",
                   allconf->count, run_cfgs.count);
        return 0;
    }
    for (i = 0; i < allconf->count; i++)
    {
        bool chgd = false;
        char block_name[256];
        const char *pname = policies.policy_list[i].name;
        snprintf(block_name, sizeof(block_name), "%s"PARAM_SUFFIX, pname);

        rc = polrun_reload(block_name, &run_cfgs.configs[i],
                           &allconf->configs[i], &chgd);
        if (rc)
        {
             DisplayLog(LVL_MAJOR, TAG, "Failed to reload parameters for policy %s (rc=%d)",
                        pname, rc);
            if (rc > err)
                err = rc;
        }
        else
        {
            DisplayLog(LVL_DEBUG, TAG, "Successfully reloaded config for policy %s",
                       pname);
        }
    }

    /* policy runs may not be in the same order as policies and run_cfgs */
//    FIXME RBHv3
//    if (chgd && policy_runs.runs != NULL)
//        policy_module_update_check_interval(&policy_runs.runs[i]);

    return err;
}
