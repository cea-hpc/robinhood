/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2007, 2008, 2009 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
/**
 * FS scan stop/start routines
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "fs_scan_main.h"
#include "fs_scan.h"
#include "rbh_misc.h"
#include "rbh_logs.h"
#include "rbh_cfg_helpers.h"
#include <pthread.h>
#include <errno.h>
#include <unistd.h>

static pthread_t scan_starter_thread;
static pthread_attr_t starter_attr;
static bool terminate = false;

/* Scan starter thread */
static void *scan_starter(void *arg)
{
    int rc;

    DisplayLog(LVL_VERB, FSSCAN_TAG, "Launching FS Scan starter thread");

    if (fsscan_flags & RUNFLG_ONCE) {
        rc = Robinhood_CheckScanDeadlines();
        if (rc)
            DisplayLog(LVL_CRIT, FSSCAN_TAG, "Error %d checking FS Scan status",
                       rc);
        pthread_exit(NULL);
        return NULL;
    }

    /* not a one-shot mode */
    while (!terminate) {
        rc = Robinhood_CheckScanDeadlines();
        if (rc)
            DisplayLog(LVL_CRIT, FSSCAN_TAG, "Error %d checking FS Scan status",
                       rc);

        /* attente de la boucle suivante */
        rh_sleep(fs_scan_config.spooler_check_interval);
    }

    return NULL;
}

/** Start FS Scan info collector */
int FSScan_Start(run_flags_t flags, const char *partial_root)
{
    int rc;

    fsscan_flags = flags;
    partial_scan_root = partial_root;

    if (partial_root) {
        /* check that partial_root is under FS root */
        if (strncmp
            (global_config.fs_path, partial_scan_root,
             strlen(global_config.fs_path))) {
            DisplayLog(LVL_CRIT, FSSCAN_TAG,
                       "ERROR scan root %s is not under fs root %s",
                       partial_scan_root, global_config.fs_path);
            return EINVAL;
        }
    }

    rc = Robinhood_InitScanModule();
    if (rc)
        return rc;

    /* start a background thread */

    pthread_attr_init(&starter_attr);
    pthread_attr_setscope(&starter_attr, PTHREAD_SCOPE_SYSTEM);

    if (pthread_create(&scan_starter_thread, &starter_attr, scan_starter,
                       NULL))
        return errno;

    return 0;
}

/** Wait for scan termination */
void FSScan_Wait(void)
{
    wait_scan_finished();
}

/** Stop FS Scan info collector */
void FSScan_Terminate(void)
{   /* @TODO */
    terminate = true;

    Robinhood_StopScanModule();
}

/** Store FS Scan into database */
void FSScan_StoreStats(lmgr_t *lmgr)
{
    robinhood_fsscan_stat_t stats;
    char tmp_buff[256];

    Robinhood_StatsScan(&stats);

    /* store the number of scanning threads */
    sprintf(tmp_buff, "%i", fs_scan_config.nb_threads_scan);
    ListMgr_SetVar(lmgr, LAST_SCAN_NB_THREADS, tmp_buff);

    if (stats.scan_running) {
        if (stats.last_action > 0) {
            sprintf(tmp_buff, "%lu", (unsigned long)stats.last_action);
            ListMgr_SetVar(lmgr, LAST_SCAN_LAST_ACTION_TIME, tmp_buff);
        }

        if (stats.scanned_entries) {
            sprintf(tmp_buff, "%u", stats.scanned_entries);
            ListMgr_SetVar(lmgr, LAST_SCAN_ENTRIES_SCANNED, tmp_buff);
            sprintf(tmp_buff, "%u", stats.error_count);
            ListMgr_SetVar(lmgr, LAST_SCAN_ERRORS, tmp_buff);
            sprintf(tmp_buff, "%.2f", stats.avg_ms_per_entry);
            ListMgr_SetVar(lmgr, LAST_SCAN_AVGMSPE, tmp_buff);
            sprintf(tmp_buff, "%.2f", stats.curr_ms_per_entry);
            ListMgr_SetVar(lmgr, LAST_SCAN_CURMSPE, tmp_buff);
        }
    }
    sprintf(tmp_buff, "%u", stats.nb_hang);
    ListMgr_SetVar(lmgr, LAST_SCAN_TIMEOUTS, tmp_buff);

}

/** Dump FS Scan stats to log file */
void FSScan_DumpStats(void)
{
    robinhood_fsscan_stat_t stats;
    struct tm paramtm;
    char tmp_buff[256];
    char tmp_buff2[256];

    Robinhood_StatsScan(&stats);

    DisplayLog(LVL_MAJOR, "STATS", "======== FS scan statistics =========");

    if (stats.last_fsscan_time != 0) {
        strftime(tmp_buff, 256, "%Y/%m/%d %T",
                 localtime_r(&stats.last_fsscan_time, &paramtm));

        DisplayLog(LVL_MAJOR, "STATS", "last scan  = %s", tmp_buff);

        FormatDuration(tmp_buff, 256, stats.last_duration);

        DisplayLog(LVL_MAJOR, "STATS", "duration    = %s (%u s)", tmp_buff,
                   stats.last_duration);
        DisplayLog(LVL_MAJOR, "STATS", "status      = %s",
                   (stats.scan_complete ? "complete" : "incomplete"));
    }

    if (stats.current_scan_interval != 0) {
        FormatDurationFloat(tmp_buff, 256, stats.current_scan_interval);
        DisplayLog(LVL_MAJOR, "STATS", "current scan interval = %s", tmp_buff);
    }

    if (stats.scan_running) {
        time_t now = time(NULL);

        DisplayLog(LVL_MAJOR, "STATS", "scan is running:");

        strftime(tmp_buff, 256, "%Y/%m/%d %T",
                 localtime_r(&stats.start_time, &paramtm));
        FormatDurationFloat(tmp_buff2, 256, now - stats.start_time);

        DisplayLog(LVL_MAJOR, "STATS", "     started at : %s (%s ago)",
                   tmp_buff, tmp_buff2);

        strftime(tmp_buff, 256, "%Y/%m/%d %T",
                 localtime_r(&stats.last_action, &paramtm));
        FormatDurationFloat(tmp_buff2, 256, now - stats.last_action);

        DisplayLog(LVL_MAJOR, "STATS", "     last action: %s (%s ago)",
                   tmp_buff, tmp_buff2);

        if (stats.scanned_entries) {
            double speed;

            DisplayLog(LVL_MAJOR, "STATS",
                       "     progress   : %u entries scanned (%u errors)",
                       stats.scanned_entries, stats.error_count);

            if (stats.curr_ms_per_entry > 0.0)
                speed =
                    (1000.0 / stats.curr_ms_per_entry) *
                    fs_scan_config.nb_threads_scan;
            else
                speed = 0.0;

            DisplayLog(LVL_MAJOR, "STATS",
                       "     inst. speed (potential): %9.2f entries/sec (%4.2f ms/entry/thread)",
                       speed, stats.curr_ms_per_entry);

            if (now - stats.start_time > 0)
                DisplayLog(LVL_MAJOR, "STATS",
                           "     avg. speed  (effective): %9.2f entries/sec (%4.2f ms/entry/thread)",
                           (float)stats.scanned_entries / (float)(now -
                                                                  stats.
                                                                  start_time),
                           stats.avg_ms_per_entry);
        }
    }

    if (stats.nb_hang > 0)
        DisplayLog(LVL_MAJOR, "STATS", "scan operation timeouts = %u",
                   stats.nb_hang);

}

/* ------------ Config management functions --------------- */

#define FSSCAN_CONFIG_BLOCK  "FS_Scan"
#define IGNORE_BLOCK  "Ignore"

#define MINUTE 60
#define HOUR 3600
#define DAY (24*HOUR)

static void fs_scan_cfg_set_default(void *module_config)
{
    fs_scan_config_t *conf = (fs_scan_config_t *) module_config;

#ifdef HAVE_CHANGELOGS
    /* scan rarely */
    conf->min_scan_interval = 7 * DAY;
    conf->max_scan_interval = 30 * DAY;
#else
    /* scan often */
    conf->min_scan_interval = 1 * DAY;
    conf->max_scan_interval = 7 * DAY;
#endif
    conf->scan_retry_delay = HOUR;
    conf->nb_threads_scan = 2;
    conf->scan_op_timeout = 0;
    conf->exit_on_timeout = false;
    conf->spooler_check_interval = MINUTE;
    conf->nb_prealloc_tasks = 256;

    conf->ignore_list = NULL;
    conf->ignore_count = 0;
    conf->dir_list = NULL;
    conf->completion_command = NULL;
}

static void fs_scan_cfg_write_default(FILE *output)
{
    print_begin_block(output, 0, FSSCAN_CONFIG_BLOCK, NULL);
#ifdef _LUSTRE_HSM
    print_line(output, 1, "min_scan_interval      :    7d");
    print_line(output, 1, "max_scan_interval      :   30d");
#else
    print_line(output, 1, "min_scan_interval      :    1d");
    print_line(output, 1, "max_scan_interval      :    7d");
#endif
    print_line(output, 1, "scan_retry_delay       :    1h");
    print_line(output, 1, "nb_threads_scan        :     2");
    print_line(output, 1, "scan_op_timeout        :     0 (disabled)");
    print_line(output, 1, "exit_on_timeout        :    no");
    print_line(output, 1, "spooler_check_interval :  1min");
    print_line(output, 1, "nb_prealloc_tasks      :   256");
    print_line(output, 1, "ignore                 :  NONE");
    print_line(output, 1, "dir_list               :  NONE");
    print_line(output, 1, "completion_command     :  NONE");
    print_end_block(output, 0);
}

/** add an item to the ignore list of the configuration */
static int add_ignore_item(fs_scan_config_t *conf, config_item_t item,
                           const char *blk_name, char *msg_out)
{
    conf->ignore_list = realloc(conf->ignore_list,
                           (conf->ignore_count + 1) * sizeof(whitelist_item_t));
    if (conf->ignore_list == NULL)
        return ENOMEM;

    conf->ignore_count++;

    /* analyze and fill boolean expression */
    return GetBoolExpr(item, blk_name,
                       &conf->ignore_list[conf->ignore_count - 1].bool_expr,
                       &conf->ignore_list[conf->ignore_count - 1].attr_mask,
                       msg_out, NULL);
}

/** add a directroy to the scan list of the configuration */
static int add_scan_dir(fs_scan_config_t *conf, const char *val,
                        const char *blk_name, char *msg_out)
{
    conf->dir_list = realloc(conf->dir_list,
                             (conf->dir_count + 1) * sizeof(char *));
    if (conf->dir_list == NULL)
        return ENOMEM;

    conf->dir_list[conf->dir_count] = strdup(val);
    conf->dir_count++;

    return 0;
}

#define critical_err_check(_ptr_, _blkname_) do { if (!_ptr_) {\
                    sprintf(msg_out, "Internal error reading %s block in " \
                            "config file", _blkname_); \
                    return EFAULT; \
                 }\
            } while (0)

static int fs_scan_cfg_read(config_file_t config, void *module_config,
                            char *msg_out)
{
    int rc, index;
    fs_scan_config_t *conf = (fs_scan_config_t *) module_config;
    bool scan_intl_set = false;
    time_t scan_intl = 0;
    config_item_t fsscan_block;

    static const char *fsscan_allowed[] = {
        "scan_interval", "min_scan_interval", "max_scan_interval",
        "scan_retry_delay", "nb_threads_scan", "scan_op_timeout",
        "exit_on_timeout", "spooler_check_interval", "nb_prealloc_tasks",
        "completion_command", "scan_only",
        IGNORE_BLOCK, NULL
    };

    const cfg_param_t cfg_params[] = {
        {"nb_threads_scan", PT_INT, PFLG_POSITIVE | PFLG_NOT_NULL,
         &conf->nb_threads_scan, 0},
        {"scan_retry_delay", PT_DURATION, PFLG_POSITIVE | PFLG_NOT_NULL,
         &conf->scan_retry_delay, 0},
        {"scan_op_timeout", PT_DURATION, PFLG_POSITIVE, &conf->scan_op_timeout,
         0},
        {"exit_on_timeout", PT_BOOL, 0, &conf->exit_on_timeout, 0},
        {"spooler_check_interval", PT_DURATION, PFLG_POSITIVE | PFLG_NOT_NULL,
         &conf->spooler_check_interval, 0},
        {"nb_prealloc_tasks", PT_INT, PFLG_POSITIVE | PFLG_NOT_NULL,
         &conf->nb_prealloc_tasks, 0},
        /* completion command can contain wildcards: {cfg}, {fspath} ... */
        {"completion_command", PT_CMD, 0,
         &conf->completion_command, 0},
        END_OF_PARAMS
    };

    /* get FS Scan block */
    rc = get_cfg_block(config, FSSCAN_CONFIG_BLOCK, &fsscan_block, msg_out);
    if (rc)
        return rc == ENOENT ? 0 : rc;   /* not mandatory */

    /* read scalar parameters */
    rc = read_scalar_params(fsscan_block, FSSCAN_CONFIG_BLOCK, cfg_params,
                            msg_out);
    if (rc)
        return rc;

    /* parameters with specific management */
    rc = GetDurationParam(fsscan_block, FSSCAN_CONFIG_BLOCK,
                          "min_scan_interval", PFLG_POSITIVE | PFLG_NOT_NULL,
                          &conf->min_scan_interval, NULL, NULL, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    else if (rc == 0)
        scan_intl_set = true;

    rc = GetDurationParam(fsscan_block, FSSCAN_CONFIG_BLOCK,
                          "max_scan_interval", PFLG_POSITIVE | PFLG_NOT_NULL,
                          &conf->max_scan_interval, NULL, NULL, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    else if (rc == 0)
        scan_intl_set = true;

    rc = GetDurationParam(fsscan_block, FSSCAN_CONFIG_BLOCK,
                          "scan_interval", PFLG_POSITIVE | PFLG_NOT_NULL,
                          &scan_intl, NULL, NULL, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    else if (rc == 0) {
        if (scan_intl_set) {
            strcpy(msg_out,
                   "scan_interval parameter cannot be used with min/max_scan_interval");
            return EINVAL;
        }
        conf->min_scan_interval = scan_intl;
        conf->max_scan_interval = scan_intl;
    }

    /* Find and parse "ignore" blocks and "scan_only" directives */
    for (index = 0; index < rh_config_GetNbItems(fsscan_block);
         index++) {
        config_item_t curr_item;
        int extra = 0;
        char *name;
        char *val;

        curr_item = rh_config_GetItemByIndex(fsscan_block, index);
        critical_err_check(curr_item, FSSCAN_CONFIG_BLOCK);

        switch (rh_config_ItemType(curr_item)) {
        case CONFIG_ITEM_VAR:
            rc = rh_config_GetKeyValue(curr_item, &name, &val, &extra);
            if (rc)
                return EINVAL;

            /* process only scan_only directives */
            if (strcasecmp(name, "scan_only") != 0)
                continue;

            rc = add_scan_dir(conf, val, name, msg_out);
            if (rc)
                return rc;
            break;

        case CONFIG_ITEM_BLOCK:
            name = rh_config_GetBlockName(curr_item);

            /* process only ignore blocks */
            if (strcasecmp(name, IGNORE_BLOCK) != 0)
                continue;

            rc = add_ignore_item(conf, curr_item, name, msg_out);
            if (rc)
                return rc;
            break;

        default:
            /* other cases: ignore */
            continue;
        }

    }   /* Loop on sub-items */

    CheckUnknownParameters(fsscan_block, FSSCAN_CONFIG_BLOCK, fsscan_allowed);

    return 0;
}

#define RELOAD_TAG  "FS_Scan_Config"

/** Update ignore rules */
static void update_ignore(whitelist_item_t *old_items, unsigned int old_count,
                          whitelist_item_t *new_items, unsigned int new_count,
                          const char *block_name)
{
    unsigned int i;

    if (old_count != new_count) {
        DisplayLog(LVL_MAJOR, RELOAD_TAG,
                   "Ignore rules count changed in block '%s' but cannot be modified dynamically: ignore update cancelled",
                   block_name);
        return;
    }

    /* compare ignore boolean expression structure */
    for (i = 0; i < new_count; i++) {
        if (!attr_mask_equal(&old_items[i].attr_mask, &new_items[i].attr_mask)
            || compare_boolexpr(&old_items[i].bool_expr,
                                &new_items[i].bool_expr)) {
            DisplayLog(LVL_MAJOR, RELOAD_TAG,
                       "Ignore expression #%u changed in block '%s'. "
                       "Only numerical values can be modified dynamically. "
                       "Skipping parameter update.", i, block_name);
            return;
        }
    }

    /* if they are all the same, update/check their values */

    for (i = 0; i < new_count; i++) {
        if (update_boolexpr(&old_items[i].bool_expr, &new_items[i].bool_expr)) {
            char criteriastr[2048];
            BoolExpr2str(&old_items[i].bool_expr, criteriastr, 2048);
            DisplayLog(LVL_EVENT, RELOAD_TAG,
                       "Ignore expression #%u in block '%s' has been updated and is now: %s",
                       i, block_name, criteriastr);
        }
    }

    /* XXX attr_mask is unchanged, since we keep the same expression
     *  structures */

}   /* end update_ignore */

static void free_ignore(whitelist_item_t *p_items, int count)
{
    int i;

    if (p_items == NULL)
        return;

    for (i = 0; i < count; i++)
        FreeBoolExpr(&p_items[i].bool_expr, false);

    free(p_items);
}

static void free_scan_dirs(char **list, int count)
{
    int i;

    if (list == NULL)
        return;

    /* last list item is NULL */
    for (i = 0;  i < count; i++)
        free(list[i]);

    free(list);
}

static int fs_scan_cfg_reload(fs_scan_config_t *conf)
{
    /* Parameters that can be modified dynamically */

    if (conf->min_scan_interval != fs_scan_config.min_scan_interval) {
        DisplayLog(LVL_EVENT, "FS_Scan_Config",
                   FSSCAN_CONFIG_BLOCK "::min_scan_interval updated: %ld->%ld",
                   fs_scan_config.min_scan_interval, conf->min_scan_interval);
        fs_scan_config.min_scan_interval = conf->min_scan_interval;
    }

    if (conf->max_scan_interval != fs_scan_config.max_scan_interval) {
        DisplayLog(LVL_EVENT, "FS_Scan_Config",
                   FSSCAN_CONFIG_BLOCK "::max_scan_interval updated: %ld->%ld",
                   fs_scan_config.max_scan_interval, conf->max_scan_interval);
        fs_scan_config.max_scan_interval = conf->max_scan_interval;
    }

    if (conf->scan_retry_delay != fs_scan_config.scan_retry_delay) {
        DisplayLog(LVL_EVENT, "FS_Scan_Config",
                   FSSCAN_CONFIG_BLOCK "::scan_retry_delay updated: %ld>%ld",
                   fs_scan_config.scan_retry_delay, conf->scan_retry_delay);
        fs_scan_config.scan_retry_delay = conf->scan_retry_delay;
    }

    if (conf->scan_op_timeout != fs_scan_config.scan_op_timeout) {
        DisplayLog(LVL_EVENT, "FS_Scan_Config",
                   FSSCAN_CONFIG_BLOCK "::scan_op_timeout updated: %ld->%ld",
                   fs_scan_config.scan_op_timeout, conf->scan_op_timeout);
        fs_scan_config.scan_op_timeout = conf->scan_op_timeout;
    }

    if (conf->exit_on_timeout != fs_scan_config.exit_on_timeout) {
        DisplayLog(LVL_EVENT, "FS_Scan_Config",
                   FSSCAN_CONFIG_BLOCK "::exit_on_timeout updated: %s->%s",
                   bool2str(fs_scan_config.exit_on_timeout),
                   bool2str(conf->exit_on_timeout));
        fs_scan_config.exit_on_timeout = conf->exit_on_timeout;
    }

    if (conf->spooler_check_interval != fs_scan_config.spooler_check_interval) {
        DisplayLog(LVL_EVENT, "FS_Scan_Config",
                   FSSCAN_CONFIG_BLOCK
                   "::spooler_check_interval updated: %ld->%ld",
                   fs_scan_config.spooler_check_interval,
                   conf->spooler_check_interval);
        fs_scan_config.spooler_check_interval = conf->spooler_check_interval;
    }

    if (compare_cmd
        (conf->completion_command, fs_scan_config.completion_command)) {
        DisplayLog(LVL_MAJOR, "FS_Scan_Config",
                   FSSCAN_CONFIG_BLOCK
                   "::completion_command changed in config file, but cannot be modified dynamically");
        g_strfreev(conf->completion_command);
        conf->completion_command = NULL;
    }

    /* Parameters that canNOT be modified dynamically */

    if (conf->nb_threads_scan != fs_scan_config.nb_threads_scan)
        DisplayLog(LVL_MAJOR, "FS_Scan_Config",
                   FSSCAN_CONFIG_BLOCK
                   "::nb_threads_scan changed in config file, but cannot be modified dynamically");

    if (conf->nb_prealloc_tasks != fs_scan_config.nb_prealloc_tasks)
        DisplayLog(LVL_MAJOR, "FS_Scan_Config",
                   FSSCAN_CONFIG_BLOCK
                   "::nb_prealloc_tasks changed in config file, but cannot be modified dynamically");

    /* compare ignore list */
    update_ignore(fs_scan_config.ignore_list, fs_scan_config.ignore_count,
                  conf->ignore_list, conf->ignore_count, FSSCAN_CONFIG_BLOCK);

    return 0;
}

static int fs_scan_cfg_set(void *cfg, bool reload)
{
    fs_scan_config_t *conf = (fs_scan_config_t *) cfg;

    if (reload)
        return fs_scan_cfg_reload(conf);

    fs_scan_config = *conf;
    return 0;
}

static void fs_scan_cfg_write_template(FILE *output)
{
    print_begin_block(output, 0, FSSCAN_CONFIG_BLOCK, NULL);

    print_line(output, 1, "# simple scan interval (fixed)");
#ifdef HAVE_CHANGELOGS
    print_line(output, 1, "scan_interval      =   2d ;");
#else
    print_line(output, 1, "scan_interval      =   6h ;");
#endif
    fprintf(output, "\n");

    print_line(output, 1, "# min/max for adaptive scan interval:");
    print_line(output, 1,
               "# the more the filesystem is full, the more frequently it is scanned.");
#ifdef HAVE_CHANGELOGS
    print_line(output, 1, "#min_scan_interval      =   24h ;");
    print_line(output, 1, "#max_scan_interval      =    7d ;");
#else
    print_line(output, 1, "#min_scan_interval      =    2h ;");
    print_line(output, 1, "#max_scan_interval      =   12h ;");
#endif
    fprintf(output, "\n");
    print_line(output, 1,
               "# number of threads used for scanning the filesystem");
    print_line(output, 1, "nb_threads_scan        =     2 ;");
    fprintf(output, "\n");
    print_line(output, 1,
               "# when a scan fails, this is the delay before retrying");
    print_line(output, 1, "scan_retry_delay       =    1h ;");
    fprintf(output, "\n");
    print_line(output, 1, "# timeout for operations on the filesystem");
    print_line(output, 1, "scan_op_timeout        =    1h ;");
    print_line(output, 1, "# exit if operation timeout is reached?");
    print_line(output, 1, "exit_on_timeout        =    yes ;");
    print_line(output, 1, "# external command called on scan termination");
    print_line(output, 1,
               "# special arguments can be specified: {cfg} = config file path,");
    print_line(output, 1, "# {fspath} = path to managed filesystem");
    print_line(output, 1,
               "#completion_command     =    \"/path/to/my/script.sh -f {cfg} -p {fspath}\" ;");
    fprintf(output, "\n");

    print_line(output, 1,
               "# Internal scheduler granularity (for testing and of scan, hangs, ...)");
    print_line(output, 1, "spooler_check_interval =  1min ;");
    fprintf(output, "\n");
    print_line(output, 1, "# Memory preallocation parameters");
    print_line(output, 1, "nb_prealloc_tasks      =   256 ;");
    fprintf(output, "\n");
    print_begin_block(output, 1, IGNORE_BLOCK, NULL);
    print_line(output, 2,
               "# ignore \".snapshot\" and \".snapdir\" directories (don't scan them)");
    print_line(output, 2, "type == directory");
    print_line(output, 2, "and");
    print_line(output, 2, "( name == \".snapdir\" or name == \".snapshot\" )");
    print_end_block(output, 1);
    print_end_block(output, 0);
}

static void *fs_scan_cfg_new(void)
{
    return calloc(1, sizeof(fs_scan_config_t));
}

static void fs_scan_cfg_free(void *cfg)
{
    fs_scan_config_t *conf;

    if (cfg == NULL)
        return;

    conf = (fs_scan_config_t *) cfg;

    free_ignore(conf->ignore_list, conf->ignore_count);
    free_scan_dirs(conf->dir_list, conf->dir_count);

    free(cfg);
}

mod_cfg_funcs_t fs_scan_cfg_hdlr = {
    .module_name = "FS scan",
    .new = fs_scan_cfg_new,
    .free = fs_scan_cfg_free,
    .set_default = fs_scan_cfg_set_default,
    .read = fs_scan_cfg_read,
    .set_config = fs_scan_cfg_set,
    .write_default = fs_scan_cfg_write_default,
    .write_template = fs_scan_cfg_write_template
};
