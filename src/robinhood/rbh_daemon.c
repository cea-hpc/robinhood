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

/**
 *  Daemon statup functions
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "policy_run.h"
#include "list_mgr.h"
#include "rbh_cfg.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "cmd_helpers.h"
#include "rbh_basename.h"

/* needed to dump their stats */
#include "fs_scan_main.h"
#include "chglog_reader.h"
#include "entry_processor.h"

#include <unistd.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <fcntl.h>              /* for open flags */
#include <signal.h>

#ifdef _LUSTRE
#include "lustre_extended_types.h"
#endif

#define MAIN_TAG    "Main"
#define RELOAD_TAG "ReloadConfig"

static time_t  boot_time;

/* values must be over max char index */
#define DRY_RUN           260
#define NO_LIMIT          261
#define TEST_SYNTAX       262
#define PARTIAL_SCAN      263
#define SHOW_DIFF         264
#define NO_GC             265
#define RUN_POLICIES      266
#define TGT_USAGE         267
#define FORCE_ALL         268

/* deprecated params */
#define FORCE_OST_PURGE   270
#define FORCE_FS_PURGE    271
#define FORCE_CLASS_PURGE 272

#define FORCE_OST_MIGR    280
#define FORCE_USER_MIGR   281
#define FORCE_GROUP_MIGR  282
#define FORCE_CLASS_MIGR  283
#define MIGR_ONE_FILE     284

#define DEPRECATED_WM     290


#define ACTION_MASK_SCAN                0x00000001
#define ACTION_MASK_PURGE               0x00000002
#define ACTION_MASK_HANDLE_EVENTS       0x00000004
#define ACTION_MASK_RUN_POLICIES        0x00000008

#ifdef HAVE_CHANGELOGS
    #define DEFAULT_ACTION_MASK     (ACTION_MASK_HANDLE_EVENTS | ACTION_MASK_RUN_POLICIES)
    #define DEFAULT_ACTION_HELP   "--read-log --run=all"
#else
    #define DEFAULT_ACTION_MASK     (ACTION_MASK_SCAN | ACTION_MASK_RUN_POLICIES)
    #define DEFAULT_ACTION_HELP   "--scan --run=all"
#endif

/* currently running modules */
static int     running_mask = 0;
/* selected modules (used for reloading config) */
static int     parsing_mask = 0;

/* currently running policies mask */
static uint64_t policy_run_mask = 0LL;

/* info for started policy modules */
static policy_info_t    *policy_run = NULL;
static unsigned int      policy_run_cpt = 0;

/* Array of options for getopt_long().
 * Each record consists of: { const char *name, int has_arg, int * flag, int val }
 */

static struct option option_tab[] = {

    /* Actions selectors */
    {"scan", optional_argument, NULL, 'S'},
    {"diff", required_argument, NULL, SHOW_DIFF},
#ifdef HAVE_CHANGELOGS
#ifdef HAVE_DNE
    {"readlog", optional_argument, NULL, 'r'},
    {"read-log", optional_argument, NULL, 'r'},
#else
    {"readlog", no_argument, NULL, 'r'},
    {"read-log", no_argument, NULL, 'r'},
    {"handle-events", no_argument, NULL, 'r'}, /* for backward compatibility */
#endif
#endif
    {"run", optional_argument, NULL, RUN_POLICIES},
    {"check-thresholds", optional_argument, NULL, 'C'},

    /* specifies a policy target */
    {"target", required_argument, NULL, 't'}, /* ost, pool, all, class, user, group, file... */
    {"target-usage", required_argument, NULL, TGT_USAGE}, /* target usage for FS, OST or pool */

    /* For policies, this forces to apply policy to files in policy scope,
     * by ignoring condition of policy rules and 'ignore' statements.
     */
    {"ignore-conditions", no_argument, NULL, 'I'},
    {"force", no_argument, NULL, 'F'},

    /* behavior flags */
    {"dry-run", no_argument, NULL, DRY_RUN},
    {"one-shot", no_argument, NULL, 'O'}, /* for backward compatibility */
    {"once", no_argument, NULL, 'O'},
    {"detach", no_argument, NULL, 'd'},
    {"no-limit", no_argument, NULL, NO_LIMIT},
    {"no-gc", no_argument, NULL, NO_GC},
    /* generic policies equivalent for --sync: alias to --once --no-limit --ignore-conditions --force */
    {"force-all", no_argument, NULL, FORCE_ALL},
    {"forceall", no_argument, NULL, FORCE_ALL},

    /* config file options */
    {"config-file", required_argument, NULL, 'f'},
    {"template", optional_argument, NULL, 'T'},
    {"defaults", no_argument, NULL, 'D'},
    {"test-syntax", no_argument, NULL, TEST_SYNTAX},

    /* override config file options */
    {"log-file", required_argument, NULL, 'L'},
    {"log-level", required_argument, NULL, 'l'},

    /* miscellaneous options */
    {"help", no_argument, NULL, 'h'},
    {"version", no_argument, NULL, 'V'},
    {"pid-file", required_argument, NULL, 'p'},

    /* kept for compatibility */
    {"partial-scan", required_argument, NULL, PARTIAL_SCAN},

    /* deprecated params */
    {"purge", no_argument, NULL, 'P'},
    {"release", no_argument, NULL, 'P'},
    {"check-watermarks", no_argument, NULL, DEPRECATED_WM},
    {"migrate", no_argument, NULL, 'M'},
    {"archive", no_argument, NULL, 'M'},
    {"sync", no_argument, NULL, 's'}, /* generic policies equivalent: --force-all */
    {"hsm-remove", no_argument, NULL, 'R'},
    {"hsm-rm", no_argument, NULL, 'R'},
    {"rmdir", no_argument, NULL, 'R'},
    {"purge-ost", required_argument, NULL, FORCE_OST_PURGE},
    {"release-ost", required_argument, NULL, FORCE_OST_PURGE},
    {"purge-fs", required_argument, NULL, FORCE_FS_PURGE},
    {"release-fs", required_argument, NULL, FORCE_FS_PURGE},
    {"purge-class", required_argument, NULL, FORCE_CLASS_PURGE},
    {"release-class", required_argument, NULL, FORCE_CLASS_PURGE},
    {"migrate-ost", required_argument, NULL, FORCE_OST_MIGR},
    {"archive-ost", required_argument, NULL, FORCE_OST_MIGR},
    {"migrate-user", required_argument, NULL, FORCE_USER_MIGR},
    {"archive-user", required_argument, NULL, FORCE_USER_MIGR},
    {"migrate-group", required_argument, NULL, FORCE_GROUP_MIGR},
    {"archive-group", required_argument, NULL, FORCE_GROUP_MIGR},
    {"migrate-class", required_argument, NULL, FORCE_CLASS_MIGR},
    {"archive-class", required_argument, NULL, FORCE_CLASS_MIGR},
    {"migrate-file", required_argument, NULL, MIGR_ONE_FILE},
    {"archive-file", required_argument, NULL, MIGR_ONE_FILE},
    /* -i replaced by -I to allow confusions with rbh-report -i */

    {NULL, 0, NULL, 0}
};

#define SHORT_OPT_STRING     "SrCt:IOdf:T:DL:l:hVp:F"
#define SHORT_OPT_DEPRECATED "PMRi"

#define MAX_OPT_LEN 1024
#define MAX_TYPE_LEN 256

typedef struct rbh_options {
    run_flags_t    flags;
    bool           detach;
    char           config_file[MAX_OPT_LEN];
    char           template_file[MAX_OPT_LEN];
    bool           write_template;
    bool           write_defaults;
    bool           force_log;
    char           log[MAX_OPT_LEN];
    bool           force_log_level;
    int            log_level;
    bool           pid_file;
    char           pid_filepath[MAX_OPT_LEN];
    bool           test_syntax;
    bool           partial_scan;
    char           partial_scan_path[RBH_PATH_MAX]; /* can be a deep path */
    attr_mask_t    diff_mask;

    char           policy_string[MAX_OPT_LEN];
    char           target_string[RBH_PATH_MAX]; /* can be a deep file */
    double         usage_target; /* set -1.0 if not set */

    int            mdtidx;

} rbh_options;

#define TGT_NOT_SET   -1.0

static inline void zero_options(struct rbh_options * opts)
{
    /* default value is 0 for most options */
    memset(opts, 0, sizeof(struct rbh_options));
    opts->usage_target = TGT_NOT_SET;
    opts->mdtidx = -1; /* all MDTs */
}


/* program options from command line  */
static struct rbh_options options;

/* special character sequences for displaying help */

/* Bold start character sequence */
#define _B "[1m"
/* Bold end charater sequence */
#define B_ "[m"

/* Underline start character sequence */
#define _U "[4m"
/* Underline end character sequence */
#define U_ "[0m"

static const char *cmd_help =
    _B "Usage:" B_ " %s [options]\n";

static const char *action_help =
    _B "Actions:" B_ "\n"
    "    " _B "-S" B_", " _B "--scan" B_ "[=" _U "dir" U_ "]\n"
    "        Scan the filesystem namespace. If "_U"dir"U_" is specified, only scan the specified subdir.\n"
#ifdef HAVE_CHANGELOGS
    "    " _B "-r" B_ ", " _B "--read-log" B_ "[=" _U "mdt_idx" U_ "]\n"
    "        Read events from MDT ChangeLog.\n"
    "        If "_U"mdt_idx"U_" is specified, only read ChangeLogs for the given MDT.\n"
    "        Else, start 1 changelog reader thread per MDT (with DNE).\n"
#endif
    "    " _B "--run" B_"[=all]\n"
    "        Run all polices (based on triggers).\n"
    "    " _B "--run" B_"=" _U"policy1"U_"("_U"args"U_"),"
                             _U"policy2"U_"("_U"args"U_")...\n"
    "        Run the given policies with the specified arguments. \n"
    "        See \"Policy run options\" for details about "_U"args"U_".\n"
    "    " _B "-C" B_ " " _U"policy1,policy2..."U_", "
            _B "--check-thresholds" B_"[=" _U "policy1,policy2..." U_ "]\n"
    "        Only check trigger thresholds without applying policy actions.\n"
    "        If no policy is specified (or 'all'), check all triggers.\n"
    "\n"
    "    Note: if no action is specified, the default action set is: "DEFAULT_ACTION_HELP"\n";

static const char *run_help =
    _B "Policy run options:" B_ "\n"
    "    " _U"args"U_ "\n"
    "       Comma-separated list of <param>=<value>.\n"
    "           e.g. --run=cleanup(target=user:foo,max-count=1000)\n"
    "       The following parameters are allowed:\n"
    "       "_B"target"B_"="_U"tgt"U_"\n"
    "           Targeted subset of entries for the policy run.\n"
    "           "_U"tgt"U_" can be one of:\n"
    "               " _B"all"B_" (all entries), " _B"user"B_":"_U"username"U_", " _B"group"B_":"_U"grpname"U_",\n"
    "               " _B"file"B_":"_U"path"U_", " _B"class"B_":"_U"fileclass"U_
#ifdef _LUSTRE
    ", "_B"ost"B_":"_U"ost_idx"U_", "_B"pool"B_":"_U"poolname"U_
#endif
    ".\n"
    "       "_B"max-count"B_"="_U"nbr"U_"\n"
    "           Max number of actions to execute for a policy run.\n"
    "       "_B"max-vol"B_"="_U"size"U_"\n"
    "           Max volume of entries impacted by a policy run.\n"
    "       "_B"target-usage"B_"="_U"pct"U_"\n"
    "           Targeted filesystem or OST usage for a policy run, in percent.\n"
    "\n"
    "    " _B "-t" B_" " _U"tgt"U_ ", " _B "--target" B_ "=" _U"tgt"U_ "\n"
    "        Specify the default target for policy runs (see target syntax above).\n"
    "    "  _B "--target-usage" B_ "=" _U"pct"U_ "\n"
    "       Specifies the default target disk usage (in pct) for 'all', 'ost' or 'pool' targets.\n"
    "    " _B "-I" B_ ", " _B "--ignore-conditions"B_"\n"
    "        Apply policy to all entries in policy scope, without checking policy rule conditions.\n"
    "    " _B "-F" B_ ", " _B "--force"B_"\n"
    "        Force applying policies even if no full scan has never been done (partial DB contents).\n"
    "    " _B "--no-limit"B_"\n"
    "        Don't limit the maximum number/volume of policy actions per pass.\n"
    "    " _B "--dry-run"B_"\n"
    "        Only report policy actions that would be performed without really doing them.\n"
    "        Note: Robinhood DB is impacted as if the reported actions were really done.\n"
    "    " _B "--force-all"B_"\n"
    "        Force applying a policy to all eligible entries, without considering\n"
    "        policy limits and rule conditions.\n"
    "        This is equivalent to: --once --no-limit --ignore-conditions --force\n";

static const char *scan_help =
    _B "Scanning options:" B_ "\n"
    "    " _B "--no-gc"B_"\n"
    "        Garbage collection of entries in DB is a long operation when terminating\n"
    "        a scan. This skips this operation if you don't care about removed\n"
    "        entries (or don't expect entries to be removed).\n"
    "        This is also recommended for partial scanning (see -scan=dir option).\n";

static const char *output_help =
    _B "Output options:" B_ "\n"
    "    " _B "--diff"B_"="_U"attrset"U_ "\n"
    "        When scanning or reading changelogs, display changes for the given set of attributes (to stdout).\n"
    "        "_U"attrset"U_" is a list of values in: path,posix,stripe,all,status,notimes,noatime.\n";

static const char *behavior_help =
    _B "Behavior options:" B_ "\n"
    "    " _B "-O" B_ ", " _B "--once" B_ "\n"
    "        Perform only one pass of the specified action and exit.\n"
    "    " _B "-d" B_ ", " _B "--detach" B_ "\n"
    "        Daemonize the process (detach from parent process).\n";

static const char *config_help =
    _B "Config file options:" B_ "\n"
    "    " _B "-f" B_ " " _U "cfg_file" U_ ", " _B "--config-file=" B_ _U "cfg_file" U_ "\n"
    "        Path to configuration file (or short name).\n"
    "    " _B "-T" B_ " " _U "output_file"
    U_ ", " _B "--template" B_"[=" _U "output_file" U_ "]\n"
    "        Write a configuration file template to the specified file.\n"
    "    " _B "-D" B_ ", " _B "--defaults" B_ "\n"
    "        Display default configuration values.\n"
    "    " _B "--test-syntax" B_ "\n"
    "        Check configuration file and exit.\n";

static const char *log_help =
    _B"Log options:" B_ "\n"
    "    " _B "-L" B_ " " _U "logfile" U_ ", " _B "--log-file=" B_ _U
    "logfile" U_ "\n" "        Force the path to the log file (overrides configuration value).\n"
    "        Special values \"stdout\" and \"stderr\" can be used.\n"
    "    " _B "-l" B_ " " _U "loglevel" U_ ", " _B "--log-level=" B_ _U "loglevel" U_ "\n"
    "        Force the log verbosity level (overrides configuration value).\n"
    "        Allowed values: CRIT, MAJOR, EVENT, VERB, DEBUG, FULL.\n";

static const char *misc_help =
     _B "Miscellaneous options:" B_ "\n"
    "    " _B "-h" B_ ", " _B "--help" B_ "\n"
    "        Display a short help about command line options.\n"
    "    " _B "-V" B_ ", " _B "--version" B_ "\n" "        Display version info\n"
    "    " _B "-p" B_ " " _U "pidfile" U_ ", "
    _B "--pid-file=" B_ _U "pidfile" U_ "\n" "         Pid file (used for service management).\n";


static inline void display_help(const char *bin_name)
{
    printf(cmd_help, bin_name);
    printf("\n");
    printf("%s\n", action_help);
    printf("%s\n", run_help);
    printf("%s\n", scan_help);
    printf("%s\n", output_help);
    printf("%s\n", behavior_help);
    printf("%s\n", config_help);
    printf("%s\n", log_help);
    printf("%s", misc_help);
}

static inline void display_version(const char *bin_name)
{
    printf("\n");
    printf("Product:         " PACKAGE_NAME "\n");
    printf("Version:         " PACKAGE_VERSION "-"RELEASE"\n");
    printf("Build:           " COMPIL_DATE "\n");
    printf("\n");
    printf("Compilation switches:\n");

/* FS type */
#ifdef _LUSTRE
    printf("    Lustre filesystems\n");
#ifdef LUSTRE_VERSION
    printf("    Lustre Version: " LUSTRE_VERSION "\n");
#endif

#else
    printf("    Posix filesystems\n");
#endif
/* Access by Fid ? */
#ifdef _HAVE_FID
    printf("    Address entries by FID\n");
#else
    printf("    Address entries by path\n");
#endif
#ifdef HAVE_CHANGELOGS
    printf("    MDT Changelogs supported\n");
#ifndef HAVE_DNE
    printf("    Support Changelogs from multiple MDT (DNE)\n");
#endif
#else
    printf("    MDT Changelogs disabled\n");
#endif
    printf("\n");

#ifdef _MYSQL
    printf("Database binding: MySQL\n");
#elif defined(_SQLITE)
    printf("Database binding: SQLite\n");
#else
#error "No database was specified"
#endif
    printf("\n");
    printf("Report bugs to: <" PACKAGE_BUGREPORT ">\n");
    printf("\n");
}

static pthread_t stat_thread;

/* database connexion for updating stats */
static lmgr_t  lmgr;
static bool    lmgr_init = false;
static char    boot_time_str[256];


static void running_mask2str(int mask, uint64_t pol_mask, char *str)
{
    str[0] = '\0';
    if (mask & MODULE_MASK_FS_SCAN)
        strcat(str, "scan,");
#ifdef HAVE_CHANGELOGS
    if (mask & MODULE_MASK_EVENT_HDLR)
        strcat(str, "log_reader,");
#endif
    if (mask & MODULE_MASK_POLICY_RUN)
    {
        bool first_pol = true;
        int  i;

        strcat(str, "policy_run(");
        for (i = 0; i < policy_run_cpt; i++)
        {
            if ((pol_mask) & (1LL<<i))
            {
                if (!first_pol)
                    strcat(str, ",");

                strcat(str, policy_run[i].descr->name);
                first_pol = false;
            }
        }
        strcat(str, "),");
    }

    /* remove final ',' */
    int len = strlen(str);
    if ((len > 0) && str[len-1] == ',')
        str[len-1] = '\0';
    return;
}

/** prevent from dumping module stats when the daemon is shutting down */
static pthread_mutex_t  shutdown_mtx  = PTHREAD_MUTEX_INITIALIZER;

/** signal flags */
static int     terminate_sig = 0;
static bool    reload_sig    = false;
static bool    dump_sig      = false;

/** async signal handler */
static pthread_t        sig_thr;

/** dump stats of all modules */
static void dump_stats(lmgr_t *lmgr, const int *module_mask,
                       const uint64_t *p_policy_mask)
{
        char           tmp_buff[256];
        time_t         now;
        struct tm      date;

        if (pthread_mutex_trylock(&shutdown_mtx) != 0)
            /* daemon is shutting down, don't dump stats */
            return;

        now = time(NULL);
        strftime(tmp_buff, sizeof(tmp_buff), "%Y/%m/%d %T",
                 localtime_r(&now, &date));

        DisplayLog(LVL_MAJOR, "STATS",
                    "==================== Dumping stats at %s =====================", tmp_buff);
        DisplayLog(LVL_MAJOR, "STATS", "======== General statistics =========");
        DisplayLog(LVL_MAJOR, "STATS", "Daemon start time: %s", boot_time_str);
        running_mask2str(*module_mask, *p_policy_mask, tmp_buff);
        DisplayLog(LVL_MAJOR, "STATS", "Started modules: %s", tmp_buff);

        if (*module_mask & MODULE_MASK_FS_SCAN)
        {
            FSScan_DumpStats();
            FSScan_StoreStats(lmgr);
        }

#ifdef HAVE_CHANGELOGS
        if (*module_mask & MODULE_MASK_EVENT_HDLR)
        {
            cl_reader_dump_stats();
            cl_reader_store_stats(lmgr);
        }
#endif

        if (*module_mask & MODULE_MASK_ENTRY_PROCESSOR)
        {
            EntryProcessor_DumpCurrentStages();
        }

        if (*module_mask & MODULE_MASK_POLICY_RUN
            && *p_policy_mask != 0LL
            && policy_run_cpt != 0
            && policy_run != NULL)
        {
            int i;

            for (i = 0; i <  policy_run_cpt; i++)
            {
                if ((*p_policy_mask) & (1LL<<i))
                    policy_module_dump_stats(&policy_run[i]);
            }
        }

        pthread_mutex_unlock(&shutdown_mtx);

        /* Flush stats */
        FlushLogs();
}

static void  *stats_thr( void *arg )
{
    struct tm      date;

    strftime( boot_time_str, 256, "%Y/%m/%d %T", localtime_r( &boot_time, &date ) );

    if ( !lmgr_init )
    {
        if ( ListMgr_InitAccess( &lmgr ) != DB_SUCCESS )
            return NULL;
        lmgr_init = true;
    }

    DisplayLog( LVL_VERB, MAIN_TAG, "Statistics thread started" );

    WaitStatsInterval();
    while (!terminate_sig)
    {
        dump_stats(&lmgr, &running_mask, &policy_run_mask);
        WaitStatsInterval();
    }
    return NULL;
}


#define SIGHDL_TAG  "SigHdlr"

static void terminate_handler( int sig )
{
    terminate_sig = sig;
}

static void reload_handler( int sig )
{
    reload_sig = true;
}

static void usr_handler( int sig )
{
    dump_sig = true;
}



static int action2parsing_mask( int act_mask )
{
    /* build config parsing mask */
    int parse_mask = 0;
    if (act_mask & ACTION_MASK_SCAN)
        parse_mask |= MODULE_MASK_FS_SCAN | MODULE_MASK_ENTRY_PROCESSOR;
    if (act_mask & ACTION_MASK_RUN_POLICIES)
        parse_mask |= MODULE_MASK_POLICY_RUN;
#ifdef HAVE_CHANGELOGS
    if ( act_mask & ACTION_MASK_HANDLE_EVENTS )
        parse_mask |= MODULE_MASK_EVENT_HDLR | MODULE_MASK_ENTRY_PROCESSOR;
#endif

    return parse_mask;
}


static void   *signal_handler_thr( void *arg )
{

    struct sigaction act_sighup;
    struct sigaction act_sigterm;
    struct sigaction act_sigusr;

    /* create signal handlers */
    memset( &act_sigterm, 0, sizeof( act_sigterm ) );
    act_sigterm.sa_flags = 0;
    act_sigterm.sa_handler = terminate_handler;
    if ( sigaction( SIGTERM, &act_sigterm, NULL ) == -1
         || sigaction( SIGINT, &act_sigterm, NULL ) == -1 )
    {
        DisplayLog( LVL_CRIT, SIGHDL_TAG,
                    "Error while setting signal handlers for SIGTERM and SIGINT: %s",
                    strerror( errno ) );
        exit( 1 );
    }
    else
        DisplayLog(LVL_VERB, SIGHDL_TAG,
                   "Signals SIGTERM and SIGINT (daemon shutdown) are ready to be used");

    memset( &act_sighup, 0, sizeof( act_sighup ) );
    act_sighup.sa_flags = 0;
    act_sighup.sa_handler = reload_handler;
    if ( sigaction( SIGHUP, &act_sighup, NULL ) == -1 )
    {
        DisplayLog( LVL_CRIT, SIGHDL_TAG, "Error while setting signal handlers for SIGHUP: %s",
                    strerror( errno ) );
        exit( 1 );
    }
    else
        DisplayLog(LVL_VERB, SIGHDL_TAG, "Signal SIGHUP (config reloading) is ready to be used");

    memset( &act_sigusr, 0, sizeof( act_sigusr ) );
    act_sigusr.sa_flags = 0;
    act_sigusr.sa_handler = usr_handler;
    if ( sigaction( SIGUSR1, &act_sigusr, NULL ) == -1 )
    {
        DisplayLog( LVL_CRIT, SIGHDL_TAG, "Error while setting signal handlers for SIGUSR1: %s",
                    strerror( errno ) );
        exit( 1 );
    }
    else
        DisplayLog(LVL_VERB, SIGHDL_TAG, "Signal SIGUSR1 (stats dump) is ready to be used");


    /* signal flag checking loop */
    while ( 1 )
    {
        /* check for signal every second */
        rh_sleep( 1 );

        if ( terminate_sig != 0 )
        {
            const struct timespec timeout = { .tv_sec = 1, .tv_nsec = 0 };

            if ( terminate_sig == SIGTERM )
                DisplayLog( LVL_MAJOR, SIGHDL_TAG, "SIGTERM received: performing clean daemon shutdown" );
            else if ( terminate_sig == SIGINT )
                DisplayLog( LVL_MAJOR, SIGHDL_TAG, "SIGINT received: performing clean daemon shutdown" );
            FlushLogs(  );

            /* wait up to 1s to get the termination mutex (avoids dumping stats while terminating the daemon) */
            if (pthread_mutex_timedlock(&shutdown_mtx, &timeout) != 0)
            {
                /* Something when wrong for an unexpected reason, but we have to shutdown now!
                 * => continuing */
                DisplayLog(LVL_MAJOR, SIGHDL_TAG, "WARNING: Failed to get termination mutex: %m");
            }

            /* first ask policy consummers and feeders to stop (long operations first) */

            /* 1- stop submitting policy actions */
            if (running_mask & MODULE_MASK_POLICY_RUN
                && policy_run_mask != 0LL
                && policy_run_cpt != 0
                && policy_run != NULL)
            {
                int i;
                for (i = 0; i <  policy_run_cpt; i++)
                {
                    if (policy_run_mask & (1LL<<i))
                    {
                        int rc = policy_module_stop(&policy_run[i]);
                        if (rc)
                            DisplayLog(LVL_CRIT, SIGHDL_TAG, "Failed to stop policy module '%s' (rc=%d).",
                                       policy_run[i].descr->name, rc);
                        FlushLogs();
                    }
                }
            }

            /* 2 - stop feeding with changelogs */
#ifdef HAVE_CHANGELOGS
            if (running_mask & MODULE_MASK_EVENT_HDLR)
            {
                /* stop changelog processing */
                cl_reader_terminate(  );
                FlushLogs(  );
            }
#endif
            /* 2b - stop feeding from scan */
            if (running_mask & MODULE_MASK_FS_SCAN)
            {
                /* avoid stats thread to try dumping the stats while terminating */
                running_mask &= ~MODULE_MASK_FS_SCAN;

                /* stop FS scan (blocking) */
                FSScan_Terminate(  );
                FlushLogs(  );
            }

            /* TODO 3) wait changelog reader (blocking) */

            /* 4 - entry processor can be stopped */
            if (running_mask & MODULE_MASK_ENTRY_PROCESSOR)
            {
                /* avoid stats thread to try dumping the status while terminating */
                running_mask &= ~MODULE_MASK_ENTRY_PROCESSOR;

                /* drop pipeline waiting operations and terminate threads */
                EntryProcessor_Terminate(false);

#ifdef HAVE_CHANGELOGS
                if (running_mask & MODULE_MASK_EVENT_HDLR)
                {
                    /* Ack last changelog records. */
                    cl_reader_done();
                }
#endif
                FlushLogs(  );
            }

            /* 5 - wait policy consumers */
            if (running_mask & MODULE_MASK_POLICY_RUN
                && policy_run_mask != 0LL
                && policy_run_cpt != 0
                && policy_run != NULL)
            {
                int i, rc;

                running_mask &= ~MODULE_MASK_POLICY_RUN;

                for (i = 0; i < policy_run_cpt; i++)
                {
                    if (policy_run_mask & (1LL<<i))
                    {
                        policy_run_mask &= ~(1LL<<i);
                        rc = policy_module_wait(&policy_run[i]);
                        if (rc)
                            DisplayLog(LVL_CRIT, SIGHDL_TAG, "Failure while waiting for policy module '%s' to end (rc=%d).",
                                       policy_run[i].descr->name, rc);
                        FlushLogs();
                    }
                }
            }

            if (lmgr_init)
            {
                ListMgr_CloseAccess(&lmgr);
                lmgr_init = false;
            }

            DisplayLog( LVL_MAJOR, SIGHDL_TAG, "Exiting." );
            FlushLogs(  );

            /* indicate the process terminated due to a signal */
            exit( 128 + terminate_sig );

        }
        else if (reload_sig)
        {
            DisplayLog(LVL_MAJOR, SIGHDL_TAG, "SIGHUP received: reloading configuration");
            DisplayLog(LVL_EVENT, RELOAD_TAG, "Reloading configuration from '%s'", config_file_path());

            if(rbh_cfg_reload(parsing_mask) == 0)
            {
                if (options.force_log && strcmp(options.log, log_config.log_file))
                {
                    DisplayLog( LVL_EVENT, RELOAD_TAG, "Restoring log file option (forced on command line): %s)",
                                options.log );
                    strcpy(log_config.log_file, options.log);
                }
                if (options.force_log_level && (options.log_level != log_config.debug_level))
                {
                    DisplayLog(LVL_EVENT, RELOAD_TAG, "Restoring log level option (forced on command line): %d)",
                               options.log_level);
                    log_config.debug_level = options.log_level;
                }
            }

            reload_sig = false;
            FlushLogs(  );
        }
        else if ( dump_sig )
        {
            DisplayLog( LVL_MAJOR, SIGHDL_TAG, "SIGUSR1 received: dumping stats" );

            if ( !lmgr_init )
            {
                if ( ListMgr_InitAccess( &lmgr ) != DB_SUCCESS )
                    return NULL;
                lmgr_init = true;
            }

            dump_stats(&lmgr, &running_mask, &policy_run_mask);
            dump_sig = false;
        }
    }
}



static inline int do_write_template( const char *file )
{
    int            rc;
    FILE          *stream;

    if ( !EMPTY_STRING( file ) )
    {
        stream = fopen( file, "w" );

        if ( stream == NULL )
        {
            rc = errno;
            fprintf( stderr, "Error opening file '%s' for writing: %s.\n", file, strerror( rc ) );
            return rc;
        }
    }
    else
        stream = stdout;

    rc = rbh_cfg_write_template(stream);
    if ( rc )
        fprintf( stderr, "Error writing configuration template: %s\n", strerror( rc ) );
    else if ( stream != stdout )
        fprintf( stderr, "Configuration template successfully written to '%s'.\n", file );

    if ( stream != stdout )
        fclose( stream );

    return rc;
}


static void create_pid_file( const char *pid_file )
{
    int            fd = open( pid_file, O_CREAT | O_TRUNC | O_WRONLY, 0644 );

    if ( fd < 0 )
    {
        DisplayLog( LVL_CRIT, MAIN_TAG, "WARNING: Could not open pid file %s: %s",
                    pid_file, strerror( errno ) );
    }
    else
    {
        char           pid_str[128];
        ssize_t        iolen;

        snprintf( pid_str, 128, "%lu\n", ( unsigned long ) getpid(  ) );
        iolen = write( fd, pid_str, strlen( pid_str ) + 1 );

        if ( iolen == -1 )
        {
            DisplayLog( LVL_CRIT, MAIN_TAG, "ERROR writing pid file %s: %s", pid_file,
                        strerror( errno ) );
        }

        close( fd );
    }

}

/** parse a target-usage parameter (float)
 * @return 0 on success. errno value on failure.
 */
static int parse_target_usage(const char *str, double *val)
{
    char extra_chr[MAX_OPT_LEN];
    int n;

    /* parse float argument */
    n = sscanf(str, "%lf%s", val, extra_chr);
    if (n != 1 && n != 2)
    {
        fprintf(stderr, "ERROR: invalid target-usage '%s'. Float expected.\n", str);
        return EINVAL;
    }
    if (n == 2 && strcmp(extra_chr, "\%") != 0)
    {
        fprintf(stderr, "ERROR: unexpected suffix '%s' in target-usage. "
                "Only '%%' is allowed.\n", extra_chr);
        return EINVAL;
    }

    return 0;
}

#define SET_ACTION_FLAG( _f_ )  do {                                    \
                                    if ( is_default_actions )           \
                                    {                                   \
                                        is_default_actions = false;     \
                                        *action_mask = _f_;             \
                                    }                                   \
                                    else                                \
                                        *action_mask |= _f_;            \
                                } while(0)

/** parse options in robinhood command line */
static int rh_read_parameters(const char *bin, int argc, char **argv,
                              int *action_mask, struct rbh_options *opt)
{
    int            c, option_index = 0;
    bool           is_default_actions = true;
    char           err_msg[4096];

    *action_mask = DEFAULT_ACTION_MASK;

    zero_options( &options );

    /* parse command line options */
    while ((c = getopt_long(argc, argv, SHORT_OPT_STRING SHORT_OPT_DEPRECATED,
                            option_tab, &option_index)) != -1)
    {
        switch ( c )
        {
        case PARTIAL_SCAN:
            fprintf(stderr, "Warning: --partial-scan is deprecated. Use '--scan=<dir>' instead.\n");
            /* same as 'scan' with optarg != NULL
             * => continue to -S:
             */
        case 'S':
            SET_ACTION_FLAG( ACTION_MASK_SCAN );

            if (optarg) {       /* optional argument => partial scan*/
                opt->flags |= RUNFLG_ONCE;
                opt->partial_scan = true;
                rh_strncpy(opt->partial_scan_path, optarg, RBH_PATH_MAX);
                /* clean final slash */
                if (FINAL_SLASH(opt->partial_scan_path))
                    REMOVE_FINAL_SLASH(opt->partial_scan_path);
            }
            break;

        case SHOW_DIFF:
            if (parse_diff_mask(optarg, &opt->diff_mask, err_msg))
            {
                fprintf(stderr,
                        "Invalid argument for --diff: %s\n", err_msg);
                return EINVAL;
            }
            break;

        case DEPRECATED_WM:
            fprintf(stderr, "Warning: '--check-watermarks' is deprecated. Use '--check-thresholds' instead.\n");
            optarg = NULL;
            /* same as '--check-thresholds' with opt_arg=NULL.
             * => continue to -C
             */
        case 'C':
            SET_ACTION_FLAG(ACTION_MASK_RUN_POLICIES);
            opt->flags |= RUNFLG_CHECK_ONLY;
            /* 4 cases:
             *  --run=foo,bar --check-thresholds=bah,boo
             *      => reject if arguments are different
             *  --run=foo,bar --check-thresholds
             *      => user may want to check thresholds for the given policies
             *  --run --check-thresholds=bah,boo
             *      => user may want to check thresholds for the given policies
             *  --run --check-thresholds
             *      => user may want to check thresholds for all policies
             */
            /* first case: */
            if (!EMPTY_STRING(opt->policy_string) && (optarg != NULL)
                && !EMPTY_STRING(optarg))
            {
                if(strcasecmp(opt->policy_string, optarg) != 0)
                {
                    fprintf(stderr, "Incompatible arguments for --run and --check-thresholds ('%s' != '%s').\n"
                                    "You can specify:\n"
                                    "--check-thresholds=foo,bar\n"
                                    "--run=foo,bar --check-thresholds\n",
                                    opt->policy_string, optarg);
                    return EINVAL;
                }
                else
                    fprintf(stderr, "Duplicate arguments for --run and --check-thresholds\n"
                                    "Assuming --check-thresholds=%s",
                                    opt->policy_string);
            }
            else /* all other cases: global check flag. */
            {
                /* copy specified policies (unless if specified by run) */
                if (EMPTY_STRING(opt->policy_string) && optarg != NULL &&
                    !EMPTY_STRING(optarg))
                    rh_strncpy(opt->policy_string, optarg, sizeof(opt->policy_string));
            }
            break;

        case 'r':
#ifndef HAVE_CHANGELOGS
            fprintf( stderr,
                     "-r | --read-log option is only supported in Lustre v2.x versions.\n" );
            return ENOTSUP;
#else
            SET_ACTION_FLAG( ACTION_MASK_HANDLE_EVENTS );
#ifdef HAVE_DNE
            if (optarg) {  /* optional argument => MDT index */
                opt->mdtidx = str2int(optarg);
                if (opt->mdtidx == -1) {
                    fprintf(stderr, "Invalid argument to --read-log: expected numeric value for <mdt_index>.\n");
                    return EINVAL;
                }
            }
#endif
#endif
            break;

        case RUN_POLICIES:
            /* avoid conflicts with check-policies */
            if (opt->flags & RUNFLG_CHECK_ONLY)
            {
                if (optarg != NULL && !EMPTY_STRING(optarg))
                    fprintf(stderr, "ERROR: --run is redundant with --check-thresholds\n"
                            "Did you mean: --check-thresholds=%s ?\n", optarg);
                else if (!EMPTY_STRING(opt->policy_string))
                    fprintf(stderr, "ERROR: --run is redundant with --check-thresholds\n"
                            "Did you just mean: --check-thresholds=%s ?\n", opt->policy_string);
                else /* both empty */
                    fprintf(stderr, "ERROR: --run is redundant with --check-thresholds\n"
                            "Did you just mean: --check-thresholds ?\n");
                return EINVAL;
            }
            else if (!EMPTY_STRING(opt->policy_string))
            {
                /* forbid using '--run' (without option)
                 * together with '--run=policy1...' (with options) */
                if (optarg == NULL)
                {
                    fprintf(stderr, "ERROR: multiple inconsistent '--run' "
                            "parameters on command line.\n");
                    return EINVAL;
                }
                /* Concatenate with previous 'run' parameters, to allow specifying
                 * --run=policy1 --run=policy2,...
                 */
                strncat(opt->policy_string, ",", sizeof(opt->policy_string)
                        - strlen(opt->policy_string) - 1);
                strncat(opt->policy_string, optarg, sizeof(opt->policy_string)
                        - strlen(opt->policy_string) - 1);
            }
            else if (optarg != NULL && !EMPTY_STRING(optarg))
            {
                /* was there a previous '--run' without policies? */
                if (!is_default_actions &&
                    (*action_mask) & ACTION_MASK_RUN_POLICIES)
                {
                    fprintf(stderr, "ERROR: multiple inconsistent '--run' or '--check-thresholds' "
                            "parameters on command line.\n");
                    return EINVAL;
                }

                rh_strncpy(opt->policy_string, optarg,
                           sizeof(opt->policy_string));
            }
            /* Set it at the end, to check if a previous --run
             * or --check-threshold was specified */
            SET_ACTION_FLAG(ACTION_MASK_RUN_POLICIES);
            break;

        case 't':
            if (!EMPTY_STRING(opt->target_string))
            {
                fprintf(stderr, "ERROR: multiple target definition on command line: '%s' and '%s'.\n",
                        opt->target_string, optarg);
                return EINVAL;
            }
            rh_strncpy(opt->target_string, optarg, sizeof(opt->target_string));
            break;
        case TGT_USAGE:
            if (opt->usage_target != TGT_NOT_SET)
            {
                fprintf(stderr, "ERROR: multiple target-usage specified on command line.\n");
                return EINVAL;
            }
            /* parse float argument */
            if (parse_target_usage(optarg, &opt->usage_target))
                return EINVAL;
            break;

        case 's':
            fprintf(stderr, "ERROR: --sync option is deprecated. Instead, use --run=<policy_name> --force-all\n");
            return EINVAL;
            break;

        case 'O':
            opt->flags |= RUNFLG_ONCE;
            break;
        case NO_LIMIT:
            opt->flags |= RUNFLG_NO_LIMIT;
            break;
        case NO_GC:
            opt->flags |= RUNFLG_NO_GC;
            break;
        case DRY_RUN:
            opt->flags |= RUNFLG_DRY_RUN;
            break;
        case 'I':
            opt->flags |= RUNFLG_IGNORE_POL;
            break;
        case 'F':
            opt->flags |= RUNFLG_FORCE_RUN;
            break;
        case FORCE_ALL:
            opt->flags |= RUNFLG_ONCE | RUNFLG_NO_LIMIT | RUNFLG_IGNORE_POL
                          | RUNFLG_FORCE_RUN;
            break;

        case 'd':
            opt->detach = true;
            break;
        case 'f':
            rh_strncpy(opt->config_file, optarg, MAX_OPT_LEN);
            break;
        case 'T':
            if ( optarg )       /* optional argument */
                rh_strncpy(opt->template_file, optarg, MAX_OPT_LEN);
            opt->write_template = true;
            break;
        case TEST_SYNTAX:
            opt->test_syntax = true;
            break;
        case 'D':
            opt->write_defaults = true;
            break;
        case 'L':
            opt->force_log = true;
            rh_strncpy(opt->log, optarg, MAX_OPT_LEN);
            break;
        case 'l':
            opt->force_log_level = true;
            opt->log_level = str2debuglevel(optarg);
            if (opt->log_level == -1)
            {
                fprintf(stderr,
                        "Unsupported log level '%s'. CRIT, MAJOR, EVENT, VERB, DEBUG or FULL expected.\n",
                        optarg);
                return EINVAL;
            }
            break;
        case 'p':
            opt->pid_file = true;
            rh_strncpy(opt->pid_filepath, optarg, MAX_OPT_LEN);
            break;
        case 'h':
            display_help( bin );
            return -1;
            break;
        case 'V':
            display_version( bin );
            return -1;
            break;

        /* Deprecated options */
        case 'P':
        case 'R':
        case 'M':
            fprintf(stderr, "ERROR: option -%c is deprecated. Instead, use: --run=<policyname>\n",
                    c);
            return EINVAL;
            break;
        case 'i':
            fprintf(stderr, "ERROR: option '-i' is deprecated: use '-I' instead.\n");
            return EINVAL;
            break;
        case FORCE_OST_PURGE:
            fprintf(stderr, "ERROR: option --%s is deprecated.\nInstead, use: --run=<policyname> --target=ost:<idx> --target-usage=<pct>\n",
                    option_tab[option_index].name);
            return EINVAL;
        case FORCE_FS_PURGE:
            fprintf(stderr, "ERROR: option --%s is deprecated.\nInstead, use: --run=<policyname> --target=all --target-usage=<pct>\n",
                    option_tab[option_index].name);
            return EINVAL;
        case FORCE_CLASS_PURGE:
            fprintf(stderr, "ERROR: option --%s is deprecated.\nInstead, use: --run=<policyname> --target=class:<name>\n",
                    option_tab[option_index].name);
            return EINVAL;
        case FORCE_OST_MIGR:
            fprintf(stderr, "ERROR: option --%s is deprecated.\nInstead, use: --run=<policyname> --target=ost:<name>\n",
                    option_tab[option_index].name);
            return EINVAL;
        case FORCE_USER_MIGR:
            fprintf(stderr, "ERROR: option --%s is deprecated.\nInstead, use: --run=<policyname> --target=user:<name>\n",
                    option_tab[option_index].name);
            return EINVAL;
        case FORCE_GROUP_MIGR:
            fprintf(stderr, "ERROR: option --%s is deprecated.\nInstead, use: --run=<policyname> --target=group:<name>\n",
                    option_tab[option_index].name);
            return EINVAL;
        case FORCE_CLASS_MIGR:
            fprintf(stderr, "ERROR: option --%s is deprecated.\nInstead, use: --run=<policyname> --target=class:<name>\n",
                    option_tab[option_index].name);
            return EINVAL;
        case MIGR_ONE_FILE:
            fprintf(stderr, "ERROR: option --%s is deprecated.\nInstead, use: --run=<policyname> --target=file:<path>\n",
                    option_tab[option_index].name);
            return EINVAL;

        case ':':
        case '?':
        default:
            fprintf(stderr,"Run '%s --help' for more details.\n", bin);
            return EINVAL;
            break;
        }
    }

    /* check there is no extra arguments */
    if (optind != argc)
    {
        fprintf(stderr, "Error: unexpected argument on command line: %s\n",
                argv[optind]);
        return EINVAL;
    }

    if (!attr_mask_is_null(opt->diff_mask) && (*action_mask != ACTION_MASK_SCAN)
        && (*action_mask != ACTION_MASK_HANDLE_EVENTS))
    {
        fprintf(stderr, "Error: --diff option only applies to --scan and --readlog actions\n");
        return EINVAL;
    }

    return 0;
} /* rh_read_parameters */

#ifdef _LUSTRE
#define TGT_HELP "Allowed values: 'all', 'user:<username>', 'group:<groupname>', 'file:<path>', 'class:<fileclass>', 'ost:<ost_idx>', 'pool:<poolname>'."
#else
#define TGT_HELP "Allowed values: 'all', 'user:<username>', 'group:<groupname>', 'file:<path>', 'class:<fileclass>'."
#endif

/** convert a target string option to a policy_opt_t structure */
static int policyopt_set_target(char *opt_string, policy_opt_t *opt)
{
    char *next;
    char *c;

    if (EMPTY_STRING(opt_string))
    {
        opt->target = TGT_NONE;
        return 0;
    }

    if (!strcasecmp(opt_string, "all"))
    {
        opt->target = TGT_FS;
        return 0;
    }

    if ((c = strchr(opt_string, ':')) == NULL)
    {
        fprintf(stderr, "Invalid target '%s'. "TGT_HELP"\n", opt_string);
        return EINVAL;
    }
    *c = '\0';
    next = c+1;
    c = opt_string;

    if (!strcasecmp(c, "all"))
    {
        fprintf(stderr, "No ':' expected after 'all' target.\n");
        return EINVAL;
    }
    else if (!strcasecmp(c, "user"))
    {
        opt->target = TGT_USER;
        opt->optarg_u.name = next;
    }
    else if (!strcasecmp(c, "group"))
    {
        opt->target = TGT_GROUP;
        opt->optarg_u.name = next;
    }
    else if (!strcasecmp(c, "file"))
    {
        opt->target = TGT_FILE;
        opt->optarg_u.name = next;
    }
    else if (!strcasecmp(c, "class"))
    {
        opt->target = TGT_CLASS;
        opt->optarg_u.name = next;
    }
#ifdef _LUSTRE
    else if (!strcasecmp(c, "ost"))
    {
        char extra_chr[MAX_OPT_LEN];
        opt->target = TGT_OST;
        if (sscanf(next, "%i%s", &opt->optarg_u.index, extra_chr) != 1
            || opt->optarg_u.index < 0)
        {
            fprintf(stderr, "Invalid ost target specification: index expected. "
                    "E.g. --target=ost:42\n");
            return EINVAL;
        }
    }
    else if (!strcasecmp(c, "pool"))
    {
        opt->target = TGT_POOL;
        opt->optarg_u.name = next;
    }
#endif
    else
    {
        fprintf(stderr, "Invalid target type '%s'. "TGT_HELP"\n", c);
        return EINVAL;
    }
    return 0;
}

/** policy and options for each policy run */
typedef struct run_item {
    int           policy_index;
    policy_opt_t  run_opt;
} run_item_t;

/** add a policy run to the list */
static int add_policy_run(run_item_t **runs, unsigned int *count,
                          const char *name, const policy_opt_t *opts)
{
    int index = -1;
    run_item_t *prun;
    int i;

    /* opts should be policy specific or the default */
    assert(opts != NULL);

    if (!policy_exists(name, &index))
    {
        fprintf(stderr, "ERROR: policy '%s' is not declared in config file.\n",
                name);
        return EINVAL;
    }

    /** check duplicates */
    for (i = 0; i < *count; i++)
    {
        if ((*runs)[i].policy_index == index)
        {
            fprintf(stderr, "ERROR: policy '%s' is invoked multiple times "
                    "in '--run' arguments\n", name);
            return EINVAL;
        }
    }

    (*count)++;
    *runs = realloc(*runs, *count * sizeof(run_item_t));
    if (*runs == NULL)
        return ENOMEM;

    prun = &((*runs)[(*count) - 1]);
    prun->policy_index = index;
    prun->run_opt = *opts;

    /* If any of the policy runs in a once shot run, assume all actions are
     * one-shot. */
    if (prun->run_opt.target != TGT_NONE)
        options.flags |= RUNFLG_ONCE;

    return 0;
}

/** parse a single argument for a policy run
 * @param[in,out] opts  Policy options resulting from policy run arguments.
 * @param[in,out] arg   String of the argument to be parsed.
 * @param[in]     name  Policy name (for error messages).
 * @param[in]     implicit true if the argument name is implicit (policy target).
 */
static int parse_policy_single_arg(policy_opt_t *opts, char *arg,
                                   const char *name, bool implicit)
{
    char *val = strchr(arg, '=');

    if (!val)
    {
        if (!implicit)
        {
            fprintf(stderr, "Invalid '--run' argument: missing parameter name "
                    "at '%s' (policy '%s')\n", arg, name);
            return EINVAL;
        }
        else
        {
            /* implicit arg name => arg is a policy target */
            return policyopt_set_target(arg, opts);
        }
    }
    *val = '\0';
    val++;

    if (!strcmp(arg, "target"))
    {
        return policyopt_set_target(val, opts);
    }
    else if (!strcmp(arg, "target-usage"))
    {
        return parse_target_usage(val, &opts->usage_pct);
    }
    else if (!strcmp(arg, "max-count"))
    {
        /* support for 'KMG...' suffixes */
        uint64_t tmp = str2size(val);

        if (tmp == (uint64_t)-1LL || tmp > UINT_MAX)
        {
            fprintf(stderr, "ERROR: invalid value '%s' for max-count: "
                   "integer (32 bits) expected.\n", val);
            return EINVAL;
        }
        opts->max_action_nbr = tmp;
    }
    else if (!strcmp(arg, "max-volume") || !strcmp(arg, "max-vol"))
    {
        /* parse val */
        uint64_t tmp;

        tmp = str2size(val);
        if (tmp == (uint64_t)-1LL)
        {
            fprintf(stderr, "ERROR: invalid value '%s' for max-vol: "
                   "<int>[KMGTPE] expected.\n", val);
            return EINVAL;
        }
        opts->max_action_vol = tmp;
    }
    else
    {
        /* error */
        fprintf(stderr, "ERROR: unexpected parameter name '%s' in run "
                "arguments for policy '%s'.\n\t'target', 'target-usage', "
                "'max-count', or 'max-vol' expected.\n", arg, name);
        return EINVAL;
    }

    return 0;
}


/** parse a list of arguments for a policy run */
static int parse_policy_args(policy_opt_t *opts, char *args, const char *name)
{
    char *curr = NULL;
    char *param;
    int rc;

    param = strtok_r(args, ",", &curr);
    if (!param)
        return 0;

    /* allow implicit parameter name for first argument (policy target) */
    rc = parse_policy_single_arg(opts, param, name, true);
    if (rc)
        return rc;

    while ((param = strtok_r(NULL, ",", &curr)))
    {
        /* don't allow implicit parameter name for next arguments */
        rc = parse_policy_single_arg(opts, param, name, false);
        if (rc)
            return rc;
    }

    return 0;
}

/** Extract a list of arguments from a string
 * like '(xxxxxxxxxxxx'),....
 * @param[out] next is set to the next policy run string
 *             or points to final '\0' if end of string is reached.
 * @return an malloc'ated string that must be freed by the caller.
 * @retval NULL on error.
 */
static char *extract_arg_list(const char *str, char **next)
{
    char *arg_end;
    char *args_tmp;
    int   arg_len;

    /*  The first char is a '(' */
    assert(str != NULL && str[0] == '(');

    /*  Match the next ')' */
    arg_end = strchr(str + 1, ')');
    if (arg_end == NULL)
    {
        fprintf(stderr, "Error in policy run specification: unmatched "
                "'(' in '%s'\n", str);
        return NULL;
    }
    /* then ',' or '\0' is expected. */
    if (arg_end[1] != ',' && arg_end[1] != '\0')
    {
        fprintf(stderr, "Error in policy run specification: ',' or "
                "end of string expected after ')', but '%s' found\n",
                arg_end + 1);
        return NULL;
    }

    /* first args char is str + 1, last args char is arg_end - 1,
     * length = last - first + 1
     */
    arg_len = (arg_end - 1) - (str + 1) + 1;
    args_tmp = strndup(str + 1, arg_len);

    if (arg_end[1] == ',')
        /* skip ',' */
        *next = arg_end + 2;
    else
        /* end of string reached */
        *next = arg_end + 1;

    return args_tmp;
}


/**
 * Read the next policy in '--run' argument.
 * @return Pointer to the next policy run string.
 * @retvall NULL on error.
 */
static const char *read_next_policy_run(run_item_t **runs, unsigned int *count,
                                        const char *param_str,
                                        const policy_opt_t *default_opt)
{
    const char *curr;
    char *name = NULL;
    char *args = NULL;

    assert(runs != NULL);
    assert(count != NULL);
    assert(param_str != NULL);
    assert(default_opt != NULL);

    /* stop at the first '(' or ',' */
    for (curr = param_str;;curr++)
    {
        /* reached end of current run */
        if (*curr == ',' || *curr == '\0')
        {
            name = strndup(param_str, curr - param_str);
            if (!name)
                return NULL;

            if (add_policy_run(runs, count, name, default_opt))
                goto err;

            free(name);

            /* return pointer to end-of-string or to the next policy run */
            return (*curr == '\0') ? curr : curr + 1;
        }
        /* starting argument list */
        if (*curr == '(')
        {
            char *next = NULL;
            policy_opt_t opts = *default_opt;

            name = strndup(param_str, curr - param_str);
            if (!name)
                return NULL;

            args = extract_arg_list(curr, &next);
            if (!args)
                goto err;

            /* parse arguments between parenthesis */
            if (parse_policy_args(&opts, args, name))
                goto err;

            if (add_policy_run(runs, count, name, &opts))
                goto err;

            free(name);

            return next;
        }
    }
err:
    free(name);
    free(args);
    return NULL;
}

/**
 * Parse policy (--run) parameters.
 * --run argument can be a list of comma-separated list of policy runs.
 *  Each policy run can include a list of arguments between parenthesis,
 *  and separated by commas.
 *  Argument names can be explicit e.g. 'target=user:foo'
 *  or implicit 'user:foo'. Arguments may depend on the target type.
 *  Example:
 *      --run=lhsm_archive(user:foo,max-count=100k),lhsm_release(ost:1,target-usage=85%)
 *
 *  Arguments from command line (e.g. --target=file:/x/y/z) are used
 *  as default for policy runs that have no specified target.
 */
static int parse_policy_runs(run_item_t **runs, unsigned int *count,
                             const char *param_str,
                             const policy_opt_t *default_opt)
{
    const char *curr;
    int i;

    /* if no policy is specified (or "all") run them all. */
    if (EMPTY_STRING(param_str) || !strcasecmp(param_str, "all"))
    {
        /* return the list of all policies */
        *count = policies.policy_count;
        *runs = calloc(*count, sizeof(run_item_t));
        if (!runs)
        {
            fprintf(stderr, "ERROR: cannot allocate memory\n");
            return ENOMEM;
        }
        for (i = 0; i < *count; i++)
        {
            (*runs)[i].policy_index = i;
            (*runs)[i].run_opt = *default_opt;
        }
        return 0;
    }

    /* split the string as: 'policy(args),policy(args),...' */
    for (curr = param_str; curr != NULL && *curr != '\0';
         curr = read_next_policy_run(runs, count, curr, default_opt))
        /* noop */;

    if (curr == NULL) /* error */
        return EINVAL;

    /* Copyback general flag to policies */
    for (i = 0; i < *count; i++)
        (*runs)[i].run_opt.flags = options.flags;

    return 0;
}


/**
 * Main daemon routine
 */
int main(int argc, char **argv)
{
    int            rc;
    bool           chgd = false;
    char           badcfg[RBH_PATH_MAX];
    int            action_mask = 0;
    char           err_msg[4096];

    /* policy runs */
    run_item_t *runs = NULL;
    unsigned int run_count = 0;

    policy_opt_t   default_policy_opt = {.target = TGT_NONE};
    const char    *bin;

    bin = rh_basename(argv[0]);

    boot_time = time(NULL);

    rc = rh_read_parameters(bin, argc, argv, &action_mask, &options);
    if (rc)
        exit((rc == -1 ? 0 : rc)); /* -1 is returned for normal exit */

    /* Template or Defaults options specified ? */
    if (options.write_template)
    {
        rc = do_write_template(options.template_file);
        exit(rc);
    }

    if (options.write_defaults)
    {
        rc = rbh_cfg_write_default(stdout);
        if (rc)
        {
            fprintf(stderr, "Error %d retrieving default configuration: %s\n", rc,
                    strerror(rc));
        }
        exit(rc);
    }

    /* initialize internal resources (glib, llapi, internal resources...) */
    rc = rbh_init_internals();
    if (rc != 0)
        exit(rc);

    /* get default config file, if not specified */
    if (SearchConfig(options.config_file, options.config_file, &chgd,
                     badcfg, MAX_OPT_LEN) != 0)
    {
        fprintf(stderr, "No config file (or too many) found matching %s\n", badcfg);
        exit(ENOENT);
    }
    else if (chgd)
        fprintf(stderr, "Using config file '%s'.\n", options.config_file);

    /* build config parsing mask */
    if (options.test_syntax)
        /* parse all configs */
        parsing_mask = 0xFFFFFFFF;
    else
        parsing_mask = action2parsing_mask(action_mask);

    /* load and set modules configuration */
    if(rbh_cfg_load(parsing_mask, options.config_file, err_msg))
    {
        fprintf(stderr, "Error reading configuration file '%s': %s\n",
                options.config_file, err_msg);
        exit(1);
    }

    if (options.test_syntax)
    {
        printf("Configuration file '%s' has been read successfully\n",
               options.config_file);
        exit(0);
    }

    /* override config file options with command line parameters */
    if (options.force_log)
        strcpy(log_config.log_file, options.log);
    else if (isatty(fileno(stderr)) && !options.detach)
    {
        options.force_log = true;
        strcpy(log_config.log_file, "stderr");
    }
    if (options.force_log_level)
        log_config.debug_level = options.log_level;

    if (action_mask & ACTION_MASK_RUN_POLICIES)
    {
        /* Parse 'target' option, if any.
         * The resulting policy_opt is used as default for policy runs. */
        rc = policyopt_set_target(options.target_string, &default_policy_opt);
        if (rc)
            exit(rc);

        /* add usage target */
        default_policy_opt.usage_pct = options.usage_target;

        /* Parse 'run' arguments. */
        rc = parse_policy_runs(&runs, &run_count, options.policy_string,
                               &default_policy_opt);
        if (rc)
            exit(rc);
    }
    else if (!EMPTY_STRING(options.target_string))
    {
        fprintf(stderr, "Warning: --target option has no effect "
                "without --run or --check-thresholds.\n");
    }

#ifdef HAVE_CHANGELOGS
    /* Only enable changelog processing for Lustre filesystems */
    if( (action_mask & ACTION_MASK_HANDLE_EVENTS)
        && ( strcmp( global_config.fs_type, "lustre" ) != 0 ) )
    {
        DisplayLog( LVL_MAJOR, MAIN_TAG, "Disabling ChangeLogs for this non-lustre filesystem" );
        action_mask &= ~ACTION_MASK_HANDLE_EVENTS;
    }

/* if the filesystem supports changelogs and a scan is requested
 * and the once option is not set, display a warning */
    if ((action_mask & ACTION_MASK_SCAN) && !(options.flags & RUNFLG_ONCE)
        && !(action_mask & ACTION_MASK_HANDLE_EVENTS)
        && strcmp(global_config.fs_type, "lustre") == 0)
    {
        fprintf(stderr, "ADVICE: this filesystem is changelog-capable, you should use changelogs instead of scanning.\n");
    }
#endif

    /* Initialize logging */
    rc = InitializeLogs(bin);
    if (rc)
    {
        fprintf(stderr, "Error opening log files: rc=%d, errno=%d: %s\n",
                 rc, errno, strerror(errno));
        exit(rc);
    }

    /* deamonize program if detach flag is set */
    if (options.detach)
    {
        rc = daemon(0, 0);

        if (rc)
        {
            DisplayLog(LVL_CRIT, MAIN_TAG, "Error detaching process from parent: %s",
                        strerror(errno));
            fprintf(stderr, "Error detaching process from parent: %s\n", strerror(errno));
            exit(1);
        }
    }

    if (options.pid_file)
        create_pid_file(options.pid_filepath);

    /* Initialize filesystem access */
    rc = InitFS();
    if (rc)
        exit(rc);

    /* Initialize status managers */
    rc = smi_init_all(options.flags);
    if (rc)
        exit(rc);

    /* create signal handling thread */
    rc = pthread_create(&sig_thr, NULL, signal_handler_thr, NULL);
    if (rc)
    {
        DisplayLog(LVL_CRIT, MAIN_TAG, "Error starting signal handler thread: %s",
                    strerror(errno));
        exit(1);
    }
    else
        DisplayLog(LVL_VERB, MAIN_TAG, "Signal handler thread started successfully");

    /* Initialize list manager */
    rc = ListMgr_Init(false);
    if (rc)
    {
        DisplayLog(LVL_CRIT, MAIN_TAG, "Error %d initializing list manager", rc);
        exit(rc);
    }
    else
        DisplayLog(LVL_VERB, MAIN_TAG, "ListManager successfully initialized");

    if (CheckLastFS() != 0)
        exit(1);

    if (options.flags & RUNFLG_ONCE)
    {
        /* used for dumping stats in one shot mode */
        pthread_create(&stat_thread, NULL, stats_thr, NULL);
    }

    if ( action_mask & ( ACTION_MASK_SCAN | ACTION_MASK_HANDLE_EVENTS ) )
    {
        if (!attr_mask_is_null(options.diff_mask))
            /* convert status[0] to all status flags */
            options.diff_mask = translate_all_status_mask(options.diff_mask);

        /* Initialize Pipeline */
#ifdef _BENCH_PIPELINE
        int nb_stages = 3;
        rc = EntryProcessor_Init(0, options.flags, &nb_stages);
#else
        rc = EntryProcessor_Init(STD_PIPELINE, options.flags, &options.diff_mask);
#endif
        if ( rc )
        {
            DisplayLog( LVL_CRIT, MAIN_TAG, "Error %d initializing EntryProcessor pipeline", rc );
            exit( rc );
        }
        else
            DisplayLog( LVL_VERB, MAIN_TAG, "EntryProcessor successfully initialized" );
    }

    /* Note: in 'one-shot' mode, we must take care of performing action in the correct order:
     * First scan, then process changelogs, then migrate, then purge, etc.
     */

    if ( action_mask & ACTION_MASK_SCAN )
    {

        /* Start FS scan */
        if (options.partial_scan)
            rc = FSScan_Start(options.flags, options.partial_scan_path);
        else
            rc = FSScan_Start(options.flags, NULL);

        if ( rc )
        {
            DisplayLog( LVL_CRIT, MAIN_TAG, "Error %d initializing FS Scan module", rc );
            exit( rc );
        }
        else
            DisplayLog( LVL_VERB, MAIN_TAG, "FS Scan module successfully initialized" );

        /* Flush logs now, to have a trace in the logs */
        FlushLogs(  );

        if (options.flags & RUNFLG_ONCE)
            running_mask = MODULE_MASK_FS_SCAN | MODULE_MASK_ENTRY_PROCESSOR;
        else
            running_mask |= MODULE_MASK_FS_SCAN | MODULE_MASK_ENTRY_PROCESSOR;

        if (options.flags & RUNFLG_ONCE)
        {
            FSScan_Wait(  );
            DisplayLog( LVL_MAJOR, MAIN_TAG, "FS Scan finished" );
        }
    }

#ifdef HAVE_CHANGELOGS
    if ( action_mask & ACTION_MASK_HANDLE_EVENTS )
    {

        /* Start reading changelogs */
        rc = cl_reader_start(options.flags, options.mdtidx);
        if ( rc )
        {
            DisplayLog( LVL_CRIT, MAIN_TAG, "Error %d initializing ChangeLog Reader", rc );
            exit( rc );
        }
        else
            DisplayLog( LVL_VERB, MAIN_TAG, "ChangeLog Reader successfully initialized" );

        /* Flush logs now, to have a trace in the logs */
        FlushLogs(  );

        if (options.flags & RUNFLG_ONCE)
            running_mask = MODULE_MASK_EVENT_HDLR | MODULE_MASK_ENTRY_PROCESSOR;
        else
            running_mask |= MODULE_MASK_EVENT_HDLR | MODULE_MASK_ENTRY_PROCESSOR;

        if (options.flags & RUNFLG_ONCE)
        {
            cl_reader_wait();
            DisplayLog( LVL_MAJOR, MAIN_TAG, "Event Processing finished" );
        }
    }
#endif

    if ( (options.flags & RUNFLG_ONCE) && (  action_mask & ( ACTION_MASK_SCAN | ACTION_MASK_HANDLE_EVENTS ) ) )
    {
        /* Pipeline must be flushed */
        EntryProcessor_Terminate(true);

#ifdef HAVE_CHANGELOGS
        if ( action_mask & ACTION_MASK_HANDLE_EVENTS )
        {
            /* Ack last changelog records. */
            cl_reader_done();
        }
#endif
        running_mask = 0;
    }

    if (action_mask & ACTION_MASK_RUN_POLICIES)
    {
        int i;
        /* allocate policy_run structure */
        policy_run = calloc(run_count, sizeof(policy_info_t));
        if (!policy_run)
        {
            DisplayLog(LVL_CRIT, MAIN_TAG, "Cannot allocate memory");
            exit(1);
        }
        policy_run_cpt = run_count;

        for (i = 0; i < run_count; i++)
        {
            unsigned int pol_idx = runs[i].policy_index;

            rc = policy_module_start(&policy_run[i],
                                     &policies.policy_list[pol_idx],
                                     &run_cfgs.configs[pol_idx],
                                     &runs[i].run_opt);
            if (rc == ENOENT)
            {
                DisplayLog(LVL_CRIT, MAIN_TAG, "Policy %s is disabled.",
                           policies.policy_list[pol_idx].name);
                continue;
            }
            else if (rc)
            {
                fprintf(stderr, "Error %d initializing Migration module\n", rc);
                exit(rc);
            }
            else
            {
                DisplayLog(LVL_VERB, MAIN_TAG, "Policy %s successfully initialized",
                           policies.policy_list[pol_idx].name);
                /* Flush logs now, to have a trace in the logs */
                FlushLogs();
            }

            /* For 'one-shot' mode, run policy after policy */
            if (options.flags & RUNFLG_ONCE)
            {
                running_mask = MODULE_MASK_POLICY_RUN;
                policy_run_mask = (1LL << i);
                rc = policy_module_wait(&policy_run[i]);
                policy_run_mask = 0;
                running_mask = 0;
                DisplayLog(LVL_MAJOR, MAIN_TAG,
                           "%s: policy run terminated (rc = %d).",
                           policies.policy_list[pol_idx].name,
                           rc);
            }
            else
               policy_run_mask |= (1LL << i);
        }
        if (!(options.flags & RUNFLG_ONCE) && (policy_run_mask != 0))
            running_mask |= MODULE_MASK_POLICY_RUN;
    }

    if (!(options.flags & RUNFLG_ONCE))
    {
        char tmpstr[1024];

        if (!running_mask)
        {
            DisplayLog(LVL_MAJOR, MAIN_TAG, "Nothing started.");
            exit(1);
        }

        running_mask2str(running_mask, policy_run_mask, tmpstr);
        DisplayLog(LVL_MAJOR, MAIN_TAG, "Daemon started (running modules: %s)", tmpstr);
        FlushLogs();

        /* dump stats periodically */
        stats_thr(&running_mask);

        /* should never return */
        exit(1);
    }
    else
    {
        DisplayLog( LVL_MAJOR, MAIN_TAG, "All tasks done! Exiting." );
        exit( 0 );
    }

    return 0;                   /* for compiler */

}
