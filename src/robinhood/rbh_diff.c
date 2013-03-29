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

#ifdef HAVE_RMDIR_POLICY
#include "rmdir.h"
#endif

#ifdef HAVE_MIGR_POLICY
#include "migration.h"
#endif

#ifdef HAVE_PURGE_POLICY
#include "resource_monitor.h"
#endif

#include "uidgidcache.h"
#include "list_mgr.h"
#include "RobinhoodConfig.h"
#include "RobinhoodLogs.h"
#include "RobinhoodMisc.h"
#include "cmd_helpers.h"

#include <unistd.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <libgen.h>
#include <pthread.h>
#include <fcntl.h>              /* for open flags */
#include <signal.h>

#ifdef _LUSTRE
#include "lustre_extended_types.h"
#endif

#ifdef _HSM_LITE
#include "backend_mgr.h"
#endif

#define DIFF_TAG    "diff"

static time_t  start_time;

/* Array of options for getopt_long().
 * Each record consists of: { const char *name, int has_arg, int * flag, int val }
 */

static struct option option_tab[] = {

    /* diff options */
    {"scan", required_argument, NULL, 's'}, /* for partial scan */
    {"apply", optional_argument, NULL, 'a'},/* to apply on DB or FS */
    {"diff", required_argument, NULL, 'd'}, /* list of diff attrs (default is all) */

    /* config file options */
    {"config-file", required_argument, NULL, 'f'},

    /* log options */
    {"log-level", required_argument, NULL, 'l'},

    /* miscellaneous options */
    {"help", no_argument, NULL, 'h'},
    {"version", no_argument, NULL, 'V'},

    {NULL, 0, NULL, 0}
};

#define SHORT_OPT_STRING    "s:a:d:f:l:hV"

#define MAX_OPT_LEN 1024
#define MAX_TYPE_LEN 256

typedef struct diff_options {
    int            flags;
    char           config_file[MAX_OPT_LEN];
    int            force_log_level;
    int            log_level;
    int            partial_scan;
    char           partial_scan_path[RBH_PATH_MAX];
    int            diff_mask;
} diff_options;

static inline void zero_options(struct diff_options * opts)
{
    /* default value is 0 for most options */
    memset(opts, 0, sizeof(struct diff_options));
    opts->flags = FLAG_ONCE;
}


/* program options from command line  */
static struct diff_options options;

/* special character sequences for displaying help */

/* Bold start character sequence */
#define _B "[1m"
/* Bold end charater sequence */
#define B_ "[m"

/* Underline start character sequence */
#define _U "[4m"
/* Underline end character sequence */
#define U_ "[0m"

static const char *help_string =
    _B "%s"B_"\n"
    "    List differences between robinhood database and the filesystem.\n"
    "\n"
    _B "Options:" B_ "\n"
    "    " _B "-s" B_" "_U"dir"U_", " _B "--scan" B_ "=" _U "dir" U_ "\n"
    "        Only scan the specified subdir.\n"
    "    " _B "-d" B_" "_U"attrset"U_", " _B "--diff"B_"="_U"attrset"U_ " :\n"
    "        Display changes for the given set of attributes.\n"
    "        "_U"attrset"U_" is a list of options in: path,posix,stripe,all,notimes,noatime.\n"
    "    " _B "-a" B_" {fs|db}, " _B "--apply" B_ "[={fs|db}]\n"
    "        Apply changes to the database (db) or to the filesystem (fs).\n"
    "        If no argument is specified, apply to the database.\n"
    "\n"
    "    " _B "-f" B_ " " _U "file" U_ ", " _B "--config-file=" B_ _U "file" U_ "\n"
    "        Path to configuration file (or short name).\n"
    "    " _B "-l" B_ " " _U "level" U_ ", " _B "--log-level=" B_ _U "level" U_ "\n"
    "        Force the log verbosity level (overrides configuration value).\n"
    "        Allowed values: CRIT, MAJOR, EVENT, VERB, DEBUG, FULL.\n" "\n" _B
    "    " _B "-h" B_ ", " _B "--help" B_ "\n"
    "        Display a short help about command line options.\n"
    "    " _B "-V" B_ ", " _B "--version" B_ "\n"
    "        Display version info\n";


static inline void display_help( char *bin_name )
{
    printf( help_string, bin_name );
}

static inline void display_version( char *bin_name )
{
    printf( "\n" );
    printf( "Product:         " PACKAGE_NAME "\n" );
    printf( "Version:         " PACKAGE_VERSION "-"RELEASE"\n" );
    printf( "Build:           " COMPIL_DATE "\n" );
    printf( "\n" );
    printf( "Compilation switches:\n" );

/* purpose of this daemon */
#ifdef _LUSTRE_HSM
    printf( "    Lustre-HSM Policy Engine\n" );
#elif defined(_TMP_FS_MGR)
    printf( "    Temporary filesystem manager\n" );
#elif defined(_HSM_LITE)
    printf( "    Basic HSM binding\n" );
#else
#error "No purpose was specified"
#endif

/* Access by Fid ? */
#ifdef _HAVE_FID
    printf( "    Address entries by FID\n" );
#else
    printf( "    Address entries by path\n" );
#endif
#ifdef HAVE_CHANGELOGS
    printf( "    MDT Changelogs supported\n" );
#else
    printf( "    MDT Changelogs disabled\n" );
#endif


    printf( "\n" );
#ifdef _LUSTRE
#ifdef LUSTRE_VERSION
    printf( "Lustre Version: " LUSTRE_VERSION "\n" );
#else
    printf( "Lustre FS support\n" );
#endif
#else
    printf( "No Lustre support\n" );
#endif

#ifdef _MYSQL
    printf( "Database binding: MySQL\n" );
#elif defined(_SQLITE)
    printf( "Database binding: SQLite\n" );
#else
#error "No database was specified"
#endif
    printf( "\n" );
    printf( "Report bugs to: <" PACKAGE_BUGREPORT ">\n" );
    printf( "\n" );
}

/* must be global so termination on signal handler can clear tag resources */
const char     *db_tag = NULL;

static pthread_t stat_thread;

/* database connexion for updating stats */
static lmgr_t  lmgr;
static int     lmgr_init = FALSE;
static char    start_time_str[256];

static inline int ensure_db_access()
{
    if ( !lmgr_init )
    {
        if ( ListMgr_InitAccess( &lmgr ) != DB_SUCCESS )
            return 0;
        lmgr_init = TRUE;
    }
    return 1;
}

static void dump_stats( lmgr_t * lmgr )
{
        char           tmp_buff[256];
        time_t         now;
        struct tm      date;

        now = time( NULL );
        strftime( tmp_buff, 256, "%Y/%m/%d %T", localtime_r( &now, &date ) );

        DisplayLog( LVL_MAJOR, "STATS",
                    "==================== Dumping stats at %s =====================", tmp_buff );
        DisplayLog( LVL_MAJOR, "STATS", "Diff start time: %s", start_time_str );

        FSScan_DumpStats();
        EntryProcessor_DumpCurrentStages();

        /* Flush stats */
        FlushLogs(  );
}

static void  *stats_thr( void *arg )
{
    struct tm      date;

    strftime( start_time_str, 256, "%Y/%m/%d %T", localtime_r( &start_time, &date ) );

    if (!ensure_db_access())
        return NULL;

    DisplayLog( LVL_VERB, DIFF_TAG, "Statistics thread started" );

    while ( 1 )
    {
        WaitStatsInterval(  );
        dump_stats(&lmgr);
    }
}


static int     terminate_sig = 0;
static int     dump_sig = FALSE;
static pthread_t sig_thr;

#define SIGHDL_TAG  "SigHdlr"

static void terminate_handler( int sig )
{
    terminate_sig = sig;
}

static void usr_handler( int sig )
{
    dump_sig = TRUE;
}


static void   *signal_handler_thr( void *arg )
{
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
        if (db_tag != NULL && ensure_db_access())
            ListMgr_DestroyTag(&lmgr, db_tag);
        exit( 1 );
    }
    else
        DisplayLog( LVL_EVENT, SIGHDL_TAG,
                    "Signals SIGTERM and SIGINT (daemon shutdown) are ready to be used" );

    memset( &act_sigusr, 0, sizeof( act_sigusr ) );
    act_sigusr.sa_flags = 0;
    act_sigusr.sa_handler = usr_handler;
    if ( sigaction( SIGUSR1, &act_sigusr, NULL ) == -1 )
    {
        DisplayLog( LVL_CRIT, SIGHDL_TAG, "Error while setting signal handlers for SIGUSR1: %s",
                    strerror( errno ) );
        if (db_tag != NULL && ensure_db_access())
            ListMgr_DestroyTag(&lmgr, db_tag);
        exit( 1 );
    }
    else
        DisplayLog( LVL_EVENT, SIGHDL_TAG, "Signal SIGUSR1 (stats dump) is ready to be used" );


    /* signal flag checking loop */

    while ( 1 )
    {
        /* check for signal every second */
        rh_sleep( 1 );

        if ( terminate_sig != 0 )
        {
            if ( terminate_sig == SIGTERM )
                DisplayLog( LVL_MAJOR, SIGHDL_TAG, "SIGTERM received: performing clean daemon shutdown" );
            else if ( terminate_sig == SIGINT )
                DisplayLog( LVL_MAJOR, SIGHDL_TAG, "SIGINT received: performing clean daemon shutdown" );
            FlushLogs(  );

            /* stop FS scan (blocking) */
            FSScan_Terminate(  );
            FlushLogs(  );

            /* drop pipeline waiting operations and terminate threads */
            EntryProcessor_Terminate( FALSE );
            FlushLogs(  );

#ifdef _HSM_LITE
            /* shutdown backend access */
            Backend_Stop();
#endif

            DisplayLog( LVL_MAJOR, SIGHDL_TAG, "Exiting." );
            FlushLogs(  );

            if (db_tag != NULL && ensure_db_access())
                ListMgr_DestroyTag(&lmgr, db_tag);

            /* indicate the process terminated due to a signal */
            exit( 128 + terminate_sig );
        }
        else if ( dump_sig )
        {
            DisplayLog( LVL_MAJOR, SIGHDL_TAG, "SIGUSR1 received: dumping stats" );

            if (!ensure_db_access())
                return NULL;
            dump_stats(&lmgr);
            dump_sig = FALSE;
        }
    }
}



/**
 * Main daemon routine
 */
int main( int argc, char **argv )
{
    int            c, option_index = 0;
    char          *bin = basename( argv[0] );

    int            rc;
    char           err_msg[4096];
    robinhood_config_t rh_config;
    int chgd = 0;
    char           badcfg[RBH_PATH_MAX];
    char           tag_name[256] = "";

    start_time = time( NULL );

    zero_options( &options );

    /* parse command line options */
    while ( ( c = getopt_long( argc, argv, SHORT_OPT_STRING, option_tab, &option_index ) ) != -1 )
    {
        switch ( c )
        {
        case 's':
            options.partial_scan = TRUE;
            strncpy(options.partial_scan_path, optarg, RBH_PATH_MAX);
            /* clean final slash */
            if (FINAL_SLASH(options.partial_scan_path))
                REMOVE_FINAL_SLASH(options.partial_scan_path);
            break;

        case 'd':
            if (parse_diff_mask(optarg, &options.diff_mask, err_msg))
            {
                fprintf(stderr,
                        "Invalid argument for --diff: %s\n", err_msg);
                exit( 1 );
            }
            break;

        case 'a':
            if (optarg)
            {
                if (!strcasecmp(optarg,"fs"))
                    options.flags |= FLAG_APPLY_FS;
                else if (!strcasecmp(optarg,"db"))
                    options.flags |= FLAG_APPLY_DB;
                else
                {
                    fprintf(stderr, "Invalid argument for --apply: '%s' (fs or db expected)\n",
                            optarg);
                    exit( 1 );
                }
            }
            else
                options.flags |= FLAG_APPLY_DB;
            break;

        case 'f':
            strncpy( options.config_file, optarg, MAX_OPT_LEN );
            break;
        case 'l':
            options.force_log_level = TRUE;
            options.log_level = str2debuglevel( optarg );
            if ( options.log_level == -1 )
            {
                fprintf( stderr,
                         "Unsupported log level '%s'. CRIT, MAJOR, EVENT, VERB, DEBUG or FULL expected.\n",
                         optarg );
                exit( 1 );
            }
            break;
        case 'h':
            display_help( bin );
            exit( 0 );
            break;
        case 'V':
            display_version( bin );
            exit( 0 );
            break;
        case ':':
        case '?':
        default:
            fprintf(stderr,"Run '%s --help' for more details.\n", bin);
            exit( 1 );
            break;
        }
    }

    /* check there is no extra arguments */
    if ( optind != argc )
    {
        fprintf( stderr, "Error: unexpected argument on command line: %s\n", argv[optind] );
        exit( 1 );
    }

    /* Initialize global tools */
#ifdef _LUSTRE
    if ( ( rc = Lustre_Init(  ) ) )
    {
        fprintf( stderr, "Error %d initializing liblustreapi\n", rc );
        exit( 1 );
    }
#endif

    /* Initilize uidgid cache */
    if ( InitUidGid_Cache(  ) )
    {
        fprintf( stderr, "Error initializing uid/gid cache\n" );
        exit( 1 );
    }

    /* get default config file, if not specified */
    if ( SearchConfig( options.config_file, options.config_file, &chgd, badcfg ) != 0 )
    {
        fprintf(stderr, "No config file found matching %s\n", badcfg);
        exit(2);
    }
    else if (chgd)
    {
        fprintf(stderr, "Using config file '%s'.\n", options.config_file );
    }

    if ( ReadRobinhoodConfig( MODULE_MASK_FS_SCAN | MODULE_MASK_ENTRY_PROCESSOR,
                              options.config_file, err_msg,
                              &rh_config, FALSE ) )
    {
        fprintf( stderr, "Error reading configuration file '%s': %s\n",
                 options.config_file, err_msg );
        exit( 1 );
    }
    process_config_file = options.config_file;

    /* set global configuration */
    global_config = rh_config.global_config;

    /* set policies info */
    policies = rh_config.policies;

    if (options.force_log_level)
        rh_config.log_config.debug_level = options.log_level;
    else
        rh_config.log_config.debug_level = LVL_CRIT; /* no event message */

    /* Set logging to stderr */
    strcpy( rh_config.log_config.log_file, "stderr" );
    strcpy( rh_config.log_config.report_file, "stderr" );
    strcpy( rh_config.log_config.alert_file, "stderr" );

    /* Initialize logging */
    rc = InitializeLogs( bin, &rh_config.log_config );
    if ( rc )
    {
        fprintf( stderr, "Error opening log files: rc=%d, errno=%d: %s\n",
                 rc, errno, strerror( errno ) );
        exit( rc );
    }

    /* Initialize filesystem access */
    rc = InitFS();
    if (rc)
        exit(rc);

#ifdef _HSM_LITE
    rc = Backend_Start( &rh_config.backend_config, options.flags );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, DIFF_TAG, "Error initializing backend" );
        exit( 1 );
    }
#endif

    /* Initialize list manager */
    rc = ListMgr_Init( &rh_config.lmgr_config, FALSE );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, DIFF_TAG, "Error %d initializing list manager", rc );
        exit( rc );
    }
    else
        DisplayLog( LVL_VERB, DIFF_TAG, "ListManager successfully initialized" );


    if ( CheckLastFS(  ) != 0 )
        exit( 1 );

    if (options.diff_mask)
        rh_config.entry_proc_config.diff_mask = options.diff_mask;
    else
    {
        /* parse "all" */
        char tmpstr[] = "all";
        if (parse_diff_mask(tmpstr, &rh_config.entry_proc_config.diff_mask, err_msg))
        {
            DisplayLog(LVL_CRIT, DIFF_TAG, "unexpected error parsing diff mask: %s", err_msg);
            exit(1);
        }
    }

    /* if no DB apply action is specified, can't use md_update field for checking
     * removed entries. So, create a special tag for that. */
    if (!(options.flags & FLAG_APPLY_DB))
    {
        /* create a connexion to the DB. this is safe to use the global lmgr var
         * as statistics thread is not running */
        if (!ensure_db_access())
            exit(1);
        /* create a tag to clear entries after the scan */

        /* There could be several diff running in parallel,
         * so set a suffix to avoid conflicts */
        sprintf(tag_name, "DIFF_%u", (unsigned int) getpid());

        /* add filter for partial scan */
        if (options.partial_scan)
        {
            lmgr_filter_t  filter;
            filter_value_t val;
            lmgr_simple_filter_init( &filter );

            char tmp[RBH_PATH_MAX];
            strcpy(tmp, options.partial_scan_path);
            strcat(tmp, "/*");
            val.val_str = tmp;
            lmgr_simple_filter_add(&filter, ATTR_INDEX_fullpath, LIKE, val, 0);

            rc = ListMgr_CreateTag(&lmgr, tag_name, &filter, FALSE);
            lmgr_simple_filter_free(&filter);
        }
        else
            rc = ListMgr_CreateTag(&lmgr, tag_name, NULL, FALSE);

        if (rc)
            exit(rc);

        db_tag = tag_name;
    }

    /* Initialise Pipeline */
    rc = EntryProcessor_Init(&rh_config.entry_proc_config, DIFF_PIPELINE,
                             options.flags, db_tag);
    if ( rc )
    {
        DisplayLog( LVL_CRIT, DIFF_TAG, "Error %d initializing EntryProcessor pipeline", rc );
        goto clean_tag;
    }
    else
        DisplayLog( LVL_VERB, DIFF_TAG, "EntryProcessor successfully initialized" );


    /* Start FS scan */
    if (options.partial_scan)
        rc = FSScan_Start(&rh_config.fs_scan_config, options.flags,
                          options.partial_scan_path);
    else
        rc = FSScan_Start(&rh_config.fs_scan_config, options.flags, NULL);

    if ( rc )
    {
        DisplayLog( LVL_CRIT, DIFF_TAG, "Error %d initializing FS Scan module", rc );
        goto clean_tag;
    }
    else
        DisplayLog( LVL_VERB, DIFF_TAG, "FS Scan module successfully initialized" );

    /* Flush logs now, to have a trace in the logs */
    FlushLogs(  );

    /* both pipeline and scan are now running, can now trap events and display stats */

    /* create signal handling thread */
    rc = pthread_create( &sig_thr, NULL, signal_handler_thr, NULL );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, DIFF_TAG, "Error starting signal handler thread: %s",
                    strerror( errno ) );
        goto clean_tag;
    }
    else
        DisplayLog( LVL_VERB, DIFF_TAG, "Signal handler thread started successfully" );

    pthread_create(&stat_thread, NULL, stats_thr, NULL);

    /* wait for FS scan to end */
    FSScan_Wait(  );
    DisplayLog( LVL_MAJOR, DIFF_TAG, "FS Scan finished" );

    /* Pipeline must be flushed */
    EntryProcessor_Terminate( TRUE );


    DisplayLog( LVL_MAJOR, DIFF_TAG, "All tasks done! Exiting." );
    rc = 0;


clean_tag:
    /* destroy the tag before exit */
    if (db_tag != NULL && ensure_db_access())
        ListMgr_DestroyTag(&lmgr, db_tag);

    exit(rc);
    return rc; /* for compiler */
}
