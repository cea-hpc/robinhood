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

#include <unistd.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <libgen.h>
#include <pthread.h>
#include <fcntl.h>              /* for open flags */
#include <signal.h>

#ifdef _LUSTRE
#include <lustre/liblustreapi.h>
#endif

#ifdef _HSM_LITE
#include "backend_mgr.h"
#endif

#define MAIN_TAG    "Main"

static time_t  boot_time;

/* values over max char index */
#define FORCE_OST_PURGE   260
#define FORCE_FS_PURGE    261
#define FORCE_CLASS_PURGE 262

#define FORCE_OST_MIGR    270
#define FORCE_USER_MIGR   271
#define FORCE_GROUP_MIGR  272
#define FORCE_CLASS_MIGR  273
#define MIGR_ONE_FILE     274

#define DRY_RUN           280
#define NO_LIMIT          281
#define TEST_SYNTAX       282

#define ACTION_MASK_SCAN                0x00000001
#define ACTION_MASK_PURGE               0x00000002
#define ACTION_MASK_MIGRATE             0x00000004
#define ACTION_MASK_HANDLE_EVENTS       0x00000008
#define ACTION_MASK_UNLINK              0x00000010
#define ACTION_MASK_RMDIR               0x00000020

#ifdef _LUSTRE_HSM

#define DEFAULT_ACTION_MASK     (ACTION_MASK_PURGE | ACTION_MASK_MIGRATE | ACTION_MASK_HANDLE_EVENTS | ACTION_MASK_UNLINK )
#define DEFAULT_ACTION_HELP   "--read-log --purge --migrate --hsm-remove"


#elif defined (_TMP_FS_MGR )

#   ifdef HAVE_CHANGELOGS
#       define DEFAULT_ACTION_MASK     (ACTION_MASK_HANDLE_EVENTS | ACTION_MASK_PURGE | ACTION_MASK_RMDIR)
#       define DEFAULT_ACTION_HELP   "--read-log --purge --rmdir"
#   else
#       define DEFAULT_ACTION_MASK     (ACTION_MASK_SCAN | ACTION_MASK_PURGE | ACTION_MASK_RMDIR)
#       define DEFAULT_ACTION_HELP   "--scan --purge --rmdir"
#   endif

#elif defined (_SHERPA)

#   ifdef HAVE_CHANGELOGS
#       define DEFAULT_ACTION_MASK     (ACTION_MASK_HANDLE_EVENTS | ACTION_MASK_PURGE | ACTION_MASK_RMDIR | ACTION_MASK_MIGRATE)
#       define DEFAULT_ACTION_HELP   "--read-log --purge --rmdir --migrate"
#   else
#       define DEFAULT_ACTION_MASK     (ACTION_MASK_SCAN | ACTION_MASK_PURGE | ACTION_MASK_RMDIR | ACTION_MASK_MIGRATE)
#       define DEFAULT_ACTION_HELP   "--scan --purge --rmdir --migrate"
#   endif

#elif defined (_HSM_LITE )

#ifdef HAVE_SHOOK
#   ifdef HAVE_CHANGELOGS
#       define DEFAULT_ACTION_MASK     (ACTION_MASK_HANDLE_EVENTS | ACTION_MASK_UNLINK | ACTION_MASK_MIGRATE | ACTION_MASK_PURGE)
#       define DEFAULT_ACTION_HELP   "--read-log --migrate --hsm-remove --purge"
#   else
#       define DEFAULT_ACTION_MASK     (ACTION_MASK_SCAN | ACTION_MASK_UNLINK | ACTION_MASK_MIGRATE | ACTION_MASK_PURGE)
#       define DEFAULT_ACTION_HELP   "--scan --migrate --hsm-remove --purge"
#   endif
#else
#   ifdef HAVE_CHANGELOGS
#       define DEFAULT_ACTION_MASK     (ACTION_MASK_HANDLE_EVENTS | ACTION_MASK_UNLINK | ACTION_MASK_MIGRATE)
#       define DEFAULT_ACTION_HELP   "--read-log --migrate --hsm-remove"
#   else
#       define DEFAULT_ACTION_MASK     (ACTION_MASK_SCAN | ACTION_MASK_UNLINK | ACTION_MASK_MIGRATE)
#       define DEFAULT_ACTION_HELP   "--scan --migrate --hsm-remove"
#   endif
#endif

#endif

static int     action_mask = DEFAULT_ACTION_MASK;

/* Array of options for getopt_long().
 * Each record consists of: { const char *name, int has_arg, int * flag, int val }
 */

static struct option option_tab[] = {

    /* Actions selectors */
    {"scan", no_argument, NULL, 'S'},
#ifdef HAVE_PURGE_POLICY
    {"purge", no_argument, NULL, 'P'},
    {"release", no_argument, NULL, 'P'},
    {"check-watermarks", no_argument, NULL, 'C'},
    {"check-thresholds", no_argument, NULL, 'C'},
#endif
#ifdef HAVE_MIGR_POLICY
    {"migrate", no_argument, NULL, 'M'},
    {"archive", no_argument, NULL, 'M'},
    {"sync", no_argument, NULL, 's'},
#endif
#ifdef HAVE_CHANGELOGS
    {"readlog", no_argument, NULL, 'r'},
    {"read-log", no_argument, NULL, 'r'},
    {"handle-events", no_argument, NULL, 'r'}, /* for backward compatibility */
#endif

    /* XXX we use the same letter 'R' for the 2 purposes
     * because there are never used together */
#ifdef HAVE_RM_POLICY
    {"hsm-remove", no_argument, NULL, 'R'},
    {"hsm-rm", no_argument, NULL, 'R'},
#endif
#ifdef HAVE_RMDIR_POLICY
    {"rmdir", no_argument, NULL, 'R'},
#endif

    /* purge by ... */
#ifdef HAVE_PURGE_POLICY
#ifdef _LUSTRE
    {"purge-ost", required_argument, NULL, FORCE_OST_PURGE},
    {"release-ost", required_argument, NULL, FORCE_OST_PURGE},
#endif
    {"purge-fs", required_argument, NULL, FORCE_FS_PURGE},
    {"release-fs", required_argument, NULL, FORCE_FS_PURGE},

    {"purge-class", required_argument, NULL, FORCE_CLASS_PURGE},
    {"release-class", required_argument, NULL, FORCE_CLASS_PURGE},
#endif

#ifdef HAVE_MIGR_POLICY
    /* migration by ... */
#ifdef _LUSTRE
    {"migrate-ost", required_argument, NULL, FORCE_OST_MIGR},
    {"archive-ost", required_argument, NULL, FORCE_OST_MIGR},
#endif
    {"migrate-user", required_argument, NULL, FORCE_USER_MIGR},
    {"archive-user", required_argument, NULL, FORCE_USER_MIGR},
    {"migrate-group", required_argument, NULL, FORCE_GROUP_MIGR},
    {"archive-group", required_argument, NULL, FORCE_GROUP_MIGR},
    {"migrate-class", required_argument, NULL, FORCE_CLASS_MIGR},
    {"archive-class", required_argument, NULL, FORCE_CLASS_MIGR},

    {"migrate-file", required_argument, NULL, MIGR_ONE_FILE},
    {"archive-file", required_argument, NULL, MIGR_ONE_FILE},
#endif

    /* For purge and migration actions,
     * this forces to migrate/purge all eligible files,
     * by ignoring policy conditions.
     */
    {"ignore-policies", no_argument, NULL, 'i'},

    /* behavior flags */
    {"dry-run", no_argument, NULL, DRY_RUN},
    {"one-shot", no_argument, NULL, 'O'}, /* for backward compatibility */
    {"once", no_argument, NULL, 'O'},
    {"detach", no_argument, NULL, 'd'},
    {"no-limit", no_argument, NULL, NO_LIMIT},

    /* config file options */
    {"config-file", required_argument, NULL, 'f'},
    {"template", optional_argument, NULL, 'T'},
    {"defaults", no_argument, NULL, 'D'},
    {"test-syntax", no_argument, NULL, TEST_SYNTAX},

    /* override config file options */
    {"fs-path", required_argument, NULL, 'F'},
    {"fs-type", required_argument, NULL, 't'},
    {"log-file", required_argument, NULL, 'L'},
    {"log-level", required_argument, NULL, 'l'},

    /* miscellaneous options */
    {"help", no_argument, NULL, 'h'},
    {"version", no_argument, NULL, 'V'},
    {"pid-file", required_argument, NULL, 'p'},

    {NULL, 0, NULL, 0}

};

#define SHORT_OPT_STRING    "CSPMsRUrOdf:T:DF:t:L:l:hVp:i"

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
    _B "Usage:" B_ " %s [options]\n"
    "\n"
    _B "Action switches:" B_ "\n"
    "    " _B "-S" B_ ", " _B "--scan" B_ "\n"
    "        Scan filesystem namespace.\n"
#ifdef HAVE_PURGE_POLICY
    "    " _B "-P" B_ ", " _B "--purge" B_ "\n"
    "        Purge non-directory entries according to policy.\n"
    "    " _B "-C" B_ ", " _B "--check-thresholds" B_ "\n"
    "        Only check thresholds without purging.\n"
#endif
#ifdef HAVE_RMDIR_POLICY
    "    " _B "-R" B_ ", " _B "--rmdir" B_ "\n"
    "        Remove empty directories according to policy.\n"
#endif
#ifdef HAVE_MIGR_POLICY
    "    " _B "-M" B_ ", " _B "--migrate" B_ "\n"
    "        Copy \"dirty\" entries to HSM.\n"
#endif
#ifdef HAVE_CHANGELOGS
    "    " _B "-r" B_ ", " _B "--read-log" B_ "\n"
    "        Read events from MDT ChangeLog.\n"
#endif
#ifdef HAVE_RM_POLICY
    "    " _B "-R" B_ ", " _B "--hsm-remove" B_ "\n"
    "        Perform deferred removal in HSM.\n"
#endif
    "\n"
    "    Default is: "DEFAULT_ACTION_HELP"\n"
    "\n"
#ifdef HAVE_PURGE_POLICY
    _B "Manual purge actions:" B_ "\n"
#ifdef _LUSTRE
    "    " _B "--purge-ost=" B_ _U "ost_index" U_ "," _U "target_usage_pct" U_ "\n"
    "        Apply purge policy on OST " _U "ost_index" U_ " until its usage reaches the specified value.\n"
#endif
    "    " _B "--purge-fs=" B_ _U "target_usage_pct" U_ "\n"
    "        Apply purge policy until the filesystem usage reaches the specified value.\n"
    "    " _B "--purge-class=" B_ _U "fileclass" U_ "\n"
    "        Purge all eligible files in the given fileclass.\n"
    "\n"
#endif
#ifdef HAVE_MIGR_POLICY
    _B "Manual migration actions:" B_ "\n"
    "    " _B "-s" B_ ", " _B "--sync" B_ "\n"
    "        Immediately migrate all modified files, ignoring policy conditions.\n"
    "        It is equivalent to \"--migrate --ignore-policies --once --no-limit\".\n"
#ifdef _LUSTRE
    "    " _B "--migrate-ost=" B_ _U "ost_index" U_ "\n"
    "        Apply migration policies to files on the given OST " _U "ost_index" U_ ".\n"
#endif
    "    " _B "--migrate-user=" B_ _U "user_name" U_ "\n"
    "        Apply migration policies to files owned by " _U "user_name" U_ ".\n"
    "    " _B "--migrate-group=" B_ _U "grp_name" U_ "\n"
    "        Apply migration policies to files of group " _U "grp_name" U_ ".\n"
    "    " _B "--migrate-class=" B_ _U "fileclass" U_ "\n"
    "        Apply migration policy on files in the given " _U "fileclass" U_ ".\n"
    "    " _B "--migrate-file=" B_ _U "filepath" U_ "\n"
    "        Apply migration policy to a single file " _U "filepath" U_ ".\n\n"
#endif
    _B "Behavior options:" B_ "\n"
    "    " _B "--dry-run"B_"\n"
    "        Only report actions that would be performed (rmdir, migration, purge)\n"
    "        without really doing them.\n"
    "    " _B "-i" B_ ", " _B "--ignore-policies"B_"\n"
    "        Force migration/purge of all eligible files, ignoring policy conditions.\n"
    "    " _B "-O" B_ ", " _B "--once" B_ "\n"
    "        Perform only one pass of the specified action and exit.\n"
    "    " _B "-d" B_ ", " _B "--detach" B_ "\n"
    "        Daemonize the process (detach from parent process).\n"
    "    " _B "--no-limit"B_"\n"
    "        Don't limit the maximum number of migrations (per pass).\n"
    "\n"
    _B "Config file options:" B_ "\n"
    "    " _B "-f" B_ " " _U "file" U_ ", " _B "--config-file=" B_ _U
    "file" U_ "\n" "        Specifies path to configuration file.\n"
    "    " _B "-T" B_ " " _U "file"
    U_ ", " _B "--template=" B_ _U "file" U_ "\n"
    "        Write a configuration file template to the specified file.\n"
    "    " _B "-D" B_ ", " _B "--defaults" B_ "\n"
    "        Display default configuration values.\n"
    "    " _B "--test-syntax" B_ "\n"
    "        Check configuration file and exit.\n"
    "\n"
    _B "Filesystem options:" B_ "\n"
    "    " _B "-F" B_ " " _U "path" U_ ", " _B "--fs-path=" B_ _U
    "path" U_ "\n"
    "        Force the path of the filesystem to be managed (overrides configuration value).\n"
    "    " _B "-t" B_ " " _U "type" U_ ", " _B "--fs-type=" B_ _U "type" U_ "\n"
    "        Force the type of filesystem to be managed (overrides configuration value).\n" "\n" _B
    "Log options:" B_ "\n"
    "    " _B "-L" B_ " " _U "logfile" U_ ", " _B "--log-file=" B_ _U
    "logfile" U_ "\n" "        Force the path to the log file (overrides configuration value).\n"
    "        Special values \"stdout\" and \"stderr\" can be used.\n"
    "    " _B "-l" B_ " " _U "level" U_ ", " _B "--log-level=" B_ _U "level" U_ "\n"
    "        Force the log verbosity level (overrides configuration value).\n"
    "        Allowed values: CRIT, MAJOR, EVENT, VERB, DEBUG, FULL.\n" "\n" _B
    "Miscellaneous options:" B_ "\n"
    "    " _B "-h" B_ ", " _B "--help" B_ "\n"
    "        Display a short help about command line options.\n"
    "    " _B "-V" B_ ", " _B "--version" B_ "\n" "        Display version info\n"
    "    " _B "-p" B_ " " _U "pidfile" U_ ", "
    _B "--pid-file=" B_ _U "pidfile" U_ "\n" "         Pid file (used for service management).\n";


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
#elif defined(_SHERPA)
    printf( "    SHERPA cache zapper\n" );
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

static pthread_t stat_thread;

/* database connexion for updating stats */
static lmgr_t  lmgr;
static int     lmgr_init = FALSE;
static char    boot_time_str[256];

static void dump_stats( lmgr_t * lmgr, const int * module_mask )
{
        char           tmp_buff[256];
        time_t         now;
        struct tm      date;

        now = time( NULL );
        strftime( tmp_buff, 256, "%Y/%m/%d %T", localtime_r( &now, &date ) );

        DisplayLog( LVL_MAJOR, "STATS",
                    "==================== Dumping stats at %s =====================", tmp_buff );
        DisplayLog( LVL_MAJOR, "STATS", "======== General statistics =========" );
        DisplayLog( LVL_MAJOR, "STATS", "Daemon start time: %s", boot_time_str );

        if ( *module_mask & MODULE_MASK_FS_SCAN )
        {
            FSScan_DumpStats(  );
            FSScan_StoreStats( lmgr );
        }

#ifdef HAVE_CHANGELOGS
        if ( *module_mask & MODULE_MASK_EVENT_HDLR )
        {
            ChgLogRdr_DumpStats(  );
            ChgLogRdr_StoreStats( lmgr );
        }
#endif
        if ( *module_mask & MODULE_MASK_ENTRY_PROCESSOR )
            EntryProcessor_DumpCurrentStages(  );
#ifdef HAVE_PURGE_POLICY
        if ( *module_mask & MODULE_MASK_RES_MONITOR )
            Dump_ResourceMonitor_Stats(  );
#endif
#ifdef HAVE_RMDIR_POLICY
        if ( *module_mask & MODULE_MASK_RMDIR )
            Dump_Rmdir_Stats(  );
#endif
#ifdef HAVE_MIGR_POLICY
        if ( *module_mask & MODULE_MASK_MIGRATION )
            Dump_Migration_Stats(  );
#endif
#ifdef HAVE_RM_POLICY
        if ( *module_mask & MODULE_MASK_UNLINK )
            Dump_HSMRm_Stats(  );
#endif
        /* Flush stats */
        FlushLogs(  );
}

static void  *stats_thr( void *arg )
{
    int           *module_mask = ( int * ) arg;
    struct tm      date;

    strftime( boot_time_str, 256, "%Y/%m/%d %T", localtime_r( &boot_time, &date ) );

    if ( !lmgr_init )
    {
        if ( ListMgr_InitAccess( &lmgr ) != DB_SUCCESS )
            return NULL;
        lmgr_init = TRUE;
    }

    DisplayLog( LVL_VERB, MAIN_TAG, "Statistics thread started" );

    while ( 1 )
    {
        WaitStatsInterval(  );
        dump_stats(&lmgr, module_mask);
    }
}


static int     terminate_sig = 0;
static int     reload_sig = FALSE;
static pthread_t sig_thr;

#define SIGHDL_TAG  "SigHdlr"

static void terminate_handler( int sig )
{
    terminate_sig = sig;
}

static void reload_handler( int sig )
{
    reload_sig = TRUE;
}


static void   *signal_handler_thr( void *arg )
{

    struct sigaction act_sighup;
    struct sigaction act_sigterm;

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
        DisplayLog( LVL_EVENT, SIGHDL_TAG,
                    "Signals SIGTERM and SIGINT (daemon shutdown) are ready to be used" );

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
        DisplayLog( LVL_EVENT, SIGHDL_TAG, "Signal SIGHUP (config reloading) is ready to be used" );


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

            if ( action_mask & ACTION_MASK_SCAN )
            {
                /* stop FS scan */
                FSScan_Terminate(  );
                FlushLogs(  );
            }

#ifdef HAVE_CHANGELOGS
            if ( action_mask & ACTION_MASK_HANDLE_EVENTS )
            {
                /* stop changelog processing */
                ChgLogRdr_Terminate(  );
                FlushLogs(  );
            }
#endif

            if ( action_mask & ( ACTION_MASK_SCAN | ACTION_MASK_HANDLE_EVENTS ) )
            {
                /* flush processor pipeline and terminate threads */
                EntryProcessor_Terminate(  );
                FlushLogs(  );
            }

#ifdef HAVE_MIGR_POLICY
            if ( action_mask & ACTION_MASK_MIGRATE )
            {
                /* abort migration */
                Wait_Migration( TRUE );
                FlushLogs(  );
            }
#endif

#ifdef HAVE_PURGE_POLICY
            if ( action_mask & ACTION_MASK_PURGE )
            {
                /* abort purge */
                Wait_ResourceMonitor( TRUE );
                FlushLogs(  );
            }
#endif


#ifdef _HSM_LITE
            Backend_Stop();
#endif

            DisplayLog( LVL_MAJOR, SIGHDL_TAG, "Exiting." );
            FlushLogs(  );

            /* indicate the process terminated due to a signal */
            exit( 128 + terminate_sig );

        }
        else if ( reload_sig )
        {
            DisplayLog( LVL_MAJOR, SIGHDL_TAG, "SIGHUP received: reloading configuration" );
            ReloadRobinhoodConfig(  );
            reload_sig = FALSE;
            FlushLogs(  );
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
            fprintf( stderr, "Error openning file '%s' for writting: %s.\n", file, strerror( rc ) );
            return rc;
        }
    }
    else
        stream = stdout;

    rc = WriteConfigTemplate( stream );
    if ( rc )
        fprintf( stderr, "Error writting configuration template: %s\n", strerror( rc ) );
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

#define SET_ACTION_FLAG( _f_ )  do {                                    \
                                    if ( is_default_actions )           \
                                    {                                   \
                                        is_default_actions = FALSE;     \
                                        action_mask = _f_;              \
                                    }                                   \
                                    else                                \
                                        action_mask |= _f_;             \
                                } while(0)

#define MAX_OPT_LEN 1024
#define MAX_TYPE_LEN 256

/**
 * Main daemon routine
 */
int main( int argc, char **argv )
{
    int            c, option_index = 0;
    char          *bin = basename( argv[0] );

    int            is_default_actions = TRUE;
    int            flags = 0;
#define once ( flags & FLAG_ONCE )
    int            detach = FALSE;
    char           config_file[MAX_OPT_LEN] = "";
    char           template_file[MAX_OPT_LEN] = "";
    int            write_template = FALSE;
    int            write_defaults = FALSE;
    int            force_fspath = FALSE;
    char           fspath[MAX_OPT_LEN] = "";
    int            force_fstype = FALSE;
    char           fstype[MAX_TYPE_LEN] = "";
    int            force_log = FALSE;
    char           log[MAX_OPT_LEN];
    int            force_log_level = FALSE;
    int            log_level = 0;
    int            pid_file = FALSE;
    char           pid_filepath[MAX_OPT_LEN];
    int            test_syntax = FALSE;

    char           extra_chr[1024];
    int            ost_trigger = FALSE;
    int            fs_trigger = FALSE;
    int            purge_target_ost = -1;
    double         usage_target = 0.0;
    int            purge_class = FALSE;
    char           purge_target_class[128] = "";

#ifdef HAVE_MIGR_POLICY
#ifdef _LUSTRE
    int            migrate_ost = FALSE;
    int            migr_target_ost = -1;
#endif
    int            migrate_user = FALSE;
    int            migrate_group = FALSE;
    int            migrate_class = FALSE;
    int            migrate_file = FALSE;
    char           migr_target_user[128] = "";
    char           migr_target_group[128] = "";
    char           migr_target_class[128] = "";
    char           migr_target_file[RBH_PATH_MAX] = "";
#endif

    int            rc;
    int            parsing_mask, currently_running_mask;
    char           err_msg[4096];
    robinhood_config_t rh_config;


    boot_time = time( NULL );

    /* parse command line options */
    while ( ( c = getopt_long( argc, argv, SHORT_OPT_STRING, option_tab, &option_index ) ) != -1 )
    {
        switch ( c )
        {
        case 'S':
            SET_ACTION_FLAG( ACTION_MASK_SCAN );
            break;
        case 'C':
            SET_ACTION_FLAG( ACTION_MASK_PURGE );
            flags |= FLAG_CHECK_ONLY;
            break;
        case 'P':
            SET_ACTION_FLAG( ACTION_MASK_PURGE );
            break;
        case 'R': /* rmdir for non Lustre-HSM systems */
#ifdef HAVE_RMDIR_POLICY
            SET_ACTION_FLAG( ACTION_MASK_RMDIR );
#elif HAVE_RM_POLICY
            SET_ACTION_FLAG( ACTION_MASK_UNLINK );
#else
            fprintf( stderr, "-R option is not supported.\n" ); 
            exit(1);
#endif
            break;

        case 'M':
#ifndef HAVE_MIGR_POLICY
            fprintf( stderr, "-M | --migrate option is only supported for HSM purposes.\n" );
            exit( 1 );
#else
            SET_ACTION_FLAG( ACTION_MASK_MIGRATE );
#endif
            break;
        case 's':
#ifndef HAVE_MIGR_POLICY
            fprintf( stderr, "-s | --sync option is only supported for HSM purposes.\n" );
            exit( 1 );
#else
            SET_ACTION_FLAG( ACTION_MASK_MIGRATE );
            flags |= (FLAG_ONCE | FLAG_IGNORE_POL | FLAG_NO_LIMIT);
#endif
            break;

        case 'r':
#ifndef HAVE_CHANGELOGS
            fprintf( stderr,
                     "-r | --read-log option is only supported in Lustre v2.x versions.\n" );
            exit( 1 );
#else
            SET_ACTION_FLAG( ACTION_MASK_HANDLE_EVENTS );
#endif
            break;

        case 'O':
            flags |= FLAG_ONCE;
            break;
        case NO_LIMIT:
            flags |= FLAG_NO_LIMIT;
            break;
        case DRY_RUN:
            flags |= FLAG_DRY_RUN;
            break;
        case 'i':
            flags |= FLAG_IGNORE_POL;
            break;

#ifdef _LUSTRE
        case FORCE_OST_PURGE:
            /* this mode is always 'one-shot' */
            flags |= FLAG_ONCE;
            SET_ACTION_FLAG( ACTION_MASK_PURGE );
            ost_trigger = TRUE;
            /* parse <index,float> argument */
            if ( sscanf( optarg, "%u,%lf%s", &purge_target_ost, &usage_target, extra_chr ) != 2 )
            {
                fprintf( stderr,
                         "Invalid argument: --purge-ost=<ost_index,target_usage> expected. E.g. --purge-ost=5,10.00\n" );
                exit( 1 );
            }
            break;
#endif

        case FORCE_FS_PURGE:
            /* this mode is always 'one-shot' */
            flags |= FLAG_ONCE;
            SET_ACTION_FLAG( ACTION_MASK_PURGE );
            fs_trigger = TRUE;

            /* parse float argument */
            if ( sscanf( optarg, "%lf%s", &usage_target, extra_chr ) != 1 )
            {
                fprintf( stderr,
                         "Invalid argument: --purge-fs=<target_usage> expected. E.g. --purge-fs=20.00\n" );
                exit( 1 );
            }

            break;

        case FORCE_CLASS_PURGE:
            /* this mode is always 'one-shot' */
            flags |= FLAG_ONCE;
            SET_ACTION_FLAG( ACTION_MASK_PURGE );
            purge_class = TRUE;
            strncpy( purge_target_class, optarg, 128 );
            break;


#ifdef HAVE_MIGR_POLICY

#ifdef _LUSTRE
        case FORCE_OST_MIGR:
            /* this mode is always 'one-shot' */
            flags |= FLAG_ONCE;
            SET_ACTION_FLAG( ACTION_MASK_MIGRATE );
            migrate_ost = TRUE;

            /* parse <index> argument */
            if ( sscanf( optarg, "%u%s", &migr_target_ost, extra_chr ) != 1 )
            {
                fprintf( stderr,
                         "Invalid argument: --migrate-ost=<ost_index> expected. E.g. --migrate-ost=5\n" );
                exit( 1 );
            }
            break;
#endif

        case FORCE_USER_MIGR:
            /* this mode is always 'one-shot' */
            flags |= FLAG_ONCE;
            SET_ACTION_FLAG( ACTION_MASK_MIGRATE );
            migrate_user = TRUE;
            strncpy( migr_target_user, optarg, 128 );
            break;

        case FORCE_GROUP_MIGR:
            /* this mode is always 'one-shot' */
            flags |= FLAG_ONCE;
            SET_ACTION_FLAG( ACTION_MASK_MIGRATE );
            migrate_group = TRUE;
            strncpy( migr_target_group, optarg, 128 );
            break;

        case FORCE_CLASS_MIGR:
            /* this mode is always 'one-shot' */
            flags |= FLAG_ONCE;
            SET_ACTION_FLAG( ACTION_MASK_MIGRATE );
            migrate_class = TRUE;
            strncpy( migr_target_class, optarg, 128 );
            break;

        case MIGR_ONE_FILE:
            /* this mode is always 'one-shot' */
            flags |= FLAG_ONCE;
            SET_ACTION_FLAG( ACTION_MASK_MIGRATE );
            migrate_file = TRUE;
            strncpy( migr_target_file, optarg, RBH_PATH_MAX );
            break;
#endif

        case 'd':
            detach = TRUE;
            break;
        case 'f':
            strncpy( config_file, optarg, MAX_OPT_LEN );
            break;
        case 'T':
            if ( optarg )       /* optional argument */
                strncpy( template_file, optarg, MAX_OPT_LEN );
            write_template = TRUE;
            break;
        case TEST_SYNTAX:
            test_syntax = TRUE;
            break;
        case 'D':
            write_defaults = TRUE;
            break;
        case 'F':
            force_fspath = TRUE;
            strncpy( fspath, optarg, MAX_OPT_LEN );
            break;
        case 't':
            force_fstype = TRUE;
            strncpy( fstype, optarg, MAX_TYPE_LEN );
            break;
        case 'L':
            force_log = TRUE;
            strncpy( log, optarg, MAX_OPT_LEN );
            break;
        case 'l':
            force_log_level = TRUE;
            log_level = str2debuglevel( optarg );
            if ( log_level == -1 )
            {
                fprintf( stderr,
                         "Unsupported log level '%s'. CRIT, MAJOR, EVENT, VERB, DEBUG or FULL expected.\n",
                         optarg );
                exit( 1 );
            }
            break;
        case 'p':
            pid_file = TRUE;
            strncpy( pid_filepath, optarg, MAX_OPT_LEN );
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

    /* check that force-purge options are not used together */
    if ( ost_trigger && fs_trigger )
    {
        fprintf( stderr,
                 "Error: --purge-ost and --purge-fs cannot be used together\n" );
        exit( 1 );
    }
    
#ifdef HAVE_MIGR_POLICY
    if ( migrate_user + migrate_group 
#ifdef _LUSTRE
        + migrate_ost
#endif
        > 1 )
    {
        fprintf( stderr,
                 "Error: --migrate-ost, --migrate-user and --migrate-group cannot be used together\n" );
        exit( 1 );
    }
#endif

    /* Template or Defaults options specified ? */

    if ( write_template )
    {
        rc = do_write_template( template_file );
        exit( rc );
    }

    if ( write_defaults )
    {
        rc = WriteConfigDefault( stdout );
        if ( rc )
        {
            fprintf( stderr, "Error %d retrieving default configuration: %s\n", rc,
                     strerror( rc ) );
        }
        exit( rc );
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

    /* build config parsing mask */
    parsing_mask = 0;
    if ( action_mask & ACTION_MASK_SCAN )
        parsing_mask |= MODULE_MASK_FS_SCAN | MODULE_MASK_ENTRY_PROCESSOR;
    if ( action_mask & ACTION_MASK_PURGE )
        parsing_mask |= MODULE_MASK_RES_MONITOR;
#ifdef HAVE_RMDIR_POLICY
    if ( action_mask & ACTION_MASK_RMDIR )
        parsing_mask |= MODULE_MASK_RMDIR;
#endif
#ifdef HAVE_CHANGELOGS
    if ( action_mask & ACTION_MASK_HANDLE_EVENTS )
        parsing_mask |= MODULE_MASK_EVENT_HDLR | MODULE_MASK_ENTRY_PROCESSOR;
#endif
#ifdef HAVE_MIGR_POLICY
    if ( action_mask & ACTION_MASK_MIGRATE )
        parsing_mask |= MODULE_MASK_MIGRATION;
#endif
#ifdef HAVE_RM_POLICY
    if ( action_mask & ACTION_MASK_UNLINK )
        parsing_mask |= MODULE_MASK_UNLINK;
#endif

    /* get default config file, if not specified */
    if ( EMPTY_STRING( config_file ) )
    {
        if ( SearchConfig( config_file ) != 0 )
        {
            fprintf(stderr, "No config file found in '/etc/robinhood.d/"PURPOSE_EXT"'\n" );
            exit(2);
        }
        else
        {
            fprintf(stderr, "Using config file '%s'.\n", config_file );
        }
    }

    if ( ReadRobinhoodConfig( parsing_mask, config_file, err_msg, &rh_config ) )
    {
        fprintf( stderr, "Error reading configuration file '%s': %s\n", config_file, err_msg );
        exit( 1 );
    }

    if ( test_syntax )
    {
        printf( "Configuration file has been read successfully\n" );
        exit(0);
    }

    /* override config file options with command line parameters */
    if ( force_fspath )
        strcpy( rh_config.global_config.fs_path, fspath );
    if ( force_fstype )
        strcpy( rh_config.global_config.fs_type, fstype );
    if ( force_log )
        strcpy( rh_config.log_config.log_file, log );
    if ( force_log_level )
        rh_config.log_config.debug_level = log_level;

    /* set global configuration */
    global_config = rh_config.global_config;

    /* set policies info */
    policies = rh_config.policies;

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
    if ( ( action_mask & ACTION_MASK_SCAN ) && !once
         && strcmp( global_config.fs_type, "lustre" ) == 0 )
    {
        fprintf(stderr, "ADVICE: this filesystem is changelog-capable, you should use changelogs instead of scanning.\n");
    }
#endif

#ifdef _SHERPA
    /* read sherpa configuration */
    rc = InitSherpa(global_config.sherpa_config, rh_config.log_config.log_file, rh_config.log_config.report_file);

    if ( rc )
    {
        fprintf( stderr, "Sherpa initialization error!\n");
        exit( 1 );
    }

    if ( flags & FLAG_DRY_RUN )
        /* sherpa config */
        config.attitudes.modes_fonctionnement |= SANS_EFFACEMENT;
#endif

    /* Initialize logging */
    rc = InitializeLogs( bin, &rh_config.log_config );
    if ( rc )
    {
        fprintf( stderr, "Error opening log files: rc=%d, errno=%d: %s\n",
                 rc, errno, strerror( errno ) );
        exit( rc );
    }

    /* deamonize program if detach flag is set */
    if ( detach )
    {
        rc = daemon( 0, 0 );

        if ( rc )
        {
            DisplayLog( LVL_CRIT, MAIN_TAG, "Error detaching process from parent: %s",
                        strerror( errno ) );
            fprintf( stderr, "Error detaching process from parent: %s\n", strerror( errno ) );
            exit( 1 );
        }
    }

    if ( pid_file )
        create_pid_file( pid_filepath );

#ifdef _HSM_LITE
    rc = Backend_Start( &rh_config.backend_config, flags );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, MAIN_TAG, "Error initializing backend" );
        exit( 1 );
    }
#endif

    /* create signal handling thread */
    rc = pthread_create( &sig_thr, NULL, signal_handler_thr, NULL );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, MAIN_TAG, "Error starting signal handler thread: %s",
                    strerror( errno ) );
        exit( 1 );
    }
    else
        DisplayLog( LVL_VERB, MAIN_TAG, "Signal handler thread started successfully" );

    if ( once )
    {
        /* used for dumping stats in one shot mode */
        currently_running_mask = 0;
        pthread_create( &stat_thread, NULL, stats_thr, &currently_running_mask );
    }

    /* Initialize list manager */
    rc = ListMgr_Init( &rh_config.lmgr_config, FALSE );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, MAIN_TAG, "Error %d initializing list manager", rc );
        exit( rc );
    }
    else
        DisplayLog( LVL_VERB, MAIN_TAG, "ListManager successfully initialized" );

    if ( CheckLastFS(  ) != 0 )
        exit( 1 );

    if ( action_mask & ( ACTION_MASK_SCAN | ACTION_MASK_HANDLE_EVENTS ) )
    {
        /* Initialise Pipeline */
        rc = EntryProcessor_Init( &rh_config.entry_proc_config, flags );
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
        rc = FSScan_Start( &rh_config.fs_scan_config, flags );
        if ( rc )
        {
            DisplayLog( LVL_CRIT, MAIN_TAG, "Error %d initializing FS Scan module", rc );
            exit( rc );
        }
        else
            DisplayLog( LVL_VERB, MAIN_TAG, "FS Scan module successfully initialized" );

        /* Flush logs now, to have a trace in the logs */
        FlushLogs(  );

        if ( once )
        {
            currently_running_mask = MODULE_MASK_FS_SCAN | MODULE_MASK_ENTRY_PROCESSOR;
            FSScan_Wait(  );
            DisplayLog( LVL_MAJOR, MAIN_TAG, "FS Scan finished" );
        }
    }

#ifdef HAVE_CHANGELOGS
    if ( action_mask & ACTION_MASK_HANDLE_EVENTS )
    {

        /* Start reading changelogs */
        rc = ChgLogRdr_Start( &rh_config.chglog_reader_config, flags );
        if ( rc )
        {
            DisplayLog( LVL_CRIT, MAIN_TAG, "Error %d initializing ChangeLog Reader", rc );
            exit( rc );
        }
        else
            DisplayLog( LVL_VERB, MAIN_TAG, "ChangeLog Reader successfully initialized" );

        /* Flush logs now, to have a trace in the logs */
        FlushLogs(  );

        if ( once )
        {
            currently_running_mask = MODULE_MASK_EVENT_HDLR | MODULE_MASK_ENTRY_PROCESSOR;
            ChgLogRdr_Wait(  );
            DisplayLog( LVL_MAJOR, MAIN_TAG, "Event Processing finished" );
        }
    }
#endif

    if ( once && (  action_mask & ( ACTION_MASK_SCAN | ACTION_MASK_HANDLE_EVENTS ) ) )
    {
        /* Pipeline must be flushed */
        EntryProcessor_Terminate(  );
    }

#ifdef HAVE_MIGR_POLICY
    if ( migrate_file )
    {
        rc = MigrateSingle( &rh_config.migr_config, migr_target_file, flags );
        DisplayLog( LVL_MAJOR, MAIN_TAG, "Migration completed with status %d", rc );
    }
    else if ( action_mask & ACTION_MASK_MIGRATE )
    {
        migr_opt_t     migr_opt;
        migr_opt.flags = flags;

#ifdef _LUSTRE
        if ( migrate_ost )
        {
            /* migrate OST (one-shot) */
            migr_opt.mode = MIGR_OST;
            migr_opt.optarg_u.ost_index = migr_target_ost;
        }
        else
#endif
        if ( migrate_user )
        {
            /* migrate user files (one-shot) */
            migr_opt.mode = MIGR_USER;
            migr_opt.optarg_u.name = migr_target_user;
        }
        else if ( migrate_group )
        {
            /* purge on FS (one-shot) */
            migr_opt.mode = MIGR_GROUP;
            migr_opt.optarg_u.name = migr_target_group;
        }
        else if ( migrate_class )
        {
            /* purge on FS (one-shot) */
            migr_opt.mode = MIGR_CLASS;
            migr_opt.optarg_u.name = migr_target_class;
        }
        else if ( once )
        {
            /* one-shot migration on all filesystem */
            migr_opt.mode = MIGR_ONCE;
        }
        else
        {
            /* daemon mode */
            migr_opt.mode = MIGR_DAEMON;
        }

        rc = Start_Migration( &rh_config.migr_config, migr_opt );
        if ( rc == ENOENT )
        {
            DisplayLog( LVL_CRIT, MAIN_TAG, "Migration module is disabled." ); 
            /* unset it in parsing mask to avoid dumping stats */
            parsing_mask &= ~MODULE_MASK_MIGRATION;
        }
        else if ( rc )
        {
            fprintf( stderr, "Error %d initializing Migration module\n", rc );
            exit( rc );
        }
        else
        {
            DisplayLog( LVL_VERB, MAIN_TAG, "Migration module successfully initialized" );

            /* Flush logs now, to have a trace in the logs */
            FlushLogs(  );

            if ( once )
            {
                currently_running_mask = MODULE_MASK_MIGRATION;
                Wait_Migration( FALSE );
                DisplayLog( LVL_MAJOR, MAIN_TAG, "Migration pass terminated" );
            }
        }
    }
#endif

#ifdef HAVE_PURGE_POLICY
    if ( action_mask & ACTION_MASK_PURGE )
    {
        resmon_opt_t   resmon_opt = {0,0,0.0};
        resmon_opt.flags = flags;
        if ( ost_trigger )
        {
            /* purge on OST (one-shot) */
            resmon_opt.mode = RESMON_PURGE_OST;
            resmon_opt.ost_index = purge_target_ost;
            resmon_opt.target_usage = usage_target;
        }
        else if ( fs_trigger )
        {
            /* purge on FS (one-shot) */
            resmon_opt.mode = RESMON_PURGE_FS;
            resmon_opt.target_usage = usage_target;
        }
        else if ( purge_class )
        {
            /* purge on FS (one-shot) */
            resmon_opt.mode = RESMON_PURGE_CLASS;
            resmon_opt.fileclass = purge_target_class;
        }
        else if ( once )
        {
            /* one-shot mode on all triggers */
            resmon_opt.mode = RESMON_ALL_TRIGGERS;
        }
        else
        {
            /* daemon mode */
            resmon_opt.mode = RESMON_DAEMON;
        }

        rc = Start_ResourceMonitor( &rh_config.res_mon_config, resmon_opt );
        if ( rc == ENOENT )
        {
            DisplayLog( LVL_CRIT, MAIN_TAG, "Resource Monitor is disabled." ); 
            /* unset it in parsing mask to avoid dumping stats */
            parsing_mask &= ~MODULE_MASK_RES_MONITOR;
        }
        else if ( rc )
        {
            fprintf( stderr, "Error %d initializing Resource Monitor\n", rc );
            exit( rc );
        }
        else
        {
            DisplayLog( LVL_VERB, MAIN_TAG, "Resource Monitor successfully initialized" );

            /* Flush logs now, to have a trace in the logs */
            FlushLogs(  );

            if ( once )
            {
                currently_running_mask = MODULE_MASK_RES_MONITOR;
                Wait_ResourceMonitor( FALSE );
                DisplayLog( LVL_MAJOR, MAIN_TAG, "ResourceMonitor terminated its task" );
            }
        }
    }
#endif

#ifdef HAVE_RMDIR_POLICY
    if ( action_mask & ACTION_MASK_RMDIR )
    {
        rc = Start_Rmdir( &rh_config.rmdir_config, flags );
        if ( rc == ENOENT )
        {
            DisplayLog( LVL_CRIT, MAIN_TAG, "Directory removal is disabled." ); 
            /* unset it in parsing mask to avoid dumping stats */
            parsing_mask &= ~MODULE_MASK_RMDIR;
        }
        else if ( rc )
        {
            fprintf( stderr, "Error %d initializing Directory Remover\n", rc );
            exit( rc );
        }
        else
        {
            DisplayLog( LVL_VERB, MAIN_TAG, "Directory Remover successfully initialized" );

            /* Flush logs now, to have a trace in the logs */
            FlushLogs(  );

            if ( once )
            {
                currently_running_mask = MODULE_MASK_RMDIR;
                Wait_Rmdir(  );
                DisplayLog( LVL_MAJOR, MAIN_TAG, "Directory Remover terminated its task" );
            }
        }
    }
#endif

#ifdef HAVE_RM_POLICY
    if ( action_mask & ACTION_MASK_UNLINK )
    {
        rc = Start_HSMRm( &rh_config.hsm_rm_config, flags );
        if ( rc == ENOENT )
        {
            DisplayLog( LVL_CRIT, MAIN_TAG, "HSM removal is disabled." ); 
            /* unset it in parsing mask to avoid dumping stats */
            parsing_mask &= ~MODULE_MASK_UNLINK;
        }
        else if ( rc )
        {
            fprintf( stderr, "Error %d initializing HSM Removal\n", rc );
            exit( rc );
        }
        else
        {
            DisplayLog( LVL_VERB, MAIN_TAG, "HSM removal successfully initialized" );

            /* Flush logs now, to have a trace in the logs */
            FlushLogs(  );

            if ( once )
            {
                currently_running_mask = MODULE_MASK_UNLINK;
                Wait_HSMRm(  );
                DisplayLog( LVL_MAJOR, MAIN_TAG, "HSM removal terminated" );
            }
        }
    }
#endif

    if ( !once )
    {
        /* dump stats periodically */
        stats_thr( &parsing_mask );

        /* should never return */
        exit( 1 );
    }
    else
    {
        DisplayLog( LVL_MAJOR, MAIN_TAG, "All tasks done! Exiting." );
        exit( 0 );
    }

    return 0;                   /* for compiler */

}
