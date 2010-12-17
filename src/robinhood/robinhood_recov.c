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
 * Command for recovering filesystem content after a disaster (backup flavor)
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "list_mgr.h"
#include "RobinhoodConfig.h"
#include "RobinhoodLogs.h"
#include "RobinhoodMisc.h"
#include "xplatform_print.h"

#include <unistd.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <libgen.h>
#include <pthread.h>

#define RECOV_TAG "Recov"

static struct option option_tab[] =
{
    /* recovery options */
    {"start", no_argument, NULL, 'S'},
    {"resume", no_argument, NULL, 'r'},
    {"complete", no_argument, NULL, 'c'},
    {"status", no_argument, NULL, 's'},
    {"reset", no_argument, NULL, 'Z'},

    {"dir", required_argument, NULL, 'D'},

    /* config file options */
    {"config-file", required_argument, NULL, 'f'},

    /* log options */
    {"log-level", required_argument, NULL, 'l'},
    {"output-dir", required_argument, NULL, 'o'},

    /* miscellaneous options */
    {"help", no_argument, NULL, 'h'},
    {"version", no_argument, NULL, 'V'},

    {NULL, 0, NULL, 0}

};

#define SHORT_OPT_STRING    "SrcZsDf:l:o:hV"

/* global variables */

static lmgr_t  lmgr;

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
    _B "Recovery actions:" B_ "\n"
    "    " _B "--start" B_ ", " _B "-S" B_ "\n"
    "        bla.\n"
    "    " _B "--resume" B_ ", " _B "-r" B_ "\n"
    "        bla.\n"
    "    " _B "--complete" B_ ", " _B "-c" B_ "\n"
    "        bla.\n"
    "    " _B "--status" B_ ", " _B "-s" B_ "\n"
    "        bla.\n"
    "    " _B "--reset" B_ ", " _B "-Z" B_ "\n"
    "        bla.\n"
    "\n"
    _B "Recovery options:" B_ "\n"
    "    " _B "--dir" B_ "=" _U "path" U_ ", " _B "-D" B_ " " _U "path" U_ "\n"
    "        (used with --resume action) bla.\n"
    "\n"
    _B "Config file options:" B_ "\n"
    "    " _B "-f" B_ " " _U "file" U_ ", " _B "--config-file=" B_ _U "file" U_ "\n"
    "        Specifies path to robinhood configuration file.\n"
    "\n"
    _B "Output options:" B_ "\n"
    "    " _B "-o" B_ " " _U "dir" U_ ", " _B "--output-dir=" B_ _U "dir" U_ "\n"
    "        Directory where recovery reports will be written (default=current dir).\n"
    "\n"
    _B "Miscellaneous options:" B_ "\n"
    "    " _B "-l" B_ " " _U "level" U_ ", " _B "--log-level=" B_ _U "level" U_ "\n"
    "        Force the log verbosity level (overides configuration value).\n"
    "        Allowed values: CRIT, MAJOR, EVENT, VERB, DEBUG, FULL.\n"
    "    " _B "-h" B_ ", " _B "--help" B_ "\n"
    "        Display a short help about command line options.\n"
    "    " _B "-V" B_ ", " _B "--version" B_ "\n" "        Display version info\n";


static inline void display_help( char *bin_name )
{
    printf( help_string, bin_name );
}

static inline void display_version( char *bin_name )
{
    printf( "\n" );
    printf( "Product:         " PACKAGE_NAME " reporting tool\n" );
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
#elif defined(_BACKUP_FS)
    printf( "    Backup filesystem to external storage\n" );
#else
#error "No purpose was specified"
#endif

/* Access by Fid ? */
#ifdef _HAVE_FID
    printf( "    Address entries by FID\n" );
#else
    printf( "    Address entries by path\n" );
#endif

#ifdef _ENABLE_PREP_STMT
    printf( "    Prepared statements enabled\n" );
#else
    printf( "    Prepared statements disabled\n" );
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


int recov_start()
{
    lmgr_recov_stat_t stats;
    int rc, diff;
    char buff[128];

    rc = ListMgr_RecovInit( &lmgr, &stats );

    if ( rc == 0 )
    {
        printf( "\nRecovery successfully initialized.\n\n" );
        printf( "It should result in the following state:\n" );
        FormatFileSize( buff, 128, stats.status_size[RS_OK] );
        printf( "   - fully recovered: %10u entries (%s)\n", stats.status_count[RS_OK], buff );
        FormatFileSize( buff, 128, stats.status_size[RS_DELTA] );
        printf( "   - old version:     %10u entries (%s)\n", stats.status_count[RS_DELTA], buff );
        FormatFileSize( buff, 128, stats.status_size[RS_NOBACKUP] );
        printf( "   - not recoverable: %10u entries (%s)\n", stats.status_count[RS_NOBACKUP], buff );
        diff = stats.total - stats.status_count[RS_OK] - stats.status_count[RS_DELTA]
               - stats.status_count[RS_NOBACKUP] - stats.status_count[RS_ERROR];
        FormatFileSize( buff, 128, stats.status_size[RS_ERROR] );
        printf( "   - other/errors:    %10u/%u (%s)\n", diff, stats.status_count[RS_ERROR], buff );
        return 0;
    }
    else if ( rc == DB_ALREADY_EXISTS )
    {
        printf( "\nA recovery is already in progress, or a previous recovery\n"
                "was not completed properly (see --resume, --complete or --reset option).\n\n" );

        unsigned int total = stats.status_count[RS_OK] + stats.status_count[RS_DELTA]
                           + stats.status_count[RS_NOBACKUP] + stats.status_count[RS_ERROR];
        printf( "The progress of this recovery is %u/%u entries\n", total, stats.total );

        FormatFileSize( buff, 128, stats.status_size[RS_OK] );
        printf( "   - successfully recovered:              %10u entries (%s)\n",
                stats.status_count[RS_OK], buff );
        FormatFileSize( buff, 128, stats.status_size[RS_DELTA] );
        printf( "   - old version successfully recovered:  %10u entries (%s)\n",
                stats.status_count[RS_DELTA], buff );
        FormatFileSize( buff, 128, stats.status_size[RS_NOBACKUP] );
        printf( "   - not recoverable:                     %10u entries (%s)\n",
                stats.status_count[RS_NOBACKUP], buff );
        FormatFileSize( buff, 128, stats.status_size[RS_ERROR] );
        printf( "   - errors:                              %10u entries (%s)\n",
                stats.status_count[RS_ERROR], buff );

        return -EALREADY;
    }
    else /* other error */
    {
        printf( "An error occured while initializing recovery: db error %d\n", rc );
        return rc;
    }
}

int recov_reset()
{
    /* TODO ask confirmation */
    return ListMgr_RecovReset( &lmgr );
}





#define MAX_OPT_LEN 1024

/**
 * Main daemon routine
 */
int main( int argc, char **argv )
{
    int            c, option_index = 0;
    char          *bin = basename( argv[0] );

    char           config_file[MAX_OPT_LEN] = "";

    int            do_start = FALSE;
    int            do_reset = FALSE;
    int            force_log_level = FALSE;

    int            log_level = 0;
    int            flags = 0;

    int            rc;
    char           err_msg[4096];
    robinhood_config_t config;

    /* parse command line options */
    while ( ( c = getopt_long( argc, argv, SHORT_OPT_STRING, option_tab, &option_index ) ) != -1 )
    {
        switch ( c )
        {
        case 'S':
            do_start = TRUE;
            break;
        case 'Z':
            do_reset = TRUE;
            break;
        case 'f':
            strncpy( config_file, optarg, MAX_OPT_LEN );
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
            display_help( bin );
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
            fprintf(stderr, "No config file specified, using '%s'.\n", config_file );
        }
    }

    /* only read ListMgr config */

    if ( ReadRobinhoodConfig( 0, config_file, err_msg, &config ) )
    {
        fprintf( stderr, "Error reading configuration file '%s': %s\n", config_file, err_msg );
        exit( 1 );
    }

    /* set global configuration */
    global_config = config.global_config;

    /* set policies info */
    policies = config.policies;

    if ( force_log_level )
        config.log_config.debug_level = log_level;

    /* XXX HOOK: Set logging to stderr */
    strcpy( config.log_config.log_file, "stderr" );
    strcpy( config.log_config.report_file, "stderr" );
    strcpy( config.log_config.alert_file, "stderr" );

    /* Initialize logging */
    rc = InitializeLogs( bin, &config.log_config );
    if ( rc )
    {
        fprintf( stderr, "Error opening log files: rc=%d, errno=%d: %s\n",
                 rc, errno, strerror( errno ) );
        exit( rc );
    }

    /* Initialize list manager */
    rc = ListMgr_Init( &config.lmgr_config );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, RECOV_TAG, "Error %d initializing list manager", rc );
        exit( rc );
    }
    else
        DisplayLog( LVL_DEBUG, RECOV_TAG, "ListManager successfully initialized" );

    if ( CheckLastFS(  ) != 0 )
        exit( 1 );

    /* Create database access */
    rc = ListMgr_InitAccess( &lmgr );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, RECOV_TAG, "Error %d: cannot connect to database", rc );
        exit( rc );
    }

    if (do_start)
        recov_start();
    else if (do_reset)
        recov_reset();


    ListMgr_CloseAccess( &lmgr );

    return 0;                   /* for compiler */

}
