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
#include "backend_ext.h"

#include <unistd.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <libgen.h>
#include <pthread.h>
#include <signal.h>

#define RECOV_TAG "Recov"
#define RECOV_TAG "Recov"

static struct option option_tab[] =
{
    /* recovery options */
    {"start", no_argument, NULL, 'S'},
    {"resume", no_argument, NULL, 'r'},
    {"run", no_argument, NULL, 'r'},
    {"complete", no_argument, NULL, 'c'},
    {"status", no_argument, NULL, 's'},
    {"reset", no_argument, NULL, 'Z'},
    {"list", required_argument, NULL, 'L'},

    {"ost", required_argument, NULL, 'o'},
    {"since", required_argument, NULL, 'b'},

    {"dir", required_argument, NULL, 'D'},
    {"retry", no_argument, NULL, 'e'},
    {"yes", no_argument, NULL, 'y'},

    /* config file options */
    {"config-file", required_argument, NULL, 'f'},

    /* log options */
    {"log-level", required_argument, NULL, 'l'},
//    {"output-dir", required_argument, NULL, 'o'},

    /* miscellaneous options */
    {"help", no_argument, NULL, 'h'},
    {"version", no_argument, NULL, 'V'},

    {NULL, 0, NULL, 0}

};

#define SHORT_OPT_STRING    "SrcZsD:eyf:l:o:b:hVL:"

/* global variables */

static lmgr_t  lmgr;
static int terminate = FALSE; /* abort signal received */

static char * path_filter = NULL;
static char path_buff[RBH_PATH_MAX];
static value_list_t ost_list = {0, NULL};
static char ost_range_str[256] = "";
static time_t since_time = 0;

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
    _B "Usage:" B_ " %s <action> [options]\n"
    "\n"
    _B "Disaster recovery actions:" B_ "\n"
    "    " _B "--start" B_ ", " _B "-S" B_ "\n"
    "        Initialize a disaster recovery process.\n"
    "    "_B "--run" B_", " _B "--resume" B_ ", " _B "-r" B_ "\n"
    "        Run/resume the recovery process.\n"
    "    " _B "--complete" B_ ", " _B "-c" B_ "\n"
    "        Terminate the recovery.\n"
    "    " _B "--status" B_ ", " _B "-s" B_ "\n"
    "        Show current recovery progress.\n"
    "    " _B "--list" B_ " "_U"state"U_", " _B "-L"B_" "_U"state"U_ "\n"
    "        List entries for the given "_U"state"U_": all, done, failed, or todo.\n"
    "    " _B "--reset" B_ ", " _B "-Z" B_ "\n"
    "        Abort current recovery (/!\\ non-recovered entries are lost).\n"
    "\n"
    _B "Start options:" B_ "\n"
    "    "  _B "--ost" B_ " " _U "ost_index" U_ "|" _U "ost_set" U_"\n"
    "        Perform the recovery only for files striped on the given OST \n"
    "        or set of OSTs (e.g. 3,5-8).\n"
    "    "  _B "--since" B_ " " _U "date_time" U_ "\n"
    "        Perform the recovery only for files updated after the given "_U"date_time"U_".\n"
    "        The expected date/time format is yyyymmdd[HHMM[SS]].\n"
//    "    " _B "--with-data" B_ "\n"
//    "        Used with --ost: only recover files that really have data on the OST.\n"
    _B "Resume options:" B_ "\n"
    "    " _B "--dir" B_ "=" _U "path" U_ ", " _B "-D" B_ " " _U "path" U_ "\n"
    "        Only recover files in the given directory.\n"
    "    " _B "--retry" B_ ", " _B "-e" B_ "\n"
    "        Recover entries even if previous recovery failed on them.\n"
    _B "Reset options:" B_ "\n"
    "    " _B "--yes" B_ ", " _B "-y" B_ "\n"
    "        Do not prompt for confirmation.\n"
    "\n"
    _B "Config file options:" B_ "\n"
    "    " _B "-f" B_ " " _U "file" U_ ", " _B "--config-file=" B_ _U "file" U_ "\n"
    "        Path to configuration file (or short name).\n"
    "\n"
//    _B "Output options:" B_ "\n"
//    "    " _B "-o" B_ " " _U "dir" U_ ", " _B "--output-dir=" B_ _U "dir" U_ "\n"
//    "        Directory where recovery reports will be written (default=current dir).\n"
//    "\n"
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
    printf( "Product:         " PACKAGE_NAME " disaster recovery tool\n" );
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

static void terminate_handler( int sig )
{
    if ( sig == SIGTERM )
        fprintf( stderr, "SIGTERM received: performing clean shutdown\n" );
    else if ( sig == SIGINT )
        fprintf( stderr, "SIGINT received: performing clean shutdown\n" );

    terminate = TRUE;
}

static void print_recov_stats( int forecast, const lmgr_recov_stat_t * p_stat )
{
    char buff[128];
    unsigned long long diff;

    FormatFileSize( buff, 128, p_stat->status_size[RS_FILE_OK]
                    + p_stat->status_size[RS_FILE_EMPTY] );
    if (forecast)
        printf( "   - full recovery: %Lu files (%s), %Lu non-files\n",
                p_stat->status_count[RS_FILE_OK] + p_stat->status_count[RS_FILE_EMPTY],
                buff, p_stat->status_count[RS_NON_FILE] );
    else
        printf( "   - successfully recovered: %Lu files (%s), %Lu non-files\n",
                p_stat->status_count[RS_FILE_OK] + p_stat->status_count[RS_FILE_EMPTY], buff,
                p_stat->status_count[RS_NON_FILE] );

    FormatFileSize( buff, 128, p_stat->status_size[RS_FILE_DELTA] );
    printf( "   - old version:     %10Lu entries (%s)\n", p_stat->status_count[RS_FILE_DELTA], buff );
    FormatFileSize( buff, 128, p_stat->status_size[RS_NOBACKUP] );
    printf( "   - not recoverable: %10Lu entries (%s)\n", p_stat->status_count[RS_NOBACKUP], buff );

    diff = p_stat->total - p_stat->status_count[RS_FILE_OK] - p_stat->status_count[RS_FILE_DELTA]
           - p_stat->status_count[RS_FILE_EMPTY] - p_stat->status_count[RS_NOBACKUP]
             - p_stat->status_count[RS_NON_FILE] - p_stat->status_count[RS_ERROR];

    FormatFileSize( buff, 128, p_stat->status_size[RS_ERROR] );

    if ( forecast )
        printf( "   - other/errors:    %10Lu/%Lu (%s)\n", diff, p_stat->status_count[RS_ERROR], buff );
    else {
        printf( "   - errors:          %10Lu entries (%s)\n", p_stat->status_count[RS_ERROR], buff );
        printf( "   - still to be recovered: %4Lu entries\n", diff );
    }
}


static int recov_start( void )
{
    lmgr_recov_stat_t stats;
    int rc;

    /* is there a filter to be applied? */
    if (ost_list.count > 0 || since_time != 0)
    {
        lmgr_filter_t  filter;
        filter_value_t fv;

        lmgr_simple_filter_init( &filter );

        /* ost filter? */
        if (ost_list.count == 1)
        {
            printf( "only recovering files striped on OST#%u\n", ost_list.values[0].val_uint);
            fv.value.val_uint = ost_list.values[0].val_uint;
            lmgr_simple_filter_add( &filter, ATTR_INDEX_stripe_items, EQUAL, fv, 0 );
        }
        else if (ost_list.count > 1)
        {
            printf( "only recovering files striped on OSTs[%s]\n", ost_range_str);
            fv.list = ost_list;
            lmgr_simple_filter_add( &filter, ATTR_INDEX_stripe_items, IN, fv,
                                    FILTER_FLAG_ALLOC_LIST ); /* allow it to free ost_list->values */
        }

        /* update time filter */
        if (since_time)
        {
            char date[128];
            struct tm t;
            strftime( date, 128, "%Y/%m/%d %T", localtime_r( &since_time, &t ) );
            printf( "only recovering files updated after %s (timestamp: %lu)\n", date, since_time);
            fv.value.val_uint = since_time;

            lmgr_simple_filter_add( &filter, ATTR_INDEX_md_update, MORETHAN, fv, 0 );
        }

        rc = ListMgr_RecovInit( &lmgr, &filter, &stats );
    }
    else
        rc = ListMgr_RecovInit( &lmgr, NULL, &stats );

    if ( rc == 0 )
    {
        printf( "\nRecovery successfully initialized.\n\n" );
        printf( "It should result in the following state:\n" );
        print_recov_stats( TRUE, &stats );
        return 0;
    }
    else if ( rc == DB_ALREADY_EXISTS )
    {
        printf( "\nERROR: a recovery is already in progress, or a previous recovery\n"
                "was not completed properly (see --resume, --complete or --reset option).\n\n" );

        unsigned long long total = stats.status_count[RS_FILE_OK] + stats.status_count[RS_FILE_DELTA]
                           + stats.status_count[RS_NON_FILE]  + stats.status_count[RS_FILE_EMPTY]
                           + stats.status_count[RS_NOBACKUP] + stats.status_count[RS_ERROR];
        printf( "The progress of this recovery is %Lu/%Lu entries\n", total, stats.total );
        print_recov_stats( FALSE, &stats );

        return -EALREADY;
    }
    else /* other error */
    {
        fprintf( stderr, "ERROR initializing recovery: db error %d\n", rc );
        return rc;
    }
}

static int recov_reset(int force)
{
    int rc;

    /* ask confirmation */
    if ( !force )
    {
        lmgr_recov_stat_t stats;
        char * buff = malloc(1024);
        size_t sz = 1024;

        rc = ListMgr_RecovStatus( &lmgr, &stats );
        if (rc)
        {
            if ( rc == DB_NOT_EXISTS )
                fprintf( stderr, "ERROR: There is no pending recovery\n" );
            return rc;
        }

        printf( "\nWARNING: you are about to abort the current recovery.\n" );
        printf( "All entries not yet recovered will be definitely lost!\n\n");

        printf( "Current recovery status:\n");
        print_recov_stats( FALSE, &stats );
        printf("\n");

        do {
            printf( "Do you really want to proceed [y/n]: " );
            if ( getline( &buff, &sz, stdin ) > 0 )
            {
                if ( !strcasecmp(buff, "y\n") || !strcasecmp(buff, "yes\n") )
                    break;
                else
                {
                    printf("Aborted\n");
                    free(buff);
                    return -ECANCELED;
                }
            }
        } while(1);
        free(buff);
    }
    return ListMgr_RecovReset( &lmgr );
}

static int recov_resume( int retry_errors )
{
    struct lmgr_iterator_t * it;
    int rc, st;
    entry_id_t  id, new_id;
    attr_set_t  attrs, new_attrs;
    char buff[128];

    /* TODO iter opt */
    it = ListMgr_RecovResume( &lmgr, path_filter, retry_errors,
                              NULL );
    if ( it == NULL )
    {
        fprintf( stderr, "ERROR: cannot get the list of entries to be recovered\n");
        return -1;
    }

    attrs.attr_mask = RECOV_ATTR_MASK;

    while ( !terminate &&
            ((rc = ListMgr_RecovGetNext( it, &id, &attrs, NULL )) != DB_END_OF_LIST) )
    {
        if (rc)
        {
            fprintf( stderr, "ERROR %d getting entry from recovery table\n", rc );
            ListMgr_CloseIterator( it );
            return rc;
        }

        FormatFileSize( buff, 128, ATTR( &attrs, size ) );

        if ( ATTR_MASK_TEST( &attrs, fullpath) )
            printf("Restoring %s (%s)...", ATTR( &attrs, fullpath), buff);
        else
            printf("Restoring "DFID" (%s)...", PFID(&id), buff);

        /* TODO process entries asynchronously, in parallel, in separate threads*/
        st = rbhext_recover( &id, &attrs, &new_id, &new_attrs, NULL );

        if ((st == RS_FILE_OK) || (st == RS_FILE_EMPTY) || (st == RS_NON_FILE)
            || (st == RS_FILE_DELTA))
        {
            /* don't insert readonly attrs */
            new_attrs.attr_mask &= ~readonly_attr_set;

            /* insert the entry in the database, and update recovery status */
            rc = ListMgr_Insert( &lmgr, &new_id, &new_attrs, TRUE );
            if (rc)
            {
                fprintf(stderr, "DB insert failure for '%s'\n", ATTR(&new_attrs, fullpath));
                st = RS_ERROR;
            }
        }

        /* old id must be used for impacting recovery table */
        if ( ListMgr_RecovSetState( &lmgr, &id, st ) )
            st = RS_ERROR;

        switch (st)
        {
            case RS_FILE_OK: printf(" OK\n"); break;
            case RS_FILE_DELTA: printf(" OK (old version)\n"); break;
            case RS_NON_FILE: printf(" OK (non-file)\n"); break;
            case RS_FILE_EMPTY: printf(" OK (empty file)\n"); break;
            case RS_NOBACKUP: printf(" No backup available\n"); break;
            case RS_ERROR: printf(" FAILED\n"); break;
            default: printf(" ERROR st=%d, rc=%d\n", st, rc ); break;
        }

        /* reset mask */
        attrs.attr_mask = RECOV_ATTR_MASK;
    }

    return 0;
}

static int recov_complete( void )
{
    int rc;
    lmgr_recov_stat_t stats;

    rc = ListMgr_RecovComplete( &lmgr, &stats );
    if ( rc == DB_NOT_ALLOWED )
    {
        printf("\nCannot complete recovery\n\n");
        printf("Current status:\n");
        print_recov_stats( FALSE, &stats );
        return rc;
    }
    else if ( rc == DB_NOT_EXISTS )
    {
        printf("\nERROR: There is no pending recovery.\n" );
        return rc;
    }
    else if ( rc != DB_SUCCESS )
    {
        printf("\nERROR %d finalizing recovery\n", rc );
        return rc;
    }
    else
    {
        printf("\nRecovery successfully completed:\n");
        print_recov_stats( FALSE, &stats );
        return 0;
    }
}

static int recov_status( void )
{
    int rc;
    lmgr_recov_stat_t stats;

    rc = ListMgr_RecovStatus( &lmgr, &stats );
    if (rc)
    {
        if ( rc == DB_NOT_EXISTS )
            fprintf( stderr, "ERROR: There is no pending recovery\n" );
        return rc;
    }

    printf( "Current recovery status:\n");
    print_recov_stats( FALSE, &stats );
    printf("\n");
    return 0;
}

static int recov_list(recov_type_e state)
{
    struct lmgr_iterator_t * it;
    int rc;
    entry_id_t  id;
    attr_set_t  attrs;
    char buff[128];
    recov_status_t st;
    const char * status;

    /* TODO iter opt */
    it = ListMgr_RecovList( &lmgr, state );
    if ( it == NULL )
    {
        fprintf( stderr, "ERROR: cannot get the list of entries\n");
        return -1;
    }

    attrs.attr_mask = RECOV_ATTR_MASK;
    printf("%-8s %-15s %-40s %s\n", "type", "state", "path", "size");

    while ( !terminate &&
            ((rc = ListMgr_RecovGetNext( it, &id, &attrs, &st )) != DB_END_OF_LIST) )
    {
        if (rc)
        {
            fprintf( stderr, "ERROR %d getting entry from recovery table\n", rc );
            ListMgr_CloseIterator( it );
            return rc;
        }

        FormatFileSize( buff, 128, ATTR( &attrs, size ) );
        switch (st)
        {
            case RS_FILE_OK:
                status = "done";
                break;
            case RS_FILE_DELTA:
                status = "done_old_data";
                break;
            case RS_NON_FILE:
                status = "done_non_file";
                break;
            case RS_FILE_EMPTY:
                status = "done_empty";
                break;
            case RS_NOBACKUP:
                status = "done_no_backup";
                break;
            case RS_ERROR:
                status = "failed";
                break;
            case -1:
                status = "todo";
                break;
            default:
                status = "?";
        }

        printf("%-8s %-15s %-40s %s\n", ATTR(&attrs, type), status, ATTR(&attrs, fullpath), buff);

        /* reset mask */
        attrs.attr_mask = RECOV_ATTR_MASK;
    }

    return 0;
}



#define RETRY_ERRORS 0x00000001
#define NO_CONFIRM   0x00000002



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
    int            do_resume = FALSE;
    int            do_complete = FALSE;
    int            do_status = FALSE;
    int            list_state = -1;
    int            force_log_level = FALSE;

    int            log_level = 0;
    int            local_flags = 0;

    int            rc;
    char           err_msg[4096];
    robinhood_config_t config;
    struct sigaction act_sigterm;
    int chgd = 0;
    char    badcfg[RBH_PATH_MAX];

    /* parse command line options */
    while ( ( c = getopt_long( argc, argv, SHORT_OPT_STRING, option_tab, &option_index ) ) != -1 )
    {
        switch ( c )
        {
        case 'S':
            do_start = TRUE;
            break;
        case 's':
            do_status = TRUE;
            break;
        case 'Z':
            do_reset = TRUE;
            break;
        case 'c':
            do_complete = TRUE;
            break;
        case 'r':
            do_resume = TRUE;
            break;
        case 'L':
            if (!strcasecmp(optarg, "all"))
                list_state = RT_ALL;
            else if (!strcasecmp(optarg, "done"))
                list_state = RT_DONE;
            else if (!strcasecmp(optarg, "failed"))
                list_state = RT_FAILED;
            else if (!strcasecmp(optarg, "todo"))
                list_state = RT_TODO;
            else
            {
                fprintf(stderr, "Invalid parameter for option --list: all, done, failed or todo expected.\n");
                exit(1);
            }
            break;
        case 'e':
            local_flags |= RETRY_ERRORS;
            break;
        case 'y':
            local_flags |= NO_CONFIRM;
            break;
        case 'f':
            rh_strncpy(config_file, optarg, MAX_OPT_LEN);
            break;
        case 'D':
            if ( !optarg )
            {
                fprintf(stderr, "Missing mandatory argument <path> for --dir\n");
                exit(1);
            }
            else
            {
                rh_strncpy(path_buff, optarg, MAX_OPT_LEN);
                path_filter = path_buff;
            }
            break;
        case 'o':
            if ( !optarg )
            {
                fprintf(stderr, "Missing mandatory argument <ost_index> for --ost\n");
                exit(1);
            }
            /* parse it as a set */
            if (lmgr_range2list(optarg, DB_UINT, &ost_list))
            {
                fprintf( stderr,
                         "Invalid value '%s' for --ost option: integer or set expected (e.g. 2 or 3,5-8,10-12).\n",
                         optarg );
                exit( 1 );
            }
            /* copy arg to display it */
            rh_strncpy(ost_range_str, optarg, sizeof(ost_range_str));
            break;
        case 'b':
            if ( !optarg )
            {
                fprintf(stderr, "Missing mandatory argument <date_time> for --since\n");
                exit(1);
            }
            since_time = str2date(optarg);
            if (since_time == (time_t)-1) {
                fprintf( stderr,
                         "Invalid date format: yyyymmdd[HH[MM[SS]]] expected\n" );
                exit(1);
            }
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
    if (SearchConfig(config_file, config_file, &chgd, badcfg, MAX_OPT_LEN) != 0)
    {
        fprintf(stderr, "No config file (or too many) found matching %s\n", badcfg );
        exit(2);
    }
    else if (chgd)
    {
        fprintf(stderr, "Using config file '%s'.\n", config_file );
    }

    /* only read ListMgr config */

    if ( ReadRobinhoodConfig( 0, config_file, err_msg, &config, FALSE ) )
    {
        fprintf( stderr, "Error reading configuration file '%s': %s\n", config_file, err_msg );
        exit( 1 );
    }
    process_config_file = config_file;

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

    /* Initialize filesystem access */
    rc = InitFS();
    if (rc)
        exit(rc);

    /* Initialize list manager */
    rc = ListMgr_Init( &config.lmgr_config, FALSE );
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

#ifdef _HSM_LITE
    rc = Backend_Start( &config.backend_config, 0 );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, RECOV_TAG, "Error initializing backend" );
        exit( 1 );
    }
#endif

    /* create signal handlers */
    memset( &act_sigterm, 0, sizeof( act_sigterm ) );
    act_sigterm.sa_flags = 0;
    act_sigterm.sa_handler = terminate_handler;
    if ( sigaction( SIGTERM, &act_sigterm, NULL ) == -1
         || sigaction( SIGINT, &act_sigterm, NULL ) == -1 )
    {
        DisplayLog( LVL_CRIT, RECOV_TAG,
                    "Error while setting signal handlers for SIGTERM and SIGINT: %s",
                    strerror( errno ) );
        exit( 1 );
    }
    else
        DisplayLog( LVL_VERB, RECOV_TAG,
                    "Signals SIGTERM and SIGINT (abort command) are ready to be used" );

    if (do_status)
        rc = recov_status();
    else if (list_state != -1)
        rc = recov_list(list_state);
    else if (do_start)
        rc = recov_start();
    else if (do_reset)
        rc = recov_reset( local_flags & NO_CONFIRM );
    else if (do_resume)
        rc = recov_resume( local_flags & RETRY_ERRORS );
    else if (do_complete)
        rc = recov_complete();
    else
    {
        display_help( bin );
        rc = 1;
    }

    ListMgr_CloseAccess( &lmgr );

    return rc;
}
