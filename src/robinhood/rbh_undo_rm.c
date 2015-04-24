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
 * Command for restoring an entry that was accidentaly removed from filesystem.
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

#define LOGTAG "UndoRm"

static struct option option_tab[] =
{
    /* options for cancelling remove operation */
    {"list", no_argument, NULL, 'L'},
    {"restore", no_argument, NULL, 'R'},

    /* config file options */
    {"config-file", required_argument, NULL, 'f'},

    /* log options */
    {"log-level", required_argument, NULL, 'l'},

    /* miscellaneous options */
    {"help", no_argument, NULL, 'h'},
    {"version", no_argument, NULL, 'V'},

    {NULL, 0, NULL, 0}

};

#define SHORT_OPT_STRING    "LRf:l:hV"

/* global variables */

static lmgr_t  lmgr;
char path_filter[RBH_PATH_MAX] = "";

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
    _B "Usage:" B_ " %s [options] <path|fid>\n"
    "\n"
    _B "Actions:" B_ "\n"
    "    " _B "--list" B_ ", " _B "-L" B_ "\n"
    "        List removed entries in the given directory.\n"
    "    " _B "--restore" B_ ", " _B "-R" B_ "\n"
    "        Restore removed entries in the given directory.\n"
    "\n"
    _B "Config file options:" B_ "\n"
    "    " _B "-f" B_ " " _U "file" U_ ", " _B "--config-file=" B_ _U "file" U_ "\n"
    "        Path to configuration file (or short name).\n"
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
    printf( "Product:         " PACKAGE_NAME " rm cancellation tool\n" );
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

/*
 * Append global filters on path
 * \param do_display [in] display filters?
 * \param initialized [in/out] indicate if the filter is initialized.
 */
static int mk_path_filter( lmgr_filter_t * filter, int do_display, int * initialized )
{
    filter_value_t fv;
    char path_regexp[RBH_PATH_MAX] = "";
    size_t  len;

    /* is a filter on path specified? */
    if ( !EMPTY_STRING( path_filter ) )
    {
        if ( (initialized != NULL) && !(*initialized) )
        {
            lmgr_simple_filter_init( filter );
            *initialized = TRUE;
        }
        if ( do_display )
            printf("filter path: %s\n", path_filter );

        len = strlen(path_filter);
        /* remove last slash */
        if (path_filter[len-1] == '/')
            path_filter[len-1] = '\0';

        /* as this is a RLIKE matching, shell regexp must be replaced by perl:
         * [abc] => OK
         * '*' => '.*'
         * '?' => '.'
         */
        str_replace(path_filter, "*", ".*");
        str_replace(path_filter, "?", ".");

        /* match 'path$' OR 'path/.*' */
        snprintf(path_regexp, RBH_PATH_MAX, "%s($|/.*)", path_filter);
        fv.value.val_str = path_regexp;

        lmgr_simple_filter_add(filter, ATTR_INDEX_fullpath, RLIKE, fv, 0);
    }
    return 0;
}

static int is_fid_filter(entry_id_t * id)
{
    lustre_fid  fid;
    if ( !EMPTY_STRING( path_filter ) )
    {
        if (sscanf(path_filter, SFID, RFID(&fid)) != FID_SCAN_CNT)
            return FALSE;
        else {
            if (id)
                *id = fid;
            return TRUE;
        }
    }
    return FALSE;
}

static inline void display_rm_entry(entry_id_t * id, const char *last_known_path,
#ifdef _HSM_LITE
                             const char *bkpath,
#endif
                             time_t soft_rm_time, time_t expiration_time)
{
    char           date_rm[128];
    char           date_exp[128];
    struct tm      t;

    strftime( date_rm, 128, "%Y/%m/%d %T", localtime_r( &soft_rm_time, &t ) );
    strftime( date_exp, 128, "%Y/%m/%d %T", localtime_r( &expiration_time, &t ) );

    printf( "Fid:               "DFID"\n", PFID(id) );
    if ( !EMPTY_STRING(last_known_path) )
        printf( "Last known path:   %s\n", last_known_path );
#ifdef _HSM_LITE
    if ( !EMPTY_STRING(bkpath) )
        printf( "Backend path:      %s\n", bkpath );
#endif
    printf( "Removal time:      %s\n", date_rm );
    if ( expiration_time <= time(NULL) )
        printf( "Delayed until:     %s (expired)\n", date_exp );
    else
        printf( "Delayed until:     %s\n", date_exp );
}


static int list_rm( void )
{
    int            rc, index;
    struct lmgr_rm_list_t * list;
    entry_id_t     id;
    char   last_known_path[RBH_PATH_MAX] = "";
#ifdef _HSM_LITE
    char   bkpath[RBH_PATH_MAX] = "";
#endif

    time_t soft_rm_time = 0;
    time_t expiration_time = 0;

    unsigned long long total_count = 0;
    lmgr_filter_t  filter;

    if (is_fid_filter(&id)) /* 1 single entry */
    {
         rc = ListMgr_GetRmEntry( &lmgr, &id, last_known_path,
#ifdef _HSM_LITE
                            bkpath,
#endif
                            &soft_rm_time, &expiration_time );
        if (rc == DB_SUCCESS)
        {
            display_rm_entry( &id, last_known_path,
#ifdef _HSM_LITE
                            bkpath,
#endif
                            soft_rm_time, expiration_time);
        }
        else if ( rc == DB_NOT_EXISTS )
            fprintf(stderr, DFID": fid not found in deferred removal list\n",
                    PFID(&id));
        else
            fprintf(stderr, "ERROR %d in ListMgr_GetRmEntry("DFID")\n",
                    rc, PFID(&id));
        return rc;
    }
    else /* list of entries */
    {
        lmgr_simple_filter_init( &filter );

        /* append global filters */
        mk_path_filter( &filter, TRUE, NULL );

        /* list all deferred rm, even if non expired */
        list = ListMgr_RmList( &lmgr, FALSE, &filter );

        if ( list == NULL )
        {
            DisplayLog( LVL_CRIT, LOGTAG,
                        "ERROR: Could not retrieve removed entries from database." );
            return -1;
        }

        index = 0;
        while ( ( rc = ListMgr_GetNextRmEntry( list, &id, last_known_path,
    #ifdef _HSM_LITE
                            bkpath,
    #endif
                            &soft_rm_time, &expiration_time )) == DB_SUCCESS )
        {
            total_count++;
            index++;

            printf( "\n" );
            display_rm_entry( &id, last_known_path,
#ifdef _HSM_LITE
                            bkpath,
#endif
                            soft_rm_time, expiration_time );

            /* prepare next call */
            last_known_path[0] = '\0';
#ifdef _HSM_LITE
            bkpath[0] = '\0';
#endif
            soft_rm_time = 0;
            expiration_time = 0;
        }

        ListMgr_CloseRmList(list);
    }
    return 0;
}

static inline void undo_rm_helper( entry_id_t * id, const char *last_known_path,
#ifdef _HSM_LITE
                             const char *bkpath
#endif
                             )
{
    entry_id_t new_id;
    recov_status_t st;
    attr_set_t     attrs, new_attrs;
    int rc;

    /* XXX src path must be in the same filesystem as backend
     * because it we be renamed */

    if ( EMPTY_STRING( last_known_path ) )
    {
        fprintf(stderr, "Last filesystem path is not known for fid "DFID", backend_path=%s.\n",
                PFID(id), bkpath);
        fprintf(stderr, " ----> skipped\n");
        return;
    }

    printf("Restoring '%s'...\n", last_known_path );

    ATTR_MASK_INIT(&attrs);
    ATTR_MASK_SET(&attrs, fullpath);
    strcpy( ATTR(&attrs, fullpath), last_known_path );

    if ( !EMPTY_STRING( bkpath ) )
    {
        ATTR_MASK_SET(&attrs, backendpath);
        strcpy( ATTR(&attrs, backendpath), bkpath );
    }

    /* copy file to Lustre */
    ATTR_MASK_INIT(&new_attrs);
    st = rbhext_recover( id, &attrs, &new_id, &new_attrs, NULL );
    if ((st == RS_FILE_OK) || (st == RS_FILE_DELTA)|| (st == RS_FILE_EMPTY)
        ||  (st == RS_NON_FILE))
    {
        printf("Success\n");
        /* discard entry from remove list */
        if ( ListMgr_SoftRemove_Discard(&lmgr, id) != 0 )
            fprintf(stderr, "Error: could not remove previous id "DFID" from database\n", PFID(id) );
        /* clean read-only attrs */
        new_attrs.attr_mask &= ~readonly_attr_set;
        /* insert or update it in the db */
        rc = ListMgr_Insert( &lmgr, &new_id, &new_attrs, TRUE );
        if ( rc == 0 )
            printf("Entry successfully updated in the dabatase\n");
        else
            fprintf(stderr, "ERROR %d inserting entry in the database\n", rc );
    }
    else
    {
        printf("ERROR\n");
    }
}


static int undo_rm( void )
{
    int            rc;
    struct lmgr_rm_list_t * list;
    entry_id_t     id;
    char   last_known_path[RBH_PATH_MAX] = "";
#ifdef _HSM_LITE
    char   bkpath[RBH_PATH_MAX] = "";
#endif
    unsigned long long total_count = 0;
    lmgr_filter_t  filter;

    if (is_fid_filter(&id)) /* 1 single entry */
    {
         rc = ListMgr_GetRmEntry( &lmgr, &id, last_known_path,
#ifdef _HSM_LITE
                            bkpath,
#endif
                            NULL, NULL );
        if (rc == DB_SUCCESS)
        {
            undo_rm_helper( &id, last_known_path,
#ifdef _HSM_LITE
                            bkpath
#endif
                            );
        }
        else if ( rc == DB_NOT_EXISTS )
            fprintf(stderr, DFID": fid not found in deferred removal list\n",
                    PFID(&id));
        else
            fprintf(stderr, "ERROR %d in ListMgr_GetRmEntry("DFID")\n",
                    rc, PFID(&id));
        return rc;
    }
    else /* recover a list of entries */
    {
        lmgr_simple_filter_init( &filter );

        /* append global filters */
        mk_path_filter( &filter, TRUE, NULL );

        /* list files to be recovered */
        list = ListMgr_RmList( &lmgr, FALSE, &filter );

        if ( list == NULL )
        {
            DisplayLog( LVL_CRIT, LOGTAG,
                        "ERROR: Could not retrieve removed entries from database." );
            return -1;
        }

        while ( ( rc = ListMgr_GetNextRmEntry( list, &id, last_known_path,
    #ifdef _HSM_LITE
                            bkpath,
    #endif
                            NULL, NULL )) == DB_SUCCESS )
        {
            total_count++;

            undo_rm_helper( &id, last_known_path,
#ifdef _HSM_LITE
                            bkpath
#endif
                            );
        }
        ListMgr_CloseRmList(list);
    }
    return 0;
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

    enum { ACTION_NONE, ACTION_LIST, ACTION_RESTORE } action = ACTION_NONE;
    int            force_log_level = FALSE;
    int            log_level = 0;

    int            rc;
    char           err_msg[4096];
    robinhood_config_t config;
    int chgd = 0;
    char    badcfg[RBH_PATH_MAX];

    /* parse command line options */
    while ( ( c = getopt_long( argc, argv, SHORT_OPT_STRING, option_tab,
                               &option_index ) ) != -1 )
    {
        switch ( c )
        {
        case 'L':
            if ( (action != ACTION_NONE) && (action != ACTION_LIST) )
                fprintf( stderr, "WARNING: only a single action (--list or --restore) is expected\n"
                                 "on command line. '--restore' will be ignored.\n" );
            action = ACTION_LIST;
            break;
        case 'R':
            if ( (action != ACTION_NONE) && (action != ACTION_RESTORE) )
                fprintf( stderr, "WARNING: only a single action (--list or --restore) is expected\n"
                                 "on command line. '--list' will be ignored.\n" );
            action = ACTION_RESTORE;
            break;
        case 'f':
            rh_strncpy(config_file, optarg, MAX_OPT_LEN);
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

    /* 1 expected argument: path */
    if ( optind != argc - 1 )
    {
        fprintf( stderr, "Error: missing mandatory argument on command line: <path|fid>\n" );
        exit( 1 );
    }
    rh_strncpy(path_filter, argv[optind], RBH_PATH_MAX);

    /* get default config file, if not specified */
    if (SearchConfig(config_file, config_file, &chgd, badcfg, MAX_OPT_LEN) != 0)
    {
        fprintf(stderr, "No config file (or too many) found matching %s\n", badcfg);
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

    /* Initialize Filesystem access */
    rc = InitFS();
    if (rc)
        exit(rc);

    /* Initialize list manager */
    rc = ListMgr_Init( &config.lmgr_config, FALSE );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, LOGTAG, "Error %d initializing list manager", rc );
        exit( rc );
    }
    else
        DisplayLog( LVL_DEBUG, LOGTAG, "ListManager successfully initialized" );

    if ( CheckLastFS(  ) != 0 )
        exit( 1 );

    /* Create database access */
    rc = ListMgr_InitAccess( &lmgr );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, LOGTAG, "Error %d: cannot connect to database", rc );
        exit( rc );
    }

#ifdef _HSM_LITE
    rc = Backend_Start( &config.backend_config, 0 );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, LOGTAG, "Error initializing backend" );
        exit( 1 );
    }
#endif

    /* perform the action */
    switch( action )
    {
        case ACTION_LIST:
            rc= list_rm();
            break;
        case ACTION_RESTORE:
            rc = undo_rm();
            break;
        case ACTION_NONE:
            display_help( bin );
            rc = 1;
            break;
        default:
            fprintf(stderr, "Unexpected action (action code=%#x)\n", action );
            display_help( bin );
            rc = EINVAL;
            break;
    }

    ListMgr_CloseAccess( &lmgr );

    return rc;

}
