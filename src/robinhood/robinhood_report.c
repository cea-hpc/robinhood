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
 * Command for retrieving stats about filesystem.
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

#define REPORT_TAG    "Report"

#define DEFAULT_TOP_SIZE 20

/* Array of options for getopt_long().
 * Each record consists of: { const char *name, int has_arg, int * flag, int val }
 */

#define OPT_DUMP_USER   256
#define OPT_DUMP_GROUP  257
#define OPT_DUMP_OST    258
#define OPT_DUMP_STATUS 259
#define OPT_CLASS_INFO  260

#ifdef ATTR_INDEX_status
/* ===  status display and conversion routines === */

/* status conversion array */
struct status_descr
{
    file_status_t db_status;
    char * short_descr;
    char * long_descr;
}
status_array[] =
{
#ifdef _LUSTRE_HSM
    { STATUS_NO_FLAGS, "new", "new file (no HSM status)" },
    { STATUS_MODIFIED, "modified", "modified (must be archived)" },
    { STATUS_RESTORE_RUNNING, "retrieving", "being retrieved" },
    { STATUS_ARCHIVE_RUNNING, "archiving", "being archived" },
    { STATUS_UP_TO_DATE, "synchro", "synchronized (can be purged)" },
    { STATUS_RELEASED, "released", "released" },
    { STATUS_RELEASE_PENDING, "release_pending", "release pending" },

    /* alternative names */
    { STATUS_MODIFIED, "dirty", "dirty (modified)" },
    { STATUS_RESTORE_RUNNING, "restoring", "being retrieved" },

#define ALLOWED_STATUS "new, modified|dirty, retrieving|restoring, archiving, synchro, released, release_pending"

#elif defined(_SHERPA)
    { STATUS_UNKNOWN, "unknown", "unknown" },
    { STATUS_NO_REF, "ref_missing", "reference is missing" },
    { STATUS_MODIFIED, "modified", "modified (must be archived)" },
    { STATUS_RESTORE_RUNNING, "retrieving", "being retrieved" },
    { STATUS_ARCHIVE_RUNNING, "archiving", "being archived" },
    { STATUS_UP_TO_DATE, "synchro", "synchronized (can be purged)" },
    { STATUS_OUT_OF_DATE, "obsolete", "obsolete (older than reference)" },

    /* alternative names */
    { STATUS_MODIFIED, "dirty", "dirty (modified)" },
    { STATUS_NO_REF, "missing_ref", "reference is missing" },
    { STATUS_RESTORE_RUNNING, "restoring", "being retrieved" },

#define ALLOWED_STATUS "unknown, ref_missing|missing_ref, modified|dirty, retrieving|restoring, archiving, synchro, obsolete"

#endif
    { (file_status_t)-1, NULL, NULL }
};

static const char * db_status2str( file_status_t status, int csv )
{
    struct status_descr * curr;

    for ( curr = status_array; curr->short_descr != NULL; curr ++ )
    {
       if ( status == curr->db_status )
       {
            if ( csv )
                return curr->short_descr;
            else
                return curr->long_descr;
       }
    }
    /* not found */
    return "?";
}

static file_status_t status2dbval( char * status_str )
{
    struct status_descr * curr;
    int len;

    if (  (status_str == NULL) || (status_str[0] == '\0') )
        return (file_status_t)-1;

    len = strlen( status_str );

    for ( curr = status_array; curr->short_descr != NULL; curr ++ )
    {
       if ( !strncmp( status_str, curr->short_descr, len ) )
            return curr->db_status;
    }
    /* not found */
    return (file_status_t)-1;
}

#endif /* status attr exists */


static struct option option_tab[] = {

    /* Stats selectors */
    {"activity", no_argument, NULL, 'a'},

    {"fsinfo", no_argument, NULL, 'i'},
    {"fs-info", no_argument, NULL, 'i'},

    {"userinfo", optional_argument, NULL, 'u'},
    {"user-info", optional_argument, NULL, 'u'},

    {"groupinfo", optional_argument, NULL, 'g'},
    {"group-info", optional_argument, NULL, 'g'},

    {"classinfo", optional_argument, NULL, OPT_CLASS_INFO},
    {"class-info", optional_argument, NULL, OPT_CLASS_INFO},
#ifndef _LUSTRE_HSM
    {"topdirs", optional_argument, NULL, 'd'},
    {"top-dirs", optional_argument, NULL, 'd'},
#endif
    {"topsize", optional_argument, NULL, 's'},
    {"top-size", optional_argument, NULL, 's'},
    {"toppurge", optional_argument, NULL, 'p'},
    {"top-purge", optional_argument, NULL, 'p'},
#ifdef HAVE_RMDIR_POLICY
    {"toprmdir", optional_argument, NULL, 'r'},
    {"top-rmdir", optional_argument, NULL, 'r'},
#endif
    {"topusers", optional_argument, NULL, 'U'},
    {"top-users", optional_argument, NULL, 'U'},
#ifdef HAVE_RM_POLICY
    {"deferred-rm", no_argument, NULL, 'R' },
#endif
    {"dump-all", no_argument, NULL, 'D' },
    {"dump-user", required_argument, NULL, OPT_DUMP_USER },
    {"dump-group", required_argument, NULL, OPT_DUMP_GROUP },
#ifdef _LUSTRE
    {"dump-ost", required_argument, NULL, OPT_DUMP_OST },
#endif
#ifdef ATTR_INDEX_status
    {"dump-status", required_argument, NULL, OPT_DUMP_STATUS },
#endif

    /* additional options for topusers etc... */
    {"filter-path", required_argument, NULL, 'P' },
    {"filter-class", required_argument, NULL, 'C' },

    /* config file options */
    {"config-file", required_argument, NULL, 'f'},

    /* output format option */
    {"csv", no_argument, NULL, 'c'},

    /* verbosity level */
    {"log-level", required_argument, NULL, 'l'},

    /* miscellaneous options */
    {"help", no_argument, NULL, 'h'},
    {"version", no_argument, NULL, 'V'},

    {NULL, 0, NULL, 0}

};

#define SHORT_OPT_STRING    "aiDu:g:d:s:p:r:U:P:C:Rf:cl:hV"

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
    _B "Stats switches:" B_ "\n"
    "    " _B "--activity" B_ ", " _B "-a" B_ "\n"
    "        Display stats abount daemon's activity.\n"
    "    " _B "--fs-info" B_ ", " _B "-i" B_ "\n"
    "        Display filesystem content statistics.\n"
    "    " _B "--class-info" B_ " [=" _U "fileclass" U_ "]\n"
    "        Display Fileclasses summary. Use optional parameter " _U "fileclass" U_ "\n"
    "        for retrieving stats about matching fileclasses.\n"
    "    " _B "--user-info" B_ " [=" _U "user" U_ "], " _B "-u" B_ " " _U "user" U_ "\n"
    "        Display user statistics. Use optional parameter " _U "user" U_ " for retrieving stats about a single user.\n"
    "    " _B "--group-info" B_ " [=" _U "group" U_ "], " _B "-g" B_ " " _U "group" U_ "\n"
    "        Display group statistics. Use optional parameter " _U "group" U_ " for retrieving stats about a single group.\n"
#ifndef _LUSTRE_HSM
    "    " _B "--top-dirs" B_ " [=" _U "count" U_ "], " _B "-d" B_ " " _U "count" U_ "\n"
    "        Display largest directories. Optional argument indicates the number of directories to be returned (default: 20).\n"
#endif
    "    " _B "--top-size" B_ " [=" _U "count" U_ "], " _B "-s" B_ " " _U "count" U_ "\n"
    "        Display largest files. Optional argument indicates the number of files to be returned (default: 20).\n"
    "    " _B "--top-purge" B_ " [=" _U "count" U_ "], " _B "-p" B_ " " _U "count" U_ "\n"
    "        Display oldest entries eligible for purge. Optional argument indicates the number of entries to be returned (default: 20).\n"
#ifdef HAVE_RMDIR_POLICY
    "    " _B "--top-rmdir" B_ " [=" _U "count" U_ "], " _B "-r" B_ " " _U "count" U_ "\n"
    "        Display oldest empty directories eligible for rmdir. Optional argument indicates the number of dirs to be returned (default: 20).\n"
#endif
    "    "  _B "--top-users" B_ " [=" _U "count" U_ "], " _B "-U" B_ " " _U "count" U_ "\n"
    "        Display top disk space consumers. Optional argument indicates the number of users to be returned (default: 20).\n"
#ifdef HAVE_RM_POLICY
    "    " _B "--deferred-rm" B_ ", " _B "-R" B_ "\n"
    "        Display files to be removed from HSM.\n"
#endif
    "    "  _B "--dump-all" B_ ", " _B "-D" B_ "\n"
    "        Dump all filesystem entries.\n"
    "    "  _B "--dump-user" B_ " " _U "user" U_ "\n"
    "        Dump all entries for the given user.\n"
    "    "  _B "--dump-group" B_ " " _U "group" U_ "\n"
    "        Dump all entries for the given group.\n"
#ifdef _LUSTRE
    "    "  _B "--dump-ost" B_ " " _U "ost_index" U_ "\n"
    "        Dump all entries on the given OST.\n"
#endif
#ifdef ATTR_INDEX_status
    "    "  _B "--dump-status" B_ " " _U "status" U_ "\n"
    "        Dump all entries with the given status ("ALLOWED_STATUS").\n"
#endif
    "\n"
    _B "Filter options:" B_ "\n"
    "The following filters can be speficied for reports:\n"
    "    " _B "-P" B_ " " _U "path" U_ ", " _B "--filter-path" B_ " " _U "path" U_ "\n"
    "        Display the report only for objects in the given path.\n"
    "    " _B "-C" B_ " " _U "class" U_ ", " _B "--filter-class" B_ " " _U "class" U_ "\n"
    "        Report only entries in the given FileClass.\n"
    "\n"
    _B "Config file options:" B_ "\n"
    "    " _B "-f" B_ " " _U "file" U_ ", " _B "--config-file=" B_ _U "file" U_ "\n"
    "        Specifies path to configuration file.\n"
    "\n"
    _B "Output format options:" B_ "\n"
    "    " _B "-c" B_ " , " _B "--csv" B_ "\n"
    "        Output stats in a csv-like format for parsing\n"
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
    printf( "Version:         " PACKAGE_VERSION "\n" );
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
#else
#error "No purpose was specified"
#endif

/* Access by Fid ? */
#ifdef _HAVE_FID
    printf( "    Address entries by FID\n" );
#else
    printf( "    Address entries by path\n" );
#endif

    printf( "\n" );
#ifdef _LUSTRE
#ifdef LUSTRE_VERSION
    printf( "Lustre Version: " LUSTRE_VERSION "\n" );
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




static lmgr_t  lmgr;

/* global filter variables */
char path_filter[1024] = "";
char path_regexp[1024] = "";

char class_filter[1024] = "";

void report_activity( int csv )
{
    char           value[1024];
    time_t         timestamp;
    char           date[128];
    struct tm      t;
    int            rc;

    if ( !csv )
        printf( "\n" );

    /* Last FS scan */
    rc = ListMgr_GetVar( &lmgr, LAST_SCAN_VAR, value );
    if ( rc == DB_SUCCESS )
    {
        timestamp = atoi( value );
        strftime( date, 128, "%Y/%m/%d %T", localtime_r( &timestamp, &t ) );
        if ( csv )
            printf( "last_scan, %s\n", date );
        else
            printf( "Last Filesystem scan:     %s\n", date );
    }
    else if ( rc == DB_NOT_EXISTS )
    {
        if ( csv )
            printf( "last_scan, none\n" );
        else
            printf( "Filesystem has never been scanned\n" );
    }
    else
    {
        DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR retrieving variable " LAST_SCAN_VAR " from database" );
    }

#ifndef _LUSTRE_HSM
    /* Last FS scan */
    rc = ListMgr_GetVar( &lmgr, SCAN_INTERVAL_VAR, value );
    if ( rc == DB_SUCCESS )
    {
        timestamp = atoi( value );
        FormatDurationFloat( date, 128, timestamp );
        if ( csv )
            printf( "current_scan_interval, %s\n", date );
        else
            printf( "Current scan interval:    %s\n", date );
    }
    else if ( rc != DB_NOT_EXISTS )
    {
        DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR retrieving variable " SCAN_INTERVAL_VAR " from database" );
    }
#endif

    if ( !csv )
        printf( "\n" );

    /* max usage */
    rc = ListMgr_GetVar( &lmgr, USAGE_MAX_VAR, value );
    if ( rc == DB_SUCCESS )
    {
        if ( csv )
            printf( "usage_max, %s\n", value );
        else
            printf( "Storage unit usage max:   %s%%\n", value );
    }
    else if ( rc == DB_NOT_EXISTS )
    {
        if ( csv )
            printf( "usage_max, not checked\n" );
        else
            printf( "Storage usage has never been checked\n" );
    }
    else
    {
        DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR retrieving variable " USAGE_MAX_VAR " from database" );
    }

#ifdef HAVE_MIGR_POLICY
    if ( !csv )
        printf( "\n" );

    /* Last migration */
    rc = ListMgr_GetVar( &lmgr, LAST_MIGR_TIME, value );
    if ( rc == DB_SUCCESS )
    {
        timestamp = atoi( value );
        strftime( date, 128, "%Y/%m/%d %T", localtime_r( &timestamp, &t ) );
        if ( csv )
            printf( "last_migration_time, %s\n", date );
        else
            printf( "Last migration:           %s\n", date );
    }
    else if ( rc == DB_NOT_EXISTS )
    {
        if ( csv )
            printf( "last_migration_time, none\n" );
        else
            printf( "No migration was performed on this filesystem\n" );
        return;
    }
    else
    {
        DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR retrieving variable " LAST_MIGR_TIME " from database" );
        return;
    }

    if ( ListMgr_GetVar( &lmgr, LAST_MIGR_STATUS, value ) == DB_SUCCESS )
    {
        if ( csv )
            printf( "last_migration_status, %s\n", value );
        else
            printf( "    Status:               %s\n", value );
    }

    if ( ListMgr_GetVar( &lmgr, LAST_MIGR_INFO, value ) == DB_SUCCESS )
    {
        if ( csv )
            printf( "last_migration_info, %s\n", value );
        else
            printf( "    Migration info:       %s\n", value );
    }

#endif

    if ( !csv )
        printf( "\n" );

    /* Last purge */
    rc = ListMgr_GetVar( &lmgr, LAST_PURGE_TIME, value );
    if ( rc == DB_SUCCESS )
    {
        timestamp = atoi( value );
        strftime( date, 128, "%Y/%m/%d %T", localtime_r( &timestamp, &t ) );
        if ( csv )
            printf( "last_purge_time, %s\n", date );
        else
            printf( "Last purge:               %s\n", date );
    }
    else if ( rc == DB_NOT_EXISTS )
    {
        if ( csv )
            printf( "last_purge_time, none\n" );
        else
            printf( "No purge was performed on this filesystem\n" );
        return;
    }
    else
    {
        DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR retrieving variable " LAST_PURGE_TIME " from database" );
        return;
    }

    if ( ListMgr_GetVar( &lmgr, LAST_PURGE_TARGET, value ) == DB_SUCCESS )
    {
        if ( csv )
            printf( "last_purge_target, %s\n", value );
        else
            printf( "    Target:               %s\n", value );
    }

    if ( ListMgr_GetVar( &lmgr, LAST_PURGE_STATUS, value ) == DB_SUCCESS )
    {
        if ( csv )
            printf( "last_purge_status, %s\n", value );
        else
            printf( "    Status:               %s\n", value );
    }
    if ( !csv )
        printf( "\n" );

}


typedef enum {DUMP_ALL, DUMP_USR, DUMP_GROUP, DUMP_OST, DUMP_STATUS } type_dump;

static inline const char * class_format( const char * class_name )
{
    if ( class_name == NULL )
        return "[n/a]";
    if ( !strcasecmp(class_name, CLASS_DEFAULT ) )
        return "[default]";
    else if ( !strcasecmp(class_name, CLASS_IGNORED ) )
        return "[ignored]";
    else
        return class_name;
}

#ifdef ATTR_INDEX_archive_class
static inline const char * migr_class( attr_set_t * attrs )
{
    if (!ATTR_MASK_TEST(attrs, archive_class))
        return "";
    else
        return class_format( ATTR(attrs, archive_class) );
}
#endif

static inline const char * release_class( attr_set_t * attrs )
{
    if (!ATTR_MASK_TEST(attrs, release_class))
        return "";
    else
        return class_format( ATTR(attrs, release_class) );
}


/*
 * Append global filters on path, class...
 * \param do_display [in] display filters?
 * \param initialized [in/out] indicate if the filter is initialized.
 */
static int mk_global_filters( lmgr_filter_t * filter, int do_display, int * initialized )
{
    filter_value_t fv;

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
        fv.val_str = path_regexp;
        lmgr_simple_filter_add( filter, ATTR_INDEX_fullpath, LIKE, fv, 0 ); 
    }

    if ( !EMPTY_STRING( class_filter ) )
    {
        if ( (initialized != NULL) && !(*initialized) )
        {
            lmgr_simple_filter_init( filter );
            *initialized = TRUE;
        }
        if ( do_display )
            printf("filter class: %s\n", class_format(class_filter) );

        fv.val_str = class_filter;

#ifndef ATTR_INDEX_archive_class
        /* single test */
        lmgr_simple_filter_add( filter, ATTR_INDEX_release_class, LIKE, fv, 0 );
#else
        /* archive class or release class */
        lmgr_simple_filter_add( filter, ATTR_INDEX_archive_class, LIKE, fv,
                                FILTER_FLAG_BEGIN );
        lmgr_simple_filter_add( filter, ATTR_INDEX_release_class, LIKE, fv,
                                FILTER_FLAG_OR | FILTER_FLAG_END );
#endif
    }

    return 0;
}


void dump_entries( type_dump type, int int_arg, char * str_arg, int csv )
{
    /* get basic information */
    int            mask_sav, rc;
    lmgr_filter_t  filter;
    filter_value_t fv;
    struct lmgr_iterator_t *it;
    attr_set_t     attrs;
    entry_id_t     id;

    lmgr_simple_filter_init( &filter );

    /* append global filters */
    mk_global_filters( &filter, TRUE, NULL );

    /* what do we dump? */
    switch( type )
    {
        case DUMP_ALL:
            /* no filter */
            break;
        case DUMP_USR:
            fv.val_str = str_arg;
            lmgr_simple_filter_add( &filter, ATTR_INDEX_owner, LIKE, fv, 0 );
            break;
        case DUMP_GROUP:
            fv.val_str = str_arg;
            lmgr_simple_filter_add( &filter, ATTR_INDEX_gr_name, LIKE, fv, 0 );
            break;
        case DUMP_OST:
            fv.val_int = int_arg;
            lmgr_simple_filter_add( &filter, ATTR_INDEX_stripe_items, EQUAL, fv, 0 );
            break;
#ifdef ATTR_INDEX_status
       case DUMP_STATUS:
                fv.val_int = int_arg;
                lmgr_simple_filter_add( &filter, ATTR_INDEX_status, EQUAL, fv, 0 );
                break;
#endif
        default:
            DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR: unexpected dump command" );
            return;
    }

    /* attributes to be retrieved */
    ATTR_MASK_INIT( &attrs );
    ATTR_MASK_SET( &attrs, type );
    ATTR_MASK_SET( &attrs, fullpath );
    ATTR_MASK_SET( &attrs, owner );
    ATTR_MASK_SET( &attrs, gr_name );
    ATTR_MASK_SET( &attrs, size );
#ifdef ATTR_INDEX_archive_class
    ATTR_MASK_SET( &attrs, archive_class );
#endif
    ATTR_MASK_SET( &attrs, release_class );

#ifdef ATTR_INDEX_status
    ATTR_MASK_SET( &attrs, status );
#endif

    mask_sav = attrs.attr_mask;

    it = ListMgr_Iterator( &lmgr, &filter, NULL, NULL );

    lmgr_simple_filter_free( &filter );

    if ( it == NULL )
    {
        DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR: Could not retrieve top file size from database." );
        return;
    }

    /* print header */
    printf( "%7s, "
#ifdef ATTR_INDEX_status
            "%10s, "
#endif
            "%*s, %10s, %10s, "
#ifdef ATTR_INDEX_archive_class
            "%20s, "
#endif
            "%20s, %s\n",
            "type",
#ifdef ATTR_INDEX_status
            "status",
#endif
            (csv?15:10),
            "size", "owner", "group",
#ifdef ATTR_INDEX_archive_class
            "migr. class",
#endif
            "purge class",
            "path" );


    while ( ( rc = ListMgr_GetNext( it, &id, &attrs ) ) == DB_SUCCESS )
    {
        if ( csv )
        {
            printf( "%7s, "
    #ifdef ATTR_INDEX_status
                    "%10s, "
    #endif
                    "%15llu, %10s, %10s, "
    #ifdef ATTR_INDEX_archive_class
                    "%20s, "
    #endif
                    "%20s, %s\n",
                    ATTR( &attrs, type ),
    #ifdef ATTR_INDEX_status
                    db_status2str( ATTR(&attrs, status), TRUE ),
    #endif
                    (unsigned long long)ATTR( &attrs, size ),
                    ATTR( &attrs, owner ), ATTR( &attrs, gr_name ),
     #ifdef ATTR_INDEX_archive_class
                    migr_class(&attrs),
     #endif
                    release_class(&attrs),
                    ATTR( &attrs, fullpath ) );
        }
        else
        {
            char strsz[128];

            printf( "%7s, "
#ifdef ATTR_INDEX_status
                    "%10s, "
#endif
                    "%10s, %10s, %10s, "
#ifdef ATTR_INDEX_archive_class
                    "%20s, "
#endif
                    "%20s, %s\n",
                    ATTR( &attrs, type ),
#ifdef ATTR_INDEX_status
                    db_status2str( ATTR(&attrs, status), TRUE ),
#endif
                    FormatFileSize( strsz, 128, ATTR( &attrs, size ) ),
                    ATTR( &attrs, owner ), ATTR( &attrs, gr_name ),
#ifdef ATTR_INDEX_archive_class
                    migr_class(&attrs),
#endif
                    release_class(&attrs),
                    ATTR( &attrs, fullpath ) );
        }

        ListMgr_FreeAttrs( &attrs );
        /* prepare next call */
        attrs.attr_mask = mask_sav;
    }

    ListMgr_CloseIterator( it );

}


void report_fs_info( int csv_format )
{
    unsigned int   result_count;
    struct lmgr_report_t *it;
    int            rc;

    lmgr_filter_t  filter;
    int is_filter = FALSE;

#ifdef _SHERPA
    filter_value_t fv;
#endif

#if defined( _LUSTRE_HSM ) || defined( _SHERPA )
#define FSINFOCOUNT 6
#else
#define FSINFOCOUNT 8
#endif

    db_value_t     result[FSINFOCOUNT];

    /* To be retrieved:
     * - type
     * - number of items for this type
     * - MIN/MAX/SUM size
     * - MIN/MAX/SUM dircount
     */
    report_field_descr_t fs_info[FSINFOCOUNT] = {
#if defined( _LUSTRE_HSM ) || defined( _SHERPA )
        {ATTR_INDEX_status, REPORT_GROUP_BY, SORT_ASC, FALSE, 0, {NULL}},
#else
        {ATTR_INDEX_type, REPORT_GROUP_BY, SORT_ASC, FALSE, 0, {NULL}},
#endif
        {0, REPORT_COUNT, SORT_NONE, FALSE, 0, {NULL}},
#if defined( _LUSTRE_HSM ) || defined( _SHERPA )
        {ATTR_INDEX_size, REPORT_SUM, SORT_NONE, FALSE, 0, {NULL}},
#endif
        {ATTR_INDEX_size, REPORT_MIN, SORT_NONE, FALSE, 0, {NULL}},
        {ATTR_INDEX_size, REPORT_MAX, SORT_NONE, FALSE, 0, {NULL}},
        {ATTR_INDEX_size, REPORT_AVG, SORT_NONE, FALSE, 0, {NULL}},
#if !defined(_LUSTRE_HSM) && !defined( _SHERPA )
        {ATTR_INDEX_dircount, REPORT_MIN, SORT_NONE, FALSE, 0, {NULL}},
        {ATTR_INDEX_dircount, REPORT_MAX, SORT_NONE, FALSE, 0, {NULL}},
        {ATTR_INDEX_dircount, REPORT_AVG, SORT_NONE, FALSE, 0, {NULL}}
#endif
    };

    /* append global filters */
    mk_global_filters( &filter, TRUE, &is_filter );

#ifdef _SHERPA
    /* only select file status  */
    if ( !is_filter )
        lmgr_simple_filter_init( &filter );
    fv.val_str = STR_TYPE_FILE;
    lmgr_simple_filter_add( &filter, ATTR_INDEX_type, EQUAL, fv, 0 );
    is_filter = TRUE;
#endif

    if ( is_filter )
        it = ListMgr_Report( &lmgr, fs_info, FSINFOCOUNT, &filter, NULL );
    else
        it = ListMgr_Report( &lmgr, fs_info, FSINFOCOUNT, NULL, NULL );

    if ( it == NULL )
    {
        DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR: Could not retrieve filesystem stats from database." );
        return;
    }


    result_count = FSINFOCOUNT;

    if ( csv_format )
#ifdef _TMP_FS_MGR
        printf( "%-10s, %10s, %15s, %15s, %15s\n",
                "type", "count", "min_size", "max_size", "avg_size" );
#else
        printf( "%-10s, %10s, %15s, %15s, %15s, %15s\n", "status",
                "count", "volume", "min_size", "max_size", "avg_size" );
#endif

    while ( ( rc = ListMgr_GetNextReportItem( it, result, &result_count ) )
              == DB_SUCCESS )
    {
#ifdef _TMP_FS_MGR
        if ( result[0].value_u.val_str == NULL )
            result[0].value_u.val_str = "(?)";

        if ( !strcmp( result[0].value_u.val_str, STR_TYPE_DIR ) )
        {
            if ( csv_format )
                printf( "%-10s, %10u, %15u, %15u, %15u\n",
                        result[0].value_u.val_str,
                        result[1].value_u.val_uint,
                        result[5].value_u.val_uint,
                        result[6].value_u.val_uint, result[7].value_u.val_uint );
            else
            {
                printf( "\n" );
                printf( "Type:         %15s\n", "directory" );
                printf( "Count:        %15u\n", result[1].value_u.val_uint );
                printf( "Dircount min: %15u\n", result[5].value_u.val_uint );
                printf( "Dircount max: %15u\n", result[6].value_u.val_uint );
                printf( "Dircount avg: %15u\n", result[7].value_u.val_uint );
            }
        }
        else
        {
            if ( csv_format )
                printf( "%-10s, %10u, %15llu, %15llu, %15llu\n",
                        result[0].value_u.val_str,
                        result[1].value_u.val_uint,
                        result[2].value_u.val_biguint,
                        result[3].value_u.val_biguint, result[4].value_u.val_biguint );
            else
            {
                char           strsize[128];
                printf( "\n" );
                printf( "Type:         %15s\n", result[0].value_u.val_str );
                printf( "Count:        %15u\n", result[1].value_u.val_uint );
                printf( "Size min:     %15s   (%llu bytes)\n",
                        FormatFileSize( strsize, 128, result[2].value_u.val_biguint ),
                        result[2].value_u.val_biguint );
                printf( "Size max:     %15s   (%llu bytes)\n",
                        FormatFileSize( strsize, 128, result[3].value_u.val_biguint ),
                        result[3].value_u.val_biguint );
                printf( "Size avg:     %15s   (%llu bytes)\n",
                        FormatFileSize( strsize, 128, result[4].value_u.val_biguint ),
                        result[4].value_u.val_biguint );
            }

        }
#else
        if ( csv_format )
            printf( "%10s, %10u, %15llu, %15llu, %15llu, %15llu\n",
                    db_status2str(result[0].value_u.val_uint,1),
                    result[1].value_u.val_uint,
                    result[2].value_u.val_biguint,
                    result[3].value_u.val_biguint,
                    result[4].value_u.val_biguint,
                    result[5].value_u.val_biguint );
        else
        {
            char           strsize[128];
            printf("\n");
            printf( "Status: %s\n",db_status2str(result[0].value_u.val_uint,0) );
            printf( "   Count:    %10u\n", result[1].value_u.val_uint );
            printf( "   Volume:   %10s   (%llu bytes)\n",
                    FormatFileSize( strsize, 128, result[2].value_u.val_biguint ),
                    result[2].value_u.val_biguint );
            printf( "   Size min: %10s   (%llu bytes)\n",
                    FormatFileSize( strsize, 128, result[3].value_u.val_biguint ),
                    result[3].value_u.val_biguint );
            printf( "   Size max: %10s   (%llu bytes)\n",
                    FormatFileSize( strsize, 128, result[4].value_u.val_biguint ),
                    result[4].value_u.val_biguint );
            printf( "   Size avg: %10s   (%llu bytes)\n",
                    FormatFileSize( strsize, 128, result[5].value_u.val_biguint ),
                    result[5].value_u.val_biguint );
        }
#endif

        /* prepare next call */
        result_count = FSINFOCOUNT;

    }
    if ( !csv_format)
        printf("\n");

    ListMgr_CloseReport( it );

}

void report_usergroup_info( char *name, int csv_format, int is_group )
{
    unsigned int   result_count;
    struct lmgr_report_t *it;
    lmgr_filter_t  filter;
    filter_value_t fv;
    int            rc;

    char           prevuser[256] = "";
    char           strsize[128] = "";
    int is_filter = FALSE;

#ifndef _LUSTRE_HSM
#define USERINFOCOUNT 10
#else
#define USERINFOCOUNT 6
#endif

    db_value_t     result[USERINFOCOUNT];

    /* To be retrieved for each user:
     * - username
     * - number of items of each type
     * - SUM(blocks)
     * - MIN/MAX/AVG size
     * - MIN/MAX/AVG dircount
     */
    report_field_descr_t user_info[USERINFOCOUNT] = {
        {ATTR_INDEX_owner, REPORT_GROUP_BY, SORT_ASC, FALSE, 0, {NULL}},
#ifndef _LUSTRE_HSM
        {ATTR_INDEX_type, REPORT_GROUP_BY, SORT_ASC, FALSE, 0, {NULL}},
#endif
        {0, REPORT_COUNT, SORT_NONE, FALSE, 0, {NULL}},
        {ATTR_INDEX_blocks, REPORT_SUM, SORT_NONE, FALSE, 0, {NULL}},
        {ATTR_INDEX_size, REPORT_MIN, SORT_NONE, FALSE, 0, {NULL}},
        {ATTR_INDEX_size, REPORT_MAX, SORT_NONE, FALSE, 0, {NULL}},
        {ATTR_INDEX_size, REPORT_AVG, SORT_NONE, FALSE, 0, {NULL}},
#ifndef _LUSTRE_HSM
        {ATTR_INDEX_dircount, REPORT_MIN, SORT_NONE, FALSE, 0, {NULL}},
        {ATTR_INDEX_dircount, REPORT_MAX, SORT_NONE, FALSE, 0, {NULL}},
        {ATTR_INDEX_dircount, REPORT_AVG, SORT_NONE, FALSE, 0, {NULL}}
#endif
    };

    /* modify user_info tab if function is for group stats */
    if ( is_group )
        user_info[0].attr_index = ATTR_INDEX_gr_name;

    if ( name )
    {
        lmgr_simple_filter_init( &filter );
        is_filter = TRUE;

        fv.val_str = name;

        if ( WILDCARDS_IN( name ) )
            lmgr_simple_filter_add( &filter, (is_group?ATTR_INDEX_gr_name:ATTR_INDEX_owner), LIKE, fv, 0 ); 
        else
            lmgr_simple_filter_add( &filter, (is_group?ATTR_INDEX_gr_name:ATTR_INDEX_owner), EQUAL, fv, 0 ); 
    }

    /* append global filters */
    mk_global_filters( &filter, TRUE, &is_filter );

    it = ListMgr_Report( &lmgr, user_info, USERINFOCOUNT, ( is_filter ? &filter : NULL ), NULL );

    if ( is_filter )
        lmgr_simple_filter_free( &filter );

    if ( it == NULL )
    {
        DisplayLog( LVL_CRIT, REPORT_TAG, "ERROR: Could not retrieve user stats from database." );
        return;
    }


    result_count = USERINFOCOUNT;

    if ( csv_format )
#ifdef ATTR_INDEX_type
        printf( "%-10s, %10s, %10s, %15s, %15s, %15s, %15s\n",
                ( is_group ? "group" : "user" ), "type", "count", "spc_used", "min_size",
                "max_size", "avg_size" );
#else
        printf( "%-10s, %10s, %15s, %15s, %15s, %15s\n",
                ( is_group ? "group" : "user" ), "count", "spc_used", "min_size", "max_size",
                "avg_size" );
#endif

    while ( ( rc = ListMgr_GetNextReportItem( it, result, &result_count ) ) == DB_SUCCESS )
    {
        /* unknown user */
        if ( result[0].value_u.val_str == NULL )
            result[0].value_u.val_str = "(?)";

#ifndef _LUSTRE_HSM
        if ( result[1].value_u.val_str == NULL )
            result[1].value_u.val_str = "(unknown)";

        if ( !strcmp( result[1].value_u.val_str, STR_TYPE_DIR ) )
        {
            if ( csv_format )
                printf( "%-10s, %10s, %10u, %15llu, %15u, %15u, %15u\n",
                        result[0].value_u.val_str,
                        result[1].value_u.val_str,
                        result[2].value_u.val_uint,
                        result[3].value_u.val_biguint * DEV_BSIZE,
                        result[7].value_u.val_uint,
                        result[8].value_u.val_uint, result[9].value_u.val_uint );
            else
            {
                if ( strcmp( prevuser, result[0].value_u.val_str ) )
                {
                    if ( !is_group )
                        printf( "\nUser:         %15s\n", result[0].value_u.val_str );
                    else
                        printf( "\nGroup:        %15s\n", result[0].value_u.val_str );

                    strncpy( prevuser, result[0].value_u.val_str, 256 );
                }

                FormatFileSize( strsize, 128, result[3].value_u.val_biguint * DEV_BSIZE );

                printf( "\n" );
                printf( "    Type:         %15s\n", "directory" );
                printf( "    Count:        %15u\n", result[2].value_u.val_uint );
                printf( "    Space used:   %15s    (%llu blks)\n", strsize,
                        result[3].value_u.val_biguint );
                printf( "    Dircount min: %15u\n", result[7].value_u.val_uint );
                printf( "    Dircount max: %15u\n", result[8].value_u.val_uint );
                printf( "    Dircount avg: %15u\n", result[9].value_u.val_uint );
            }
        }
        else
        {
            if ( csv_format )
                printf( "%-10s, %10s, %10u, %15llu, %15llu, %15llu, %15llu\n",
                        result[0].value_u.val_str,
                        result[1].value_u.val_str,
                        result[2].value_u.val_uint,
                        result[3].value_u.val_biguint * DEV_BSIZE,
                        result[4].value_u.val_biguint,
                        result[5].value_u.val_biguint, result[6].value_u.val_biguint );
            else
            {
                if ( strcmp( prevuser, result[0].value_u.val_str ) )
                {
                    if ( !is_group )
                        printf( "\nUser:         %15s\n", result[0].value_u.val_str );
                    else
                        printf( "\nGroup:        %15s\n", result[0].value_u.val_str );

                    strncpy( prevuser, result[0].value_u.val_str, 256 );
                }

                FormatFileSize( strsize, 128, result[3].value_u.val_biguint * DEV_BSIZE );

                printf( "\n" );
                printf( "    Type:         %15s\n", result[1].value_u.val_str );
                printf( "    Count:        %15u\n", result[2].value_u.val_uint );
                printf( "    Space used:   %15s    (%llu blks)\n",
                        FormatFileSize( strsize, 128,
                                        result[3].value_u.val_biguint * DEV_BSIZE ),
                        result[3].value_u.val_biguint );
                printf( "    Size min:     %15s    (%llu bytes)\n",
                        FormatFileSize( strsize, 128, result[4].value_u.val_biguint ),
                        result[4].value_u.val_biguint );
                printf( "    Size max:     %15s    (%llu bytes)\n",
                        FormatFileSize( strsize, 128, result[5].value_u.val_biguint ),
                        result[5].value_u.val_biguint );
                printf( "    Size avg:     %15s    (%llu bytes)\n",
                        FormatFileSize( strsize, 128, result[6].value_u.val_biguint ),
                        result[6].value_u.val_biguint );
            }

        }

#else /* Lustre HSM */
        if ( csv_format )
            printf( "%-10s, %10u, %15llu, %15llu, %15llu, %15llu\n",
                    result[0].value_u.val_str,
                    result[1].value_u.val_uint,
                    result[2].value_u.val_biguint * DEV_BSIZE,
                    result[3].value_u.val_biguint,
                    result[4].value_u.val_biguint, result[5].value_u.val_biguint );
        else
        {
            if ( strcmp( prevuser, result[0].value_u.val_str ) )
            {
                if ( !is_group )
                    printf( "\nUser:         %15s\n", result[0].value_u.val_str );
                else
                    printf( "\nGroup:        %15s\n", result[0].value_u.val_str );

                strncpy( prevuser, result[0].value_u.val_str, 256 );
            }

            FormatFileSize( strsize, 128, result[3].value_u.val_biguint * DEV_BSIZE );

            printf( "\n" );
            printf( "    Count:        %15u\n", result[1].value_u.val_uint );
            printf( "    Space used:   %15s    (%llu blks)\n",
                    FormatFileSize( strsize, 128,
                                    result[2].value_u.val_biguint * DEV_BSIZE ),
                    result[2].value_u.val_biguint );
            printf( "    Size min:     %15s    (%llu bytes)\n",
                    FormatFileSize( strsize, 128, result[3].value_u.val_biguint ),
                    result[3].value_u.val_biguint );
            printf( "    Size max:     %15s    (%llu bytes)\n",
                    FormatFileSize( strsize, 128, result[4].value_u.val_biguint ),
                    result[4].value_u.val_biguint );
            printf( "    Size avg:     %15s    (%llu bytes)\n",
                    FormatFileSize( strsize, 128, result[5].value_u.val_biguint ),
                    result[5].value_u.val_biguint );
        }

#endif
        /* prepare next call */
        result_count = USERINFOCOUNT;

    }

    ListMgr_CloseReport( it );


}

#ifndef _LUSTRE_HSM             /* dirs are not considered for LUSTRE_HSM */
void report_topdirs( unsigned int count, int csv_format )
{
    /* To be retrieved for dirs:
     * fullpath, owner, dircount, last_mod
     * => sorted by dircount DESC
     */
    int            mask_sav, rc, index;
    lmgr_sort_type_t sorttype;
    lmgr_filter_t  filter;
    filter_value_t fv;
    lmgr_iter_opt_t opt;
    struct lmgr_iterator_t *it;
    attr_set_t     attrs;
    entry_id_t     id;
    char           date[128];
    struct tm      t;

    /* select only directories */
    fv.val_str = STR_TYPE_DIR;
    lmgr_simple_filter_init( &filter );
    lmgr_simple_filter_add( &filter, ATTR_INDEX_type, EQUAL, fv, 0 );

    /* append global filters */
    mk_global_filters( &filter, TRUE, NULL );

    /* order by dircount desc */
    sorttype.attr_index = ATTR_INDEX_dircount;
    sorttype.order = SORT_DESC;

    /* select only the top dirs */
    opt.list_count_max = count;

    ATTR_MASK_INIT( &attrs );
    ATTR_MASK_SET( &attrs, fullpath );
    ATTR_MASK_SET( &attrs, owner );
    ATTR_MASK_SET( &attrs, gr_name );
    ATTR_MASK_SET( &attrs, dircount );
    ATTR_MASK_SET( &attrs, last_mod );

    mask_sav = attrs.attr_mask;

    it = ListMgr_Iterator( &lmgr, &filter, &sorttype, &opt );

    lmgr_simple_filter_free( &filter );

    if ( it == NULL )
    {
        DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR: Could not retrieve top directories from database." );
        return;
    }

    if ( csv_format )
        printf( "%3s, %-40s, %6s, %10s, %10s, %s\n", "rank", "path", "dircount", "owner", "group",
                "last_mod" );

    index = 0;
    while ( ( rc = ListMgr_GetNext( it, &id, &attrs ) ) == DB_SUCCESS )
    {
        time_t         mod = ATTR( &attrs, last_mod );
        index++;
        /* format last mod */
        strftime( date, 128, "%Y/%m/%d %T", localtime_r( &mod, &t ) );

        if ( csv_format )
            printf( "%3u, %-40s, %6u, %10s, %10s, %s\n", index, ATTR( &attrs, fullpath ),
                    ATTR( &attrs, dircount ), ATTR( &attrs, owner ), ATTR( &attrs, gr_name ),
                    date );
        else
        {
            printf( "\n" );
            printf( "Rank:              %u\n", index );
            printf( "Path:              %s\n", ATTR( &attrs, fullpath ) );
            printf( "Dircount:          %u\n", ATTR( &attrs, dircount ) );
            printf( "Last modification: %s\n", date );
            printf( "Owner/Group:       %s/%s\n", ATTR( &attrs, owner ), ATTR( &attrs, gr_name ) );
        }

        ListMgr_FreeAttrs( &attrs );

        /* prepare next call */
        attrs.attr_mask = mask_sav;
    }

    ListMgr_CloseIterator( it );

}
#endif

void report_topsize( unsigned int count, int csv_format )
{
    /* To be retrieved for files
     * fullpath, owner, size, stripe_info, last_access, last_mod
     * => sorted by size DESC
     */
    int            mask_sav, rc, index;
    lmgr_sort_type_t sorttype;
    lmgr_filter_t  filter;
    filter_value_t fv;
    lmgr_iter_opt_t opt;
    struct lmgr_iterator_t *it;
    attr_set_t     attrs;
    entry_id_t     id;
    char           acc[128];
    char           mod[128];
    char           sz[128];
    char           sl[1024];
    struct tm      t;

    /* select only files */
    fv.val_str = STR_TYPE_FILE;
    lmgr_simple_filter_init( &filter );
#ifndef _LUSTRE_HSM
    lmgr_simple_filter_add( &filter, ATTR_INDEX_type, EQUAL, fv, 0 );
#endif

    /* append global filters */
    mk_global_filters( &filter, TRUE, NULL );

    /* order by size desc */
    sorttype.attr_index = ATTR_INDEX_size;
    sorttype.order = SORT_DESC;

    /* select only the top size */
    opt.list_count_max = count;

    ATTR_MASK_INIT( &attrs );
    ATTR_MASK_SET( &attrs, fullpath );
    ATTR_MASK_SET( &attrs, owner );
    ATTR_MASK_SET( &attrs, gr_name );
    ATTR_MASK_SET( &attrs, size );
    ATTR_MASK_SET( &attrs, stripe_info );
    ATTR_MASK_SET( &attrs, stripe_items );
    ATTR_MASK_SET( &attrs, last_access );
    ATTR_MASK_SET( &attrs, last_mod );

#ifdef ATTR_INDEX_status
    ATTR_MASK_SET( &attrs, status );
#endif

#ifdef ATTR_INDEX_archive_class
    ATTR_MASK_SET( &attrs, archive_class );
#endif
    ATTR_MASK_SET( &attrs, release_class );

    mask_sav = attrs.attr_mask;

    it = ListMgr_Iterator( &lmgr, &filter, &sorttype, &opt );

    lmgr_simple_filter_free( &filter );

    if ( it == NULL )
    {
        DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR: Could not retrieve top file size from database." );
        return;
    }

    if ( csv_format )
        printf( "%3s, %-40s, "
#ifdef ATTR_INDEX_status
                "%15s,"
#endif
                "%15s, %10s, %10s, %20s, %20s, "
#ifdef ATTR_INDEX_archive_class
                "%15s, "
#endif
                "%15s, %5s, %7s, %8s, %s\n", "rank", "path",
#ifdef ATTR_INDEX_status
                "status",
#endif
                "size", "owner", "group", "last_access", "last_mod",
#ifdef ATTR_INDEX_archive_class
                "migr_class",
#endif
                "purge_class",
                "stripe_count", "stripe_size", "pool", "storage_units" );

    index = 0;
    while ( ( rc = ListMgr_GetNext( it, &id, &attrs ) ) == DB_SUCCESS )
    {
        time_t         access = ATTR( &attrs, last_access );
        time_t         modif = ATTR( &attrs, last_mod );

        index++;
        /* format last mod and last acess */
        strftime( acc, 128, "%Y/%m/%d %T", localtime_r( &access, &t ) );
        strftime( mod, 128, "%Y/%m/%d %T", localtime_r( &modif, &t ) );

        if ( csv_format )
            printf( "%3u, %-40s, "
#ifdef ATTR_INDEX_status
                    "%15s ,"
#endif
                    "%15" PRINT_SIZE_T ", %10s, %10s, %20s, %20s, "
#ifdef ATTR_INDEX_archive_class
                    "%15s, "
#endif
                    "%15s, %5u, %7"PRINT_SIZE_T ", %8s, %s\n",
                    index, ATTR( &attrs, fullpath ),
#ifdef ATTR_INDEX_status
                    db_status2str( ATTR( &attrs, status), TRUE ),
#endif
                    (unsigned long long)ATTR( &attrs, size ),
                    ATTR( &attrs, owner ), ATTR( &attrs, gr_name ), acc, mod,
#ifdef ATTR_INDEX_archive_class
                    migr_class(&attrs),
#endif
                    release_class(&attrs),
                    ATTR( &attrs, stripe_info ).stripe_count,
                    (unsigned long long)ATTR( &attrs, stripe_info ).stripe_size,
                    ATTR( &attrs, stripe_info ).pool_name,
                    FormatStripeList( sl, 1024, &ATTR( &attrs, stripe_items ) ) );
        else
        {
            FormatFileSize( sz, 128, ATTR( &attrs, size ) );

            printf( "\n" );
            printf( "Rank:              %u\n", index );

            if ( ATTR_MASK_TEST( &attrs, fullpath ) )
                printf( "Path:              %s\n", ATTR( &attrs, fullpath ) );

#ifdef ATTR_INDEX_status
            printf( "Status:            %s\n", db_status2str( ATTR( &attrs, status), FALSE ) );
#endif

            printf( "Size:              %s   (%" PRINT_SIZE_T " bytes)\n", sz,
                    (unsigned long long)ATTR( &attrs, size ) );
            printf( "Last access:       %s\n", acc );
            printf( "Last modification: %s\n", mod );
            printf( "Owner/Group:       %s/%s\n", ATTR( &attrs, owner ), ATTR( &attrs, gr_name ) );

#ifdef ATTR_INDEX_archive_class
            if ( ATTR_MASK_TEST( &attrs, archive_class ) )
                printf( "Migration class:   %s\n", migr_class(&attrs) );
#endif
            if ( ATTR_MASK_TEST( &attrs, release_class ) )
                printf( "Purge class:       %s\n", release_class(&attrs) );

            if ( ATTR_MASK_TEST( &attrs, stripe_info )
                 && ( ATTR( &attrs, stripe_info ).stripe_count > 0 ) )
            {
                printf( "Stripe count:      %u\n", ATTR( &attrs, stripe_info ).stripe_count );
                printf( "Stripe size:       %s   (%" PRINT_SIZE_T " bytes)\n",
                        FormatFileSize( sz, 128, ATTR( &attrs, stripe_info ).stripe_size ),
                        (unsigned long long)ATTR( &attrs, stripe_info ).stripe_size );
                if ( !EMPTY_STRING( ATTR( &attrs, stripe_info ).pool_name ) )
                    printf( "Pool:              %s\n", ATTR( &attrs, stripe_info ).pool_name );
                printf( "Storage units:     %s\n",
                        FormatStripeList( sl, 1024, &ATTR( &attrs, stripe_items ) ) );
            }

        }

        ListMgr_FreeAttrs( &attrs );
        /* prepare next call */
        attrs.attr_mask = mask_sav;
    }

    ListMgr_CloseIterator( it );

}


void report_toppurge( unsigned int count, int csv_format )
{
    /* To be retrieved: non whitelisted, non directories, non invalid
     * fullpath, type, last_access, last_mod, size, stripe_info
     * => sorted by last_access ASC
     */
    int            mask_sav, rc, index;
    lmgr_sort_type_t sorttype;
    lmgr_filter_t  filter;
    filter_value_t fv;
    lmgr_iter_opt_t opt;
    struct lmgr_iterator_t *it;
    attr_set_t     attrs;
    entry_id_t     id;
    char           acc[128];
    char           mod[128];
    char           sz[128];
    char           sl[1024];
    struct tm      t;

    lmgr_simple_filter_init( &filter );

#ifndef _LUSTRE_HSM
    /* select only non directories */
    fv.val_str = STR_TYPE_DIR;
    lmgr_simple_filter_add( &filter, ATTR_INDEX_type, NOTEQUAL, fv, 0 );
#endif

    /* append global filters */
    mk_global_filters( &filter, TRUE, NULL );

    /* select only non whitelisted */
#ifndef _LUSTRE_HSM
    fv.val_bool = TRUE;
    lmgr_simple_filter_add( &filter, ATTR_INDEX_whitelisted, NOTEQUAL, fv, 0);
#else
    fv.val_bool = TRUE;
    lmgr_simple_filter_add( &filter, ATTR_INDEX_no_release, NOTEQUAL, fv, 0);
#endif

#ifdef ATTR_INDEX_status
    fv.val_int = STATUS_UP_TO_DATE;
    lmgr_simple_filter_add( &filter, ATTR_INDEX_status, EQUAL, fv, 0);
#endif

#ifdef ATTR_INDEX_invalid
    /* select only non invalid */
    fv.val_bool = TRUE;
    lmgr_simple_filter_add( &filter, ATTR_INDEX_invalid, NOTEQUAL, fv, 0);
#endif

    sorttype.attr_index = ATTR_INDEX_last_access;
    sorttype.order = SORT_ASC;

    /* select only the top size */
    opt.list_count_max = count;

    ATTR_MASK_INIT( &attrs );
    ATTR_MASK_SET( &attrs, fullpath );
    ATTR_MASK_SET( &attrs, type );
    ATTR_MASK_SET( &attrs, last_access );
    ATTR_MASK_SET( &attrs, last_mod );
    ATTR_MASK_SET( &attrs, size );
    ATTR_MASK_SET( &attrs, blocks );
    ATTR_MASK_SET( &attrs, stripe_info );
    ATTR_MASK_SET( &attrs, stripe_items );

    mask_sav = attrs.attr_mask;

    it = ListMgr_Iterator( &lmgr, &filter, &sorttype, &opt );

    lmgr_simple_filter_free( &filter );

    if ( it == NULL )
    {
        DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR: Could not retrieve top purge list from database." );
        return;
    }

    if ( csv_format )
        printf( "%3s, %-40s, %8s, %20s, %20s, %15s, %10s, %5s, %7s, %8s, %s\n", "rank",
                "path", "type", "last_access", "last_mod", "size", "blks",
                "stripe_count", "stripe_size", "pool", "storage_units" );

    index = 0;
    while ( ( rc = ListMgr_GetNext( it, &id, &attrs ) ) == DB_SUCCESS )
    {
        time_t         access = ATTR( &attrs, last_access );
        time_t         modif = ATTR( &attrs, last_mod );

        index++;
        /* format last mod and last acess */
        strftime( acc, 128, "%Y/%m/%d %T", localtime_r( &access, &t ) );
        strftime( mod, 128, "%Y/%m/%d %T", localtime_r( &modif, &t ) );

        if ( csv_format )
            printf( "%3u, %-40s, %8s, %20s, %20s, %15" PRINT_SIZE_T ", %10llu, %5u, %7"
                    PRINT_SIZE_T ", %8s, %s\n", index, ATTR( &attrs, fullpath ),
                    ATTR( &attrs, type ), acc, mod,
                    (unsigned long long)ATTR( &attrs, size ),
                    (unsigned long long)ATTR( &attrs, blocks ),
                    ATTR( &attrs, stripe_info ).stripe_count,
                    (unsigned long long)ATTR( &attrs, stripe_info ).stripe_size,
                    ATTR( &attrs, stripe_info ).pool_name,
                    FormatStripeList( sl, 1024, &ATTR( &attrs, stripe_items ) ) );
        else
        {
            printf( "\n" );
            printf( "Rank:              %u\n", index );

            if ( ATTR_MASK_TEST( &attrs, fullpath ) )
                printf( "Path:              %s\n", ATTR( &attrs, fullpath ) );

            printf( "Type:              %s\n", ATTR( &attrs, type ) );
            printf( "Last access:       %s\n", acc );
            printf( "Last modification: %s\n", mod );
            printf( "Size:              %s   (%" PRINT_SIZE_T " bytes)\n",
                    FormatFileSize( sz, 128, ATTR( &attrs, size ) ),
                    (unsigned long long)ATTR( &attrs, size ) );
            printf( "Space used:        %s   (%llu blocks)\n",
                    FormatFileSize( sz, 128, ATTR( &attrs, blocks ) * DEV_BSIZE ),
                    (unsigned long long)ATTR( &attrs, blocks ) );

            if ( ATTR_MASK_TEST( &attrs, stripe_info )
                 && ( ATTR( &attrs, stripe_info ).stripe_count > 0 ) )
            {
                printf( "Stripe count:      %u\n", ATTR( &attrs, stripe_info ).stripe_count );
                printf( "Stripe size:       %s   (%" PRINT_SIZE_T " bytes)\n",
                        FormatFileSize( sz, 128, ATTR( &attrs, stripe_info ).stripe_size ),
                        (unsigned long long)ATTR( &attrs, stripe_info ).stripe_size );
                if ( !EMPTY_STRING( ATTR( &attrs, stripe_info ).pool_name ) )
                    printf( "Pool:              %s\n", ATTR( &attrs, stripe_info ).pool_name );
                printf( "Storage units:     %s\n",
                        FormatStripeList( sl, 1024, &ATTR( &attrs, stripe_items ) ) );
            }

        }

        ListMgr_FreeAttrs( &attrs );

        /* prepare next call */
        attrs.attr_mask = mask_sav;
    }

    ListMgr_CloseIterator( it );

}

#ifndef _LUSTRE_HSM             /* dirs are not considered for LUSTRE_HSM */
void report_toprmdir( unsigned int count, int csv_format )
{
    /* To be retrieved for dirs:
     * fullpath, owner, last_mod
     * filter: type=dir, not invalid, not whitelisted, empty
     * => sorted by last_mod ASC
     */
    int            mask_sav, rc, index;
    lmgr_sort_type_t sorttype;
    lmgr_filter_t  filter;
    filter_value_t fv;
    lmgr_iter_opt_t opt;
    struct lmgr_iterator_t *it;
    attr_set_t     attrs;
    entry_id_t     id;
    char           date[128];
#
    char           dur[128];
    struct tm      t;

    lmgr_simple_filter_init( &filter );

    /* select only directories */
    fv.val_str = STR_TYPE_DIR;
    lmgr_simple_filter_add( &filter, ATTR_INDEX_type, EQUAL, fv, 0 );

    /* select only non whitelisted */
    fv.val_bool = TRUE;
    lmgr_simple_filter_add( &filter, ATTR_INDEX_whitelisted, NOTEQUAL, fv, 0);

    /* select only non invalid */
    fv.val_bool = TRUE;
    lmgr_simple_filter_add( &filter, ATTR_INDEX_invalid, NOTEQUAL, fv, 0);

    /* only consider empty directories */
    fv.val_uint = 0;
    rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_dircount, EQUAL, fv, 0);

    mk_global_filters( &filter, TRUE, NULL );

    /* order by last_mod asc */
    sorttype.attr_index = ATTR_INDEX_last_mod;
    sorttype.order = SORT_ASC;

    /* select only the top dirs */
    opt.list_count_max = count;

    ATTR_MASK_INIT( &attrs );
    ATTR_MASK_SET( &attrs, fullpath );
    ATTR_MASK_SET( &attrs, owner );
    ATTR_MASK_SET( &attrs, gr_name );
    ATTR_MASK_SET( &attrs, last_mod );

    mask_sav = attrs.attr_mask;

    it = ListMgr_Iterator( &lmgr, &filter, &sorttype, &opt );

    lmgr_simple_filter_free( &filter );

    if ( it == NULL )
    {
        DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR: Could not retrieve top directories from database." );
        return;
    }

    if ( csv_format )
        printf( "%3s, %-40s, %10s, %10s, %20s, %s\n", "rank", "path", "owner", "group",
                "last_mod", "deadline" );

    index = 0;
    while ( ( rc = ListMgr_GetNext( it, &id, &attrs ) ) == DB_SUCCESS )
    {
        time_t         mod = ATTR( &attrs, last_mod );

        index++;
        /* format last mod */
        strftime( date, 128, "%Y/%m/%d %T", localtime_r( &mod, &t ) );

        if ( policies.rmdir_policy.age_rm_empty_dirs == 0 )
            strcpy( dur, "disabled" );
        else if ( ATTR( &attrs, last_mod ) <
                  time( NULL ) - policies.rmdir_policy.age_rm_empty_dirs )
            strcpy( dur, "expired" );
        else if ( csv_format )
            sprintf( dur, "%u",
                     ( unsigned int ) ( ATTR( &attrs, last_mod ) - time( NULL ) +
                                        policies.rmdir_policy.age_rm_empty_dirs ) );
        else
            FormatDurationFloat( dur, 128,
                                 ATTR( &attrs,
                                       last_mod ) - time( NULL ) +
                                 policies.rmdir_policy.age_rm_empty_dirs );


        if ( csv_format )
            printf( "%3u, %-40s, %10s, %10s, %20s, %s\n", index, ATTR( &attrs, fullpath ),
                    ATTR( &attrs, owner ), ATTR( &attrs, gr_name ), date, dur );
        else
        {
            printf( "\n" );
            printf( "Rank:              %u\n", index );
            printf( "Path:              %s\n", ATTR( &attrs, fullpath ) );
            printf( "Rmdir deadline:    %s\n", dur );
            printf( "Last modification: %s\n", date );
            printf( "Owner/Group:       %s/%s\n", ATTR( &attrs, owner ), ATTR( &attrs, gr_name ) );
        }

        ListMgr_FreeAttrs( &attrs );

        /* prepare next call */
        attrs.attr_mask = mask_sav;
    }

    ListMgr_CloseIterator( it );

}
#endif

void report_topuser( unsigned int count, int csv_format )
{
    unsigned int   result_count;
    struct lmgr_report_t *it;
    lmgr_iter_opt_t opt;
    int            rc;
    char           strsize[128] = "";
    unsigned int   rank = 1;
    lmgr_filter_t  filter;
    int is_filter = FALSE;

#define TOPUSERCOUNT 6

    db_value_t     result[TOPUSERCOUNT];

    /* To be retrieved for each user:
     * - username
     * - SUM(blocks)
     * - NB entries
     * - MIN/MAX/AVG size
     */
    report_field_descr_t user_info[TOPUSERCOUNT] = {
        {ATTR_INDEX_owner, REPORT_GROUP_BY, SORT_NONE, FALSE, 0, {NULL}},
        {ATTR_INDEX_blocks, REPORT_SUM, SORT_DESC, FALSE, 0, {NULL}},
        {0, REPORT_COUNT, SORT_NONE, FALSE, 0, {NULL}},
        {ATTR_INDEX_size, REPORT_MIN, SORT_NONE, FALSE, 0, {NULL}},
        {ATTR_INDEX_size, REPORT_MAX, SORT_NONE, FALSE, 0, {NULL}},
        {ATTR_INDEX_size, REPORT_AVG, SORT_NONE, FALSE, 0, {NULL}},
    };

    /* select only the top users */
    opt.list_count_max = count;

    is_filter = FALSE;

    mk_global_filters( &filter, TRUE, &is_filter );

    /* is a filter specified? */
    if ( is_filter )
        it = ListMgr_Report( &lmgr, user_info, TOPUSERCOUNT, &filter, &opt );
    else
        it = ListMgr_Report( &lmgr, user_info, TOPUSERCOUNT, NULL, &opt );


    if ( it == NULL )
    {
        DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR: Could not retrieve top space consumers from database." );
        return;
    }

    result_count = TOPUSERCOUNT;

    if ( csv_format )
        printf( "%3s, %-10s, %15s, %10s, %15s, %15s, %15s\n", "rank",
                "user", "spc_used", "nb_entries", "min_size", "max_size", "avg_size" );

    while ( ( rc = ListMgr_GetNextReportItem( it, result, &result_count ) ) == DB_SUCCESS )
    {
        /* unknown user */
        if ( result[0].value_u.val_str == NULL )
            result[0].value_u.val_str = "(?)";

        if ( csv_format )
            printf( "%3u, %-10s, %15llu, %10u, %15llu, %15llu, %15llu\n",
                    rank,
                    result[0].value_u.val_str,
                    result[1].value_u.val_biguint * DEV_BSIZE,
                    result[2].value_u.val_uint,
                    result[3].value_u.val_biguint,
                    result[4].value_u.val_biguint, result[5].value_u.val_biguint );
        else
        {
            printf( "\n" );
            printf( "Rank:         %15u\n", rank );
            printf( "User:         %15s\n", result[0].value_u.val_str );
            printf( "Space used:   %15s    (%llu blks)\n",
                    FormatFileSize( strsize, 128,
                                    result[1].value_u.val_biguint * DEV_BSIZE ),
                    result[1].value_u.val_biguint );

            printf( "Nb entries:   %15u\n", result[2].value_u.val_uint );
            printf( "Size min:     %15s    (%llu bytes)\n",
                    FormatFileSize( strsize, 128, result[3].value_u.val_biguint ),
                    result[3].value_u.val_biguint );
            printf( "Size max:     %15s    (%llu bytes)\n",
                    FormatFileSize( strsize, 128, result[4].value_u.val_biguint ),
                    result[4].value_u.val_biguint );
            printf( "Size avg:     %15s    (%llu bytes)\n",
                    FormatFileSize( strsize, 128, result[5].value_u.val_biguint ),
                    result[5].value_u.val_biguint );
        }

        rank++;

        /* prepare next call */
        result_count = TOPUSERCOUNT;

    }

    ListMgr_CloseReport( it );


}

#ifdef _LUSTRE_HSM
void report_deferred_rm( int csv_format )
{
    int            rc, index;
    struct lmgr_rm_list_t * list;
    entry_id_t     id;
    char   last_known_path[MAXPATHLEN] = "";
    time_t soft_rm_time = 0;
    time_t expiration_time = 0;
    char           date_rm[128];
    char           date_exp[128];
    struct tm      t;

    /* list all deferred rm, even if non expired */
    list = ListMgr_RmList( &lmgr, FALSE );

    if ( list == NULL )
    {
        DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR: Could not retrieve removed entries from database." );
        return;
    }

    if ( csv_format )
        printf( "%3s, %21s, %-40s, %19s, %19s\n", "rank", "fid", "last_known_path", "lustre_rm", "hsm_rm" );

    index = 0;
    while ( ( rc = ListMgr_GetNextRmEntry( list, &id, last_known_path,
                        &soft_rm_time, &expiration_time )) == DB_SUCCESS )
    {
        index++;
        /* format last mod */
        strftime( date_rm, 128, "%Y/%m/%d %T", localtime_r( &soft_rm_time, &t ) );
        strftime( date_exp, 128, "%Y/%m/%d %T", localtime_r( &expiration_time, &t ) );

        if ( csv_format )
            printf( "%3u, "DFID", %-40s, %19s, %19s\n", index, PFID(&id),
                    last_known_path, date_rm, date_exp );
        else
        {
            printf( "\n" );
            printf( "Rank:              %u\n", index );
            printf( "Fid:               "DFID"\n", PFID(&id) );
            if ( !EMPTY_STRING(last_known_path) )
                printf( "Last known path:   %s\n", last_known_path );
            printf( "Lustre rm time:    %s\n", date_rm );
            if ( expiration_time <= time(NULL) )
                printf( "HSM rm time:       %s (expired)\n", date_exp );
            else
                printf( "HSM rm time:       %s\n", date_exp );
        }

        /* prepare next call */
        last_known_path[0] = '\0';
        soft_rm_time = 0;
        expiration_time = 0;
    }

    ListMgr_CloseRmList(list);
}
#endif


#ifdef ATTR_INDEX_archive_class

#ifdef _LUSTRE_HSM
    #define IS_PURGE_CONCERNED( _status ) ( ((_status) == STATUS_UP_TO_DATE)  \
                                     || ((_status) == STATUS_RELEASED) \
                                     || ((_status) == STATUS_RELEASE_PENDING) )
    #define IS_MODIFIED( _status )  ( ((_status) == STATUS_MODIFIED) \
                                      || ((_status) == STATUS_NO_FLAGS))
#elif defined(_SHERPA)
    #define IS_PURGE_CONCERNED( _status ) ( (_status) == STATUS_UP_TO_DATE )
    #define IS_MODIFIED( _status )  ((_status) == STATUS_MODIFIED)
#endif


/* used for grouping archive/release classes depending on entry status */

struct class_record {
    char class[1024];
    unsigned int count;
    unsigned int nb_modif;
    unsigned int nb_synchro;
    unsigned long long spc_used;
    unsigned long long spc_modif;
    unsigned long long spc_synchro;
    unsigned long long size_min;
    unsigned long long size_max;
    struct class_record * p_next;
} * rec_list = NULL;

static int classname_cmp( const char *s1, const char *s2 )
{
   if ( (s1[0] == '[') && (s2[0] != '[') )
        /* s2 must be before => s1 is bigger */
        return 1;
   else if ( (s1[0] != '[') && (s2[0] == '[') )
        /* s1 must be before => s1 is smaller */
        return -1;
   else
        return strcmp( s1, s2 );
}

static inline int class_add( const char * name, db_value_t * res_array )
{
   struct class_record * p_curr;
   int found = FALSE;
   for ( p_curr = rec_list; p_curr != NULL; p_curr = p_curr->p_next )
   {
       if ( !strcmp( p_curr->class, name ) )
       {
           /* found it ! */
           found = TRUE;
           break;
       }
   }

   if ( !found )
   {
       p_curr = (struct class_record *)malloc( sizeof(struct class_record) );
       strcpy( p_curr->class, name );
       p_curr->count = p_curr->nb_modif = p_curr->nb_synchro = 0;
       p_curr->spc_used = p_curr->spc_modif
            = p_curr->spc_synchro = p_curr->size_max = 0;
       /* initialize min to current value */
       p_curr->size_min = res_array[5].value_u.val_biguint;

       /* add sorted */
       if ( rec_list == NULL )
       {
            p_curr->p_next = NULL;
            rec_list = p_curr;
       }
       else
       {
           struct class_record * ptr;
           struct class_record * prev;
           prev = NULL;
           for ( ptr = rec_list; ptr != NULL; prev=ptr, ptr=ptr->p_next )
           {
                /* name is before class? */
                if ( classname_cmp( name, ptr->class) <= 0 )
                {
                    /* HEAD insert */
                    if ( prev==NULL )
                    {
                        p_curr->p_next = rec_list;
                        rec_list = p_curr;
                    }
                    else /* in place insert */
                    {
                        p_curr->p_next = ptr;
                        prev->p_next = p_curr;
                    }
                    break; /* OK inserted */
                }
                else if ( ptr->p_next == NULL )
                {
                    /* insert in last position */
                    p_curr->p_next = NULL;
                    ptr->p_next = p_curr;
                    break;
                }
           }
       }
   }

   /* increment stats */
   p_curr->count += res_array[3].value_u.val_uint;
   p_curr->spc_used += (res_array[4].value_u.val_biguint * DEV_BSIZE);

   if IS_MODIFIED(res_array[2].value_u.val_uint)
   {
        p_curr->nb_modif += res_array[3].value_u.val_uint;
        p_curr->spc_modif += (res_array[4].value_u.val_biguint * DEV_BSIZE);
   }
   else if ( res_array[2].value_u.val_uint == STATUS_UP_TO_DATE )
   {
        p_curr->nb_synchro += res_array[3].value_u.val_uint;
        p_curr->spc_synchro += (res_array[4].value_u.val_biguint * DEV_BSIZE);
   }

   /* size min */
   if ( res_array[5].value_u.val_biguint < p_curr->size_min )
      p_curr->size_min = res_array[5].value_u.val_biguint;
   if ( res_array[6].value_u.val_biguint > p_curr->size_max )
      p_curr->size_max = res_array[6].value_u.val_biguint;

   return 0;
} /* end of helper defintion */

#endif

static void report_class_info( int csv )
{
#ifdef ATTR_INDEX_archive_class
    #define CLASSINFO_FIELDS 8
    struct class_record * p_curr;
#else
    #define CLASSINFO_FIELDS 6
#endif
    db_value_t     result[CLASSINFO_FIELDS];

    struct lmgr_report_t *it;
    lmgr_filter_t  filter;
    int            is_filter = FALSE;
    int            rc;
    unsigned int   result_count;

#ifndef _LUSTRE_HSM /* no filter on type */
    filter_value_t fv;
#endif

    /* To be retrieved for each group:
     * - class names and status
     * - NB entries
     * - SUM(blocks)
     * - MIN/MAX/AVG file size
     */
    report_field_descr_t user_info[CLASSINFO_FIELDS] = {
        {ATTR_INDEX_release_class, REPORT_GROUP_BY, SORT_ASC, FALSE, 0, {NULL}},
#ifdef ATTR_INDEX_archive_class
        {ATTR_INDEX_archive_class, REPORT_GROUP_BY, SORT_ASC, FALSE, 0, {NULL}},
        {ATTR_INDEX_status, REPORT_GROUP_BY, SORT_ASC, FALSE, 0, {NULL}},
#endif
        {0, REPORT_COUNT, SORT_NONE, FALSE, 0, {NULL}},
        {ATTR_INDEX_blocks, REPORT_SUM, SORT_NONE, FALSE, 0, {NULL}},
        {ATTR_INDEX_size, REPORT_MIN, SORT_NONE, FALSE, 0, {NULL}},
        {ATTR_INDEX_size, REPORT_MAX, SORT_NONE, FALSE, 0, {NULL}},
        {ATTR_INDEX_size, REPORT_AVG, SORT_NONE, FALSE, 0, {NULL}},
    };

    is_filter = FALSE;

#ifndef _LUSTRE_HSM /* type is not stored in database: only files are considered */
    /* select only files */
    fv.val_str = STR_TYPE_FILE;
    lmgr_simple_filter_init( &filter );
    lmgr_simple_filter_add( &filter, ATTR_INDEX_type, EQUAL, fv, 0 );
    is_filter = TRUE;
#endif

    mk_global_filters( &filter, TRUE, &is_filter );

    result_count = CLASSINFO_FIELDS;

    /* is a filter specified? */
    if ( is_filter )
        it = ListMgr_Report( &lmgr, user_info, CLASSINFO_FIELDS, &filter, NULL );
    else
        it = ListMgr_Report( &lmgr, user_info, CLASSINFO_FIELDS, NULL, NULL );

    if ( it == NULL )
    {
        DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR: Could not retrieve class information from database." );
        return;
    }

#ifndef ATTR_INDEX_archive_class

    /* a single class column (release), can print as is */
    if ( csv )
        printf( "%20s, %10s, %15s, %15s, %15s, %15s\n",
                "fileclass", "nb_entries", "spc_used", "min_size",
                "max_size", "avg_size" );

    while ( ( rc = ListMgr_GetNextReportItem( it, result, &result_count ) )
            == DB_SUCCESS )
    {
        if ( csv )
            printf( "%20s, %10u, %15Lu, %15Lu, %15Lu, %15Lu\n",
                    class_format( result[0].value_u.val_str ),
                    result[1].value_u.val_uint,
                    result[2].value_u.val_biguint * DEV_BSIZE,
                    result[3].value_u.val_biguint,
                    result[4].value_u.val_biguint,
                    result[5].value_u.val_biguint );
        else
        {
            char           strsize[128] = "";

            printf( "\n" );
            printf( "Class:   %20s\n",
                    class_format( result[0].value_u.val_str ) );
            printf( "Nb entries:   %15u\n", result[1].value_u.val_uint );
            printf( "Space used:   %15s    (%llu blks)\n",
                    FormatFileSize( strsize, 128,
                                    result[2].value_u.val_biguint * DEV_BSIZE ),
                                    result[2].value_u.val_biguint );

            printf( "Size min:     %15s    (%llu bytes)\n",
                    FormatFileSize( strsize, 128, result[3].value_u.val_biguint ),
                                    result[3].value_u.val_biguint );
            printf( "Size max:     %15s    (%llu bytes)\n",
                    FormatFileSize( strsize, 128, result[4].value_u.val_biguint ),
                                    result[4].value_u.val_biguint );
            printf( "Size avg:     %15s    (%llu bytes)\n",
                    FormatFileSize( strsize, 128, result[5].value_u.val_biguint ),
                    result[5].value_u.val_biguint );
        }

    }

#else

    while ( ( rc = ListMgr_GetNextReportItem( it, result, &result_count ) )
            == DB_SUCCESS )
    {
        /* what class do we display? */
        const char * class;
        if ( result[0].value_u.val_str == NULL )
            class = result[1].value_u.val_str;
        else if ( result[1].value_u.val_str == NULL )
            class = result[0].value_u.val_str;
        else /* both are defined */
        {
            /* up to date, released or release_pending:  take purge class */
            if ( IS_PURGE_CONCERNED( result[2].value_u.val_uint ) )
                class = result[0].value_u.val_str;
            else
                class = result[1].value_u.val_str;
        }
        class = class_format(class);

        class_add( class, result );
    }

    if (csv)
        printf( "%20s, %10s, %10s, %10s, %15s, %15s, %15s, %15s, %15s\n",
                "fileclass", "count", "nb_modif", "nb_synchro",
                "spc_used", "spc_modif", "spc_synchro","size_min", "size_max" );
    for ( p_curr = rec_list; p_curr != NULL; p_curr = p_curr->p_next )
    {
        if (csv)
            printf( "%20s, %10u, %10u, %10u, %15Lu, %15Lu, %15Lu, %15Lu, %15Lu\n",
                    class_format( p_curr->class ),
                    p_curr->count, p_curr->nb_modif, p_curr->nb_synchro,
                    p_curr->spc_used, p_curr->spc_modif, p_curr->spc_synchro,
                    p_curr->size_min, p_curr->size_max );
        else
        {
            char strsize[128] = "";
            char str2[128] = "";
            printf( "\n" );
            printf( "Class:   %20s\n",
                    class_format( p_curr->class ) );
            printf( "Nb entries:   %15u", p_curr->count );
            printf( "    (%u modified, %u synchro)\n", p_curr->nb_modif,
                    p_curr->nb_synchro );
            printf( "Space used:   %15s",
                    FormatFileSize( strsize, 128, p_curr->spc_used ) );
            printf( "    (%s modified, %s synchro)\n",
                    FormatFileSize( strsize, 128, p_curr->spc_modif ),
                    FormatFileSize( str2, 128, p_curr->spc_synchro ) );

            printf( "Size min:     %15s    (%llu bytes)\n",
                    FormatFileSize( strsize, 128, p_curr->size_min ),
                    p_curr->size_min );

            printf( "Size max:     %15s    (%llu bytes)\n",
                    FormatFileSize( strsize, 128, p_curr->size_max ),
                    p_curr->size_max );
        }
    }

    /* todo: free list */

#endif

    ListMgr_CloseReport(it);


}



#define MAX_OPT_LEN 1024

/**
 * Main daemon routine
 */
int main( int argc, char **argv )
{
    int            c, option_index = 0;
    char          *bin = basename( argv[0] );
    char          *tmpstr;

    char           config_file[MAX_OPT_LEN] = "";

    int            force_log_level = FALSE;
    int            log_level = 0;

    int            activity = FALSE;
    int            fs_info = FALSE;

    int            user_info = FALSE;
    char           user_name[256] = "";

    int            group_info = FALSE;
    char           group_name[256] = "";

    int            class_info = FALSE;

#ifndef _LUSTRE_HSM
    int            topdirs = 0;
#endif
    int            topsize = 0;
    int            toppurge = 0;
    int            toprmdir = 0;
    int            topuser = 0;
    int            deferred_rm = 0;

    int            dump_all = FALSE;
    int            dump_user = FALSE;
    char           dump_user_name[256]; 
    int            dump_group = FALSE;
    char           dump_group_name[256]; 
#ifdef _LUSTRE
    int            dump_ost = FALSE;
    int            dump_ost_index = -1;
#endif
#ifdef ATTR_INDEX_status
    int            dump_status = FALSE;
    file_status_t  status_to_dump = -1;
#endif

    int            csv = FALSE;

    int            rc;
    char           err_msg[4096];
    robinhood_config_t config;

    /* parse command line options */
    while ( ( c = getopt_long( argc, argv, SHORT_OPT_STRING, option_tab, &option_index ) ) != -1 )
    {
        switch ( c )
        {
        case 'a':
            activity = TRUE;
            break;

        case 'P':
            if ( !optarg )
            {
                fprintf(stderr, "Missing mandatory argument <path> for --filter-path\n");
                exit(1);
            }
            else
            {
                int len;
                strncpy( path_filter, optarg, 1024 );
                strncpy( path_regexp, optarg, 1024 );
                len = strlen(path_regexp);

                if ( path_regexp[len-1] != '/' )
                {
                    path_regexp[len] = '/';
                    len++;
                }
                path_regexp[len] = '*';
                path_regexp[len+1] = '\0';
            }
            break;

        case 'C':
            if ( !optarg )
            {
                fprintf(stderr, "Missing mandatory argument <class> for --filter-class\n");
                exit(1);
            }
            if ( class_info )
            {
                fprintf(stderr, "WARNING: --filter-class option conflicts with --class-info report type. ignored.\n");
                break;
            }
            if (!strcasecmp( optarg, "default"))
                strncpy( class_filter, CLASS_DEFAULT, 1024 );
            else if ( !strcasecmp( optarg, "ignored"))
                strncpy( class_filter, CLASS_IGNORED, 1024 );
            else
                strncpy( class_filter, optarg, 1024 );
            break;

        case OPT_CLASS_INFO:

            if ( class_info )
                fprintf(stderr, "WARNING: --class-info parameter already specified on command line.\n");

            if ( !EMPTY_STRING(class_filter) )
                fprintf(stderr, "WARNING: --class-info conflicts with --filter-class parameter. overriding filter.\n");

            class_info = TRUE;
            if ( optarg )
            {
                if (!strcasecmp( optarg, "default"))
                    strncpy( class_filter, CLASS_DEFAULT, 1024 );
                else if ( !strcasecmp( optarg, "ignored"))
                    strncpy( class_filter, CLASS_IGNORED, 1024 );
                else
                    strncpy( class_filter, optarg, 1024 );
            }
            break;

        case 'i':
            fs_info = TRUE;
            break;

        case 'u':
            user_info = TRUE;
            if ( optarg )
                strncpy( user_name, optarg, 256 );
            break;

        case 'g':
            group_info = TRUE;
            if ( optarg )
                strncpy( group_name, optarg, 256 );
            break;

        case 'D':
            dump_all = TRUE;
            break;

        case OPT_DUMP_USER:
            dump_user = TRUE;
            if ( !optarg )
            {
                fprintf(stderr, "Missing mandatory argument <username> for --dump-user\n");
                exit(1);
            }
            strncpy( dump_user_name, optarg, 256 );
            break; 

        case OPT_DUMP_GROUP:
            dump_group = TRUE;
            if ( !optarg )
            {
                fprintf(stderr, "Missing mandatory argument <groupname> for --dump-group\n");
                exit(1);
            }
            strncpy( dump_group_name, optarg, 256 );
            break; 

#ifdef _LUSTRE
        case OPT_DUMP_OST:
            dump_ost = TRUE;
            if ( !optarg )
            {
                fprintf(stderr, "Missing mandatory argument <ost_index> for --dump-ost\n");
                exit(1);
            }
            dump_ost_index = str2int( optarg );
            if (dump_ost_index == -1)
            {
                    fprintf( stderr,
                             "Invalid value '%s' for --dump-ost option: integer expected\n",
                             optarg );
                    exit( 1 );
            }
            break; 
#endif

#ifdef ATTR_INDEX_status
        case OPT_DUMP_STATUS:
            dump_status = TRUE;
            if ( !optarg )
            {
                fprintf(stderr, "Missing mandatory argument <status> for --dump-status\n");
                exit(1);
            }

            status_to_dump = status2dbval( optarg );
            if ( status_to_dump == (file_status_t)-1 )
            {
                fprintf(stderr, "Unknown status '%s'. Allowed status: "ALLOWED_STATUS".\n", optarg );
                exit(1);
            }
            break;
#endif

#ifndef _LUSTRE_HSM
        case 'd':
            if ( optarg )
            {
                topdirs = str2int( optarg );
                if ( topdirs == -1 )
                {
                    fprintf( stderr,
                             "Invalid parameter '%s' for --topdirs option: positive integer expected\n",
                             optarg );
                    exit( 1 );
                }
            }
            else
                topdirs = DEFAULT_TOP_SIZE;
            break;
#endif

        case 's':
            if ( optarg )
            {
                topsize = str2int( optarg );
                if ( topsize == -1 )
                {
                    fprintf( stderr,
                             "Invalid parameter '%s' for --topsize option: positive integer expected\n",
                             optarg );
                    exit( 1 );
                }
            }
            else
                topsize = DEFAULT_TOP_SIZE;
            break;

        case 'p':
            if ( optarg )
            {
                toppurge = str2int( optarg );
                if ( toppurge == -1 )
                {
                    fprintf( stderr,
                             "Invalid parameter '%s' for --toppurge option: positive integer expected\n",
                             optarg );
                    exit( 1 );
                }
            }
            else
                toppurge = DEFAULT_TOP_SIZE;
            break;

        case 'r':
            if ( optarg )
            {
                toprmdir = str2int( optarg );
                if ( toprmdir == -1 )
                {
                    fprintf( stderr,
                             "Invalid parameter '%s' for --toprmdir option: positive integer expected\n",
                             optarg );
                    exit( 1 );
                }
            }
            else
                toprmdir = DEFAULT_TOP_SIZE;
            break;

        case 'U':
            if ( optarg )
            {
                topuser = str2int( optarg );
                if ( topuser == -1 )
                {
                    fprintf( stderr,
                             "Invalid parameter '%s' for --topusers option: positive integer expected\n",
                             optarg );
                    exit( 1 );
                }
            }
            else
                topuser = DEFAULT_TOP_SIZE;
            break;

#ifdef _LUSTRE_HSM
        case 'R':
            deferred_rm = TRUE;
            break;
#endif
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

        case 'c':
            csv = TRUE;
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

    if ( !activity && !fs_info && !user_info && !group_info
         && !topsize && !toppurge && !topuser && !dump_all
         && !dump_user && !dump_group && !class_info
#ifdef ATTR_INDEX_status
         && !dump_status
#endif
#ifdef _LUSTRE_HSM
        && !deferred_rm
#else
        && !topdirs && !toprmdir
#endif
#ifdef _LUSTRE
        && !dump_ost
#endif
        )
    {
        display_help( bin );
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
        DisplayLog( LVL_CRIT, REPORT_TAG, "Error %d initializing list manager", rc );
        exit( rc );
    }
    else
        DisplayLog( LVL_DEBUG, REPORT_TAG, "ListManager successfully initialized" );

    if ( CheckLastFS(  ) != 0 )
        exit( 1 );

    /* Create database access */
    rc = ListMgr_InitAccess( &lmgr );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, REPORT_TAG, "Error %d: cannot connect to database", rc );
        exit( rc );
    }


    /* retrieve and display info */
    if ( activity )
        report_activity( csv );

    if ( fs_info )
        report_fs_info( csv );

    if ( user_info )
        report_usergroup_info( ( EMPTY_STRING( user_name ) ? NULL : user_name ), csv, FALSE );

    if ( group_info )
        report_usergroup_info( ( EMPTY_STRING( group_name ) ? NULL : group_name ), csv, TRUE );

    if ( class_info )
        report_class_info(csv);

#ifndef _LUSTRE_HSM
    if ( topdirs )
        report_topdirs( topdirs, csv );
#endif

    if ( topsize )
        report_topsize( topsize, csv );

    if ( toppurge )
        report_toppurge( toppurge, csv );

#ifndef _LUSTRE_HSM
    if ( toprmdir )
        report_toprmdir( toprmdir, csv );
#endif

    if ( topuser )
        report_topuser( topuser, csv );

#ifdef _LUSTRE_HSM
    if ( deferred_rm )
        report_deferred_rm( csv );
#endif

    if ( dump_all )
        dump_entries( DUMP_ALL, 0, NULL, csv );

    if ( dump_user )
        dump_entries( DUMP_USR, 0, dump_user_name, csv );

    if ( dump_group )
        dump_entries( DUMP_GROUP, 0, dump_group_name, csv );
    
#ifdef _LUSTRE
    if ( dump_ost )
        dump_entries( DUMP_OST, dump_ost_index, NULL, csv );
#endif

#ifdef ATTR_INDEX_status
    if ( dump_status )
        dump_entries( DUMP_STATUS, status_to_dump, NULL, csv );
#endif

    ListMgr_CloseAccess( &lmgr );

    return 0;                   /* for compiler */

}
