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
#include "cmd_helpers.h"
#include "RobinhoodConfig.h"
#include "RobinhoodLogs.h"
#include "RobinhoodMisc.h"
#include "xplatform_print.h"
#include "uidgidcache.h"

#include <unistd.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <libgen.h>
#include <pthread.h>
#include <string.h>

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

#define SET_NEXT_MAINT    261
#define CLEAR_NEXT_MAINT  262

#define OPT_BY_COUNT      263
#define OPT_BY_AVGSIZE    264
#define OPT_COUNT_MIN     265

#define OPT_TOPRMDIR      266

#define OPT_SIZE_PROFILE  267
#define OPT_BY_SZ_RATIO   268

/* options flags */
#define OPT_FLAG_CSV        0x0001
#define OPT_FLAG_NOHEADER   0x0002
#define OPT_FLAG_GROUP      0x0004
#define OPT_FLAG_MATCH_NULL_STATUS 0x0008
#define OPT_FLAG_NO_ACCT 0x0010
#define OPT_FLAG_SPLITUSERGROUP 0x0020
#define OPT_FLAG_BY_COUNT       0x0040
#define OPT_FLAG_BY_AVGSIZE     0x0080
#define OPT_FLAG_REVERSE        0x0100
#define OPT_FLAG_SPROF          0x0200
#define OPT_FLAG_BY_SZRATIO     0x0400

#define CSV(_x) ((_x)&OPT_FLAG_CSV)
#define NOHEADER(_x) ((_x)&OPT_FLAG_NOHEADER)
#define ISGROUP(_x) ((_x)&OPT_FLAG_GROUP)
#define ISSPLITUSERGROUP(_x) ((_x)&OPT_FLAG_SPLITUSERGROUP)
#define MATCH_NULL_STATUS(_x) ((_x)&OPT_FLAG_MATCH_NULL_STATUS)
#define FORCE_NO_ACCT(_x) ((_x)&OPT_FLAG_NO_ACCT)
#define SORT_BY_COUNT(_x) ((_x)&OPT_FLAG_BY_COUNT)
#define SORT_BY_AVGSIZE(_x) ((_x)&OPT_FLAG_BY_AVGSIZE)
#define SORT_BY_SZRATIO(_x) ((_x)&OPT_FLAG_BY_SZRATIO)
#define REVERSE(_x) ((_x)&OPT_FLAG_REVERSE)
#define SPROF(_x) ((_x)&OPT_FLAG_SPROF)

static profile_field_descr_t size_profile =
{
    .attr_index = ATTR_INDEX_size,
    .range_ratio_start = 0,
    .range_ratio_len = 0,
    .range_ratio_sort = SORT_NONE
};

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
#ifdef ATTR_INDEX_dircount
    {"topdirs", optional_argument, NULL, 'd'},
    {"top-dirs", optional_argument, NULL, 'd'},
#endif
    {"topsize", optional_argument, NULL, 's'},
    {"top-size", optional_argument, NULL, 's'},
    {"toppurge", optional_argument, NULL, 'p'},
    {"top-purge", optional_argument, NULL, 'p'},
#ifdef HAVE_RMDIR_POLICY
    {"toprmdir", optional_argument, NULL, OPT_TOPRMDIR},
    {"top-rmdir", optional_argument, NULL, OPT_TOPRMDIR},
#endif
    {"topusers", optional_argument, NULL, 'U'},
    {"top-users", optional_argument, NULL, 'U'},
#ifdef HAVE_RM_POLICY
    {"deferred-rm", no_argument, NULL, 'R' },
#endif
    {"dump", no_argument, NULL, 'D' },
    {"dump-all", no_argument, NULL, 'D' }, /* for backward compatibility */
    {"dump-user", required_argument, NULL, OPT_DUMP_USER },
    {"dump-group", required_argument, NULL, OPT_DUMP_GROUP },
#ifdef _LUSTRE
    {"dump-ost", required_argument, NULL, OPT_DUMP_OST },
#endif
#ifdef ATTR_INDEX_status
    {"dump-status", required_argument, NULL, OPT_DUMP_STATUS },
#endif

    {"szprof", no_argument, NULL, OPT_SIZE_PROFILE}, /* size profile */
    {"size-profile", no_argument, NULL, OPT_SIZE_PROFILE},

    /* additional options for topusers etc... */
    {"filter-path", required_argument, NULL, 'P' },
    {"filter-class", required_argument, NULL, 'C' },
    {"split-user-groups", no_argument, NULL, 'S'},
    {"by-count", no_argument, NULL, OPT_BY_COUNT},
    {"by-avgsize", no_argument, NULL, OPT_BY_AVGSIZE},
    {"by-avg-size", no_argument, NULL, OPT_BY_AVGSIZE},
    {"by-szratio", required_argument, NULL, OPT_BY_SZ_RATIO},
    {"by-size-ratio", required_argument, NULL, OPT_BY_SZ_RATIO},

    {"count-min", required_argument, NULL, OPT_COUNT_MIN },
    {"reverse", no_argument, NULL, 'r' },

#ifdef HAVE_MIGR_POLICY
    {"next-maintenance", optional_argument, NULL, SET_NEXT_MAINT},
    {"cancel-maintenance", no_argument, NULL, CLEAR_NEXT_MAINT},
#endif

    /* config file options */
    {"config-file", required_argument, NULL, 'f'},

    /* output format option */
    {"csv", no_argument, NULL, 'c'},
    {"no-header", no_argument, NULL, 'q'},

    /* verbosity level */
    {"log-level", required_argument, NULL, 'l'},

    /* miscellaneous options */
    {"help", no_argument, NULL, 'h'},
    {"version", no_argument, NULL, 'V'},
    {"force-no-acct", no_argument, NULL, 'F'},

    {NULL, 0, NULL, 0}

};

#define SHORT_OPT_STRING    "aiDu:g:d:s:p:rU:P:C:Rf:cql:hVFS"

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
#ifdef ATTR_INDEX_dircount
    "    " _B "--top-dirs" B_ " [=" _U "count" U_ "], " _B "-d" B_ " " _U "count" U_ "\n"
    "        Display largest directories. Optional argument indicates the number of directories to be returned (default: 20).\n"
#endif
    "    " _B "--top-size" B_ " [=" _U "count" U_ "], " _B "-s" B_ " " _U "count" U_ "\n"
    "        Display largest files. Optional argument indicates the number of files to be returned (default: 20).\n"
    "    " _B "--top-purge" B_ " [=" _U "count" U_ "], " _B "-p" B_ " " _U "count" U_ "\n"
    "        Display oldest entries eligible for purge. Optional argument indicates the number of entries to be returned (default: 20).\n"
#ifdef HAVE_RMDIR_POLICY
    "    " _B "--top-rmdir" B_ " [=" _U "count" U_ "]\n"
    "        Display oldest empty directories eligible for rmdir. Optional argument indicates the number of dirs to be returned (default: 20).\n"
#endif
    "    " _B "--top-users" B_ " [=" _U "count" U_ "], " _B "-U" B_ " " _U "count" U_ "\n"
    "        Display top disk space consumers. Optional argument indicates the number of users to be returned (default: 20).\n"
#ifdef HAVE_RM_POLICY
    "    " _B "--deferred-rm" B_ ", " _B "-R" B_ "\n"
    "        Display files to be removed from HSM.\n"
#endif
    "\n"_B "Deprecated:" B_ "\n"
    "    "  _B "--dump" B_ ", "_B "--dump-user" B_ ", "_B "--dump-group" B_
#ifdef _LUSTRE
     ", " _B "--dump-ost" B_
#endif
    ": use rbh-find instead\n"

#ifdef ATTR_INDEX_status
    "    "  _B "--dump-status" B_ " " _U "status" U_ "\n"
    "        Dump all entries with the given status (%s).\n"
#endif
    "\n"
    _B "Filter options:" B_ "\n"
    "The following filters can be speficied for reports:\n"
    "    " _B "-P" B_ " " _U "path" U_ ", " _B "--filter-path" B_ " " _U "path" U_ "\n"
    "        Display the report only for objects in the given path.\n"
    "    " _B "-C" B_ " " _U "class" U_ ", " _B "--filter-class" B_ " " _U "class" U_ "\n"
    "        Report only entries in the given FileClass.\n"
    "    " _B "--count-min" B_ " "_U"cnt"U_"\n"
    "        Display only topuser/userinfo with at least "_U"cnt"U_" entries\n\n"
    "\n"
#ifdef HAVE_MIGR_POLICY
    _B "Pre-maintenance commands:" B_ "\n"
    "    " _B "--next-maintenance[="B_ _U"date_time"U_"]\n"
    "        Set/display time of next maintenance.\n"
    "        Expected "_U"date_time"U_" format is yyyymmddHHMM[SS].\n"
    "    " _B "--cancel-maintenance"B_"\n"
    "        Cancel next maintenance.\n\n"
#endif
    _B "Accounting report options:" B_ "\n"
    "    " _B "--size-profile" B_ ", "_B "--szprof" B_ "\n"
    "        Display size profile statistics\n"
    "    " _B "--by-count" B_ "\n"
    "        Sort by count\n"
    "    " _B "--by-avgsize" B_ "\n"
    "        Sort by average file size\n"
    "    " _B "--by-size-ratio" B_ " "_U"range"U_", "_B "--by-szratio"B_" " _U"range"U_"\n"
    "        Sort on the ratio of files in the given size-range\n"
    "        "_U"range"U_": <val><sep><val>- or <val><sep><val-1> or <val><sep>inf\n"
    "           <val>: 0, 1, 32, 1K 32K, 1M, 32M, 1G, 32G, 1T\n"
    "           <sep>: ~ or ..\n"
    "           e.g: 1G..inf, 1..1K-, 0..31M\n"

 /* expected format:
        0
        <start_val><sep>[<end_val>]
            <start_val>: 0, 1, 32, 1K, 32K, 1M, 32M, 1G, 32G, 1T
            <sep>: ~ or ..
            <end_val>: <start_val>- (e.g. "1K-") or <start_val - 1> (e.g. 31K)
                       if no end_val is specified, the range has no upper limit

    examples:
            1G- => 1GB to infinite
            1K..1G- => 1K to 1GB-1
            1K..1023M => 1K to 1GB-1
    */

    "    " _B "--reverse" B_ "\n"
    "        Reverse sort order\n"
    "    " _B "-S" B_ ", " _B "--split-user-groups" B_ "\n"
    "        Display the report by user AND group\n"
    "    " _B "-F" B_ ", " _B "--force-no-acct" B_ "\n"
    "        Generate the report without using accounting table (slower)\n\n"
    _B "Config file options:" B_ "\n"
    "    " _B "-f" B_ " " _U "file" U_ ", " _B "--config-file=" B_ _U "file" U_ "\n"
    "        Path to configuration file (or short name).\n"
    "\n"
    _B "Output format options:" B_ "\n"
    "    " _B "-c" B_ " , " _B "--csv" B_ "\n"
    "        Output stats in a csv-like format for parsing\n"
    "    " _B "-q" B_ " , " _B "--no-header" B_ "\n"
    "        Don't display column headers/footers\n"
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
    printf( help_string, bin_name
#ifdef ATTR_INDEX_status
,allowed_status()
#endif
);
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




static lmgr_t  lmgr;

/* global filter variables */
char path_filter[RBH_PATH_MAX] = "";
char class_filter[1024] = "";
unsigned int count_min = 0;

#define KB  1024L
#define MB  (KB*KB)
#define GB  (KB*MB)
#define TB  (KB*GB)
#define PB  (KB*TB)
#define EB  (KB*PB)

static char * print_brief_sz(uint64_t sz, char * buf)
{
    if (sz < KB)
        sprintf(buf, "%"PRIu64, sz);
    else if (sz < MB)
        sprintf(buf, "%"PRIu64"K", sz/KB);
    else if (sz < GB)
        sprintf(buf, "%"PRIu64"M", sz/MB);
    else if (sz < TB)
        sprintf(buf, "%"PRIu64"G", sz/GB);
    else if (sz < PB)
        sprintf(buf, "%"PRIu64"T", sz/TB);
    else if (sz < EB)
        sprintf(buf, "%"PRIu64"P", sz/PB);
    else
        sprintf(buf, "%"PRIu64"E", sz/EB);
    return buf;
}

/**
 * @param exact exact range value expected
 * @return index of the range it matches
 * @retval -1 on error
 */
static int szrange_val2index(uint64_t val, int exact)
{
    int i;
    if (exact) /* search exact value */
    {
        for (i=0; i<SZ_PROFIL_COUNT; i++)
            if (val == SZ_MIN_BY_INDEX(i))
                return i;
    }
    else /* search val-1:  eg 1023M for 1G-, 31M for 32M- */
    {
        i=0;
        while (val > SZ_MIN_BY_INDEX(i))
        {
            if (i < SZ_PROFIL_COUNT-1)
            {
                if (val < SZ_MIN_BY_INDEX(i+1))
                {
                    return i;
                }
            }
            else
            {
                /* matches the last */
                return SZ_PROFIL_COUNT-1;
            }
            i++;
        }
    }
    /* not found */
    return -1;
}

#define EXPECTED_SZ_RANGES "0, 1, 32, 1K, 32K, 1M, 32M, 1G, 32G, 1T"
static int parse_size_range(const char * str, profile_field_descr_t * p_profile)
{
    char argcp[1024];
    char *beg = NULL;
    char *end = NULL;
    char *sep = NULL;
    uint64_t sz1;
    uint64_t sz2;
   /* expected format:
        0
        <start_val><sep>[<end_val>]
            <start_val>: 0, 1, 32, 1K, 32K, 1M, 32M, 1G, 32G, 1T
            <sep>: ~ or ..
            <end_val>: <start_val>- (e.g. "1K-") or <start_val - 1> (e.g. 31K)
                       if no end_val is specified, the range has no upper limit

    examples:
            1G- => 1GB to infinite
            1K..1G- => 1K to 1GB-1
            1K..1023M => 1K to 1GB-1
    */
    strcpy(argcp, str);
    /* is there a separator? */
    if ((sep = strchr(argcp, '~')))
    {
        *sep = '\0';
        beg=argcp;
        end=sep+1;
    }
    else if ((sep = strstr(argcp, "..")))
    {
        *sep = '\0';
        beg=argcp;
        end=sep+2;
    }
    else /* single value? */
    {
        beg=argcp;
        end=NULL;
    }

    /* parse first value */
    sz1 = str2size(beg);
    if (sz1 == (uint64_t)-1LL)
    {
        fprintf(stderr, "Invalid argument: '%s' is not a valid size format\n",
                                           beg );
        return -EINVAL;
    }
    if (end == NULL || !strcmp(end, "0"))
    {
        /* size value range: only 0 allowed */
        if (sz1 != 0LL)
        {
            fprintf(stderr, "Only 0 is allowed for single value range (%s not allowed)\n", beg);
            return -EINVAL;
        }
        p_profile->range_ratio_start = 0;
        p_profile->range_ratio_len = 1;
        /* sort order is determined later */
        return 0;
    }

    p_profile->range_ratio_start = szrange_val2index(sz1, TRUE);
    if (p_profile->range_ratio_start == (unsigned int)-1)
    {
        fprintf(stderr, "Invalid argument: %s is not a valid range start. Allowed values: "EXPECTED_SZ_RANGES"\n", beg);
        return -EINVAL;
    }

    /* to the infinite ? */
    if (end[0] == '\0' || !strcasecmp(end, "inf"))
    {
        if (p_profile->range_ratio_start >= SZ_PROFIL_COUNT)
        {
            fprintf(stderr, "Error: range end < range start\n");
            return -EINVAL;
        }
        p_profile->range_ratio_len = SZ_PROFIL_COUNT - p_profile->range_ratio_start;
        return 0;
    }

    /* is second value ends with a '-' ? */
    if (end[strlen(end)-1] == '-')
    {
        int end_idx;
        /* exact match */
        end[strlen(end)-1] = '\0';
        sz2=str2size(end);
        if (sz2 == (uint64_t)-1LL)
        {
            fprintf(stderr, "Invalid argument: '%s' is not a valid size format\n",
                                               end );
            return -EINVAL;
        }
        end_idx = szrange_val2index(sz2, TRUE); /* actually the upper index */
        if (end_idx <= 0)
        {
            fprintf(stderr, "Invalid argument: %s is not a valid range end. Allowed values: "EXPECTED_SZ_RANGES"\n", end);
            return -EINVAL;
        }
        if (p_profile->range_ratio_start >= end_idx)
        {
            fprintf(stderr, "Error: range end < range start\n");
            return -EINVAL;
        }
        p_profile->range_ratio_len = end_idx - p_profile->range_ratio_start;
        return 0;
    }
    else
    {
        int end_idx;
        sz2=str2size(end);
        if (sz2 == (uint64_t)-1LL)
        {
            fprintf(stderr, "Invalid argument: '%s' is not a valid size format\n",
                                               end );
            return -EINVAL;
        }
        end_idx = szrange_val2index(sz2, FALSE);
        if (end_idx < 0)
        {
            fprintf(stderr, "Invalid argument: %s is not a valid range end: terminate it with '-'\n", end);
            return -EINVAL;
        }
        if (p_profile->range_ratio_start > end_idx)
        {
            fprintf(stderr, "Error: range end < range start\n");
            return -EINVAL;
        }
        p_profile->range_ratio_len = end_idx - p_profile->range_ratio_start + 1;
        return 0;
    }

    return -1;
}



/**
 *  Read variable from DB and allocate value.
 *  @param value must be of size 1024.
 **/
int ListMgr_GetVar_helper( lmgr_t * p_mgr, const char *varname, char *value )
{
    int rc;
    rc = ListMgr_GetVar( &lmgr, varname, value);
    if ( rc == DB_SUCCESS )
        return 0;
    else if ( rc == DB_NOT_EXISTS )
    {
        strcpy(value,"unknown");
        DisplayLog( LVL_VERB, REPORT_TAG, "WARNING variable %s not in database", varname );
        return rc;
    }
    else
    {
        strcpy(value,"error");
        DisplayLog( LVL_CRIT, REPORT_TAG, "ERROR %d retrieving variable %s from database", rc, varname );
        return rc;
    }
}

#ifdef _HAVE_FID
/**
 * Manage fid2path resolution
 */
int TryFid2path( lmgr_t * p_mgr, const entry_id_t * p_id,  char * path )
{
    static int is_init = 0;
    static int is_resolvable = 0;
    int rc;
    char value[1024];
    dev_t      dev;


    if ( !is_init ) {
        is_init = 1;
        /* try to get fspath from DB */
        rc = ListMgr_GetVar(&lmgr, FS_PATH_VAR, value);
        if (rc)
            return -1;
        /* try to check filesystem */
        if (CheckFSInfo( value, "lustre", &dev, TRUE, TRUE) == 0) {
            is_resolvable = 1;
            /* may be used for solving uids from filesystem */
            InitUidGid_Cache();
            Lustre_Init();
        }
        else
            return -1;
    }
    if ( !is_resolvable )
        return -1;

    /* filesystem is mounted and fsname can be get: solve the fid */
    rc = Lustre_GetFullPath( p_id, path, RBH_PATH_MAX );
    return rc;
}
#endif

static const char * ResolvName(const entry_id_t * p_id, attr_set_t * attrs,
                               char * buff)
{
    if (ATTR_MASK_TEST(attrs, fullpath))
    {
        return  ATTR(attrs, fullpath);
    }
    else
    {
#ifdef _HAVE_FID
        /* try to get dir path from fid if it's mounted */
        if ( TryFid2path( &lmgr, p_id, ATTR(attrs, fullpath)) == 0 )
        {
            struct stat st;
            ATTR_MASK_SET(attrs, fullpath);

            /* we're lucky, try lstat now! */
            if (lstat(ATTR(attrs, fullpath ), &st) == 0)
                PosixStat2EntryAttr(&st, attrs, TRUE);
            return ATTR(attrs, fullpath);
        }
        else
        {
            sprintf(buff, DFID, PFID(p_id));
            return buff;
        }
#else
        sprintf(buff, "%#"PRI_DT"/%Lu", p_id->device,
                (unsigned long long)p_id->inode);
        return buff;
#endif
    }
}


void report_activity( int flags )
{
    char           value[1024];
    time_t         timestamp;
    time_t         timestamp2;
    char           date[128];
    struct tm      t;
    int            rc;
    char           scan_status[128];
    int            nb_threads;


    if ( !CSV(flags) )
        printf( "\nFilesystem scan activity:\n\n" );

#ifndef _LUSTRE_HSM
    /* Scan interval */
    if ( ListMgr_GetVar_helper( &lmgr, SCAN_INTERVAL_VAR, value ) == 0 )
    {
        timestamp = str2int( value );
        FormatDurationFloat( date, 128, timestamp );
        if ( CSV(flags) )
            printf( "current_scan_interval, %s\n", date );
        else
            printf( "    Current scan interval:   %s\n\n", date );
    }
#endif

    /* Previous FS scan */

    if ( ListMgr_GetVar_helper( &lmgr, PREV_SCAN_START_TIME, value ) == 0 )
    {
        timestamp = str2int( value );
        if ( timestamp >= 0 ) {
            strftime( date, 128, "%Y/%m/%d %T", localtime_r( &timestamp, &t ) );
            if ( !CSV(flags) )
            {
                printf( "    Previous filesystem scan:\n" );
                printf( "            start:           %s\n", date );
            }
            else
                printf("previous_scan_start, %s\n", date);

            if ( ListMgr_GetVar_helper( &lmgr, PREV_SCAN_END_TIME, value ) == 0 )
            {
                timestamp2 = str2int( value );
                if ( timestamp2 >= timestamp )
                {
                    int dur = (int)difftime( timestamp2, timestamp );
                    if ( !CSV(flags) )
                    {
                        FormatDuration( value, 1024, dur );
                        printf( "            duration:        %s\n\n", value);
                    }
                    else
                        printf( "previous_scan_duration, %i sec\n", dur );
                }
            }
        }
    }

    /* Last FS scan */

    // status
    rc = ListMgr_GetVar_helper( &lmgr, LAST_SCAN_STATUS, scan_status );

    if ( rc == 0 )
    {
        if ( !CSV(flags) )
        {
            printf( "    Last filesystem scan:\n" );
            printf( "            status:          %s\n", scan_status );
        }
        else
            printf( "last_scan_status, %s\n", scan_status );
    }
    else if ( rc == DB_NOT_EXISTS )
    {
        if ( CSV(flags) )
            printf( "last_scan_status, no scan done\n" );
        else
            printf( "    Filesystem has never been scanned\n" );
    }

    // start
    if ( ListMgr_GetVar_helper( &lmgr, LAST_SCAN_START_TIME, value ) == 0 )
        timestamp = str2int( value );
    else
        timestamp = -1;

    if ( timestamp > 0 )
    {
        strftime( date, 128, "%Y/%m/%d %T", localtime_r( &timestamp, &t ) );
        if ( CSV(flags) )
            printf( "last_scan_start, %s\n", date );
        else {
            int ago = difftime( time( NULL ), timestamp );
            if ( !strcmp( scan_status, SCAN_STATUS_RUNNING ) )
            {
                FormatDuration( value, 1024, ago );
                printf( "            start:           %s (%s ago)\n", date, value );
            }
            else
                printf( "            start:           %s\n", date );
        }
    }

    // last action
    if( !strcmp( scan_status, SCAN_STATUS_RUNNING ) &&
        ListMgr_GetVar_helper( &lmgr, LAST_SCAN_LAST_ACTION_TIME, value ) == 0 )
    {
        timestamp2 = str2int( value );
        if (timestamp2 > 0)
        {
            strftime( date, 128, "%Y/%m/%d %T", localtime_r( &timestamp2, &t ) );
            if ( CSV(flags) )
                printf( "last_action_time, %s\n", date );
            else {
                int ago = difftime( time( NULL ), timestamp2 );
                if ( !strcmp( scan_status, SCAN_STATUS_RUNNING ) )
                {
                    FormatDuration( value, 1024, ago );
                    printf( "            last action:     %s (%s ago)\n", date, value );
                }
                else
                    printf( "            last action:     %s\n", date );
            }
        }
    }

    // end
    if ( ListMgr_GetVar_helper( &lmgr, LAST_SCAN_END_TIME, value ) == 0 )
    {
        timestamp2 = str2int( value );
        if ( timestamp2 >= timestamp )
        {
            strftime( date, 128, "%Y/%m/%d %T", localtime_r( &timestamp2, &t ) );
            if ( CSV(flags) )
                printf( "last_scan_end, %s\n", date );
            else
                printf( "            end:             %s\n", date );

            // duration
            if ( timestamp > 0 )
            {
                int dur = (int)difftime( timestamp2, timestamp );
                if ( CSV(flags) )
                    printf( "last_scan_duration, %i sec\n", dur);
                else
                {
                    FormatDuration( value, 1024, dur );
                    printf( "            duration:        %s\n", value);
                }
            }
        }
    }

    rc = ListMgr_GetVar_helper( &lmgr, LAST_SCAN_ENTRIES_SCANNED, value);
    if (rc == 0)
    {
        // entries scanned
        if ( !CSV(flags) )
        {
            printf( "\n" );
            printf( "         Statistics:\n" );
        }
        if ( CSV(flags) )
            printf( "entries_scanned, %s\n", value );
        else
            printf( "            entries scanned: %s\n", value );

        // errors
        ListMgr_GetVar_helper( &lmgr, LAST_SCAN_ERRORS, value);
        if ( CSV(flags) )
            printf( "scan_errors, %s\n", value );
        else
            printf( "            errors:          %s\n", value );

        // timeouts
        ListMgr_GetVar_helper( &lmgr, LAST_SCAN_TIMEOUTS, value);
        if ( CSV(flags) )
            printf( "scan_timeouts, %s\n", value );
        else
            printf( "            timeouts:        %s\n", value );

        // nb threads
        ListMgr_GetVar_helper( &lmgr, LAST_SCAN_NB_THREADS, value );
        nb_threads = atoi( value );
        if ( CSV(flags) )
            printf( "scan_nb_threads, %i\n", nb_threads );
        else
            printf( "            # threads:       %i\n", nb_threads );

        // average speed
        ListMgr_GetVar_helper( &lmgr, LAST_SCAN_AVGMSPE, value);
        double speed = 0.0;
        double avgmspe = atof(value);
        if ( avgmspe > 0 )
            speed = ( 1000.0 / avgmspe ) * nb_threads;
        if ( CSV(flags) )
            printf( "scan_average_speed, %.2f entries/sec\n", speed );
        else
            printf( "            average speed:   %.2f entries/sec\n", speed );

        // current speed
        if ( !strcmp( scan_status, SCAN_STATUS_RUNNING ) )
        {
            ListMgr_GetVar_helper( &lmgr, LAST_SCAN_CURMSPE, value);
            double speed = 0.0;
            double curmspe = atof(value);
            if ( value > 0 )
                speed = ( 1000.0 / curmspe ) * nb_threads;
            if ( CSV(flags) )
                printf( "scan_current_speed, %.2f\n", speed );
            else
                printf( "        >>> current speed:   %.2f entries/sec\n", speed );
        }
    }

    if ( !CSV(flags) )
        printf( "\n" );

#ifdef HAVE_CHANGELOGS
    /* changelog stats */
    rc = ListMgr_GetVar( &lmgr, CL_LAST_READ_ID, value );
    if ( rc == DB_SUCCESS )
    {
        int i;
        unsigned int interval;

        if ( CSV(flags) )
            printf( "changelog_last_record_id, %s\n", value );
        else
        {
            printf( "\nChangelog stats:\n\n" );
            printf( "        Last read record id:      %s\n", value );
        }

        if ( ListMgr_GetVar( &lmgr, CL_LAST_READ_TIME, value ) == DB_SUCCESS )
        {
            if ( CSV(flags) )
                printf( "changelog_last_time, %s\n", value );
            else
                printf( "        Last record read time:    %s\n", value );
        }

        if ( ListMgr_GetVar( &lmgr, CL_LAST_COMMITTED, value ) == DB_SUCCESS )
        {
            if ( CSV(flags) )
                printf( "changelog_last_committed_id, %s\n", value );
            else
                printf( "        Last committed record id: %s\n", value );
        }

        if ( !CSV(flags) )
        {
            printf("        Changelog stats:\n");
            printf("                %5s  %15s \t(%s)\t(%s)\n", "type",
                   "total", "diff", "rate" );
        }
        else
            printf("%11s, %12s, %8s, %s\n",
                   "record_type", "total", "diff", "rate (ops/sec)" );

        /* get diff interval */
        if ( ListMgr_GetVar( &lmgr, CL_DIFF_INTERVAL, value ) != DB_SUCCESS )
            interval = 0;
        else
            interval = str2int(value);

        for (i = 0; i < CL_LAST; i++)
        {
            char varname[256];
            char varname2[256];
            char diff_str[256];
            unsigned long long diff;
            double rate;

            sprintf( varname, "%s_%s", CL_COUNT_PREFIX, changelog_type2str(i) );
            sprintf( varname2, "%s_%s", CL_DIFF_PREFIX, changelog_type2str(i) );

            rc = ListMgr_GetVar( &lmgr, varname, value );
            if (rc == DB_NOT_EXISTS )
                strcpy(value, "0");
            else if (rc != 0)
                strcpy(value, "db_error");

            if ((interval > 0) && (ListMgr_GetVar( &lmgr, varname2, diff_str ) == DB_SUCCESS))
            {
                diff = str2bigint(diff_str);
                rate = (0.0+diff)/(0.0+interval);
            }
            else
            {
                diff = 0;
                rate = 0.0;
            }

            if ( CSV(flags) )
                printf( "%11s, %12s, %8Lu, %8.2f\n",  changelog_type2str(i), value,
                        diff, rate );
            else if (diff != 0)
                printf("                %5s: %15s \t(+%Lu)\t(%.2f/sec)\n",
                        changelog_type2str(i), value, diff, rate );
            else
                printf("                %5s: %15s\n", changelog_type2str(i), value );

        }

        if ( !CSV(flags) )
            printf( "\n" );
    }
#endif

    /* max usage */
    rc = ListMgr_GetVar( &lmgr, USAGE_MAX_VAR, value );
    if ( rc == DB_SUCCESS )
    {
        if ( CSV(flags) )
            printf( "usage_max, %s\n", value );
        else
            printf( "Storage unit usage max:   %s%%\n", value );
    }
    else if ( rc == DB_NOT_EXISTS )
    {
        if ( CSV(flags) )
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
    if ( ! CSV(flags) )
        printf( "\n" );

    /* Last migration */
    rc = ListMgr_GetVar( &lmgr, LAST_MIGR_TIME, value );
    if ( rc == DB_SUCCESS )
    {
        timestamp = atoi( value );
        strftime( date, 128, "%Y/%m/%d %T", localtime_r( &timestamp, &t ) );
        if (  CSV(flags) )
            printf( "last_migration_time, %s\n", date );
        else
            printf( "Last migration:           %s\n", date );
    }
    else if ( rc == DB_NOT_EXISTS )
    {
        if ( CSV(flags) )
            printf( "last_migration_time, unknown\n" );
        else
            printf( "No migration was performed on this filesystem\n\n" );
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
        if ( CSV(flags) )
            printf( "last_migration_status, %s\n", value );
        else
            printf( "    Status:               %s\n", value );
    }

    if ( ListMgr_GetVar( &lmgr, LAST_MIGR_INFO, value ) == DB_SUCCESS )
    {
        if ( CSV(flags) )
            printf( "last_migration_info, %s\n", value );
        else
            printf( "    Migration info:       %s\n", value );
    }

#endif

    if ( ! CSV(flags) )
        printf( "\n" );

    /* Last purge */
    rc = ListMgr_GetVar( &lmgr, LAST_PURGE_TIME, value );
    if ( rc == DB_SUCCESS )
    {
        timestamp = atoi( value );
        strftime( date, 128, "%Y/%m/%d %T", localtime_r( &timestamp, &t ) );
        if ( CSV(flags) )
            printf( "last_purge_time, %s\n", date );
        else
            printf( "Last purge:               %s\n", date );
    }
    else if ( rc == DB_NOT_EXISTS )
    {
        if ( CSV(flags) )
            printf( "last_purge_time, unknown\n" );
        else
            printf( "No purge was performed on this filesystem\n\n" );
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
        if ( CSV(flags) )
            printf( "last_purge_target, %s\n", value );
        else
            printf( "    Target:               %s\n", value );
    }

    if ( ListMgr_GetVar( &lmgr, LAST_PURGE_STATUS, value ) == DB_SUCCESS )
    {
        if ( CSV(flags) )
            printf( "last_purge_status, %s\n", value );
        else
            printf( "    Status:               %s\n", value );
    }
    if ( !CSV(flags) )
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
static inline const char * migr_class( const attr_set_t * attrs )
{
    if (!ATTR_MASK_TEST(attrs, archive_class))
        return "";
    else
        return class_format( ATTR(attrs, archive_class) );
}
#endif

#ifdef ATTR_INDEX_release_class
static inline const char * release_class( const attr_set_t * attrs )
{
    if (!ATTR_MASK_TEST(attrs, release_class))
        return "";
    else
        return class_format( ATTR(attrs, release_class) );
}
#endif


/*
 * Append global filters on path, class...
 * \param do_display [in] display filters?
 * \param initialized [in/out] indicate if the filter is initialized.
 */
static int mk_global_filters( lmgr_filter_t * filter, int do_display, int * initialized )
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
        if ( path_filter[len-1] != '/' )
        {
            /* ( fullpath LIKE 'path' OR fullpath LIKE 'path/%' ) */
            fv.val_str = path_filter;
            lmgr_simple_filter_add( filter, ATTR_INDEX_fullpath, LIKE, fv,
                                    FILTER_FLAG_BEGIN );

            snprintf( path_regexp, RBH_PATH_MAX, "%s/*", path_filter );
            fv.val_str = path_regexp;
            lmgr_simple_filter_add( filter, ATTR_INDEX_fullpath, LIKE, fv,
                                    FILTER_FLAG_OR | FILTER_FLAG_END ); 
        }
        else /* ends with slash */
        {
            snprintf( path_regexp, RBH_PATH_MAX, "%s*", path_filter );
            /* directory or directory/% */

            fv.val_str = path_regexp;
            lmgr_simple_filter_add( filter, ATTR_INDEX_fullpath, LIKE, fv,
                                    FILTER_FLAG_BEGIN );
            /* remove last slash */
            path_filter[len-1] = '\0';
            fv.val_str = path_filter;
            lmgr_simple_filter_add( filter, ATTR_INDEX_fullpath, LIKE, fv,
                                    FILTER_FLAG_OR | FILTER_FLAG_END );
        }

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
#elif !defined(ATTR_INDEX_release_class)
        lmgr_simple_filter_add( filter, ATTR_INDEX_archive_class, LIKE, fv, 0 );
#else /* both */
        /* archive class or release class */
        lmgr_simple_filter_add( filter, ATTR_INDEX_archive_class, LIKE, fv,
                                FILTER_FLAG_BEGIN );
        lmgr_simple_filter_add( filter, ATTR_INDEX_release_class, LIKE, fv,
                                FILTER_FLAG_OR | FILTER_FLAG_END );
#endif
    }

    return 0;
}

/* return attr name to be displayed */
static inline const char * attrindex2name(unsigned int index)
{
    switch(index)
    {
        case ATTR_INDEX_fullpath: return "path";
        case ATTR_INDEX_avgsize: return "avgsize";
#ifdef ATTR_INDEX_dircount
        case ATTR_INDEX_dircount: return "dircount";
#endif
#ifdef ATTR_INDEX_status
        case ATTR_INDEX_status: return "status";
#endif
#ifdef ATTR_INDEX_type
        case ATTR_INDEX_type: return "type";
#endif
        case ATTR_INDEX_owner: return "user";
        case ATTR_INDEX_gr_name: return "group";
        case ATTR_INDEX_blocks: return "spc_used";
        case ATTR_INDEX_size: return "size";
        case ATTR_INDEX_last_access: return "last_access";
        case ATTR_INDEX_last_mod: return "last_mod";

#ifdef ATTR_INDEX_archive_class
        case ATTR_INDEX_archive_class: return "migr. class";
#endif
#ifdef ATTR_INDEX_release_class
        case ATTR_INDEX_release_class: return "purge class";
#endif
        case ATTR_INDEX_stripe_info: return "stripe_cnt, stripe_size,      pool";
        case ATTR_INDEX_stripe_items: return "ost_idx";
    }
    return "?";
}


//                "stripe_count", "stripe_size", "pool", "storage_units" );

static inline unsigned int attrindex2len(unsigned int index, int csv)
{
    switch(index)
    {
        case 0: return 10; /* count */
#ifdef ATTR_INDEX_dircount
        case ATTR_INDEX_dircount: return 8;
#endif
#ifdef  ATTR_INDEX_status
        case ATTR_INDEX_status: return 10;
#endif
#ifdef ATTR_INDEX_type
        case ATTR_INDEX_type: return 8;
#endif
        case ATTR_INDEX_fullpath: return 40;
        case ATTR_INDEX_owner: return 10;
        case ATTR_INDEX_gr_name: return 10;

        case ATTR_INDEX_last_access: return 20;
        case ATTR_INDEX_last_mod: return 20;

#ifdef ATTR_INDEX_archive_class
        case ATTR_INDEX_archive_class: return 20;
#endif
#ifdef ATTR_INDEX_release_class
        case ATTR_INDEX_release_class: return 20;
#endif

        case ATTR_INDEX_blocks:
        case ATTR_INDEX_avgsize:
        case ATTR_INDEX_size:
            if (csv)
                return 15; /* up to 999To */
            else
                return 10; /* 1024.21 GB */
        case ATTR_INDEX_stripe_info: return strlen(attrindex2name(ATTR_INDEX_stripe_info));
        case ATTR_INDEX_stripe_items: return 10;
    }
    return 1;
}

#define PROF_CNT_LEN     8
#define PROF_RATIO_LEN   7

static int list2mask(int * attr_list, int attr_count)
{
    int i, mask, tmpmask;
    mask = 0;
    for (i=0; i < attr_count; i++)
    {
        tmpmask = 1 << attr_list[i];
        mask |= tmpmask;
    }
    return mask;
}


static void print_attr_list(int rank_field, int * attr_list, int attr_count,
                            profile_field_descr_t * p_profile, int csv)
{
    int i;
    int coma = 0;

    if (rank_field)
    {
        printf("rank");
        coma = 1;
    }
    for (i=0; i < attr_count; i++)
    {
        if (coma)
            printf(", %*s", attrindex2len(attr_list[i], csv),
                   attrindex2name(attr_list[i]));
        else
        {
            printf("%*s", attrindex2len(attr_list[i], csv),
                   attrindex2name(attr_list[i]));
            coma = 1;
        }
    }
    if (p_profile)
    {
        if (p_profile->attr_index == ATTR_INDEX_size)
        {
            for (i=0; i < SZ_PROFIL_COUNT; i++)
            {
                if (coma)
                    printf(", %*s", PROF_CNT_LEN, size_range[i].title);
                else
                {
                    printf("%*s", PROF_CNT_LEN, size_range[i].title);
                    coma = 1; 
                }
            }
            if (p_profile->range_ratio_len > 0)
            {
                char tmp[128];
                char tmp1[128];
                char tmp2[128];
                if (p_profile->range_ratio_start + p_profile->range_ratio_len == SZ_PROFIL_COUNT)
                    sprintf(tmp, "ratio(%s..inf)", print_brief_sz( SZ_MIN_BY_INDEX(p_profile->range_ratio_start), tmp1));
                else
                    sprintf(tmp, "ratio(%s..%s-)", 
                            print_brief_sz( SZ_MIN_BY_INDEX(p_profile->range_ratio_start), tmp1),
                            print_brief_sz( SZ_MIN_BY_INDEX(p_profile->range_ratio_start + p_profile->range_ratio_len), tmp2));

                printf(", %*s", PROF_RATIO_LEN, tmp);
            }
        }
    }
    printf("\n");
}


static const char * attr2str(attr_set_t * attrs, const entry_id_t * id,
                int attr_index, int csv, int resolv_id, char * out)
{
    time_t tt;
    struct tm stm;
    switch(attr_index)
    {
        case ATTR_INDEX_fullpath:
            if (ATTR_MASK_TEST(attrs, fullpath))
                return ATTR(attrs, fullpath);
            else if (resolv_id)
                return ResolvName(id, attrs, out);
            else
                return "n/a"; /* TODO fid2path if possible? */
        case ATTR_INDEX_avgsize:
            if (csv)
                sprintf(out, "%"PRIu64, ATTR(attrs, avgsize));
            else
                FormatFileSize(out, 128, ATTR(attrs, avgsize));
            return out;
#ifdef ATTR_INDEX_dircount
        case ATTR_INDEX_dircount: 
            sprintf(out, "%u", ATTR(attrs, dircount));
            return out;
#endif
#ifdef ATTR_INDEX_status
        case ATTR_INDEX_status: return db_status2str(ATTR(attrs, status), csv);
#endif
#ifdef ATTR_INDEX_type
        case ATTR_INDEX_type:
             return ATTR(attrs, type);
#endif
        case ATTR_INDEX_owner: return ATTR(attrs, owner);
        case ATTR_INDEX_gr_name: return ATTR(attrs, gr_name);
        case ATTR_INDEX_blocks:
            if (csv)
                sprintf(out, "%"PRIu64, ATTR(attrs, blocks) * DEV_BSIZE);
            else
                FormatFileSize(out, 128, ATTR(attrs, blocks) * DEV_BSIZE);
            return out;

        case ATTR_INDEX_size:
            if (csv)
                sprintf(out, "%"PRIu64, ATTR(attrs, size));
            else
                FormatFileSize(out, 128, ATTR(attrs, size));
            return out;

        case ATTR_INDEX_last_access:
            tt = ATTR(attrs, last_access);
            strftime(out, 128, "%Y/%m/%d %T", localtime_r(&tt, &stm));
            return out;

        case ATTR_INDEX_last_mod:
            tt = ATTR(attrs, last_mod);
            strftime(out, 128, "%Y/%m/%d %T", localtime_r(&tt, &stm));
            return out;

#ifdef ATTR_INDEX_archive_class
        case ATTR_INDEX_archive_class:
            return migr_class(attrs);
#endif
#ifdef ATTR_INDEX_release_class
        case ATTR_INDEX_release_class:
            return release_class(attrs);
#endif
        case ATTR_INDEX_stripe_info:
            if (csv)
                sprintf(out, "%10u, %11"PRIu64", %9s",
                    ATTR( attrs, stripe_info ).stripe_count,
                    ATTR( attrs, stripe_info ).stripe_size,
                    ATTR( attrs, stripe_info ).pool_name);
            else {
                char tmp[128];
                FormatFileSize(tmp, 128, ATTR( attrs, stripe_info ).stripe_size);
                sprintf(out, "%10u, %11s, %9s",
                    ATTR( attrs, stripe_info ).stripe_count, tmp,
                    ATTR( attrs, stripe_info ).pool_name);
            }
            return out;

        case ATTR_INDEX_stripe_items:
            FormatStripeList(out, 1024, &ATTR( attrs, stripe_items ));
            return out;
    }
    return "?";
}


static void print_attr_values(int rank, int * attr_list, int attr_count,
                              attr_set_t * attrs, const entry_id_t * id,
                              int csv, char * out, int resolv_id)
{
    int i, coma = 0;
    char str[1024];
    if (rank)
    {
        printf("%4d", rank);
        coma = 1;
    }
    for (i=0; i < attr_count; i++)
    {
        if (coma)
            printf(", %*s", attrindex2len(attr_list[i], csv),
                   attr2str(attrs, id, attr_list[i], csv, resolv_id, str));
        else
        {
            printf("%*s", attrindex2len(attr_list[i], csv),
                   attr2str(attrs, id, attr_list[i], csv, resolv_id, str));
            coma = 1;
        }
    }
    printf("\n");

}


/* return attr name to be displayed */
static inline const char * attrdesc2name(const report_field_descr_t * desc)
{
    switch(desc->attr_index)
    {
        case 0:
            if (desc->report_type == REPORT_COUNT)
                return "count";
            break;
        case ATTR_INDEX_size:
            if (desc->report_type == REPORT_MIN)
                return "min_size";
            else if (desc->report_type == REPORT_MAX)
                return "max_size";
            else if (desc->report_type == REPORT_AVG)
                return "avg_size";
            else if (desc->report_type == REPORT_SUM)
                return "volume";
            else
                return "size";
        /*default: */
    }
    return attrindex2name(desc->attr_index);
}


static inline const char * result_val2str(const report_field_descr_t * desc,
                                    const db_value_t * val, int csv,
                                    char * out)
{
    out[0] = '\0';
    switch(desc->attr_index)
    {
        case 0: /* count */
            sprintf(out, "%Lu", val->value_u.val_biguint);
            break;
#ifdef  ATTR_INDEX_status
        case ATTR_INDEX_status:
            sprintf(out, db_status2str(val->value_u.val_uint,csv));
            break;
#endif
#ifdef ATTR_INDEX_type
        case ATTR_INDEX_type:
#endif
        case ATTR_INDEX_owner:
        case ATTR_INDEX_gr_name:
            strcpy(out, val->value_u.val_str);
            break;
        case ATTR_INDEX_size:
            if (csv)
                sprintf(out, "%Lu", val->value_u.val_biguint);
            else
                FormatFileSize( out, 128, val->value_u.val_biguint );
            break;
        case ATTR_INDEX_blocks:
            if (csv)
                sprintf(out, "%Lu", val->value_u.val_biguint * DEV_BSIZE);
            else
                FormatFileSize( out, 128, val->value_u.val_biguint * DEV_BSIZE);
            break;

#ifdef ATTR_INDEX_archive_class
        case ATTR_INDEX_archive_class:
            return class_format( val->value_u.val_str );
#endif
#ifdef ATTR_INDEX_release_class
        case ATTR_INDEX_release_class:
            return class_format( val->value_u.val_str );
#endif
    }
    return out;
}



/**
 * Generic fucntion to display a report
 */
static void display_report( const report_field_descr_t * descr, unsigned int field_count,
                            const db_value_t * result, unsigned int result_count,
                            const profile_field_descr_t * prof_descr, profile_u * p_prof,
                            int flags, int header, int rank )
{
    unsigned int i;

    if (!NOHEADER(flags) && header)
    {
        if (rank)
            printf("rank, ");

        printf("%-*s", attrindex2len(descr[0].attr_index, CSV(flags)),
               attrdesc2name(&descr[0]));
        for (i = 1; i < field_count && i < result_count; i++)
            if (!result || !DB_IS_NULL(&result[i]))
                printf( ", %*s", attrindex2len(descr[i].attr_index, CSV(flags)),
                        attrdesc2name(&descr[i]));
        if (prof_descr)
        {
            if (prof_descr->attr_index == ATTR_INDEX_size)
            {
                for (i=0; i < SZ_PROFIL_COUNT; i++)
                    printf(", %*s", PROF_CNT_LEN, size_range[i].title);

                if (prof_descr->range_ratio_len > 0)
                {
                    char tmp[128];
                    char tmp1[128];
                    char tmp2[128];
                    if (prof_descr->range_ratio_start + prof_descr->range_ratio_len == SZ_PROFIL_COUNT)
                        sprintf(tmp, "ratio(%s..inf)", print_brief_sz( SZ_MIN_BY_INDEX(prof_descr->range_ratio_start), tmp1));
                    else
                        sprintf(tmp, "ratio(%s..%s)", 
                                print_brief_sz( SZ_MIN_BY_INDEX(prof_descr->range_ratio_start), tmp1),
                                print_brief_sz( SZ_MIN_BY_INDEX(prof_descr->range_ratio_start + prof_descr->range_ratio_len) -1, tmp2));

                    printf(", %*s", PROF_RATIO_LEN, tmp);
                }
            }
        }

        printf("\n");
    }

    if (result)
    {
        if (rank)
            printf("%4d, ", rank);

        char tmpstr[1024];
        printf("%-*s", attrindex2len(descr[0].attr_index, CSV(flags)),
               result_val2str(&descr[0],&result[0], CSV(flags), tmpstr));
        for (i = 1; i < field_count && i < result_count; i++)
            if (!DB_IS_NULL(&result[i]))
                printf( ", %*s", attrindex2len(descr[i].attr_index, CSV(flags)),
                       result_val2str(&descr[i],&result[i], CSV(flags), tmpstr));
        if (prof_descr && p_prof)
        {
            if (prof_descr->attr_index == ATTR_INDEX_size)
            {
                uint64_t tot=0;
                uint64_t range=0;

                for (i=0; i < SZ_PROFIL_COUNT; i++)
                {
                    printf(", %*"PRIu64, PROF_CNT_LEN, p_prof->size.file_count[i]);
                    tot += p_prof->size.file_count[i];
                    if ((prof_descr->range_ratio_len > 0) &&
                        (i >= prof_descr->range_ratio_start) &&
                        (i < prof_descr->range_ratio_start + prof_descr->range_ratio_len))
                        range += p_prof->size.file_count[i];
                }

                if (prof_descr->range_ratio_len > 0)
                    printf(", %.2f%%", 100.0*range/tot);
            }
        }

        printf("\n");
    }
}



void dump_entries( type_dump type, int int_arg, char * str_arg, int flags )
{
    /* get basic information */
    int            mask_sav, rc;
    lmgr_filter_t  filter;
    filter_value_t fv;
    struct lmgr_iterator_t *it;
    attr_set_t     attrs;
    entry_id_t     id;

    unsigned long long total_size, total_count;
    total_size = total_count = 0;

    int list[] = {
                   ATTR_INDEX_type,
#ifdef ATTR_INDEX_status
                   ATTR_INDEX_status,
#endif
                   ATTR_INDEX_size,
                   ATTR_INDEX_owner,
                   ATTR_INDEX_gr_name,
#ifdef ATTR_INDEX_archive_class
                   ATTR_INDEX_archive_class,
#endif
#ifdef ATTR_INDEX_release_class
                   ATTR_INDEX_release_class,
#endif
                   ATTR_INDEX_fullpath };
    int list_cnt = sizeof(list)/sizeof(int);

    lmgr_simple_filter_init( &filter );

    /* append global filters */
    mk_global_filters( &filter, !NOHEADER(flags), NULL );

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
                if ( MATCH_NULL_STATUS( flags ) )
                    lmgr_simple_filter_add( &filter, ATTR_INDEX_status, EQUAL,
                                            fv, FILTER_FLAG_ALLOW_NULL );
                else
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
    mask_sav = attrs.attr_mask = list2mask(list, list_cnt);

    it = ListMgr_Iterator( &lmgr, &filter, NULL, NULL );

    lmgr_simple_filter_free( &filter );

    if ( it == NULL )
    {
        DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR: Could not retrieve top file size from database." );
        return;
    }

    print_attr_list(0, list, list_cnt, NULL, CSV(flags));

    while ( ( rc = ListMgr_GetNext( it, &id, &attrs ) ) == DB_SUCCESS )
    {
        char out[1024];
        total_count ++ ;
        total_size += ATTR( &attrs, size );

        print_attr_values(0, list, list_cnt, &attrs, &id,
                          CSV(flags), out, FALSE);

        ListMgr_FreeAttrs( &attrs );

        /* prepare next call */
        attrs.attr_mask = mask_sav;
    }

    ListMgr_CloseIterator( it );

    /* display summary */
    if ( !NOHEADER(flags) )
    {
        char strsz[128];
        FormatFileSize( strsz, 128, total_size );
        printf( "\nTotal: %Lu entries, %Lu bytes (%s)\n",
                total_count, total_size, strsz );
    }
}

void report_fs_info( int flags )
{
    unsigned int   result_count;
    struct lmgr_report_t *it;
    int            rc;

    lmgr_filter_t  filter;
    int is_filter = FALSE;

#ifdef _SHERPA
    filter_value_t fv;
#endif

#ifdef  ATTR_INDEX_status
#define FSINFOCOUNT 7
#define _SHIFT 1
#else
#define FSINFOCOUNT 6
#define _SHIFT 0
#endif

    db_value_t     result[FSINFOCOUNT];

    /* To be retrieved:
     * - type
     * - number of items for this type
     * - MIN/MAX/SUM size
     * - MIN/MAX/SUM dircount
     */
    report_field_descr_t fs_info[FSINFOCOUNT] = {
#ifdef  ATTR_INDEX_status
        {ATTR_INDEX_status, REPORT_GROUP_BY, SORT_ASC, FALSE, 0, {NULL}},
#endif
        {ATTR_INDEX_type, REPORT_GROUP_BY, SORT_ASC, FALSE, 0, {NULL}},
        {0, REPORT_COUNT, SORT_NONE, FALSE, 0, {NULL}},
        {ATTR_INDEX_size, REPORT_SUM, SORT_NONE, FALSE, 0, {NULL}}, /* XXX ifdef STATUS ? */
        {ATTR_INDEX_size, REPORT_MIN, SORT_NONE, FALSE, 0, {NULL}},
        {ATTR_INDEX_size, REPORT_MAX, SORT_NONE, FALSE, 0, {NULL}},
        {ATTR_INDEX_size, REPORT_AVG, SORT_NONE, FALSE, 0, {NULL}},
    };

    unsigned long long total_size, total_count;
    total_size = total_count = 0;
    lmgr_iter_opt_t opt;
    profile_u   prof;
    int display_header = 1;

    if (REVERSE(flags))
        fs_info[0].sort_flag = SORT_DESC;

    if (count_min) {
        fs_info[1+_SHIFT].filter = TRUE;
        fs_info[1+_SHIFT].filter_compar = MORETHAN;
        fs_info[1+_SHIFT].filter_value.val_biguint = count_min;
    }

    /* no limit */
    opt.list_count_max = 0;
    /* skip missing entries */
    opt.allow_no_attr = FALSE;

    /* test FORCE NO ACCT option */
    if( FORCE_NO_ACCT( flags ) )
        opt.force_no_acct = TRUE;
    else
        opt.force_no_acct = FALSE;


    /* append global filters */
    mk_global_filters( &filter, !NOHEADER(flags), &is_filter );

#ifdef _SHERPA
    /* only select file status  */
    if ( !is_filter )
        lmgr_simple_filter_init( &filter );
    fv.val_str = STR_TYPE_FILE;
    lmgr_simple_filter_add( &filter, ATTR_INDEX_type, EQUAL, fv, 0 );
    is_filter = TRUE;
#endif

    if ( is_filter )
        it = ListMgr_Report( &lmgr, fs_info, FSINFOCOUNT,
                             SPROF(flags)?&size_profile:NULL, &filter, &opt );
    else
        it = ListMgr_Report( &lmgr, fs_info, FSINFOCOUNT,
                             SPROF(flags)?&size_profile:NULL, NULL, &opt );

    if ( it == NULL )
    {
        DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR: Could not retrieve filesystem stats from database." );
        return;
    }


    result_count = FSINFOCOUNT;

    while ((rc = ListMgr_GetNextReportItem(it, result, &result_count,
                                             SPROF(flags)?&prof:NULL))
              == DB_SUCCESS )
    {
        if (result[1+_SHIFT].value_u.val_biguint == 0) /* count=0 (don't display)*/
            display_report( fs_info, FSINFOCOUNT, NULL, result_count,
                            SPROF(flags)?&size_profile:NULL, SPROF(flags)?&prof:NULL,
                            flags, display_header, 0 );
        else
            display_report( fs_info, FSINFOCOUNT, result, result_count,
                            SPROF(flags)?&size_profile:NULL, SPROF(flags)?&prof:NULL,
                            flags, display_header, 0 );
        display_header = 0; /* just display it once */

        total_count += result[1+_SHIFT].value_u.val_biguint;
        total_size += result[2+_SHIFT].value_u.val_biguint;

        /* prepare next call */
        result_count = FSINFOCOUNT;
    }

    ListMgr_CloseReport( it );

    /* display summary */
    if ( !NOHEADER(flags) )
    {
        char strsz[128];
        FormatFileSize( strsz, 128, total_size );
        printf( "\nTotal: %Lu entries, %Lu bytes (%s)\n",
                total_count, total_size, strsz );
    }
}

static inline void set_report_rec_nofilter( report_field_descr_t* ent,
                                   unsigned int   attr_index,
                                   report_type_t  report_type,
                                   sort_order_t   sort_flag )
{
    ent->attr_index = attr_index;
    ent->report_type = report_type;
    ent->sort_flag = sort_flag;
    ent->filter = FALSE;
    ent->filter_compar = 0;
}

void report_usergroup_info( char *name, int flags )
{
    unsigned int   result_count;
    struct lmgr_report_t *it;
    lmgr_filter_t  filter;
    filter_value_t fv;
    int            rc;
    unsigned int   field_count = 0;
    unsigned int   shift = 0;
    int            is_filter = FALSE;

    unsigned long long total_size, total_count;
    total_size = total_count = 0;
    lmgr_iter_opt_t opt;
    int display_header = TRUE;

#define USERINFOCOUNT_MAX 8

    db_value_t     result[USERINFOCOUNT_MAX];
    profile_u   prof;

    /* To be retrieved for each user:
     * - username
     * - number of items of each type
     * - SUM(blocks)
     * - MIN/MAX/AVG size
     */
    report_field_descr_t user_info[USERINFOCOUNT_MAX];

    if (ISSPLITUSERGROUP(flags) && ISGROUP(flags))
    {
        set_report_rec_nofilter(&user_info[field_count], ATTR_INDEX_gr_name,
                                REPORT_GROUP_BY, REVERSE(flags)?SORT_DESC:SORT_ASC );
        field_count++;
        set_report_rec_nofilter(&user_info[field_count], ATTR_INDEX_owner,
                                REPORT_GROUP_BY, REVERSE(flags)?SORT_DESC:SORT_ASC );
        field_count++;
        shift++ ;
    }
    else if (ISSPLITUSERGROUP(flags) && !ISGROUP(flags))
    {
        set_report_rec_nofilter(&user_info[field_count], ATTR_INDEX_owner,
                                REPORT_GROUP_BY, REVERSE(flags)?SORT_DESC:SORT_ASC );
        field_count++;
        set_report_rec_nofilter(&user_info[field_count], ATTR_INDEX_gr_name,
                                REPORT_GROUP_BY, REVERSE(flags)?SORT_DESC:SORT_ASC );
        field_count++;
        shift++;
    }
    else if (ISGROUP(flags))
    {
        set_report_rec_nofilter(&user_info[field_count], ATTR_INDEX_gr_name,
                                REPORT_GROUP_BY, REVERSE(flags)?SORT_DESC:SORT_ASC );
        field_count++;
    }
    else
    {
        set_report_rec_nofilter(&user_info[field_count], ATTR_INDEX_owner,
                                REPORT_GROUP_BY, REVERSE(flags)?SORT_DESC:SORT_ASC );
        field_count++;
    }

#ifdef ATTR_INDEX_type
    set_report_rec_nofilter(&user_info[field_count], ATTR_INDEX_type,
                            REPORT_GROUP_BY, REVERSE(flags)?SORT_DESC:SORT_ASC );
    field_count++;
    shift++;
#endif

    set_report_rec_nofilter(&user_info[field_count], 0, REPORT_COUNT, SORT_NONE );
    if (count_min) {
        user_info[field_count].filter = TRUE;
        user_info[field_count].filter_compar = MORETHAN;
        user_info[field_count].filter_value.val_biguint = count_min;
    }

    field_count++;
    set_report_rec_nofilter(&user_info[field_count], ATTR_INDEX_blocks, REPORT_SUM, SORT_NONE );
    field_count++;
    set_report_rec_nofilter(&user_info[field_count], ATTR_INDEX_size, REPORT_MIN, SORT_NONE );
    field_count++;
    set_report_rec_nofilter(&user_info[field_count], ATTR_INDEX_size, REPORT_MAX, SORT_NONE );
    field_count++;
    set_report_rec_nofilter(&user_info[field_count], ATTR_INDEX_size, REPORT_AVG, SORT_NONE );
    field_count++;

    /* test FORCE NO ACCT option */
    if( FORCE_NO_ACCT( flags ) )
        opt.force_no_acct = TRUE;
    else
        opt.force_no_acct = FALSE;
    /* no limit */
    opt.list_count_max = 0;
    /* skip missing entries */
    opt.allow_no_attr = FALSE;

    if ( name )
    {
        lmgr_simple_filter_init( &filter );
        is_filter = TRUE;

        fv.val_str = name;

        if ( WILDCARDS_IN( name ) )
            lmgr_simple_filter_add( &filter, (ISGROUP(flags)?ATTR_INDEX_gr_name:ATTR_INDEX_owner), LIKE, fv, 0 ); 
        else
            lmgr_simple_filter_add( &filter, (ISGROUP(flags)?ATTR_INDEX_gr_name:ATTR_INDEX_owner), EQUAL, fv, 0 ); 
    }

    /* append global filters */
    mk_global_filters( &filter, !NOHEADER(flags), &is_filter );

    it = ListMgr_Report( &lmgr, user_info, field_count,
                         SPROF(flags)?&size_profile:NULL,
                         is_filter ? &filter : NULL, &opt );

    if ( is_filter )
        lmgr_simple_filter_free( &filter );

    if ( it == NULL )
    {
        DisplayLog( LVL_CRIT, REPORT_TAG, "ERROR: Could not retrieve user stats from database." );
        return;
    }

    result_count = field_count;

    while ( ( rc = ListMgr_GetNextReportItem( it, result, &result_count,
                                              SPROF(flags)?&prof:NULL ) ) == DB_SUCCESS )
    {
        result_count = field_count;
        display_report( user_info, result_count, result, result_count,
                        SPROF(flags)?&size_profile:NULL, SPROF(flags)?&prof:NULL,
                        flags, display_header, 0 );
        display_header = 0; /* just display it once */

        total_count += result[1+shift].value_u.val_biguint;
        total_size += (result[2+shift].value_u.val_biguint * DEV_BSIZE);
    }

    ListMgr_CloseReport( it );

    /* display summary */
    if ( !NOHEADER(flags) )
    {
        char strsz[128];
        FormatFileSize( strsz, 128, total_size );
        printf( "\nTotal: %Lu entries, %Lu bytes used (%s)\n",
                total_count, total_size, strsz );
    }

}

#ifdef ATTR_INDEX_dircount
void report_topdirs( unsigned int count, int flags )
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

    int list[] = { ATTR_INDEX_fullpath,
                   ATTR_INDEX_dircount,
                   ATTR_INDEX_avgsize,
                   ATTR_INDEX_owner,
                   ATTR_INDEX_gr_name,
                   ATTR_INDEX_last_mod };
    int list_cnt = sizeof(list)/sizeof(int);

    /* select only directories */
    lmgr_simple_filter_init( &filter );

    /* This filter is implicit when sorting dirs by count */
//    fv.val_str = STR_TYPE_DIR;
//    lmgr_simple_filter_add( &filter, ATTR_INDEX_type, EQUAL, fv, 0 );

    if (count_min) {
        /* @TODO Not supported by ListMgr yet */
        fv.val_biguint = count_min;
        lmgr_simple_filter_add( &filter, ATTR_INDEX_dircount, MORETHAN, fv, 0 );
    }

    /* append global filters */
    mk_global_filters( &filter, !NOHEADER(flags), NULL );

    if (SORT_BY_AVGSIZE(flags))
        sorttype.attr_index = ATTR_INDEX_avgsize;
    else if (!SORT_BY_SZRATIO(flags)) /* sort by count (default) */
        /* default: order by dircount */
        sorttype.attr_index = ATTR_INDEX_dircount;
    else {
        /* SORT_BY_SZRATIO? */
        DisplayLog( LVL_MAJOR, REPORT_TAG,
                   "WARNING: sorting directories by size-ratio is not supported" );
        sorttype.attr_index = ATTR_INDEX_dircount; /* keep the default */
    }

    sorttype.order = REVERSE(flags)?SORT_ASC:SORT_DESC;

    /* select only the top dirs */
    opt.list_count_max = count;
    opt.force_no_acct = FALSE;
    /* allow missing entries */
    opt.allow_no_attr = TRUE;

    ATTR_MASK_INIT( &attrs );
    mask_sav = attrs.attr_mask = list2mask(list, list_cnt);

    it = ListMgr_Iterator( &lmgr, &filter, &sorttype, &opt );

    lmgr_simple_filter_free( &filter );

    if ( it == NULL )
    {
        DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR: Could not retrieve top directories from database." );
        return;
    }

    print_attr_list(1, list, list_cnt, NULL, CSV(flags));

    index = 0;
    while ( ( rc = ListMgr_GetNext( it, &id, &attrs ) ) == DB_SUCCESS )
    {
        char   out[1024];

        index++;
        /* resolv id for dir requests */
        print_attr_values(index, list, list_cnt, &attrs, &id,
                          CSV(flags), out, TRUE);

        ListMgr_FreeAttrs( &attrs );

        /* prepare next call */
        attrs.attr_mask = mask_sav;
    }
    ListMgr_CloseIterator( it );
}
#endif

void report_topsize( unsigned int count, int flags )
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

    int list[] = { ATTR_INDEX_fullpath,
#ifdef ATTR_INDEX_status
                   ATTR_INDEX_status,
#endif
                   ATTR_INDEX_size,
                   ATTR_INDEX_owner,
                   ATTR_INDEX_gr_name,
                   ATTR_INDEX_last_access,
                   ATTR_INDEX_last_mod,
#ifdef ATTR_INDEX_archive_class
                   ATTR_INDEX_archive_class,
#endif
#ifdef ATTR_INDEX_release_class
                   ATTR_INDEX_release_class,
#endif
                   ATTR_INDEX_stripe_info,
                   ATTR_INDEX_stripe_items
                };
    int list_cnt = sizeof(list)/sizeof(int);

    /* select only files */
    fv.val_str = STR_TYPE_FILE;
    lmgr_simple_filter_init( &filter );
    lmgr_simple_filter_add( &filter, ATTR_INDEX_type, EQUAL, fv, 0 );

    /* append global filters */
    mk_global_filters( &filter, !NOHEADER(flags), NULL );

    /* order by size desc */
    sorttype.attr_index = ATTR_INDEX_size;
    sorttype.order = REVERSE(flags)?SORT_ASC:SORT_DESC;

    /* select only the top size */
    opt.list_count_max = count;
    opt.force_no_acct = FALSE;
    /* skip missing entries */
    opt.allow_no_attr = FALSE;

    ATTR_MASK_INIT( &attrs );
    mask_sav = attrs.attr_mask = list2mask(list, list_cnt);

    it = ListMgr_Iterator( &lmgr, &filter, &sorttype, &opt );

    lmgr_simple_filter_free( &filter );

    if ( it == NULL )
    {
        DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR: Could not retrieve top file size from database." );
        return;
    }

    print_attr_list(1, list, list_cnt, NULL, CSV(flags));

    index = 0;
    while ( ( rc = ListMgr_GetNext( it, &id, &attrs ) ) == DB_SUCCESS )
    {
        char out[1024];
        index++;
        print_attr_values(index, list, list_cnt, &attrs, &id,
                          CSV(flags), out, FALSE);

        ListMgr_FreeAttrs( &attrs );
        /* prepare next call */
        attrs.attr_mask = mask_sav;
    }

    ListMgr_CloseIterator( it );
}


void report_toppurge( unsigned int count, int flags )
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
    char           out[128];

    int list[] = {
                ATTR_INDEX_fullpath,
                ATTR_INDEX_type,
                ATTR_INDEX_last_access,
                ATTR_INDEX_last_mod,
                ATTR_INDEX_size,
                ATTR_INDEX_blocks,
                ATTR_INDEX_stripe_info,
                ATTR_INDEX_stripe_items
                };
    int list_cnt = sizeof(list)/sizeof(int);

    lmgr_simple_filter_init( &filter );

    /* select only non directories */
    fv.val_str = STR_TYPE_DIR;
    lmgr_simple_filter_add( &filter, ATTR_INDEX_type, NOTEQUAL, fv, 0 );

    /* append global filters */
    mk_global_filters( &filter, !NOHEADER(flags), NULL );

    /* select only non whitelisted */
#ifdef ATTR_INDEX_whitelisted
    fv.val_bool = TRUE;
    lmgr_simple_filter_add( &filter, ATTR_INDEX_whitelisted, NOTEQUAL, fv, 0);
#elif defined(ATTR_INDEX_no_release)
    fv.val_bool = TRUE;
    lmgr_simple_filter_add( &filter, ATTR_INDEX_no_release, NOTEQUAL, fv, 0);
#endif

#ifdef ATTR_INDEX_status
    fv.val_int = STATUS_SYNCHRO;
    lmgr_simple_filter_add( &filter, ATTR_INDEX_status, EQUAL, fv, 0);
#endif

#ifdef ATTR_INDEX_invalid
    /* select only non invalid */
    fv.val_bool = TRUE;
    lmgr_simple_filter_add( &filter, ATTR_INDEX_invalid, NOTEQUAL, fv, 0);
#endif

    sorttype.attr_index = ATTR_INDEX_last_access;
    sorttype.order = REVERSE(flags)?SORT_DESC:SORT_ASC;

    /* select only the top size */
    opt.list_count_max = count;
    opt.force_no_acct = FALSE;
    /* skip missing entries */
    opt.allow_no_attr = FALSE;

    ATTR_MASK_INIT( &attrs );
    mask_sav = attrs.attr_mask = list2mask(list, list_cnt);

    it = ListMgr_Iterator( &lmgr, &filter, &sorttype, &opt );

    lmgr_simple_filter_free( &filter );

    if ( it == NULL )
    {
        DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR: Could not retrieve top purge list from database." );
        return;
    }

    print_attr_list(1, list, list_cnt, NULL, CSV(flags));

    index = 0;
    while ( ( rc = ListMgr_GetNext( it, &id, &attrs ) ) == DB_SUCCESS )
    {
        index++;

        print_attr_values(index, list, list_cnt, &attrs, &id,
                          CSV(flags), out, FALSE);
        ListMgr_FreeAttrs( &attrs );

        /* prepare next call */
        attrs.attr_mask = mask_sav;
    }

    ListMgr_CloseIterator( it );

}

#ifdef HAVE_RMDIR_POLICY
void report_toprmdir( unsigned int count, int flags )
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
    lmgr_simple_filter_add( &filter, ATTR_INDEX_whitelisted, NOTEQUAL, fv, FILTER_FLAG_ALLOW_NULL);

    /* select only non invalid */
    fv.val_bool = TRUE;
    lmgr_simple_filter_add( &filter, ATTR_INDEX_invalid, NOTEQUAL, fv, FILTER_FLAG_ALLOW_NULL);

    /* only consider empty directories */
    fv.val_uint = 0;
    rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_dircount, EQUAL, fv, 0);

    mk_global_filters( &filter, !NOHEADER(flags), NULL );

    /* order by last_mod asc */
    sorttype.attr_index = ATTR_INDEX_last_mod;
    sorttype.order = REVERSE(flags)?SORT_DESC:SORT_ASC;

    /* select only the top dirs */
    opt.list_count_max = count;
    opt.force_no_acct = FALSE;
    /* allow missing entries */
    opt.allow_no_attr = TRUE;

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

    // @TODO replace: print_attr_mask(1, mask_sav, list, list_cnt, CSV(flags));

    if ( CSV(flags) && !NOHEADER(flags) )
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
        else if ( CSV(flags) )
            sprintf( dur, "%u",
                     ( unsigned int ) ( ATTR( &attrs, last_mod ) - time( NULL ) +
                                        policies.rmdir_policy.age_rm_empty_dirs ) );
        else
            FormatDurationFloat( dur, 128,
                                 ATTR( &attrs,
                                       last_mod ) - time( NULL ) +
                                 policies.rmdir_policy.age_rm_empty_dirs );


        if ( CSV(flags) )
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

void report_topuser( unsigned int count, int flags )
{
    unsigned int   result_count;
    struct lmgr_report_t *it;
    lmgr_iter_opt_t opt;
    int            rc;
    unsigned int   rank = 1;
    lmgr_filter_t  filter;
    int is_filter = FALSE;
    profile_u   prof;

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

    if (REVERSE(flags))
        user_info[1].sort_flag = SORT_ASC;

    if (SORT_BY_COUNT(flags)) {
        /* replace sort on blocks by sort on count */
        user_info[1].sort_flag = SORT_NONE;
        user_info[2].sort_flag = REVERSE(flags)?SORT_ASC:SORT_DESC;
    } else if (SORT_BY_AVGSIZE(flags)) {
        /* sort (big files first) */
        user_info[1].sort_flag = SORT_NONE;
        user_info[5].sort_flag = REVERSE(flags)?SORT_ASC:SORT_DESC;
    }

    if (count_min) {
        user_info[2].filter = TRUE;
        user_info[2].filter_compar = MORETHAN;
        user_info[2].filter_value.val_biguint = count_min;
    }

    /* select only the top users */
    opt.list_count_max = count;
    opt.force_no_acct = FALSE;
    /* skip missing entries */
    opt.allow_no_attr = FALSE;

    /* test FORCE NO ACCT option */
    if( FORCE_NO_ACCT( flags ) )
        opt.force_no_acct = TRUE;
    else
        opt.force_no_acct = FALSE;

    is_filter = FALSE;

    mk_global_filters( &filter, !NOHEADER(flags), &is_filter );

    /* is a filter specified? */
    if ( is_filter )
        it = ListMgr_Report( &lmgr, user_info, TOPUSERCOUNT,
                             SPROF(flags)?&size_profile:NULL, &filter, &opt );
    else
        it = ListMgr_Report( &lmgr, user_info, TOPUSERCOUNT,
                             SPROF(flags)?&size_profile:NULL, NULL, &opt );


    if ( it == NULL )
    {
        DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR: Could not retrieve top space consumers from database." );
        return;
    }

    result_count = TOPUSERCOUNT;
    while ( ( rc = ListMgr_GetNextReportItem( it, result, &result_count,
                              SPROF(flags)?&prof:NULL ) ) == DB_SUCCESS )
    {
        display_report( user_info, result_count, result, result_count,
                        SPROF(flags)?&size_profile:NULL, SPROF(flags)?&prof:NULL,
                        flags, rank == 1, rank ); /* display header once */

        rank++;

        /* prepare next call */
        result_count = TOPUSERCOUNT;

    }

    ListMgr_CloseReport( it );


}

#ifdef HAVE_RM_POLICY
void report_deferred_rm( int flags )
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
    char           date_rm[128];
    char           date_exp[128];
    struct tm      t;

    unsigned long long total_count = 0;
    lmgr_filter_t  filter;
    int is_filter = FALSE;

    lmgr_simple_filter_init( &filter );

    /* append global filters */
    mk_global_filters( &filter, !NOHEADER(flags), &is_filter );

    /* list all deferred rm, even if non expired */
    list = ListMgr_RmList( &lmgr, FALSE, is_filter? &filter : NULL );

    if ( list == NULL )
    {
        DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR: Could not retrieve removed entries from database." );
        return;
    }

    if ( CSV(flags) && !NOHEADER(flags) )
#ifdef _HSM_LITE
        printf( "%3s, %21s, %-40s, %19s, %19s, %s\n", "rank", "fid",
                "last_known_path", "lustre_rm", "hsm_rm", "backend_path" );
#else
        printf( "%3s, %21s, %-40s, %19s, %19s\n", "rank", "fid", "last_known_path", "lustre_rm", "hsm_rm" );
#endif

    index = 0;
    while ( ( rc = ListMgr_GetNextRmEntry( list, &id, last_known_path,
#ifdef _HSM_LITE
                        bkpath,
#endif
                        &soft_rm_time, &expiration_time )) == DB_SUCCESS )
    {
        total_count++;

        index++;
        /* format last mod */
        strftime( date_rm, 128, "%Y/%m/%d %T", localtime_r( &soft_rm_time, &t ) );
        strftime( date_exp, 128, "%Y/%m/%d %T", localtime_r( &expiration_time, &t ) );

        if ( CSV(flags) )
#ifdef _HSM_LITE
            printf( "%3u, "DFID", %-40s, %19s, %19s, %s\n", index, PFID(&id),
                    last_known_path, date_rm, date_exp, bkpath );
#else
            printf( "%3u, "DFID", %-40s, %19s, %19s\n", index, PFID(&id),
                    last_known_path, date_rm, date_exp );
#endif
        else
        {
            printf( "\n" );
            printf( "Rank:              %u\n", index );
            printf( "Fid:               "DFID"\n", PFID(&id) );
            if ( !EMPTY_STRING(last_known_path) )
                printf( "Last known path:   %s\n", last_known_path );
#ifdef _HSM_LITE
            if ( !EMPTY_STRING(bkpath) )
                printf( "Backend path:      %s\n", bkpath );
#endif
            printf( "Lustre rm time:    %s\n", date_rm );
            if ( expiration_time <= time(NULL) )
                printf( "HSM rm time:       %s (expired)\n", date_exp );
            else
                printf( "HSM rm time:       %s\n", date_exp );
        }

        /* prepare next call */
        last_known_path[0] = '\0';
#ifdef _HSM_LITE
        bkpath[0] = '\0';
#endif
        soft_rm_time = 0;
        expiration_time = 0;
    }

    ListMgr_CloseRmList(list);

    /* display summary */
    if ( !NOHEADER(flags) )
        printf( "\nTotal: %Lu entries\n", total_count );

}
#endif


#ifdef ATTR_INDEX_archive_class

#ifdef _LUSTRE_HSM
    #define IS_PURGE_CONCERNED( _status ) ( ((_status) == STATUS_SYNCHRO)  \
                                     || ((_status) == STATUS_RELEASED) \
                                     || ((_status) == STATUS_RELEASE_PENDING) )
    #define IS_MODIFIED( _status )  ( ((_status) == STATUS_MODIFIED) \
                                      || ((_status) == STATUS_NEW))
#elif defined(_SHERPA)
    #define IS_PURGE_CONCERNED( _status ) ( (_status) == STATUS_SYNCHRO )
    #define IS_MODIFIED( _status )  ((_status) == STATUS_MODIFIED)
#elif defined(_HSM_LITE)
    #ifdef HAVE_PURGE_POLICY
    #define IS_PURGE_CONCERNED( _status ) ( ((_status) == STATUS_SYNCHRO)  \
                                     || ((_status) == STATUS_RELEASED) \
                                     || ((_status) == STATUS_RELEASE_PENDING) )
    #else
    #define IS_PURGE_CONCERNED( _status ) FALSE
    #endif
    #define IS_MODIFIED( _status )  (((_status) == STATUS_MODIFIED)|| \
                                     ((_status) == STATUS_NEW))
#endif


/* used for grouping archive/release classes depending on entry status */

struct class_record {
    char class[1024];
    unsigned long long count;
    unsigned long long nb_modif;
    unsigned long long nb_synchro;
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

static inline int class_add( const char * name, db_value_t * res_array, int shift )
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
   p_curr->count += res_array[shift+1].value_u.val_biguint;
   p_curr->spc_used += (res_array[shift+2].value_u.val_biguint * DEV_BSIZE);

   if IS_MODIFIED(res_array[shift].value_u.val_uint)
   {
        p_curr->nb_modif += res_array[shift+1].value_u.val_biguint;
        p_curr->spc_modif += (res_array[shift+2].value_u.val_biguint * DEV_BSIZE);
   }
   else if ( res_array[shift].value_u.val_uint == STATUS_SYNCHRO )
   {
        p_curr->nb_synchro += res_array[shift+1].value_u.val_biguint;
        p_curr->spc_synchro += (res_array[shift+2].value_u.val_biguint * DEV_BSIZE);
   }

   /* size min */
   if ( res_array[shift+3].value_u.val_biguint < p_curr->size_min )
      p_curr->size_min = res_array[shift+3].value_u.val_biguint;
   if ( res_array[shift+4].value_u.val_biguint > p_curr->size_max )
      p_curr->size_max = res_array[shift+4].value_u.val_biguint;

   return 0;
} /* end of helper defintion */

#endif

static void report_class_info( int flags )
{
#if defined(ATTR_INDEX_archive_class) && defined(ATTR_INDEX_release_class)
    #define CLASSINFO_FIELDS 8
    struct class_record * p_curr;
#elif defined(ATTR_INDEX_archive_class)
    #define CLASSINFO_FIELDS 7
    struct class_record * p_curr;
#else
    #define CLASSINFO_FIELDS 6
#endif
    db_value_t     result[CLASSINFO_FIELDS];

    struct lmgr_report_t *it;
    lmgr_filter_t  filter;
    int            rc;
    profile_u prof;
#ifndef ATTR_INDEX_archive_class
    int header = 1;
#endif
    unsigned int   result_count;

    unsigned long long total_size, total_count;
    total_size = total_count = 0;

    /* no filter on type */
    filter_value_t fv;

    /* To be retrieved for each group:
     * - class names and status
     * - NB entries
     * - SUM(blocks)
     * - MIN/MAX/AVG file size
     */
    report_field_descr_t class_info[CLASSINFO_FIELDS] = {
#ifdef ATTR_INDEX_release_class
        {ATTR_INDEX_release_class, REPORT_GROUP_BY, SORT_ASC, FALSE, 0, {NULL}},
#endif
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

    /* don't select dirs for policies */
    fv.val_str = STR_TYPE_DIR;
    lmgr_simple_filter_init( &filter );
    lmgr_simple_filter_add( &filter, ATTR_INDEX_type, NOTEQUAL, fv, 0 );

    mk_global_filters( &filter, !NOHEADER(flags), NULL );

    result_count = CLASSINFO_FIELDS;

    /* is a filter specified? */
    it = ListMgr_Report( &lmgr, class_info, CLASSINFO_FIELDS,
                         SPROF(flags)?&size_profile:NULL, &filter, NULL );

    if ( it == NULL )
    {
        DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR: Could not retrieve class information from database." );
        return;
    }

#ifndef ATTR_INDEX_archive_class

    /* a single class column (release), can print as is */
    header = !NOHEADER(flags);

    result_count = CLASSINFO_FIELDS;
    while ( ( rc = ListMgr_GetNextReportItem( it, result, &result_count, SPROF(flags)?&prof:NULL ))
            == DB_SUCCESS )
    {
        display_report( class_info, result_count, result, result_count,
                        SPROF(flags)?&size_profile:NULL, SPROF(flags)?&prof:NULL,
                        flags, header, 0);
        header = 0; /* display header once */

        total_count += result[1].value_u.val_biguint;
        total_size += result[2].value_u.val_biguint * DEV_BSIZE;
        result_count = CLASSINFO_FIELDS;
    }

#else

    while ( ( rc = ListMgr_GetNextReportItem( it, result, &result_count, NULL ) )
            == DB_SUCCESS )
    {

        /* what class do we display? */
        const char * class;
        int shift;
        #if defined( ATTR_INDEX_archive_class ) && defined( ATTR_INDEX_release_class )
        shift = 2;
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
        #elif defined(ATTR_INDEX_archive_class)
        /* archive class only */
        shift = 1;
        class = result[0].value_u.val_str;
        #endif
        class = class_format(class);

        class_add( class, result, shift );
    }

    if ( CSV(flags) && !NOHEADER(flags) )
        printf( "%20s, %10s, %10s, %10s, %15s, %15s, %15s, %15s, %15s\n",
                "fileclass", "count", "nb_modif", "nb_synchro",
                "spc_used", "spc_modif", "spc_synchro","size_min", "size_max" );
    for ( p_curr = rec_list; p_curr != NULL; p_curr = p_curr->p_next )
    {
        total_count += p_curr->count;
        total_size +=  p_curr->spc_used;

        if (CSV(flags))
            printf( "%20s, %10Lu, %10Lu, %10Lu, %15Lu, %15Lu, %15Lu, %15Lu, %15Lu\n",
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
            printf( "Nb entries:   %15Lu", p_curr->count );
            printf( "    (%Lu modified, %Lu synchro)\n", p_curr->nb_modif,
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

    /* display summary */
    if ( !NOHEADER(flags) )
    {
        char strsz[128];
        FormatFileSize( strsz, 128, total_size );
        printf( "\nTotal: %Lu entries, %Lu bytes used (%s)\n",
                total_count, total_size, strsz );
    }
}

void maintenance_get( int flags )
{
    char           value[1024];
    time_t         timestamp;
    char           date[128];
    struct tm      t;
    int            rc;

    rc = ListMgr_GetVar( &lmgr, NEXT_MAINT_VAR, value );
    if ( rc == DB_SUCCESS )
    {
        timestamp = atoi( value );
        strftime( date, 128, "%Y/%m/%d %T", localtime_r( &timestamp, &t ) );
        if ( time(NULL) >= timestamp )
        {
            if ( CSV(flags) )
                printf( "next_maintenance, %s (in the past: no effect)\n", date );
            else
                printf( "Next maintenance: %s (in the past: no effect)\n", date );
        }
        else
        {
            if ( CSV(flags) )
                printf( "next_maintenance, %s\n", date );
            else
                printf( "Next maintenance: %s\n", date );
        }
    }
    else if ( rc == DB_NOT_EXISTS )
    {
        if ( CSV(flags) )
            printf( "next_maintenance, none\n" );
        else
            printf( "No maintenance is planned\n" );
    }
    else
    {
        DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR retrieving variable " NEXT_MAINT_VAR " from database" );
    }
}

void maintenance_set( int flags, time_t when )
{
    char           value[1024];
    int            rc;

    if (when == 0)
    {
        rc = ListMgr_SetVar( &lmgr, NEXT_MAINT_VAR, NULL );

        if (rc)
            DisplayLog( LVL_CRIT, REPORT_TAG,
                        "ERROR deleting variable " NEXT_MAINT_VAR " in database" );
        else
            DisplayLog( LVL_EVENT, REPORT_TAG, "Next maintenance time has been cleared successfully" );

        return;
    }

    sprintf( value, "%u", (unsigned int)when );

    rc = ListMgr_SetVar( &lmgr, NEXT_MAINT_VAR, value );
    if ( rc == DB_SUCCESS )
    {
        DisplayLog( LVL_EVENT, REPORT_TAG, "Next maintenance time has been set successfully" );
    }
    else
    {
        DisplayLog( LVL_CRIT, REPORT_TAG,
                    "ERROR setting variable " NEXT_MAINT_VAR " in database" );
    }
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

    int            force_log_level = FALSE;
    int            log_level = 0;

    int            activity = FALSE;
    int            fs_info = FALSE;

    int            user_info = FALSE;
    char           user_name[256] = "";

    int            group_info = FALSE;
    char           group_name[256] = "";

    int            class_info = FALSE;

#ifdef ATTR_INDEX_dircount
    int            topdirs = 0;
#endif
    int            topsize = 0;
    int            toppurge = 0;
    int            toprmdir = 0;
    int            topuser = 0;
#ifdef HAVE_RM_POLICY
    int            deferred_rm = 0;
#endif

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

#ifdef HAVE_MIGR_POLICY
    time_t         next_maint = 0;
    int            get_next_maint = FALSE;
    int            cancel_next_maint = FALSE;
#endif

    int            flags = 0;

    int            rc;
    char           err_msg[4096];
    robinhood_config_t config;
    int chgd = 0;

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
                strncpy( path_filter, optarg, RBH_PATH_MAX );
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
            fprintf(stderr, "Warning: 'dump' command is deprecated. Use \"rbh-find\" instead.\n");
            break;

        case OPT_DUMP_USER:
            dump_user = TRUE;
            fprintf(stderr, "Warning: 'dump-user' command is deprecated. Use \"rbh-find -user\" instead.\n");
            if ( !optarg )
            {
                fprintf(stderr, "Missing mandatory argument <username> for --dump-user\n");
                exit(1);
            }
            strncpy( dump_user_name, optarg, 256 );
            break; 

        case OPT_DUMP_GROUP:
            dump_group = TRUE;
            fprintf(stderr, "Warning: 'dump-group' command is deprecated. Use \"rbh-find -group\" instead.\n");
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
            fprintf(stderr, "Warning: 'dump-ost' command is deprecated. Use \"rbh-find -ost\" instead.\n");
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
            fprintf(stderr, "Warning: 'dump-status' command is deprecated. Use \"rbh-find -status\" instead.\n");
            if ( !optarg )
            {
                fprintf(stderr, "Missing mandatory argument <status> for --dump-status\n");
                exit(1);
            }

            status_to_dump = status2dbval( optarg );
            if ( status_to_dump == (file_status_t)-1 )
            {
                fprintf(stderr, "Unknown status '%s'. Allowed status: %s.\n", optarg,
                        allowed_status());
                exit(1);
            }

            /* match NULL value for unknown status */
            if ( status_to_dump == STATUS_UNKNOWN )
                flags |= OPT_FLAG_MATCH_NULL_STATUS;

            break;
#endif

#ifdef ATTR_INDEX_dircount
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

        case OPT_TOPRMDIR:
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

#ifdef HAVE_RM_POLICY
        case 'R':
            deferred_rm = TRUE;
            break;
#endif

#ifdef HAVE_MIGR_POLICY
        case CLEAR_NEXT_MAINT:
            cancel_next_maint = TRUE;
            get_next_maint = TRUE;
            break;
        case SET_NEXT_MAINT:
            if ( optarg )       /* optional argument */
            {
                /* parse date/time yyyymmddHHMM[SS] */
                next_maint = str2date(optarg);
                if (next_maint == (time_t)-1) {
                    fprintf( stderr,
                             "Invalid date format: yyyymmdd[HH[MM[SS]]] expected\n" );
                    exit(1);
                }
            }
            /* in all cases, display next maintenance time */
            get_next_maint = TRUE;
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

        case 'r':
            flags |= OPT_FLAG_REVERSE;
            break;
        case 'c':
            flags |= OPT_FLAG_CSV;
            break;
        case 'q':
            flags |= OPT_FLAG_NOHEADER;
            break;
        case 'h':
            display_help( bin );
            exit( 0 );
            break;
        case 'V':
            display_version( bin );
            exit( 0 );
            break;
        case 'F':
            flags |= OPT_FLAG_NO_ACCT;
            break;
        case 'S':
            flags |= OPT_FLAG_SPLITUSERGROUP;
            break;
        case OPT_BY_COUNT:
            flags |= OPT_FLAG_BY_COUNT;
            break;
        case OPT_BY_AVGSIZE:
            flags |= OPT_FLAG_BY_AVGSIZE;
            break;
        case OPT_BY_SZ_RATIO:
            flags |= OPT_FLAG_BY_SZRATIO;
            /* auto-enable size profiling */
            flags |= OPT_FLAG_SPROF;
            /* parse range */
            if (parse_size_range(optarg, &size_profile))
                exit(1);
            break;
        case OPT_COUNT_MIN:
            count_min = atoi(optarg);
            break;

        case OPT_SIZE_PROFILE:
            flags |= OPT_FLAG_SPROF;
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

    /* if a size range was specified, determine sort order:
        default DESC, ASC for reverse */
    if (size_profile.range_ratio_len > 0)
        size_profile.range_ratio_sort = REVERSE(flags)?SORT_ASC:SORT_DESC;

    if ( !activity && !fs_info && !user_info && !group_info
         && !topsize && !toppurge && !topuser && !dump_all
         && !dump_user && !dump_group && !class_info
#ifdef ATTR_INDEX_dircount
         && !topdirs
#endif
#ifdef ATTR_INDEX_status
         && !dump_status
#endif
#ifdef HAVE_RM_POLICY
        && !deferred_rm
#endif
#ifdef HAVE_RMDIR_POLICY
         && !toprmdir
#endif
#ifdef _LUSTRE
        && !dump_ost
#endif
#ifdef HAVE_MIGR_POLICY
        && !next_maint && !get_next_maint && !cancel_next_maint
#endif
        )
    {
        display_help( bin );
        exit( 1 );
    }

    /* get default config file, if not specified */
    if ( SearchConfig( config_file, config_file, &chgd ) != 0 )
    {
        fprintf(stderr, "No config file found in '/etc/robinhood.d/"PURPOSE_EXT"', ...\n" );
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

    /* set global configuration */
    global_config = config.global_config;

    /* set policies info */
    policies = config.policies;

    if ( force_log_level )
        config.log_config.debug_level = log_level;
    else
        config.log_config.debug_level = LVL_MAJOR; /* no event message */

    /* XXX set logging to stderr */
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
    rc = ListMgr_Init( &config.lmgr_config, TRUE );
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
        report_activity( flags );

    if ( fs_info )
        report_fs_info( flags );

    if ( user_info )
        report_usergroup_info( ( EMPTY_STRING( user_name ) ? NULL : user_name ),
                               flags );

    if ( group_info )
        report_usergroup_info( ( EMPTY_STRING( group_name ) ? NULL : group_name ),
                               flags | OPT_FLAG_GROUP );

    if ( class_info )
        report_class_info(flags);

#ifdef ATTR_INDEX_dircount
    if ( topdirs )
        report_topdirs( topdirs, flags );
#endif

    if ( topsize )
        report_topsize( topsize, flags );

    if ( toppurge )
        report_toppurge( toppurge, flags );

#ifdef HAVE_RMDIR_POLICY
    if ( toprmdir )
        report_toprmdir( toprmdir, flags );
#endif

    if ( topuser )
        report_topuser( topuser, flags );

#ifdef HAVE_RM_POLICY
    if ( deferred_rm )
        report_deferred_rm( flags );
#endif

    if ( dump_all )
        dump_entries( DUMP_ALL, 0, NULL, flags );

    if ( dump_user )
        dump_entries( DUMP_USR, 0, dump_user_name, flags );

    if ( dump_group )
        dump_entries( DUMP_GROUP, 0, dump_group_name, flags );

#ifdef _LUSTRE
    if ( dump_ost )
        dump_entries( DUMP_OST, dump_ost_index, NULL, flags );
#endif

#ifdef ATTR_INDEX_status
    if ( dump_status )
        dump_entries( DUMP_STATUS, status_to_dump, NULL, flags );
#endif

#ifdef HAVE_MIGR_POLICY
    if ( cancel_next_maint )
        maintenance_set( flags, 0 );

    if ( next_maint != 0 )
        maintenance_set( flags, next_maint );

    if ( get_next_maint )
        maintenance_get(flags);
#endif

    ListMgr_CloseAccess( &lmgr );

    return 0;                   /* for compiler */

}
