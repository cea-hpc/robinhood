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
 * Du clone based on robinhood DB.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "list_mgr.h"
#include "cmd_helpers.h"
#include "RobinhoodConfig.h"
#include "RobinhoodLogs.h"
#include "RobinhoodMisc.h"
#include "Memory.h"
#include "xplatform_print.h"

#include <unistd.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <libgen.h>
#include <pthread.h>

#define DU_TAG "du"

static struct option option_tab[] =
{
    {"user", required_argument, NULL, 'u'},
    {"group", required_argument, NULL, 'g'},
    {"type", required_argument, NULL, 't'},
#ifdef ATTR_INDEX_status
    {"status", required_argument, NULL, 'S'},
#endif

    /* config file options */
    {"config-file", required_argument, NULL, 'f'},

    /* log options */
    {"log-level", required_argument, NULL, 'l'},

    /* miscellaneous options */
    {"help", no_argument, NULL, 'h'},
    {"version", no_argument, NULL, 'V'},

    {NULL, 0, NULL, 0}

};

#define SHORT_OPT_STRING    "u:g:t:S:f:l:hV"
#define TYPE_HELP "'f' (file), 'd' (dir), 'l' (symlink), 'b' (block), 'c' (char), 'p' (named pipe/FIFO), 's' (socket)"

/* global variables */

static lmgr_t  lmgr;
robinhood_config_t config;

/* program options */
struct du_opt
{
    const char * user;
    const char * group;
    const char * type;
#ifdef ATTR_INDEX_status
    file_status_t status;
#endif

    /* condition flags */
    unsigned int match_user:1;
    unsigned int match_group:1;
    unsigned int match_type:1;
#ifdef ATTR_INDEX_status
    unsigned int match_status:1;
#endif

    /* behavior flags */
//    unsigned int no_dir:1; /* if -t != dir => no dir to be displayed */
//    unsigned int dir_only:1; /* if -t dir => only display dir */
} prog_options = {
    .user = NULL, .group = NULL, .type = NULL,
#ifdef ATTR_INDEX_status
    .status = STATUS_UNKNOWN,
#endif
    .match_user = 0, .match_group = 0, .match_type = 0,
#ifdef ATTR_INDEX_status
    .match_status = 0,
#endif
 //   .no_dir = 0, .dir_only = 0
};


/* filter on entries to be summed */
static lmgr_filter_t    entry_filter;

/* filter for root entries */
static bool_node_t      match_expr;
static int              is_expr = 0; /* is it set? */

static int disp_mask = ATTR_MASK_fullpath | ATTR_MASK_type | ATTR_MASK_blocks | ATTR_MASK_size;
static int query_mask = 0;

typedef struct stats_du_t
{
    const char * type;
    uint64_t     count;
    uint64_t     blocks;
    uint64_t     size;
} stats_du_t;

static const stats_du_t  stats_zero[TYPE_SOCK+1] = {
    {"?", 0, 0, 0},
    {STR_TYPE_LINK, 0, 0, 0},
    {STR_TYPE_DIR, 0, 0, 0},
    {STR_TYPE_FILE, 0, 0, 0},
    {STR_TYPE_CHR, 0, 0, 0},
    {STR_TYPE_BLK, 0, 0, 0},
    {STR_TYPE_FIFO, 0, 0, 0},
    {STR_TYPE_SOCK, 0, 0, 0}
};
static stats_du_t stats[TYPE_SOCK+1];

static void reset_stats()
{
    int i;
    for (i = 0; i < TYPE_SOCK+1; i++ )
        stats[i] = stats_zero[i];
}

/* build filters depending on program options */
static int mkfilters()
{
    /* create boolean expression for matching root entries */
    if (prog_options.match_user)
    {
        compare_value_t val;
        strcpy(val.str, prog_options.user);
        if (!is_expr)
            CreateBoolCond(&match_expr, COMP_LIKE, CRITERIA_OWNER, val);
        else
            AppendBoolCond(&match_expr, COMP_LIKE, CRITERIA_OWNER, val);
        is_expr = 1;
        query_mask |= ATTR_MASK_owner;
    }

    if (prog_options.match_group)
    {
        compare_value_t val;
        strcpy(val.str, prog_options.group);
        if (!is_expr)
            CreateBoolCond(&match_expr, COMP_LIKE, CRITERIA_GROUP, val);
        else
            AppendBoolCond(&match_expr, COMP_LIKE, CRITERIA_GROUP, val);
        is_expr = 1;
        query_mask |= ATTR_MASK_gr_name;
    }

    if (prog_options.match_type)
    {
        compare_value_t val;
        val.type = ListMgr2PolicyType(prog_options.type);
        if (!is_expr)
            CreateBoolCond(&match_expr, COMP_EQUAL, CRITERIA_TYPE, val);
        else
            AppendBoolCond(&match_expr, COMP_EQUAL, CRITERIA_TYPE, val);
        is_expr = 1;
        query_mask |= ATTR_MASK_type;
    }

    /* create DB filters */
    lmgr_simple_filter_init( &entry_filter );

#ifdef ATTR_INDEX_status
    if (prog_options.match_status)
    {
        /* not part of user policies, only add it to DB filter */
        fv.val_uint = prog_options.status;
        lmgr_simple_filter_add( &entry_filter, ATTR_INDEX_status, EQUAL, fv, 0 );
    }
#endif

    if (is_expr)
    {
        char expr[RBH_PATH_MAX];
        /* for debug */
        if (BoolExpr2str(&match_expr, expr, RBH_PATH_MAX)>0)
            DisplayLog(LVL_FULL, DU_TAG, "Expression matching: %s", expr);

        /* append bool expr to entry filter */
        convert_boolexpr_to_simple_filter( &match_expr, &entry_filter );
    }

    return 0;
}



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
    _B "Usage:" B_ " %s [options] [path|fid]\n"
    "\n"
    _B "Filters:" B_ "\n"
    "    " _B "-u" B_ " " _U "user" U_ "\n"
    "    " _B "-g" B_ " " _U "group" U_ "\n"
    "    " _B "-t" B_ " " _U "type" U_ "\n"
    "       "TYPE_HELP"\n"
#ifdef ATTR_INDEX_status
    "    " _B "-S" B_ " " _U "status" U_ "\n"
    "       %s\n"
#endif
    "\n"
    _B "Program options:" B_ "\n"
    "    " _B "-f" B_ " " _U "config_file" U_ "\n"
    "    " _B "-l" B_ " " _U "log_level" U_ "\n"
    "    " _B "-h" B_ ", " _B "--help" B_ "\n"
    "        Display a short help about command line options.\n"
    "    " _B "-V" B_ ", " _B "--version" B_ "\n"
    "        Display version info\n";

static inline void display_help( char *bin_name )
{
#ifdef ATTR_INDEX_status
    printf( help_string, bin_name, allowed_status() );
#else
    printf( help_string, bin_name );
#endif
}

static inline void display_version( char *bin_name )
{
    printf( "\n" );
    printf( "Product:         " PACKAGE_NAME " 'du' command\n" );
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

static const char * opt2type(const char * type_opt)
{
    if (strlen(type_opt) != 1)
        return NULL;

    switch (type_opt[0])
    {
        case 'b': return STR_TYPE_BLK;
        case 'c': return STR_TYPE_CHR;
        case 'd': return STR_TYPE_DIR;
        case 'p': return STR_TYPE_FIFO;
        case 'f': return STR_TYPE_FILE;
        case 'l': return STR_TYPE_LINK;
        case 's': return STR_TYPE_SOCK;
        default:
            return NULL;
    }
}

static int Path2Id(const char *path, entry_id_t * id)
{
    int rc;
#ifndef _HAVE_FID
    struct stat inode;
    if (lstat(path, &inode))
        return -errno;

    id->inode = inode.st_ino;
    id->device = inode.st_dev;
    id->validator = inode.st_ctime;
    return 0;
#else
    /* perform path2fid */
    rc = Lustre_GetFidFromPath(path, id);
    return rc;
#endif
}

/**
 *  Get id of root dir
 */
static int get_root_id(entry_id_t * root_id)
{
    int rc;
    rc = Path2Id(config.global_config.fs_path, root_id);
    if (rc)
        DisplayLog(LVL_MAJOR, DU_TAG, "Can't access filesystem's root %s: %s",
                   config.global_config.fs_path, strerror(-rc));
    return rc;
}

#define REPCNT    4
static report_field_descr_t dir_info[REPCNT] = {
    {ATTR_INDEX_type, REPORT_GROUP_BY, SORT_NONE, FALSE, 0, {NULL}},
    {0, REPORT_COUNT, SORT_NONE, FALSE, 0, {NULL}},
    {ATTR_INDEX_blocks, REPORT_SUM, SORT_NONE, FALSE, 0, {NULL}},
    {ATTR_INDEX_size, REPORT_SUM, SORT_NONE, FALSE, 0, {NULL}}
};

/* directory callback */
static int dircb(entry_id_t * id_list, attr_set_t * attr_list,
                       unsigned int entry_count )
{
    /* sum child entries stats for all directories */
    int i, rc;
    filter_value_t fv;
    struct lmgr_report_t *it;
    db_value_t     result[REPCNT];
    unsigned int   result_count;

    /* filter on parent_id */

    for (i = 0; i < entry_count; i++)
    {
        fv.val_id = id_list[i];
        rc = lmgr_simple_filter_add_or_replace( &entry_filter,
                                                ATTR_INDEX_parent_id,
                                                EQUAL,
                                                fv, 0 );
        if (rc)
            return rc;

        it = ListMgr_Report(&lmgr, dir_info, REPCNT, &entry_filter, NULL);
        if (it == NULL)
            return -1;

        result_count = REPCNT;
        while ( ( rc = ListMgr_GetNextReportItem( it, result, &result_count ) ) == DB_SUCCESS )
        {
/*            printf("%Lu %s, %Lu blocks, size %Lu\n",
                   result[1].value_u.val_biguint,
                   result[0].value_u.val_str,
                   result[2].value_u.val_biguint,
                   result[3].value_u.val_biguint);*/

            unsigned int idx = ListMgr2PolicyType(result[0].value_u.val_str);
            stats[idx].count += result[1].value_u.val_biguint;
            stats[idx].blocks += result[2].value_u.val_biguint;
            stats[idx].size += result[3].value_u.val_biguint;

            result_count = REPCNT;
        }

        ListMgr_CloseReport( it );


    }
    return 0;
}

static int list_all()
{
    attr_set_t  root_attrs;
    entry_id_t  root_id;
    int rc, i;
    struct stat st;
    struct lmgr_report_t *it;

    db_value_t     result[REPCNT];
    unsigned int   result_count;



    ATTR_MASK_INIT( &root_attrs );

    rc = get_root_id(&root_id);
    if (rc)
        return rc;

    /* root is not a part of the DB: sum it now if it matches */
    ATTR_MASK_SET(&root_attrs, fullpath);
    strcpy(ATTR(&root_attrs, fullpath), config.global_config.fs_path);

    if (lstat(ATTR(&root_attrs, fullpath ), &st) == 0)
        PosixStat2EntryAttr(&st, &root_attrs, TRUE);

    /* sum root if it matches */
    if (!is_expr || (EntryMatches(&root_id, &root_attrs,
                     &match_expr, NULL) == POLICY_MATCH))
    {
        unsigned int idx = ListMgr2PolicyType(ATTR(&root_attrs, type));
        stats[idx].count ++;
        stats[idx].blocks += ATTR(&root_attrs, blocks);
        stats[idx].size += ATTR(&root_attrs, size);
    }


    it = ListMgr_Report(&lmgr, dir_info, REPCNT, &entry_filter, NULL);
    if (it == NULL)
        return -1;

    result_count = REPCNT;
    while ( ( rc = ListMgr_GetNextReportItem( it, result, &result_count ) ) == DB_SUCCESS )
    {
        /*printf("%Lu %s, %Lu blocks, size %Lu\n",
               result[1].value_u.val_biguint,
               result[0].value_u.val_str,
               result[2].value_u.val_biguint,
               result[3].value_u.val_biguint);*/

        unsigned int idx = ListMgr2PolicyType(result[0].value_u.val_str);
        stats[idx].count += result[1].value_u.val_biguint;
        stats[idx].blocks += result[2].value_u.val_biguint;
        stats[idx].size += result[3].value_u.val_biguint;

        result_count = REPCNT;
    }

    ListMgr_CloseReport( it );

    printf("%s:\n", config.global_config.fs_path);
    for (i = 0; i < TYPE_SOCK+1; i++)
        if (stats[i].count > 0)
            printf("\t%s count:%Lu, size:%Lu, spc_used:%Lu\n", stats[i].type,
                   stats[i].count, stats[i].size, stats[i].blocks * DEV_BSIZE);

    return 0;
}

/**
 * List the content of the given id/path list
 */
static int list_content(char ** id_list, int id_count)
{
    entry_id_t * ids;
    int i, rc;
    attr_set_t root_attrs;
    entry_id_t root_id;
    int is_id;

    rc = get_root_id(&root_id);
    if (rc)
        return rc;

    ids = MemCalloc(id_count, sizeof(entry_id_t));
    if (!ids)
        return -ENOMEM;

    for (i = 0; i < id_count; i++)
    {
        is_id = TRUE;
        /* is it a path or fid? */
        if (sscanf(id_list[i], SFID, RFID(&ids[i])) != FID_SCAN_CNT)
        {
            is_id = FALSE;
            /* take it as a path */
            rc = Path2Id(id_list[i], &ids[i]);
            if (rc)
            {
                DisplayLog(LVL_MAJOR, DU_TAG, "Invalid parameter: %s: %s",
                           id_list[i], strerror(-rc));
                goto out;
            }
        }

        if ((id_count == 1) && entry_id_equal(&ids[i], &root_id))
        {
            /* the ID is FS root: use list_all instead */
            DisplayLog(LVL_DEBUG, DU_TAG, "Optimization: command argument is filesystem's root: performing bulk sum in DB");
            return list_all();
        }

        /* get root attrs to print it (if it matches program options) */
        root_attrs.attr_mask = disp_mask | query_mask;
        rc = ListMgr_Get(&lmgr, &ids[i], &root_attrs);
        if (rc == 0)
            dircb(&ids[i], &root_attrs, 1);
        else
        {
            DisplayLog(LVL_VERB, DU_TAG, "Notice: no attrs in DB for %s", id_list[i]);

            if (!is_id)
            {
                struct stat st;
                ATTR_MASK_SET(&root_attrs, fullpath);
                strcpy(ATTR(&root_attrs, fullpath), id_list[i]);

                if (lstat(ATTR(&root_attrs, fullpath ), &st) == 0)
                    PosixStat2EntryAttr(&st, &root_attrs, TRUE);
            }
            else if (entry_id_equal(&ids[i], &root_id))
            {
                /* this is root id */
                struct stat st;
                ATTR_MASK_SET(&root_attrs, fullpath);
                strcpy(ATTR(&root_attrs, fullpath), config.global_config.fs_path);

                if (lstat(ATTR(&root_attrs, fullpath ), &st) == 0)
                    PosixStat2EntryAttr(&st, &root_attrs, TRUE);
            }

            dircb(&ids[i], &root_attrs, 1);
        }

        /* sum root if it matches */
        if (!is_expr || (EntryMatches(&ids[i], &root_attrs,
                         &match_expr, NULL) == POLICY_MATCH))
        {
            printf("type=%s\n", ATTR(&root_attrs, type));
            unsigned int idx = ListMgr2PolicyType(ATTR(&root_attrs, type));
            stats[idx].count ++;
            stats[idx].blocks += ATTR(&root_attrs, blocks);
            stats[idx].size += ATTR(&root_attrs, size);
        }
    }

    rc = rbh_scrub(&lmgr, ids, id_count, disp_mask, dircb);

    for (i = 0; i < TYPE_SOCK+1; i++)
        if (stats[i].count > 0)
            printf("\t%s count:%Lu, size:%Lu, spc_used:%Lu\n", stats[i].type,
                   stats[i].count, stats[i].size, stats[i].blocks * DEV_BSIZE);



out:
    /* ids have been processed, free them */
    MemFree(ids);
    return rc;
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
    int            rc, i;
    char           err_msg[4096];

    /* parse command line options */
    while ((c = getopt_long(argc, argv, SHORT_OPT_STRING, option_tab,
                            &option_index )) != -1)
    {
        switch ( c )
        {
            case 'u':
                prog_options.match_user = 1;
                prog_options.user = optarg;
                break;
            case 'g':
                prog_options.match_group = 1;
                prog_options.group = optarg;
                break;
            case 't':
                prog_options.match_type = 1;
                prog_options.type = opt2type(optarg);
                if (prog_options.type == NULL)
                {
                    fprintf(stderr, "invalid type '%s': expected types: "TYPE_HELP".\n", optarg);
                    exit(1);
                }
                break;
#ifdef ATTR_INDEX_status
            case 'S':
                prog_options.match_status = 1;
                prog_options.status = status2dbval(optarg);
                if ( prog_options.status == (file_status_t)-1 )
                {
                    fprintf(stderr, "Unknown status '%s'. Allowed status: %s.\n", optarg,
                            allowed_status());
                    exit(1);
                }
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
                    fprintf(stderr,
                            "Unsupported log level '%s'. CRIT, MAJOR, EVENT, VERB, DEBUG or FULL expected.\n",
                            optarg);
                    exit(1);
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

    /* Set logging to stderr */
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
        DisplayLog( LVL_CRIT, DU_TAG, "Error %d initializing list manager", rc );
        exit( rc );
    }
    else
        DisplayLog( LVL_DEBUG, DU_TAG, "ListManager successfully initialized" );

    if (CheckLastFS(  ) != 0)
        exit(1);

    /* Create database access */
    rc = ListMgr_InitAccess(&lmgr);
    if (rc)
    {
        DisplayLog( LVL_CRIT, DU_TAG, "Error %d: cannot connect to database", rc );
        exit(rc);
    }

    mkfilters();
    reset_stats();

    if (argc == optind)
        /* no path in argument: du the entire FS */
        rc = list_all();
    else
        rc = list_content(argv+optind, argc-optind);

    ListMgr_CloseAccess( &lmgr );

    return rc;

}
