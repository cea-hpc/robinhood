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
 * Find clone based on robinhood DB
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

#define FIND_TAG "find"

static struct option option_tab[] =
{
    {"user", required_argument, NULL, 'u'},
    {"group", required_argument, NULL, 'g'},
    {"type", required_argument, NULL, 't'},
    {"size", required_argument, NULL, 's'},
    {"name", required_argument, NULL, 'n'},
#ifdef _LUSTRE
    {"ost", required_argument, NULL, 'o'},
#endif
#ifdef ATTR_INDEX_status
    {"status", required_argument, NULL, 'S'},
#endif
    {"ls", no_argument, NULL, 'l'},

    /* config file options */
    {"config-file", required_argument, NULL, 'f'},

    /* log options */
    {"debug-level", required_argument, NULL, 'd'},

    /* miscellaneous options */
    {"help", no_argument, NULL, 'h'},
    {"version", no_argument, NULL, 'V'},

    {NULL, 0, NULL, 0}

};

#define SHORT_OPT_STRING    "lu:g:t:s:n:S:o:f:d:hV"

#define TYPE_HELP "'f' (file), 'd' (dir), 'l' (symlink), 'b' (block), 'c' (char), 'p' (named pipe/FIFO), 's' (socket)"
#define SIZE_HELP "[-|+]<val>[K|M|G|T]"

/* global variables */

static lmgr_t  lmgr;
robinhood_config_t config;

/* program options */
struct find_opt
{
    const char * user;
    const char * group;
    const char * type;
    // size cond: gt/eq/lt <val>
    compare_direction_t sz_compar;
    uint64_t            sz_val;
    const char * name;
    unsigned int ost_idx;
#ifdef ATTR_INDEX_status
    file_status_t status;
#endif

    /* output flags */
    unsigned int ls:1;
    /* condition flags */
    unsigned int match_user:1;
    unsigned int match_group:1;
    unsigned int match_type:1;
    unsigned int match_size:1;
    unsigned int match_name:1;
#ifdef _LUSTRE
    unsigned int match_ost:1;
#endif
#ifdef ATTR_INDEX_status
    unsigned int match_status:1;
#endif

    /* behavior flags */
    unsigned int no_dir:1; /* if -t != dir => no dir to be displayed */
    unsigned int dir_only:1; /* if -t dir => only display dir */
} prog_options = {
    .user = NULL, .group = NULL, .type = NULL, .name = NULL,
#ifdef ATTR_INDEX_status
    .status = STATUS_UNKNOWN,
#endif
    .ls = 0, .match_user = 0, .match_group = 0,
    .match_type = 0, .match_size = 0, .match_name = 0,
#ifdef ATTR_INDEX_status
    .match_status = 0,
#endif
    .no_dir = 0, .dir_only = 0
};

#ifdef ATTR_INDEX_status
#define DISPLAY_MASK (ATTR_MASK_type | ATTR_MASK_fullpath | ATTR_MASK_owner |\
                      ATTR_MASK_gr_name | ATTR_MASK_size | ATTR_MASK_last_mod | ATTR_MASK_status)
#else
#define DISPLAY_MASK (ATTR_MASK_type | ATTR_MASK_fullpath | ATTR_MASK_owner |\
                      ATTR_MASK_gr_name | ATTR_MASK_size | ATTR_MASK_last_mod)
#endif
static int disp_mask = ATTR_MASK_fullpath | ATTR_MASK_type;
static int query_mask = 0;

//static lmgr_filter_t    dir_filter;

/* for filtering entries from DB */
static lmgr_filter_t    entry_filter;

/* post filter for all entries */
static bool_node_t      match_expr;
static int              is_expr = 0; /* is it set? */

/* build filters depending on program options */
static int mkfilters(int exclude_dirs)
{
    filter_value_t fv;

    /* create boolean expression for matching */

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

    if (prog_options.match_name)
    {
        /* this is not converted to DB filter, but will be used in post checking */
        compare_value_t val;
        strcpy(val.str, prog_options.name);
        if (!is_expr)
            CreateBoolCond(&match_expr, COMP_LIKE, CRITERIA_FILENAME, val);
        else
            AppendBoolCond(&match_expr, COMP_LIKE, CRITERIA_FILENAME, val);
        is_expr = 1;
        query_mask |= ATTR_MASK_name;
    }

    if (prog_options.match_size)
    {
        compare_value_t val;
        val.size = prog_options.sz_val;
        if (!is_expr)
            CreateBoolCond(&match_expr, prog_options.sz_compar, CRITERIA_SIZE, val);
        else
            AppendBoolCond(&match_expr, prog_options.sz_compar, CRITERIA_SIZE, val);
        is_expr = 1;
        query_mask |= ATTR_MASK_size;
    }

#ifdef _LUSTRE
    if (prog_options.match_ost)
    {
        /* this is not converted to DB filter, but will be used in post checking */
        compare_value_t val;
        val.integer = prog_options.ost_idx;
        if (!is_expr)
            CreateBoolCond(&match_expr, COMP_EQUAL, CRITERIA_OST, val);
        else
            AppendBoolCond(&match_expr, COMP_EQUAL, CRITERIA_OST, val);
        is_expr = 1;
        query_mask |= ATTR_MASK_stripe_items;
    }
#endif

    /* create DB filters */
    lmgr_simple_filter_init( &entry_filter );

    /* analyze type filter */
    if (prog_options.match_type)
    {
        if (!strcasecmp(prog_options.type, STR_TYPE_DIR))
        {
            /* only match dirs */
            prog_options.dir_only = 1;
            if (!exclude_dirs)
            {
                fv.val_str = STR_TYPE_DIR;
                lmgr_simple_filter_add(&entry_filter, ATTR_INDEX_type, EQUAL, fv, 0);
            }
        }
        else
        {
            /* smthg different from dir */
            prog_options.no_dir = 1;
            fv.val_str = prog_options.type;
            lmgr_simple_filter_add(&entry_filter, ATTR_INDEX_type, EQUAL, fv, 0);
        }
    }
    else if (exclude_dirs) /* no specific type specified => exclude dirs if required */
    {
        /* filter non directories (directories are handled during recursive DB scan) */
        fv.val_str = STR_TYPE_DIR;
        lmgr_simple_filter_add( &entry_filter, ATTR_INDEX_type, NOTEQUAL, fv, 0 );
    }

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
            DisplayLog(LVL_FULL, FIND_TAG, "Expression matching: %s", expr);

        /* append bool expr to entry filter */
        convert_boolexpr_to_simple_filter( &match_expr, &entry_filter );
    }

    return 0;
}

static const char *help_string =
    _B "Usage:" B_ " %s [options] [path|fid]...\n"
    "\n"
    _B "Filters:" B_ "\n"
    "    " _B "-user" B_ " " _U "user" U_ "\n"
    "    " _B "-group" B_ " " _U "group" U_ "\n"
    "    " _B "-type" B_ " " _U "type" U_ "\n"
    "       "TYPE_HELP"\n"
    "    " _B "-size" B_ " " _U "size_crit" U_ "\n"
    "       "SIZE_HELP"\n"
    "    " _B "-name" B_ " " _U "filename" U_ "\n"
#ifdef _LUSTRE
    "    " _B "-ost" B_ " " _U "ost_index" U_ "\n"
#endif
#ifdef ATTR_INDEX_status
    "    " _B "-status" B_ " " _U "status" U_ "\n"
    "       %s\n"
#endif
    "\n"
    _B "Output options:" B_ "\n"
    "    " _B "-ls" B_" \t: display attributes\n"
    "\n"
    _B "Program options:" B_ "\n"
    "    " _B "-f" B_ " " _U "config_file" U_ "\n"
    "    " _B "-d" B_ " " _U "log_level" U_ "\n"
    "       CRIT, MAJOR, EVENT, VERB, DEBUG, FULL\n"
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
    printf( "Product:         " PACKAGE_NAME " 'find' command\n" );
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

static const char * type2char(const char * type)
{
    if (!strcasecmp(type, STR_TYPE_DIR))
        return "dir";
    else if (!strcasecmp(type, STR_TYPE_FILE))
        return "file";
    else if (!strcasecmp(type, STR_TYPE_LINK))
        return "link";
    else if (!strcasecmp(type, STR_TYPE_CHR))
        return "char";
    else if (!strcasecmp(type, STR_TYPE_BLK))
        return "blk";
    else if (!strcasecmp(type, STR_TYPE_FIFO))
        return "fifo";
    else if (!strcasecmp(type, STR_TYPE_SOCK))
        return "sock";
    return "?";
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

/* parse size filter and set prog_options struct */
int set_size_filter(char * str)
{
    compare_direction_t comp;
    char * curr = str;
    uint64_t val;
    char suffix[1024];
    int n;

    if (str[0] == '+')
    {
        comp = COMP_GRTHAN;
        curr++;
    }
    else if (str[0] == '-')
    {
        comp = COMP_LSTHAN;
        curr++;
    }
    else
        comp = COMP_EQUAL;

    n = sscanf(curr, "%"PRIu64"%s", &val, suffix);
    if (n < 1 || n > 2)
    {
        fprintf(stderr, "Invalid size: '%s'. Expected size format: "SIZE_HELP"\n", str);
        return -EINVAL;
    }
    if ((n == 1) || !strcmp(suffix, ""))
    {
        prog_options.sz_compar = comp;
        prog_options.sz_val = val;
        prog_options.match_size = 1;
    }
    else
    {
        switch(suffix[0])
        {
            case 'k':
            case 'K':
                val *= 1024LL;
                break;
            case 'm':
            case 'M':
                val *= 1024LL * 1024LL;
                break;
            case 'g':
            case 'G':
                val *= 1024LL * 1024LL * 1024LL;
                break;
            case 't':
            case 'T':
                val *= 1024LL * 1024LL * 1024LL * 1024LL;
                break;

            case 'p':
            case 'P':
                val *= 1024LL * 1024LL * 1024LL * 1024LL *1024LL;
                break;
            default:
                fprintf(stderr, "Invalid suffix for size: '%s'. Expected size format: "SIZE_HELP"\n", str);
                return -EINVAL;
        }
        prog_options.sz_compar = comp;
        prog_options.sz_val = val;
        prog_options.match_size = 1;
    }
    return 0;
}


static inline void print_entry(const entry_id_t * id, const attr_set_t * attrs)
{
#ifdef ATTR_INDEX_status
    if (prog_options.match_status)
    {
        if (ATTR_MASK_TEST(attrs, status) && (ATTR(attrs, status) != prog_options.status))
        {
            /* no match -> no display */
            return;
        }
    }
#endif

    if (!prog_options.ls)
    {
        /* just display name */
        if (ATTR_MASK_TEST(attrs, fullpath))
            printf("%s\n", ATTR(attrs, fullpath));
        else
            printf(DFID"\n", PFID(id));
    }
    else
    {
        const char * type;
        char date_str[128];
#ifdef ATTR_INDEX_status
        const char * status_str = "";

        /* add status after type */
        if (ATTR_MASK_TEST(attrs, status) && (ATTR(attrs, status) != STATUS_UNKNOWN))
            status_str = db_status2str(ATTR(attrs, status), 1); /* 1 for brief */

        #define STATUS_FORMAT   "%-10s"
        #define STATUS_VAL ,status_str
#else
        #define STATUS_FORMAT   ""
        #define STATUS_VAL
#endif
        /* type2char */
        if (!ATTR_MASK_TEST(attrs, type))
            type = "?";
        else
            type = type2char(ATTR(attrs, type));

        if (!ATTR_MASK_TEST(attrs, type))
            type = "?";
        else
            type = type2char(ATTR(attrs, type));

        if (!ATTR_MASK_TEST(attrs, last_mod))
            strcpy(date_str, "");
        else
        {
            time_t tt;
            struct tm stm;
            tt = ATTR(attrs, last_mod);
            strftime(date_str, 128, "%Y/%m/%d %T", localtime_r(&tt, &stm));
        }

        /* display all: id, type, owner, group, size, mtime, path */
        printf(DFID" %-4s "STATUS_FORMAT"%-10s %-10s %15"PRIu64" %20s %s\n",
               PFID(id), type STATUS_VAL, ATTR(attrs, owner), ATTR(attrs, gr_name),
               ATTR(attrs, size), date_str, ATTR(attrs, fullpath));
    }
}

/* directory callback */
static int dircb(entry_id_t * id_list, attr_set_t * attr_list,
                       unsigned int entry_count )
{
    /* retrieve child entries for all directories */
    int i, rc;

    for (i = 0; i < entry_count; i++)
    {
        entry_id_t * chids = NULL;
        attr_set_t * chattrs = NULL;
        unsigned int chcount = 0;
        int j;

        /* match condition on dirs parent */
        if (!is_expr || (EntryMatches(&id_list[i], &attr_list[i],
                         &match_expr, NULL) == POLICY_MATCH))
        {
            /* don't display dirs if no_dir is specified */
            if (! (prog_options.no_dir && ATTR_MASK_TEST(&attr_list[i], type)
                   && !strcasecmp(ATTR(&attr_list[i], type), STR_TYPE_DIR)) )
                print_entry(&id_list[i], &attr_list[i]);
        }

        if (!prog_options.dir_only)
        {
            rc = ListMgr_GetChild( &lmgr, &entry_filter, id_list+i, 1,
                                   disp_mask | query_mask,
                                   &chids, &chattrs, &chcount );
            if (rc)
            {
                DisplayLog(LVL_MAJOR, FIND_TAG, "ListMgr_GetChild() failed with error %d", rc);
                return rc;
            }

            for (j = 0; j < chcount; j++)
            {
                if (!is_expr || (EntryMatches(&chids[j], &chattrs[j],
                                 &match_expr, NULL) == POLICY_MATCH))
                    print_entry(&chids[j], &chattrs[j]);
            }
        }
    }
    return 0;
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
        DisplayLog(LVL_MAJOR, FIND_TAG, "Can't access filesystem's root %s: %s",
                   config.global_config.fs_path, strerror(-rc));
    return rc;
}

/**
 * List the content of all the DB
 */
static int list_all()
{
    attr_set_t  root_attrs, attrs;
    entry_id_t  root_id, id;
    int rc;
    struct stat st;
    struct lmgr_iterator_t *it;

    ATTR_MASK_INIT( &root_attrs );

    rc = get_root_id(&root_id);
    if (rc)
        return rc;

    /* root is not a part of the DB: print it now */
    ATTR_MASK_SET(&root_attrs, fullpath);
    strcpy(ATTR(&root_attrs, fullpath), config.global_config.fs_path);

    if (lstat(ATTR(&root_attrs, fullpath ), &st) == 0)
    {
        PosixStat2EntryAttr(&st, &root_attrs, TRUE);
        ListMgr_GenerateFields( &root_attrs, disp_mask | query_mask);
    }

    /* match condition on dirs parent */
    if (!is_expr || (EntryMatches(&root_id, &root_attrs,
                     &match_expr, NULL) == POLICY_MATCH))
    {
        /* don't display dirs if no_dir is specified */
        if (! (prog_options.no_dir && ATTR_MASK_TEST(&root_attrs, type)
               && !strcasecmp(ATTR(&root_attrs, type), STR_TYPE_DIR)) )
            print_entry(&root_id, &root_attrs);
    }

    /* list all, including dirs */
    it = ListMgr_Iterator( &lmgr, &entry_filter, NULL, NULL );
    if (!it)
    {
        DisplayLog(LVL_MAJOR, FIND_TAG, "ERROR: cannot retrieve entry list from database");
        return -1;
    }

    attrs.attr_mask = disp_mask | query_mask;
    while ((rc = ListMgr_GetNext( it, &id, &attrs )) == DB_SUCCESS)
    {
        if (!is_expr || (EntryMatches(&id, &attrs, &match_expr, NULL)
                                      == POLICY_MATCH))
        {
            /* don't display dirs if no_dir is specified */
            if (! (prog_options.no_dir && ATTR_MASK_TEST(&attrs, type)
                   && !strcasecmp(ATTR(&attrs, type), STR_TYPE_DIR)) )
                print_entry(&id, &attrs);
            /* don't display non dirs is dir_only is specified */
            else if (! (prog_options.dir_only && ATTR_MASK_TEST(&attrs, type)
                   && strcasecmp(ATTR(&attrs, type), STR_TYPE_DIR)))
                print_entry(&id, &attrs);
            else
                /* return entry don't match? */
                DisplayLog(LVL_DEBUG, FIND_TAG, "Warning: returned DB entry doesn't match filter: %s",
                           ATTR(&attrs, fullpath));
        }
        ListMgr_FreeAttrs( &attrs );

        /* prepare next call */
        attrs.attr_mask = disp_mask | query_mask;
    }
    ListMgr_CloseIterator( it );



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
                DisplayLog(LVL_MAJOR, FIND_TAG, "Invalid parameter: %s: %s",
                           id_list[i], strerror(-rc));
                goto out;
            }
        }

        if ((id_count == 1) && entry_id_equal(&ids[i], &root_id))
        {
            /* the ID is FS root: use list_all instead */
            DisplayLog(LVL_DEBUG, FIND_TAG, "Optimization: command argument is filesystem's root: performing bulk DB dump");

            mkfilters(FALSE); /* keep dirs */
            return list_all();
        }

        /* get root attrs to print it (if it matches program options) */
        root_attrs.attr_mask = disp_mask | query_mask;
        rc = ListMgr_Get(&lmgr, &ids[i], &root_attrs);
        if (rc == 0)
            dircb(&ids[i], &root_attrs, 1);
        else
        {
            DisplayLog(LVL_VERB, FIND_TAG, "Notice: no attrs in DB for %s", id_list[i]);

            if (!is_id)
            {
                struct stat st;
                ATTR_MASK_SET(&root_attrs, fullpath);
                strcpy(ATTR(&root_attrs, fullpath), id_list[i]);

                if (lstat(ATTR(&root_attrs, fullpath ), &st) == 0)
                {
                    PosixStat2EntryAttr(&st, &root_attrs, TRUE);
                    ListMgr_GenerateFields( &root_attrs, disp_mask | query_mask);
                }
            }
            else if (entry_id_equal(&ids[i], &root_id))
            {
                /* this is root id */
                struct stat st;
                ATTR_MASK_SET(&root_attrs, fullpath);
                strcpy(ATTR(&root_attrs, fullpath), config.global_config.fs_path);

                if (lstat(ATTR(&root_attrs, fullpath ), &st) == 0)
                {
                    PosixStat2EntryAttr(&st, &root_attrs, TRUE);
                    ListMgr_GenerateFields( &root_attrs, disp_mask | query_mask);
                }
            }

            dircb(&ids[i], &root_attrs, 1);
        }
    }

    rc = rbh_scrub(&lmgr, ids, id_count, disp_mask | query_mask, dircb);

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
    int            rc;
    char           err_msg[4096];

    /* parse command line options */
    while ((c = getopt_long_only(argc, argv, SHORT_OPT_STRING, option_tab,
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
        case 'n':
            prog_options.match_name = 1;
            prog_options.name = optarg;
            break;
        case 'o':
            prog_options.match_ost = 1;
            prog_options.ost_idx = str2int(optarg);
            if (prog_options.ost_idx == (unsigned int)-1)
            {
                fprintf(stderr, "invalid ost index '%s': unsigned integer expected\n", optarg);
                exit(1);
            }
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
        case 's':
            if (set_size_filter(optarg))
                exit(1);
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
        case 'l':
            prog_options.ls = 1;
            disp_mask = DISPLAY_MASK;
            break;
        case 'f':
            strncpy( config_file, optarg, MAX_OPT_LEN );
            break;
        case 'd':
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
        DisplayLog( LVL_CRIT, FIND_TAG, "Error %d initializing list manager", rc );
        exit( rc );
    }
    else
        DisplayLog( LVL_DEBUG, FIND_TAG, "ListManager successfully initialized" );

    if (CheckLastFS(  ) != 0)
        exit(1);

    /* Create database access */
    rc = ListMgr_InitAccess(&lmgr);
    if (rc)
    {
        DisplayLog( LVL_CRIT, FIND_TAG, "Error %d: cannot connect to database", rc );
        exit(rc);
    }

    if (argc == optind)
    {
        mkfilters(FALSE); /* keep dirs */
        /* no path speficied, list all entries */
        rc = list_all();
    }
    else
    {
        mkfilters(TRUE); /* exclude dirs */
        rc = list_content(argv+optind, argc-optind);
    }

    ListMgr_CloseAccess( &lmgr );

    return rc;

}
