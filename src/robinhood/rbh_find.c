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
    {"mtime", required_argument, NULL, 'M'},
    {"ctime", required_argument, NULL, 'C'},
    {"mmin", required_argument, NULL, 'm'},
    {"msec", required_argument, NULL, 'z'},
    {"atime", required_argument, NULL, 'A'},
    {"amin", required_argument, NULL, 'a'},
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
    {"not", no_argument, NULL, '!'},

    {NULL, 0, NULL, 0}

};

#define SHORT_OPT_STRING    "lu:g:t:s:n:S:o:A:M:m:z:f:d:hV!"

#define TYPE_HELP "'f' (file), 'd' (dir), 'l' (symlink), 'b' (block), 'c' (char), 'p' (named pipe/FIFO), 's' (socket)"
#define SIZE_HELP "[-|+]<val>[K|M|G|T]"
#define TIME_HELP "[-|+]<val>[s|m|h|d|y] (s: sec, m: min, h: hour, d:day, y:year. default unit is days)"

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

    // ctime cond: gt/eq/lt <time>
    compare_direction_t crt_compar;
    time_t              crt_val;

    // mtime cond: gt/eq/lt <time>
    compare_direction_t mod_compar;
    time_t              mod_val;

    // atime cond: gt/eq/lt <time>
    compare_direction_t acc_compar;
    time_t              acc_val;

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
    unsigned int match_ctime:1;
    unsigned int match_mtime:1;
    unsigned int match_atime:1;
#ifdef _LUSTRE
    unsigned int match_ost:1;
#endif
#ifdef ATTR_INDEX_status
    unsigned int match_status:1;
    unsigned int statusneg:1;
#endif

    /* -not flags */
    unsigned int userneg:1;
    unsigned int groupneg:1;
    unsigned int nameneg:1;

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
    .match_ctime = 0, .match_mtime = 0, .match_atime = 0,
#ifdef ATTR_INDEX_status
    .match_status = 0, .statusneg = 0,
#endif
    .userneg = 0 , .groupneg = 0, .nameneg = 0,
    .no_dir = 0, .dir_only = 0
};

#ifdef ATTR_INDEX_status
#define DISPLAY_MASK (ATTR_MASK_type | ATTR_MASK_nlink | ATTR_MASK_mode | ATTR_MASK_name | ATTR_MASK_owner |\
                      ATTR_MASK_gr_name | ATTR_MASK_size | ATTR_MASK_last_mod | ATTR_MASK_link | ATTR_MASK_status)
#else
#define DISPLAY_MASK (ATTR_MASK_type | ATTR_MASK_nlink | ATTR_MASK_mode | ATTR_MASK_name | ATTR_MASK_owner |\
                      ATTR_MASK_gr_name | ATTR_MASK_size | ATTR_MASK_last_mod | ATTR_MASK_link )
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
    int compflag;

    /* create boolean expression for matching */

    if (prog_options.match_user)
    {
        compare_value_t val;
        strcpy(val.str, prog_options.user);
        if (prog_options.userneg)
            compflag = COMP_UNLIKE;
        else
            compflag = COMP_LIKE;
        if (!is_expr)
            CreateBoolCond(&match_expr, compflag, CRITERIA_OWNER, val);
        else
            AppendBoolCond(&match_expr, compflag, CRITERIA_OWNER, val);
        is_expr = 1;
        query_mask |= ATTR_MASK_owner;
    }

    if (prog_options.match_group)
    {
        compare_value_t val;
        strcpy(val.str, prog_options.group);
        if (prog_options.groupneg)
            compflag = COMP_UNLIKE;
        else
            compflag = COMP_LIKE;
        if (!is_expr)
            CreateBoolCond(&match_expr, compflag, CRITERIA_GROUP, val);
        else
            AppendBoolCond(&match_expr, compflag, CRITERIA_GROUP, val);
        is_expr = 1;
        query_mask |= ATTR_MASK_gr_name;
    }

    if (prog_options.match_name)
    {
        /* this is not converted to DB filter, but will be used in post checking */
        compare_value_t val;
        strcpy(val.str, prog_options.name);
        if (prog_options.nameneg)
            compflag = COMP_UNLIKE;
        else
            compflag = COMP_LIKE;
        if (!is_expr)
            CreateBoolCond(&match_expr, compflag, CRITERIA_FILENAME, val);
        else
            AppendBoolCond(&match_expr, compflag, CRITERIA_FILENAME, val);
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

    if (prog_options.match_ctime)
    {
        compare_value_t val;
        val.duration = prog_options.crt_val;
        if (!is_expr)
            CreateBoolCond(&match_expr, prog_options.crt_compar, CRITERIA_CREATION, val);
        else
            AppendBoolCond(&match_expr, prog_options.crt_compar, CRITERIA_CREATION, val);
        is_expr = 1;
        query_mask |= ATTR_MASK_creation_time;
    }

    if (prog_options.match_mtime)
    {
        compare_value_t val;
        val.duration = prog_options.mod_val;
        if (!is_expr)
            CreateBoolCond(&match_expr, prog_options.mod_compar, CRITERIA_LAST_MOD, val);
        else
            AppendBoolCond(&match_expr, prog_options.mod_compar, CRITERIA_LAST_MOD, val);
        is_expr = 1;
        query_mask |= ATTR_MASK_last_mod;
    }

    if (prog_options.match_atime)
    {
        compare_value_t val;
        val.duration = prog_options.acc_val;
        if (!is_expr)
            CreateBoolCond(&match_expr, prog_options.acc_compar, CRITERIA_LAST_ACCESS, val);
        else
            AppendBoolCond(&match_expr, prog_options.acc_compar, CRITERIA_LAST_ACCESS, val);
        is_expr = 1;
        query_mask |= ATTR_MASK_last_access;
    }
#ifdef _LUSTRE
    if (prog_options.match_ost)
    {
        /* this partially converted to DB filter, and will be fully used in post checking */
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
                fv.value.val_str = STR_TYPE_DIR;
                lmgr_simple_filter_add(&entry_filter, ATTR_INDEX_type, EQUAL, fv, 0);
            }
        }
        else
        {
            /* smthg different from dir */
            prog_options.no_dir = 1;
            fv.value.val_str = prog_options.type;
            lmgr_simple_filter_add(&entry_filter, ATTR_INDEX_type, EQUAL, fv, 0);
        }
    }
    else if (exclude_dirs) /* no specific type specified => exclude dirs if required */
    {
        /* filter non directories (directories are handled during recursive DB scan) */
        fv.value.val_str = STR_TYPE_DIR;
        lmgr_simple_filter_add( &entry_filter, ATTR_INDEX_type, NOTEQUAL, fv, 0 );
    }

#ifdef ATTR_INDEX_status
    if (prog_options.match_status)
    {
        if (prog_options.statusneg)
            compflag = NOTEQUAL;
        else
            compflag = EQUAL;
        /* not part of user policies, only add it to DB filter */
        fv.value.val_uint = prog_options.status;
        lmgr_simple_filter_add( &entry_filter, ATTR_INDEX_status, compflag, fv, 0 );
    }
#endif

    if (prog_options.match_name)
    {
        char tmpstr[RBH_PATH_MAX];
        sprintf(tmpstr, "*/%s", prog_options.name);
        fv.value.val_str = tmpstr;
        lmgr_simple_filter_add( &entry_filter, ATTR_INDEX_fullpath, LIKE, fv, 0 );
    }

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
    "    " _B "-ctime" B_ " " _U "time_crit" U_ "\n"
    "       "TIME_HELP"\n"
    "    " _B "-mtime" B_ " " _U "time_crit" U_ "\n"
    "       "TIME_HELP"\n"
    "    " _B "-mmin" B_ " " _U "minute_crit" U_ "\n"
    "        same as '-mtime "_U"N"U_"m'\n"
    "    " _B "-msec" B_ " " _U "second_crit" U_ "\n"
    "        same as '-mtime "_U"N"U_"s'\n"
    "    " _B "-atime" B_ " " _U "time_crit" U_ "\n"
    "       "TIME_HELP"\n"
    "    " _B "-amin" B_ " " _U "minute_crit" U_ "\n"
    "       "TIME_HELP"\n"
#ifdef _LUSTRE
    "    " _B "-ost" B_ " " _U "ost_index" U_ "\n"
#endif
#ifdef ATTR_INDEX_status
    "    " _B "-status" B_ " " _U "status" U_ "\n"
    "       %s\n"
#endif
    "\n"
    "    " _B "-not" B_ ", "_B"-!"B_"\n"
    "        negate next argument\n"
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

static compare_direction_t prefix2comp(char** curr)
{
    char * str = *curr;

    if (str[0] == '+')
    {
        (*curr)++;
        return COMP_GRTHAN;
    }
    else if (str[0] == '-')
    {
        (*curr)++;
        return COMP_LSTHAN;
    }
    else
        return COMP_EQUAL;
}

/* parse size filter and set prog_options struct */
static int set_size_filter(char * str)
{
    compare_direction_t comp;
    char * curr = str;
    uint64_t val;
    char suffix[1024];
    int n;

    comp = prefix2comp(&curr);

    n = sscanf(curr, "%"PRIu64"%s", &val, suffix);
    if (n < 1 || n > 2)
    {
        fprintf(stderr, "Invalid size '%s' : expected size format: "SIZE_HELP"\n", str);
        return -EINVAL;
    }
    if ((n == 1) || !strcmp(suffix, ""))
    {
        prog_options.sz_compar = comp;
        prog_options.sz_val = val;
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
    }
    return 0;
}

typedef enum {atime, rh_ctime, mtime} e_time;
/* parse time filter and set prog_options struct */
static int set_time_filter(char * str, unsigned int multiplier, int allow_suffix, e_time what)
{
    compare_direction_t comp;
    char * curr = str;
    uint64_t val;
    char suffix[1024];
    int n;

    comp = prefix2comp(&curr);

    n = sscanf(curr, "%"PRIu64"%s", &val, suffix);
    /* allow_suffix => 1 or 2 is allowed
       else => only 1 is allowed */
    if (allow_suffix && (n < 1 || n > 2))
    {
        fprintf(stderr, "Invalid time '%s' : expected time format: "TIME_HELP"\n", str);
        return -EINVAL;
    }
    else if (!allow_suffix && (n != 1))
    {
        fprintf(stderr, "Invalid value '%s' : [+|-]<integer> expected\n", str);
        return -EINVAL;
    }

    if ((n == 1) || !strcmp(suffix, ""))
    {
        if ( what == rh_ctime )
        {
            prog_options.crt_compar = comp;
            if (multiplier != 0)
                prog_options.crt_val = val * multiplier;
            else /* default multiplier is days */
                prog_options.crt_val =  val * 86400;
        }
        else
        if ( what == mtime )
        {
            prog_options.mod_compar = comp;
            if (multiplier != 0)
                prog_options.mod_val = val * multiplier;
            else /* default multiplier is days */
                prog_options.mod_val =  val * 86400;
        }
        else
        {
            prog_options.acc_compar = comp;
            if (multiplier != 0)
                prog_options.acc_val = val * multiplier;
            else /* default multiplier is days */
                prog_options.acc_val =  val * 86400;
        }
    }
    else
    {
        switch(suffix[0])
        {
            case 's':
                /* keep unchanged */
                break;
            case 'm':
                val *= 60;
                break;
            case 'h':
                val *= 3600;
                break;
            case 'd':
                val *= 86400;
                break;
            case 'y':
                val *= 31557600; /* 365.25 * 86400 */
                break;
            default:
                fprintf(stderr, "Invalid suffix for time: '%s'. Expected time format: "TIME_HELP"\n", str);
                return -EINVAL;
        }
        if ( what == rh_ctime )
        {
            prog_options.crt_compar = comp;
            prog_options.crt_val = val;
        }
        else
        if ( what == mtime )
        {
            prog_options.mod_compar = comp;
            prog_options.mod_val = val;
        }
        else
        {
            prog_options.acc_compar = comp;
            prog_options.acc_val = val;
        }
    }
    return 0;
}


static inline void print_entry(const wagon_t *id, const attr_set_t * attrs)
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
        if (id->fullname)
            printf("%s\n", id->fullname);
        else
            printf(DFID"\n", PFID(&id->id));
    }
    else
    {
        const char * type;
        char date_str[128];
        char mode_str[128];
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

        memset(mode_str, 0, sizeof(mode_str));
        mode_string(ATTR(attrs, mode), mode_str);

        if (!ATTR_MASK_TEST(attrs, last_mod))
            strcpy(date_str, "");
        else
        {
            time_t tt;
            struct tm stm;
            tt = ATTR(attrs, last_mod);
            strftime(date_str, 128, "%Y/%m/%d %T", localtime_r(&tt, &stm));
        }

        if (ATTR_MASK_TEST(attrs, type) && !strcmp(ATTR(attrs, type), STR_TYPE_LINK)
            && ATTR_MASK_TEST(attrs, link))
            /* display: id, type, mode, nlink, (status,) owner, group, size, mtime, path -> link */
            printf(DFID" %-4s %s %3u  "STATUS_FORMAT"%-10s %-10s %15"PRIu64" %20s %s -> %s\n",
                   PFID(&id->id), type, mode_str, ATTR(attrs, nlink) STATUS_VAL,
                   ATTR(attrs, owner), ATTR(attrs, gr_name),
                   ATTR(attrs, size), date_str, id->fullname, ATTR(attrs,link));
        else
            /* display all: id, type, mode, nlink, (status,) owner, group, size, mtime, path */
            printf(DFID" %-4s %s %3u  "STATUS_FORMAT"%-10s %-10s %15"PRIu64" %20s %s\n",
                   PFID(&id->id), type, mode_str, ATTR(attrs, nlink) STATUS_VAL,
                   ATTR(attrs, owner), ATTR(attrs, gr_name),
                   ATTR(attrs, size), date_str, id->fullname);
    }
}

/* directory callback */
static int dircb(wagon_t * id_list, attr_set_t * attr_list,
                 unsigned int entry_count, void * dummy )
{
    /* retrieve child entries for all directories */
    int i, rc;

    for (i = 0; i < entry_count; i++)
    {
        wagon_t * chids = NULL;
        attr_set_t * chattrs = NULL;
        unsigned int chcount = 0;
        int j;

        /* match condition on dirs parent */
        if (!is_expr || (EntryMatches(&id_list[i].id, &attr_list[i],
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
                                   &chids, &chattrs, &chcount);
            if (rc)
            {
                DisplayLog(LVL_MAJOR, FIND_TAG, "ListMgr_GetChild() failed with error %d", rc);
                return rc;
            }

            for (j = 0; j < chcount; j++)
            {
                if (!is_expr || (EntryMatches(&chids[j].id, &chattrs[j],
                                 &match_expr, NULL) == POLICY_MATCH))
                    print_entry(&chids[j], &chattrs[j]);
            }

            free_wagon(chids, 0, chcount);
            MemFree(chids);
            MemFree(chattrs);
        }
    }
    return 0;
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
 * List the content of the given id/path list
 */
static int list_content(char ** id_list, int id_count)
{
    wagon_t *ids;
    int i, rc;
    attr_set_t root_attrs;
    entry_id_t root_id;
    int is_id;

    rc = get_root_id(&root_id);
    if (rc)
        return rc;

    ids = MemCalloc(id_count, sizeof(wagon_t));
    if (!ids)
        return -ENOMEM;

    for (i = 0; i < id_count; i++)
    {
        is_id = TRUE;
        /* is it a path or fid? */
        if (sscanf(id_list[i], SFID, RFID(&ids[i].id)) != FID_SCAN_CNT)
        {
            is_id = FALSE;
            /* take it as a path */
            rc = Path2Id(id_list[i], &ids[i].id);
            if (rc)
            {
                DisplayLog(LVL_MAJOR, FIND_TAG, "Invalid parameter: %s: %s",
                           id_list[i], strerror(-rc));
                goto out;
            }

            ids[i].fullname = id_list[i];
            if (FINAL_SLASH(ids[i].fullname))
                REMOVE_FINAL_SLASH(ids[i].fullname);

        } else {
            /* Take it as an FID. */
            /* TODO: if it's an ID, get the path. And may need to remove
             * trailing slashes, like find does. */
            abort();
        }

        /* get root attrs to print it (if it matches program options) */
        root_attrs.attr_mask = disp_mask | query_mask;
        rc = ListMgr_Get(&lmgr, &ids[i].id, &root_attrs);
        if (rc == 0)
            dircb(&ids[i], &root_attrs, 1, NULL);
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
            else if (entry_id_equal(&ids[i].id, &root_id))
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

            dircb(&ids[i], &root_attrs, 1, NULL);
        }

        rc = rbh_scrub(&lmgr, &ids[i], 1, disp_mask | query_mask, dircb, NULL);
    }

out:
    /* ids have been processed, free them */
    MemFree(ids);
    return rc;
}

#define toggle_option(_opt, _name)              \
            do {                                \
                if (prog_options. _opt )        \
                    fprintf(stderr, "warning: -%s option already specified: will be overridden\n", \
                            _name);             \
                prog_options. _opt = 1;         \
            } while(0)


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
    int            chgd = 0;
    char           err_msg[4096];
    int            neg = 0;
    char           badcfg[RBH_PATH_MAX];

    /* parse command line options */
    while ((c = getopt_long_only(argc, argv, SHORT_OPT_STRING, option_tab,
                            &option_index )) != -1)
    {
        switch ( c )
        {
        case '!':
            neg = 1;
            break;
        case 'u':
            toggle_option(match_user, "user");
            prog_options.user = optarg;
            prog_options.userneg = neg;
            neg = 0;
            break;
        case 'g':
            toggle_option(match_group, "group");
            prog_options.group = optarg;
            prog_options.groupneg = neg;
            neg = 0;
            break;
        case 'n':
            toggle_option(match_name, "name");
            prog_options.name = optarg;
            prog_options.nameneg = neg;
            neg = 0;
            break;
#ifdef _LUSTRE
        case 'o':
            toggle_option(match_ost, "ost");
            prog_options.ost_idx = str2int(optarg);
            if (prog_options.ost_idx == (unsigned int)-1)
            {
                fprintf(stderr, "invalid ost index '%s': unsigned integer expected\n", optarg);
                exit(1);
            }
            if (neg) {
                fprintf(stderr, "! () is not supported for ost criteria\n");
                exit(1);
            }
            break;
#endif
        case 't':
            toggle_option(match_type, "type");
            prog_options.type = opt2type(optarg);
            if (prog_options.type == NULL)
            {
                fprintf(stderr, "invalid type '%s': expected types: "TYPE_HELP".\n", optarg);
                exit(1);
            }
            if (neg) {
                fprintf(stderr, "! (-not) is not supported for type criteria\n");
                exit(1);
            }
            break;
        case 's':
            toggle_option(match_size, "size");
            if (set_size_filter(optarg))
                exit(1);
            if (neg) {
                fprintf(stderr, "! (-not) is not supported for size criteria\n");
                exit(1);
            }
            break;

        case 'A':
            toggle_option(match_atime, "atime/amin");
            if (set_time_filter(optarg, 0, TRUE, atime))
                exit(1);
            if (neg) {
                fprintf(stderr, "! (-not) is not supported for time criteria\n");
                exit(1);
            }
            break;

        case 'a':
            toggle_option(match_atime, "atime/amin");
            if (set_time_filter(optarg, 60, TRUE, atime))
                exit(1);
            if (neg) {
                fprintf(stderr, "! (-not) is not supported for time criteria\n");
                exit(1);
            }
            break;

        case 'C':
            toggle_option(match_ctime, "ctime");
            if (set_time_filter(optarg, 0, TRUE, rh_ctime))
                exit(1);
            if (neg) {
                fprintf(stderr, "! (-not) is not supported for time criteria\n");
                exit(1);
            }
            break;

        case 'M':
            toggle_option(match_mtime, "mtime/mmin/msec");
            if (set_time_filter(optarg, 0, TRUE, mtime))
                exit(1);
            if (neg) {
                fprintf(stderr, "! (-not) is not supported for time criteria\n");
                exit(1);
            }
            break;

        case 'm':
            toggle_option(match_mtime, "mtime/mmin/msec");
            if (set_time_filter(optarg, 60, FALSE, mtime)) /* don't allow suffix (multiplier is 1min) */
                exit(1);
            if (neg) {
                fprintf(stderr, "! (-not) is not supported for time criteria\n");
                exit(1);
            }
            break;

        case 'z':
            toggle_option(match_mtime, "mtime/mmin/msec");
            if (set_time_filter(optarg, 1, FALSE, mtime)) /* don't allow suffix (multiplier is 1sec) */
                exit(1);
            if (neg) {
                fprintf(stderr, "! (-not) is not supported for time criteria\n");
                exit(1);
            }
            break;


#ifdef ATTR_INDEX_status
        case 'S':
            toggle_option(match_status, "status");
            prog_options.status = status2dbval(optarg);
            if ( prog_options.status == (file_status_t)-1 )
            {
                fprintf(stderr, "Unknown status '%s'. Allowed status: %s.\n", optarg,
                        allowed_status());
                exit(1);
            }
            prog_options.statusneg = neg;
            neg = 0;
            break;
#endif
        case 'l':
            prog_options.ls = 1;
            disp_mask = DISPLAY_MASK;
            if (neg) {
                fprintf(stderr, "! (-not) unexpected before -l option\n");
                exit(1);
            }
            break;
        case 'f':
            strncpy( config_file, optarg, MAX_OPT_LEN );
            if (neg) {
                fprintf(stderr, "! (-not) unexpected before -f option\n");
                exit(1);
            }
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
            if (neg) {
                fprintf(stderr, "! (-not) unexpected before -d option\n");
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
    if ( SearchConfig( config_file, config_file, &chgd, badcfg ) != 0 )
    {
        fprintf(stderr, "No config file found matching %s\n", badcfg);
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

    /* Initialize filesystem access */
    rc = InitFS();
    if (rc)
        exit(rc);

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
        /* TODO: find will start from then local dir. Older version of
         * RH would dump the whole database, which we can't do. */
        abort();
    }
    else
    {
        mkfilters(TRUE); /* exclude dirs */
        rc = list_content(argv+optind, argc-optind);
    }

    ListMgr_CloseAccess( &lmgr );

    return rc;

}
