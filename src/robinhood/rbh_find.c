/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009, 2010 CEA/DAM
 * Copyright 2013 Cray Inc. All Rights Reserved.
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
#include "rbh_cfg.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "Memory.h"
#include "xplatform_print.h"
#include "rbh_basename.h"

#include <unistd.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <pthread.h>

#define FIND_TAG "find"

#define LSSTATUS_OPT 260

static struct option option_tab[] =
{
    {"user", required_argument, NULL, 'u'},
    {"group", required_argument, NULL, 'g'},
    {"nouser", no_argument, NULL, 'U'},
    {"nogroup", no_argument, NULL, 'G'},
    {"type", required_argument, NULL, 't'},
    {"size", required_argument, NULL, 's'},
    {"name", required_argument, NULL, 'n'},
    {"mtime", required_argument, NULL, 'M'},
    {"crtime", required_argument, NULL, 'C'},
    {"mmin", required_argument, NULL, 'm'},
    {"msec", required_argument, NULL, 'z'},
    {"atime", required_argument, NULL, 'A'},
    {"amin", required_argument, NULL, 'a'},
    {"status", required_argument, NULL, 'S'},
#ifdef _LUSTRE
    {"ost", required_argument, NULL, 'o'},
    {"pool", required_argument, NULL, 'P'},
    {"lsost", no_argument, NULL, 'O'},
#endif
    {"lsclass", no_argument, NULL, 'c'},
    {"lsstatus", optional_argument, NULL, LSSTATUS_OPT},

    {"ls", no_argument, NULL, 'l'},
    {"print", no_argument, NULL, 'p'},
    {"exec", required_argument, NULL, 'E'},
    /* TODO dry-run mode for exec ? */

    /* query options */
    {"not", no_argument, NULL, '!'},
    {"nobulk", no_argument, NULL, 'b'},

    /* config file options */
    {"config-file", required_argument, NULL, 'f'},

    /* log options */
    {"debug-level", required_argument, NULL, 'd'},

    /* miscellaneous options */
    {"help", no_argument, NULL, 'h'},
    {"version", no_argument, NULL, 'V'},

    {NULL, 0, NULL, 0}

};

#define SHORT_OPT_STRING    "lpOu:g:t:s:n:S:o:P:E:A:M:C:m:z:f:d:hV!bUGc"

#define TYPE_HELP "'f' (file), 'd' (dir), 'l' (symlink), 'b' (block), 'c' (char), 'p' (named pipe/FIFO), 's' (socket)"
#define SIZE_HELP "[-|+]<val>[K|M|G|T]"
#define TIME_HELP "[-|+]<val>[s|m|h|d|y] (s: sec, m: min, h: hour, d:day, y:year. default unit is days)"

/* global variables */

static lmgr_t  lmgr;

/* program options */
struct find_opt
{
    const char  *user;
    const char  *group;
    const char  *type;
    // size cond: gt/eq/lt <val>
    compare_direction_t sz_compar;
    uint64_t            sz_val;
    const char  *name;
    unsigned int ost_idx;
    const char  *pool;
    /* status manager for -lsstatus */
    const char  *lsstatus_name;
    sm_instance_t *smi;

    /* status name and value for -status */
    sm_instance_t *filter_smi;
    char  *filter_status_name;
    char  *filter_status_value;

    // crtime cond: gt/eq/lt <time>
    compare_direction_t crt_compar;
    time_t              crt_val;

    // mtime cond: gt/eq/lt <time>
    compare_direction_t mod_compar;
    time_t              mod_val;

    // atime cond: gt/eq/lt <time>
    compare_direction_t acc_compar;
    time_t              acc_val;

    const char * exec_cmd;

    /* query option */
    enum { bulk_unspec = 0,
           force_bulk,
           force_nobulk
    } bulk;

    /* output flags */
    unsigned int ls:1;
    unsigned int lsost:1;
    unsigned int lsclass:1;
    unsigned int lsstatus:1;
    unsigned int print:1;
    /* condition flags */
    unsigned int match_user:1;
    unsigned int match_group:1;
    unsigned int match_type:1;
    unsigned int match_size:1;
    unsigned int match_name:1;
    unsigned int match_crtime:1;
    unsigned int match_mtime:1;
    unsigned int match_atime:1;
#ifdef _LUSTRE
    unsigned int match_ost:1;
    unsigned int match_pool:1;
#endif
    unsigned int match_status:1;
    unsigned int statusneg:1;

    /* -not flags */
    unsigned int userneg:1;
    unsigned int groupneg:1;
    unsigned int nameneg:1;

    /* behavior flags */
    unsigned int no_dir:1; /* if -t != dir => no dir to be displayed */
    unsigned int dir_only:1; /* if -t dir => only display dir */

    /* actions */
    unsigned int exec:1;

} prog_options = {
    .user = NULL, .group = NULL, .type = NULL, .name = NULL,
    .lsstatus_name = NULL, .smi = NULL,
#ifdef _LUSTRE
    .match_ost = 0, .match_pool = 0, .pool = NULL,
#endif
    .filter_smi = NULL, .filter_status_name = NULL, .filter_status_value = NULL,
    .bulk = bulk_unspec,
    .ls = 0, .lsost = 0, .lsclass = 0, .lsstatus = 0, .print = 1,
    .match_user = 0, .match_group = 0,
    .match_type = 0, .match_size = 0, .match_name = 0,
    .match_crtime = 0, .match_mtime = 0, .match_atime = 0,
    .match_status = 0, .statusneg = 0,
    .userneg = 0 , .groupneg = 0, .nameneg = 0,
    .no_dir = 0, .dir_only = 0, .exec = 0
};

static const attr_mask_t LS_DISPLAY_MASK = {.std = ATTR_MASK_type
                                       | ATTR_MASK_nlink
                                       | ATTR_MASK_mode | ATTR_MASK_owner
                                       | ATTR_MASK_gr_name | ATTR_MASK_size
                                       | ATTR_MASK_last_mod | ATTR_MASK_link};

static const attr_mask_t LSOST_DISPLAY_MASK = {.std = ATTR_MASK_type
                                     | ATTR_MASK_size | ATTR_MASK_stripe_items};
static const attr_mask_t LSCLASS_DISPLAY_MASK = {.std = ATTR_MASK_type
                                     | ATTR_MASK_size | ATTR_MASK_fileclass};

static attr_mask_t disp_mask = {.std = ATTR_MASK_type};
static attr_mask_t query_mask = {0};

//static lmgr_filter_t    dir_filter;

/* for filtering entries from DB */
static lmgr_filter_t    entry_filter;

/* post filter for all entries */
static bool_node_t      match_expr;
static int              is_expr = 0; /* is it set? */

/* build filters depending on program options */
static int mkfilters(bool exclude_dirs)
{
    filter_value_t fv;
    int compflag;

    /* Create boolean expression for matching.
     * All expressions are then converted to a DB filter.
     */

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
        query_mask.std |= ATTR_MASK_owner;
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
        query_mask.std |= ATTR_MASK_gr_name;
    }

    if (prog_options.match_name)
    {
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
        query_mask.std |= ATTR_MASK_name;
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
        query_mask.std |= ATTR_MASK_size;
    }

    if (prog_options.match_crtime)
    {
        compare_value_t val;
        val.duration = prog_options.crt_val;
        if (!is_expr)
            CreateBoolCond(&match_expr, prog_options.crt_compar, CRITERIA_CREATION, val);
        else
            AppendBoolCond(&match_expr, prog_options.crt_compar, CRITERIA_CREATION, val);
        is_expr = 1;
        query_mask.std |= ATTR_MASK_creation_time;
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
        query_mask.std |= ATTR_MASK_last_mod;
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
        query_mask.std |= ATTR_MASK_last_access;
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
        query_mask.std |= ATTR_MASK_stripe_items;
    }

    if (prog_options.match_pool)
    {
        compare_value_t val;
        strcpy(val.str, prog_options.pool);
        if (!is_expr)
            CreateBoolCond(&match_expr, COMP_LIKE, CRITERIA_POOL, val);
        else
            AppendBoolCond(&match_expr, COMP_LIKE, CRITERIA_POOL, val);
        is_expr = 1;
        query_mask.std |= ATTR_MASK_stripe_info;
    }
#endif

    if (prog_options.match_status)
    {
        compare_value_t val;

        strcpy(val.str, prog_options.filter_status_value);

        if (prog_options.statusneg)
            compflag = COMP_DIFF;
        else
            compflag = COMP_EQUAL;

        if (!is_expr)
            CreateBoolCond(&match_expr, compflag, CRITERIA_STATUS, val);
        else
            AppendBoolCond(&match_expr, compflag, CRITERIA_STATUS, val);

        is_expr = 1;
        query_mask.status |= SMI_MASK(prog_options.filter_smi->smi_index);
    }

    /* create DB filters */
    lmgr_simple_filter_init(&entry_filter);

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
        lmgr_simple_filter_add(&entry_filter, ATTR_INDEX_type, NOTEQUAL, fv, 0);
    }

    if (is_expr)
    {
        char expr[RBH_PATH_MAX];
        /* for debug */
        if (BoolExpr2str(&match_expr, expr, RBH_PATH_MAX)>0)
            DisplayLog(LVL_FULL, FIND_TAG, "Expression matching: %s", expr);

        /* append bool expr to entry filter */
        /* Do not use 'OR' expression there */
        convert_boolexpr_to_simple_filter(&match_expr, &entry_filter,
                                          prog_options.filter_smi, NULL, 0);
    }

    return 0;
}

static const char *help_string =
    _B "Usage:" B_ " %s [options] [path|fid]...\n"
    "\n"
    _B "Filters:" B_ "\n"
    "    " _B "-user" B_ " " _U "user" U_ "\n"
    "    " _B "-group" B_ " " _U "group" U_ "\n"
    "    " _B "-nouser" B_ "\n"
    "    " _B "-nogroup" B_ "\n"
    "    " _B "-type" B_ " " _U "type" U_ "\n"
    "       "TYPE_HELP"\n"
    "    " _B "-size" B_ " " _U "size_crit" U_ "\n"
    "       "SIZE_HELP"\n"
    "    " _B "-name" B_ " " _U "filename" U_ "\n"
    "    " _B "-crtime" B_ " " _U "time_crit" U_ "\n"
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
    "        same as '-atime "_U"N"U_"m'\n"
#ifdef _LUSTRE
    "    " _B "-ost" B_ " " _U "ost_index" U_ "\n"
    "    " _B "-pool" B_ " " _U "ost_pool" U_ "\n"
#endif
    "    " _B "-status" B_ " " _U "status_name"U_":"_U"status_value" U_ "\n"
    "\n"
    "    " _B "-not" B_ ", "_B"-!"B_" \t Negate next argument\n"
    "\n"
    _B "Output options:" B_ "\n"
    "    " _B "-ls" B_" \t Display attributes\n"
#ifdef _LUSTRE
    "    " _B "-lsost" B_" \t Display OST information\n"
#endif
    "    " _B "-lsclass" B_" \t Display fileclass information\n"
    "    " _B "-lsstatus" B_"[="_U"policy"U_"] \t Display status information (optionally: only for the given "_U"policy"U_").\n"
    "    " _B "-print" B_" \t Display the fullpath of matching entries (this is the default, unless -ls, -lsost or -exec are used).\n"
    "\n"
    _B "Actions:" B_ "\n"
    "    " _B "-exec" B_" "_U "\"cmd\"" U_ "\n"
    "       Execute the given command for each matching entry. Unlike classical 'find',\n"
    "       cmd must be a single (quoted) shell param, not necessarily terminated with ';'.\n"
    "       '{}' is replaced by the entry path. Example: -exec 'md5sum {}'\n"
    "\n"
    _B "Behavior:"B_"\n"
    "    " _B "-nobulk" B_ "\n"
    "       When running rbh-find on the filesystem root, rbh-find automatically switches\n"
    "       to bulk DB request instead of browsing the namespace from the DB.\n"
    "       This speeds up the query, but this may result in an arbitrary output ordering,\n"
    "       and a single path may be displayed in case of multiple hardlinks.\n"
    "       Use -nobulk to disable this optimization.\n"
    "\n"
    _B "Program options:" B_ "\n"
    "    " _B "-f" B_ " " _U "config_file" U_ "\n"
    "    " _B "-d" B_ " " _U "log_level" U_ "\n"
    "       CRIT, MAJOR, EVENT, VERB, DEBUG, FULL\n"
    "    " _B "-h" B_ ", " _B "--help" B_ "\n"
    "        Display a short help about command line options.\n"
    "    " _B "-V" B_ ", " _B "--version" B_ "\n"
    "        Display version info\n";

static inline void display_help(const char *bin_name)
{
    printf(help_string, bin_name);
}

static inline void display_version(const char *bin_name)
{
    printf("\n");
    printf("Product:         " PACKAGE_NAME " 'find' command\n");
    printf("Version:         " PACKAGE_VERSION "-"RELEASE"\n");
    printf("Build:           " COMPIL_DATE "\n");
    printf("\n");
    printf("Compilation switches:\n");

/* Access by Fid ? */
#ifdef _HAVE_FID
    printf("    Address entries by FID\n");
#else
    printf("    Address entries by path\n");
#endif

#ifdef HAVE_CHANGELOGS
    printf("    MDT Changelogs supported\n");
#else
    printf("    MDT Changelogs disabled\n");
#endif

    printf("\n");
#ifdef _LUSTRE
#ifdef LUSTRE_VERSION
    printf("Lustre Version: " LUSTRE_VERSION "\n");
#else
    printf("Lustre FS support\n");
#endif
#else
    printf("No Lustre support\n");
#endif

#ifdef _MYSQL
    printf("Database binding: MySQL\n");
#elif defined(_SQLITE)
    printf("Database binding: SQLite\n");
#else
#error "No database was specified"
#endif
    printf("\n");
    printf("Report bugs to: <" PACKAGE_BUGREPORT ">\n");
    printf("\n");
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

typedef enum {atime, rh_crtime, mtime} e_time;
/* parse time filter and set prog_options struct */
static int set_time_filter(char * str, unsigned int multiplier,
                           bool allow_suffix, e_time what)
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
        if (what == rh_crtime)
        {
            prog_options.crt_compar = comp;
            if (multiplier != 0)
                prog_options.crt_val = val * multiplier;
            else /* default multiplier is days */
                prog_options.crt_val =  val * 86400;
        }
        else
        if (what == mtime)
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
        if (what == rh_crtime)
        {
            prog_options.crt_compar = comp;
            prog_options.crt_val = val;
        }
        else
        if (what == mtime)
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
    char ostbuf[24576] = "";
    char classbuf[1024] = "";
    char statusbuf[1024] = "";

    /* HERE: post-filter attributes that are not part of the DB request */

#ifdef _LUSTRE
    /* prepare OST display buffer */
    if (prog_options.lsost && ATTR_MASK_TEST(attrs, stripe_items)
        && (ATTR(attrs, stripe_items).count > 0))
    {
        /* leave 2 spaces as first char */
        ostbuf[0] = ostbuf[1] = ' ';
        FormatStripeList(ostbuf+2, sizeof(ostbuf)-2, &ATTR(attrs, stripe_items), true);
    }
#endif

    /* prepare class display buffer */
    if (prog_options.lsclass)
    {
        /* leave a space before and after */
        snprintf(classbuf, sizeof(classbuf), " %-20s ",
                 class_format(ATTR_MASK_TEST(attrs, fileclass)?
                              ATTR(attrs, fileclass) : NULL));
    }

    /* prepare status display buffer */
    if (prog_options.lsstatus)
    {
        /* if a status is specified: display it */
        if (prog_options.smi)
        {
            /* if matching a status != lsstatus: display both (filter first) */
            if (prog_options.match_status)
            {
                snprintf(statusbuf, sizeof(statusbuf), " %s:%s,%s:%s ",
                         prog_options.filter_smi->instance_name,
                    status_format(ATTR_MASK_STATUS_TEST(attrs, prog_options.filter_smi->smi_index)?
                                  STATUS_ATTR(attrs, prog_options.filter_smi->smi_index):NULL),
                         prog_options.smi->instance_name,
                    status_format(ATTR_MASK_STATUS_TEST(attrs, prog_options.smi->smi_index)?
                                  STATUS_ATTR(attrs, prog_options.smi->smi_index):NULL));
            }
            else /* just the requested lsstatus, with no prefix */
                snprintf(statusbuf, sizeof(statusbuf), " %-15s ",
                    status_format(ATTR_MASK_STATUS_TEST(attrs, prog_options.smi->smi_index)?
                                  STATUS_ATTR(attrs, prog_options.smi->smi_index):NULL));
        }
        else
        {
            int i;
            char *curr = statusbuf;
            int remain = sizeof(statusbuf);

            /* if no status is specified: display them all
             * (no extra display for filter_status in this case) */
            for (i = 0; i < sm_inst_count && remain > 0; i++)
            {
                curr += snprintf(curr, remain, "%s%s:%s",
                                 i == 0 ? " " : ",",
                                 get_sm_instance(i)->instance_name,
                                 status_format(ATTR_MASK_STATUS_TEST(attrs, i)?
                                               STATUS_ATTR(attrs, i):NULL));
                remain = (ptrdiff_t)(sizeof(statusbuf) - (curr - statusbuf));
            }
            strncat(curr, " ", remain);
        }
    }
    else if (prog_options.filter_smi)
    {
        /* just the matched status, with no prefix */
        snprintf(statusbuf, sizeof(statusbuf), " %-15s ",
            status_format(ATTR_MASK_STATUS_TEST(attrs, prog_options.filter_smi->smi_index)?
                          STATUS_ATTR(attrs, prog_options.filter_smi->smi_index):NULL));
    }

    if (prog_options.ls)
    {
        const char * type;
        char date_str[128];
        char mode_str[128];

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
            printf(DFID" %-4s %s %3u  %-10s %-10s %15"PRIu64" %20s %s%s%s -> %s\n",
                   PFID(&id->id), type, mode_str, ATTR(attrs, nlink),
                   ATTR(attrs, owner), ATTR(attrs, gr_name),
                   ATTR(attrs, size), date_str, statusbuf, classbuf, id->fullname, ATTR(attrs,link));
        else
            /* display all: id, type, mode, nlink, (status,) owner, group, size, mtime, path */
            printf(DFID" %-4s %s %3u  %-10s %-10s %15"PRIu64" %20s %s%s%s%s\n",
                   PFID(&id->id), type, mode_str, ATTR(attrs, nlink),
                   ATTR(attrs, owner), ATTR(attrs, gr_name),
                   ATTR(attrs, size), date_str, statusbuf, classbuf, id->fullname, ostbuf);
    }
    else if (prog_options.lsost || prog_options.lsclass || prog_options.lsstatus) /* lsost or lsclass without -ls */
    {
        const char * type;

        /* type2char */
        if (!ATTR_MASK_TEST(attrs, type))
            type = "?";
        else
            type = type2char(ATTR(attrs, type));

        /* display: id, type, size, path */
        printf(DFID" %-4s %15"PRIu64" %s%s%s%s\n",
               PFID(&id->id), type, ATTR(attrs, size), statusbuf, classbuf, id->fullname, ostbuf);

    }
    else if (prog_options.print)
    {
        /* just display name */
        if (id->fullname)
            printf("%s\n", id->fullname);
        else
            printf(DFID"\n", PFID(&id->id));
    }

    if (prog_options.exec)
    {
        const char *vars[] = {
            "", id->fullname,
            NULL, NULL
        };
        gchar *cmd = subst_params(prog_options.exec_cmd, "exec option",
                                  &id->id, attrs, NULL, vars, true, true);
        if (cmd)
        {
            /* display both stdout and stderr */
            execute_shell_command(cmd, cb_redirect_all, NULL);
            g_free(cmd);
        }
    }
}

/* directory callback */
static int dircb(wagon_t * id_list, attr_set_t * attr_list,
                 unsigned int entry_count, void * dummy)
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
        if (!is_expr || (entry_matches(&id_list[i].id, &attr_list[i],
                                      &match_expr, NULL, prog_options.filter_smi)
                         == POLICY_MATCH))
        {
            /* don't display dirs if no_dir is specified */
            if (! (prog_options.no_dir && ATTR_MASK_TEST(&attr_list[i], type)
                   && !strcasecmp(ATTR(&attr_list[i], type), STR_TYPE_DIR)))
                print_entry(&id_list[i], &attr_list[i]);
        }

        if (!prog_options.dir_only)
        {
            rc = ListMgr_GetChild(&lmgr, &entry_filter, id_list+i, 1,
                                   attr_mask_or(&disp_mask, &query_mask),
                                   &chids, &chattrs, &chcount);
            if (rc)
            {
                DisplayLog(LVL_MAJOR, FIND_TAG, "ListMgr_GetChild() failed with error %d", rc);
                return rc;
            }

            for (j = 0; j < chcount; j++)
            {
                if (!is_expr || (entry_matches(&chids[j].id, &chattrs[j],
                                 &match_expr, NULL, prog_options.filter_smi)
                                 == POLICY_MATCH))
                    print_entry(&chids[j], &chattrs[j]);

                ListMgr_FreeAttrs(&chattrs[j]);
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
static int retrieve_root_id(entry_id_t *root_id)
{
    int rc;
    rc = Path2Id(global_config.fs_path, root_id);
    if (rc)
        DisplayLog(LVL_MAJOR, FIND_TAG, "Can't access filesystem's root %s: %s",
                   global_config.fs_path, strerror(-rc));
    return rc;
}

/**
 * Bulk filtering in the DB.
 */
static int list_bulk(void)
{
    attr_set_t  root_attrs, attrs;
    entry_id_t  root_id, id;
    int rc;
    struct stat st;
    struct lmgr_iterator_t *it;

    /* no tranvsersal => no wagon
     * so we need the path from the DB.
     */
    query_mask.std |= ATTR_MASK_fullpath;

    ATTR_MASK_INIT(&root_attrs);

    rc = retrieve_root_id(&root_id);
    if (rc)
        memset(&root_id, 0, sizeof(root_id));

    /* root is not a part of the DB: print it now */
    ATTR_MASK_SET(&root_attrs, fullpath);
    strcpy(ATTR(&root_attrs, fullpath), global_config.fs_path);

    if (lstat(ATTR(&root_attrs, fullpath), &st) == 0)
    {
        stat2rbh_attrs(&st, &root_attrs, true);
        ListMgr_GenerateFields(&root_attrs, attr_mask_or(&disp_mask, &query_mask));
    }
    /* root has no name... */
    ATTR_MASK_SET(&root_attrs, name);
    ATTR(&root_attrs, name)[0] = '\0';

    /* match condition on dirs parent */
    if (!is_expr || (entry_matches(&root_id, &root_attrs,
                     &match_expr, NULL, prog_options.filter_smi) == POLICY_MATCH))
    {
        /* don't display dirs if no_dir is specified */
        if (! (prog_options.no_dir && ATTR_MASK_TEST(&root_attrs, type)
               && !strcasecmp(ATTR(&root_attrs, type), STR_TYPE_DIR))) {
            wagon_t w;
            w.id = root_id;
            w.fullname = ATTR(&root_attrs, fullpath);
            print_entry(&w, &root_attrs);
        }
    }

    /* list all, including dirs */
    it = ListMgr_Iterator(&lmgr, &entry_filter, NULL, NULL);
    if (!it)
    {
        DisplayLog(LVL_MAJOR, FIND_TAG, "ERROR: cannot retrieve entry list from database");
        return -1;
    }

    attrs.attr_mask = attr_mask_or(&disp_mask, &query_mask);
    while ((rc = ListMgr_GetNext(it, &id, &attrs)) == DB_SUCCESS)
    {
        if (!is_expr || (entry_matches(&id, &attrs, &match_expr, NULL,
                                       prog_options.filter_smi) == POLICY_MATCH))
        {
            /* don't display dirs if no_dir is specified */
            if (! (prog_options.no_dir && ATTR_MASK_TEST(&attrs, type)
                   && !strcasecmp(ATTR(&attrs, type), STR_TYPE_DIR))) {
                wagon_t w;
                w.id = id;
                w.fullname = ATTR(&attrs, fullpath);
                print_entry(&w, &attrs);
            }
            /* don't display non dirs is dir_only is specified */
            else if (! (prog_options.dir_only && ATTR_MASK_TEST(&attrs, type)
                        && strcasecmp(ATTR(&attrs, type), STR_TYPE_DIR))) {
                wagon_t w;
                w.id = id;
                w.fullname = ATTR(&attrs, fullpath);
                print_entry(&w, &attrs);
            }
            else
                /* return entry don't match? */
                DisplayLog(LVL_DEBUG, FIND_TAG, "Warning: returned DB entry doesn't match filter: %s",
                           ATTR(&attrs, fullpath));
        }
        ListMgr_FreeAttrs(&attrs);

        /* prepare next call */
        attrs.attr_mask = attr_mask_or(&disp_mask, &query_mask);
    }
    ListMgr_CloseIterator(it);

    return 0;
}

/**
 * List contents of the given id/path list
 */
static int list_contents(char ** id_list, int id_count)
{
    wagon_t *ids;
    int i, rc;
    attr_set_t root_attrs;
    entry_id_t root_id;
    bool is_id;

    rc = retrieve_root_id(&root_id);
    if (rc)
        return rc;

    ids = MemCalloc(id_count, sizeof(wagon_t));
    if (!ids)
        return -ENOMEM;

    for (i = 0; i < id_count; i++)
    {
        is_id = true;
        /* is it a path or fid? */
        if (sscanf(id_list[i], SFID, RFID(&ids[i].id)) != FID_SCAN_CNT)
        {
            is_id = false;
            /* take it as a path */
            rc = Path2Id(id_list[i], &ids[i].id);
            if (!rc) {
                ids[i].fullname = id_list[i];
                if (FINAL_SLASH(ids[i].fullname))
                    REMOVE_FINAL_SLASH(ids[i].fullname);
            }
        } else {
#if _HAVE_FID
            /* Take it as an FID. */
            char path[RBH_PATH_MAX];
            rc = Lustre_GetFullPath(&ids[i].id, path, sizeof(path));
            if (!rc)
                ids[i].fullname = strdup(path);
#endif
        }

        if (rc) {
            DisplayLog(LVL_MAJOR, FIND_TAG, "Invalid parameter: %s: %s",
                       id_list[i], strerror(-rc));
            goto out;
        }

        if ((prog_options.bulk != force_nobulk) &&
            (id_count == 1) && entry_id_equal(&ids[i].id, &root_id))
        {
            /* the ID is FS root: use list_bulk instead */
            DisplayLog(LVL_DEBUG, FIND_TAG, "Optimization: switching to bulk DB request mode");
            mkfilters(false); /* keep dirs */
            MemFree(ids);
            return list_bulk();
        }

        /* get root attrs to print it (if it matches program options) */
        root_attrs.attr_mask = attr_mask_or(&disp_mask, &query_mask);
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

                if (lstat(ATTR(&root_attrs, fullpath), &st) == 0)
                {
                    stat2rbh_attrs(&st, &root_attrs, true);
                    ListMgr_GenerateFields(&root_attrs,
                                         attr_mask_or(&disp_mask, &query_mask));
                }
            }
            else if (entry_id_equal(&ids[i].id, &root_id))
            {
                /* this is root id */
                struct stat st;
                ATTR_MASK_SET(&root_attrs, fullpath);
                strcpy(ATTR(&root_attrs, fullpath), global_config.fs_path);

                if (lstat(ATTR(&root_attrs, fullpath), &st) == 0)
                {
                    stat2rbh_attrs(&st, &root_attrs, true);
                    ListMgr_GenerateFields(&root_attrs,
                                         attr_mask_or(&disp_mask, &query_mask));
                }
            }

            dircb(&ids[i], &root_attrs, 1, NULL);
        }

        rc = rbh_scrub(&lmgr, &ids[i], 1, attr_mask_or(&disp_mask, &query_mask),
                       dircb, NULL);
    }

out:
    /* ids have been processed, free them */
    MemFree(ids);
    return rc;
}


#define toggle_option(_opt, _name)              \
            do {                                \
                if (prog_options. _opt)        \
                    fprintf(stderr, "warning: -%s option already specified: will be overridden\n", \
                            _name);             \
                prog_options. _opt = 1;         \
            } while(0)


#define MAX_OPT_LEN 1024

/**
 * Main daemon routine
 */
int main(int argc, char **argv)
{
    int            c, option_index = 0;
    const char    *bin;
    char           config_file[MAX_OPT_LEN] = "";
    bool           force_log_level = false;
    int            log_level = 0;
    int            rc;
    char           err_msg[4096];
    bool           chgd = false;
    char           badcfg[RBH_PATH_MAX];
    bool           neg = false;

    bin = rh_basename(argv[0]);

    /* parse command line options */
    while ((c = getopt_long_only(argc, argv, SHORT_OPT_STRING, option_tab,
                            &option_index)) != -1)
    {
        switch (c)
        {
        case '!':
            neg = true;
            break;

        case 'u':
            toggle_option(match_user, "user");
            prog_options.user = optarg;
            prog_options.userneg = neg;
            neg = false;
            break;

        case 'g':
            toggle_option(match_group, "group");
            prog_options.group = optarg;
            prog_options.groupneg = neg;
            neg = false;
            break;

        case 'U': /* match numerical (non resolved) users */
            toggle_option(match_user, "user");
            prog_options.user = "[0-9]*";
            prog_options.userneg = neg;
            neg = false;
            break;

        case 'G': /* match numerical (non resolved) groups */
            toggle_option(match_group, "group");
            prog_options.group = "[0-9]*";
            prog_options.groupneg = neg;
            neg = false;
            break;

        case 'n':
            toggle_option(match_name, "name");
            prog_options.name = optarg;
            prog_options.nameneg = neg;
            neg = false;
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

        case 'P':
            toggle_option(match_pool, "pool");
            prog_options.pool = optarg;
            break;

        case 'O':
            prog_options.lsost = 1;
            prog_options.print = 0;
            disp_mask = attr_mask_or(&disp_mask, &LSOST_DISPLAY_MASK);
            if (neg) {
                fprintf(stderr, "! (-not) unexpected before -lsost option\n");
                exit(1);
            }
            break;
#endif

        case 'c':
            prog_options.lsclass = 1;
            prog_options.print = 0;
            disp_mask = attr_mask_or(&disp_mask, &LSCLASS_DISPLAY_MASK);
            if (neg) {
                fprintf(stderr, "! (-not) unexpected before -lsclass option\n");
                exit(1);
            }
            break;

        case LSSTATUS_OPT:
            prog_options.lsstatus = 1;
            prog_options.print = 0;
            prog_options.lsstatus_name = optarg;
            if (neg) {
                fprintf(stderr, "! (-not) unexpected before -lsstatus option\n");
                exit(1);
            }
            break;

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
            if (set_time_filter(optarg, 0, true, atime))
                exit(1);
            if (neg) {
                fprintf(stderr, "! (-not) is not supported for time criteria\n");
                exit(1);
            }
            break;

        case 'a':
            toggle_option(match_atime, "atime/amin");
            if (set_time_filter(optarg, 60, true, atime))
                exit(1);
            if (neg) {
                fprintf(stderr, "! (-not) is not supported for time criteria\n");
                exit(1);
            }
            break;

        case 'C':
            toggle_option(match_crtime, "crtime");
            if (set_time_filter(optarg, 0, true, rh_crtime))
                exit(1);
            if (neg) {
                fprintf(stderr, "! (-not) is not supported for time criteria\n");
                exit(1);
            }
            break;

        case 'M':
            toggle_option(match_mtime, "mtime/mmin/msec");
            if (set_time_filter(optarg, 0, true, mtime))
                exit(1);
            if (neg) {
                fprintf(stderr, "! (-not) is not supported for time criteria\n");
                exit(1);
            }
            break;

        case 'm':
            toggle_option(match_mtime, "mtime/mmin/msec");
            if (set_time_filter(optarg, 60, false, mtime)) /* don't allow suffix (multiplier is 1min) */
                exit(1);
            if (neg) {
                fprintf(stderr, "! (-not) is not supported for time criteria\n");
                exit(1);
            }
            break;

        case 'z':
            toggle_option(match_mtime, "mtime/mmin/msec");
            if (set_time_filter(optarg, 1, false, mtime)) /* don't allow suffix (multiplier is 1sec) */
                exit(1);
            if (neg) {
                fprintf(stderr, "! (-not) is not supported for time criteria\n");
                exit(1);
            }
            break;


        case 'S':
            toggle_option(match_status, "status");
            rc = parse_status_arg("-status", optarg, &prog_options.filter_status_name,
                                  &prog_options.filter_status_value, true);
            if (rc)
                exit(rc);
            prog_options.statusneg = neg;
            neg = false;
            break;

        case 'l':
            prog_options.ls = 1;
            prog_options.print = 0;
            disp_mask = attr_mask_or(&disp_mask, &LS_DISPLAY_MASK);
            if (neg) {
                fprintf(stderr, "! (-not) unexpected before -ls option\n");
                exit(1);
            }
            break;

        case 'p':
            prog_options.print = 1;
            disp_mask = attr_mask_or(&disp_mask, &LS_DISPLAY_MASK);
            if (neg) {
                fprintf(stderr, "! (-not) unexpected before -ls option\n");
                exit(1);
            }
            break;

        case 'E':
            toggle_option(exec, "exec");
            prog_options.exec_cmd = optarg;
            prog_options.print = 0;
            break;

        case 'f':
            rh_strncpy(config_file, optarg, MAX_OPT_LEN);
            if (neg) {
                fprintf(stderr, "! (-not) unexpected before -f option\n");
                exit(1);
            }
            break;

        case 'd':
            force_log_level = true;
            log_level = str2debuglevel(optarg);
            if (log_level == -1)
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

        case 'b':
            prog_options.bulk = force_nobulk;
            break;

        case 'h':
            display_help(bin);
            exit(0);
            break;

        case 'V':
            display_version(bin);
            exit(0);
            break;

        case ':':
        case '?':
        default:
            display_help(bin);
            exit(1);
            break;
        }
    }

    rc = rbh_init_internals();
    if (rc != 0)
        exit(rc);

    /* get default config file, if not specified */
    if (SearchConfig(config_file, config_file, &chgd, badcfg, MAX_OPT_LEN) != 0)
    {
        fprintf(stderr, "No config file (or too many) found matching %s\n", badcfg);
        exit(2);
    }
    else if (chgd)
    {
        fprintf(stderr, "Using config file '%s'.\n", config_file);
    }

    /* only read common config (listmgr, ...) (mask=0) */
    if(rbh_cfg_load(0, config_file, err_msg))
    {
        fprintf(stderr, "Error reading configuration file '%s': %s\n",
                config_file, err_msg);
        exit(1);
    }

    if (force_log_level)
        log_config.debug_level = log_level;
    else
        log_config.debug_level = LVL_MAJOR; /* no event message */

    /* Set logging to stderr */
    strcpy(log_config.log_file, "stderr");
    strcpy(log_config.report_file, "stderr");
    strcpy(log_config.alert_file, "stderr");

    /* Initialize logging */
    rc = InitializeLogs(bin);
    if (rc)
    {
        fprintf(stderr, "Error opening log files: rc=%d, errno=%d: %s\n",
                 rc, errno, strerror(errno));
        exit(rc);
    }

    /* Initialize filesystem access */
    rc = InitFS();
    if (rc)
        fprintf(stderr, "WARNING: cannot access filesystem %s (%s), find output may be incomplete.\n",
                global_config.fs_path, strerror(abs(rc)));

    /* Initialize list manager (report only) */
    rc = ListMgr_Init(true);
    if (rc)
    {
        DisplayLog(LVL_CRIT, FIND_TAG, "Error %d initializing list manager", rc);
        exit(rc);
    }
    else
        DisplayLog(LVL_DEBUG, FIND_TAG, "ListManager successfully initialized");

    if (CheckLastFS() != 0)
        exit(1);

    /* Create database access */
    rc = ListMgr_InitAccess(&lmgr);
    if (rc)
    {
        DisplayLog(LVL_CRIT, FIND_TAG, "Error %d: cannot connect to database", rc);
        exit(rc);
    }

    /* manage status args:
     * lsstatus: check optional argument
     *           set the display mask appropriately.
     */
    if (prog_options.lsstatus)
    {
        if (prog_options.lsstatus_name)
        {
            const char *dummy;
            rc = check_status_args(prog_options.lsstatus_name, NULL, &dummy,
                                   &prog_options.smi);
            if (rc)
                exit(rc);
            disp_mask.status |= SMI_MASK(prog_options.smi->smi_index);
        }
        else /* display all status */
            disp_mask.status |= all_status_mask();
    }

    if (prog_options.match_status)
    {
        const char *strval;

        rc = check_status_args(prog_options.filter_status_name,
                               prog_options.filter_status_value, &strval,
                               &prog_options.filter_smi);
        if (rc)
            exit(rc);
        /* add it to display mask */
        disp_mask.status |= SMI_MASK(prog_options.filter_smi->smi_index);
        prog_options.filter_status_value = (char *)strval;
    }

    if (argc == optind)
    {
        /* no argument: default is root
         * => switch to bulk mode (unless nobulk is specified)
         */
        if (prog_options.bulk != force_nobulk)
        {
            DisplayLog(LVL_DEBUG, FIND_TAG, "Optimization: switching to bulk DB request mode");
            mkfilters(false); /* keep dirs */
            return list_bulk();
        }
        else
        {
            char *id = global_config.fs_path;
            mkfilters(true); /* exclude dirs */
            /* no path specified, list all entries */
            rc = list_contents(&id, 1);
        }
    }
    else
    {
        mkfilters(true); /* exclude dirs */
        rc = list_contents(argv+optind, argc-optind);
    }

    ListMgr_CloseAccess(&lmgr);

    return rc;
}
