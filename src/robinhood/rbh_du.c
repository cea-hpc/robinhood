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

#define DU_TAG "du"

static struct option option_tab[] = {
    {"user", required_argument, NULL, 'u'},
    {"group", required_argument, NULL, 'g'},
    {"type", required_argument, NULL, 't'},
    {"status", required_argument, NULL, 'S'},

    /* output options */
    {"sum", no_argument, NULL, 's'},
    {"count", no_argument, NULL, 'c'},
    {"bytes", no_argument, NULL, 'b'},
    {"kilo", no_argument, NULL, 'k'},
    {"mega", no_argument, NULL, 'm'},
    {"human-readable", no_argument, NULL, 'H'},
    {"details", no_argument, NULL, 'd'},

    /* config file options */
    {"config-file", required_argument, NULL, 'f'},

    /* log options */
    {"log-level", required_argument, NULL, 'l'},

    /* miscellaneous options */
    {"help", no_argument, NULL, 'h'},
    {"version", no_argument, NULL, 'V'},

    {NULL, 0, NULL, 0}

};

#define SHORT_OPT_STRING    "u:g:t:S:scbkmHdf:l:hV"
#define TYPE_HELP "'f' (file), 'd' (dir), 'l' (symlink), 'b' (block), "\
                  "'c' (char), 'p' (named pipe/FIFO), 's' (socket)"

/* global variables */

static lmgr_t lmgr;

typedef enum { disp_usage, disp_count, disp_size, disp_details } display_mode;
typedef enum { disp_byte, disp_kilo, disp_mega, disp_human } display_unit;

/* program options */
struct du_opt {
    const char *user;
    const char *group;
    const char *type;

    /* status name and value for -status */
    sm_instance_t *smi;
    char *status_name;
    char *status_value;

    /* condition flags */
    unsigned int match_user:1;
    unsigned int match_group:1;
    unsigned int match_type:1;
    unsigned int match_status:1;

    /* behavior flags */
    display_mode disp_what;
    display_unit disp_how;
    unsigned int sum:1;

} prog_options = {
    .disp_what = disp_usage, .disp_how = disp_kilo
};

/** filter on entries to be summed */
static lmgr_filter_t    entry_filter;
/** same as entry_filter + condition on parent id */
static lmgr_filter_t    parent_filter;

/* filter for root entries */
static bool_node_t      match_expr;
static int              is_expr = 0; /**< is it set? */

static attr_mask_t disp_mask =
    { .std = ATTR_MASK_type | ATTR_MASK_blocks | ATTR_MASK_size };
static attr_mask_t query_mask = { 0 };

typedef struct stats_du_t {
    const char *type;
    uint64_t    count;
    uint64_t    blocks;
    uint64_t    size;
} stats_du_t;

#define TYPE_COUNT  (TYPE_SOCK+1)
static const stats_du_t stats_zero[TYPE_COUNT] = {
    {"?", 0, 0, 0},
    {STR_TYPE_LINK, 0, 0, 0},
    {STR_TYPE_DIR, 0, 0, 0},
    {STR_TYPE_FILE, 0, 0, 0},
    {STR_TYPE_CHR, 0, 0, 0},
    {STR_TYPE_BLK, 0, 0, 0},
    {STR_TYPE_FIFO, 0, 0, 0},
    {STR_TYPE_SOCK, 0, 0, 0}
};

static void reset_stats(stats_du_t *stats)
{
    int i;

    for (i = 0; i < TYPE_COUNT; i++)
        stats[i] = stats_zero[i];
}

static char *sprint_size(char *buf, uint64_t sz)
{
    switch (prog_options.disp_how) {
    case disp_byte:
        sprintf(buf, "%" PRIu64, sz);
        break;
    case disp_kilo:
        if (sz % KB)
            sprintf(buf, "%llu", 1 + (sz / KB));
        else
            sprintf(buf, "%llu", sz / KB);
        break;
    case disp_mega:
        if (sz % MB)
            sprintf(buf, "%llu", 1 + (sz / MB));
        else
            sprintf(buf, "%llu", sz / MB);
        break;
    case disp_human:
        if (sz < KB)
            sprintf(buf, "%" PRIu64, sz);
        else if (sz < MB)
            sprintf(buf, "%.1fK", 1.0 * sz / KB);
        else if (sz < GB)
            sprintf(buf, "%.1fM", 1.0 * sz / MB);
        else if (sz < TB)
            sprintf(buf, "%.1fG", 1.0 * sz / GB);
        else if (sz < PB)
            sprintf(buf, "%.1fT", 1.0 * sz / TB);
        else if (sz < EB)
            sprintf(buf, "%.1fP", 1.0 * sz / PB);
        else
            sprintf(buf, "%.1fE", 1.0 * sz / EB);
        break;
    }
    return buf;
}

static void print_stats(const char *name, stats_du_t *stats)
{
    int i;
    char b1[1024];
    char b2[1024];
    uint64_t total = 0;

    switch (prog_options.disp_what) {
    case disp_details:
        printf("%s\n", name);
        for (i = 0; i < TYPE_COUNT; i++)
            if (stats[i].count > 0)
                printf("\t%s count:%" PRIu64 ", size:%s, spc_used:%s\n",
                       stats[i].type, stats[i].count,
                       sprint_size(b1, stats[i].size),
                       sprint_size(b2, stats[i].blocks * DEV_BSIZE));
        break;
    case disp_usage:
        for (i = 0; i < TYPE_COUNT; i++)
            if (stats[i].count > 0)
                total += stats[i].blocks * DEV_BSIZE;
        printf("%s\t%s\n", sprint_size(b1, total), name);
        break;
    case disp_size:
        for (i = 0; i < TYPE_COUNT; i++)
            if (stats[i].count > 0)
                total += stats[i].size;
        printf("%s\t%s\n", sprint_size(b1, total), name);
        break;
    case disp_count:
        for (i = 0; i < TYPE_COUNT; i++)
            total += stats[i].count;
        printf("%" PRIu64 "\t%s\n", total, name);
        break;
    }
}

/* build filters depending on program options */
static int mkfilters(void)
{
    /* create boolean expression for matching root entries */
    if (prog_options.match_user) {
        compare_value_t val;
        compare_direction_t comp;

        if (global_config.uid_gid_as_numbers) {
            val.integer = atoi(prog_options.user);
            comp = COMP_EQUAL;
        } else {
            strcpy(val.str, prog_options.user);
            comp = COMP_LIKE;
        }

        if (!is_expr)
            CreateBoolCond(&match_expr, comp, CRITERIA_OWNER, val, 0);
        else
            AppendBoolCond(&match_expr, comp, CRITERIA_OWNER, val, 0);

        is_expr = 1;
        query_mask.std |= ATTR_MASK_uid;
    }

    if (prog_options.match_group) {
        compare_value_t val;
        compare_direction_t comp;

        if (global_config.uid_gid_as_numbers) {
            val.integer = atoi(prog_options.group);
            comp = COMP_EQUAL;
        } else {
            strcpy(val.str, prog_options.group);
            comp = COMP_LIKE;
        }

        if (!is_expr)
            CreateBoolCond(&match_expr, comp, CRITERIA_GROUP, val, 0);
        else
            AppendBoolCond(&match_expr, comp, CRITERIA_GROUP, val, 0);

        is_expr = 1;
        query_mask.std |= ATTR_MASK_gid;
    }

    if (prog_options.match_type) {
        compare_value_t val;
        val.type = db2type(prog_options.type);
        if (!is_expr)
            CreateBoolCond(&match_expr, COMP_EQUAL, CRITERIA_TYPE, val, 0);
        else
            AppendBoolCond(&match_expr, COMP_EQUAL, CRITERIA_TYPE, val, 0);
        is_expr = 1;
        query_mask.std |= ATTR_MASK_type;
    }

    if (prog_options.match_status) {
        compare_value_t val;

        strcpy(val.str, prog_options.status_value);
        if (!is_expr)
            CreateBoolCond(&match_expr, COMP_EQUAL, CRITERIA_STATUS, val, 0);
        else
            AppendBoolCond(&match_expr, COMP_EQUAL, CRITERIA_STATUS, val, 0);

        is_expr = 1;
        query_mask.status |= SMI_MASK(prog_options.smi->smi_index);
    }

    /* create DB filters */
    lmgr_simple_filter_init(&entry_filter);
    lmgr_simple_filter_init(&parent_filter);

    if (is_expr) {
        char expr[RBH_PATH_MAX];
        /* for debug */
        if (BoolExpr2str(&match_expr, expr, RBH_PATH_MAX) > 0)
            DisplayLog(LVL_FULL, DU_TAG, "Expression matching: %s", expr);

        /* append bool expr to entry filter */
        /* Do not use 'OR' expression there */
        convert_boolexpr_to_simple_filter(&match_expr, &entry_filter,
                                          prog_options.smi, NULL, 0);
        convert_boolexpr_to_simple_filter(&match_expr, &parent_filter,
                                          prog_options.smi, NULL, 0);
    }

    return 0;
}

static const char *help_string =
    _B "Usage:" B_ " %s [options] [path|fid]\n"
    "\n"
    _B "Filters:" B_ "\n"
    "    " _B "-u" B_ " " _U "user" U_ "\n"
    "    " _B "-g" B_ " " _U "group" U_ "\n"
    "    " _B "-t" B_ " " _U "type" U_ "\n"
    "       " TYPE_HELP "\n"
    "    " _B "-S" B_ " " _U "<status_name>" U_ ":" _U "<status_value>" U_ "\n"
    "\n"
    _B "Output options:" B_ "\n"
    "    " _B "-s" B_ ", " _B "--sum" B_ "\n"
    "       display total instead of stats per argument\n"
    "    " _B "-c" B_ ", " _B "--count" B_ "\n"
    "       display entry count instead of disk usage\n"
    "    " _B "-b" B_ ", " _B "--bytes" B_ "\n"
    "       display size instead of disk usage (display in bytes)\n"
    "    " _B "-k" B_ ", " _B "--kilo" B_ "\n"
    "       display disk usage in blocks of 1K (default)\n"
    "    " _B "-m" B_ ", " _B "--mega" B_ "\n"
    "       display disk usage in blocks of 1M\n"
    "    " _B "-H" B_ ", " _B "--human-readable" B_ "\n"
    "       display in human readable format (e.g 512K 123.7M)\n"
    "    " _B "-d" B_ ", " _B "--details" B_ "\n"
    "       show detailed stats: type, count, size, disk usage\n"
    "       (display in bytes by default)\n"
    "\n"
    _B "Program options:" B_ "\n"
    "    " _B "-f" B_ " " _U "config_file" U_ "\n"
    "    " _B "-l" B_ " " _U "log_level" U_ "\n"
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
    printf("Product:         " PACKAGE_NAME " 'du' command\n");
    printf("Version:         " PACKAGE_VERSION "-" RELEASE "\n");
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

static const char *opt2type(const char *type_opt)
{
    if (strlen(type_opt) != 1)
        return NULL;

    switch (type_opt[0]) {
    case 'b':
        return STR_TYPE_BLK;
    case 'c':
        return STR_TYPE_CHR;
    case 'd':
        return STR_TYPE_DIR;
    case 'p':
        return STR_TYPE_FIFO;
    case 'f':
        return STR_TYPE_FILE;
    case 'l':
        return STR_TYPE_LINK;
    case 's':
        return STR_TYPE_SOCK;
    default:
        return NULL;
    }
}

/**
 *  Get id of root dir
 */
static int retrieve_root_id(entry_id_t *root_id)
{
    int rc;
    rc = Path2Id(global_config.fs_path, root_id);
    if (rc)
        DisplayLog(LVL_MAJOR, DU_TAG, "Can't access filesystem's root %s: %s",
                   global_config.fs_path, strerror(-rc));
    return rc;
}

#define REPCNT    4
static report_field_descr_t dir_info[REPCNT] = {
    {ATTR_INDEX_type, REPORT_GROUP_BY, SORT_NONE, false, 0, FV_NULL},
    {0, REPORT_COUNT, SORT_NONE, false, 0, FV_NULL},
    {ATTR_INDEX_blocks, REPORT_SUM, SORT_NONE, false, 0, FV_NULL},
    {ATTR_INDEX_size, REPORT_SUM, SORT_NONE, false, 0, FV_NULL}
};

/* directory callback */
static int dircb(wagon_t *id_list, attr_set_t *attr_list,
                 unsigned int entry_count, void *arg)
{
    /* sum child entries stats for all directories */
    int i, rc;
    filter_value_t fv;
    struct lmgr_report_t *it;
    db_value_t result[REPCNT];
    unsigned int result_count;
    stats_du_t *stats = (stats_du_t *) arg;

    /* filter on parent_id */

    for (i = 0; i < entry_count; i++) {
        fv.value.val_id = id_list[i].id;
        rc = lmgr_simple_filter_add_or_replace(&parent_filter,
                                               ATTR_INDEX_parent_id,
                                               EQUAL, fv, 0);
        if (rc)
            return rc;

        it = ListMgr_Report(&lmgr, dir_info, REPCNT, NULL, &parent_filter,
                            NULL);
        if (it == NULL)
            return -1;

        result_count = REPCNT;
        while ((rc =
                ListMgr_GetNextReportItem(it, result, &result_count,
                                          NULL)) == DB_SUCCESS) {
            unsigned int idx = db2type(result[0].value_u.val_str);
            stats[idx].count += result[1].value_u.val_biguint;
            stats[idx].blocks += result[2].value_u.val_biguint;
            stats[idx].size += result[3].value_u.val_biguint;

            result_count = REPCNT;
        }

        ListMgr_CloseReport(it);
    }

    return 0;
}

/**
 * perform du command on the entire FS
 * \param stats array to be filled in
 * \param display_stats the function display the stats by itself
 */
static int list_all(stats_du_t *stats, bool display_stats)
{
    attr_set_t root_attrs;
    entry_id_t root_id;
    int rc;
    struct stat st;
    struct lmgr_report_t *it;

    db_value_t result[REPCNT];
    unsigned int result_count;

    ATTR_MASK_INIT(&root_attrs);

    rc = retrieve_root_id(&root_id);
    if (rc)
        memset(&root_id, 0, sizeof(root_id));

    /* root is not a part of the DB: sum it now if it matches */
    ATTR_MASK_SET(&root_attrs, fullpath);
    strcpy(ATTR(&root_attrs, fullpath), global_config.fs_path);

    if (lstat(ATTR(&root_attrs, fullpath), &st) == 0) {
        stat2rbh_attrs(&st, &root_attrs, true);
        ListMgr_GenerateFields(&root_attrs,
                               attr_mask_or(&disp_mask, &query_mask));
    }

    /* sum root if it matches */
    if (!is_expr || (entry_matches(&root_id, &root_attrs,
                                   &match_expr, NULL,
                                   prog_options.smi) == POLICY_MATCH)) {
        unsigned int idx = db2type(ATTR(&root_attrs, type));
        stats[idx].count++;
        stats[idx].blocks += ATTR(&root_attrs, blocks);
        stats[idx].size += ATTR(&root_attrs, size);
    }

    it = ListMgr_Report(&lmgr, dir_info, REPCNT, NULL, &entry_filter, NULL);
    if (it == NULL)
        return -1;

    result_count = REPCNT;
    while ((rc =
            ListMgr_GetNextReportItem(it, result, &result_count,
                                      NULL)) == DB_SUCCESS) {
        unsigned int idx = db2type(result[0].value_u.val_str);
        stats[idx].count += result[1].value_u.val_biguint;
        stats[idx].blocks += result[2].value_u.val_biguint;
        stats[idx].size += result[3].value_u.val_biguint;

        result_count = REPCNT;
    }

    ListMgr_CloseReport(it);

    if (display_stats)
        print_stats(global_config.fs_path, stats);

    return 0;
}

/**
 * List the content of the given id/path list
 */
static int list_content(char **id_list, int id_count)
{
    wagon_t *ids;
    int i, rc;
    attr_set_t root_attrs;
    entry_id_t root_id;
    bool is_id;
    stats_du_t stats[TYPE_COUNT];

    if (prog_options.sum)
        reset_stats(stats);

    rc = retrieve_root_id(&root_id);
    if (rc)
        return rc;

    ids = MemCalloc(id_count, sizeof(wagon_t));
    if (!ids)
        return -ENOMEM;

    for (i = 0; i < id_count; i++) {
        if (!prog_options.sum)
            reset_stats(stats);

        is_id = true;
        /* is it a path or fid? */
        if (sscanf(id_list[i], SFID, RFID(&ids[i].id)) != FID_SCAN_CNT) {
            is_id = false;
            /* take it as a path */
            rc = Path2Id(id_list[i], &ids[i].id);
            if (!rc)
                ids[i].fullname = id_list[i];
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
            DisplayLog(LVL_MAJOR, DU_TAG, "Invalid parameter: %s: %s",
                       id_list[i], strerror(-rc));
            goto out;
        }

        if (entry_id_equal(&ids[i].id, &root_id)) {
            /* the ID is FS root: use list_all instead */
            DisplayLog(LVL_DEBUG, DU_TAG,
                       "Optimization: command argument is filesystem's root: performing bulk sum in DB");
            rc = list_all(stats, !prog_options.sum);
            if (rc)
                goto out;
            continue;
        }

        /* get root attrs to print it (if it matches program options) */
        root_attrs.attr_mask = attr_mask_or(&disp_mask, &query_mask);
        rc = ListMgr_Get(&lmgr, &ids[i].id, &root_attrs);
        if (rc == 0)
            dircb(&ids[i], &root_attrs, 1, stats);
        else {
            DisplayLog(LVL_VERB, DU_TAG, "Notice: no attrs in DB for %s",
                       id_list[i]);

            if (!is_id) {
                struct stat st;
                ATTR_MASK_SET(&root_attrs, fullpath);
                strcpy(ATTR(&root_attrs, fullpath), id_list[i]);

                if (lstat(ATTR(&root_attrs, fullpath), &st) == 0) {
                    stat2rbh_attrs(&st, &root_attrs, true);
                    ListMgr_GenerateFields(&root_attrs,
                                           attr_mask_or(&disp_mask,
                                                        &query_mask));
                }
            } else if (entry_id_equal(&ids[i].id, &root_id)) {
                /* this is root id */
                struct stat st;
                ATTR_MASK_SET(&root_attrs, fullpath);
                strcpy(ATTR(&root_attrs, fullpath), global_config.fs_path);

                if (lstat(ATTR(&root_attrs, fullpath), &st) == 0) {
                    stat2rbh_attrs(&st, &root_attrs, true);
                    ListMgr_GenerateFields(&root_attrs,
                                           attr_mask_or(&disp_mask,
                                                        &query_mask));
                }
            }

            dircb(&ids[i], &root_attrs, 1, stats);
        }

        /* sum root if it matches */
        if (!is_expr || (entry_matches(&ids[i].id, &root_attrs,
                                       &match_expr, NULL,
                                       prog_options.smi) == POLICY_MATCH)) {
            unsigned int idx = db2type(ATTR(&root_attrs, type));
            stats[idx].count++;
            stats[idx].blocks += ATTR(&root_attrs, blocks);
            stats[idx].size += ATTR(&root_attrs, size);
        }

        if (!prog_options.sum) {
            /* if not group all, run and display stats now */
            rc = rbh_scrub(&lmgr, &ids[i], 1, disp_mask, dircb, stats);

            if (rc)
                goto out;

            print_stats(ids[i].fullname, stats);
        }
    }

    if (prog_options.sum) {
        rc = rbh_scrub(&lmgr, ids, id_count, disp_mask, dircb, stats);
        if (rc)
            goto out;
        print_stats("total", stats);
    }

 out:
    /* ids have been processed, free them */
    MemFree(ids);
    return rc;
}

#define MAX_OPT_LEN 1024

/**
 * Main daemon routine
 */
int main(int argc, char **argv)
{
    int c, option_index = 0;
    const char *bin;
    char config_file[MAX_OPT_LEN] = "";
    bool force_log_level = false;
    int log_level = 0;
    int rc;
    bool chgd = false;
    char err_msg[4096];
    char badcfg[RBH_PATH_MAX];

    bin = rh_basename(argv[0]);

    /* parse command line options */
    while ((c = getopt_long(argc, argv, SHORT_OPT_STRING, option_tab,
                            &option_index)) != -1) {
        switch (c) {
        case 's':
            prog_options.sum = 1;
            break;
        case 'c':
            prog_options.disp_what = disp_count;
            break;
        case 'b':
            /* only change the default */
            if (prog_options.disp_what == disp_usage)
                prog_options.disp_what = disp_size;
            /* only change the default */
            if (prog_options.disp_how == disp_kilo)
                prog_options.disp_how = disp_byte;
            break;
        case 'k':
            prog_options.disp_how = disp_kilo;
            break;
        case 'm':
            prog_options.disp_how = disp_mega;
            break;
        case 'd':
            prog_options.disp_what = disp_details;
            /* only change the default for display */
            if (prog_options.disp_how == disp_kilo)
                prog_options.disp_how = disp_byte;
            break;
        case 'H':
            prog_options.disp_how = disp_human;
            break;

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
            if (prog_options.type == NULL) {
                fprintf(stderr,
                        "invalid type '%s': expected types: " TYPE_HELP ".\n",
                        optarg);
                exit(1);
            }
            break;

        case 'S':
            rc = parse_status_arg("-status", optarg, &prog_options.status_name,
                                  &prog_options.status_value, true);
            if (rc)
                exit(rc);
            prog_options.match_status = 1;
            break;

        case 'f':
            rh_strncpy(config_file, optarg, MAX_OPT_LEN);
            break;
        case 'l':
            force_log_level = true;
            log_level = str2debuglevel(optarg);
            if (log_level == -1) {
                fprintf(stderr,
                        "Unsupported log level '%s'. CRIT, MAJOR, EVENT, VERB, DEBUG or FULL expected.\n",
                        optarg);
                exit(1);
            }
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

    /* initialize internal resources (glib, llapi, internal resources...) */
    rc = rbh_init_internals();
    if (rc != 0)
        exit(rc);

    /* get default config file, if not specified */
    if (SearchConfig(config_file, config_file, &chgd, badcfg,
                     MAX_OPT_LEN) != 0) {
        fprintf(stderr, "No config file (or too many) found matching %s\n",
                badcfg);
        exit(2);
    } else if (chgd) {
        fprintf(stderr, "Using config file '%s'.\n", config_file);
    }

    /* only read common config (listmgr, ...) (mask=0) */
    if (rbh_cfg_load(0, config_file, err_msg)) {
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
    if (rc) {
        fprintf(stderr, "Error opening log files: rc=%d, errno=%d: %s\n",
                rc, errno, strerror(errno));
        exit(rc);
    }

    /* Initialize filesystem access */
    rc = InitFS();
    if (rc)
        fprintf(stderr,
                "WARNING: cannot access filesystem %s (%s), du output may be wrong or incomplete.\n",
                global_config.fs_path, strerror(abs(rc)));

    /* Initialize list manager */
    rc = ListMgr_Init(LIF_REPORT_ONLY);
    if (rc) {
        DisplayLog(LVL_CRIT, DU_TAG, "Error initializing list manager: %s (%d)",
                   lmgr_err2str(rc), rc);
        exit(rc);
    } else
        DisplayLog(LVL_DEBUG, DU_TAG, "ListManager successfully initialized");

    if (CheckLastFS() != 0)
        exit(1);

    /* Create database access */
    rc = ListMgr_InitAccess(&lmgr);
    if (rc) {
        DisplayLog(LVL_CRIT, DU_TAG, "Error %d: cannot connect to database",
                   rc);
        exit(rc);
    }

    if (prog_options.match_status) {
        const char *strval;

        rc = check_status_args(prog_options.status_name,
                               prog_options.status_value, &strval,
                               &prog_options.smi);
        if (rc)
            exit(rc);
        prog_options.status_value = (char *)strval;
    }

    mkfilters();

    if (argc == optind) {
        stats_du_t stats[TYPE_COUNT];
        reset_stats(stats);

        /* no path in argument: du the entire FS */
        rc = list_all(stats, true); /* display the stats by itself */
    } else
        rc = list_content(argv + optind, argc - optind);

    ListMgr_CloseAccess(&lmgr);

    return rc;

}
