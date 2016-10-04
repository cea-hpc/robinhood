/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009-2015 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

/**
 * Command for restoring an entry that was accidentally removed from filesystem.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "list_mgr.h"
#include "rbh_cfg.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "xplatform_print.h"
#include "rbh_basename.h"
#include "cmd_helpers.h"

#include <unistd.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <pthread.h>

#define LOGTAG "Undelete"

static struct option option_tab[] = {
    /* options for cancelling remove operation */
    {"list", no_argument, NULL, 'L'},
    {"restore", no_argument, NULL, 'R'},

    {"statusmgr", required_argument, NULL, 's'},
    {"status-mgr", required_argument, NULL, 's'},

    /* config file options */
    {"config-file", required_argument, NULL, 'f'},

    /* log options */
    {"log-level", required_argument, NULL, 'l'},

    /* miscellaneous options */
    {"help", no_argument, NULL, 'h'},
    {"version", no_argument, NULL, 'V'},

    {NULL, 0, NULL, 0}

};

#define SHORT_OPT_STRING    "LRs:f:l:hV"

/* global variables */

static lmgr_t         lmgr;
char                  path_filter[RBH_PATH_MAX] = "";
static sm_instance_t *smi = NULL;

/* special character sequences for displaying help */

/* Bold start character sequence */
#define _B "[1m"
/* Bold end character sequence */
#define B_ "[m"

/* Underline start character sequence */
#define _U "[4m"
/* Underline end character sequence */
#define U_ "[0m"

static const char *help_string =
    _B "Usage:" B_ " %s [options] [<path>|<fid>]\n"
    "\n"
    _B "Actions:" B_ "\n"
    "    " _B "--list" B_ ", " _B "-L" B_ "\n"
    "        List removed entries in the given directory.\n"
    "    " _B "--restore" B_ ", " _B "-R" B_ "\n"
    "        Restore removed entries in the given directory.\n"
    "\n"
    _B "Behavior options:" B_ "\n"
    "    " _B "--status-mgr" B_ _U "statusmgr" U_", " _B "-s" B_ _U "statusmgr" U_"\n"
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
    "    " _B "-V" B_ ", " _B "--version" B_ "\n"
    "        Display version info\n";

static inline void display_help(const char *bin_name)
{
    printf(help_string, bin_name);
}

static inline void display_version(const char *bin_name)
{
    printf("\n");
    printf("Product:         " PACKAGE_NAME " rm cancellation tool\n");
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

/*
 * Append global filters on path
 * \param do_display [in] display filters?
 * \param initialized [in/out] indicate if the filter is initialized.
 */
static int mk_path_filter(lmgr_filter_t *filter, bool do_display,
                          bool *initialized)
{
    filter_value_t fv;
    char path_regexp[RBH_PATH_MAX] = "";
    size_t len;

    /* is a filter on path specified? */
    if (!EMPTY_STRING(path_filter)) {
        if ((initialized != NULL) && !(*initialized)) {
            lmgr_simple_filter_init(filter);
            *initialized = true;
        }
        if (do_display)
            printf("filter path: %s\n", path_filter);

        len = strlen(path_filter);
        /* remove last slash */
        if (path_filter[len - 1] == '/')
            path_filter[len - 1] = '\0';

        /* as this is a RLIKE matching, shell regexp must be replaced by perl:
         * [abc] => OK
         * '*' => '.*'
         * '?' => '.'
         */
        str_subst(path_filter, "*", ".*");
        str_subst(path_filter, "?", ".");

        /* match 'path$' OR 'path/.*' */
        snprintf(path_regexp, RBH_PATH_MAX, "%s($|/.*)", path_filter);
        fv.value.val_str = path_regexp;

        lmgr_simple_filter_add(filter, ATTR_INDEX_fullpath, RLIKE, fv, 0);
    }
    return 0;
}

static bool is_id_filter(entry_id_t *id)
{
    entry_id_t fid = { 0 };

    if (!EMPTY_STRING(path_filter)) {
        if (sscanf(path_filter, SFID, RFID(&fid)) != FID_SCAN_CNT)
            return false;
        else {
            if (id)
                *id = fid;
            return true;
        }
    }
    return false;
}

static int list_rm(void)
{
    int rc, index;
    entry_id_t id;
    attr_set_t attrs = ATTR_SET_INIT;
    attr_mask_t mask;

    static unsigned int list[] = {
        ATTR_INDEX_rm_time,
        ATTR_INDEX_ID,  /* id */
        ATTR_INDEX_type,
        ATTR_INDEX_uid,
        ATTR_INDEX_gid,
        ATTR_INDEX_size,
        ATTR_INDEX_last_mod,
        0,  /* to be set in the code: status index */
        ATTR_INDEX_fullpath,
    };
    int list_cnt = sizeof(list) / sizeof(*list);

    list[7] = ATTR_INDEX_FLG_STATUS | smi->smi_index;

    mask = list2mask(list, list_cnt);

    print_attr_list(0, list, list_cnt, NULL, false);

    if (is_id_filter(&id)) {    /* 1 single entry */
        rc = ListMgr_GetRmEntry(&lmgr, &id, &attrs);
        if (rc == DB_SUCCESS) {
            print_attr_values(0, list, list_cnt, &attrs, &id, false, NULL);
        } else if (rc == DB_NOT_EXISTS)
            DisplayLog(LVL_CRIT, LOGTAG,
                       DFID ": fid not found in deferred removal list",
                       PFID(&id));
        else
            DisplayLog(LVL_CRIT, LOGTAG,
                       "ERROR %d in ListMgr_GetRmEntry(" DFID ")",
                       rc, PFID(&id));
        return rc;
    } else {    /* list of entries */

        struct lmgr_rm_list_t *rm_list;
        lmgr_filter_t filter = { 0 };
        bool filter_init = false;

        /* set filters */
        mk_path_filter(&filter, false, &filter_init);

        /* list all deferred rm */
        rm_list = ListMgr_RmList(&lmgr, filter_init ? &filter : NULL, NULL);

        if (filter_init)
            lmgr_simple_filter_free(&filter);

        if (rm_list == NULL) {
            DisplayLog(LVL_CRIT, LOGTAG,
                       "ERROR: Could not retrieve removed entries from "
                       "database.");
            return -1;
        }

        index = 0;
        attrs.attr_mask = mask;

        while ((rc = ListMgr_GetNextRmEntry(rm_list, &id, &attrs))
               == DB_SUCCESS) {
            index++;

            print_attr_values(0, list, list_cnt, &attrs, &id, false, NULL);

            /* prepare next call */
            ListMgr_FreeAttrs(&attrs);
            memset(&attrs, 0, sizeof(attrs));
            attrs.attr_mask = mask;
        }

        ListMgr_CloseRmList(rm_list);
    }
    return 0;
}

static ull_t counters[RS_COUNT] = { 0 };

static ull_t db_err = 0;

static const char *st_names[] = {
    [RS_FILE_OK] = "files",
    [RS_FILE_DELTA] = "old version",
    [RS_FILE_EMPTY] = "empty files",
    [RS_NON_FILE] = "non-files",
    [RS_NOBACKUP] = "no backup",
    [RS_ERROR] = "errors"
};

static void undelete_helper(const entry_id_t *id, const attr_set_t *attrs)
{
    entry_id_t new_id = { 0 };
    recov_status_t st;
    attr_set_t new_attrs = ATTR_SET_INIT;
    int rc;

    printf("Restoring '%s'...", ATTR(attrs, fullpath));

    st = smi->sm->undelete_func(smi, id, attrs, &new_id, &new_attrs, false);

    counters[st]++;

    switch (st) {
    case RS_FILE_OK:
        printf("\t restore OK (file)\n");
        break;
    case RS_FILE_DELTA:
        printf("\t restored previous version (file)\n");
        break;
    case RS_FILE_EMPTY:
        printf("\t restore OK (empty file)\n");
        break;
    case RS_NON_FILE:
        printf("\t restore OK (%s)\n", ATTR(attrs, type));
        break;
    case RS_NOBACKUP:
        printf("\t cannot restore %s (no backup)\n", ATTR(attrs, type));
        break;
    case RS_ERROR:
        printf("\t ERROR\n");
        break;
    default:
        printf("ERROR: UNEXPECTED STATUS %d\n", st);
    }
    /* TODO for symlinks and dir, we can implement a common recovery
     * that consists in setting entry attributes from DB.
     * FIXME these entries may not be matches by status managers.
     * XXX Use create_from_attrs() */

    if ((st == RS_FILE_OK) || (st == RS_FILE_DELTA) || (st == RS_FILE_EMPTY)
        || (st == RS_NON_FILE)) {
        /* discard entry from remove list */
        if (ListMgr_SoftRemove_Discard(&lmgr, id) != 0) {
            db_err++;
            fprintf(stderr, "Error: could not remove previous id " DFID
                    " from database\n", PFID(id));
        }

        /* clean read-only attrs */
        attr_mask_unset_readonly(&new_attrs.attr_mask);

        /* insert or update it in the db */
        rc = ListMgr_Insert(&lmgr, &new_id, &new_attrs, true);
        if (rc == 0)
            printf("\tEntry successfully updated in the dabatase\n");
        else {
            db_err++;
            fprintf(stderr, "\tERROR %d inserting entry in the database\n", rc);
        }
    }
}

static int undelete(void)
{
    int rc;
    struct lmgr_rm_list_t *list;
    entry_id_t id;
    attr_set_t attrs = ATTR_SET_INIT;
    attr_mask_t mask;
    recov_status_t st;

    /* get all POSIX + status manager mask */
    mask = smi->sm->softrm_table_mask;
    mask.std |= POSIX_ATTR_MASK;

    attrs.attr_mask = mask;

    if (is_id_filter(&id)) {    /* 1 single entry */
        rc = ListMgr_GetRmEntry(&lmgr, &id, &attrs);
        if (rc == DB_SUCCESS) {
            undelete_helper(&id, &attrs);
        } else if (rc == DB_NOT_EXISTS)
            DisplayLog(LVL_CRIT, LOGTAG,
                       DFID ": fid not found in removed entries", PFID(&id));
        else
            DisplayLog(LVL_CRIT, LOGTAG,
                       "ERROR %d in ListMgr_GetRmEntry(" DFID ")",
                       rc, PFID(&id));
        return rc;
    } else {    /* recover a list of entries */

        lmgr_filter_t filter = { 0 };
        bool filter_init = false;

        /* set filters */
        mk_path_filter(&filter, false, &filter_init);

        /* list files to be recovered */
        list = ListMgr_RmList(&lmgr, filter_init ? &filter : NULL, NULL);

        if (filter_init)
            lmgr_simple_filter_free(&filter);

        if (list == NULL) {
            DisplayLog(LVL_CRIT, LOGTAG,
                       "ERROR: Could not retrieve removed entries from database.");
            return -1;
        }

        while ((rc = ListMgr_GetNextRmEntry(list, &id, &attrs)) == DB_SUCCESS) {
            undelete_helper(&id, &attrs);

            /* prepare next call */
            ListMgr_FreeAttrs(&attrs);
            memset(&attrs, 0, sizeof(attrs));
            attrs.attr_mask = mask;
        }
        ListMgr_CloseRmList(list);
    }

    /* display summary */
    printf("\nundelete summary:\n");
    for (st = RS_FILE_OK; st < RS_COUNT; st++) {
        printf("\t%9llu %s\n", counters[st], st_names[st]);
    }
    printf("\t%9llu DB errors\n", db_err);

    return 0;
}

/**
 * Check if there is a single status manager that supports
 * undelete, and load it.
 * \retval EINVAL if more than 1 status managers implement 'undelete'.
 * \retval 0 if a single status manager was found.
 * \retval ENOENT if no status manager implements undelete.
 */
static int load_single_smi(void)
{
    int i = 0;
    sm_instance_t *smi_curr;

    /** XXX based on policies or status managers? what about the scope? */
    while ((smi_curr = get_sm_instance(i)) != NULL) {
        if (smi_curr->sm->undelete_func != NULL) {
            if (smi != NULL) {
                DisplayLog(LVL_CRIT, LOGTAG,
                           "ERROR: no status manager specified, but several of "
                           "them implement 'undelete'");
                return EINVAL;
            }
            smi = smi_curr;
        }
        i++;
    }

    if (smi == NULL) {
        DisplayLog(LVL_CRIT, LOGTAG,
                   "ERROR: no status manager implements 'undelete'");
        return ENOENT;
    }

    return 0;
}

/** load the Status Manager Instance with the given name */
static int load_smi(const char *sm_name)
{
    int rc;
    const char *dummy;

    rc = check_status_args(sm_name, NULL, &dummy, &smi);
    if (rc)
        return rc;

    if (smi->sm->undelete_func == NULL) {
        DisplayLog(LVL_CRIT, LOGTAG,
                   "ERROR: the specified status manager '%s' doesn't "
                   "implement 'undelete'", sm_name);
        return EINVAL;
    }

    return 0;
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

    enum { ACTION_NONE, ACTION_LIST, ACTION_RESTORE } action = ACTION_NONE;
    bool force_log_level = false;
    int log_level = 0;

    int rc;
    char err_msg[4096];
    bool chgd = false;
    char badcfg[RBH_PATH_MAX];

    char sm_name[SM_NAME_MAX + 1] = "";

    bin = rh_basename(argv[0]); /* supports NULL argument */

    /* parse command line options */
    while ((c = getopt_long(argc, argv, SHORT_OPT_STRING, option_tab,
                            &option_index)) != -1) {
        switch (c) {
        case 'L':
            if ((action != ACTION_NONE) && (action != ACTION_LIST))
                fprintf(stderr,
                        "WARNING: only a single action (--list or --restore) is expected\n"
                        "on command line. '--restore' will be ignored.\n");
            action = ACTION_LIST;
            break;
        case 'R':
            if ((action != ACTION_NONE) && (action != ACTION_RESTORE))
                fprintf(stderr,
                        "WARNING: only a single action (--list or --restore) is expected\n"
                        "on command line. '--list' will be ignored.\n");
            action = ACTION_RESTORE;
            break;

        case 's':
            if (!EMPTY_STRING(sm_name))
                fprintf(stderr,
                        "WARNING: only a single status manager is expected "
                        "on command line. '%s' ignored.\n", optarg);
            else
                rh_strncpy(sm_name, optarg, sizeof(sm_name));
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

    /* 1 expected argument: path */
    if (optind < argc - 1) {
        fprintf(stderr,
                "Error: too many arguments on command line: expected <path|fid>\n");
        exit(1);
    } else if (optind == argc - 1)
        rh_strncpy(path_filter, argv[optind], RBH_PATH_MAX);

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

    /* only read common config */
    if (rbh_cfg_load(0, config_file, err_msg)) {
        fprintf(stderr, "Error reading configuration file '%s': %s\n",
                config_file, err_msg);
        exit(1);
    }

    if (force_log_level)
        log_config.debug_level = log_level;

    /* XXX HOOK: Set logging to stderr */
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

    /* Initialize Filesystem access */
    rc = InitFS();
    if (rc)
        exit(rc);

    /* Initialize status managers (XXX all or just the one used for undelete?)
     */
    rc = smi_init_all(0);
    if (rc)
        exit(rc);

    /* load the status manager */
    if (!EMPTY_STRING(sm_name)) {
        rc = load_smi(sm_name);
        if (rc)
            exit(rc);
    } else {
        /* if there is a single smi that allows undelete, use it */
        rc = load_single_smi();
        if (rc)
            exit(rc);
    }

    /* Initialize list manager */
    rc = ListMgr_Init(0);
    if (rc) {
        DisplayLog(LVL_CRIT, LOGTAG, "Error initializing list manager: %s (%d)",
                   lmgr_err2str(rc), rc);
        exit(rc);
    }
    DisplayLog(LVL_DEBUG, LOGTAG, "ListManager successfully initialized");

    if (CheckLastFS() != 0)
        exit(1);

    /* Create database access */
    rc = ListMgr_InitAccess(&lmgr);
    if (rc) {
        DisplayLog(LVL_CRIT, LOGTAG, "Error %d: cannot connect to database",
                   rc);
        exit(rc);
    }

    if (!has_deletion_policy()) {
        DisplayLog(LVL_CRIT, LOGTAG, "Unsupported action: no defined policy "
                   "manages deleted files");
        exit(ENOTSUP);
    }

    /* perform the action */
    switch (action) {
    case ACTION_LIST:
        rc = list_rm();
        break;
    case ACTION_RESTORE:
        rc = undelete();
        break;
    case ACTION_NONE:
        display_help(bin);
        rc = 1;
        break;
    default:
        fprintf(stderr, "Unexpected action (action code=%#x)\n", action);
        display_help(bin);
        rc = EINVAL;
        break;
    }

    ListMgr_CloseAccess(&lmgr);

    return rc;
}
