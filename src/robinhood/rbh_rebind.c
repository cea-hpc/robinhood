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
 * Command for rebinding a backend entry to a new fid.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "list_mgr.h"
#include "rbh_cfg.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "xplatform_print.h"
#include "cmd_helpers.h"
#include "rbh_basename.h"

#include <unistd.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>

#define LOGTAG "Rebind"

static struct option option_tab[] = {
    /* config file options */
    {"config-file", required_argument, NULL, 'f'},

    /* log options */
    {"log-level", required_argument, NULL, 'l'},

    /* status manager selector */
    {"statusmgr", required_argument, NULL, 's'},
    {"status-mgr", required_argument, NULL, 's'},

    /* miscellaneous options */
    {"help", no_argument, NULL, 'h'},
    {"version", no_argument, NULL, 'V'},

    {NULL, 0, NULL, 0}

};

static sm_instance_t *smi = NULL;

#define SHORT_OPT_STRING    "f:l:hVs:"

/* special character sequences for displaying help */

/* Bold start character sequence */
#define _B "[1m"
/* Bold end character sequence */
#define B_ "[m"

/* Underline start character sequence */
#define _U "[4m"
/* Underline end character sequence */
#define U_ "[0m"

// rbh-rebind  <old_identifier> <new_identifier> <target_path> [new_fid]
// Examples:
// backup:     backend_path=/old   fid=0xxx:xx:xx      /fs/foo
// lhsm:       fid=xxx             fid=yyy             /fs/foo
// lhsm:       uuid=abc            uuid=def            /fs/foo
// other:      output=abc          output=def          /fs/foo 0xxx:xx:xx

static const char *help_string =
    _B "Usage:" B_ " %s [options] <old_bk_id> <new_bk_id> <new_path> [new_fid]\n"
    "\n"
    "<old_bk_id>: old backend identifier specified as <attr>=<value>\n"
    "             e.g. fid=0x:x:x, uuid=xxxx, backend_path=/x/y\n"
    "<new_bk_id>: new backend identifier specified as <attr>=<value>\n"
    "             e.g. fid=0x:y:y, uuid=yyyy, backend_path=/x/z\n"
    "<new_path>: path in the filesystem where the new entry is (or will be) located.\n"
    "<new_fid>: by default, new_fid is taken as the current fid of new_path \n"
    "           but it might be different\n"
    "\n"
    _B "Module option:" B_ "\n"
    "    " _B "--status-mgr" B_" " _U "statusmgr" U_", "
           _B "-s" B_" " _U "statusmgr" U_"\n"
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
    printf("Product:         " PACKAGE_NAME " rebind tool\n");
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

static int read_fid(const char *fid_str, entry_id_t *fid)
{
    int nb_read;

    /* parse fid */
    if (fid_str[0] == '[')
        nb_read = sscanf(fid_str, "[" SFID "]", RFID(fid));
    else
        nb_read = sscanf(fid_str, SFID, RFID(fid));

    if (nb_read != FID_SCAN_CNT) {
        DisplayLog(LVL_CRIT, LOGTAG, "Unexpected format for fid %s",
                   fid_str);
        return -EINVAL;
    }

    return 0;
}

static const char *recov_status2str(recov_status_t st)
{
    switch (st) {
    case RS_FILE_OK:
        return "OK";
    case RS_FILE_DELTA:
        return "previous version";
    case RS_FILE_EMPTY:
        return "OK (empty file)";
    case RS_NON_FILE:
        return "OK (non file)";
    case RS_NOBACKUP:
        return "no backup";
    case RS_ERROR:
        return "ERROR";
    default:
        return "ERROR: unexpected status";
    }
}

static int parse_bk_id(attr_set_t *attrs, const char *str, entry_id_t *id,
                       bool *id_set)
{
    char attr[128];
    char *val;
    int rc;

    val = strchr(str, '=');
    if (!val) {
        fprintf(stderr, "Invalid attr format '%s': expected <attr>=<value>\n",
                str);
        return -EINVAL;
    }
    rh_strncpy(attr, str, MIN2(val - str + 1, sizeof(attr)));
    val++;

    if (!strcasecmp(attr, "id") || !strcasecmp(attr, "fid")) {
        rc = read_fid(val, id);
        if (rc == 0 && id_set)
            *id_set = true;
        return rc;
    }

    rc = set_attr_value_from_strings(attr, val, attrs, smi);

    return rc;
}


static inline int rebind_helper(const char *old_bk_id,
                                const char *new_bk_id,
                                const char *new_path,
                                const char *new_fid_str)
{
    int rc;
    attr_set_t old_attrs = ATTR_SET_INIT;
    attr_set_t new_attrs = ATTR_SET_INIT;
    entry_id_t new_fid;
    entry_id_t old_fid;
    bool old_fid_set = false;
    char *tmp;

    /* full path required */
    tmp = realpath(new_path, NULL);
    if (tmp == NULL) {
        rc = -errno;
        DisplayLog(LVL_CRIT, LOGTAG, "Error in realpath(%s): %s",
                   new_path, strerror(-rc));
        return rc;
    }
    if (strlen(tmp) >= RBH_PATH_MAX) {
        DisplayLog(LVL_CRIT, LOGTAG, "Path length is too long!");
        return -ENAMETOOLONG;
    }
    /* safe because of previous check */
    strcpy(ATTR(&new_attrs, fullpath), tmp);
    ATTR_MASK_SET(&new_attrs, fullpath);
    strcpy(ATTR(&old_attrs, fullpath), tmp);
    ATTR_MASK_SET(&old_attrs, fullpath);
    /* now we can free tmp path */
    free(tmp);

    if ((new_fid_str != NULL) && !EMPTY_STRING(new_fid_str))
        rc = read_fid(new_fid_str, &new_fid);
    else
        /* get fid for the given file */
        rc = Path2Id(new_path, &new_fid);

    if (rc)
        return rc;

    printf("Rebinding '%s' (" DFID ") from '%s' to '%s'...\n", new_path,
           PFID(&new_fid), old_bk_id, new_bk_id);

    /* parse old/new bk ids and set attr accordingly */
    if (parse_bk_id(&old_attrs, old_bk_id, &old_fid, &old_fid_set))
        return -EINVAL;
    if (parse_bk_id(&new_attrs, new_bk_id, &new_fid, NULL))
        return -EINVAL;

    /* rebind is like undelete with 'already recovered = true' */
    rc = smi->sm->undelete_func(smi, old_fid_set ? &old_fid : NULL,
                                &old_attrs, &new_fid, &new_attrs,
                                true);
    fprintf(stderr, "Rebind status for '%s': %s\n", ATTR(&new_attrs, fullpath),
            recov_status2str(rc));
    if (rc == RS_NOBACKUP || rc == RS_ERROR)
        return -1;
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

    int rc;
    char err_msg[4096];
    bool chgd = false;
    char badcfg[RBH_PATH_MAX];
    char sm_name[SM_NAME_MAX + 1] = "";

    bin = rh_basename(argv[0]);

    /* parse command line options */
    while ((c = getopt_long(argc, argv, SHORT_OPT_STRING, option_tab,
                            &option_index)) != -1) {
        switch (c) {
        case 'f':
            rh_strncpy(config_file, optarg, MAX_OPT_LEN);
            break;
        case 'l':
        {
            int log_level = str2debuglevel(optarg);

            if (log_level == -1) {
                fprintf(stderr,
                        "Unsupported log level '%s'. CRIT, MAJOR, EVENT, VERB, DEBUG or FULL expected.\n",
                        optarg);
                exit(1);
            }
            force_debug_level(log_level);
            break;
        }

        case 's':
            if (!EMPTY_STRING(sm_name))
                fprintf(stderr,
                        "WARNING: only a single status manager is expected "
                        "on command line. '%s' ignored.\n", optarg);
            else
                rh_strncpy(sm_name, optarg, sizeof(sm_name));
            break;

        case 'h':
            display_help(bin);
            exit(EXIT_SUCCESS);
            break;
        case 'V':
            display_version(bin);
            exit(EXIT_SUCCESS);
            break;
        case ':':
        case '?':
        default:
            display_help(bin);
            exit(1);
            break;
        }
    }

    /* 2 expected argument: old backend path, new path is FS */
    if (optind > argc - 3) {
        fprintf(stderr, "Error: missing arguments on command line.\n");
        display_help(bin);
        exit(1);
    } else if (optind < argc - 4) {
        fprintf(stderr, "Error: too many arguments on command line.\n");
        display_help(bin);
        exit(1);
    }

    rc = rbh_init_internals();
    if (rc != 0)
        exit(EXIT_FAILURE);

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

    if (!log_config.force_debug_level)
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
        exit(EXIT_FAILURE);
    }

    /* Initialize Filesystem access */
    rc = InitFS();
    if (rc)
        exit(EXIT_FAILURE);

    /* Initialize status managers (XXX all or just the one used for undelete?)
     */
    rc = smi_init_all(0);
    if (rc)
        exit(EXIT_FAILURE);

    /* load the status manager */
    if (!EMPTY_STRING(sm_name)) {
        rc = load_smi(sm_name, &smi);
        if (rc)
            exit(EXIT_FAILURE);
    } else {
        /* if there is a single smi that allows undelete, use it */
        rc = load_single_smi(&smi);
        if (rc)
            exit(EXIT_FAILURE);
    }

    if (optind == argc - 3)
        rc = rebind_helper(argv[optind], argv[optind + 1], argv[optind + 2],
                          NULL);
    else if (optind == argc - 4)
        rc = rebind_helper(argv[optind], argv[optind + 1], argv[optind + 2],
                           argv[optind + 3]);

    exit(rc ? EXIT_FAILURE: EXIT_SUCCESS);
}
