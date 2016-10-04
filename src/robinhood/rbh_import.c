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
#include "backend_ext.h"
#include "rbh_basename.h"

#include <unistd.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>

#define LOGTAG "Import"

static struct option option_tab[] = {
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

static lmgr_t lmgr;
static int force_stop = 0;

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
    _B "Usage:" B_ " %s [options] <backend_path> <import_path>\n" "\n"
// TODO: to be implemented
//    _B "Import options:" B_ "\n"
//    "    " _B "-H" B_ ", " _B "--hardlink" B_ "\n"
//    "        Create hardlinks instead of moving files.\n"
//    "\n"
    _B "Config file options:" B_ "\n"
    "    " _B "-f" B_ " " _U "file" U_ ", " _B "--config-file=" B_ _U "file" U_
    "\n" "        Path to configuration file (or short name).\n" "\n" _B
    "Miscellaneous options:" B_ "\n" "    " _B "-l" B_ " " _U "level" U_ ", " _B
    "--log-level=" B_ _U "level" U_ "\n"
    "        Force the log verbosity level (overides configuration value).\n"
    "        Allowed values: CRIT, MAJOR, EVENT, VERB, DEBUG, FULL.\n" "    " _B
    "-h" B_ ", " _B "--help" B_ "\n"
    "        Display a short help about command line options.\n" "    " _B "-V"
    B_ ", " _B "--version" B_ "\n" "        Display version info\n";

static inline void display_help(const char *bin_name)
{
    printf(help_string, bin_name);
}

static inline void display_version(const char *bin_name)
{
    printf("\n");
    printf("Product:         " PACKAGE_NAME " import tool\n");
    printf("Version:         " PACKAGE_VERSION "-" RELEASE "\n");
    printf("Build:           " COMPIL_DATE "\n");
    printf("\n");
    printf("Compilation switches:\n");

/* purpose of this daemon */
#ifdef _LUSTRE_HSM
    printf("    Lustre-HSM Policy Engine\n");
#elif defined(_TMP_FS_MGR)
    printf("    Temporary filesystem manager\n");
#elif defined(_HSM_LITE)
    printf("    Backup filesystem to external storage\n");
#else
#error "No purpose was specified"
#endif

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

static int import_helper(const char *backend_path, char *tgt_path,  /* in/out */
                         char *new_backend_path, struct stat *src_md)
{
    entry_id_t old_id, new_id;
    recov_status_t st;
    attr_set_t attrs, new_attrs, src_attrs;
    int rc;

    /* to check src path */
    const char *name;
    const char *first;
    const char *second;
    char dummy[RBH_PATH_MAX] = "";

    name = rh_basename(backend_path);

    /* clean import path if it already has fid in it */
    if ((second = strrchr(name, '_')) && (second != name)
        && (*(first = second - 1) == '_')
        && (sscanf(second + 1, SFID "%s", RFID(&old_id), dummy) >= 3)) {
        if (EMPTY_STRING(dummy) || !strcmp(dummy, "z")) {
            DisplayLog(LVL_EVENT, LOGTAG, "'%s' ends with a fid: " DFID_NOBRACE,
                       name, PFID(&old_id));

            if (strlen(first) <= strlen(tgt_path)) {
                /* otherwise, it can't terminate with a fid */
                char *end_of_tgt = tgt_path + strlen(tgt_path) - strlen(first);
                if (!strcmp(end_of_tgt, first))
                    /* clean fid in target path */
                    *end_of_tgt = '\0';
            }
        } else {
            DisplayLog(LVL_MAJOR, LOGTAG,
                       "'%s' has garbage ('%s') after fid (" DFID_NOBRACE ")",
                       name, dummy, PFID(&old_id));
            memset(&old_id, 0, sizeof(old_id));
        }
    } else
        memset(&old_id, 0, sizeof(old_id));

    printf("Importing '%s' as '%s'...\n", backend_path, tgt_path);

    ATTR_MASK_INIT(&attrs);
    ATTR_MASK_INIT(&src_attrs);
    ATTR_MASK_INIT(&new_attrs);

    ATTR_MASK_SET(&attrs, backendpath);
    strcpy(ATTR(&attrs, backendpath), backend_path);

    ATTR_MASK_SET(&attrs, fullpath);
    strcpy(ATTR(&attrs, fullpath), tgt_path);

    /* merge with source MD (but don't override) */
    if (src_md) {
        /* if the entry is a symlink, get its content */
        if (S_ISLNK(src_md->st_mode)) {
            const size_t bufflen = sizeof(ATTR(&attrs, link));
            rc = readlink(backend_path, ATTR(&attrs, link), bufflen);
            if (rc >= 0) {
                if (rc >= bufflen)
                    ATTR(&attrs, link)[bufflen - 1] = '\0';
                else
                    ATTR(&attrs, link)[rc] = '\0';

                ATTR_MASK_SET(&attrs, link);
            }
        }

        stat2rbh_attrs(src_md, &src_attrs, true);
        ListMgr_MergeAttrSets(&attrs, &src_attrs, false);
    }

    /* create file in Lustre */
    st = rbhext_recover(&old_id, &attrs, &new_id, &new_attrs, src_md);
    if ((st == RS_FILE_OK) || (st == RS_FILE_DELTA) || (st == RS_FILE_EMPTY)
        || (st == RS_NON_FILE)) {
        printf("\tSuccess\n");

        /* don't insert readonly attrs */
        new_attrs.attr_mask &= ~readonly_attr_set;

        /* insert or update it in the db */
        rc = ListMgr_Insert(&lmgr, &new_id, &new_attrs, true);
        if (rc == 0)
            printf("\tEntry successfully updated in the dabatase\n");
        else
            fprintf(stderr, "ERROR %d inserting entry in the database\n", rc);
        return rc;
    } else {
        fprintf(stderr, "ERROR importing '%s' as '%s'\n", backend_path,
                tgt_path);
        return -1;
    }
}

static int perform_import(const char *src_path, const char *tgt_path,
                          uint64_t *import_count, uint64_t *err_count,
                          struct stat *md_in)
{
    int rc;

    char bk_path[RBH_PATH_MAX] = "";
    char fs_path[RBH_PATH_MAX] = "";
    char new_bk_path[RBH_PATH_MAX] = "";

    DIR *dirp;
    struct dirent direntry;
    struct dirent *dircookie;
    struct stat md, src_md, tgt_md;
    int dir_init_err = 0;   /* errors before importing the directory */

    printf("%s\n", src_path);
    if (md_in)
        src_md = *md_in;
    else if (lstat(src_path, &src_md) != 0) {
        rc = -errno;
        DisplayLog(LVL_CRIT, LOGTAG, "ERROR: lstat failed on %s: %s",
                   src_path, strerror(-rc));
        return rc;
    }

    /* handle the case when src_path is a file or symlink */
    if (!S_ISDIR(src_md.st_mode)) {
        /* tmp copy of path to modify it */
        rh_strncpy(fs_path, tgt_path, sizeof(fs_path));

        /* is target an exitsting dir? (or link to a dir) */
        if ((stat(tgt_path, &tgt_md) == 0)
            && (S_ISDIR(tgt_md.st_mode))) {
            /* tmp copy of path to modify it */
            rh_strncpy(bk_path, src_path, sizeof(fs_path));
            snprintf(fs_path, sizeof(fs_path), "%s/%s", tgt_path,
                     rh_basename(bk_path));
        }

        if ((rc = import_helper(src_path, fs_path, new_bk_path, &src_md)))
            (*err_count)++;
        else
            (*import_count)++;

        return rc;
    } else {
        /* 2nd arg of import_helper is in/out */
        rh_strncpy(fs_path, tgt_path, sizeof(fs_path));

        /* import directory (create in the backend with the same rights
         * and owner) */
        if ((rc = import_helper(src_path, fs_path, new_bk_path, &src_md)))
            (*err_count)++;
        else
            (*import_count)++;
    }

    /* scan bkpath */
    if ((dirp = opendir(src_path)) == NULL) {
        rc = -errno;
        DisplayLog(LVL_CRIT, LOGTAG,
                   "opendir on %s failed: Error %d: %s",
                   src_path, -rc, strerror(-rc));
        (*err_count)++;
        return rc;
    }
    dir_init_err = *err_count;

    while (1) {
        rc = readdir_r(dirp, &direntry, &dircookie);

        if (rc == 0 && dircookie == NULL)
            /* end of directory */
            break;
        else if (force_stop) {
            DisplayLog(LVL_EVENT, LOGTAG,
                       "Stop requested: cancelling import of %s", src_path);
            return 0;
        } else if (rc != 0) {
            DisplayLog(LVL_CRIT, LOGTAG, "ERROR %d reading directory '%s': %s",
                       rc, src_path, strerror(rc));
            (*err_count)++;
            break;
        }
        /* ignore . and .. */
        else if (!strcmp(direntry.d_name, ".")
                 || !strcmp(direntry.d_name, ".."))
            continue;

        snprintf(bk_path, sizeof(bk_path), "%s/%s", src_path, direntry.d_name);
        snprintf(fs_path, sizeof(fs_path), "%s/%s", tgt_path, direntry.d_name);

        /* what kind of entry is it? */
        if (lstat(bk_path, &md) != 0) {
            DisplayLog(LVL_CRIT, LOGTAG, "ERROR calling lstat(%s): %s",
                       bk_path, strerror(errno));
            (*err_count)++;
            continue;
        }
        if (S_ISDIR(md.st_mode)) {
            /* recurse */
            rc = perform_import(bk_path, fs_path, import_count, err_count, &md);
            if (rc)
                continue;
        } else {
            if (import_helper(bk_path, fs_path, new_bk_path, &md))
                (*err_count)++;
            else
                (*import_count)++;
        }
    }
    closedir(dirp);

    /* no error when importing this directory => remove it from source dir */
    if (dir_init_err == *err_count) {
        if (rmdir(src_path)) {
            DisplayLog(LVL_MAJOR, LOGTAG,
                       "Cannot remove source directory %s: %s", src_path,
                       strerror(errno));
            (*err_count)++;
        } else
            printf("Removed empty source directory %s\n", src_path);
    }

    return 0;
}

static void terminate_handler(int sig)
{
    force_stop = 1;
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
    uint64_t total = 0;
    uint64_t err = 0;

    bool force_log_level = false;
    int log_level = 0;

    int rc;
    char err_msg[4096];
    robinhood_config_t config;
    bool chgd = false;
    char badcfg[RBH_PATH_MAX];

    struct sigaction act_sigterm;

    bin = rh_basename(argv[0]); /* supports NULL argument */

    /* parse command line options */
    while ((c = getopt_long(argc, argv, SHORT_OPT_STRING, option_tab,
                            &option_index)) != -1) {
        switch (c) {
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

    /* 2 expected argument: src_path, tgt_path */
    if (optind != argc - 2) {
        fprintf(stderr, "Error: missing arguments on command line.\n");
        display_help(bin);
        exit(1);
    }

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

    /* only read ListMgr config */
    if (ReadRobinhoodConfig(0, config_file, err_msg, &config, false)) {
        fprintf(stderr, "Error reading configuration file '%s': %s\n",
                config_file, err_msg);
        exit(1);
    }

    if (force_log_level)
        config.log_config.debug_level = log_level;

    /* XXX HOOK: Set logging to stderr */
    strcpy(config.log_config.log_file, "stderr");
    strcpy(config.log_config.report_file, "stderr");
    strcpy(config.log_config.alert_file, "stderr");

    /* Initialize logging */
    rc = InitializeLogs(bin, &config.log_config);
    if (rc) {
        fprintf(stderr, "Error opening log files: rc=%d, errno=%d: %s\n",
                rc, errno, strerror(errno));
        exit(rc);
    }

    /* Initialize Filesystem access */
    rc = InitFS();
    if (rc)
        exit(rc);

    /* Initialize status managers (XXX all or just the one used for import?) */
    rc = smi_init_all(options.flags);
    if (rc)
        exit(rc);

    /* Initialize list manager */
    rc = ListMgr_Init(0);
    if (rc) {
        DisplayLog(LVL_CRIT, LOGTAG, "Error initializing list manager: %s (%d)",
                   lmgr_err2str(rc), rc);
        exit(rc);
    } else
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
#ifdef _HSM_LITE
    rc = Backend_Start(&config.backend_config, 0);
    if (rc) {
        DisplayLog(LVL_CRIT, LOGTAG, "Error initializing backend");
        exit(1);
    }
#endif

    /* create signal handlers */
    memset(&act_sigterm, 0, sizeof(act_sigterm));
    act_sigterm.sa_flags = 0;
    act_sigterm.sa_handler = terminate_handler;
    if (sigaction(SIGTERM, &act_sigterm, NULL) == -1
        || sigaction(SIGINT, &act_sigterm, NULL) == -1) {
        DisplayLog(LVL_CRIT, LOGTAG,
                   "Error while setting signal handlers for SIGTERM and SIGINT: %s",
                   strerror(errno));
        exit(1);
    }

    rc = perform_import(argv[optind], argv[optind + 1], &total, &err, NULL);
    if (rc)
        fprintf(stderr, "Import terminated with error %d\n", rc);
    else if (force_stop)
        fprintf(stderr, "Import aborted by user\n");

    printf("Import summary: %" PRIu64 " entries imported, %" PRIu64 " errors\n",
           total, err);

    ListMgr_CloseAccess(&lmgr);

    return rc;

}
