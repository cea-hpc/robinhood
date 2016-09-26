/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2010-2014 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "mod_internal.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "rbh_cfg_helpers.h"
#include "rbh_modules.h"
#include "xplatform_print.h"
#include "Memory.h"
#include "rbh_basename.h"
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <utime.h>
#include <libgen.h>
#include <pwd.h>
#include <grp.h>
#include <ctype.h>
#include <fnmatch.h>
#include <zlib.h>
#include <sys/sendfile.h>

#ifdef HAVE_SHOOK
#include <shook_svr.h>
#else
#endif

/* ---------- config parsing stuff ------------ */
#define OLD_BACKUP_BLOCK "Backend"

#ifdef HAVE_SHOOK
#define TAG "Shook"
#define BACKUP_BLOCK "shook_config"
#define BKL_TAG "shook_cfg"
#else
#define TAG "Backup"
#define BACKUP_BLOCK "backup_config"
#define BKL_TAG "backup_cfg"
#endif

typedef struct backup_config_t {
    char root[RBH_PATH_MAX];
    char mnt_type[RBH_NAME_MAX];
    bool check_mounted;
#ifdef HAVE_SHOOK
    char shook_cfg[RBH_PATH_MAX];
#endif
    /** Distinct from action_timeout that is only defined at policy run time.
     * This one is needed to determine the current entry status
     * by checking if a transfer is currently active.
     */
    time_t copy_timeout;
    /** This is not only an action parameter, as it is needed to check
     * entry status (backend path differs if the entry is compressed).
     */
    bool compress;

    /** recovery action */
    policy_action_t recovery_action;

} backup_config_t;

/* backup config is global as the status manager is shared */
static backup_config_t config;

static void backup_cfg_set_default(void *module_config)
{
    backup_config_t *conf = (backup_config_t *) module_config;

    strcpy(conf->root, "/backend");
    strcpy(conf->mnt_type, "nfs");
    conf->check_mounted = true;
    conf->compress = false;
    conf->copy_timeout = 6 * 3600;  /* 6h */
#ifdef HAVE_SHOOK
    strcpy(conf->shook_cfg, "/etc/shook.cfg");
#endif

    /* must be explicitly specified */
    conf->recovery_action.type = ACTION_UNSET;
    conf->recovery_action.action_u.func.name = "";
    conf->recovery_action.action_u.func.call = NULL;
}

static void backup_cfg_write_default(FILE *output)
{
    print_begin_block(output, 0, BACKUP_BLOCK, NULL);
    print_line(output, 1, "root          : \"/backend\"");
    print_line(output, 1, "mnt_type      : nfs");
    print_line(output, 1, "check_mounted : yes");
    print_line(output, 1, "copy_timeout  : 6h");
    print_line(output, 1, "compress      : no");
#ifdef HAVE_SHOOK
    print_line(output, 1, "shook_cfg     : \"/etc/shook.cfg\"")
#endif
        print_line(output, 1, "recovery_action: <mandatory>");

    print_end_block(output, 0);
}

/* forward declaration */
static status_manager_t backup_sm;

static int backup_cfg_read(config_file_t config, void *module_config,
                           char *msg_out)
{
    int rc;
    backup_config_t *conf = (backup_config_t *) module_config;
    config_item_t block;
    char tmp[RBH_PATH_MAX];
    char **extra = NULL;
    unsigned int extra_cnt = 0;
    attr_mask_t mask = null_mask;

    const cfg_param_t backend_params[] = {
        {"root", PT_STRING, PFLG_ABSOLUTE_PATH | PFLG_REMOVE_FINAL_SLASH
         | PFLG_NO_WILDCARDS | PFLG_NOT_EMPTY, conf->root, sizeof(conf->root)}
        ,
        {"mnt_type", PT_STRING, 0, conf->mnt_type, sizeof(conf->mnt_type)}
        ,
        {"check_mounted", PT_BOOL, 0, &conf->check_mounted, 0}
        ,
        {"compress", PT_BOOL, 0, &conf->compress, 0}
        ,
        {"copy_timeout", PT_DURATION, 0, &conf->copy_timeout, 0}
        ,
#ifdef HAVE_SHOOK
        /* shook only */
        {"shook_cfg", PT_STRING, PFLG_ABSOLUTE_PATH | PFLG_NO_WILDCARDS,
         conf->shook_cfg, sizeof(conf->shook_cfg)}
        ,
#endif
        /** TODO parse recovery action */
        END_OF_PARAMS
    };

    static const char *allowed_params[] = {
        "root", "mnt_type", "check_mounted", "copy_timeout", "compress",
        "recovery_action",
#ifdef HAVE_SHOOK
        "shook_cfg",
#endif
        /** TODO parse recovery action */
        NULL
    };

    /* get Backup block */
    rc = get_cfg_block(config, BACKUP_BLOCK, &block, msg_out);
    if (rc)
        return rc == ENOENT ? 0 : rc;   /* not mandatory */

    /* read std parameters */
    rc = read_scalar_params(block, BACKUP_BLOCK, backend_params, msg_out);
    if (rc)
        return rc;

    /* read specific params */
    rc = GetStringParam(block, BACKUP_BLOCK, "recovery_action",
                        PFLG_MANDATORY, tmp, sizeof(tmp), &extra,
                        &extra_cnt, msg_out);
    if (rc != 0)
        return rc;

    rc = parse_policy_action("recovery_action", tmp, extra, extra_cnt,
                             &conf->recovery_action, &mask, msg_out);
    if (rc)
        return rc;

    /* add the mask to softrm table mask */
    backup_sm.softrm_table_mask =
        attr_mask_or(&backup_sm.softrm_table_mask, &mask);

    CheckUnknownParameters(block, BACKUP_BLOCK, allowed_params);

    return 0;
}

static void backup_cfg_write_template(FILE *output)
{
    print_begin_block(output, 0, BACKUP_BLOCK, NULL);
    print_line(output, 1, "# backend path and type");
    print_line(output, 1, "root          = \"/backend\";");
    print_line(output, 1, "mnt_type      = nfs;");
    print_line(output, 1, "# check if the backend is mounted on startup");
    print_line(output, 1, "check_mounted = yes;");
    print_line(output, 1, "copy_timeout  = 6h;");
#ifdef HAVE_SHOOK
    print_line(output, 1, "# shook server configuration");
    print_line(output, 1, "shook_cfg     = \"/etc/shook.cfg\";");
#endif
#ifdef HAVE_SHOOK
    print_line(output, 1, "#recovery_action = shook.recover;");
#else
    print_line(output, 1, "#recovery_action = common.copy;");
#endif
    print_end_block(output, 0);
}

static void *backup_cfg_new(void)
{
    return calloc(1, sizeof(backup_config_t));
}

static void backup_cfg_free(void *cfg)
{
    if (cfg != NULL)
        free(cfg);
}

static int backup_cfg_set(void *cfg, bool reload)
{
    backup_config_t *new = cfg;

    if (!reload) {
        config = *new;
        return 0;
    }

    /* reload case */
    /* only copy timeout can be modified dynamically */
    if (new->copy_timeout != config.copy_timeout) {
        DisplayLog(LVL_EVENT, BKL_TAG,
                   BACKUP_BLOCK "::copy_timeout updated: %ld->%ld",
                   config.copy_timeout, new->copy_timeout);
        config.copy_timeout = new->copy_timeout;
    }

    return 0;
}

static const mod_cfg_funcs_t backup_cfg_hdlr = {
#ifdef HAVE_SHOOK
    .module_name = "shook",
#else
    .module_name = "backup",
#endif
    .new = backup_cfg_new,
    .free = backup_cfg_free,
    .set_default = backup_cfg_set_default,
    .read = backup_cfg_read,
    .set_config = backup_cfg_set,
    .write_default = backup_cfg_write_default,
    .write_template = backup_cfg_write_template,
};

/* -------------- status management stuff ------------- */

/** set of managed status */
typedef enum {
    STATUS_UNKNOWN = 0, /* undetermined status */
    STATUS_NEW, /* file does not exists in the backend */
    STATUS_MODIFIED, /* file has been modified since it was stored in
                      * the backend */
    STATUS_RESTORE_RUNNING, /* file is being retrieved */
    STATUS_ARCHIVE_RUNNING, /* file is being archived */
    STATUS_SYNCHRO, /* file has been synchronized in HSM, file can be purged */
    STATUS_RELEASED,    /* file is released (nothing to do). */
    STATUS_RELEASE_PENDING, /* file is being released */

    STATUS_COUNT    /* number of possible file status */
} file_status_t;

/* XXX /!\ Must match file_status_t order */
static const char *backup_status_list[] =
    { "new", "modified", "retrieving", "archiving",
    "synchro", "released", "release_pending"
};

static const char *backup_status2str(file_status_t st)
{
    if ((st >= STATUS_COUNT) || (st == STATUS_UNKNOWN))
        return NULL;
    else
        return backup_status_list[st - 1];  /* st=1 => new */
}

/** enum of specific attributes */
enum backup_info_e {
    ATTR_BK_PATH = 0,
    ATTR_LAST_ARCH
};

/** size of specific info to be stored in DB:
 * backend_path: full path in backend
 * last_archive: unix epoch
 */
static sm_info_def_t backup_info[] = {
    [ATTR_BK_PATH] =
        {"backend_path", "bkpath", DB_TEXT, RBH_PATH_MAX - 1, {.val_str = NULL},
         PT_STRING},
    [ATTR_LAST_ARCH] =
        {"last_archive", "lstarc", DB_UINT, 0, {.val_uint = 0}, PT_DURATION}
};

/** helper to compare a status */
static bool status_equal(struct sm_instance *smi, const attr_set_t *attrs,
                         file_status_t status)
{
    return !strcmp(STATUS_ATTR(attrs, smi->smi_index),
                   backup_status2str(status));
}

/** to check backend mount point */
static dev_t backend_dev = 0;
static char backend_name[RBH_PATH_MAX] = "";

/* is it a special shell character */
static inline bool is_shell_special(char c)
{
    static const char specials[] = "`#$*?!|;&<>[]{}'\"\\";
    const char *curr;

    for (curr = specials; (*curr) != '\0'; curr++)
        if (c == (*curr))
            return true;

    /* not found */
    return false;
}

#define is_allowed_char(_c) (isprint(_c) && !isspace(_c) \
                             && !is_shell_special(_c))

/* clean non printable characters, spaces, special chars, ... */
static void clean_bad_chars(char *path)
{
    char *curr;
    for (curr = path; *curr != '\0'; curr++) {
        if (!is_allowed_char(*curr))
            *curr = '_';
    }
}

#ifdef HAVE_SHOOK
static char lock_dirname[RBH_NAME_MAX] = "";
static char restripe_dirname[RBH_NAME_MAX] = "";
#endif

/**
 * Initialize the extension module.
 * set_config function is supposed to have to been called before.
 * \param[in] flags from command line.
 */
static int backup_init(struct sm_instance *smi, run_flags_t flags)
{
    int rc;

#ifdef HAVE_SHOOK   /* releasing files need shook */
    rc = shook_svr_init(config.shook_cfg);
    if (rc) {
        DisplayLog(LVL_CRIT, TAG, "ERROR %d initializing shook server library",
                   rc);
        return rc;
    }

    rh_strncpy(lock_dirname, rh_basename(LOCK_DIR), sizeof(lock_dirname));
    rh_strncpy(restripe_dirname, rh_basename(RESTRIPE_DIR),
               sizeof(restripe_dirname));
#endif

    /* check that backend filesystem is mounted */
    rc = check_fs_info(config.root, config.mnt_type, &backend_dev,
                       backend_name, config.check_mounted, false);
    if (rc)
        return rc;

    return 0;
}

/** @TODO te be called by get_status function, changelog callback, etc. */
static bool backup_ignore(const entry_id_t *p_id, attr_set_t *p_attrs)
{
#ifndef HAVE_SHOOK
    /* ignore nothing */
    return false;
#else
    /* if we don't know the full path, but the name looks like
     * an ignored entry, get the path */
    if (!ATTR_MASK_TEST(p_attrs, fullpath)
        && ATTR_MASK_TEST(p_attrs, name)) {
        if (!strcmp(ATTR(p_attrs, name), SHOOK_DIR)
            || !strcmp(ATTR(p_attrs, name), lock_dirname)
            || !strcmp(ATTR(p_attrs, name), restripe_dirname)
            || !strncmp(SHOOK_LOCK_PREFIX, ATTR(p_attrs, name),
                        strlen(SHOOK_LOCK_PREFIX))
            || !strncmp(RESTRIPE_SRC_PREFIX, ATTR(p_attrs, name),
                        strlen(RESTRIPE_SRC_PREFIX))
            || !strncmp(RESTRIPE_TGT_PREFIX, ATTR(p_attrs, name),
                        strlen(RESTRIPE_TGT_PREFIX))) {
            if (Lustre_GetFullPath(p_id, ATTR(p_attrs, fullpath), RBH_PATH_MAX)
                != 0)
                /* ignore, by default */
                return true;
            else
                /* continue with path checking */
                ATTR_MASK_SET(p_attrs, fullpath);
        } else  /* no possible match */
            return false;
    }

    if (ATTR_MASK_TEST(p_attrs, fullpath)) {
        /* check lock file */
        if (!fnmatch
            ("*/" LOCK_DIR "/" SHOOK_LOCK_PREFIX "*", ATTR(p_attrs, fullpath),
             0)) {
            /* skip the entry */
            DisplayLog(LVL_DEBUG, TAG, "%s is a shook lock",
                       ATTR(p_attrs, fullpath));
            /** @TODO raise special event for the file: LOCK/UNLOCK */
            return true;
        }
        /* check lock dir */
        else if (!fnmatch("*/" LOCK_DIR, ATTR(p_attrs, fullpath), 0)) {
            /* skip the entry */
            DisplayLog(LVL_DEBUG, TAG, "%s is a shook lock dir",
                       ATTR(p_attrs, fullpath));
            return true;
        }
        /* check restripe dir */
        else if (!fnmatch("*/" RESTRIPE_DIR, ATTR(p_attrs, fullpath), 0)) {
            /* skip the entry */
            DisplayLog(LVL_DEBUG, TAG, "%s is a shook restripe dir",
                       ATTR(p_attrs, fullpath));
            return true;
        }
    }

    /* match '.shook' directory */
    if (p_attrs && ATTR_MASK_TEST(p_attrs, name)
        && ATTR_MASK_TEST(p_attrs, type)) {
        if (!strcmp(STR_TYPE_DIR, ATTR(p_attrs, type)) &&
            !strcmp(SHOOK_DIR, ATTR(p_attrs, name))) {
            /* skip the entry */
            DisplayLog(LVL_DEBUG, TAG, "\"%s\" is a shook dir",
                       ATTR(p_attrs, name));
            return true;
        }
    }

    /* if the removed entry is a restripe source,
     * we MUST NOT remove the backend entry
     * as it will be linked to the restripe target
     */
    if ((ATTR_MASK_TEST(p_attrs, fullpath)
         && !fnmatch("*/" RESTRIPE_DIR "/" RESTRIPE_SRC_PREFIX "*",
                     ATTR(p_attrs, fullpath), 0))
        || (ATTR_MASK_TEST(p_attrs, name)
            && !strncmp(RESTRIPE_SRC_PREFIX, ATTR(p_attrs, name),
                        strlen(RESTRIPE_SRC_PREFIX)))) {
        DisplayLog(LVL_DEBUG, TAG,
                   "Removing shook stripe source %s: no removal in backend!",
                   ATTR_MASK_TEST(p_attrs, fullpath) ? ATTR(p_attrs,
                                                            fullpath) :
                   ATTR(p_attrs, name));
        return true;
    }

    return false;
#endif
}

typedef enum {
    FOR_LOOKUP,
    FOR_NEW_COPY
} what_for_e;

/* path for entry we don't known the path in Lustre */
#define UNK_PATH    "__unknown_path"
/* name for entry we don't known the name in Lustre */
#define UNK_NAME    "__unknown_name"
/* extension for temporary copy file */
#define COPY_EXT    "xfer"
/* trash directory for orphan files */
#define TRASH_DIR   ".orphans"

/**
 * Build the path of a given entry in the backend.
 */
static void entry2backend_path(sm_instance_t *smi,
                               const entry_id_t *p_id,
                               const attr_set_t *p_attrs_in,
                               what_for_e what_for,
                               char *backend_path, int allow_compress)
{
    int pathlen;
    char rel_path[RBH_PATH_MAX];

    if (ATTR_MASK_INFO_TEST(p_attrs_in, smi, ATTR_BK_PATH)) {
        DisplayLog(LVL_DEBUG, TAG, "%s: previous backend_path: %s",
                   (what_for == FOR_LOOKUP) ? "LOOKUP" : "NEW_COPY",
                   (char *)SMI_INFO(p_attrs_in, smi, ATTR_BK_PATH));
    } else if (ATTR_MASK_TEST(p_attrs_in, type) &&
               !strcasecmp(ATTR(p_attrs_in, type), STR_TYPE_DIR)) {
        if (ATTR_MASK_TEST(p_attrs_in, fullpath) &&
            ATTR(p_attrs_in, fullpath)[0] == '/' &&
            relative_path(ATTR(p_attrs_in, fullpath), global_config.fs_path,
                          rel_path) == 0) {
            DisplayLog(LVL_DEBUG, TAG,
                       "%s is a directory: backend path is the same",
                       ATTR(p_attrs_in, fullpath));

            if (!strcmp(config.root, "/"))  /* root is '/' */
                sprintf(backend_path, "/%s", rel_path);
            else
                sprintf(backend_path, "%s/%s", config.root, rel_path);
        } else {    /* we don't have fullpath available */

            const char *fname;

            /* There is something in the fullpath, but it is not under FS root
             * or it is relative */
            if (ATTR_MASK_TEST(p_attrs_in, fullpath)) {
                if (ATTR(p_attrs_in, fullpath)[0] == '/')
                    fname = ATTR(p_attrs_in, fullpath) + 1;
                else
                    fname = ATTR(p_attrs_in, fullpath);
            } else if (ATTR_MASK_TEST(p_attrs_in, name))
                fname = ATTR(p_attrs_in, name);
            else
                fname = UNK_NAME;

            /* backup entry to a special dir */
            if (!strcmp(config.root, "/"))  /* root is '/' */
                sprintf(backend_path, "/%s/%s", UNK_PATH, fname);
            else
                sprintf(backend_path, "%s/%s/%s", config.root, UNK_PATH, fname);
        }

        /* clean bad characters */
        clean_bad_chars(backend_path);
        return;
    }
#ifdef HAVE_SHOOK
    else if (what_for != FOR_NEW_COPY) {
        int rc;
        char fidpath[RBH_PATH_MAX];

        BuildFidPath(p_id, fidpath);

        /* retrieve backend path from shook xattrs */
        rc = shook_get_hsm_info(fidpath, backend_path, NULL);
        if ((rc == 0) && !EMPTY_STRING(backend_path))
            return;
    }
#endif

    if ((what_for == FOR_LOOKUP)
        && ATTR_MASK_INFO_TEST(p_attrs_in, smi, ATTR_BK_PATH)) {
        /* For lookup, if there is a previous path in the backend, use it. */
        strcpy(backend_path, (char *)SMI_INFO(p_attrs_in, smi, ATTR_BK_PATH));
    } else {    /* in any other case, build a path from scratch */

        /* if the fullpath is available, build human readable path */
        if (ATTR_MASK_TEST(p_attrs_in, fullpath) &&
            ATTR(p_attrs_in, fullpath)[0] == '/' &&
            relative_path(ATTR(p_attrs_in, fullpath), global_config.fs_path,
                          rel_path) == 0) {
            /* backend path is '<bakend_root>/<rel_path>' */

            if (!strcmp(config.root, "/"))  /* root is '/' */
                sprintf(backend_path, "/%s", rel_path);
            else
                sprintf(backend_path, "%s/%s", config.root, rel_path);
        } else {
            /* no fullpath available (or not in FS root, or relative) */
            const char *fname;

            /* There is something in the fullpath, but it is not under FS root
             * or it is relative */
            if (ATTR_MASK_TEST(p_attrs_in, fullpath)) {
                if (ATTR(p_attrs_in, fullpath)[0] == '/')
                    fname = ATTR(p_attrs_in, fullpath) + 1;
                else
                    fname = ATTR(p_attrs_in, fullpath);
            } else if (ATTR_MASK_TEST(p_attrs_in, name))
                fname = ATTR(p_attrs_in, name);
            else
                fname = UNK_NAME;

            /* backup entry to a special dir */
            if (!strcmp(config.root, "/"))  /* root is '/' */
                sprintf(backend_path, "/%s/%s", UNK_PATH, fname);
            else
                sprintf(backend_path, "%s/%s/%s", config.root, UNK_PATH, fname);
        }

        /* clean bad characters */
        clean_bad_chars(backend_path);

        /* add __<id> after the name */
        pathlen = strlen(backend_path);
#ifdef _HAVE_FID
        sprintf(backend_path + pathlen, "__" DFID_NOBRACE, PFID(p_id));
#else
        sprintf(backend_path + pathlen, "__%#LX:%#LX",
                (unsigned long long)p_id->device,
                (unsigned long long)p_id->inode);
#endif
        /* check if compression is enabled and if the entry is a file */
        if (allow_compress
            && !strcasecmp(ATTR(p_attrs_in, type), STR_TYPE_FILE)) {
            /* append z in this case */
            strcat(backend_path, "z");
        }
    }
    return;
}

/**
 * Determine if an entry is being archived
 * \retval 0: not archiving
 * \retval <0: error
 * \retval >0: last modification time
 */
static int entry_is_archiving(const char *backend_path)
{
    char xfer_path[RBH_PATH_MAX];
    struct stat cp_md;
    int rc;
    sprintf(xfer_path, "%s.%s", backend_path, COPY_EXT);

    if (lstat(xfer_path, &cp_md) != 0) {
        rc = -errno;
        if ((rc == -ENOENT) || (rc == -ESTALE))
            return 0;
        else
            return rc;
    }
    /* xfer is running. return last action time */
    return MAX3(cp_md.st_mtime, cp_md.st_ctime, cp_md.st_atime);
}

/**
 * Cleans a timed-out transfer
 */
static int transfer_cleanup(const char *backend_path)
{
    char xfer_path[RBH_PATH_MAX];
    int rc;
    sprintf(xfer_path, "%s.%s", backend_path, COPY_EXT);

    if (unlink(xfer_path) != 0) {
        rc = -errno;
        return rc;
    }
    return 0;
}

/**
 * Move an orphan file to orphan directory
 */
static int move_orphan(const char *path)
{
    char dest[RBH_PATH_MAX];
    const char *fname;
    int rc;

    /* does the trash directory exist? */
    sprintf(dest, "%s/%s", config.root, TRASH_DIR);
    if ((mkdir(dest, 0750) != 0) && (errno != EEXIST)) {
        rc = -errno;
        DisplayLog(LVL_MAJOR, TAG, "Error creating directory %s: %s",
                   dest, strerror(-rc));
        return rc;
    }

    fname = rh_basename(path);
    if (fname == NULL || (strcmp(fname, "/") == 0) || EMPTY_STRING(fname)) {
        DisplayLog(LVL_MAJOR, TAG, "Invalid path '%s'", path);
        return -EINVAL;
    }
    /* move the orphan to the directory */
    snprintf(dest, RBH_PATH_MAX, "%s/%s/%s", config.root, TRASH_DIR, fname);

    if (rename(path, dest) != 0) {
        rc = -errno;
        DisplayLog(LVL_MAJOR, TAG, "Error moving '%s' to '%s'", path, dest);
        return rc;
    }

    DisplayLog(LVL_EVENT, TAG, "'%s' moved to '%s'", path, dest);
    return 0;
}

/* check if there is a running copy and if it timed-out
 * return <0 on error
 * 0 if no copy is running
 * 1 if a copy is already running
 * */
static int check_running_copy(const char *bkpath)
{
    int rc;
    /* is a copy running for this entry? */
    rc = entry_is_archiving(bkpath);
    if (rc < 0) {
        DisplayLog(LVL_MAJOR, TAG,
                   "Error %d checking if copy is running for %s: %s", rc,
                   bkpath, strerror(-rc));
        return rc;
    } else if (rc > 0) {
        if (config.copy_timeout && (time(NULL) - rc > config.copy_timeout)) {
            DisplayLog(LVL_EVENT, TAG,
                       "Copy timed out for %s (inactive for %us)", bkpath,
                       (unsigned int)(time(NULL) - rc));
            /* previous copy timed out: clean it */
            transfer_cleanup(bkpath);
        } else {
            DisplayLog(LVL_DEBUG, TAG,
                       "'%s' is being archived (last mod: %us ago)",
                       bkpath, (unsigned int)(time(NULL) - rc));
            return 1;
        }
    }
    return 0;
}

/**
 * Get entry info from the backend (like lstat), but also check if the
 * entry is compressed.
 * Prioritarily check the entry with the selected compression on/off.
 */
static int bk_lstat(const char *bkpath, struct stat *bkmd,
                    bool check_compressed, bool *compressed)
{
    char tmp[RBH_PATH_MAX];
    int len = strlen(bkpath);

    *compressed = !!(bkpath[len - 1] == 'z');

    if (!check_compressed)  /* not a file, call standard lstat() */
        return lstat(bkpath, bkmd);

    if (!lstat(bkpath, bkmd))
        return 0;

    if ((errno == ENOENT) || (errno == ESTALE)) {
        if (*compressed) {
            /* try without compression */
            strcpy(tmp, bkpath);
            tmp[len - 1] = '\0';

            if (lstat(tmp, bkmd) == 0) {
                *compressed = 0;
                return 0;
            }
        } else if (!(*compressed)) {
            /* try with compression */
            sprintf(tmp, "%sz", bkpath);
            if (lstat(tmp, bkmd) == 0) {
                *compressed = true;
                return 0;
            }
        }
    }
    return -1;
}

/** helper to set the entry status for the given SMI */
static inline int set_backup_status(sm_instance_t *smi, attr_set_t *pattrs,
                                    file_status_t st)
{
    return set_status_attr(smi, pattrs, backup_status2str(st));
}

/** helper to get backend path from attribute structure */
#define BKPATH(_pattr, _smi) ((char *)SMI_INFO((_pattr), (_smi), ATTR_BK_PATH))

/** helper to set backend path */
static inline int set_backend_path(sm_instance_t *smi, attr_set_t *pattrs,
                                   const char *bkpath)
{
    char *info = strdup(bkpath);
    int rc;

    if (info == NULL)
        return -ENOMEM;

    rc = set_sm_info(smi, pattrs, ATTR_BK_PATH, info);
    if (rc)
        free(info);

    return rc;
}

/** helper to set last_archive */
static inline int set_last_archive(sm_instance_t *smi, attr_set_t *pattrs,
                                   time_t last_arch)
{
    return set_uint_info(smi, pattrs, ATTR_LAST_ARCH, (unsigned int)last_arch);
}

/** return the path to access an entry in the filesystem */
static int entry_fs_path(const entry_id_t *p_id, const attr_set_t *p_attrs,
                         char *fspath)
{
#ifdef _HAVE_FID
    /* for Lustre 2, use fid path so the operation is not disturbed by
     * renames... */
    BuildFidPath(p_id, fspath);
#else
    /* we need the posix path */
    if (!ATTR_MASK_TEST(p_attrs, fullpath)) {
        DisplayLog(LVL_CRIT, TAG, "Error in %s(): path argument is "
                   "mandatory for archive command", __FUNCTION__);
        return -EINVAL;
    }
    strcpy(fspath, ATTR(p_attrs, fullpath));
#endif
    return 0;
}

/**
 * Get the status for an entry.
 * \param[in] p_id pointer to entry id
 * \param[in] attrs_in pointer to entry attributes
 * \param[out] p_attrs_changed changed/retrieved attributes
 */
static int backup_status(struct sm_instance *smi,
                         const entry_id_t *p_id, const attr_set_t *p_attrs_in,
                         attr_set_t *p_attrs_changed)
{
    int rc;
    struct stat bkmd;
    obj_type_t entry_type;
    char bkpath[RBH_PATH_MAX];
    bool compressed = false;

    /* check if mtime is provided (mandatory) */
    if (!ATTR_MASK_TEST(p_attrs_in, last_mod)
        || !ATTR_MASK_TEST(p_attrs_in, type)) {
        DisplayLog(LVL_MAJOR, TAG,
                   "Missing mandatory attribute for checking entry status");
        return -EINVAL;
    }

    /* path to lookup the entry in the backend */
    entry2backend_path(smi, p_id, p_attrs_in, FOR_LOOKUP, bkpath,
                       config.compress);

    /* is the entry has a supported type? */
    entry_type = db2type(ATTR(p_attrs_in, type));

    if ((entry_type != TYPE_FILE) && (entry_type != TYPE_LINK)) {
        DisplayLog(LVL_VERB, TAG, "Unsupported type %s for this backend",
                   ATTR(p_attrs_in, type));
        return -ENOTSUP;
    }
#ifdef HAVE_SHOOK
    /* @TODO: ignore shook special entries */

    /* check status from libshook.
     * return if status != ONLINE
     * else, continue checking.
     */
    char fidpath[RBH_PATH_MAX];
    file_status_t status;

    BuildFidPath(p_id, fidpath);

    rc = ShookGetStatus(fidpath, &status);
    if (rc)
        return rc;

    /* if status is 'release_pending' or 'restore_running',
     * check timeout. */
    if (status == STATUS_RELEASE_PENDING || status == STATUS_RESTORE_RUNNING) {
        rc = ShookRecoverById(p_id, &status);
        if (rc < 0)
            return rc;
    }

    if (status != STATUS_SYNCHRO) {
        DisplayLog(LVL_FULL, TAG, "shook reported status<>online: %d", status);
        ATTR_MASK_SET(p_attrs_changed, status);
        ATTR(p_attrs_changed, status) = status;

        /* set backend path if it is not known */
        if (!ATTR_MASK_INFO_TEST(p_attrs_in, smi, ATTR_BK_PATH)
            && !ATTR_MASK_INFO_TEST(p_attrs_changed, smi, ATTR_BK_PATH)) {
            rc = set_backend_path(smi, p_attrs_changed, bkpath);
            if (rc)
                return rc;
        }

        return 0;
    }
    /* else: must compare status with backend */
#endif

    if (entry_type == TYPE_FILE) {
        /* is a copy running for this entry? */
        rc = check_running_copy(bkpath);
        if (rc < 0)
            return rc;
        else if (rc > 0) {  /* current archive */
            return set_backup_status(smi, p_attrs_changed,
                                     STATUS_ARCHIVE_RUNNING);
        }
    }

    /* get entry info */
    if (bk_lstat(bkpath, &bkmd, entry_type == TYPE_FILE, &compressed) != 0) {
        rc = -errno;
        if ((rc != -ENOENT) && (rc != -ESTALE)) {
            DisplayLog(LVL_MAJOR, TAG, "Lookup error for path '%s': %s",
                       bkpath, strerror(-rc));
            return rc;
        } else {
            DisplayLog(LVL_DEBUG, TAG,
                       "'%s' does not exist in the backend (new entry): %s",
                       bkpath, strerror(-rc));
            /* no entry in the backend: new entry */
            return set_backup_status(smi, p_attrs_changed, STATUS_NEW);
        }
    }

    if (entry_type == TYPE_FILE) {
        if (!S_ISREG(bkmd.st_mode)) {
            /* entry of invalid type */
            DisplayLog(LVL_MAJOR, TAG,
                       "Different type in backend for entry %s. Moving it to orphan dir.",
                       bkpath);
            rc = move_orphan(bkpath);
            if (rc)
                return rc;
            return set_backup_status(smi, p_attrs_changed, STATUS_NEW);
        }
        /* compare mtime and size to check if the entry changed */
        /* XXX consider it modified this even if mtime is smaller */
        if ((ATTR(p_attrs_in, last_mod) != bkmd.st_mtime)
            || ((ATTR(p_attrs_in, size) != bkmd.st_size) && !compressed)) {
            /* display a warning if last_mod in FS < mtime in backend */
            if (ATTR(p_attrs_in, last_mod) < bkmd.st_mtime)
                DisplayLog(LVL_MAJOR, TAG,
                           "Warning: mtime in filesystem < mtime in backend (%s)",
                           bkpath);

            rc = set_backup_status(smi, p_attrs_changed, STATUS_MODIFIED);
            if (rc)
                return rc;

            /* update path in the backend */
            return set_backend_path(smi, p_attrs_changed, bkpath);
        } else {
            rc = set_backup_status(smi, p_attrs_changed, STATUS_SYNCHRO);
            if (rc)
                return rc;

            /* update path in the backend */
            return set_backend_path(smi, p_attrs_changed, bkpath);
        }
    } else if (entry_type == TYPE_LINK) {
        char lnk1[RBH_PATH_MAX];
        char lnk2[RBH_PATH_MAX];
        char fspath[RBH_PATH_MAX];

        if (!S_ISLNK(bkmd.st_mode)) {
            DisplayLog(LVL_MAJOR, TAG,
                       "Different type in backend for entry %s. Moving it to orphan dir.",
                       bkpath);
            rc = move_orphan(bkpath);
            if (rc)
                return rc;

            return set_backup_status(smi, p_attrs_changed, STATUS_NEW);
        }

        rc = entry_fs_path(p_id, p_attrs_in, fspath);
        if (rc)
            return rc;

        /* compare symlink contents */
        if ((rc = readlink(bkpath, lnk1, RBH_PATH_MAX)) < 0) {
            rc = -errno;
            if (rc == -ENOENT) {
                /* entry disappeared */
                return set_backup_status(smi, p_attrs_changed, STATUS_NEW);
            } else
                return rc;
        }
        lnk1[rc] = '\0';
        DisplayLog(LVL_FULL, TAG, "backend symlink => %s", lnk1);
        if ((rc = readlink(fspath, lnk2, RBH_PATH_MAX)) < 0) {
            rc = -errno;
            DisplayLog(LVL_EVENT, TAG, "Error performing readlink(%s): %s",
                       fspath, strerror(-rc));
            return rc;
        }
        lnk2[rc] = '\0';
        DisplayLog(LVL_FULL, TAG, "FS symlink => %s", lnk2);
        if (strcmp(lnk1, lnk2)) {
            /* symlink contents is different */
            rc = set_backup_status(smi, p_attrs_changed, STATUS_MODIFIED);
            if (rc)
                return rc;

            /* update path in the backend */
            return set_backend_path(smi, p_attrs_changed, bkpath);
        } else {    /* same contents */

            rc = set_backup_status(smi, p_attrs_changed, STATUS_SYNCHRO);
            if (rc)
                return rc;

            /* update path in the backend */
            return set_backend_path(smi, p_attrs_changed, bkpath);
        }
    } else {
        return -ENOTSUP;
    }

    /* TODO What about STATUS_REMOVED? */
}

/**
 * Function to determine if a deleted entry must be inserted to SOFTRM table
 */
static proc_action_e backup_softrm_filter(struct sm_instance *smi,
                                          const entry_id_t *id,
                                          const attr_set_t *attrs)
{
    /** @TODO support dirs here to clean empty directories? */

    if (ATTR_MASK_STATUS_TEST(attrs, smi->smi_index)
        && status_equal(smi, attrs, STATUS_NEW)) {
        DisplayLog(LVL_DEBUG, TAG,
                   "Removing 'new' entry (" DFID "): no remove in backend",
                   PFID(id));
        return PROC_ACT_RM_ALL;
    }
#ifdef HAVE_SHOOK
    /* if the removed entry is a restripe source,
     * we MUST NOT remove the backend entry
     * as it will be linked to the restripe target
     */
    /** @TODO adapt to RBHv3 */
    else if ((ATTR_FSorDB_TEST(p_op, fullpath)
              && !fnmatch("*/" RESTRIPE_DIR "/" RESTRIPE_SRC_PREFIX "*",
                          ATTR_FSorDB(p_op, fullpath), 0))
             || (ATTR_FSorDB_TEST(p_op, name)
                 && !strncmp(RESTRIPE_SRC_PREFIX, ATTR_FSorDB(p_op, name),
                             strlen(RESTRIPE_SRC_PREFIX)))) {
        DisplayLog(LVL_DEBUG, ENTRYPROC_TAG,
                   "Removing shook stripe source %s: no remove in backend!",
                   ATTR_FSorDB_TEST(p_op, fullpath) ? ATTR_FSorDB(p_op,
                                                                  fullpath) :
                   ATTR_FSorDB(p_op, name));
        return PROC_ACT_NONE;
    }
#endif
    /* If we have a doubt, always insert to softrm.
     *  In the worst case, it's just a useless backup_rm operation.
     */
    return PROC_ACT_SOFTRM_ALWAYS;
}

static int backup_cl_cb(struct sm_instance *smi, const CL_REC_TYPE *logrec,
                        const entry_id_t *id, const attr_set_t *attrs,
                        attr_set_t *refreshed_attrs, bool *getit,
                        proc_action_e *rec_action)
{
    /* If this is a CREATE record, we know its status is NEW
     * (except if it is already set to another value) */
    if (logrec->cr_type == CL_CREATE || logrec->cr_type == CL_SOFTLINK) {
        if (!ATTR_MASK_STATUS_TEST(attrs, smi->smi_index)) {
            /* new file, status is new */
            set_backup_status(smi, refreshed_attrs, STATUS_NEW);
            /* no need to retrieve it from filesystem */
            *getit = false;
        }
        /* else: file is already known. Preserve the known status. */
    } else if ((logrec->cr_type == CL_MKDIR) || (logrec->cr_type == CL_RMDIR)) {
        /* no status for directories */
        *getit = false;
    } else if (logrec->cr_type == CL_MTIME || logrec->cr_type == CL_TRUNC ||
               logrec->cr_type == CL_CLOSE) {
        /* If file is modified or truncated, need to check its status
         * (probably modified) EXCEPT if its status is already 'modified' */
        if (!ATTR_MASK_STATUS_TEST(attrs, smi->smi_index)
            || (!status_equal(smi, attrs, STATUS_NEW) &&
                !status_equal(smi, attrs, STATUS_MODIFIED))) {
            DisplayLog(LVL_DEBUG, TAG,
                       "Getstatus needed because this is a %s event "
                       "and status is not already 'modified' or 'new': status=%s",
                       changelog_type2str(logrec->cr_type),
                       ATTR_MASK_STATUS_TEST(attrs, smi->smi_index) ?
                       STATUS_ATTR(attrs, smi->smi_index) : "<not set>");
            *getit = true;
        }
    } else if ((logrec->cr_type == CL_UNLINK)
               && (logrec->cr_flags & CLF_UNLINK_LAST)) {
        *rec_action = backup_softrm_filter(smi, id, attrs);
    }
#ifdef HAVE_SHOOK
    else if (logrec->cr_type == CL_XATTR) {
        /* need to update status */
        *getit = true;
    } else if (logrec->cr_type == CL_CTIME || (logrec->cr_type == CL_SETATTR)) {
        /* in Lustre v2.O, changing trusted xattr generates CTIME/SATTR event */
        *getit = true;
    }

    /** @FIXME */
    if (p_op->db_exists) {
        /* if the old name is a restripe file, update the status */
        if (!strncmp(RESTRIPE_TGT_PREFIX, ATTR(&p_op->db_attrs, name),
                     strlen(RESTRIPE_TGT_PREFIX))) {
            p_op->fs_attr_need |= ATTR_MASK_status;
            DisplayLog(LVL_DEBUG, TAG,
                       "Getstatus needed because entry was a restripe target");
        }
    }
#endif

    return 0;
}

typedef enum { TO_FS, TO_BACKEND } target_e;

/**
 * get metadata of a directory in filesystem or in backend
 * by target path
 */
static int get_orig_dir_md(const char *target_dir, struct stat *st,
                           target_e target)
{
    char rel_path[RBH_PATH_MAX];
    char orig_path[RBH_PATH_MAX];
    int rc;
    const char *dest_root;
    const char *src_root;

    if (target == TO_BACKEND) {
        dest_root = config.root;
        src_root = global_config.fs_path;
    } else {
        dest_root = global_config.fs_path;
        src_root = config.root;
    }

    rc = relative_path(target_dir, dest_root, rel_path);
    if (rc)
        return rc;

    /* orig path is '<fs_root>/<rel_path>' */
    sprintf(orig_path, "%s/%s", src_root, rel_path);

    DisplayLog(LVL_FULL, TAG, "Target directory: %s, source directory: %s",
               target_dir, orig_path);

    if (lstat(orig_path, st)) {
        rc = -errno;
        DisplayLog(LVL_DEBUG, TAG, "Cannot stat %s: %s",
                   orig_path, strerror(-rc));
        return rc;
    } else
        return 0;
}

/**
 *  Ensure POSIX directory exists
 */
static int mkdir_recurse_clone_attrs(const char *full_path, mode_t default_mode,
                                     target_e target)
{
    char path_copy[MAXPATHLEN];
    const char *curr;
    struct stat st;
    mode_t mode;
    int rc;
    bool setattrs = false;

    /* to backend or the other way? */
    if (target == TO_BACKEND) {
        if (strncmp(config.root, full_path, strlen(config.root)) != 0) {
            DisplayLog(LVL_MAJOR, TAG,
                       "Error: '%s' is not under backend root '%s'", full_path,
                       config.root);
            return -EINVAL;
        }
        /* skip backend root */
        curr = full_path + strlen(config.root);
    } else {
        /* is it relative? */
        if (!EMPTY_STRING(full_path) && (full_path[0] != '/')) {
            curr = full_path;
            goto relative;
        } else
            if (strncmp
                (global_config.fs_path, full_path,
                 strlen(global_config.fs_path)) != 0) {
            DisplayLog(LVL_MAJOR, TAG,
                       "Error: '%s' is not under filesystem root '%s'",
                       full_path, global_config.fs_path);
            return -EINVAL;
        }
        /* skip fs root */
        curr = full_path + strlen(global_config.fs_path);
    }

    if (*curr == '\0')  /* full_path is root dir */
        return 0;
    else if (*curr != '/') {    /* slash expected */
        DisplayLog(LVL_MAJOR, TAG, "Error: '%s' is not under backend root '%s'",
                   full_path,
                   (target ==
                    TO_BACKEND) ? config.root : global_config.fs_path);
        return -EINVAL;
    }

    /* skip first slash */
    curr++;
 relative:

    while ((curr = strchr(curr, '/')) != NULL) {
        /* if fullpath = '/a/b',
         * curr = &(fullpath[2]);
         * so, copy 2 chars to get '/a'.
         * and set fullpath[2] = '\0'
         */
        int path_len = curr - full_path;

        /* extract directory name */
        strncpy(path_copy, full_path, path_len);
        path_copy[path_len] = '\0';

        /* stat dir */
        if (lstat(path_copy, &st) != 0) {
            rc = -errno;
            if (rc != -ENOENT) {
                DisplayLog(LVL_CRIT, TAG, "Cannot lstat() '%s': %s", path_copy,
                           strerror(-rc));
                return rc;
            }

            if (get_orig_dir_md(path_copy, &st, target) == 0) {
                mode = st.st_mode & 07777;
                setattrs = true;
            } else {
                mode = default_mode;
                setattrs = false;
            }

            DisplayLog(LVL_FULL, TAG, "mkdir(%s)", path_copy);
            if ((mkdir(path_copy, mode) != 0) && (errno != EEXIST)) {
                rc = -errno;
                DisplayLog(LVL_CRIT, TAG, "mkdir(%s) failed: %s",
                           path_copy, strerror(-rc));
                return rc;
            }

            if (setattrs) {
                /* set owner and group */
                if (lchown(path_copy, st.st_uid, st.st_gid))
                    DisplayLog(LVL_MAJOR, TAG,
                               "Error setting owner/group for '%s': %s",
                               path_copy, strerror(errno));
                /* mode is set by mkdir */
            }
        } else if (!S_ISDIR(st.st_mode)) {
            DisplayLog(LVL_CRIT, TAG,
                       "Cannot create directory '%s': existing non-directory",
                       path_copy);
            return -ENOTDIR;
        }

        curr++;
    }

    if (get_orig_dir_md(full_path, &st, target) == 0) {
        mode = st.st_mode & 07777;
        setattrs = true;
    } else {
        mode = default_mode;
        setattrs = false;
    }

    /* finally create this dir */
    DisplayLog(LVL_FULL, TAG, "mkdir(%s)", full_path);
    if ((mkdir(full_path, mode) != 0) && (errno != EEXIST)) {
        rc = -errno;
        DisplayLog(LVL_CRIT, TAG, "mkdir(%s) failed: %s", full_path,
                   strerror(-rc));
        return rc;
    } else if (setattrs) {
        /* set owner and group */
        if (lchown(full_path, st.st_uid, st.st_gid))
            DisplayLog(LVL_MAJOR, TAG, "Error setting owner/group for '%s': %s",
                       full_path, strerror(errno));
        /* mode is set by mkdir (FIXME but can be cleared by chown) */
    }

    return 0;
}

/**
 * Create the parent of the given entry.
 * if target = FS, return the parent fid.
 */
static int create_parent(const char *child_path, target_e target,
                         entry_id_t *p_parent_id)
{
    char tmp[RBH_PATH_MAX];
    char *destdir;
    int rc;

    /* extract parnet dir path */
    strcpy(tmp, child_path);
    destdir = dirname(tmp);
    if (destdir == NULL) {
        DisplayLog(LVL_CRIT, TAG, "Error extracting directory path of '%s'",
                   child_path);
        return -EINVAL;
    }

    /* create the directory */
    rc = mkdir_recurse_clone_attrs(destdir, 0750, target);
    if (rc)
        return rc;

    if (target == TO_FS && p_parent_id != NULL) {
        /* retrieve parent fid */
        rc = path2id(destdir, p_parent_id, NULL);
    }

    return rc;
}

/** create the parent directory (target = filesystem) */
static inline int create_parent_in_fs(const char *child_path,
                                      entry_id_t *p_parent_id)
{
    return create_parent(child_path, TO_FS, p_parent_id);
}

/** create the parent directory (in backend storage) */
static int create_parent_in_backend(const char *child_path)
{
    return create_parent(child_path, TO_BACKEND, NULL);
}

/** get entry information before performing an archive operation */
static int copy_action_precheck(sm_instance_t *smi, const entry_id_t *p_id,
                                attr_set_t *p_attrs, char *bkpath,
                                obj_type_t *entry_type, bool *bk_moved)
{
    int rc;
    struct stat void_stat;

    /* if status is not determined, retrieve it */
    if (!ATTR_MASK_STATUS_TEST(p_attrs, smi->smi_index)) {
        DisplayLog(LVL_DEBUG, TAG, "%s not provided to perform pre-copy checks",
                   smi->db_field);
        rc = backup_status(smi, p_id, p_attrs, p_attrs);
        if (rc)
            return rc;
    }

    if (!ATTR_MASK_TEST(p_attrs, type)) {
        DisplayLog(LVL_MAJOR, TAG,
                   "Missing mandatory attribute 'type' to perform pre-copy checks");
        return -EINVAL;
    }

    /* is it the good type? */
    *entry_type = db2type(ATTR(p_attrs, type));
    if ((*entry_type != TYPE_FILE) && (*entry_type != TYPE_LINK)) {
        DisplayLog(LVL_MAJOR, TAG, "Unsupported type for copy operation: %s",
                   ATTR(p_attrs, type));
        return -ENOTSUP;
    }

    /* compute path for target file */
    entry2backend_path(smi, p_id, p_attrs, FOR_NEW_COPY, bkpath,
                       config.compress);

    /* check the status */
    if (status_equal(smi, p_attrs, STATUS_NEW)) {
        /* check the entry does not already exist */
        if ((lstat(bkpath, &void_stat) == 0) || (errno != ENOENT)) {
            rc = -errno;
            DisplayLog(LVL_MAJOR, TAG,
                       "ERROR: new entry %s already exists in the backend?! errno=%d, %s",
                       bkpath, -rc, strerror(-rc));
            return rc;
        }
    } else if (status_equal(smi, p_attrs, STATUS_MODIFIED)
               || status_equal(smi, p_attrs, STATUS_ARCHIVE_RUNNING)) {
        /* check if somebody else is about to copy */
        rc = check_running_copy(bkpath);
        if (rc < 0)
            return rc;
        else if (rc > 0)    /* current archive */
            return -EALREADY;

        /* check that previous path exists */
        if (ATTR_MASK_INFO_TEST(p_attrs, smi, ATTR_BK_PATH)) {
            char *bp = BKPATH(p_attrs, smi);

            /* need to check if the entry was renamed */
            *bk_moved = true;
            if (lstat(bp, &void_stat) != 0) {
                rc = -errno;
                DisplayLog(LVL_MAJOR, TAG,
                           "Warning: previous copy %s not found in the backend (errno=%d, %s): "
                           "entry will be archived again as %s.", bp, -rc,
                           strerror(-rc), bkpath);
            }
        }
    } else {    /* invalid status */

        /* invalid status for performing archive() */
        DisplayLog(LVL_MAJOR, TAG, "Unexpected status '%s' in %s()",
                   STATUS_ATTR(p_attrs, smi->smi_index), __FUNCTION__);
        return -EINVAL;
    }
    return 0;
}

/** clone a symlink from the filesystem to the archive */
static int backup_symlink(sm_instance_t *smi, attr_set_t *p_attrs,
                          const char *src, const char *dst)
{
    int rc;
    struct stat info;
    char link[RBH_PATH_MAX] = "";

    /* read link content from filesystem */
    if (readlink(src, link, RBH_PATH_MAX) < 0) {
        rc = -errno;
        DisplayLog(LVL_MAJOR, TAG, "Error reading symlink contents (%s): %s",
                   src, strerror(-rc));
        return rc;
    }
    /* link contents is not supposed to change during its lifetime */
    if (symlink(link, dst) != 0) {
        rc = -errno;
        DisplayLog(LVL_MAJOR, TAG,
                   "Error creating symlink %s->\"%s\" in backend: %s", dst,
                   link, strerror(-rc));
        /* keep the same status */
        return rc;
    }

    set_backup_status(smi, p_attrs, STATUS_SYNCHRO);
    set_backend_path(smi, p_attrs, dst);
    set_last_archive(smi, p_attrs, time(NULL));

    /* set symlink owner/group (ignore error, as link contents is saved) */
    if (lstat(src, &info) != 0) {
        rc = -errno;
        DisplayLog(LVL_EVENT, TAG, "Error performing lstat(%s): %s",
                   src, strerror(-rc));
        /* there is something wrong: set the status to unknown */
        set_backup_status(smi, p_attrs, STATUS_UNKNOWN);
    } else if (lchown(dst, info.st_uid, info.st_gid)) {
        rc = -errno;
        DisplayLog(LVL_EVENT, TAG,
                   "error setting owner/group in backend on %s: %s", dst,
                   strerror(-rc));
    }
    return 0;
}

struct attr_save {
    attr_mask_t attr_mask;
    char *attr_path;
};

/**
 * Replace path attribute with target in case of copyback, and save previous
 * value in attr_save.
 */
static void path_replace(struct attr_save *save, attr_set_t *p_attrs,
                         const char *path)
{
    save->attr_mask = null_mask;
    save->attr_mask.std = p_attrs->attr_mask.std & ATTR_MASK_fullpath;

    save->attr_path = ATTR_MASK_TEST(p_attrs, fullpath) ?
        strdup(ATTR(p_attrs, fullpath)) : NULL;

    ATTR_MASK_SET(p_attrs, fullpath);
    strncpy(ATTR(p_attrs, fullpath), path, sizeof(ATTR(p_attrs, fullpath)));
}

/**
 * Restore path and backend path attributes, free allocated fields in
 * attr_save.
 */
static void path_restore(struct attr_save *save, attr_set_t *p_attrs)
{
    /* restore initial values */
    p_attrs->attr_mask.std &= ~ATTR_MASK_fullpath;
    p_attrs->attr_mask = attr_mask_or(&p_attrs->attr_mask, &save->attr_mask);
    if (save->attr_path != NULL) {
        strcpy(ATTR(p_attrs, fullpath), save->attr_path);
        free(save->attr_path);
    }
}

/** wrap the copy action for a file */
static int wrap_file_copy(sm_instance_t *smi,
                          const entry_id_t *p_id, attr_set_t *p_attrs,
                          const char *srcpath, const char *bkpath,
                          bool bk_moved, const action_params_t *params,
                          const policy_action_t *action, post_action_e *after,
                          db_cb_func_t db_cb_fn, void *db_cb_arg)
{
    char *tmp = NULL;
    int rc;
    struct stat info;
    struct attr_save sav = ATTR_SET_INIT;
    action_params_t tmp_params = { 0 };

    /* build tmp copy path */
    asprintf(&tmp, "%s.%s", bkpath, COPY_EXT);
    if (!tmp)
        return -ENOMEM;

#ifdef HAVE_SHOOK
    rc = shook_archive_start(get_fsname(), p_id, bkpath);
    if (rc) {
        DisplayLog(LVL_CRIT, TAG,
                   "Failed to initialize transfer: shook_archive_start() returned error %d",
                   rc);
        goto err_out;
    }
#endif

    /* Actions expect to get a source path in 'fullpath' and targetpath in
     * 'targetpath' parameter.
     * So, build a fake attribute and new parameter set with these values.
     */
    rc = rbh_params_copy(&tmp_params, params);
    if (rc)
        goto err_out;

    if (config.compress) {
        if (rbh_param_set(&tmp_params, "compress", "1", false) != 0) {
            DisplayLog(LVL_CRIT, TAG,
                       "ERROR: failed to set action param 'compress'");
            return -EFAULT;
        }
    }

    rbh_param_set(&tmp_params, TARGET_PATH_PARAM, tmp, true);
    path_replace(&sav, p_attrs, srcpath);

    rc = action_helper(action, "copy", p_id, p_attrs, &tmp_params,
                       smi, NULL, after, db_cb_fn, db_cb_arg);

    /* restore real entry attributes */
    path_restore(&sav, p_attrs);

    if (rc) {
#ifdef HAVE_SHOOK
        shook_archive_abort(get_fsname(), p_id);
#endif
        /* cleanup tmp copy */
        unlink(tmp);
        /* the transfer failed. entry still needs to be archived */
        set_backup_status(smi, p_attrs, STATUS_MODIFIED);
        goto free_params;
    }

    /* finalize the transfer */

    /* owner/group is supposed to be saved by the copy command */

    /* set same mtime as the source file (initial value) */
    if (ATTR_MASK_TEST(p_attrs, last_mod)) {
        struct utimbuf tbuf;
        tbuf.actime = time(NULL);
        tbuf.modtime = ATTR(p_attrs, last_mod);

        if (utime(tmp, &tbuf) != 0) {
            rc = -errno;
            DisplayLog(LVL_CRIT, TAG, "Error setting mtime for file %s: %s",
                       tmp, strerror(-rc));
            /* ignore the error */
            rc = 0;
        }
    }

    /* move the entry to final path */
    if (rename(tmp, bkpath) != 0) {
        rc = -errno;
        DisplayLog(LVL_CRIT, TAG,
                   "Error renaming tmp copy file '%s' to final name '%s': %s",
                   tmp, bkpath, strerror(-rc));

        /* the transfer failed. entry still needs to be archived */
        set_backup_status(smi, p_attrs, STATUS_MODIFIED);
        goto free_params;
    }

    /* has the file been renamed since last copy? */
    if (bk_moved) {
        char *bp = ATTR_MASK_INFO_TEST(p_attrs, smi, ATTR_BK_PATH) ?
            BKPATH(p_attrs, smi) : NULL;

        /* bp is not supposed to be NULL when bk_moved is true */
        assert(bp != NULL);

        /* check if the backend path is different */
        if (strcmp(bkpath, bp)) {
            DisplayLog(LVL_DEBUG, TAG, "Removing previous copy %s", bp);
            if (unlink(bp)) {
                rc = -errno;
                DisplayLog(LVL_DEBUG, TAG,
                           "Error removing previous copy %s: %s", bp,
                           strerror(-rc));
                /* ignore */
                rc = 0;
            }
        }
    }
#ifdef HAVE_SHOOK
    rc = shook_archive_finalize(get_fsname(), p_id, bkpath);
    if (rc) {
        DisplayLog(LVL_CRIT, TAG,
                   "Failed to finalize transfer: shook_archive_finalize() returned error %d",
                   rc);
        return rc;
    }
#endif

    set_backup_status(smi, p_attrs, STATUS_SYNCHRO);
    set_backend_path(smi, p_attrs, bkpath);
    set_last_archive(smi, p_attrs, time(NULL));

    /* get and check attributes after the transfer */
    if (lstat(srcpath, &info) != 0) {
        rc = -errno;
        DisplayLog(LVL_EVENT, TAG, "Error performing final lstat(%s): %s",
                   srcpath, strerror(-rc));
        set_backup_status(smi, p_attrs, STATUS_UNKNOWN);
        return rc;
    }

    /* check final size/mtime */
    if ((info.st_mtime != ATTR(p_attrs, last_mod))
        || (info.st_size != ATTR(p_attrs, size))) {
        DisplayLog(LVL_EVENT, TAG,
                   "Entry %s has been modified during transfer: "
                   "size before/after: %" PRI_SZ "/%" PRI_SZ ", "
                   "mtime before/after: %u/%" PRI_TT, srcpath, ATTR(p_attrs,
                                                                    size),
                   info.st_size, ATTR(p_attrs, last_mod), info.st_mtime);
        set_backup_status(smi, p_attrs, STATUS_MODIFIED);
    }

    /* update entry attributes */
    stat2rbh_attrs(&info, p_attrs, true);
    rc = 0;

 free_params:
    rbh_params_free(&tmp_params);
 err_out:
    free(tmp);
    return rc;
}

/** check this is a supported action */
static bool backup_check_action_name(const char *name)
{
    if (strcasecmp(name, "archive") &&
#ifdef _HAVE_SHOOK
        strcasecmp(name, "release") &&
#endif
        /* special values for deleted entries (for backup_remove) */
        strcasecmp(name, "removed") && strcasecmp(name, "deleted"))
        return false;

    return true;
}

/** executor for copy actions */
static int copy_executor(sm_instance_t *smi, const policy_action_t *action,
                         const entry_id_t *p_id, attr_set_t *p_attrs,
                         const action_params_t *params, post_action_e *after,
                         db_cb_func_t db_cb_fn, void *db_cb_arg)
{
    int rc;
    char bkpath[RBH_PATH_MAX];
    char fspath[RBH_PATH_MAX];
    obj_type_t entry_type;
    bool bk_moved = false;

    /* check mandatory attributes, entry type and status */
    rc = copy_action_precheck(smi, p_id, p_attrs, bkpath, &entry_type,
                              &bk_moved);
    if (rc)
        return rc;

    /* build path in filesystem to access the entry */
    rc = entry_fs_path(p_id, p_attrs, fspath);
    if (rc)
        return rc;

    rc = create_parent_in_backend(bkpath);
    if (rc)
        return rc;

    /* set default for 'after', so it can be overriden in copy action */
    *after = PA_UPDATE;

    /** @TODO if compression is enabled, append 'compress' hint */

    /* run the copy action */
    if (entry_type == TYPE_FILE)
        rc = wrap_file_copy(smi, p_id, p_attrs, fspath, bkpath, bk_moved,
                            params, action, after, db_cb_fn, db_cb_arg);
    else if (entry_type == TYPE_LINK)
        rc = backup_symlink(smi, p_attrs, fspath, bkpath);
    else
        rc = -ENOTSUP;

    return rc;
}

static int remove_executor(sm_instance_t *smi, const policy_action_t *action,
                           const entry_id_t *p_id, attr_set_t *p_attrs,
                           const action_params_t *params,
                           post_action_e *after, db_cb_func_t db_cb_fn,
                           void *db_cb_arg)
{
    const char *backend_path;
    struct attr_save sav = ATTR_SET_INIT;
    char bkpath[RBH_PATH_MAX];
    int rc;

    if (ATTR_MASK_INFO_TEST(p_attrs, smi, ATTR_BK_PATH))
        backend_path = BKPATH(p_attrs, smi);
    else
        /* if there is no backend path, try to guess */
    {
        int lvl_log;

        if (ATTR_MASK_TEST(p_attrs, type)
            && !strcasecmp(ATTR(p_attrs, type), STR_TYPE_FILE))
            lvl_log = LVL_EVENT;
        else
            lvl_log = LVL_VERB;

        entry2backend_path(smi, p_id, p_attrs, FOR_LOOKUP, bkpath,
                           config.compress);
        DisplayLog(lvl_log, TAG,
                   "No backend path is set for " DFID
                   ", guess it could be '%s'", PFID(p_id), bkpath);
        backend_path = bkpath;
    }

    /* replace the path argument by backend_path */
    path_replace(&sav, p_attrs, backend_path);

    rc = action_helper(action, "remove", p_id, p_attrs, params, smi, NULL,
                       after, db_cb_fn, db_cb_arg);

    /* restore real entry attributes */
    path_restore(&sav, p_attrs);

    return rc;
}

/** Wrap command execution */
static int backup_common_executor(sm_instance_t *smi, const char *implements,
                                  const policy_action_t *action,
                                  const entry_id_t *p_id, attr_set_t *p_attrs,
                                  const action_params_t *params,
                                  post_action_e *after, db_cb_func_t db_cb_fn,
                                  void *db_cb_arg)
{

    /** @TODO support execution of hsm_remove actions */
    if (!strcmp(implements, "archive")) {
        return copy_executor(smi, action, p_id, p_attrs, params, after,
                             db_cb_fn, db_cb_arg);
    } else if (!strcmp(implements, "removed")
               || !strcmp(implements, "deleted")) {
        return remove_executor(smi, action, p_id, p_attrs, params, after,
                               db_cb_fn, db_cb_arg);
    } else {
        DisplayLog(LVL_CRIT, TAG,
                   "Operation not supported by status manager %s: '%s'",
                   smi->sm->name, implements);
        return -ENOTSUP;
    }
}

#define IS_ZIP_NAME(_n) (_n[strlen(_n) - 1] == 'z')

/* Rebind a backend entry to a new file in Lustre (with new fid)
 * Notice: fs_path is not necessarily the current path of new_id
 * but it should be moved to this path at the end.
 */
static int backup_rebind(sm_instance_t *smi, const char *fs_path,
                         const char *old_bk_path, char *new_bk_path,
                         const entry_id_t *new_id)
{
    int rc;
    attr_set_t attrs_new;
    struct stat st;
    char tmp[RBH_PATH_MAX];
    char fidpath[RBH_PATH_MAX];
    char *destdir;
    bool compressed = IS_ZIP_NAME(old_bk_path);
    bool retry = false;

    BuildFidPath(new_id, fidpath);

    if (lstat(fidpath, &st)) {
        rc = -errno;
        DisplayLog(LVL_CRIT, TAG,
                   "ERROR: lstat() failed on target " DFID ": %s", PFID(new_id),
                   strerror(-rc));
        return rc;
    }

    if (!S_ISREG(st.st_mode)) {
        DisplayLog(LVL_MAJOR, TAG, "%s() is only supported for files",
                   __func__);
        return -ENOTSUP;
    }

    /* build attr structure to pass to entry2backend_path() */
    ATTR_MASK_INIT(&attrs_new);
    stat2rbh_attrs(&st, &attrs_new, true);
    strcpy(ATTR(&attrs_new, fullpath), fs_path);
    ATTR_MASK_SET(&attrs_new, fullpath);

    /* Build new path in backend. */
    /* Ensure the target name is not compressed if the source was not. */
    entry2backend_path(smi, new_id, &attrs_new, FOR_NEW_COPY, new_bk_path,
                       compressed);
    /* set compression name if the previous entry was compressed */
    if (compressed && !IS_ZIP_NAME(new_bk_path))
        strcat(new_bk_path, "z");

    /* -- move entry from old bk path to the new location -- */

    /* recursively create the parent directory */
    /* extract dir path */
    strcpy(tmp, new_bk_path);
    destdir = dirname(tmp);
    if (destdir == NULL) {
        DisplayLog(LVL_CRIT, TAG, "Error extracting directory path of '%s'",
                   new_bk_path);
        return -EINVAL;
    }

    rc = mkdir_recurse_clone_attrs(destdir, 0750, TO_BACKEND);
    if (rc)
        return rc;

    do {
        /* rename the entry in backend */
        DisplayLog(LVL_DEBUG, TAG, "Moving entry in the backend: '%s'->'%s'",
                   old_bk_path, new_bk_path);
        if (rename(old_bk_path, new_bk_path)) {
            rc = -errno;

            /* only retry once if error is EXDEV */
            if (!retry && rc == -EXDEV) {
                const char *fname;

                DisplayLog(LVL_MAJOR, TAG,
                           "Could not move entry in the backend "
                           "because target path is in different device (error EXDEV): '%s'->'%s'",
                           old_bk_path, new_bk_path);

                /* try to move file from one backend fileset to another:
                 * in this case, just change filename within the same directory
                 */
                /* 1-extract current dirname in backend */
                strcpy(tmp, old_bk_path);
                destdir = dirname(tmp);
                /* 2-extract new filename */
                fname = rh_basename(fs_path);
                /* 3-build the new backend path */
#ifdef _HAVE_FID
                sprintf(new_bk_path, "%s/%s__" DFID_NOBRACE, destdir, fname,
                        PFID(new_id));
#else
                sprintf(new_bk_path, "%s/%s__%#LX:%#LX", destdir, fname,
                        (unsigned long long)new_id->device,
                        (unsigned long long)new_id->inode);
#endif
                retry = true;

                DisplayLog(LVL_MAJOR, TAG, "Trying to rename to '%s' instead",
                           new_bk_path);
                continue;
            } else {
                DisplayLog(LVL_MAJOR, TAG,
                           "Could not move entry in the backend ('%s'->'%s'): %s",
                           old_bk_path, new_bk_path, strerror(-rc));
                /* keep the old path */
                strcpy(new_bk_path, old_bk_path);
                return rc;
            }
        }
        /* rename succeeded */
        retry = false;
    } while (retry);

#ifdef HAVE_SHOOK
    /* save new backendpath to filesystem */
    /* XXX for now, don't manage several hsm_index */
    rc = shook_set_hsm_info(fidpath, new_bk_path, 0);
    if (rc)
        DisplayLog(LVL_MAJOR, TAG,
                   "Could not set backend path for " DFID ": error %d",
                   PFID(new_id), rc);
#endif

    return rc;
}

/** recover a directory */
static recov_status_t recov_dir(const char *backend_path, const char *fspath,
                                const attr_set_t *attrs, bool *set_mode)
{
    mode_t mode_create = 0750 /* default */ ;
    int rc;

    /* overwrite default if a mode is specified */
    if (ATTR_MASK_TEST(attrs, mode))
        mode_create = ATTR(attrs, mode);

    rc = mkdir(fspath, mode_create) ? errno : 0;

    if (rc != 0 && rc != EEXIST) {
        DisplayLog(LVL_CRIT, TAG, "ERROR: cannot create directory '%s': %s",
                   fspath, strerror(rc));
        return RS_ERROR;
    } else if (rc == EEXIST)
        /* must set the mode */
        *set_mode = true;

    return RS_NON_FILE;
}

/** recover a symlink */
static recov_status_t recov_symlink(const char *backend_path,
                                    const char *fspath,
                                    const attr_set_t *attrs, bool *set_mode)
{
    int rc;
    char link[RBH_PATH_MAX] = "";

    /* restore from DB */
    if (ATTR_MASK_TEST(attrs, link))
        rh_strncpy(link, ATTR(attrs, link), sizeof(link));
    else {  /* restore from FS */

        /* read link contents from backend */
        rc = readlink(backend_path, link, sizeof(link));
        if (rc < 0) {
            rc = errno;
            DisplayLog(LVL_MAJOR, TAG,
                       "Error reading symlink contents (%s): %s", backend_path,
                       strerror(rc));
            if (rc == ENOENT)
                return RS_NOBACKUP;
            else
                return RS_ERROR;
        }
        /* safety */
        if (rc < RBH_PATH_MAX)
            link[rc] = '\0';
        else
            link[RBH_PATH_MAX - 1] = '\0';
    }

    if (symlink(link, fspath) != 0) {
        rc = errno;
        DisplayLog(LVL_MAJOR, TAG,
                   "Error creating symlink %s->\"%s\" in filesystem: %s",
                   fspath, link, strerror(rc));
        return RS_ERROR;
    }

    return RS_NON_FILE;
}

/** recover a regular file */
static recov_status_t recov_file(sm_instance_t *smi, const entry_id_t *p_id,
                                 const char *backend_path, const char *fspath,
                                 attr_set_t *attrs, bool *set_mode,
                                 bool *compressed, bool *stat_done,
                                 struct stat *bk_stat, bool *no_copy)
{
    struct utimbuf utb;
    struct stat st_dest;
    mode_t mode_create;
    int rc;

    /* test if this copy exists */
    if (!*stat_done) {
        if (bk_lstat(backend_path, bk_stat, 1, compressed) != 0) {
            rc = errno;
            if (rc != ENOENT) {
                DisplayLog(LVL_MAJOR, TAG, "Cannot stat '%s' in backend: %s",
                           backend_path, strerror(rc));
                return RS_ERROR;
            }
        } else
            *stat_done = true;
    }

    if (!*stat_done) {
        /* if no stat done and file != 0 => no backup */
        if (!ATTR_MASK_TEST(attrs, size) || ATTR(attrs, size) != 0) {
            DisplayLog(LVL_MAJOR, TAG, "%s has no backup copy (%s not found)",
                       fspath, backend_path);
            return RS_NOBACKUP;
        } else
            *no_copy = true;
    }

    if (!*no_copy) {    /* only if there is a copy in the backend */
        attr_set_t attr_bk;

        if (!S_ISREG(bk_stat->st_mode)) {
            DisplayLog(LVL_CRIT, TAG,
                       "ERROR: recovering file from non-file object %s",
                       backend_path);
            return RS_ERROR;
        }

        ATTR_MASK_INIT(&attr_bk);
        /* merge missing posix attrs to p_attrs_old */
        stat2rbh_attrs(bk_stat, &attr_bk, true);
        /* leave attrs unchanged if they are already set in p_attrs_old */
        ListMgr_MergeAttrSets(attrs, &attr_bk, false);
    }

    /* test if the target does not already exist */
    rc = lstat(fspath, &st_dest) ? errno : 0;
    if (rc == 0) {
        DisplayLog(LVL_MAJOR, TAG, "Error: cannot recover '%s': already exists",
                   fspath);
        return RS_ERROR;
    } else if (rc != ENOENT) {
        DisplayLog(LVL_MAJOR, TAG, "Unexpected error performing lstat(%s): %s",
                   fspath, strerror(rc));
        return RS_ERROR;
    }

    /* Check that this is not a cross-device import or recovery
     * (entry could not be moved in that case) */
    if (!*no_copy && config.check_mounted && (backend_dev != bk_stat->st_dev)) {
        DisplayLog(LVL_MAJOR, TAG,
                   "Source file %s is not in the same device as target %s",
                   backend_path, config.root);
        return RS_ERROR;
    }

    if (ATTR_MASK_TEST(attrs, mode))
        mode_create = ATTR(attrs, mode);
    else if (!*no_copy)
        mode_create = bk_stat->st_mode;
    else
        mode_create = 0640; /* default */

#ifdef _LUSTRE
    /* restripe the file in Lustre */
    if (ATTR_MASK_TEST(attrs, stripe_info)) {
        CreateStriped(fspath, &ATTR(attrs, stripe_info), false);
        *set_mode = true;
    } else {
#endif
        int fd = creat(fspath, mode_create & 07777);

        if (fd < 0) {
            rc = errno;
            DisplayLog(LVL_CRIT, TAG, "ERROR: couldn't create '%s': %s",
                       fspath, strerror(rc));
            return RS_ERROR;
        }
        close(fd);
#ifdef _LUSTRE
    }
#endif

    if (!*no_copy) {
        struct attr_save sav = ATTR_SET_INIT;
        action_params_t recov_params = { 0 };
        post_action_e dummy_after;

        /* In any case, set 'copyback' param. */
        if (rbh_param_set(&recov_params, "copyback", "1", false)) {
            DisplayLog(LVL_CRIT, TAG,
                       "ERROR: failed to set action param 'copyback'");
            return RS_ERROR;
        }
        /* If compression is enabled, append 'compress' param.  */
        if (*compressed) {
            if (rbh_param_set(&recov_params, "compress", "1", false) != 0) {
                DisplayLog(LVL_CRIT, TAG,
                           "ERROR: failed to set action param 'compress'");
                return RS_ERROR;
            }
        }

        /* fspath may be a pointer to attrs, so make sure we set the right
         * path in TARGET_PATH before 'path_replace' modifies it. */
        if (rbh_param_set(&recov_params, TARGET_PATH_PARAM, fspath, false)) {
            DisplayLog(LVL_CRIT, TAG, "ERROR: failed to set action param '%s'",
                       TARGET_PATH_PARAM);
            return RS_ERROR;
        }

        /* actions expect to get a source path in 'fullpath' and targetpath
         * in 'targetpath' parameter.
         * So, build a fake attribute and new parameter set with these values */
        path_replace(&sav, attrs, backend_path);

        /* perform the data copy (if needed) */
        rc = action_helper(&config.recovery_action, "recover", p_id, attrs,
                           &recov_params, smi, NULL, &dummy_after, NULL, NULL);

        /* restore real entry attributes */
        path_restore(&sav, attrs);

        if (rc)
            return RS_ERROR;

        utb.actime = bk_stat->st_atime;
        utb.modtime = bk_stat->st_mtime;

        /* set the same mtime as in the backend */
        DisplayLog(LVL_FULL, TAG,
                   "Restoring times from backend for '%s': atime=%lu, mtime=%lu",
                   fspath, utb.actime, utb.modtime);
        if (utime(fspath, &utb))
            DisplayLog(LVL_MAJOR, TAG,
                       "Warning: couldn't restore times for '%s': %s", fspath,
                       strerror(errno));
    } else if (ATTR_MASK_TEST(attrs, last_mod)) {
        utb.modtime = ATTR(attrs, last_mod);

        if (ATTR_MASK_TEST(attrs, last_access))
            utb.actime = ATTR(attrs, last_access);
        else
            utb.actime = utb.modtime;

        /* set the same mtime as in the DB */
        DisplayLog(LVL_FULL, TAG,
                   "Restoring times from DB for '%s': atime=%lu, mtime=%lu",
                   fspath, utb.actime, utb.modtime);
        if (utime(fspath, &utb))
            DisplayLog(LVL_MAJOR, TAG,
                       "Warning: couldn't restore times for '%s': %s", fspath,
                       strerror(errno));
    }

    return *no_copy ? RS_FILE_EMPTY : RS_FILE_OK;
}

#define VALID_FULLPATH(_attrs) (ATTR_MASK_TEST(_attrs, fullpath) && \
                                !EMPTY_STRING(ATTR(_attrs, fullpath)))

/** recover a file from the backend (after rm, or a disaster...)
 * \return recovery status
 */
static recov_status_t backup_recover(struct sm_instance *smi,
                                     const entry_id_t *p_old_id,
                                     const attr_set_t *p_attrs_old_in,
                                     entry_id_t *p_new_id,
                                     attr_set_t *p_attrs_new,
                                     bool already_recovered)
{
    char bkpath[RBH_PATH_MAX] = "";
    char link[RBH_PATH_MAX] = "";
    const char *backend_path = NULL;
    const char *fspath;
    int rc;
    struct stat st_bk;
    struct stat st_dest;
    recov_status_t success_status;
    entry_id_t parent_id;
    mode_t mode_create = 0;
    bool set_mode = false;
    bool stat_done = false;
    bool no_copy = false;
    bool compressed = false;
    char *name;
    attr_set_t attrs_old = ATTR_SET_INIT;

    /* build writable copy of input attrs. */
    ListMgr_MergeAttrSets(&attrs_old, p_attrs_old_in, true);

    /* if the entry is already specified, associate with the new fspath,
     * if specified */
    if (already_recovered && VALID_FULLPATH(p_attrs_new))
        fspath = ATTR(p_attrs_new, fullpath);
    else if (VALID_FULLPATH(&attrs_old))
        fspath = ATTR(&attrs_old, fullpath);
    else {
        DisplayLog(LVL_MAJOR, TAG,
                   "Missing mandatory attribute 'fullpath' for recover entry "
                   DFID, PFID(p_old_id));
        success_status = RS_ERROR;
        goto out;
    }

#if 0   /* Code from RBHv2: keep it? */
    /* if FS path is not absolute, get the relative backend path and append to
     * FS root */
    if (fspath[0] != '/') {
        char tmp[RBH_PATH_MAX];
        if (ATTR_MASK_TEST(p_attrs_old, backendpath)) {
            relative_path(ATTR(p_attrs_old, backendpath), config.root, tmp);
            sprintf(buff, "%s/%s/%s", global_config.fs_path, dirname(tmp),
                    strrchr(fspath, '/') + 1);
            fspath = buff;
        } else {    /* use the given relative path */

            sprintf(buff, "%s/%s", global_config.fs_path,
                    ATTR(p_attrs_old, fullpath));
            fspath = buff;
        }
    }
#endif

    if (ATTR_MASK_INFO_TEST(&attrs_old, smi, ATTR_BK_PATH))
        backend_path = BKPATH(&attrs_old, smi);
    else
        /* if there is no backend path, try to guess */
    {
        int lvl_log;

        if (ATTR_MASK_TEST(&attrs_old, type)
            && !strcasecmp(ATTR(&attrs_old, type), STR_TYPE_FILE))
            lvl_log = LVL_EVENT;
        else
            lvl_log = LVL_VERB;

        entry2backend_path(smi, p_old_id, &attrs_old, FOR_LOOKUP, bkpath,
                           config.compress);
        DisplayLog(lvl_log, TAG,
                   "No backend path is set for '%s', guess it could be '%s'",
                   fspath, bkpath);
        backend_path = bkpath;
    }

    /* Another status manager recovered it. Just rebind in the backend. */
    if (already_recovered) {
        char bknew[RBH_PATH_MAX];

        success_status = backup_rebind(smi, fspath, backend_path, bknew,
                                       p_new_id);
        if (success_status == 0)
            set_backend_path(smi, p_attrs_new, bknew);

        /* @FIXME make backup_rebind return a recov_status */
        goto out;
    }

    if (!ATTR_MASK_TEST(&attrs_old, type)) {
        const char *type;

        if (bk_lstat(backend_path, &st_bk, 1, &compressed) != 0) {
            rc = errno;
            DisplayLog(LVL_MAJOR, TAG, "Cannot restore entry " DFID
                       ": '%s' not found in backend.", PFID(p_old_id),
                       backend_path);
            if (rc == ENOENT)
                success_status = RS_NOBACKUP;
            else
                success_status = RS_ERROR;
            goto out;
        }
        stat_done = true;

        /* set type in attrs_old */
        type = mode2type(st_bk.st_mode);
        if (type != NULL) {
            ATTR_MASK_SET(&attrs_old, type);
            strcpy(ATTR(&attrs_old, type), type);
        } else {
            DisplayLog(LVL_MAJOR, TAG, "%s has unsupported type", backend_path);
            success_status = RS_NOBACKUP;
            goto out;
        }
    }

    /* create parent in filesystem in any case */
    if (create_parent_in_fs(fspath, &parent_id)) {
        success_status = RS_ERROR;
        goto out;
    }

    if (!strcasecmp(ATTR(&attrs_old, type), STR_TYPE_DIR)) {
        success_status = recov_dir(backend_path, fspath, &attrs_old, &set_mode);
        if (success_status != RS_NON_FILE)
            goto out;
    } else if (!strcasecmp(ATTR(&attrs_old, type), STR_TYPE_LINK)) {
        success_status =
            recov_symlink(backend_path, fspath, &attrs_old, &set_mode);
        if (success_status != RS_NON_FILE)
            goto out;
    } else if (!strcasecmp(ATTR(&attrs_old, type), STR_TYPE_FILE)) {
        success_status =
            recov_file(smi, p_old_id, backend_path, fspath, &attrs_old,
                       &set_mode, &compressed, &stat_done, &st_bk, &no_copy);
        if ((success_status != RS_FILE_EMPTY) && (success_status != RS_FILE_OK))
            goto out;
    } else {
        /* type not supported */
        DisplayLog(LVL_CRIT, TAG,
                   "Error: cannot restore entries with type '%s' (%s)",
                   ATTR(&attrs_old, type), fspath);
        success_status = RS_NOBACKUP;
        goto out;
    }

    /* set owner, group */
    if (ATTR_MASK_TEST(&attrs_old, uid) || ATTR_MASK_TEST(&attrs_old, gid)) {
        uid_t uid = -1;
        gid_t gid = -1;
        char buff[4096];

        if (ATTR_MASK_TEST(&attrs_old, uid)) {
            if (global_config.uid_gid_as_numbers) {
                uid = ATTR(&attrs_old, uid).num;
            } else {
                struct passwd pw;
                struct passwd *p_pw;

                if ((getpwnam_r
                     (ATTR(&attrs_old, uid).txt, &pw, buff, 4096, &p_pw) != 0)
                    || (p_pw == NULL)) {
                    DisplayLog(LVL_MAJOR, TAG,
                               "Warning: couldn't resolve uid for user '%s'",
                               ATTR(&attrs_old, uid).txt);
                    uid = -1;
                } else
                    uid = p_pw->pw_uid;
            }
        }

        if (ATTR_MASK_TEST(&attrs_old, gid)) {
            if (global_config.uid_gid_as_numbers) {
                gid = ATTR(&attrs_old, gid).num;
            } else {
                struct group gr;
                struct group *p_gr;

                if ((getgrnam_r
                     (ATTR(&attrs_old, gid).txt, &gr, buff, 4096, &p_gr) != 0)
                    || (p_gr == NULL)) {
                    DisplayLog(LVL_MAJOR, TAG,
                               "Warning: couldn't resolve gid for group '%s'",
                               ATTR(&attrs_old, gid).txt);
                    gid = -1;
                } else
                    gid = p_gr->gr_gid;
            }
        }

        DisplayLog(LVL_FULL, TAG,
                   "Restoring owner/group for '%s': uid=%u, gid=%u", fspath,
                   uid, gid);

        if (lchown(fspath, uid, gid)) {
            rc = errno;
            DisplayLog(LVL_MAJOR, TAG,
                       "Warning: cannot set owner/group for '%s': %s", fspath,
                       strerror(rc));
        } else {
            /* According to chown(2) manual: chown may clear sticky bits even
             * if root does it,
             * so, we must set the mode again if it contains special bits */
            if (!set_mode && (mode_create & 07000))
                set_mode = true;
        }
    }

    if (set_mode) {
        /* set the same mode as in the backend */
        DisplayLog(LVL_FULL, TAG, "Restoring mode for '%s': mode=%#o",
                   fspath, mode_create & 07777);
        if (chmod(fspath, mode_create & 07777))
            DisplayLog(LVL_MAJOR, TAG,
                       "Warning: couldn't restore mode for '%s': %s", fspath,
                       strerror(errno));
    }

    if (lstat(fspath, &st_dest)) {
        rc = errno;
        DisplayLog(LVL_CRIT, TAG,
                   "ERROR: lstat() failed on restored entry '%s': %s", fspath,
                   strerror(rc));
        success_status = RS_ERROR;
        goto out;
    }

    /* Compare restored size and mtime with the one saved in the DB
     * for warning purpose (not for directories) */
    if (!S_ISDIR(st_dest.st_mode)) {
        if (ATTR_MASK_TEST(&attrs_old, size)
            && (st_dest.st_size != ATTR(&attrs_old, size))) {
            if (!compressed) {
                DisplayLog(LVL_MAJOR, TAG, "%s: the restored size (%zu) is "
                           "different from the last known size in filesystem (%"
                           PRIu64 "): "
                           "it may have been modified in filesystem after the last backup.",
                           fspath, st_dest.st_size, ATTR(&attrs_old, size));
                success_status = RS_FILE_DELTA;
            } else
                success_status = RS_FILE_OK;
        }
    }
    /* only for files */
    else if (S_ISREG(st_dest.st_mode)) {
        if (ATTR_MASK_TEST(&attrs_old, last_mod)
            && (st_dest.st_mtime != ATTR(&attrs_old, last_mod))) {
            DisplayLog(LVL_MAJOR, TAG,
                       "%s: the restored mtime (%lu) is "
                       "different from the last time in filesystem (%u): "
                       "it may have been modified in filesystem after the last backup.",
                       fspath, st_dest.st_mtime, ATTR(&attrs_old, last_mod));
            success_status = RS_FILE_DELTA;
        }
    }

    /* set the new attributes */
    ATTR_MASK_INIT(p_attrs_new);
    stat2rbh_attrs(&st_dest, p_attrs_new, true);
    strcpy(ATTR(p_attrs_new, fullpath), fspath);
    ATTR_MASK_SET(p_attrs_new, fullpath);

    rc = path2id(fspath, p_new_id, &st_dest);
    if (rc) {
        success_status = RS_ERROR;
        goto out;
    }

    /* set parent id */
    ATTR_MASK_SET(p_attrs_new, parent_id);
    ATTR(p_attrs_new, parent_id) = parent_id;

    /* set name */
    name = strrchr(ATTR(p_attrs_new, fullpath), '/');
    if ((name != NULL) && (*(name + 1) != '\0')) {
        rh_strncpy(ATTR(p_attrs_new, name), name + 1,
                   sizeof(ATTR(p_attrs_new, name)));
        ATTR_MASK_SET(p_attrs_new, name);
    }
#ifdef _LUSTRE
    if (!ATTR_MASK_TEST(p_attrs_new, type)
        || !strcmp(ATTR(p_attrs_new, type), STR_TYPE_FILE)) {
        /* get the new stripe info */
        if (File_GetStripeByPath(fspath,
                                 &ATTR(p_attrs_new, stripe_info),
                                 &ATTR(p_attrs_new, stripe_items)) == 0) {
            ATTR_MASK_SET(p_attrs_new, stripe_info);
            ATTR_MASK_SET(p_attrs_new, stripe_items);
        }
    }
#endif

    if (S_ISLNK(st_dest.st_mode)) {
        strcpy(ATTR(p_attrs_new, link), link);
        ATTR_MASK_SET(p_attrs_new, link);
    }

    if (!no_copy && (S_ISREG(st_dest.st_mode) || S_ISLNK(st_dest.st_mode))) {
        char tmp[RBH_PATH_MAX];
        char *destdir;

#ifdef HAVE_SHOOK
        /* only files are recovered as released, others are synchro */
        if (S_ISREG(st_dest.st_mode))
            rc = set_backup_status(smi, p_attrs_new, STATUS_RELEASED);
        else
#endif
            rc = set_backup_status(smi, p_attrs_new, STATUS_SYNCHRO);

        /* set the new entry path in backend, according to the new fid,
         * and actual compression */
        entry2backend_path(smi, p_new_id, p_attrs_new,
                           FOR_NEW_COPY, tmp, compressed);
        set_backend_path(smi, p_attrs_new, tmp);

        /* recursively create the parent directory */
        /* Extract dir path. We can modify tmp now,
         * as it has been copied by set_backend_path(). */
        destdir = dirname(tmp);
        if (destdir == NULL) {
            DisplayLog(LVL_CRIT, TAG, "Error extracting directory path of '%s'",
                       BKPATH(p_attrs_new, smi));
            success_status = RS_ERROR;
            goto out;
        }

        rc = mkdir_recurse_clone_attrs(destdir, 0750, TO_BACKEND);
        if (rc) {
            success_status = RS_ERROR;
            goto out;
        }

        /* rename the entry in backend */
        if (strcmp(BKPATH(p_attrs_new, smi), backend_path) != 0) {
            DisplayLog(LVL_DEBUG, TAG,
                       "Moving the entry in backend: '%s'->'%s'", backend_path,
                       BKPATH(p_attrs_new, smi));
            if (rename(backend_path, BKPATH(p_attrs_new, smi))) {
                rc = errno;
                DisplayLog(LVL_MAJOR, TAG,
                           "Could not move entry in backend ('%s'->'%s'): %s",
                           backend_path, BKPATH(p_attrs_new, smi),
                           strerror(rc));
                /* keep the old path */
                set_backend_path(smi, p_attrs_new, backend_path);
            }
        }
#ifdef HAVE_SHOOK
        /* save new backendpath to filesystem */
        /* XXX for now, don't manage several hsm_index */
        rc = shook_set_hsm_info(fspath, BKPATH(p_attrs_new, smi), 0);
        if (rc)
            DisplayLog(LVL_MAJOR, TAG,
                       "Could not set backend path for %s: error %d", fspath,
                       rc);
#endif
    }

 out:
    ListMgr_FreeAttrs(&attrs_old);
    return success_status;
}

#ifdef HAVE_SHOOK
/** recovery function
    XXX used to be called like this:
    rc = shook_recov_file(fspath, (*compressed && ATTR_MASK_TEST(attrs, size))?
                          ATTR(attrs, size) : bk_stat->st_size);
    => must be turned to a standard function prototype.
*/
static int rbh_shook_recov_file(const char *fspath, size_t size)
{
    int rc;

    /* set the file in "released" state */
    rc = shook_set_status(fspath, SS_RELEASED);
    if (rc) {
        DisplayLog(LVL_CRIT, TAG, "ERROR setting released state for '%s': %s",
                   fspath, strerror(-rc));
        return rc;
    }

    rc = truncate(fspath, size) ? errno : 0;
    if (rc) {
        DisplayLog(LVL_CRIT, TAG,
                   "ERROR could not set original size %" PRI_SZ " for '%s': %s",
                   bk_stat->st_size, fspath, strerror(rc));
        return rc;
    }

    return 0;
}

/** action function */
int rbh_shook_release(const entry_id_t *p_id, attr_set_t *p_attrs)
{
    int rc;
    obj_type_t entry_type;

    /* if status is not determined, retrieve it */
    if (!ATTR_MASK_TEST(p_attrs, status)) {
        DisplayLog(LVL_DEBUG, TAG, "Status not provided to backup_release()");
        rc = backup_get_status(p_id, p_attrs, p_attrs);
        if (rc)
            return rc;
    }

    /* is it the good type? */
    if (!ATTR_MASK_TEST(p_attrs, type)) {
        DisplayLog(LVL_MAJOR, TAG, "Missing mandatory attribute 'type' in %s()",
                   __FUNCTION__);
        return -EINVAL;
    }

    entry_type = ListMgr2PolicyType(ATTR(p_attrs, type));
    if (entry_type != TYPE_FILE) {
        DisplayLog(LVL_MAJOR, TAG, "Unsupported type for release operation: %s",
                   ATTR(p_attrs, type));
        return -ENOTSUP;
    }

    return shook_release(get_fsname(), p_id);
}
#endif

/** Status manager for backup or shook (2 builds with different flags) */
static status_manager_t backup_sm = {
#ifdef HAVE_SHOOK
    .name = "shook",
#else
    .name = "backup",
#endif
    .flags = SM_SHARED | SM_DELETED | SM_MULTI_ACTION,
    .status_enum = backup_status_list,  /* unknown is empty(unset) status */
    .status_count = STATUS_COUNT - 1,
    .nb_info = G_N_ELEMENTS(backup_info),
    .info_types = backup_info,

    /* Previous backup path is also needed.
     * It is only in DB (so it is a cached information). */
    .status_needs_attrs_cached = {.std = ATTR_MASK_type | ATTR_MASK_fullpath,
                                  /* XXX used last_archive in RBH2.5: */
                                  .sm_info = GENERIC_INFO_BIT(ATTR_BK_PATH)},

    /* needs fresh mtime/size information from lustre
     * to determine if the entry changed */
    .status_needs_attrs_fresh = {.std = ATTR_MASK_last_mod | ATTR_MASK_size},

    .get_status_func = backup_status,
    .changelog_cb = backup_cl_cb,

    .executor = backup_common_executor,

    .check_action_name = backup_check_action_name,
    /* no action callback as it has an executor */

    /* fields for checking if entries must be inserted to SOFTRM */
    .softrm_filter_mask = {.std = ATTR_MASK_type, .status = SMI_MASK(0)},
    .softrm_filter_func = backup_softrm_filter,

    /** needed attributes for undelete in addition to POSIX and fullpath:
     * - backup_status: to know the original status of the 'undeleted' entry.
     * - backend_path: to rebind undeleted entry in backend.
     */
    .softrm_table_mask = {.std = ATTR_MASK_type | ATTR_MASK_fullpath,
                          .status = SMI_MASK(0),
                          .sm_info = GENERIC_INFO_BIT(ATTR_BK_PATH)},
    .undelete_func = backup_recover,

    .cfg_funcs = &backup_cfg_hdlr,
    .init_func = backup_init
};

const char *mod_get_name(void)
{
    return backup_sm.name;
}

status_manager_t *mod_get_status_manager(void)
{
    return &backup_sm;
}

action_func_t mod_get_action_by_name(const char *action_name)
{
    /* none implemented */
    return NULL;
}
