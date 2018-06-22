/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2004-2015 CEA/DAM
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

#include "rbh_misc.h"
#include "rbh_cfg.h"
#include "global_config.h"
#include "rbh_logs.h"
#include "xplatform_print.h"
#include "uidgidcache.h"
#include "status_manager.h"

#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <stdio.h>
#include <pthread.h>
#include <libgen.h>
#include <stdarg.h>
#include <fnmatch.h>
#include <sys/types.h>
#include <utime.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ctype.h>
#include <string.h>

#ifndef HAVE_GETMNTENT_R
#include "mntent_compat.h"
#else
#include <mntent.h> /* for handling mntent */
#endif

/* Exa-scale definitions ;-) */
#define KILO_BYTE  (1024LL)
#define MEGA_BYTE  (1024LL * KILO_BYTE)
#define GIGA_BYTE  (1024LL * MEGA_BYTE)
#define TERA_BYTE  (1024LL * GIGA_BYTE)
#define PETA_BYTE  (1024LL * TERA_BYTE)
#define EXA_BYTE   (1024LL * PETA_BYTE)

/* Durations  */
#define MINUTE   60
#define HOUR  (60*MINUTE)
#define DAY   (24*HOUR)
#define WEEK  (7*DAY)
#define YEAR  (365*DAY)

void Exit(int error_code)
{
    DisplayLog(LVL_MAJOR, "EXIT", "Exiting program with code %d", error_code);

#if 0
    /* Remove pid_file, if any */
    if (pid_file != NULL) {
        if (unlink(pid_file) != 0) {
            DisplayLog(LVL_CRIT, "EXIT",
                       "Could not remove pid file %s: %s", pid_file,
                       strerror(errno));
        }
    }
#endif

    FlushLogs();
    exit(error_code);
}

/* global info about the filesystem to be managed */
static char *mount_point;
static char fsname[RBH_PATH_MAX] = "";
static dev_t dev_id = 0;
static uint64_t fs_key = 0;
static entry_id_t root_id;

/* to optimize string concatenation */
static unsigned int mount_len = 0;

#ifdef _HAVE_FID
#define DOTLUSTRE   ".lustre"
#define FIDDIR      "/.lustre/fid/"
static char *fid_dir;
static char *dot_lustre_dir;
static entry_id_t dot_lustre_fid;
static entry_id_t fid_fid;
#endif

/* used at initialization time, to avoid several modules
 * that start in parallel to check it several times.
 */
static pthread_mutex_t mount_point_lock = PTHREAD_MUTEX_INITIALIZER;

#define LAST_32PRIME    0xFFFFFFFB
#define LAST_64PRIME    0xFFFFFFFFFFFFFFC5
static uint64_t hash_name(const char *str)
{
    unsigned int i;
    uint64_t val = 1;

    for (i = 0; i < strlen(str); i++)
        val = (val << 5) - val + (unsigned int)(str[i]);

    return val % LAST_32PRIME;
}

static uint64_t fsidto64(fsid_t fsid)
{
    uint64_t out;
    if (sizeof(fsid_t) <= sizeof(uint64_t)) {
        memset(&out, 0, sizeof(out));
        memcpy((&out) + (sizeof(out) - sizeof(fsid_t)), &fsid, sizeof(fsid));
        DisplayLog(LVL_DEBUG, __func__, "sizeof(fsid)=%lu <= 64bits, "
                   "fsid as 64=%" PRIX64, sizeof(fsid_t), out);
        return out;
    } else {
        unsigned int i;
        out = 1;
        char *str = (char *)(&fsid);

        for (i = 0; i < sizeof(fsid_t); i++)
            out = (out << 5) - out + (unsigned int)(str[i]);

        out = out % LAST_64PRIME;
        DisplayLog(LVL_DEBUG, __func__, "sizeof(fsid)=%lu > 64bits, "
                   "hash64(fsid)=%" PRIX64, sizeof(fsid_t), out);
        return out;
    }
}

/* this set of functions is for retrieving/checking mount point
 * and fs name (once for all threads):
 */
static int _set_mount_point(char *mntpnt)
{
    char path[RBH_PATH_MAX + 100];

    /* don't change mount_path while already running */
    if (mount_len != 0)
        return 0;

    strcpy(path, mntpnt);

    /* remove final slash, if any */
    if ((mount_len > 1) && (path[mount_len - 1] == '/')) {
        path[mount_len - 1] = '\0';
    }

    mount_point = strdup(path);
    mount_len = strlen(mount_point);

#ifdef _HAVE_FID
    int rc;

    /* build the .lustre directory */
    if (asprintf(&dot_lustre_dir, "%s/%s", path, DOTLUSTRE) == -1)
        RBH_BUG("Not enough memory to initialize");

    /* build the .fid directory */
    if (asprintf(&fid_dir, "%s%s", path, FIDDIR) == -1)
        RBH_BUG("Not enough memory to initialize");

    /* also get their fid */
    rc = path2id(dot_lustre_dir, &dot_lustre_fid, NULL);
    if (rc) {
        DisplayLog(LVL_MAJOR, __func__,
                   "Error: failed to get FID for special directory <%s>: %s.",
                   dot_lustre_dir, strerror(abs(rc)));
        return rc;
    }

    rc = path2id(fid_dir, &fid_fid, NULL);
    if (rc) {
        DisplayLog(LVL_MAJOR, __func__,
                   "Error: failed to get FID for special directory <%s>: %s.",
                   fid_dir, strerror(abs(rc)));
        return rc;
    }
#endif
    return 0;
}

static int set_fs_info(char *name, char *mountp, dev_t dev, fsid_t fsid)
{
    int rc = 0;

    P(mount_point_lock);
    rc = _set_mount_point(mountp);
    if (rc)
        goto out_unlock;

    strcpy(fsname, name);
    dev_id = dev;

    switch (global_config.fs_key) {
    case FSKEY_FSNAME:
        fs_key = hash_name(name);
        DisplayLog(LVL_DEBUG, "FSInfo", "fs_key: hash(fsname=%s)=%" PRIX64,
                   name, fs_key);
        break;

    case FSKEY_FSID:
        fs_key = fsidto64(fsid);
        DisplayLog(LVL_DEBUG, "FSInfo", "fs_key: fsid as 64=%" PRIX64, fs_key);
        break;

    case FSKEY_DEVID:
        fs_key = dev_id;
        DisplayLog(LVL_DEBUG, "FSInfo", "fs_key: devid=%" PRIX64, fs_key);
        break;

    default:
        DisplayLog(LVL_MAJOR, "FSInfo", "Invalid fs_key type %#x",
                   global_config.fs_key);
        fs_key = 0;
    }

    /* now, path2id can be called */
    rc = path2id(global_config.fs_path, &root_id, NULL);
    if (rc)
        DisplayLog(LVL_CRIT, "FSInfo",
                   "Failed to get id for root directory %s: %s", mountp,
                   strerror(-rc));

 out_unlock:
    V(mount_point_lock);
    return rc;
}

/** retrieve the mount point from any module
 * without final slash.
 */
const char *get_mount_point(unsigned int *plen)
{
    if (plen)
        (*plen) = mount_len;
    return mount_point;
}

#if _HAVE_FID
/** Retrieve the .fid directory */
const char *get_fid_dir(void)
{
    return fid_dir;
}

/** Retrieve the .lustre directory */
const char *get_dot_lustre_dir(void)
{
    return dot_lustre_dir;
}

/** retrieve the fid of "<root>/.lustre" directory */
const entry_id_t *get_dot_lustre_fid(void)
{
    return &dot_lustre_fid;
}

/** retrieve the fid of "<root>/.lustre/fid" directory */
const entry_id_t *get_fid_fid(void)
{
    return &fid_fid;
}
#endif

/* retrieve fsname from any module */
const char *get_fsname(void)
{
    return fsname;
}

/* return Filesystem device id  */
dev_t get_fsdev(void)
{
    return dev_id;
}

uint64_t get_fskey(void)
{
    return fs_key;
}

const entry_id_t *get_root_id(void)
{
    return &root_id;
}

/**
 * send a mail
 */
int SendMail(const char *recipient, const char *subject, const char *message)
{
    char buffer[MAIL_TITLE_MAX + MAIL_ADDRESS_MAX + 32];
    FILE *fichier;

    snprintf(buffer, MAIL_TITLE_MAX, "mailx -s \"%s\" %s", subject, recipient);

    if ((fichier = popen(buffer, "w")) == NULL) {
        DisplayLog(LVL_CRIT, "SENDMAIL",
                   "Error %d sending mail with the following command=%s", errno,
                   buffer);
        return -1;
    }
    fwrite(message, strlen(message), 1, fichier);
    pclose(fichier);
    return 0;
}

/**
 * Search for Robinhood config file.
 * search a config file with the given name (+extension)
 * If cfg_in is empty: search any config in config paths
 * /!\ not thread safe
 */
int SearchConfig(const char *cfg_in, char *cfg_out, bool *changed,
                 char *unmatched, size_t max_len)
{
    static const char *default_cfg_path = SYSCONFDIR "/robinhood.d";
    DIR *dir;
    struct dirent *ent;
    const char *cfg = cfg_in;

    *changed = true;    /* most of the cases */

    if (cfg == NULL || EMPTY_STRING(cfg)) {
        /* check if a default config file is specified */
        cfg = getenv(DEFAULT_CFG_VAR);
    }

    /* set unmatched, for better logging */
    if (unmatched) {
        if (cfg)
            rh_strncpy(unmatched, cfg, max_len);
        else
            snprintf(unmatched, max_len, "%s/*.conf", default_cfg_path);
    }

    if (cfg == NULL || EMPTY_STRING(cfg)) {
        int found = 0;

        /* look for files in default config path */
        dir = opendir(default_cfg_path);
        if (dir) {
            while ((ent = readdir(dir)) != NULL) {
                /* ignore .xxx files */
                if (ent->d_name[0] == '.')
                    continue;
                if (fnmatch("*.conf", ent->d_name, 0)
                    && fnmatch("*.cfg", ent->d_name, 0))
                    /* not a config file */
                    continue;

                sprintf(cfg_out, "%s/%s", default_cfg_path, ent->d_name);
                if (access(cfg_out, F_OK) == 0) {
                    /* that file matches. */
                    found++;
                    if (found >= 2)
                        /* No need to continue. */
                        break;
                }
            }

            closedir(dir);
        }

        if (found == 1) {
            /* Only one file found. cfg_out is already set. We're
             * good. */
            return 0;
        }
    } else if (access(cfg, F_OK) == 0) {
        /* the specified config file exists */
        if (cfg_out != cfg)
            rh_strncpy(cfg_out, cfg, max_len);
        *changed = false;
        return 0;
    } else if (strchr(cfg, '/')) {
        /* the argument is a path (not a single name
         * and this path was not found) */
        *changed = false;
        goto notfound;
    } else {    /* look for a file in the given paths */

        char cfg_cp[RBH_PATH_MAX] = "";
        int has_ext = (strchr(cfg, '.') != NULL);

        rh_strncpy(cfg_cp, cfg, MIN2(max_len, RBH_PATH_MAX));

        /* if the file already has an extension, try path/name */
        if (has_ext) {
            snprintf(cfg_out, max_len, "%s/%s", default_cfg_path, cfg_cp);
            if (access(cfg_out, F_OK) == 0)
                return 0;
        }

        /* try path/name.cfg, path/name.conf */
        snprintf(cfg_out, max_len, "%s/%s.conf", default_cfg_path, cfg_cp);
        if (access(cfg_out, F_OK) == 0)
            return 0;

        snprintf(cfg_out, max_len, "%s/%s.cfg", default_cfg_path, cfg_cp);
        if (access(cfg_out, F_OK) == 0)
            return 0;
    }

 notfound:
    /* no file found, cleaning cfg_out */
    cfg_out[0] = '\0';
    return -ENOENT;
}

char *uid2str(uid_t uid, char *username)
{
    const struct passwd *p = GetPwUid(uid);

    if (p != NULL)
        snprintf(username, RBH_LOGIN_MAX, "%s", p->pw_name);
    else
        snprintf(username, RBH_LOGIN_MAX, "%d", (int)uid);

    return username;
}

char *gid2str(gid_t gid, char *groupname)
{
    const struct group *g = GetGrGid(gid);

    if (g != NULL)
        snprintf(groupname, RBH_LOGIN_MAX, "%s", g->gr_name);
    else
        snprintf(groupname, RBH_LOGIN_MAX, "%d", (int)gid);

    return groupname;
}

const char *mode2type(mode_t mode)
{
    if (S_ISREG(mode))
        return STR_TYPE_FILE;
    else if (S_ISDIR(mode))
        return STR_TYPE_DIR;
    else if (S_ISLNK(mode))
        return STR_TYPE_LINK;
    else if (S_ISCHR(mode))
        return STR_TYPE_CHR;
    else if (S_ISBLK(mode))
        return STR_TYPE_BLK;
    else if (S_ISFIFO(mode))
        return STR_TYPE_FIFO;
    else if (S_ISSOCK(mode))
        return STR_TYPE_SOCK;
    else
        return NULL;
}

void stat2rbh_attrs(const struct stat *p_inode, attr_set_t *p_attr_set,
                    bool size_info)
{
    ATTR_MASK_SET(p_attr_set, uid);
    if (global_config.uid_gid_as_numbers)
        ATTR(p_attr_set, uid).num = p_inode->st_uid;
    else
        uid2str(p_inode->st_uid, ATTR(p_attr_set, uid).txt);

    ATTR_MASK_SET(p_attr_set, gid);
    if (global_config.uid_gid_as_numbers)
        ATTR(p_attr_set, gid).num = p_inode->st_gid;
    else
        gid2str(p_inode->st_gid, ATTR(p_attr_set, gid).txt);

    if (size_info) {
        ATTR_MASK_SET(p_attr_set, size);
        ATTR(p_attr_set, size) = p_inode->st_size;

        ATTR_MASK_SET(p_attr_set, blocks);
        ATTR(p_attr_set, blocks) = p_inode->st_blocks;

        /* times are also wrong when they come from the MDT device */
        ATTR_MASK_SET(p_attr_set, last_access);

        /* Vary the setting of last_access depending on value of
         * global_config.last_access_only_atime */
        if (global_config.last_access_only_atime)
            ATTR(p_attr_set, last_access) = p_inode->st_atime;
        else
            ATTR(p_attr_set, last_access) =
                MAX(p_inode->st_atime, p_inode->st_mtime);

        ATTR_MASK_SET(p_attr_set, last_mod);
        ATTR(p_attr_set, last_mod) = p_inode->st_mtime;

        ATTR_MASK_SET(p_attr_set, last_mdchange);
        ATTR(p_attr_set, last_mdchange) = p_inode->st_ctime;
    }

    if (ATTR_MASK_TEST(p_attr_set, creation_time)) {
        /* creation time is always <= ctime */
        if (p_inode->st_ctime < ATTR(p_attr_set, creation_time))
            ATTR(p_attr_set, creation_time) = p_inode->st_ctime;
    } else {
        ATTR_MASK_SET(p_attr_set, creation_time);
        ATTR(p_attr_set, creation_time) = p_inode->st_ctime;
    }

    const char *type = mode2type(p_inode->st_mode);
    if (type != NULL) {
        ATTR_MASK_SET(p_attr_set, type);
        strcpy(ATTR(p_attr_set, type), type);
    }

    ATTR_MASK_SET(p_attr_set, nlink);
    ATTR(p_attr_set, nlink) = p_inode->st_nlink;

    ATTR_MASK_SET(p_attr_set, mode);
    ATTR(p_attr_set, mode) = p_inode->st_mode & 07777;  /* mode + sticky bits */
}

void rbh_attrs2stat(const attr_set_t *p_attr_set, struct stat *p_inode)
{
    char buff[4096];

    if (ATTR_MASK_TEST(p_attr_set, mode))
        p_inode->st_mode = ATTR(p_attr_set, mode);
    /* default to 600 for files, 700 for other cases */
    else if (ATTR_MASK_TEST(p_attr_set, type) &&
             !strcmp(ATTR(p_attr_set, type), STR_TYPE_FILE))
        p_inode->st_mode = 0600;
    else
        p_inode->st_mode = 0700;

    if (ATTR_MASK_TEST(p_attr_set, nlink))
        p_inode->st_nlink = ATTR(p_attr_set, nlink);
    else
        p_inode->st_nlink = 1;

    /* default to 0 (root) */
    p_inode->st_uid = 0;
    if (ATTR_MASK_TEST(p_attr_set, uid)) {
        struct passwd pw;
        struct passwd *p_pw;

        if (global_config.uid_gid_as_numbers)
            p_inode->st_uid = ATTR(p_attr_set, uid).num;
        else if ((getpwnam_r
                  (ATTR(p_attr_set, uid).txt, &pw, buff, sizeof(buff),
                   &p_pw) != 0)
                 || (p_pw == NULL))
            DisplayLog(LVL_MAJOR, __func__,
                       "Warning: couldn't resolve uid for user '%s'",
                       ATTR(p_attr_set, uid).txt);
        else
            p_inode->st_uid = p_pw->pw_uid;
    }

    if (ATTR_MASK_TEST(p_attr_set, gid)) {
        struct group gr;
        struct group *p_gr;

        if (global_config.uid_gid_as_numbers)
            p_inode->st_gid = ATTR(p_attr_set, gid).num;
        else if ((getgrnam_r
                  (ATTR(p_attr_set, gid).txt, &gr, buff, sizeof(buff),
                   &p_gr) != 0)
                 || (p_gr == NULL))
            DisplayLog(LVL_MAJOR, __func__,
                       "Warning: couldn't resolve gid for group '%s'",
                       ATTR(p_attr_set, gid).txt);
        else
            p_inode->st_gid = p_gr->gr_gid;
    }

    if (ATTR_MASK_TEST(p_attr_set, size))
        p_inode->st_size = ATTR(p_attr_set, size);
    else
        p_inode->st_size = 0;

    if (ATTR_MASK_TEST(p_attr_set, last_access))
        p_inode->st_atime = ATTR(p_attr_set, last_access);
    else    /* default to current time */
        p_inode->st_atime = time(NULL);

    if (ATTR_MASK_TEST(p_attr_set, last_mod))
        p_inode->st_mtime = ATTR(p_attr_set, last_mod);
    else    /* default to current time */
        p_inode->st_mtime = time(NULL);

    if (ATTR_MASK_TEST(p_attr_set, last_mdchange))
        p_inode->st_ctime = ATTR(p_attr_set, last_mdchange);
    else    /* default to current time */
        p_inode->st_ctime = time(NULL);
}

#ifndef HAVE_GETMNTENT_R

/* if getmntent_r() does not exists, define it as a wrapper of getmntent().
 * use a lock to ensure thread-safety.
 */
static pthread_mutex_t mntent_lock = PTHREAD_MUTEX_INITIALIZER;

/* copy a mntent structure to caller's buffer */
static int copy_mntent(struct mntent *mntout, char *buf, int buflen,
                       const struct mntent *mntin)
{
    char *curr = buf;

    if (!buf || !mntout)
        return EFAULT;

    if (strlen(mntin->mnt_fsname) + 1
        + strlen(mntin->mnt_dir) + 1
        + strlen(mntin->mnt_type) + 1 + strlen(mntin->mnt_opts) + 1 > buflen)
        return ENOMEM;

    strcpy(curr, mntin->mnt_fsname);
    mntout->mnt_fsname = curr;
    curr += strlen(mntin->mnt_fsname) + 1;

    strcpy(curr, mntin->mnt_dir);
    mntout->mnt_dir = curr;
    curr += strlen(mntin->mnt_dir) + 1;

    strcpy(curr, mntin->mnt_type);
    mntout->mnt_type = curr;
    curr += strlen(mntin->mnt_type) + 1;

    strcpy(curr, mntin->mnt_opts);
    mntout->mnt_opts = curr;
    curr += strlen(mntin->mnt_opts) + 1;

    mntout->mnt_freq = mntin->mnt_freq;
    mntout->mnt_passno = mntin->mnt_passno;

    return 0;
}

static struct mntent *getmntent_r(FILE *fp, struct mntent *mntbuf,
                                  char *buf, int buflen)
{
    struct mntent *pmntent;
    /* struct mntent *getmntent(FILE *fp); */
    P(mntent_lock);
    pmntent = getmntent(fp);
    /* copy mntent structure to caller buffer */
    if (pmntent) {
        if (copy_mntent(mntbuf, buf, buflen, pmntent) != 0)
            pmntent = NULL; /* causes an error */
    }
    V(mntent_lock);
    return pmntent;
}
#endif

/**
 * Check if the given input path matches or in under
 * a mount entry.
 * @param[in] in_path       Path to be matched.
 * @param[in] p_mnt         Mount entry to be matched against.
 * @param[in] allow_root    Allow matching root filesystem.
 */
static bool match_mount_path(const char *in_path, const struct mntent *p_mnt,
                             bool allow_root)
{
    int pathlen = strlen(p_mnt->mnt_dir);

    /* Matching root filesystem. */
    if (!strcmp(p_mnt->mnt_dir, "/")) {
        DisplayLog(LVL_DEBUG, "CheckFS",
                   "Root mountpoint is%s allowed for matching %s, type=%s, fs=%s",
                   allow_root ? "" : " NOT", in_path, p_mnt->mnt_type,
                   p_mnt->mnt_fsname);
        return allow_root;
    }

    /* In other cases The filesystem must be <mountpoint>/<smthg>
     * or <mountpoint>\0
     */
    if (!strncmp(in_path, p_mnt->mnt_dir, pathlen) &&
        ((in_path[pathlen] == '/') || (in_path[pathlen] == '\0'))) {
        DisplayLog(LVL_DEBUG, "CheckFS",
                   "%s is under mountpoint %s, type=%s, fs=%s",
                   in_path, p_mnt->mnt_dir, p_mnt->mnt_type, p_mnt->mnt_fsname);
        return true;
    }
    /* don't match */
    return false;
}

/* Check mount point and FS type.
 * Also return the associated device number.
 * (for STAY_IN_FS security option).
 */
int check_fs_info(const char *path, const char *expected_type,
                  dev_t *p_fs_dev, char *fsname_out,
                  bool check_mounted, bool save_fs)
{
    FILE *fp;
    struct mntent *p_mnt;
    struct mntent mnt_ent;
    char *match_dir = NULL;
    char *match_type = NULL;
    char *match_fs = NULL;
    char mnt_buff[4096];
    char *rpath = NULL;
    char *parentmntdir;
    char *name = NULL;
    char *tmp_buff;
    struct stat pathstat;
    struct stat parentmntstat;
    int rc = 0;

    if ((expected_type == NULL) || (expected_type[0] == '\0')) {
        DisplayLog(LVL_CRIT, "CheckFS",
                   "/!\\ ERROR /!\\ No filesystem type specified");
        return EINVAL;
    }

    /* Convert to canonic path. */
    /* Let realpath determine the output length (NULL argument). */
    rpath = realpath(path, NULL);
    if (rpath == NULL) {
        DisplayLog(LVL_CRIT, "CheckFS", "Error %d in realpath(%s): %s",
                   errno, (path ? path : "<null>"), strerror(errno));
        return errno;
    }
    if (strlen(rpath) >= RBH_PATH_MAX) {
        rc = -ENAMETOOLONG;
        DisplayLog(LVL_CRIT, "CheckFS", "Path length is too long!");
        goto out_free;
    }

    /* open mount tab and look for the given path */
    fp = setmntent(MOUNTED, "r");
    if (fp == NULL) {
        rc = -errno;
        DisplayLog(LVL_CRIT, "CheckFS", "Error in setmntent(%s): %s",
                   MOUNTED, strerror(-rc));
        goto out_free;
    }

    while ((p_mnt = getmntent_r(fp, &mnt_ent, mnt_buff,
                                sizeof(mnt_buff))) != NULL) {
        if (p_mnt->mnt_dir == NULL)
            continue;

        /* allow matching root if 'check_mounted' is disabled */
        if (match_mount_path(rpath, p_mnt, !check_mounted)) {
            /* free previous paths */
            free(match_dir);
            free(match_type);
            free(match_fs);

            /* duplicate new values */
            match_dir = strdup(p_mnt->mnt_dir);
            match_type = strdup(p_mnt->mnt_type);
            match_fs = strdup(p_mnt->mnt_fsname);
        }
    }
    endmntent(fp);

    if (match_dir == NULL) {
        DisplayLog(LVL_CRIT, "CheckFS", "No mount entry matches '%s' in %s",
                   rpath, MOUNTED);
        DisplayLog(LVL_CRIT, "CheckFS",
                   "Set 'check_mounted = no' in configuration to force using root filesystem");
        rc = -ENOENT;
        goto out_free;
    }

    /* display the matching entry */
    DisplayLog(LVL_EVENT, "CheckFS",
               "'%s' matches mount point '%s', type=%s, fs=%s", rpath,
               match_dir, match_type, match_fs);

    /* check filesystem type */
    if (strcasecmp(match_type, expected_type) != 0) {
        if (check_mounted) {
            DisplayLog(LVL_CRIT, "CheckFS",
                       "/!\\ ERROR /!\\ The specified type for '%s' (%s) does not match actual filesystem type (%s)",
                       rpath, expected_type, match_type);
            rc = -EINVAL;
            goto out_free;
        } else {
            DisplayLog(LVL_MAJOR, "CheckFS",
                       "/!\\ WARNING /!\\ The specified type for '%s' (%s) "
                       "does not match actual filesystem type (%s).",
                       rpath, expected_type, match_type);
            DisplayLog(LVL_MAJOR, "CheckFS",
                       "check_mounted is disabled: continuing.");
        }
    }

    /* stat the given fs_path */
    if (stat(rpath, &pathstat) != 0) {
        rc = -errno;
        DisplayLog(LVL_CRIT, "CheckFS",
                   "/!\\ ERROR /!\\ Couldn't stat '%s': %s", rpath,
                   strerror(-rc));
        goto out_free;
    }

    /* Stat upper level of mount point, to check if
     * the filesystem is mounted (device must be different).
     * (dirname modifies string content, so we work on a copy
     * in tmp_buff).
     */
    tmp_buff = strdup(match_dir);
    parentmntdir = dirname(tmp_buff);

    if (lstat(parentmntdir, &parentmntstat) != 0) {
        rc = -errno;
        DisplayLog(LVL_CRIT, "CheckFS",
                   "/!\\ ERROR /!\\ Couldn't stat %s: %s", parentmntdir,
                   strerror(-rc));
        goto out_free;
    }

    /* check that filesystem device is different from root
     * (except if check_mounted is disabled) */
    if ((pathstat.st_dev == parentmntstat.st_dev) && check_mounted) {
        rc = -ENOENT;
        DisplayLog(LVL_CRIT, "CheckFS",
                   "/!\\ ERROR /!\\ Filesystem '%s' is not mounted ! dev(%s)=dev(%s)=%#"
                   PRIx64, match_dir, parentmntdir, rpath,
                   (uint64_t)parentmntstat.st_dev);
        goto out_free;
    }
    free(tmp_buff);
#ifdef _LUSTRE
    if (!strcmp(match_type, "lustre")) {
        char *ptr = strstr(match_fs, ":/");

        if (ptr != NULL)
            name = ptr + 2;
        else
            name = match_fs;
    } else
#endif
        name = match_fs;

    /* all checks are OK */
    if (save_fs) {
        /* getting filesystem fsid (needed for fskey) */
        if (global_config.fs_key == FSKEY_FSID) {
            struct statfs stf;

            if (statfs(match_dir, &stf)) {
                rc = -errno;
                DisplayLog(LVL_CRIT, "CheckFS", "ERROR calling statfs(%s): %s",
                           match_dir, strerror(-rc));
                goto out_free;
            }
            /* if fsid == 0, it may mean that fsid is not significant on the
             * current system => DISPLAY A WARNING */
            if (fsidto64(stf.f_fsid) == 0)
                DisplayLog(LVL_MAJOR, "CheckFS",
                           "WARNING: fsid(0) doesn't look significant on this system."
                           "It should not be used as fs_key!");

            rc = set_fs_info(name, match_dir, pathstat.st_dev, stf.f_fsid);
        } else {
            fsid_t dummy_fsid;

            memset(&dummy_fsid, 0, sizeof(fsid_t));
            rc = set_fs_info(name, match_dir, pathstat.st_dev, dummy_fsid);
        }
        if (rc)
            goto out_free;
    }

    if (p_fs_dev != NULL)
        *p_fs_dev = pathstat.st_dev;

    if (fsname_out != NULL)
        strcpy(fsname_out, name);

    rc = 0;

 out_free:
    free(match_dir);
    free(match_type);
    free(match_fs);
    free(rpath);
    return rc;
}

/**
 * Initialize filesystem access and retrieve current devid/fs_key
 * - global_config must be set
 * - initialize mount_point, fsname and dev_id
 */
int InitFS(void)
{
    static bool initialized = false;
    int rc;

    if (initialized)
        return 0;

    /* Initialize mount point info */
#ifdef _LUSTRE
    if (!strcmp(global_config.fs_type, "lustre")) {
        if ((rc = Lustre_Init())) {
            DisplayLog(LVL_CRIT, "InitFS", "Error %d initializing liblustreapi",
                       rc);
            return rc;
        }
    }
#endif

    rc = check_fs_info(global_config.fs_path, global_config.fs_type, NULL, NULL,
                       global_config.check_mounted, true);
    if (rc) {
        DisplayLog(LVL_CRIT, "InitFS", "Error %d checking Filesystem", rc);
        return rc;
    }

    /* OK */
    initialized = true;
    return 0;
}

/**
 * This is to be called after a dev_id change was detected
 * return 0 if fskey is unchanged and update mount_point, fsname and dev_id
 * else, return != 0
 */
int ResetFS(void)
{
    char name[RBH_PATH_MAX];
    dev_t dev;
    struct statfs stf;
    int rc;
    /* check depending on FS key type:
     * - fsname: check mount tab
     * - fsid: check statfs
     * - devid: check dev_id
     */
    switch (global_config.fs_key) {
    case FSKEY_FSNAME:
        /* get and compare FS name */
        rc = check_fs_info(global_config.fs_path, global_config.fs_type, NULL,
                           name, global_config.check_mounted, false);
        if (rc)
            return rc;
        /* did the name changed ? */
        if (strcmp(name, fsname)) {
            DisplayLog(LVL_CRIT, "FSInfo", "fsname change detected: %s->%s",
                       fsname, name);
            RaiseAlert("Filesystem changed",
                       "fsname of '%s' has changed !!! %s->%s => EXITING",
                       global_config.fs_path, fsname, name);
            return -1;
        }
        /* update fsid and devid */
        rc = check_fs_info(global_config.fs_path, global_config.fs_type, NULL,
                           NULL, global_config.check_mounted, true);
        return rc;

    case FSKEY_FSID:
        /* get and compare FS ID */
        if (statfs(global_config.fs_path, &stf)) {
            rc = -errno;
            DisplayLog(LVL_CRIT, "FSInfo", "ERROR calling statfs(%s): %s",
                       global_config.fs_path, strerror(-rc));
            return rc;
        }
        if (fsidto64(stf.f_fsid) != fs_key) {
            DisplayLog(LVL_CRIT, "FSInfo",
                       "fsid change detected: %" PRIX64 "->%" PRIX64, fs_key,
                       fsidto64(stf.f_fsid));
            RaiseAlert("Filesystem changed",
                       "fsid of '%s' has changed !!! %" PRIX64 "->%" PRIX64
                       " => EXITING", global_config.fs_path, fs_key,
                       fsidto64(stf.f_fsid));
            return -1;
        }
        /* update fsname and devid */
        rc = check_fs_info(global_config.fs_path, global_config.fs_type, NULL,
                           NULL, global_config.check_mounted, true);
        return rc;

    case FSKEY_DEVID:
        /* get and compare dev id */
        rc = check_fs_info(global_config.fs_path, global_config.fs_type, &dev,
                           NULL, global_config.check_mounted, false);
        if (rc)
            return rc;
        /* did the device change? */
        if (dev != dev_id) {
            DisplayLog(LVL_CRIT, "FSInfo",
                       "devid change detected: %" PRI_DT "->%" PRI_DT, dev_id,
                       dev);

            RaiseAlert("Filesystem changed",
                       "devid of '%s' has changed !!! %" PRI_DT "->%" PRI_DT
                       " => EXITING", global_config.fs_path, dev_id, dev);
            return -1;
        }
        /* update fsname and fsid */
        rc = check_fs_info(global_config.fs_path, global_config.fs_type, NULL,
                           NULL, global_config.check_mounted, true);
        return rc;

    default:
        DisplayLog(LVL_MAJOR, "FSInfo", "Invalid fs_key type %#x",
                   global_config.fs_key);
        return -1;
    }

}

/**
 *  Check that FS path is the same as the last time.
 */
int CheckLastFS(void)
{
    int rc;
    lmgr_t lmgr;
    char value[1024];
    char str_id[128];

    rc = ListMgr_InitAccess(&lmgr);
    if (rc) {
        DisplayLog(LVL_CRIT, "CheckFS", "Error %d connecting to database", rc);
        return rc;
    }
    rc = ListMgr_GetVar(&lmgr, FS_PATH_VAR, value, sizeof(value));
    if (rc == DB_SUCCESS) {
        if (strcmp(value, global_config.fs_path)) {
            DisplayLog(LVL_CRIT, "CheckFS",
                       "Filesystem %s does not correspond to database content (%s)",
                       global_config.fs_path, value);
            DisplayLog(LVL_CRIT, "CheckFS",
                       "Drop the database and restart the daemon.");
            rc = -1;
        } else {
            DisplayLog(LVL_DEBUG, "CheckFS", "%s matches database content.",
                       global_config.fs_path);
            rc = 0;
        }
    } else if (rc == DB_NOT_EXISTS) {
        DisplayLog(LVL_FULL, "CheckFS", FS_PATH_VAR "='%s'.",
                   global_config.fs_path);
        rc = ListMgr_SetVar(&lmgr, FS_PATH_VAR, global_config.fs_path);
        if (rc)
            DisplayLog(LVL_CRIT, "CheckFS",
                       "Error %d setting variable 'FS_path'%s", rc,
                       rc ==
                       DB_NOT_EXISTS ?
                       " (likely: database schema is not created yet, and you have a read-only DB access)."
                       : "");
    } else {
        DisplayLog(LVL_CRIT, "CheckFS",
                   "Error %d retrieving variable 'FS_path'", rc);
    }

    /* can't check root id if not initialized */
    if (fs_key == 0)
        goto out;

    snprintf(str_id, sizeof(str_id), DFID_NOBRACE, PFID(&root_id));

    rc = ListMgr_GetVar(&lmgr, ROOT_ID_VAR, value, sizeof(value));
    if (rc == DB_SUCCESS) {
        if (strcmp(str_id, value) != 0) {
            DisplayLog(LVL_CRIT, "CheckFS",
                       "Root id changed! Previous value: '%s', new value: '%s'",
                       value, str_id);
            DisplayLog(LVL_CRIT, "CheckFS",
                       "Drop the database and restart the daemon.");
            rc = -1;
        } else {
            DisplayLog(LVL_DEBUG, "CheckFS",
                       "Root id '%s' matches previous value.",
                       str_id);
            rc = 0;
        }
    } else if (rc == DB_NOT_EXISTS) {
        DisplayLog(LVL_FULL, "CheckFS", ROOT_ID_VAR "='%s'.",
                   str_id);
        rc = ListMgr_SetVar(&lmgr, ROOT_ID_VAR, str_id);
        if (rc)
            DisplayLog(LVL_CRIT, "CheckFS",
                       "Error %d setting variable '"ROOT_ID_VAR"'%s", rc,
                       rc == DB_NOT_EXISTS ?
                       " (likely: database schema is not created yet, and you have a read-only DB access)."
                       : "");
    } else {
        DisplayLog(LVL_CRIT, "CheckFS",
                   "Error %d retrieving variable '"ROOT_ID_VAR"'", rc);
    }

out:
    ListMgr_CloseAccess(&lmgr);
    return rc;
}

/* return 0 if thread has been killed,
 * -1 if thread was already terminated.
 */
int TerminateThread(pthread_t thread_id)
{
    if (pthread_cancel(thread_id) == ESRCH)
        return -1;

    return 0;
}

/* Format a size in a "human readable" format */

char *FormatFileSize(char *buff, size_t str_sz, uint64_t file_size)
{
    if (file_size < KILO_BYTE)
        snprintf(buff, str_sz, "%llu", (unsigned long long)file_size);
    else if (file_size < MEGA_BYTE)
        snprintf(buff, str_sz, "%.2f KB",
                 (0.0 + file_size) / (0.0 + KILO_BYTE));
    else if (file_size < GIGA_BYTE)
        snprintf(buff, str_sz, "%.2f MB",
                 (0.0 + file_size) / (0.0 + MEGA_BYTE));
    else if (file_size < TERA_BYTE)
        snprintf(buff, str_sz, "%.2f GB",
                 (0.0 + file_size) / (0.0 + GIGA_BYTE));
    else if (file_size < PETA_BYTE)
        snprintf(buff, str_sz, "%.2f TB",
                 (0.0 + file_size) / (0.0 + TERA_BYTE));
    else if (file_size < EXA_BYTE)
        snprintf(buff, str_sz, "%.2f PB",
                 (0.0 + file_size) / (0.0 + PETA_BYTE));
    else
        snprintf(buff, str_sz, "%.2f EB", (0.0 + file_size) / (0.0 + EXA_BYTE));

    return buff;

}

/**
 * Format a duration (in seconds) to a string with days hours minutes
 * seconds...
 */
char *FormatDuration(char *buff, size_t str_sz, time_t duration)
{

    unsigned int days;
    unsigned int hours;
    unsigned int minutes;
    unsigned int secondes;

    size_t written = 0;

    if (duration < 0)
        duration = -duration;

    days = (unsigned int)duration / DAY;
    hours = ((unsigned int)duration % DAY) / HOUR;
    minutes = ((unsigned int)duration % HOUR) / MINUTE;
    secondes = ((unsigned int)duration % MINUTE);

    buff[0] = '\0';

    if (days > 0)
        written += snprintf(buff, str_sz, "%ud ", days);

    if (hours > 0)
        written += snprintf(buff + written, str_sz - written, "%uh ", hours);

    if (minutes > 0)
        written +=
            snprintf(buff + written, str_sz - written, "%.2umin ", minutes);

    if (secondes > 0 || duration == 0)
        written +=
            snprintf(buff + written, str_sz - written, "%.2us ", secondes);

    if ((written > 0) && (buff[written - 1] == ' '))
        buff[written - 1] = '\0';

    return buff;

}

/**
 * Format a duration (in seconds) to a string with the best fitting unit
 * (float value)
 */
char *FormatDurationFloat(char *buff, size_t str_sz, time_t duration)
{
    float days;
    float hours;
    float minutes;
    unsigned int secondes;

    if (duration < 0)
        duration = -duration;

    days = (float)duration / DAY;
    hours = (float)duration / HOUR;
    minutes = (float)duration / MINUTE;
    secondes = (unsigned int)duration;

    buff[0] = '\0';

    if (days >= 1.0)
        snprintf(buff, str_sz, "%.1fd", days);
    else if (hours >= 1.0)
        snprintf(buff, str_sz, "%.1fh", hours);
    else if (minutes >= 1.0)
        snprintf(buff, str_sz, "%.1fmin", minutes);
    else
        snprintf(buff, str_sz, "%.2us", secondes);

    return buff;

}

/**
 * Convert a string to a long integer
 * @return -1 on error.
 */
long long str2bigint(const char *str)
{
    char suffix[256];
    int nb_read;
    long long value;

    if (str == NULL)
        return -1;

    nb_read = sscanf(str, "%lld%s", &value, suffix);

    if (nb_read <= 0)
        return -1;  /* invalid format */

    if ((nb_read == 1) || (suffix[0] == '\0'))
        return value;   /* no suffix => 0K */
    else
        return -1;
}

/**
 * Convert a string to a boolean
 * @return -1 on error.
 */
int str2bool(const char *str)
{
    if (str == NULL)
        return -1;

    if (!strcmp(str, "1") || !strcasecmp(str, "true") ||
        !strcasecmp(str, "yes") || !strcasecmp(str, "enabled") ||
        !strcasecmp(str, "on"))
        return 1;

    if (!strcmp(str, "0") || !strcasecmp(str, "false") ||
        !strcasecmp(str, "no") || !strcasecmp(str, "disabled") ||
        !strcasecmp(str, "off"))
        return 0;

    return -1;
}

/**
 * Convert a string to a duration in seconds
 * @return -1 on error.
 */
int str2duration(const char *str)
{
    int nb_read, duration;
    char suffix[256];

    if (str == NULL)
        return -1;

    nb_read = sscanf(str, "%d%s", &duration, suffix);

    if (nb_read <= 0)
        return -1;  /* invalid format */

    if ((nb_read == 1) || (suffix[0] == '\0'))
        return duration;    /* no suffix: duration in seconds */

    if (!strcasecmp(suffix, "s") || !strcasecmp(suffix, "sec"))
        return duration;
    if (!strcasecmp(suffix, "m") || !strcasecmp(suffix, "min"))
        return MINUTE * duration;
    if (!strcasecmp(suffix, "h") || !strcasecmp(suffix, "hour"))
        return HOUR * duration;
    if (!strcasecmp(suffix, "d") || !strcasecmp(suffix, "day"))
        return DAY * duration;
    if (!strcasecmp(suffix, "w") || !strcasecmp(suffix, "week"))
        return WEEK * duration;
    if (!strcasecmp(suffix, "y") || !strcasecmp(suffix, "year"))
        return YEAR * duration;

    return -1;
}

/**
 * Convert a string to a size (in bytes)
 * @return -1 on error.
 */
uint64_t str2size(const char *str)
{
    int nb_read;
    unsigned long long size;
    char suffix[256];

    if (str == NULL)
        return (uint64_t)-1LL;

    nb_read = sscanf(str, "%llu%s", &size, suffix);

#ifdef _DEBUG_PARSING
    printf("nb_read = %d, str = %s, size = %llu, suffix = %s\n", nb_read, str,
           size, suffix);
#endif

    if (nb_read <= 0)
        return (uint64_t)-1LL;  /* invalid format */

    if ((nb_read == 1) || (suffix[0] == '\0'))
        return size;    /* no suffix: size in bytes */

    if (!strcasecmp(suffix, "B"))
        return size;
    if (!strcasecmp(suffix, "kB") || !strcasecmp(suffix, "K"))
        return KILO_BYTE * size;
    if (!strcasecmp(suffix, "MB") || !strcasecmp(suffix, "M"))
        return MEGA_BYTE * size;
    if (!strcasecmp(suffix, "GB") || !strcasecmp(suffix, "G"))
        return GIGA_BYTE * size;
    if (!strcasecmp(suffix, "TB") || !strcasecmp(suffix, "T"))
        return TERA_BYTE * size;
    if (!strcasecmp(suffix, "PB") || !strcasecmp(suffix, "P"))
        return PETA_BYTE * size;
    if (!strcasecmp(suffix, "EB") || !strcasecmp(suffix, "E"))
        return EXA_BYTE * size;

    return (uint64_t)-1LL;
}

/**
 * extracts up to count digits from src string,
 * and copy them to dest string. dest is completed
 * with '\0'.
 * @return the number of digits copied to dest.
 */
static inline int extract_digits(const char *src, char *dest,
                                 unsigned int count)
{
    unsigned int i;
    unsigned int cpd = 0;
    for (i = 0; (i < count) && (src[i] != '\0'); i++) {
        dest[i] = src[i];
        cpd++;
    }
    dest[cpd] = '\0';
    return cpd;
}

/** parse date/time yyyymmdd[HH[MM[SS]]] */
time_t str2date(const char *str)
{
    struct tm datetime = {
        .tm_sec = 0,
        .tm_min = 0,
        .tm_hour = 0,
        .tm_mday = 0,
        .tm_mon = 0,
        .tm_year = 0,
        .tm_wday = 0,
        .tm_yday = 0,
        .tm_isdst = -1
    };
    char tmpstr[16];
    int tmpint;
    const char *curr = str;

    /* extract year */
    if (extract_digits(curr, tmpstr, 4) < 4)
        return (time_t)-1;
    curr += 4;
    if ((tmpint = str2int(tmpstr)) == -1)
        return (time_t)-1;
    datetime.tm_year = tmpint - 1900;   /* 1900 => 0 */

    /* extract month */
    if (extract_digits(curr, tmpstr, 2) < 2)
        return (time_t)-1;
    curr += 2;
    if ((tmpint = str2int(tmpstr)) <= 0)
        return (time_t)-1;
    else if (tmpint > 12)
        return (time_t)-1;
    datetime.tm_mon = tmpint - 1;   /* January => 0 */

    /* extract day */
    if (extract_digits(curr, tmpstr, 2) < 2)
        return (time_t)-1;
    curr += 2;
    if ((tmpint = str2int(tmpstr)) <= 0)
        return (time_t)-1;
    else if (tmpint > 31)
        return (time_t)-1;
    datetime.tm_mday = tmpint;  /* 1st => 1 */

    /* extract hours */
    tmpint = extract_digits(curr, tmpstr, 2);
    if (tmpint == 0)    /* not specified */
        goto convert;
    else if (tmpint < 2)    /* invalid */
        return (time_t)-1;
    curr += 2;
    if ((tmpint = str2int(tmpstr)) == -1)
        return (time_t)-1;
    else if (tmpint > 23)
        return (time_t)-1;
    datetime.tm_hour = tmpint;

    /* extract minutes */
    tmpint = extract_digits(curr, tmpstr, 2);
    if (tmpint == 0)    /* not specified */
        goto convert;
    else if (tmpint < 2)    /* invalid */
        return (time_t)-1;
    curr += 2;
    if ((tmpint = str2int(tmpstr)) == -1)
        return (time_t)-1;
    else if (tmpint > 59)
        return (time_t)-1;
    datetime.tm_min = tmpint;

    /* extract seconds */
    tmpint = extract_digits(curr, tmpstr, 2);
    if (tmpint == 0)    /* not specified */
        goto convert;
    else if (tmpint < 2)    /* invalid */
        return (time_t)-1;
    curr += 2;
    if ((tmpint = str2int(tmpstr)) == -1)
        return (time_t)-1;
    else if (tmpint > 59)
        return (time_t)-1;
    datetime.tm_sec = tmpint;

    if (*curr != '\0')
        return (time_t)-1;

 convert:
    return mktime(&datetime);
}

#define TYPEINDEX(mode) (((mode) >> 12) & 0x0f)
#define TYPECHAR(mode)  ("0pcCd?bB-?l?s???"[TYPEINDEX(mode)])

/* The special bits. If set, display SMODE0/1 instead of MODE0/1 */
static const mode_t SBIT[] = {
    0, 0, S_ISUID,
    0, 0, S_ISGID,
    0, 0, S_ISVTX
};

/* The 9 mode bits to test */
static const mode_t MBIT[] = {
    S_IRUSR, S_IWUSR, S_IXUSR,
    S_IRGRP, S_IWGRP, S_IXGRP,
    S_IROTH, S_IWOTH, S_IXOTH
};

static const char MODE1[] = "rwxrwxrwx";
static const char MODE0[] = "---------";
static const char SMODE1[] = "..s..s..t";
static const char SMODE0[] = "..S..S..T";

/*
 * Return the standard ls-like mode string from a file mode.
 * This is static and so is overwritten on each call.
 */
const char *mode_string(mode_t mode, char *buf)
{
    int i;

    for (i = 0; i < 9; i++) {
        if (mode & SBIT[i])
            buf[i] = (mode & MBIT[i]) ? SMODE1[i] : SMODE0[i];
        else
            buf[i] = (mode & MBIT[i]) ? MODE1[i] : MODE0[i];
    }
    return buf;
}

/**
 *  Print attributes to a GString.
 *  This is used for alerts and diff display (brief argument).
 */
void print_attrs(GString *str, const attr_set_t *p_attr_set,
                 attr_mask_t overide_mask, bool brief)
{
    attr_mask_t mask = p_attr_set->attr_mask;
    char tmpbuf[256];
    const char *format;
    int i;

    assert(str != NULL);

    if (!attr_mask_is_null(overide_mask))
        mask = attr_mask_and(&mask, &overide_mask);

    /* initialize to empty string */
    g_string_assign(str, "");

    if (mask.std & ATTR_MASK_fullpath) {
        if (brief)
            format = "path='%s',";
        else
            format = "Path:     \"%s\"\n";

        g_string_append_printf(str, format, ATTR(p_attr_set, fullpath));
    }
    /* this information is redundant with fullpath,
     * so only display it if path is not known */
    else if (mask.std & ATTR_MASK_name) {
        if (brief)
            format = "name='%s',";
        else
            format = "Name:     \"%s\"\n";

        g_string_append_printf(str, format, ATTR(p_attr_set, name));
    }
    if (mask.std & ATTR_MASK_parent_id) {
        if (brief)
            format = "parent=" DFID ",";
        else
            format = "Parent:   " DFID "\n";

        g_string_append_printf(str, format, PFID(&ATTR(p_attr_set, parent_id)));
    }

    if (mask.std & ATTR_MASK_type) {
        if (brief)
            format = "type=%s,";
        else
            format = "Type:     %s\n";

        g_string_append_printf(str, format, ATTR(p_attr_set, type));
    }

    if (mask.std & ATTR_MASK_nlink) {
        if (brief)
            format = "nlink=%u,";
        else
            format = "Nlinks:   %u\n";

        g_string_append_printf(str, format, ATTR(p_attr_set, nlink));
    }

    if (mask.std & ATTR_MASK_mode) {
        if (brief)
            format = "mode=%#o,";
        else
            format = "Mode:     %#o\n";

        g_string_append_printf(str, format, ATTR(p_attr_set, mode));
    }

    if (mask.std & ATTR_MASK_uid) {
        if (global_config.uid_gid_as_numbers) {
            if (brief)
                format = "owner=%d,";
            else
                format = "Owner:    \"%d\"\n";

            g_string_append_printf(str, format, ATTR(p_attr_set, uid).num);
        } else {
            if (brief)
                format = "owner=%s,";
            else
                format = "Owner:    \"%s\"\n";

            g_string_append_printf(str, format, ATTR(p_attr_set, uid).txt);
        }
    }
    if (mask.std & ATTR_MASK_gid) {
        if (global_config.uid_gid_as_numbers) {
            if (brief)
                format = "group=%d,";
            else
                format = "Group:    \"%d\"\n";

            g_string_append_printf(str, format, ATTR(p_attr_set, gid).num);
        } else {
            if (brief)
                format = "group=%s,";
            else
                format = "Group:    \"%s\"\n";

            g_string_append_printf(str, format, ATTR(p_attr_set, gid).txt);
        }
    }
    if (mask.std & ATTR_MASK_size) {
        if (brief) {
            g_string_append_printf(str, "size=%" PRIu64 ",",
                                   ATTR(p_attr_set, size));
        } else {
            FormatFileSize(tmpbuf, sizeof(tmpbuf), ATTR(p_attr_set, size));
            g_string_append_printf(str, "Size:     %s\n", tmpbuf);
        }
    }
    if (mask.std & ATTR_MASK_blocks) {
        if (brief)
            format = "blocks=%Lu,";
        else
            format = "Blocks:   %Lu\n";

        g_string_append_printf(str, format, ATTR(p_attr_set, blocks));
    }
    if (mask.std & ATTR_MASK_depth) {
        if (brief)
            format = "depth=%u,";
        else
            format = "Depth:    %u\n";

        g_string_append_printf(str, format, ATTR(p_attr_set, depth));
    }

    if (mask.std & ATTR_MASK_dircount) {
        if (brief)
            format = "dircount=%u,";
        else
            format = "DirCount: %u\n";

        g_string_append_printf(str, format, ATTR(p_attr_set, dircount));
    }

    if (mask.std & ATTR_MASK_last_access) {
        if (brief) {
            g_string_append_printf(str, "access=%u,",
                                   ATTR(p_attr_set, last_access));
        } else {
            FormatDurationFloat(tmpbuf, sizeof(tmpbuf),
                                time(NULL) - ATTR(p_attr_set, last_access));

            g_string_append_printf(str, "Last Access: %s ago\n", tmpbuf);
        }
    }
    if (mask.std & ATTR_MASK_last_mod) {
        if (brief) {
            g_string_append_printf(str, "modif=%u,",
                                   ATTR(p_attr_set, last_mod));
        } else {
            FormatDurationFloat(tmpbuf, sizeof(tmpbuf),
                                time(NULL) - ATTR(p_attr_set, last_mod));
            g_string_append_printf(str, "Last Mod: %s ago\n", tmpbuf);
        }
    }

    if (mask.std & ATTR_MASK_last_mdchange) {
        if (brief) {
            g_string_append_printf(str, "change=%u,",
                                   ATTR(p_attr_set, last_mdchange));
        } else {
            FormatDurationFloat(tmpbuf, sizeof(tmpbuf),
                                time(NULL) - ATTR(p_attr_set, last_mdchange));
            g_string_append_printf(str, "Last Change: %s ago\n", tmpbuf);
        }
    }

    if (mask.std & ATTR_MASK_creation_time) {
        if (brief) {
            g_string_append_printf(str, "creation=%u,",
                                   ATTR(p_attr_set, creation_time));
        } else {
            FormatDurationFloat(tmpbuf, sizeof(tmpbuf),
                                time(NULL) - ATTR(p_attr_set, creation_time));
            g_string_append_printf(str, "Creation: %s ago\n", tmpbuf);
        }
    }
#ifdef _LUSTRE
    if (mask.std & ATTR_MASK_stripe_items) {
        if (brief)
            g_string_append(str, "stripes={");
        else
            g_string_append(str, "Stripes: ");

        append_stripe_list(str, &ATTR(p_attr_set, stripe_items), brief);

        if (brief)
            g_string_append(str, "},");
        else
            g_string_append_c(str, '\n');
    }

    if (mask.std & ATTR_MASK_stripe_info) {
        if (brief) {
            g_string_append_printf(str,
                                   "stripe_count=%u,stripe_size=%" PRIu64 ",",
                                   ATTR(p_attr_set, stripe_info).stripe_count,
                                   ATTR(p_attr_set, stripe_info).stripe_size);

            if (!EMPTY_STRING(ATTR(p_attr_set, stripe_info).pool_name)) {
                g_string_append_printf(str, "ost_pool=%s,",
                                       ATTR(p_attr_set, stripe_info).pool_name);
            }
        } else {
            format = "Stripe count: %u\n" "Stripe size:  %s\n";

            FormatFileSize(tmpbuf, sizeof(tmpbuf),
                           ATTR(p_attr_set, stripe_info).stripe_size);

            g_string_append_printf(str, format,
                                   ATTR(p_attr_set, stripe_info).stripe_count,
                                   tmpbuf);

            if (!EMPTY_STRING(ATTR(p_attr_set, stripe_info).pool_name)) {
                format = "OST pool:     %s\n";

                g_string_append_printf(str, format,
                                       ATTR(p_attr_set, stripe_info).pool_name);
            }
        }
    }
#endif

    for (i = 0; i < sm_inst_count; i++) {
        sm_instance_t *smi = get_sm_instance(i);

        /* print status */
        if (mask.status & SMI_MASK(i)) {
            if (brief)
                format = "%s=%s,";
            else
                format = "%s:  %s\n";

            g_string_append_printf(str, format, smi->user_name,
                                   STATUS_ATTR(p_attr_set, i));
        }
        /* print specific info for this status manager */
        if (mask.sm_info & smi_info_bits(smi)) {
            for (i = 0; i < smi->sm->nb_info; i++) {
                if (mask.sm_info & smi_info_bit(smi, i)) {

                    g_string_append_printf(str, brief ? "%s=" : "%s:  ",
                                           sm_attr_info[smi->sm_info_offset +
                                                        i].user_attr_name);

                    ListMgr_PrintAttrPtr(str,
                                         sm_attr_info[smi->sm_info_offset +
                                                      i].def->db_type,
                                         SMI_INFO(p_attr_set, smi, i),
                                         brief ? "'" : "\"");

                    g_string_append_c(str, brief ? ',' : '\n');
                }
            }
        }
    }

    if (mask.std & ATTR_MASK_link) {
        if (brief)
            format = "lnk='%s',";
        else
            format = "link: \"%s\"\n";

        g_string_append_printf(str, format, ATTR(p_attr_set, link));
    }

    if (brief && str->len != 0) {
        /* remove final ',' */
        g_string_truncate(str, str->len - 1);
    }
}

/* helpers for attr change */
#define APPLYTAG "ChgAttr"
#define LOG_ATTR_CHANGE(_nfunc, _arg_fmt, _dr, _rc, ...) do { \
            if (_rc)                                          \
                DisplayLog(LVL_CRIT, APPLYTAG, "%s("_arg_fmt") failed: %s", \
                           _nfunc, __VA_ARGS__, strerror(_rc)); \
            else                                              \
                DisplayReport("%s%s("_arg_fmt")", _dr ? "(dry-run) " : "", \
                              _nfunc, __VA_ARGS__); \
        } while (0)

/**
 *  Apply attribute changes
 *  \param change_mask mask of attributes to be changed
 */
int ApplyAttrs(const entry_id_t *p_id, const attr_set_t *p_attr_new,
               const attr_set_t *p_attr_old,
               attr_mask_t change_mask, bool dry_run)
{
    attr_mask_t mask = attr_mask_and(&p_attr_new->attr_mask, &change_mask);
    int rc, err = 0;
    const char *chattr_path = NULL;
#ifdef _HAVE_FID
    char fid_path[RBH_PATH_MAX];
#endif

    if (attr_mask_is_null(mask))
        return 0;

    if (!ATTR_MASK_TEST(p_attr_new, fullpath)) {
#ifdef _HAVE_FID
        /* build fid path */
        BuildFidPath(p_id, fid_path);
        chattr_path = fid_path;
#else
        DisplayLog(LVL_CRIT, APPLYTAG,
                   "No path: cannot apply changes to entry");
        return -EINVAL;
#endif
    } else
        chattr_path = ATTR(p_attr_new, fullpath);

    if (mask.std & ATTR_MASK_fullpath) {
        if (!ATTR_MASK_TEST(p_attr_old, fullpath)) {
            DisplayLog(LVL_CRIT, APPLYTAG,
                       "Cannot rename: source path is unknown");
            err++;
        } else {
            if (!dry_run
                && rename(ATTR(p_attr_old, fullpath),
                          ATTR(p_attr_new, fullpath)))
                rc = errno;
            else
                rc = 0;

            LOG_ATTR_CHANGE("rename", "%s, %s", dry_run, rc,
                            ATTR(p_attr_old, fullpath), ATTR(p_attr_new,
                                                             fullpath));
        }
    } else if (mask.std & ATTR_MASK_parent_id) {
        /* can't change parent without changing path!!! */
    } else if (mask.std & ATTR_MASK_name) {
        /* just change name */
    }

    if (mask.std & ATTR_MASK_type) {
        /* can't change entry type without creating/removing it */
    }

    if (mask.std & (ATTR_MASK_uid | ATTR_MASK_gid)) {
        uid_t u = -1;
        gid_t g = -1;

        if (mask.std & ATTR_MASK_uid) {
            if (global_config.uid_gid_as_numbers) {
                u = ATTR(p_attr_new, uid).num;
            } else {
                struct passwd p;
                char buf[4096];
                struct passwd *res = NULL;

                rc = getpwnam_r(ATTR(p_attr_new, uid).txt, &p, buf, 4096, &res);
                if (rc == 0 && res != NULL)
                    u = res->pw_uid;
            }
        }

        if (mask.std & ATTR_MASK_gid) {
            if (global_config.uid_gid_as_numbers) {
                g = ATTR(p_attr_new, gid).num;
            } else {
                struct group gs;
                char buf[4096];
                struct group *res = NULL;

                rc = getgrnam_r(ATTR(p_attr_new, gid).txt, &gs, buf, 4096,
                                &res);
                if (rc == 0 && res != NULL)
                    g = res->gr_gid;
            }
        }

        if (u != -1 || g != -1) {

            if (!dry_run && lchown(chattr_path, u, g))
                rc = errno;
            else
                rc = 0;

            LOG_ATTR_CHANGE("lchown", "%s, u=%d, g=%d", dry_run, rc,
                            chattr_path, u, g);
        }
    }

    /* always set mode after chown, as it can be changed by chown */
    if (mask.std & ATTR_MASK_mode) {

        if (!dry_run && chmod(chattr_path, ATTR(p_attr_new, mode)))
            rc = errno;
        else
            rc = 0;

        LOG_ATTR_CHANGE("chmod", "%s, %#o", dry_run, rc,
                        chattr_path, ATTR(p_attr_new, mode));
    }

    /* the following changes can't be applied (not supported) */
    /* stripe_items / stripe_info => restripe the file? */
    /* status => perform the needed action? */

    if (mask.std & ATTR_MASK_size) {
        /** @TODO if new size is zero: truncate.
         * else, we have no idea of what's in the file...
         */
    }
    if (mask.std & (ATTR_MASK_last_access | ATTR_MASK_last_mod |
                    ATTR_MASK_last_mdchange)) {
        struct utimbuf t = {
            .actime = -1,
            .modtime = -1
        };
        int get_stat = 0;

        if (mask.std & ATTR_MASK_last_access)
            t.actime = ATTR(p_attr_new, last_access);
        if (mask.std & ATTR_MASK_last_mod)
            t.modtime = ATTR(p_attr_new, last_mod);

        /* if there is still a value == -1, we must fill it
         * or utime will set a bad value
         */
        if (t.actime == -1) {
            if (ATTR_MASK_TEST(p_attr_old, last_access))
                t.actime = ATTR(p_attr_old, last_access);
            else
                /* need to get old value of atime */
                get_stat = 1;
        }
        if (t.modtime == -1) {
            if (ATTR_MASK_TEST(p_attr_old, last_mod))
                t.modtime = ATTR(p_attr_old, last_mod);
            else
                /* need to get old value of atime */
                get_stat = 1;
        }
        if (get_stat) {
            struct stat st;
            if (lstat(chattr_path, &st) == 0) {
                if (t.modtime == -1)
                    t.modtime = st.st_mtime;
                if (t.actime == -1)
                    t.actime = st.st_atime;
            }
        }

        if (!dry_run && utime(chattr_path, &t))
            rc = errno;
        else
            rc = 0;

        LOG_ATTR_CHANGE("utime", "%s, a=%ld, m=%ld", dry_run, rc,
                        chattr_path, t.actime, t.modtime);
    }

    return err;
}

/** Compute greatest common divisor (GCD) of 2 numbers */
unsigned int gcd(unsigned int x, unsigned int y)
{
    unsigned int a = x;
    unsigned int b = y;
    while ((a * b) != 0) {
        if (a > b)
            a = a - b;
        if (a < b)
            b = b - a;
        if (a == b)
            b = 0;
    }
    return a;
}

/** Ensure that the thread is suspended for a given amount
 * of time, event if the process gets interrupts.
 */
void rh_sleep(unsigned int seconds)
{
    time_t start = time(NULL);
    int remain = seconds;
    int spent;

    while (remain > 0) {
        remain = sleep(remain);
        if (remain <= 0) {
            spent = time(NULL) - start;
            if (spent < seconds)
                remain = seconds - spent;
        }
    }
}

/** Substitute a pattern in a string with another sub-string
 * \param str_in_out must be large enough to receive
 *  the resulting string, and cannot exceed 1024.
 */
int str_subst(char *str_in_out, const char *to_be_replaced,
              const char *replacement)
{
    size_t len_from = strlen(to_be_replaced);
    size_t len_to = strlen(replacement);
    char *curr;

    /* - same size: replace inline
     * - smaller string size: replace inline then shift
     */

    for (curr = strstr(str_in_out, to_be_replaced);
         curr != NULL; curr = strstr(curr, to_be_replaced)) {
        unsigned int i;
        char *curr_src;
        char tmp_buff[1024];

        /* if replacement is longer, save end of line */
        if (len_to > len_from)
            strcpy(tmp_buff, curr + len_from);

        for (i = 0; i < len_to; i++)
            curr[i] = replacement[i];
        curr = curr + len_to;

        /* if replacement is smaller, need to shift */
        if (len_to < len_from) {
            if (((char *)(curr + len_from - len_to))[0] == '\0')
                curr[0] = '\0';

            /* shift of len_from - len_to */
            for (curr_src = curr + len_from - len_to, i = 0;
                 *curr_src != '\0'; curr_src++, i++) {
                curr[i] = *curr_src;
                if (curr_src[1] == '\0')
                    curr[i + 1] = '\0';
            }
        } else if (len_to > len_from)
            /* copy saved data */
            strcpy(curr, tmp_buff);
    }
    return 0;
}

/** escape every special character in a regex
 *
 * \param dest      the string to copy the escaped regex to
 * \param dest_size the size of dest (including the terminating char)
 * \param src       the null terminated string representing the regex to
 *                  escape
 * \param charset   a string that contains every char to escape
 *
 * \return          0 on success, -error_code on error
 */
int str_escape_charset(char *dest, size_t dest_size, const char *src,
                       char *charset)
{
    size_t last_idx = 0;
    size_t escape_size = 0;

    for (size_t idx = 0; idx < strlen(src); idx++) {
        /* Is this a special character ? */
        char *token = strchr(charset, src[idx]);
        if (token == NULL)
            continue;

        /* Is there enough space left to escape the next token? */
        if (idx + escape_size + 2 > dest_size)
            return -ENOBUFS;

        /* Copy from last position in src to the current one */
        strncpy(&dest[last_idx + escape_size], &src[last_idx],
                idx - last_idx);
        /* Add an escape char */
        dest[idx + escape_size] = '\\';

        /* Update internals */
        escape_size++;
        last_idx = idx;
    }
    if (strlen(src) + escape_size + 1 > dest_size)
        return -ENOBUFS;

    /* Copy the rest of src (including the terminating char) */
    strcpy(&dest[last_idx + escape_size], &src[last_idx]);
    return 0;
}

/**
 * extract relative path from full path
 */
int relative_path(const char *fullpath, const char *root, char *rel_path)
{
    size_t len;
    char rootcopy[1024];

    if (!strcmp(root, fullpath)) {
        /* arg is root */
        rel_path[0] = '\0';
        return 0;
    }

    /* copy root path */
    strcpy(rootcopy, root);
    len = strlen(rootcopy);

    /* add '/' if needed */
    if ((len > 1) && (rootcopy[len - 1] != '/')) {
        rootcopy[len] = '/';
        rootcopy[len + 1] = '\0';
        len++;
    }

    /* test if the full path starts with the same dirs */
    if (strncmp(rootcopy, fullpath, len)) {
        DisplayLog(LVL_MAJOR, "RelPath",
                   "ERROR: file path '%s' is not under filesystem root '%s'",
                   fullpath, rootcopy);
        return -EINVAL;
    }

    strcpy(rel_path, fullpath + len);
    return 0;
}

void upperstr(char *str)
{
    int i = 0;

    for (i = 0; str[i]; i++)
        str[i] = toupper(str[i]);
}

void lowerstr(char *str)
{
    int i = 0;

    for (i = 0; str[i]; i++)
        str[i] = tolower(str[i]);
}

int path2id(const char *path, entry_id_t *id, const struct stat *st)
{
    int rc;

#ifdef _HAVE_FID
    rc = Lustre_GetFidFromPath(path, id);
    if (rc)
        return rc;
#else
    struct stat stn;

    if (st == NULL) {
        if (lstat(path, &stn)) {
            rc = -errno;
            DisplayLog(LVL_CRIT, __func__, "ERROR: cannot stat '%s': %s",
                       path, strerror(-rc));
            return rc;
        }
        st = &stn;
    }
    /* build id from dev/inode */
    id->inode = st->st_ino;
    id->fs_key = get_fskey();
#endif
    return 0;
}

#define MKDIR_TAG "MkDir"
int mkdir_recurse(const char *full_path, mode_t mode, entry_id_t *dir_id)
{
    char path_copy[MAXPATHLEN];
    const char *curr;
    int rc;
    int exists = 0;

    if (strncmp(global_config.fs_path, full_path, strlen(global_config.fs_path))
        != 0) {
        DisplayLog(LVL_MAJOR, MKDIR_TAG,
                   "Error: '%s' in not under filesystem root '%s'", full_path,
                   global_config.fs_path);
        return -EINVAL;
    }
    /* skip fs root */
    curr = full_path + strlen(global_config.fs_path);

    if (*curr == '\0') {    /* full_path is root dir */
        exists = 1;
        goto get_id;
    } else if (*curr != '/') {  /* slash expected */
        DisplayLog(LVL_MAJOR, MKDIR_TAG,
                   "Error: '%s' in not under filesystem root '%s'", full_path,
                   global_config.fs_path);
        return -EINVAL;
    }

    /* skip first slash */
    curr++;

    while ((curr = strchr(curr, '/')) != NULL) {
        /* if fullpath = '/a/b',
         * curr = &(fullpath[2]);
         * so, copy 2 chars to get '/a'.
         * and set fullpath[2] = '\0'
         */
        int path_len = curr - full_path + 1;

        /* extract directory name */
        rh_strncpy(path_copy, full_path, path_len);

        DisplayLog(LVL_FULL, MKDIR_TAG, "mkdir(%s)", path_copy);
        if ((mkdir(path_copy, mode) != 0) && (errno != EEXIST)) {
            rc = -errno;
            DisplayLog(LVL_CRIT, MKDIR_TAG, "mkdir(%s) failed: %s",
                       path_copy, strerror(-rc));
            return rc;
        }

        curr++;
    }

    /* finally create last level of dir */
    DisplayLog(LVL_FULL, MKDIR_TAG, "mkdir(%s)", full_path);
    if ((mkdir(full_path, mode) != 0) && (errno != EEXIST)) {
        rc = -errno;
        DisplayLog(LVL_CRIT, MKDIR_TAG, "mkdir(%s) failed: %s", full_path,
                   strerror(-rc));
        return rc;
    } else if (errno == EEXIST)
        exists = 1;

 get_id:
    /* must return directory id */
    if (dir_id) {
        rc = path2id(full_path, dir_id, NULL);
        if (rc)
            return rc;
    }

    if (exists)
        return -EEXIST;
    else
        return 0;
}

/** create parent directory, and return its id (even if it already exists) */
int create_parent_of(const char *child_path, entry_id_t *p_parent_id)
{
    char tmp[RBH_PATH_MAX];
    char *destdir;

    /* copy to tmp buffer as dirname modifies its argument */
    strcpy(tmp, child_path);
    /* extract parent dir path */
    destdir = dirname(tmp);
    if (destdir == NULL) {
        DisplayLog(LVL_CRIT, MKDIR_TAG,
                   "Error extracting directory path of '%s'", child_path);
        return -EINVAL;
    }

    /* create the directory */
    return mkdir_recurse(destdir, 0750, p_parent_id);
}

#define CREAT_TAG "Create"
/* create an object with the given attributes */
int create_from_attrs(const attr_set_t *attrs_in,
                      attr_set_t *attrs_out,
                      entry_id_t *new_id, bool overwrite, bool setstripe)
{
    char link[RBH_PATH_MAX] = "";
    const char *fspath;
    int rc;
    struct stat st_dest;
    int fd;
    mode_t mode_create = 0;
    bool set_mode = false;

    if (!ATTR_MASK_TEST(attrs_in, fullpath)
        || !ATTR_MASK_TEST(attrs_in, type)) {
        DisplayLog(LVL_MAJOR, CREAT_TAG,
                   "Missing mandatory attribute to create entry");
        return -EINVAL;
    }
    fspath = ATTR(attrs_in, fullpath);

    /* initialize out attrs */
    ATTR_MASK_INIT(attrs_out);

    /* first create parent and retrieve parent id */
    rc = create_parent_of(fspath, &ATTR(attrs_out, parent_id));
    if (rc != 0 && rc != -EEXIST)
        return rc;
    else
        ATTR_MASK_SET(attrs_out, parent_id);

    if (!strcasecmp(ATTR(attrs_in, type), STR_TYPE_DIR)) {
        /* entry is a directory */
        if (ATTR_MASK_TEST(attrs_in, mode))
            mode_create = ATTR(attrs_in, mode);
        else
            mode_create = 750;

        /* then create the directory itself */
        rc = mkdir(fspath, mode_create) ? -errno : 0;
        if (rc != 0 && rc != -EEXIST)
            return rc;
        else if (rc == -EEXIST)
            set_mode = true;
    } else if (!strcasecmp(ATTR(attrs_in, type), STR_TYPE_LINK)) {
        /* entry is a symlink */

        if (!ATTR_MASK_TEST(attrs_in, link)) {
            DisplayLog(LVL_MAJOR, CREAT_TAG,
                       "Missing mandatory attribute 'link' to create link");
            return -EINVAL;
        }

        if (symlink(ATTR(attrs_in, link), fspath) != 0) {
            rc = -errno;
            DisplayLog(LVL_MAJOR, CREAT_TAG,
                       "Error creating symlink %s->\"%s\" in filesystem: %s",
                       fspath, link, strerror(-rc));
            return rc;
        }
        /* can't set mode on a symlink */
    } else if (!strcasecmp(ATTR(attrs_in, type), STR_TYPE_FILE)) {
        int created = false;

        if (ATTR_MASK_TEST(attrs_in, mode))
            mode_create = ATTR(attrs_in, mode);
        else
            mode_create = 0640; /* default */

#ifdef _LUSTRE
        if (setstripe) {
            /* create the file with the appropriate stripe in Lustre */
            if (ATTR_MASK_TEST(attrs_in, stripe_info)) {
                rc = CreateStriped(fspath, &ATTR(attrs_in, stripe_info),
                                   overwrite);
                if (rc == 0 || rc == -EEXIST) {
                    created = true;
                    set_mode = true;
                } else
                    DisplayLog(LVL_MAJOR, CREAT_TAG,
                               "setstripe failed: trying to create file with default striping");
            }
        } else {
            /* create with no stripe */
            rc = CreateWithoutStripe(fspath, mode_create & 07777, overwrite);
            if (rc == 0) {
                created = true;
                set_mode = false;
            } else if (rc == -EEXIST) {
                created = true;
                set_mode = true;
            } else
                DisplayLog(LVL_MAJOR, CREAT_TAG,
                           "create(O_LOV_DELAY_CREATE) failed: trying to create file with default striping");
        }
#endif
        if (!created) {
            fd = creat(fspath, mode_create & 07777);
            if (fd < 0) {
                rc = -errno;
                DisplayLog(LVL_CRIT, CREAT_TAG,
                           "ERROR: couldn't create '%s': %s", fspath,
                           strerror(-rc));
                return rc;
            } else
                close(fd);
        }

        /* set times */
        if (ATTR_MASK_TEST(attrs_in, last_mod)) {
            struct utimbuf utb;
            utb.modtime = ATTR(attrs_in, last_mod);

            if (ATTR_MASK_TEST(attrs_in, last_access))
                utb.actime = ATTR(attrs_in, last_access);
            else
                utb.actime = utb.modtime;

            /* set the same mtime as in the DB */
            DisplayLog(LVL_FULL, CREAT_TAG,
                       "Restoring times for '%s': atime=%lu, mtime=%lu", fspath,
                       utb.actime, utb.modtime);
            if (utime(fspath, &utb))
                DisplayLog(LVL_MAJOR, CREAT_TAG,
                           "Warning: couldn't restore times for '%s': %s",
                           fspath, strerror(errno));
        }
    } else {
        /* type not supported */
        DisplayLog(LVL_CRIT, CREAT_TAG,
                   "Error: cannot restore entries of type '%s' (%s)",
                   ATTR(attrs_in, type), fspath);
        return -ENOTSUP;
    }

    /* set owner, group */
    if (ATTR_MASK_TEST(attrs_in, uid) || ATTR_MASK_TEST(attrs_in, gid)) {
        uid_t uid = -1;
        gid_t gid = -1;
        char buff[4096];

        if (ATTR_MASK_TEST(attrs_in, uid)) {
            if (global_config.uid_gid_as_numbers) {
                uid = ATTR(attrs_in, uid).num;
            } else {
                struct passwd pw;
                struct passwd *p_pw;

                if ((getpwnam_r(ATTR(attrs_in, uid).txt, &pw, buff, 4096, &p_pw)
                     != 0)
                    || (p_pw == NULL)) {
                    DisplayLog(LVL_MAJOR, CREAT_TAG,
                               "Warning: couldn't resolve uid for user '%s'",
                               ATTR(attrs_in, uid).txt);
                    uid = -1;
                } else
                    uid = p_pw->pw_uid;
            }
        }

        if (ATTR_MASK_TEST(attrs_in, gid)) {
            if (global_config.uid_gid_as_numbers) {
                gid = ATTR(attrs_in, gid).num;
            } else {
                struct group gr;
                struct group *p_gr;

                if ((getgrnam_r(ATTR(attrs_in, gid).txt, &gr, buff, 4096, &p_gr)
                     != 0)
                    || (p_gr == NULL)) {
                    DisplayLog(LVL_MAJOR, CREAT_TAG,
                               "Warning: couldn't resolve gid for group '%s'",
                               ATTR(attrs_in, gid).txt);
                    gid = -1;
                } else
                    gid = p_gr->gr_gid;
            }
        }

        DisplayLog(LVL_FULL, CREAT_TAG,
                   "Restoring owner/group for '%s': uid=%u, gid=%u", fspath,
                   uid, gid);

        if (lchown(fspath, uid, gid)) {
            rc = -errno;
            DisplayLog(LVL_MAJOR, CREAT_TAG,
                       "Warning: cannot set owner/group for '%s': %s", fspath,
                       strerror(-rc));
        } else {
            /* According to chown(2) manual: chown may clear sticky bits even
             * if root does it, so, we must set the mode again if it contains
             * special bits */
            if (!set_mode && (mode_create & 07000))
                set_mode = true;
        }
    }

    if (set_mode) {
        /* set the same mode as in the backend */
        DisplayLog(LVL_FULL, CREAT_TAG, "Restoring mode for '%s': mode=%#o",
                   fspath, mode_create & 07777);
        if (chmod(fspath, mode_create & 07777))
            DisplayLog(LVL_MAJOR, CREAT_TAG,
                       "Warning: couldn't restore mode for '%s': %s", fspath,
                       strerror(errno));
    }

    if (lstat(fspath, &st_dest)) {
        rc = -errno;
        DisplayLog(LVL_CRIT, CREAT_TAG,
                   "ERROR: lstat() failed on restored entry '%s': %s", fspath,
                   strerror(-rc));
        return rc;
    }

    rc = path2id(fspath, new_id, &st_dest);
    if (rc)
        return rc;

    /* update with the new attributes */
    stat2rbh_attrs(&st_dest, attrs_out, true);

    /* copy missing info: path, name, link, ... */
    strcpy(ATTR(attrs_out, fullpath), fspath);
    ATTR_MASK_SET(attrs_out, fullpath);

    char *name = strrchr(fspath, '/');
    if (name) {
        name++;
        strcpy(ATTR(attrs_out, name), name);
        ATTR_MASK_SET(attrs_out, name);
    }
    ATTR(attrs_out, path_update) = time(NULL);
    ATTR_MASK_SET(attrs_out, path_update);
    ATTR(attrs_out, md_update) = time(NULL);
    ATTR_MASK_SET(attrs_out, md_update);

    if (S_ISLNK(st_dest.st_mode)) {
        strcpy(ATTR(attrs_out, link), link);
        ATTR_MASK_SET(attrs_out, link);
    }
#ifdef _LUSTRE
    /* get new stripe */
    if (S_ISREG(st_dest.st_mode)) {
        /* get the new stripe info */
        if (File_GetStripeByPath(fspath,
                                 &ATTR(attrs_out, stripe_info),
                                 &ATTR(attrs_out, stripe_items)) == 0) {
            ATTR_MASK_SET(attrs_out, stripe_info);
            ATTR_MASK_SET(attrs_out, stripe_items);
        }
    }
#endif
    return 0;
}

enum path_check_return path_check_update(const entry_id_t *p_id,
                                         const char *fid_path,
                                         attr_set_t *p_attrs,
                                         attr_mask_t attr_mask)
{
#ifndef _HAVE_FID
    return PCR_NO_CHANGE;
#else
    int rc;
    bool updated = false;

    if (attr_mask.std & (ATTR_MASK_name | ATTR_MASK_parent_id)) {
        rc = Lustre_GetNameParent(fid_path, 0, &ATTR(p_attrs, parent_id),
                                  ATTR(p_attrs, name), RBH_NAME_MAX);
        if (rc == 0) {
            ATTR_MASK_SET(p_attrs, name);
            ATTR_MASK_SET(p_attrs, parent_id);
            /* update path refresh time */
            ATTR_MASK_SET(p_attrs, path_update);
            ATTR(p_attrs, path_update) = time(NULL);
            updated = true;
        } else if (rc == -ENODATA) {
            /* Entry has no path in namespace. It is likely a volatile,
             * and should be ignored. */
            DisplayLog(LVL_DEBUG, "PatchCheck", "Entry "DFID" has no path. "
                       "It is likely a volatile", PFID(p_id));
            return PCR_ORPHAN;
        } else if (rc != -ENOENT) {
            DisplayLog(LVL_MAJOR, "PathCheck",
                       "Failed to get parent+name for " DFID ": %s", PFID(p_id),
                       strerror(-rc));
        }
    }

    /* if fullpath is in the policy, get the fullpath */
    if (attr_mask.std & ATTR_MASK_fullpath) {
        rc = Lustre_GetFullPath(p_id, ATTR(p_attrs, fullpath), RBH_PATH_MAX);
        if (rc == 0) {
            ATTR_MASK_SET(p_attrs, fullpath);
            updated = true;
        } else if (rc != -ENOENT) {
            DisplayLog(LVL_MAJOR, "PathCheck",
                       "Failed to retrieve fullpath for " DFID ": %s",
                       PFID(p_id), strerror(-rc));
        }
    }
    return updated ? PCR_UPDATED : PCR_NO_CHANGE;
#endif
}

/* Find the numerical user ID (UID) for a given user name, which is
 * either a real name or a string containing a number.
 * Return 0 on success, and non-zero on error. */
int set_uid_val(const char *username, db_type_u *val)
{
    long uid;
    char *endptr;

    if (!global_config.uid_gid_as_numbers) {
        val->val_str = username;
        return 0;
    }

    if (WILDCARDS_IN(username)) {
        DisplayLog(LVL_CRIT, __func__,
                   "ERROR: Wilcards not allowed in user name");
        return -1;
    }

    /* The name could be a number already. */
    errno = 0;
    uid = strtol(username, &endptr, 0);

    if ((errno == ERANGE && (uid == LONG_MAX || uid == LONG_MIN)) ||
        (errno != 0 && uid == 0) || endptr == username) {
        /* Not a number. */
        struct passwd pw;
        struct passwd *result;
        char buff[4096];

        if (getpwnam_r(username, &pw, buff, sizeof(buff), &result) == 0) {
            val->val_int = pw.pw_uid;
            return 0;
        } else {
            DisplayLog(LVL_CRIT, __func__, "couldn't resolve uid for user '%s'",
                       username);
            return -1;
        }
    }

    if (uid < 0) {
        DisplayLog(LVL_CRIT, __func__,
                   "ERROR: Given UID is negative (%ld)", uid);
        return -1;
    }

    if (uid > UINT_MAX) {
        DisplayLog(LVL_CRIT, __func__,
                   "ERROR: Given UID is too big (%ld)", uid);
        return -1;
    }

    val->val_int = uid;
    return 0;
}

/* Find the numerical group ID (GID) for a given group name, which is
 * either a real name or a string containing a number.
 * Return 0 on success, and non-zero on error. */
int set_gid_val(const char *groupname, db_type_u *val)
{
    long gid;
    char *endptr;

    if (!global_config.uid_gid_as_numbers) {
        val->val_str = groupname;
        return 0;
    }

    if (WILDCARDS_IN(groupname)) {
        DisplayLog(LVL_CRIT, __func__,
                   "ERROR: Wilcards not allowed in group name");
        return -1;
    }

    /* The name could be a number already. */
    errno = 0;
    gid = strtol(groupname, &endptr, 0);

    if ((errno == ERANGE && (gid == LONG_MAX || gid == LONG_MIN)) ||
        (errno != 0 && gid == 0) || endptr == groupname) {
        /* Not a number. */
        struct group grp;
        struct group *result;
        char buff[4096];

        if (getgrnam_r(groupname, &grp, buff, sizeof(buff), &result) == 0) {
            val->val_int = grp.gr_gid;
            return 0;
        } else {
            DisplayLog(LVL_CRIT, __func__,
                       "couldn't resolve gid for group '%s'", groupname);
            return -1;
        }
    }

    if (gid < 0) {
        DisplayLog(LVL_CRIT, __func__,
                   "ERROR: Given GID is negative (%ld)", gid);
        return -1;
    }

    if (gid > UINT_MAX) {
        DisplayLog(LVL_CRIT, __func__,
                   "ERROR: Given GID is too big (%ld)", gid);
        return -1;
    }

    val->val_int = gid;
    return 0;
}

/* Returns a printable string for a UID or GID, whether it's a number
 * or an actual string. */
const char *id_as_str(db_type_u *val)
{
    static __thread char buf[20];

    if (!global_config.uid_gid_as_numbers)
        return val->val_str;

    sprintf(buf, "%d", val->val_int);
    return buf;
}
