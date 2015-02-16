/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2010 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

/**
 * \file   backend_basic.c
 * \author Th. Leibovici
 * \brief  basic backend implementation
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "backend_ext.h"
#include "backend_mgr.h"
#include "RobinhoodLogs.h"
#include "RobinhoodMisc.h"
#include "global_config.h"
#include "xplatform_print.h"
#include "Memory.h"
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


#ifdef HAVE_PURGE_POLICY
#ifdef HAVE_SHOOK
#include <shook_svr.h>
#endif
#endif


#define RBHEXT_TAG "Backend"

/**
 * Get compatibility information,
 * to check compatibility with the current FS.
 */
int rbhext_compat_flags()
{
    int compat_flags = 0;

    /* if entry id is fid, this module is only compatible with Lustre filesystems */
#ifdef _HAVE_FID
    compat_flags |= RBHEXT_COMPAT_LUSTRE;
#endif
    return compat_flags;
}

static backend_config_t config;
static dev_t backend_dev = 0;
static char  backend_name[RBH_PATH_MAX] = "";

/* is it a special shell character */
static inline int is_shell_special(char c)
{
    static const char * specials = "`#$*?!|;&<>[]{}'\"\\";
    const char * curr;
    for (curr = specials; (*curr) != '\0'; curr++)
        if (c == (*curr))
            return TRUE;
    /* not found */
    return FALSE;
}

#define is_allowed_char(_c) (isprint(_c) && !isspace(_c) && !is_shell_special(_c))

/* clean non printable characters, spaces, special chars, ... */
static void clean_bad_chars(char * path)
{
    char * curr;
    for ( curr = path; *curr != '\0'; curr++ )
    {
        if ( !is_allowed_char(*curr) )
            *curr = '_';
    }
}

#ifdef HAVE_SHOOK
static char lock_dirname[RBH_NAME_MAX] = "";
static char restripe_dirname[RBH_NAME_MAX] = "";
#endif

/**
 * Initialize the extension module.
 * \param[in] config_string specific config string (e.g path to config file...)
 * \param[out] p_behaviors_flags pointer to output mask that describes extension behavior
 */
int rbhext_init( const backend_config_t * conf,
                 unsigned int * p_behaviors_flags )
{
    int rc;

    config = *conf;

    /* synchronous archiving and rm support */
    *p_behaviors_flags = RBHEXT_SYNC_ARCHIVE | RBHEXT_RM_SUPPORT;

#ifdef HAVE_PURGE_POLICY
    *p_behaviors_flags |= RBHEXT_RELEASE_SUPPORT;
#endif

#ifdef HAVE_SHOOK
    rc = shook_svr_init(config.shook_cfg);
    if (rc)
    {
        DisplayLog( LVL_CRIT, RBHEXT_TAG, "ERROR %d initializing shook server library",
                    rc );
        return rc;
    }

    strcpy(lock_dirname, basename(LOCK_DIR));
    strcpy(restripe_dirname, basename(RESTRIPE_DIR));
#endif

    /* check that backend filesystem is mounted */
    rc = CheckFSInfo( config.root, config.mnt_type, &backend_dev, backend_name,
                      config.check_mounted, FALSE );
    if ( rc )
        return -rc;

    return 0;
}


/* return the mask of needed attributes to know if an entry is to be ignored */
int rbhext_ignore_need()
{
    return ATTR_MASK_fullpath | ATTR_MASK_name | ATTR_MASK_parent_id | ATTR_MASK_type;
}

int rbhext_ignore(const entry_id_t *p_id, attr_set_t *p_attrs)
{
#ifndef HAVE_SHOOK
    return 0;
#else
    /* if we don't know the full path, but the name looks like
     * an ignored entry, get the path */
    if (!ATTR_MASK_TEST(p_attrs, fullpath)
        && ATTR_MASK_TEST(p_attrs, name))
    {
        if (!strcmp(ATTR(p_attrs, name), SHOOK_DIR)
            || !strcmp(ATTR(p_attrs, name), lock_dirname)
            || !strcmp(ATTR(p_attrs, name), restripe_dirname)
            || !strncmp(SHOOK_LOCK_PREFIX, ATTR(p_attrs, name), strlen(SHOOK_LOCK_PREFIX))
            || !strncmp(RESTRIPE_SRC_PREFIX, ATTR(p_attrs, name), strlen(RESTRIPE_SRC_PREFIX))
            || !strncmp(RESTRIPE_TGT_PREFIX, ATTR(p_attrs, name), strlen(RESTRIPE_TGT_PREFIX)))
        {
            if (Lustre_GetFullPath(p_id, ATTR(p_attrs,fullpath), RBH_PATH_MAX) != 0)
                /* ignore, by default */
                return TRUE;
            else
                /* continue with path checking */
                ATTR_MASK_SET(p_attrs, fullpath);
        }
        else /* no possible match */
            return FALSE;
    }

    if (ATTR_MASK_TEST(p_attrs, fullpath))
    {
        /* check lock file */
        if (!fnmatch("*/"LOCK_DIR"/"SHOOK_LOCK_PREFIX"*", ATTR(p_attrs, fullpath ), 0))
        {
            /* skip the entry */
            DisplayLog(LVL_DEBUG, RBHEXT_TAG, "%s is a shook lock",
                       ATTR(p_attrs, fullpath));
            /** @TODO raise special event for the file: LOCK/UNLOCK */
            return TRUE;
        }
        /* check lock dir */
        else if (!fnmatch("*/"LOCK_DIR, ATTR(p_attrs, fullpath ), 0))
        {
            /* skip the entry */
            DisplayLog(LVL_DEBUG, RBHEXT_TAG, "%s is a shook lock dir",
                       ATTR(p_attrs, fullpath));
            return TRUE;
        }
        /* check restripe dir */
        else if (!fnmatch("*/"RESTRIPE_DIR, ATTR(p_attrs, fullpath ), 0))
        {
            /* skip the entry */
            DisplayLog(LVL_DEBUG, RBHEXT_TAG, "%s is a shook restripe dir",
                       ATTR(p_attrs, fullpath));
            return TRUE;
        }
    }

    /* match '.shook' directory */
    if (p_attrs && ATTR_MASK_TEST( p_attrs, name )
        && ATTR_MASK_TEST( p_attrs, type ))
    {
        if ( !strcmp(STR_TYPE_DIR, ATTR(p_attrs, type)) &&
             !strcmp(SHOOK_DIR, ATTR(p_attrs, name)) )
        {
            /* skip the entry */
            DisplayLog(LVL_DEBUG, RBHEXT_TAG, "\"%s\" is a shook dir",
                       ATTR(p_attrs, name));
            return TRUE;
        }
    }

    /* if the removed entry is a restripe source,
     * we MUST NOT remove the backend entry
     * as it will be linked to the restripe target
     */
    if ( (ATTR_MASK_TEST(p_attrs, fullpath)
               && !fnmatch("*/"RESTRIPE_DIR"/"RESTRIPE_SRC_PREFIX"*",
                     ATTR(p_attrs, fullpath), 0))
        ||
        (ATTR_MASK_TEST(p_attrs, name)
         && !strncmp(RESTRIPE_SRC_PREFIX, ATTR(p_attrs, name),
                     strlen(RESTRIPE_SRC_PREFIX))))
    {
        DisplayLog( LVL_DEBUG, RBHEXT_TAG, "Removing shook stripe source %s: no removal in backend!",
                    ATTR_MASK_TEST(p_attrs, fullpath)?
                    ATTR(p_attrs, fullpath) : ATTR(p_attrs, name));
        return TRUE;
    }

    return FALSE;
#endif
}

/**
 * Determine attributes to be provided for rbhext_get_status().
 * \param[in] entry_type type of entry to check status.
 * \param[out] p_attr_allow_cached list of attributes needed for determining status
 *                                 that can be retrieved from DB (cached)
 * \param[out] p_attr_need_fresh list of attributes needed for determining status
 *                                 that need to be up-to-date.
 * \retval 0 on success
 * \retval <0 on error
 * \retval -ENOTSUP backup is not implemented for this type of entry.
 */
int rbhext_status_needs( obj_type_t   entry_type,
                         unsigned int * p_attr_allow_cached,
                         unsigned int * p_attr_need_fresh )
{
    *p_attr_allow_cached = 0;
    *p_attr_need_fresh = 0;

    /* support symlinks? */
    if ((entry_type == TYPE_LINK) &&
        !config.archive_symlinks)
        return -ENOTSUP;
    else if ((entry_type != TYPE_FILE)
         && (entry_type != TYPE_LINK)
         && (entry_type != TYPE_NONE))
        /* support other types? */
        return -ENOTSUP;

    /* type is useful in any case (does not change during entry lifetime,
     * so we can use a cached value). */
    (*p_attr_allow_cached) |= ATTR_MASK_type;

    /* Previous backup path is also needed.
     * it is only from DB (so it is a cached information). */
    (*p_attr_allow_cached) |= ATTR_MASK_backendpath;
    (*p_attr_allow_cached) |= ATTR_MASK_last_archive;

    /* needs fresh mtime/size information from lustre
     * to determine if the entry changed */
    (*p_attr_need_fresh) |= ATTR_MASK_last_mod;
    (*p_attr_need_fresh) |= ATTR_MASK_size;

#ifndef _HAVE_FID
    /* for lustre<2.0, need fresh entry path */
    (*p_attr_need_fresh) |= ATTR_MASK_fullpath;
#else
    /* just needed to have human readable backend path */
    (*p_attr_allow_cached) |= ATTR_MASK_fullpath;
#endif
    return 0;
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
static void entry2backend_path( const entry_id_t * p_id,
                               const attr_set_t * p_attrs_in,
                               what_for_e what_for,
                               char * backend_path,
                               int allow_compress)
{
    int pathlen;
    char rel_path[RBH_PATH_MAX];

    if ( ATTR_MASK_TEST(p_attrs_in, backendpath) )
    {
       DisplayLog( LVL_DEBUG, RBHEXT_TAG, "%s: previous backend_path: %s",
                   (what_for == FOR_LOOKUP)?"LOOKUP":"NEW_COPY",
                   ATTR(p_attrs_in, backendpath) );
    }
    else if (ATTR_MASK_TEST(p_attrs_in, type) &&
             !strcasecmp(ATTR(p_attrs_in, type), STR_TYPE_DIR))
    {
        if (ATTR_MASK_TEST(p_attrs_in, fullpath) &&
            ATTR(p_attrs_in, fullpath)[0] == '/' &&
            relative_path(ATTR(p_attrs_in, fullpath), global_config.fs_path,
                          rel_path ) == 0)
        {
            DisplayLog(LVL_DEBUG, RBHEXT_TAG, "%s is a directory: backend path is the same",
                       ATTR(p_attrs_in, fullpath));

            if (!strcmp(config.root, "/")) /* root is '/' */
                sprintf(backend_path, "/%s", rel_path);
            else
                sprintf(backend_path, "%s/%s", config.root, rel_path);
        }
        else /* we don't have fullpath available */
        {
            const char * fname;

            /* There is something in the fullpath, but it is not under FS root
             * or it is relative */
            if (ATTR_MASK_TEST(p_attrs_in, fullpath))
            {
                if (ATTR(p_attrs_in, fullpath)[0] == '/')
                    fname = ATTR(p_attrs_in, fullpath) + 1;
                else
                    fname = ATTR(p_attrs_in, fullpath);
            }
            else if (ATTR_MASK_TEST(p_attrs_in, name))
                fname = ATTR(p_attrs_in, name);
            else
                fname = UNK_NAME;

            /* backup entry to a special dir */
            if ( !strcmp(config.root, "/") ) /* root is '/' */
                sprintf(backend_path, "/%s/%s", UNK_PATH, fname);
            else
                sprintf(backend_path, "%s/%s/%s", config.root, UNK_PATH, fname );
        }

        /* clean bad characters */
        clean_bad_chars(backend_path);
        return;
    }
#ifdef HAVE_SHOOK
    else
    {
        int rc;
        char fidpath[RBH_PATH_MAX];

        BuildFidPath( p_id, fidpath );

        /* retrieve backend path from shook xattrs */
        rc = shook_get_hsm_info(fidpath, backend_path, NULL);
        if ((rc == 0) && !EMPTY_STRING(backend_path))
            return;
    }
#endif

    if ( (what_for == FOR_LOOKUP) && ATTR_MASK_TEST(p_attrs_in, backendpath) )
    {
        /* For lookup, if there is a previous path in the backend, use it. */
        strcpy(backend_path, ATTR(p_attrs_in, backendpath));
    }
    else /* in any other case, build a path from scratch */
    {
        /* if the fullpath is available, build human readable path */
        if ( ATTR_MASK_TEST(p_attrs_in, fullpath) &&
             ATTR(p_attrs_in, fullpath)[0] == '/' &&
             relative_path( ATTR(p_attrs_in, fullpath), global_config.fs_path,
                            rel_path ) == 0 )
        {
            /* backend path is '<bakend_root>/<rel_path>' */

            if ( !strcmp(config.root, "/") ) /* root is '/' */
                sprintf(backend_path, "/%s", rel_path);
            else
                sprintf(backend_path, "%s/%s", config.root, rel_path);
        }
        else /* we don't have fullpath available (or not in FS root, or relative) */
        {
            const char * fname;

            /* There is something in the fullpath, but it is not under FS root
             * or it is relative */
            if (ATTR_MASK_TEST(p_attrs_in, fullpath))
            {
                if (ATTR(p_attrs_in, fullpath)[0] == '/')
                    fname = ATTR(p_attrs_in, fullpath) + 1;
                else
                    fname = ATTR(p_attrs_in, fullpath);
            }
            else if (ATTR_MASK_TEST(p_attrs_in, name))
                fname = ATTR(p_attrs_in, name);
            else
                fname = UNK_NAME;

            /* backup entry to a special dir */
            if ( !strcmp(config.root, "/") ) /* root is '/' */
                sprintf(backend_path, "/%s/%s", UNK_PATH, fname);
            else
                sprintf(backend_path, "%s/%s/%s", config.root, UNK_PATH, fname );
        }

        /* clean bad characters */
        clean_bad_chars(backend_path);

        /* add __<id> after the name */
        pathlen = strlen(backend_path);
#ifdef  _HAVE_FID
        sprintf( backend_path + pathlen, "__"DFID_NOBRACE, PFID(p_id) );
#else
        sprintf( backend_path + pathlen, "__%#LX:%#LX",
                 (unsigned long long)p_id->device,
                 (unsigned long long)p_id->inode );
#endif
        /* check if compression is enabled and if the entry is a file */
        if (allow_compress && !strcasecmp(ATTR(p_attrs_in, type), STR_TYPE_FILE))
        {
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
static int entry_is_archiving(const char * backend_path )
{
    char xfer_path[RBH_PATH_MAX];
    struct stat cp_md;
    int rc;
    sprintf(xfer_path, "%s.%s", backend_path, COPY_EXT );

    if ( lstat(xfer_path, &cp_md ) != 0 )
    {
        rc = -errno;
        if ( (rc == -ENOENT) || (rc == -ESTALE) )
            return 0;
        else
            return rc;
    }
    /* xfer is running. return last action time */
    return MAX3( cp_md.st_mtime, cp_md.st_ctime, cp_md.st_atime );
}

/**
 * Cleans a timed-out transfer
 */
static int transfer_cleanup(const char * backend_path)
{
    char xfer_path[RBH_PATH_MAX];
    int rc;
    sprintf(xfer_path, "%s.%s", backend_path, COPY_EXT );

    if ( unlink(xfer_path) != 0 )
    {
        rc = -errno;
        return rc;
    }
    return 0;
}

/**
 * Move an orphan file to orphan directory
 */
static int move_orphan(const char * path)
{
    char dest[RBH_PATH_MAX];
    char tmp[RBH_PATH_MAX];
    char * fname;
    int rc;

    /* does the trash directory exist? */
    sprintf( dest, "%s/%s", config.root, TRASH_DIR );
    if ( (mkdir(dest, 0750) != 0) && (errno != EEXIST) )
    {
        rc = -errno;
        DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Error creating directory %s: %s",
                    dest, strerror(-rc) );
        return rc;
    }

    strcpy(tmp, path);
    fname = basename(tmp);
    if ( fname == NULL || (strcmp(fname, "/") == 0) || EMPTY_STRING(fname) )
    {
        DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Invalid path '%s'",
                    path );
        return -EINVAL;
    }
    /* move the orphan to the directory */
    sprintf( dest, "%s/%s/%s", config.root, TRASH_DIR, fname );

    if ( rename(path, dest) != 0 )
    {
        rc = -errno;
        DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Error moving '%s' to '%s'",
                    path, dest );
        return rc;
    }

    DisplayLog( LVL_EVENT, RBHEXT_TAG, "'%s' moved to '%s'",
                path, dest );
    return 0;
}


/* check if there is a running copy and if it timed-out
 * return <0 on error
 * 0 if no copy is running
 * 1 if a copy is already running
 * */
static int check_running_copy(const char * bkpath)
{
    int rc;
    /* is a copy running for this entry? */
    rc = entry_is_archiving( bkpath );
    if ( rc < 0 )
    {
        DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Error %d checking if copy is running for %s: %s",
                    rc, bkpath, strerror(-rc) );
        return rc;
    }
    else if ( rc > 0 )
    {
        if ( config.copy_timeout && ( time(NULL) - rc > config.copy_timeout ))
        {
            DisplayLog( LVL_EVENT, RBHEXT_TAG, "Copy timed out for %s (inactive for %us)",
                        bkpath, (unsigned int)(time(NULL) - rc) );
            /* previous copy timed out: clean it */
            transfer_cleanup( bkpath );
        }
        else
        {
            DisplayLog( LVL_DEBUG, RBHEXT_TAG,
                        "'%s' is being archived (last mod: %us ago)",
                        bkpath, (unsigned int)(time(NULL) - rc) );
            return 1;
        }
    }
    return 0;
}

/* get entry info from the backend (like lstat), but also check if the entry is compressed.
 * prioritarily check the entry with the selected compression on/off.
 */
static int bk_lstat(const char *bkpath, struct stat *bkmd, int check_compressed, int *compressed)
{
    char tmp[RBH_PATH_MAX];
    int len = strlen(bkpath);
    *compressed = (bkpath[len - 1] == 'z');

    if (!check_compressed) /* not a file, call standard lstat() */
        return lstat(bkpath, bkmd);

    if (!lstat(bkpath, bkmd))
        return 0;

    if ((errno == ENOENT) || (errno == ESTALE))
    {
        if (*compressed)
        {
            /* try without compression */
            strcpy(tmp, bkpath);
            tmp[len - 1] = '\0';

            if (lstat(tmp, bkmd) == 0)
            {
                *compressed = 0;
                return 0;
            }
        }
        else if (!(*compressed))
        {
            /* try with compression */
            sprintf(tmp, "%sz", bkpath);
            if (lstat(tmp, bkmd) == 0)
            {
                *compressed = 1;
                return 0;
            }
        }
    }
    return -1;
}


/**
 * Get the status for an entry.
 * \param[in] p_id pointer to entry id
 * \param[in] p_attrs_in pointer to entry attributes
 * \param[out] p_attrs_changed changed/retrieved attributes
 */
int rbhext_get_status( const entry_id_t * p_id,
                       const attr_set_t * p_attrs_in,
                       attr_set_t * p_attrs_changed )
{
    int rc;
    struct stat bkmd;
    obj_type_t entry_type;
    char bkpath[RBH_PATH_MAX];
    int compressed = 0;

    /* check if mtime is provided (mandatory) */
    if ( !ATTR_MASK_TEST(p_attrs_in, last_mod) || !ATTR_MASK_TEST(p_attrs_in, type) )
    {
        DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Missing mandatory attribute for checking entry status" );
        return -EINVAL;
    }

    /* path to lookup the entry in the backend */
    entry2backend_path(p_id, p_attrs_in, FOR_LOOKUP, bkpath, config.compress);

    /* is the entry has a supported type? */
    entry_type = ListMgr2PolicyType(ATTR(p_attrs_in, type));

    if ((entry_type == TYPE_LINK) && !config.archive_symlinks)
    {
        DisplayLog(LVL_VERB, RBHEXT_TAG, "Symlink support is disabled in configuration");
        return -ENOTSUP;
    }
    else if ((entry_type != TYPE_FILE) && (entry_type != TYPE_LINK))
    {
        DisplayLog( LVL_VERB, RBHEXT_TAG, "Unsupported type %s for this backend",
                    ATTR(p_attrs_in, type) );
        return -ENOTSUP;
    }

#ifdef HAVE_PURGE_POLICY
#ifdef HAVE_SHOOK
    /* @TODO: ignore shook special entries */

    /* check status from libshook.
     * return if status != ONLINE
     * else, continue checking.
     */
    char fidpath[RBH_PATH_MAX];
    file_status_t status;

    BuildFidPath( p_id, fidpath );

    rc = ShookGetStatus( fidpath, &status );
    if (rc)
        return rc;

    /* if status is 'release_pending' or 'restore_running',
     * check timeout. */
    if (status == STATUS_RELEASE_PENDING || status == STATUS_RESTORE_RUNNING)
    {
        rc = ShookRecoverById(p_id, &status);
        if (rc < 0)
            return rc;
    }

    if ( status != STATUS_SYNCHRO )
    {
        DisplayLog( LVL_FULL, RBHEXT_TAG, "shook reported status<>online: %d",
                    status );
        ATTR_MASK_SET( p_attrs_changed, status );
        ATTR( p_attrs_changed, status ) = status;

        /* set backend path if it is not known */
        if (!ATTR_MASK_TEST(p_attrs_in, backendpath)
            && !ATTR_MASK_TEST(p_attrs_changed, backendpath))
        {
            ATTR_MASK_SET(p_attrs_changed, backendpath);
            strcpy(ATTR(p_attrs_changed, backendpath), bkpath);
        }

        return 0;
    }
    /* else: must compare status with backend */
#else
    #error "Unexpected compilation case"
#endif
#endif

    if ( entry_type == TYPE_FILE )
    {
        /* is a copy running for this entry? */
        rc = check_running_copy(bkpath);
        if (rc < 0)
            return rc;
        else if (rc > 0)/* current archive */
        {
            ATTR_MASK_SET( p_attrs_changed, status );
            ATTR( p_attrs_changed, status ) = STATUS_ARCHIVE_RUNNING;
            return 0;
        }
    }

    /* get entry info */
    if (bk_lstat(bkpath, &bkmd, entry_type == TYPE_FILE, &compressed) != 0)
    {
        rc = -errno;
        if ( (rc != -ENOENT) && (rc != -ESTALE) )
        {
            DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Lookup error for path '%s': %s",
                        bkpath, strerror(-rc) );
            return rc;
        }
        else
        {
            DisplayLog( LVL_DEBUG, RBHEXT_TAG,
                        "'%s' does not exist in the backend (new entry): %s",
                        bkpath, strerror(-rc) );
            /* no entry in the backend: new entry */
            ATTR_MASK_SET( p_attrs_changed, status );
            ATTR( p_attrs_changed, status ) = STATUS_NEW;
            return 0;
        }
    }

    if ( entry_type == TYPE_FILE )
    {
        if ( !S_ISREG(bkmd.st_mode))
        {
            /* entry of invalid type */
            DisplayLog( LVL_MAJOR, RBHEXT_TAG,
                        "Different type in backend for entry %s. Moving it to orphan dir.",
                        bkpath );
            rc = move_orphan( bkpath );
            if (rc)
                return rc;
            ATTR_MASK_SET( p_attrs_changed, status );
            ATTR( p_attrs_changed, status ) = STATUS_NEW;
            return 0;
        }
        /* compare mtime and size to check if the entry changed */
        /* XXX consider it modified this even if mtime is smaller */
        if ( (ATTR( p_attrs_in, last_mod ) != bkmd.st_mtime )
             || ((ATTR( p_attrs_in, size ) != bkmd.st_size) && !compressed) )
        {
                /* display a warning if last_mod in FS < mtime in backend */
                if (ATTR( p_attrs_in, last_mod ) < bkmd.st_mtime)
                    DisplayLog(LVL_MAJOR, RBHEXT_TAG,
                               "Warning: mtime in filesystem < mtime in backend (%s)",
                               bkpath);

                ATTR_MASK_SET( p_attrs_changed, status );
                ATTR( p_attrs_changed, status ) = STATUS_MODIFIED;

                /* update path in the backend */
                ATTR_MASK_SET( p_attrs_changed, backendpath );
                strcpy( ATTR( p_attrs_changed, backendpath ), bkpath) ;
                return 0;
        }
        else
        {
                ATTR_MASK_SET( p_attrs_changed, status );
                ATTR( p_attrs_changed, status ) = STATUS_SYNCHRO;

                /* update path in the backend */
                ATTR_MASK_SET( p_attrs_changed, backendpath );
                strcpy( ATTR( p_attrs_changed, backendpath ), bkpath) ;
                return 0;
        }
    }
    else if ( entry_type == TYPE_LINK )
    {
        char lnk1[RBH_PATH_MAX];
        char lnk2[RBH_PATH_MAX];
        char fspath[RBH_PATH_MAX];

        if (!config.archive_symlinks)
            return -ENOTSUP;

        if ( !S_ISLNK(bkmd.st_mode))
        {
            DisplayLog( LVL_MAJOR, RBHEXT_TAG,
                        "Different type in backend for entry %s. Moving it to orphan dir.",
                        bkpath );
            rc = move_orphan( bkpath );
            if (rc)
                return rc;
            ATTR_MASK_SET( p_attrs_changed, status );
            ATTR( p_attrs_changed, status ) = STATUS_NEW;
            return 0;
        }

#ifdef _HAVE_FID
        /* for Lustre 2, use fid path so the operation is not disturbed by renames... */
        BuildFidPath( p_id, fspath );
#else
        /* we need the posix path */
        if ( !ATTR_MASK_TEST(p_attrs, fullpath) )
        {
            DisplayLog( LVL_CRIT, RBHEXT_TAG, "Error in %s(): path argument is mandatory for archive command",
                        __FUNCTION__ );
            return -EINVAL;
        }
        strcpy(fspath, ATTR(p_attrs, fullpath));
#endif

        /* compare symlink content */
        if ( (rc = readlink(bkpath, lnk1, RBH_PATH_MAX )) < 0 )
        {
            rc = -errno;
            if ( rc == -ENOENT )
            {
                /* entry disapeared */
                ATTR_MASK_SET( p_attrs_changed, status );
                ATTR( p_attrs_changed, status ) = STATUS_NEW;
                return 0;
            }
            else
                return rc;
        }
        lnk1[rc] = '\0';
        DisplayLog( LVL_FULL, RBHEXT_TAG, "backend symlink => %s", lnk1 );
        if ( (rc = readlink(fspath, lnk2, RBH_PATH_MAX )) < 0 )
        {
            rc = -errno;
            DisplayLog( LVL_EVENT, RBHEXT_TAG, "Error performing readlink(%s): %s",
                        fspath, strerror(-rc) );
            return rc;
        }
        lnk2[rc] = '\0';
        DisplayLog( LVL_FULL, RBHEXT_TAG, "FS symlink => %s", lnk2 );
        if ( strcmp(lnk1, lnk2) )
        {
            /* symlink content is different */
            ATTR_MASK_SET( p_attrs_changed, status );
            ATTR( p_attrs_changed, status ) = STATUS_MODIFIED;

            /* update path in the backend */
            ATTR_MASK_SET( p_attrs_changed, backendpath );
            strcpy( ATTR( p_attrs_changed, backendpath ), bkpath ) ;
            return 0;
        }
        else /* same content */
        {
            ATTR_MASK_SET( p_attrs_changed, status );
            ATTR( p_attrs_changed, status ) = STATUS_SYNCHRO;

            /* update path in the backend */
            ATTR_MASK_SET( p_attrs_changed, backendpath );
            strcpy( ATTR( p_attrs_changed, backendpath ), bkpath ) ;
            return 0;
        }
    }
    else
    {
        return -ENOTSUP;
    }

    /* TODO What about STATUS_REMOVED? */
}

typedef enum { TO_FS, TO_BACKEND } target_e;

/**
 * get metadata of a directory in filesystem or in backend
 * by target path
 */
static int get_orig_dir_md( const char * target_dir, struct stat * st,
                            target_e target )
{
    char rel_path[RBH_PATH_MAX];
    char orig_path[RBH_PATH_MAX];
    int rc;
    const char * dest_root;
    const char * src_root;

    if ( target == TO_BACKEND )
    {
        dest_root = config.root;
        src_root = global_config.fs_path;
    }
    else
    {
        dest_root = global_config.fs_path;
        src_root = config.root;
    }

    rc =  relative_path( target_dir, dest_root, rel_path );
    if (rc)
        return rc;

    /* orig path is '<fs_root>/<rel_path>' */
    sprintf(orig_path, "%s/%s", src_root, rel_path);

    DisplayLog( LVL_FULL, RBHEXT_TAG, "Target directory: %s, source directory: %s",
                target_dir, orig_path );

    if ( lstat(orig_path, st) )
    {
        rc = -errno;
        DisplayLog( LVL_DEBUG, RBHEXT_TAG, "Cannot stat %s: %s",
                    orig_path, strerror(-rc) );
        return rc;
    }
    else
        return 0;
}

/**
 *  Ensure POSIX directory exists
 */
static int mkdir_recurse_clone_attrs( const char * full_path, mode_t default_mode,
                          target_e target )
{
    char path_copy[MAXPATHLEN];
    const char * curr;
    struct stat st;
    mode_t mode;
    int rc;
    int setattrs = FALSE;

    /* to backend or the other way? */
    if ( target == TO_BACKEND )
    {
        if (strncmp(config.root,full_path, strlen(config.root)) != 0)
        {
            DisplayLog(LVL_MAJOR, RBHEXT_TAG, "Error: '%s' is not under backend root '%s'",
                       full_path, config.root);
            return -EINVAL;
        }
        /* skip backend root */
        curr = full_path + strlen(config.root);
    }
    else
    {
        /* is it relative? */
        if (!EMPTY_STRING(full_path) && (full_path[0] != '/'))
        {
            curr = full_path;
            goto relative;
        }
        else if (strncmp(global_config.fs_path,full_path, strlen(global_config.fs_path)) != 0)
        {
            DisplayLog(LVL_MAJOR, RBHEXT_TAG, "Error: '%s' is not under filesystem root '%s'",
                       full_path, global_config.fs_path);
            return -EINVAL;
        }
        /* skip fs root */
        curr = full_path + strlen(global_config.fs_path);
    }

    if (*curr == '\0') /* full_path is root dir */
        return 0;
    else if (*curr != '/') /* slash expected */
    {
        DisplayLog(LVL_MAJOR, RBHEXT_TAG, "Error: '%s' is not under backend root '%s'",
                   full_path, (target == TO_BACKEND)?config.root:global_config.fs_path);
        return -EINVAL;
    }

    /* skip first slash */
    curr ++;
relative:

    while( (curr = strchr( curr, '/' )) != NULL )
    {
         /* if fullpath = '/a/b',
         * curr = &(fullpath[2]);
         * so, copy 2 chars to get '/a'.
         * and set fullpath[2] = '\0'
         */
        int path_len = curr - full_path;

        /* extract directory name */
        strncpy(path_copy, full_path, path_len);
        path_copy[path_len]='\0';

        /* stat dir */
        if ( lstat( path_copy, &st ) != 0 )
        {
            rc = -errno;
            if (rc != -ENOENT)
            {
                DisplayLog( LVL_CRIT, RBHEXT_TAG, "Cannot lstat() '%s': %s", path_copy, strerror(-rc) );
                return rc;
            }

            if (get_orig_dir_md(path_copy, &st, target) == 0)
            {
                mode = st.st_mode & 07777;
                setattrs = TRUE;
            }
            else
            {
                mode = default_mode;
                setattrs = FALSE;
            }

            DisplayLog(LVL_FULL, RBHEXT_TAG, "mkdir(%s)", path_copy );
            if ( (mkdir( path_copy, mode ) != 0) && (errno != EEXIST) )
            {
                rc = -errno;
                DisplayLog( LVL_CRIT, RBHEXT_TAG, "mkdir(%s) failed: %s",
                            path_copy, strerror(-rc) );
                return rc;
            }

            if ( setattrs )
            {
                /* set owner and group */
                if ( lchown( path_copy, st.st_uid, st.st_gid ) )
                    DisplayLog( LVL_MAJOR, RBHEXT_TAG,
                                "Error setting owner/group for '%s': %s",
                                path_copy, strerror(errno) );
                /* mode is set by mkdir */
            }
        }
        else if ( !S_ISDIR( st.st_mode ) )
        {
            DisplayLog( LVL_CRIT, RBHEXT_TAG,
                        "Cannot create directory '%s': existing non-directory",
                        path_copy );
            return -ENOTDIR;
        }

        curr++;
    }

    if (get_orig_dir_md(full_path, &st, target) == 0)
    {
        mode = st.st_mode & 07777;
        setattrs = TRUE;
    }
    else
    {
        mode = default_mode;
        setattrs = FALSE;
    }

    /* finaly create this dir */
    DisplayLog(LVL_FULL, RBHEXT_TAG, "mkdir(%s)", full_path );
    if ( (mkdir( full_path, mode ) != 0) && (errno != EEXIST) )
    {
        rc = -errno;
        DisplayLog( LVL_CRIT, RBHEXT_TAG, "mkdir(%s) failed: %s", full_path, strerror(-rc) );
        return rc;
    } else if (setattrs) {
        /* set owner and group */
        if ( lchown( full_path, st.st_uid, st.st_gid ) )
            DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Error setting owner/group for '%s': %s",
                        full_path, strerror(errno) );
        /* mode is set by mkdir (FIXME but can be cleared by chown) */
    }

    return 0;
}

static int file_clone_attrs(const char *tgt, const struct stat *st)
{
    struct utimbuf tbuf;

    if (lchown(tgt, st->st_uid, st->st_gid))
        return -errno;
    if (chmod(tgt, st->st_mode & 07777))
        return -errno;

    tbuf.actime = st->st_atime;
    tbuf.modtime = st->st_mtime;

    if (utime(tgt, &tbuf))
        return -errno;

    return 0;
}

/* log tag for built-in copy */
#define CP_TAG "cp"

#define COMPRESSED_SRC  (1 << 0)
#define COMPRESS_DEST   (1 << 1)
#define USE_SENDFILE    (1 << 2)

struct copy_info {
    const char  *src;
    const char  *dst;
    int          src_fd;
    int          dst_fd;
    struct stat  src_st;
};

static int flush_data(int srcfd, int dstfd)
{
    posix_fadvise(srcfd, 0, 0, POSIX_FADV_DONTNEED);
    if (config.sync_archive_data)
    {
        if (fdatasync(dstfd) < 0)
            return -errno;
    }
    posix_fadvise(dstfd, 0, 0, POSIX_FADV_DONTNEED);
    return 0;
}

static int builtin_copy_standard(const struct copy_info *cp_nfo, int flags)
{
    int srcfd, dstfd;
    struct stat dst_st;
    int rc = 0;
    size_t  io_size;
    ssize_t r = 0, w = 0;
    char *io_buff = NULL;
    gzFile gz = NULL;
    int gzerr, err_close = 0;

    if ((flags & COMPRESSED_SRC) && (flags & COMPRESS_DEST))
    {
        DisplayLog(LVL_MAJOR, CP_TAG, "Copy function doesn't handle "
                   "compression for both source and destination");
        return -EINVAL;
    }

    if (flags & COMPRESSED_SRC)
    {
        srcfd = dup(cp_nfo->src_fd);
        dstfd = cp_nfo->dst_fd;

        gz = gzdopen(srcfd, "rb");
        if (gz == NULL)
        {
            DisplayLog(LVL_MAJOR, CP_TAG,
                       "Failed to initialize decompression stream");
            close(srcfd);
            return -EIO;
        }
    }
    else if (flags & COMPRESS_DEST)
    {
        srcfd = cp_nfo->src_fd;
        dstfd = dup(cp_nfo->dst_fd);

        gz = gzdopen(dstfd, "wb");
        if (gz == NULL)
        {
            DisplayLog(LVL_MAJOR, CP_TAG,
                       "Failed to initialize decompression stream");
            close(dstfd);
            return -EIO;
        }
    }
    else
    {
        /* Uncompressed regular copy */
        srcfd = cp_nfo->src_fd;
        dstfd = cp_nfo->dst_fd;
    }

    /* needed to get the biggest IO size of source and destination. */
    if (fstat(dstfd, &dst_st))
    {
        rc = -errno;
        DisplayLog(LVL_MAJOR, CP_TAG, "Failed to stat %s: %s",
                   cp_nfo->dst, strerror(-rc));
        goto out_close;
    }

    io_size = MAX2(cp_nfo->src_st.st_blksize, dst_st.st_blksize);
    DisplayLog(LVL_DEBUG, CP_TAG, "using IO size = %"PRI_SZ, io_size);

    io_buff = MemAlloc(io_size);
    if (!io_buff)
    {
        rc = -ENOMEM;
        goto out_close;
    }

    /* Do the copy */
    do
    {
        if (flags & COMPRESSED_SRC)
            r = gzread(gz, io_buff, io_size);
        else
            r = read(srcfd, io_buff, io_size);

        if (r <= 0)
            break;

        if (flags & COMPRESS_DEST)
            w = gzwrite(gz, io_buff, r);
        else
            w = write(dstfd, io_buff, r);

        if (w < 0)
        {
            rc = -errno;
            DisplayLog(LVL_MAJOR, CP_TAG, "Copy error (%s -> %s): %s",
                       cp_nfo->src, cp_nfo->dst, strerror(-rc));
            goto out_free;
        }
        else if (w < r)
        {
            DisplayLog(LVL_MAJOR, CP_TAG, "Short write on %s, aborting copy",
                       cp_nfo->dst);
            rc = -EAGAIN;
            goto out_free;
        }
    } while (r > 0);

    if (r < 0) /* error */
    {
        rc = -errno;
        goto out_free;
    }
    /* else (r == 0): EOF */

    /* need to flush the compression buffer before system sync */
    if (flags & COMPRESS_DEST)
    {
        if (gzflush(gz, Z_FINISH) != Z_OK)
        {
            DisplayLog(LVL_MAJOR, CP_TAG, "compression error for %s: %s",
                       cp_nfo->dst, gzerror(gz, &gzerr));
            rc = -EIO;
            goto out_free;
        }
    }

    /* Free the kernel buffer cache as we don't expect to read the files again.
     * This can be done immediatly for the read file.
     * For the written file, we need to flush it to disk to ensure
     * that it is correctly archived and to allow freeing the buffer cache. */
    rc = flush_data(srcfd, dstfd);
    if (rc)
        goto out_free;

out_free:
    MemFree(io_buff);

out_close:
    if (flags & (COMPRESSED_SRC | COMPRESS_DEST))
        err_close = (gzclose(gz) != Z_OK);

    if (err_close && rc == 0)
    {
        rc = errno ? -errno : -EIO;
        DisplayLog(LVL_MAJOR, CP_TAG, "close failed on %s: %s",
                   cp_nfo->src, "error closing compression stream");
    }

    return rc;
}

static int builtin_copy_sendfile(const struct copy_info *cp_nfo, int flags)
{
    int rc;
    int srcfd = cp_nfo->src_fd;
    int dstfd = cp_nfo->dst_fd;
    size_t fsize = cp_nfo->src_st.st_size;

#if HAVE_FALLOCATE
    rc = fallocate(dstfd, 0, 0, fsize);
    if (rc)
    {
        rc = -errno;
        DisplayLog(LVL_MAJOR, CP_TAG, "Failed to fallocate %s: %s",
                   cp_nfo->dst, strerror(-rc));
        goto out;
    }
#endif

    /* sendfile() copy loop. The function seems to reliably return every 2GB.
     * Make sure the whole file gets copied. The NULL offset delegates current
     * offset management to sendfile. */
    while (fsize > 0) {
        ssize_t rw;

        rw = sendfile(dstfd, srcfd, NULL, fsize);
        if (rw < 0)
        {
            rc = -errno;
            DisplayLog(LVL_MAJOR, CP_TAG, "Failed to sendfile(%s->%s): %s",
                       cp_nfo->src, cp_nfo->dst, strerror(-rc));
            goto out;
        }

        assert(rw <= (ssize_t)fsize);
        fsize -= rw;
    }

    rc = flush_data(srcfd, dstfd);
    if (rc)
        goto out;

out:
    return rc;
}

static int builtin_copy(const char *src, const char *dst, int dst_flags,
                        int save_attrs, int flags)
{
    struct copy_info cp_nfo;
    int rc, err_close = 0;

    cp_nfo.src = src;
    cp_nfo.dst = dst;

    cp_nfo.src_fd = open(src, O_RDONLY | O_NOATIME);
    if (cp_nfo.src_fd < 0)
    {
        rc = -errno;
        DisplayLog(LVL_MAJOR, CP_TAG, "Can't open %s for read: %s", src,
                   strerror(-rc));
        return rc;
    }

    if (fstat(cp_nfo.src_fd, &cp_nfo.src_st))
    {
        rc = -errno;
        DisplayLog(LVL_MAJOR, CP_TAG, "Failed to stat %s: %s", src,
                   strerror(-rc));
        goto close_src;
    }

    cp_nfo.dst_fd = open(dst, dst_flags, cp_nfo.src_st.st_mode & 07777);
    if (cp_nfo.dst_fd < 0)
    {
        rc = -errno;
        DisplayLog(LVL_MAJOR, CP_TAG, "Can't open %s for write: %s",
                   dst, strerror(-rc));
        goto close_src;
    }

    if (flags & (COMPRESSED_SRC | COMPRESS_DEST))
        rc = builtin_copy_standard(&cp_nfo, flags);
    else if (flags & USE_SENDFILE)
        rc = builtin_copy_sendfile(&cp_nfo, flags);
    else
        rc = builtin_copy_standard(&cp_nfo, flags);

    err_close = close(cp_nfo.dst_fd);
    if (err_close && (rc == 0))
    {
        rc = errno ? -errno : -EIO;
        DisplayLog(LVL_MAJOR, CP_TAG, "close failed on %s: %s",
                   dst, strerror(-rc));
    }

close_src:
    close(cp_nfo.src_fd);

    if (rc == 0 && save_attrs)
        rc = file_clone_attrs(dst, &cp_nfo.src_st);

    return rc;
}


/**
 * Performs an archiving operation.
 * \param[in] arch_meth archiving method (sync or async)
 * \param[in] p_id pointer to id of entry to be archived
 * \param[in,out] p_attrs pointer to entry attributes
 *        function must update at least the entry status
 *        and the path in the backend.
 */
int rbhext_archive( rbhext_arch_meth arch_meth,
                    const entry_id_t * p_id,
                    attr_set_t * p_attrs,
                    const char * hints )
{
    int rc;
    char bkpath[RBH_PATH_MAX];
    char fspath[RBH_PATH_MAX];
    char tmp[RBH_PATH_MAX];
    char * destdir;
    struct stat info;
    struct stat void_stat;
    int check_moved = FALSE;
    obj_type_t entry_type;

    if ( arch_meth != RBHEXT_SYNC )
        return -ENOTSUP;

    /* if status is not determined, retrieve it */
    if ( !ATTR_MASK_TEST(p_attrs, status) )
    {
        DisplayLog( LVL_DEBUG, RBHEXT_TAG, "Status not provided to rbhext_archive()" );
        rc = rbhext_get_status( p_id, p_attrs, p_attrs );
        if (rc)
            return rc;
    }

    /* is it the good type? */
    if ( !ATTR_MASK_TEST(p_attrs, type) )
    {
        DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Missing mandatory attribute 'type' in %s()",
                    __FUNCTION__ );
        return -EINVAL;
    }

    entry_type = ListMgr2PolicyType(ATTR(p_attrs, type));
    if ((entry_type == TYPE_LINK) && !config.archive_symlinks)
    {
        DisplayLog(LVL_VERB, RBHEXT_TAG, "Symlink support is disabled in configuration");
        return -ENOTSUP;
    }
    else if ((entry_type != TYPE_FILE) && (entry_type != TYPE_LINK))
    {
        DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Unsupported type for archive operation: %s",
                    ATTR(p_attrs, type) );
        return -ENOTSUP;
    }

    /* compute path for target file */
    entry2backend_path(p_id, p_attrs, FOR_NEW_COPY, bkpath, config.compress);

    /* check the status */
    if ( ATTR(p_attrs, status) == STATUS_NEW )
    {
        /* check the entry does not already exist */
        if ( (lstat(bkpath, &void_stat) == 0) || (errno != ENOENT) )
        {
            rc = -errno;
            DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Error: new entry %s already exist. errno=%d, %s",
                        bkpath, -rc, strerror(-rc) );
            return rc;
        }
    }
    else if ( ATTR(p_attrs, status) == STATUS_MODIFIED
             || ATTR(p_attrs, status) == STATUS_ARCHIVE_RUNNING ) /* for timed out copies.. or ourselves! */
     {
        /* check if somebody else is about to copy */
        rc = check_running_copy(bkpath);
        if (rc < 0)
            return rc;
        else if (rc > 0)/* current archive */
            return -EALREADY;

       /* check that previous path exists */
        if ( ATTR_MASK_TEST(p_attrs, backendpath) )
        {
            /* need to check if the entry was renamed */
            check_moved = TRUE;
            if ( lstat(ATTR(p_attrs,backendpath), &void_stat) != 0 )
            {
                rc = -errno;
                DisplayLog(LVL_MAJOR, RBHEXT_TAG, "Warning: previous copy %s not found in backend (errno=%d, %s): "
                           "entry will be archived again as %s.",
                           ATTR(p_attrs,backendpath), -rc, strerror(-rc), bkpath);
            }
        }
    }
    else /* invalid status */
    {
        /* invalid status for performing archive() */
        DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Unexpected status %d for calling %s()",
                     ATTR(p_attrs, status), __FUNCTION__ );
        return -EINVAL;
    }

#ifdef _HAVE_FID
    /* for Lustre 2, use fid path so the operation is not disturbed by renames... */
    BuildFidPath( p_id, fspath );
#else
    /* we need the posix path */
    if ( !ATTR_MASK_TEST(p_attrs, fullpath) )
    {
        DisplayLog( LVL_CRIT, RBHEXT_TAG, "Error in %s(): path argument is mandatory for archive command",
                    __FUNCTION__ );
        return -EINVAL;
    }
    strcpy(fspath, ATTR(p_attrs, fullpath));
#endif

    /* 1) extract dir path */
    strcpy( tmp, bkpath );
    destdir = dirname( tmp );
    if ( destdir == NULL )
    {
        DisplayLog( LVL_CRIT, RBHEXT_TAG, "Error extracting directory path of '%s'",
                    bkpath );
        return -EINVAL;
    }
    /* 2) create it recursively */
    rc = mkdir_recurse_clone_attrs( destdir, 0750, TO_BACKEND );
    if ( rc )
        return rc;

    if ( entry_type == TYPE_FILE )
    {
        /* temporary copy path */
        sprintf( tmp, "%s.%s", bkpath, COPY_EXT );

#ifdef HAVE_SHOOK
        rc = shook_archive_start(get_fsname(), p_id, bkpath);
        if (rc)
        {
            DisplayLog( LVL_CRIT, RBHEXT_TAG, "Failed to initialize transfer: shook_archive_start() returned error %d",
                        rc );
            return rc;
        }
#endif

        /* execute the archive command (or built-in, if not set) */
        if (EMPTY_STRING(config.action_cmd))
            rc = builtin_copy(fspath, tmp, O_WRONLY | O_CREAT | O_TRUNC, TRUE,
                              (config.compress?COMPRESS_DEST:0) |
                              (config.sendfile?USE_SENDFILE:0));
        else
        {
            if (hints)
                rc = execute_shell_command(TRUE, config.action_cmd, 4, "ARCHIVE", fspath, tmp, hints);
            else
                rc = execute_shell_command(TRUE, config.action_cmd, 3, "ARCHIVE", fspath, tmp);
        }

        if (rc)
        {
#ifdef HAVE_SHOOK
            shook_archive_abort(get_fsname(), p_id);
#endif
            /* cleanup tmp copy */
            unlink(tmp);
            /* the transfer failed. still needs to be archived */
            ATTR_MASK_SET( p_attrs, status );
            ATTR( p_attrs, status ) = STATUS_MODIFIED;
            return rc;
        }
        else
        {
            /* finalize tranfer */

            /* owner/group is saved by the copy command */

            /* reset initial mtime */
            if ( ATTR_MASK_TEST( p_attrs, last_mod ) )
            {
                  struct utimbuf tbuf;
                  tbuf.actime = time(NULL);
                  tbuf.modtime = ATTR( p_attrs, last_mod );

                if (utime(tmp, &tbuf) != 0)
                {
                    rc = -errno;
                    DisplayLog( LVL_CRIT, RBHEXT_TAG, "Error setting mtime for file %s: %s",
                                tmp, strerror(-rc) );
                    /* ignore the error */
                    rc = 0;
                }
            }

            /* move entry to final path */
            if (rename(tmp, bkpath) != 0 )
            {
                rc = -errno;
                DisplayLog( LVL_CRIT, RBHEXT_TAG, "Error renaming tmp copy file '%s' to final name '%s': %s",
                            tmp, bkpath, strerror(-rc) );

                /* the transfer failed. still needs to be archived */
                ATTR_MASK_SET( p_attrs, status );
                ATTR( p_attrs, status ) = STATUS_MODIFIED;
                return rc;
            }

            /* did the file been renamed since last copy? */
            if ( check_moved && strcmp( bkpath, ATTR( p_attrs, backendpath ) ))
            {
                DisplayLog( LVL_DEBUG, RBHEXT_TAG, "Removing previous copy %s",
                             ATTR( p_attrs, backendpath ) );
                if ( unlink( ATTR( p_attrs, backendpath ) ))
                {
                    rc = -errno;
                    DisplayLog( LVL_DEBUG, RBHEXT_TAG, "Error removing previous copy %s: %s",
                                ATTR( p_attrs, backendpath ), strerror(-rc) );
                    /* ignore */
                    rc = 0;
                }
            }

            ATTR_MASK_SET( p_attrs, status );
            ATTR( p_attrs, status ) = STATUS_SYNCHRO;

            ATTR_MASK_SET( p_attrs, backendpath );
            strcpy( ATTR( p_attrs, backendpath ), bkpath );

            ATTR_MASK_SET( p_attrs, last_archive );
            ATTR( p_attrs, last_archive) = time(NULL);

#ifdef HAVE_SHOOK
            rc = shook_archive_finalize(get_fsname(), p_id, bkpath);
            if (rc)
            {
                DisplayLog( LVL_CRIT, RBHEXT_TAG, "Failed to finalize transfer: shook_archive_finalize() returned error %d",
                            rc );
                return rc;
           }
#endif
        }

        if ( lstat(fspath, &info) != 0 )
        {
            rc = -errno;
            DisplayLog( LVL_EVENT, RBHEXT_TAG, "Error performing final lstat(%s): %s",
                        fspath, strerror(-rc) );
            ATTR_MASK_SET( p_attrs, status );
            ATTR( p_attrs, status ) = STATUS_UNKNOWN;
        }
        else
        {
            if ( (info.st_mtime != ATTR( p_attrs, last_mod ))
                 || (info.st_size != ATTR( p_attrs, size )) )
            {
                DisplayLog( LVL_EVENT, RBHEXT_TAG, "Entry %s has been modified during transfer: "
                            "size before/after: %"PRI_SZ"/%"PRI_SZ", "
                            "mtime before/after: %u/%"PRI_TT,
                            fspath, ATTR( p_attrs, size ), info.st_size,
                            ATTR( p_attrs, last_mod ), info.st_mtime );
                ATTR_MASK_SET( p_attrs, status );
                ATTR( p_attrs, status ) = STATUS_MODIFIED;
            }

            /* update entry attributes */
            PosixStat2EntryAttr( &info, p_attrs, TRUE );
        }
    }
    else if ( entry_type == TYPE_LINK )
    {
        char link[RBH_PATH_MAX] = "";

        if (!config.archive_symlinks)
            return -ENOTSUP;

        /* read link content from filesystem */
        if ( readlink(fspath, link, RBH_PATH_MAX) < 0 )
        {
            rc = -errno;
            DisplayLog( LVL_MAJOR,  RBHEXT_TAG, "Error reading symlink content (%s): %s",
                        fspath, strerror(-rc) );
            return rc;
        }
        /* link content is not supposed to change during its lifetime */
        if ( symlink(link, bkpath) != 0 )
        {
            rc = -errno;
            DisplayLog( LVL_MAJOR,  RBHEXT_TAG, "Error creating symlink %s->\"%s\" in backend: %s",
                        bkpath, link, strerror(-rc) );
            return rc;
        }

        ATTR_MASK_SET( p_attrs, status );
        ATTR( p_attrs, status ) = STATUS_SYNCHRO;

        /* set symlink owner/group */
        if ( lstat(fspath, &info) != 0 )
        {
            rc = -errno;
            DisplayLog( LVL_EVENT, RBHEXT_TAG, "Error performing final lstat(%s): %s",
                        fspath, strerror(-rc) );
            ATTR_MASK_SET( p_attrs, status );
            ATTR( p_attrs, status ) = STATUS_UNKNOWN;
        }
        else
        {
            if (lchown(bkpath, info.st_uid, info.st_gid))
            {
                DisplayLog( LVL_EVENT, RBHEXT_TAG, "error setting owner/group in backend on %s: %s",
                            bkpath, strerror(-rc) );
            }
        }

        ATTR_MASK_SET( p_attrs, backendpath );
        strcpy( ATTR( p_attrs, backendpath ), bkpath );

        ATTR_MASK_SET( p_attrs, last_archive );
        ATTR( p_attrs, last_archive) = time(NULL);
    }

    return 0;
} /* archive */

/**
 * Performs entry removal in the backend
 * \param[in] p_id pointer to id of entry to be archived
 * \param[in,out] p_attrs pointer to entry attributes
 *                        must be updated even on failure
 * \retval  -ENOENT entry not in backend
 * \retval  -EINVAL empty path provided
 */
int rbhext_remove( const entry_id_t * p_id, const char * backend_path )
{
    int rc;
    if ( backend_path && !EMPTY_STRING(backend_path) )
    {
        if ( unlink(backend_path) != 0 )
        {
            rc = -errno;
            if ( rc == -ENOENT )
            {
                DisplayLog( LVL_DEBUG, RBHEXT_TAG, "'%s' not found in backend",
                            backend_path );
                return rc;
            }
            else
            {
                DisplayLog( LVL_EVENT, RBHEXT_TAG, "Error removing '%s' from backend: %s",
                            backend_path, strerror(-rc) );
                return rc;
            }
        }
    }
    else
        return -EINVAL;
    return 0;
}

static inline int create_parent_of_clone_attrs(const char * child_path, entry_id_t * p_parent_id)
{
    char tmp[RBH_PATH_MAX];
    char * destdir;
    int rc;

    /* extract parnet dir path */
    strcpy( tmp, child_path );
    destdir = dirname( tmp );
    if (destdir == NULL)
    {
        DisplayLog( LVL_CRIT, RBHEXT_TAG, "Error extracting directory path of '%s'",
                    child_path );
        return -EINVAL;
    }

    /* create the directory */
    rc = mkdir_recurse_clone_attrs(destdir, 0750, TO_FS);
    if (rc)
        return rc;

    /* retrieve parent fid */
#ifdef _HAVE_FID
    rc = Lustre_GetFidFromPath( destdir, p_parent_id );
    if (rc)
        return rc;
#else
    struct stat parent_stat;
    if (lstat(destdir, &parent_stat))
    {
        rc = -errno;
        DisplayLog( LVL_CRIT, RBHEXT_TAG, "ERROR: cannot stat target directory '%s': %s",
                    destdir, strerror(-rc) );
        return rc;
    }
    /* build id from dev/inode*/
    parent_id.inode = parent_stat.st_ino;
    parent_id.device = parent_stat.st_dev;
    parent_id.validator = parent_stat.st_ctime;
#endif
    return 0;
}

#define IS_ZIP_NAME(_n) (_n[strlen(_n) - 1] == 'z')

/** recover a file from the backend after formatting FS
 * \retval recovery status
 */
recov_status_t rbhext_recover( const entry_id_t * p_old_id,
                               attr_set_t * p_attrs_old,
                               entry_id_t * p_new_id,
                               attr_set_t * p_attrs_new,
                               struct stat * bkinfo )
{
    char bkpath[RBH_PATH_MAX] = "";
    char link[RBH_PATH_MAX] = "";
    char buff[RBH_PATH_MAX] = "";
    const char * backend_path = NULL;
    const char * fspath;
    int rc;
    struct stat st_bk;

    struct stat st_dest;
    recov_status_t success_status = RS_ERROR;
    attr_set_t attr_bk;
    int fd;
    entry_id_t  parent_id;
    mode_t mode_create = 0;
    int set_mode = FALSE;
    int stat_done = FALSE;
    int no_copy = FALSE;
    int compressed = 0;

    if (!ATTR_MASK_TEST(p_attrs_old, fullpath) || EMPTY_STRING(ATTR(p_attrs_old, fullpath)))
    {
        DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Missing mandatory attribute 'fullpath' for restoring entry "DFID, PFID(p_old_id) );
        return RS_ERROR;
    }
    fspath = ATTR(p_attrs_old, fullpath);
    /* if FS path is not absolute, get the relative backend path and append to FS root */
    if (fspath[0] != '/')
    {
        char tmp[RBH_PATH_MAX];
        if (ATTR_MASK_TEST(p_attrs_old, backendpath))
        {
            relative_path(ATTR(p_attrs_old, backendpath), config.root, tmp);
            sprintf(buff, "%s/%s/%s",global_config.fs_path, dirname(tmp), strrchr(fspath, '/') + 1);
            fspath = buff;
        }
        else /* use the given relative path */
        {
            sprintf(buff, "%s/%s", global_config.fs_path, ATTR(p_attrs_old, fullpath));
            fspath = buff;
        }
    }

    if ( ATTR_MASK_TEST( p_attrs_old, backendpath) )
        backend_path = ATTR( p_attrs_old, backendpath );
    else
    /* if there is no backend path, try to guess */
    {
        int lvl_log;

        if (ATTR_MASK_TEST(p_attrs_old, type) && !strcasecmp(ATTR(p_attrs_old, type), STR_TYPE_FILE))
            lvl_log = LVL_EVENT;
        else
            lvl_log = LVL_VERB;
        entry2backend_path(p_old_id, p_attrs_old, FOR_LOOKUP, bkpath, config.compress);
        DisplayLog( lvl_log, RBHEXT_TAG,
                    "No backend path is set for '%s', guess it could be '%s'",
                    fspath, bkpath );
        backend_path = bkpath;
    }

    if (!ATTR_MASK_TEST(p_attrs_old, type))
    {
        if (bkinfo)
            st_bk=*bkinfo;
        else if (bk_lstat(backend_path, &st_bk, 1, &compressed) != 0)
        {
            rc = errno;
            DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Cannot restore entry "
                        DFID ": '%s' not found in backend.", PFID(p_old_id), backend_path );
            if (rc == ENOENT )
                return RS_NOBACKUP;
            else
                return RS_ERROR;
        }
        stat_done = TRUE;
        /* set type in attrs_old */
        const char *type = mode2type(st_bk.st_mode);
        if (type != NULL)
        {
            ATTR_MASK_SET(p_attrs_old, type);
            strcpy(ATTR(p_attrs_old, type), type);
        }
        else
        {
            DisplayLog( LVL_MAJOR, RBHEXT_TAG, "%s has unsupported type", backend_path);
            return RS_NOBACKUP;
        }
    }

    if (!strcasecmp(ATTR(p_attrs_old, type), STR_TYPE_DIR))
    {
        /* entry is a directory */

        /* create parent dir if it does not exist */
        if (create_parent_of_clone_attrs(fspath, &parent_id))
            return RS_ERROR;

        if (ATTR_MASK_TEST(p_attrs_old,mode))
            mode_create = ATTR(p_attrs_old, mode);
        else
            mode_create = 750;

        rc = mkdir(fspath, mode_create) ? errno : 0;

        if (rc != 0 && rc != EEXIST)
        {
            DisplayLog(LVL_CRIT, RBHEXT_TAG, "ERROR: cannot create directory '%s': %s",
                       fspath, strerror(rc));
            return RS_ERROR;
        }
        else if (rc == EEXIST)
            /* must set the mode */
            set_mode = TRUE;

        success_status = RS_NON_FILE;
    }
    else if (!strcasecmp(ATTR(p_attrs_old, type), STR_TYPE_LINK))
    {
        /* entry is a symlink */

        /* create parent dir if it does not exist */
        if (create_parent_of_clone_attrs(fspath, &parent_id))
            return RS_ERROR;

        /* restore from DB */
        if (ATTR_MASK_TEST(p_attrs_old, link))
            strcpy(link, ATTR(p_attrs_old, link));
        else /* restore from FS */
        {

            /* read link content from backend */
            if ( readlink(backend_path, link, RBH_PATH_MAX) < 0 )
            {
                rc = errno;
                DisplayLog( LVL_MAJOR,  RBHEXT_TAG, "Error reading symlink content (%s): %s",
                            backend_path, strerror(rc) );
                if (rc == ENOENT)
                    return RS_NOBACKUP;
                else
                    return RS_ERROR;
            }
            /* safety */
            link[RBH_PATH_MAX-1] = '\0';
        }

        if ( symlink(link, fspath) != 0 )
        {
            rc = errno;
            DisplayLog( LVL_MAJOR,  RBHEXT_TAG, "Error creating symlink %s->\"%s\" in filesystem: %s",
                        fspath, link, strerror(rc) );
            return RS_ERROR;
        }

        success_status = RS_NON_FILE;
    }
    else if (!strcasecmp(ATTR(p_attrs_old, type), STR_TYPE_FILE))
    {
        struct utimbuf utb;

        /* test if this copy exists */
        if (!stat_done)
        {
            if (bkinfo)
            {
                st_bk=*bkinfo;
                stat_done = TRUE;
                /* does the backend path looks compressed? */
                compressed = IS_ZIP_NAME(backend_path);
            }
            else if (bk_lstat(backend_path, &st_bk, 1, &compressed) != 0)
            {
                rc = errno;
                if (rc != ENOENT)
                {
                    DisplayLog(LVL_MAJOR, RBHEXT_TAG, "Cannot stat '%s' in backend: %s",
                               backend_path, strerror(rc));
                    return RS_ERROR;
                }
            }
            else
                stat_done = TRUE;
        }

        if (!stat_done)
        {
            /* if no stat done and file != 0 => no backup */
            if (!ATTR_MASK_TEST(p_attrs_old, size) || ATTR(p_attrs_old, size) != 0)
            {
                DisplayLog( LVL_MAJOR, RBHEXT_TAG, "%s has no backup copy (%s not found)",
                            fspath, backend_path);
                return RS_NOBACKUP;
            }
            else
                no_copy = TRUE;
        }

        if (!no_copy) /* only if there is a copy in backend */
        {
            if (!S_ISREG(st_bk.st_mode))
            {
                DisplayLog( LVL_CRIT, RBHEXT_TAG, "ERROR: recovering file from non-file object %s", backend_path);
                return RS_ERROR;
            }

            ATTR_MASK_INIT( &attr_bk );
            /* merge missing posix attrs to p_attrs_old */
            PosixStat2EntryAttr(&st_bk, &attr_bk, TRUE);
            /* leave attrs unchanged if they are already set in p_attrs_old */
            ListMgr_MergeAttrSets(p_attrs_old, &attr_bk, FALSE);
        }

        /* test if the target does not already exist */
        rc = lstat(fspath, &st_dest) ? errno : 0;
        if (rc == 0)
        {
            DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Error: cannot recover '%s': already exists",
                        fspath );
            return RS_ERROR;
        }
        else if (rc != ENOENT)
        {
            DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Unexpected error performing lstat(%s): %s",
                        fspath, strerror(rc) );
            return RS_ERROR;
        }

        /* check that this is not a cross-device import or recovery (entry could not be moved
         * in that case) */
        if (!no_copy && config.check_mounted && (backend_dev != st_bk.st_dev))
        {
            DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Source file %s is not in the same device as target %s",
                        backend_path, config.root );
            return RS_ERROR;
        }

        if (create_parent_of_clone_attrs(fspath, &parent_id))
            return RS_ERROR;

        if (ATTR_MASK_TEST(p_attrs_old, mode))
            mode_create = ATTR(p_attrs_old, mode);
        else if (!no_copy)
            mode_create = st_bk.st_mode;
        else
            mode_create = 0640; /* default */

#ifdef _LUSTRE
        /* restripe the file in Lustre */
        if (ATTR_MASK_TEST(p_attrs_old, stripe_info))
        {
            CreateStriped(fspath, &ATTR( p_attrs_old, stripe_info ), FALSE);
            set_mode= TRUE;
        }
        else {
#endif
        fd = creat(fspath, mode_create & 07777);
        if (fd < 0)
        {
            rc = errno;
            DisplayLog( LVL_CRIT, RBHEXT_TAG, "ERROR: couldn't create '%s': %s",
                        fspath, strerror(rc) );
            return RS_ERROR;
        }
        else
            close(fd);
#ifdef _LUSTRE
        }
#endif

        if (!no_copy)
        {
    #ifdef HAVE_PURGE_POLICY
        /* this backend is restore/release capable.
         * Recover the entry in released state (md only),
         * so it will be recovered at first open.
         */
    #ifdef HAVE_SHOOK
            size_t restore_size = st_bk.st_size;

            /* set the file in "released" state */
            rc = shook_set_status(fspath, SS_RELEASED);
            if (rc)
            {
                DisplayLog( LVL_CRIT, RBHEXT_TAG, "ERROR setting released state for '%s': %s",
                            fspath, strerror(-rc));
                return RS_ERROR;
             }

            /* if the file was compressed, set the saved size (from the DB), not the one from the backend */
            if (compressed && p_attrs_old && ATTR_MASK_TEST(p_attrs_old, size))
                restore_size = ATTR(p_attrs_old, size);

            rc = truncate(fspath, restore_size) ? errno : 0;
            if (rc)
            {
                DisplayLog( LVL_CRIT, RBHEXT_TAG, "ERROR could not set original size %"PRI_SZ" for '%s': %s",
                            st_bk.st_size, fspath, strerror(rc));
                return RS_ERROR;
            }
    #else
        #error "Unexpected case"
    #endif
    #else
            /* full restore (even data) */
            if (EMPTY_STRING(config.action_cmd))
                rc = builtin_copy(backend_path, fspath, O_WRONLY, FALSE,
                                  (compressed?COMPRESSED_SRC:0) |
                                  (config.sendfile?USE_SENDFILE:0));
            else
                rc = execute_shell_command(TRUE, config.action_cmd, 3, "RESTORE",
                                           backend_path, fspath);
            if (rc)
                return RS_ERROR;
            /* TODO: remove partial copy */
    #endif
        }

        if (!no_copy)
        {
            utb.actime = st_bk.st_atime;
            utb.modtime = st_bk.st_mtime;

            /* set the same mtime as in the backend */
            DisplayLog( LVL_FULL, RBHEXT_TAG, "Restoring times from backend for '%s': atime=%lu, mtime=%lu",
                        fspath, utb.actime, utb.modtime );
            if ( utime( fspath, &utb ) )
                DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Warning: couldn't restore times for '%s': %s",
                            fspath, strerror(errno) );
        }
        else if (ATTR_MASK_TEST(p_attrs_old, last_mod))
        {
            utb.modtime = ATTR(p_attrs_old, last_mod);

            if (ATTR_MASK_TEST(p_attrs_old, last_access))
                utb.actime = ATTR(p_attrs_old, last_access);
            else
                utb.actime = utb.modtime;

            /* set the same mtime as in the DB */
            DisplayLog( LVL_FULL, RBHEXT_TAG, "Restoring times from DB for '%s': atime=%lu, mtime=%lu",
                        fspath, utb.actime, utb.modtime );
            if ( utime( fspath, &utb ) )
                DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Warning: couldn't restore times for '%s': %s",
                            fspath, strerror(errno) );
        }

        if (no_copy)
            success_status = RS_FILE_EMPTY;
        else
            success_status = RS_FILE_OK;
    }
    else
    {
        /* type not supported */
        DisplayLog( LVL_CRIT, RBHEXT_TAG, "Error: cannot restore entries with type '%s' (%s)",
                    ATTR(p_attrs_old,type), fspath );
        return RS_NOBACKUP;
    }

    /* set owner, group */
    if ( ATTR_MASK_TEST( p_attrs_old, owner ) || ATTR_MASK_TEST( p_attrs_old, gr_name ) )
    {
        uid_t uid = -1;
        gid_t gid = -1;
        char buff[4096];

        if ( ATTR_MASK_TEST( p_attrs_old, owner ) )
        {
            struct passwd pw;
            struct passwd * p_pw;

            if ((getpwnam_r(ATTR(p_attrs_old, owner), &pw, buff, 4096, &p_pw ) != 0)
                 || (p_pw == NULL))
            {
                DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Warning: couldn't resolve uid for user '%s'",
                            ATTR(p_attrs_old, owner ));
                uid = -1;
            }
            else
                uid = p_pw->pw_uid;
        }

        if ( ATTR_MASK_TEST( p_attrs_old, gr_name ) )
        {
            struct group gr;
            struct group * p_gr;
            if ((getgrnam_r( ATTR(p_attrs_old, gr_name ), &gr, buff, 4096, &p_gr ) != 0)
                 || (p_gr == NULL))
            {
                DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Warning: couldn't resolve gid for group '%s'",
                            ATTR(p_attrs_old, gr_name ) );
                gid = -1;
            }
            else
                gid = p_gr->gr_gid;
        }

        DisplayLog( LVL_FULL, RBHEXT_TAG, "Restoring owner/group for '%s': uid=%u, gid=%u",
                    fspath, uid, gid );

        if ( lchown( fspath, uid, gid ) )
        {
            rc = errno;
            DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Warning: cannot set owner/group for '%s': %s",
                        fspath, strerror(rc) );
        }
        else
        {
            /* According to chown(2) manual: chown may clear sticky bits even if root does it,
             * so, we must set the mode again if it contains special bits */
            if (!set_mode && (mode_create & 07000))
                set_mode = TRUE;
        }
    }

    if (set_mode)
    {
        /* set the same mode as in the backend */
        DisplayLog( LVL_FULL, RBHEXT_TAG, "Restoring mode for '%s': mode=%#o",
                    fspath, mode_create & 07777 );
        if ( chmod( fspath, mode_create & 07777 ) )
            DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Warning: couldn't restore mode for '%s': %s",
                        fspath, strerror(errno) );

    }

    if ( lstat( fspath, &st_dest ) )
    {
        rc = errno;
        DisplayLog( LVL_CRIT, RBHEXT_TAG, "ERROR: lstat() failed on restored entry '%s': %s",
                    fspath, strerror(rc) );
        return RS_ERROR;
    }

    /* compare restored size and mtime with the one saved in the DB (for warning purpose)
     * (not for directories) */
    if (!S_ISDIR(st_dest.st_mode))
    {
        if ( ATTR_MASK_TEST(p_attrs_old, size) && (st_dest.st_size != ATTR(p_attrs_old, size)) )
        {
            if (!compressed)
            {
                DisplayLog( LVL_MAJOR, RBHEXT_TAG, "%s: the restored size (%zu) is "
                            "different from the last known size in filesystem (%"PRIu64"): "
                            "it may have been modified in filesystem after the last backup.",
                            fspath, st_dest.st_size, ATTR(p_attrs_old, size) );
                success_status = RS_FILE_DELTA;
            }
            else
                success_status = RS_FILE_OK;
        }
    }
    /* only for files */
    if (S_ISREG(st_dest.st_mode))
    {
        if ( ATTR_MASK_TEST( p_attrs_old, last_mod) && (st_dest.st_mtime != ATTR(p_attrs_old, last_mod)) )
        {
            DisplayLog( LVL_MAJOR, RBHEXT_TAG, "%s: the restored mtime (%lu) is "
                        "different from the last time in filesystem (%u): "
                        "it may have been modified in filesystem after the last backup.",
                        fspath, st_dest.st_mtime, ATTR(p_attrs_old, last_mod) );
            success_status = RS_FILE_DELTA;
        }
    }

    /* set the new attributes */
    ATTR_MASK_INIT( p_attrs_new );
    PosixStat2EntryAttr( &st_dest, p_attrs_new, TRUE );
    strcpy( ATTR( p_attrs_new, fullpath ), fspath );
    ATTR_MASK_SET( p_attrs_new, fullpath );

#ifdef _HAVE_FID
    /* get the new fid */
    rc = Lustre_GetFidFromPath( fspath, p_new_id );
    if (rc)
        return RS_ERROR;
#else
    /* build id from dev/inode*/
    p_new_id->inode =  st_dest.st_ino;
    p_new_id->device =  st_dest.st_dev;
    p_new_id->validator =  st_dest.st_ctime;
#endif

    /* set parent id */
    ATTR_MASK_SET( p_attrs_new, parent_id );
    ATTR( p_attrs_new, parent_id ) = parent_id;

    /* set name */
    char * name = strrchr(ATTR(p_attrs_new, fullpath), '/');
    if (name && *(name + 1) != '\0')
    {
        rh_strncpy(ATTR(p_attrs_new, name), name+1, RBH_NAME_MAX);
        ATTR_MASK_SET(p_attrs_new, name);
    }

#ifdef _LUSTRE
    if (!ATTR_MASK_TEST( p_attrs_new, type) || !strcmp(ATTR(p_attrs_new, type), STR_TYPE_FILE))
    {
        /* get the new stripe info */
        if ( File_GetStripeByPath( fspath,
                                   &ATTR( p_attrs_new, stripe_info ),
                                   &ATTR( p_attrs_new, stripe_items ) ) == 0 )
        {
            ATTR_MASK_SET( p_attrs_new, stripe_info );
            ATTR_MASK_SET( p_attrs_new, stripe_items );
        }
    }
#endif

    if (S_ISLNK(st_dest.st_mode))
    {
        strcpy(ATTR(p_attrs_new,link), link);
        ATTR_MASK_SET(p_attrs_new, link);
    }

    if (!no_copy && (S_ISREG(st_dest.st_mode)
         || (S_ISLNK(st_dest.st_mode)&& config.archive_symlinks)))
    {
        char tmp[RBH_PATH_MAX];
        char * destdir;

#ifdef HAVE_SHOOK
        /* only files remain released, others are synchro */
        if (S_ISREG(st_dest.st_mode))
        {
            ATTR( p_attrs_new, status ) = STATUS_RELEASED;
            ATTR_MASK_SET( p_attrs_new, status );
        }
        else
#endif
        {
            ATTR( p_attrs_new, status ) = STATUS_SYNCHRO;
            ATTR_MASK_SET( p_attrs_new, status );
        }

        /* set the new entry path in backend, according to the new fid, and actual compression */
        entry2backend_path(p_new_id, p_attrs_new,
                           FOR_NEW_COPY,
                           ATTR(p_attrs_new, backendpath), compressed);
        ATTR_MASK_SET( p_attrs_new, backendpath );

        /* recursively create the parent directory */
        /* extract dir path */
        strcpy( tmp, ATTR(p_attrs_new, backendpath) );
        destdir = dirname( tmp );
        if ( destdir == NULL )
        {
            DisplayLog( LVL_CRIT, RBHEXT_TAG, "Error extracting directory path of '%s'",
                        ATTR(p_attrs_new, backendpath) );
            return -EINVAL;
        }

        rc = mkdir_recurse_clone_attrs( destdir, 0750, TO_BACKEND );
        if (rc)
            return RS_ERROR;

        /* rename the entry in backend */
        if ( strcmp( ATTR(p_attrs_new, backendpath), backend_path ) != 0 )
        {
            DisplayLog( LVL_DEBUG, RBHEXT_TAG, "Moving the entry in backend: '%s'->'%s'",
                        backend_path, ATTR(p_attrs_new, backendpath) );
            if ( rename( backend_path, ATTR(p_attrs_new, backendpath) ) )
            {
                rc = errno;
                DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Could not move entry in backend ('%s'->'%s'): %s",
                            backend_path, ATTR(p_attrs_new, backendpath), strerror(rc) );
                /* keep the old path */
                strcpy( ATTR(p_attrs_new, backendpath), backend_path );
            }
        }

#ifdef HAVE_SHOOK
        /* save new backendpath to filesystem */
        /* XXX for now, don't manage several hsm_index */
        rc = shook_set_hsm_info( fspath, ATTR(p_attrs_new, backendpath), 0 );
        if (rc)
            DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Could not set backend path for %s: error %d",
                        fspath, rc );
#endif
    }

    return success_status;
}


/* rebind a backend entry to a new file in Lustre (with new fid)
 * Notice: fs_path is not necessarily the current path of new_id
 * but it should be moved to this path in the end.
 */
int rbhext_rebind(const char *fs_path, const char *old_bk_path,
                  char *new_bk_path, entry_id_t *new_id)
{
    int rc;
    attr_set_t attrs_new;
    struct stat st;
    char tmp[RBH_PATH_MAX];
    char fidpath[RBH_PATH_MAX];
    char * destdir;
    int compressed = IS_ZIP_NAME(old_bk_path);

    BuildFidPath( new_id, fidpath );

    if (lstat(fidpath, &st))
    {
        rc = -errno;
        DisplayLog(LVL_CRIT, RBHEXT_TAG, "ERROR: lstat() failed on target "DFID": %s",
                   PFID(new_id), strerror(-rc));
        return rc;
    }

    if (!S_ISREG(st.st_mode))
    {
        DisplayLog( LVL_MAJOR, RBHEXT_TAG, "%s() is only supported for files", __func__);
        return -ENOTSUP;
    }

    /* build attr struct */
    ATTR_MASK_INIT( &attrs_new );
    PosixStat2EntryAttr( &st, &attrs_new, TRUE );
    strcpy( ATTR( &attrs_new, fullpath ), fs_path );
    ATTR_MASK_SET( &attrs_new, fullpath );

    /* build new path in backend */
    entry2backend_path(new_id, &attrs_new, FOR_NEW_COPY, new_bk_path,
                       compressed); /* Ensure the target name is not compressed
                                     * if the source was not */
    /* set compression name if the previous entry was compressed */
    if (compressed && !IS_ZIP_NAME(new_bk_path))
        strcat(new_bk_path, "z");

    /* -- move entry from old bk path to the new location -- */

    /* recursively create the parent directory*/
    /* extract dir path */
    strcpy(tmp, new_bk_path);
    destdir = dirname(tmp);
    if (destdir == NULL)
    {
        DisplayLog( LVL_CRIT, RBHEXT_TAG, "Error extracting directory path of '%s'",
                    new_bk_path );
        return -EINVAL;
    }

    rc = mkdir_recurse_clone_attrs(destdir, 0750, TO_BACKEND);
    if (rc)
        return rc;

    int retry = 0;
    do {

        /* rename the entry in backend */
        DisplayLog(LVL_DEBUG, RBHEXT_TAG, "Moving entry in the backend: '%s'->'%s'",
                   old_bk_path, new_bk_path);
        if (rename(old_bk_path, new_bk_path))
        {
            rc = -errno;

            /* only retry once if error is EXDEV */
            if (!retry && rc == -EXDEV)
            {
                char tmp2[RBH_PATH_MAX];
                char *fname;

                DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Could not move entry in the backend "
                            "because target path is in different device (error EXDEV): '%s'->'%s'",
                            old_bk_path, new_bk_path );

                /* try to move file from one backend fileset to another:
                 * in this case, just change filename within the same directory
                 */
                /* 1-extract current dirname in backend */
                strcpy(tmp, old_bk_path);
                destdir = dirname(tmp);
                /* 2-extract new filename */
                strcpy(tmp2, fs_path);
                fname = basename(tmp2);
                /* 3-build the new backend path */
    #ifdef  _HAVE_FID
                sprintf(new_bk_path, "%s/%s__"DFID_NOBRACE, destdir, fname,
                        PFID(new_id));
    #else
                sprintf(new_bk_path, "%s/%s__%#LX:%#LX", destdir, fname,
                     (unsigned long long)new_id->device,
                     (unsigned long long)new_id->inode );
    #endif
                retry = 1;

                DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Trying to rename to '%s' instead", new_bk_path);
                continue;
            }
            else
            {
                DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Could not move entry in the backend ('%s'->'%s'): %s",
                            old_bk_path, new_bk_path, strerror(-rc) );
                /* keep the old path */
                strcpy( new_bk_path, old_bk_path);
                return rc;
            }
        }
        /* rename succeeded */
        retry = 0;
    } while(retry);

#ifdef HAVE_SHOOK
    /* save new backendpath to filesystem */
    /* XXX for now, don't manage several hsm_index */
    rc = shook_set_hsm_info(fidpath, new_bk_path, 0);
    if (rc)
        DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Could not set backend path for "DFID": error %d",
                    PFID(new_id), rc );
#endif

    return rc;
}


int rbhext_release( const entry_id_t * p_id,
                    attr_set_t * p_attrs )
{
#ifndef HAVE_PURGE_POLICY
    return -ENOTSUP;
#else
    int rc;
    obj_type_t entry_type;

    /* if status is not determined, retrieve it */
    if ( !ATTR_MASK_TEST(p_attrs, status) )
    {
        DisplayLog( LVL_DEBUG, RBHEXT_TAG, "Status not provided to rbhext_release()" );
        rc = rbhext_get_status( p_id, p_attrs, p_attrs );
        if (rc)
            return rc;
    }

    /* is it the good type? */
    if ( !ATTR_MASK_TEST(p_attrs, type) )
    {
        DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Missing mandatory attribute 'type' in %s()",
                    __FUNCTION__ );
        return -EINVAL;
    }

    entry_type = ListMgr2PolicyType(ATTR(p_attrs, type));
    if ( entry_type != TYPE_FILE )
    {
        DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Unsupported type for release operation: %s",
                    ATTR(p_attrs, type) );
        return -ENOTSUP;
    }

    return shook_release(get_fsname(), p_id);
#endif
}
