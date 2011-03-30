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
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include <utime.h>
#include <libgen.h>
#include <pwd.h>
#include <grp.h>

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

    /* check that backend filesystem is mounted */
    rc = CheckFSInfo( config.root, config.mnt_type, &backend_dev,
                      config.check_mounted, FALSE );
    if ( rc )
        return -rc;

    return 0;
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

    /* support files and symlinks */
    if ( (entry_type != TYPE_FILE)
         && (entry_type != TYPE_LINK)
         && (entry_type != TYPE_NONE) )
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
static int entry2backend_path( const entry_id_t * p_id,
                               const attr_set_t * p_attrs_in,
                               what_for_e what_for,
                               char * backend_path )
{
    int pathlen;

    if ( ATTR_MASK_TEST(p_attrs_in, backendpath) )
       DisplayLog( LVL_DEBUG, RBHEXT_TAG, "%s: previous backend_path: %s",
                   (what_for == FOR_LOOKUP)?"LOOKUP":"NEW_COPY",
                   ATTR(p_attrs_in, backendpath) );

    if ( (what_for == FOR_LOOKUP) && ATTR_MASK_TEST(p_attrs_in, backendpath) )
    {
        /* For lookup, if there is a previous path in the backend, use it. */
        strcpy(backend_path, ATTR(p_attrs_in, backendpath));
    }
    else /* in any other case, build a path from scratch */
    {
        char rel_path[RBH_PATH_MAX];

        /* if the fullpath is available, build human readable path */
        if ( ATTR_MASK_TEST(p_attrs_in, fullpath) &&
             relative_path( ATTR(p_attrs_in, fullpath), global_config.fs_path,
                            rel_path ) == 0 )
        {
            /* backend path is '<bakend_root>/<rel_path>' */

            if ( !strcmp(config.root, "/") ) /* root is '/' */
                sprintf(backend_path, "/%s", rel_path);
            else
                sprintf(backend_path, "%s/%s", config.root, rel_path);
        }
        else /* we don't have fullpath available */
        {
            const char * fname;

            if ( ATTR_MASK_TEST(p_attrs_in, name) )
                fname = ATTR(p_attrs_in, name);
            else
                fname = UNK_NAME;

            /* backup entry to a special dir */
            if ( !strcmp(config.root, "/") ) /* root is '/' */
                sprintf(backend_path, "/%s/%s", UNK_PATH, fname);
            else
                sprintf(backend_path, "%s/%s/%s", config.root, UNK_PATH, fname );
        }

        /* add __<id> after the name */
        pathlen = strlen(backend_path);
#ifdef  _HAVE_FID
        sprintf( backend_path + pathlen, "__"DFID_NOBRACE, PFID(p_id) );
#else
        sprintf( backend_path + pathlen, "__%#LX:%#LX",
                 (unsigned long long)p_id->device,
                 (unsigned long long)p_id->inode );
#endif
    }
    return 0;
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

    /* check if mtime is provided (mandatory) */
    if ( !ATTR_MASK_TEST(p_attrs_in, last_mod) || !ATTR_MASK_TEST(p_attrs_in, type) )
    {
        DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Missing mandatory attribute for checking entry status" );
        return -EINVAL;
    }

    /* path to lookup the entry in the backend */
    rc = entry2backend_path( p_id, p_attrs_in, FOR_LOOKUP, bkpath );
    if (rc)
    {
        DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Error %d building backend path: %s",
                    rc, strerror(-rc) );
        return rc;
    }

    /* is the entry has a supported type? */
    entry_type = ListMgr2PolicyType(ATTR(p_attrs_in, type));
    if ( (entry_type != TYPE_FILE) && (entry_type != TYPE_LINK) )
    {
        DisplayLog( LVL_EVENT, RBHEXT_TAG, "Unsupported type %s for this backend",
                    ATTR(p_attrs_in, type) );
        return -ENOTSUP;
    }

#ifdef HAVE_PURGE_POLICY
#ifdef HAVE_SHOOK
    /* @TODO check status from libshook.
     * return if status != ONLINE
     * else, continue checking.
     */

    /* @TODO: ignore shook special entries */
#endif
#endif

    if ( entry_type == TYPE_FILE )
    {
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
                ATTR_MASK_SET( p_attrs_changed, status );
                ATTR( p_attrs_changed, status ) = STATUS_ARCHIVE_RUNNING;
                return 0;
            }
        }
    }

    /* get entry info */
    if ( lstat( bkpath, &bkmd ) != 0 )
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
        if ( (ATTR( p_attrs_in, last_mod ) > bkmd.st_mtime )
             || (ATTR( p_attrs_in, size ) != bkmd.st_size ) )
        {
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
        if ( readlink(bkpath, lnk1, RBH_PATH_MAX ) < 0 )
        {
            rc = -errno;
            if ( rc == ENOENT )
            {
                /* entry disapeared */
                ATTR_MASK_SET( p_attrs_changed, status );
                ATTR( p_attrs_changed, status ) = STATUS_NEW;
                return 0;
            }
            else
                return rc;
        }
        if ( readlink(fspath, lnk2, RBH_PATH_MAX ) < 0 )
        {
            rc = -errno;
            DisplayLog( LVL_EVENT, RBHEXT_TAG, "Error performing readlink(%s): %s",
                        fspath, strerror(-rc) );
            return rc;
        }
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
static int mkdir_recurse( const char * full_path, mode_t default_mode,
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
        if ( strncmp(config.root,full_path, strlen(config.root)) != 0 )
        {
            DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Error: '%s' in not under backend root '%s'",
                        full_path, config.root );
            return -EINVAL;
        }
        /* skip backend root */
        curr = full_path + strlen(config.root);
    }
    else
    {
        if ( strncmp(global_config.fs_path,full_path, strlen(global_config.fs_path)) != 0 )
        {
            DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Error: '%s' in not under filesystem root '%s'",
                        full_path, global_config.fs_path );
            return -EINVAL;
        }
        /* skip fs root */
        curr = full_path + strlen(global_config.fs_path);
    }

    if ( *curr == '\0' ) /* full_path is root dir */
        return 0;
    else if ( *curr != '/' ) /* slash expected */
    {
        DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Error: '%s' in not under backend root '%s'",
                    full_path, (target == TO_BACKEND)?config.root:global_config.fs_path );
        return -EINVAL;
    }

    /* skip first slash */
    curr ++;

    while( (curr = strchr( curr, '/' )) != NULL )
    {
         /* if fullpath = '/a/b',
         * curr = &(fullpath[2]);
         * so, copy 2 chars to get '/a'.
         * and set fullpath[2] = '\0'
         */
        int path_len = curr - full_path;

        /* extract directory name */
        strncpy( path_copy, full_path, path_len );
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
    }

    return 0;
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
    if ( (entry_type != TYPE_FILE) && (entry_type != TYPE_LINK) )
    {
        DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Unsupported type for archive operation: %s",
                    ATTR(p_attrs, type) );
        return -ENOTSUP;
    }

    /* compute path for target file */
    rc = entry2backend_path( p_id, p_attrs, FOR_NEW_COPY, bkpath );
    if (rc)
    {
        DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Error %d building backend path: %s",
                    rc, strerror(-rc) );
        return rc;
    }

    /* check the status */
    if ( ATTR(p_attrs, status) == STATUS_NEW )
    {
        /* check the entry does not already exist */
        if ( (access(bkpath, F_OK) == 0) || (errno != ENOENT) )
        {
            rc = -errno;
            DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Error: new entry %s already exist. errno=%d, %s",
                        bkpath, -rc, strerror(-rc) );
            return rc;
        }
    }
    else if ( ATTR(p_attrs, status) == STATUS_MODIFIED )
    {
       /* check that previous path exists */
        if ( ATTR_MASK_TEST(p_attrs, backendpath) )
        {
            /* need to check if the entry was renamed */
            check_moved = TRUE;
            if ( access(ATTR(p_attrs,backendpath), F_OK) != 0 )
            {
                rc = -errno;
                DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Warning: previous copy %s not found in backend. errno=%d, %s",
                            ATTR(p_attrs,backendpath) , -rc, strerror(-rc) );
                return rc;
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
    rc = mkdir_recurse( destdir, 0750, TO_BACKEND );
    if ( rc )
        return rc;

    if ( entry_type == TYPE_FILE )
    {
        /* temporary copy path */
        sprintf( tmp, "%s.%s", bkpath, COPY_EXT );

        /* execute the archive command */
        if ( hints )
            rc = execute_shell_command( config.action_cmd, 4, "ARCHIVE", fspath, tmp, hints);
        else
            rc = execute_shell_command( config.action_cmd, 3, "ARCHIVE", fspath, tmp );

        if (rc)
        {
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
    }

    return 0;
}

/**
 * Performs entry removal in the backend
 * \param[in] p_id pointer to id of entry to be archived
 * \param[in,out] p_attrs pointer to entry attributes
 *                        must be updated even on failure
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
                DisplayLog( LVL_EVENT, RBHEXT_TAG, "'%s' not found in backend: "
                            "assuming backend removal is successfull",
                            backend_path );
                return 0;
            }
            else
            {
                DisplayLog( LVL_EVENT, RBHEXT_TAG, "Error removing '%s' from backend: %s",
                            backend_path, strerror(-rc) );
                return -rc;
            }
        }
    }
    return 0;
}

/** recover a file from the backend after formatting FS
 * \retval recovery status
 */
recov_status_t rbhext_recover( const entry_id_t * p_old_id,
                               attr_set_t * p_attrs_old,
                               entry_id_t * p_new_id,
                               attr_set_t * p_attrs_new )
{
    char bkpath[RBH_PATH_MAX];
    const char * backend_path;
    const char * fspath;
    char tmp[RBH_PATH_MAX];
    char * destdir;
    int rc;
    struct stat st_bk;
    struct stat st_dest;
    int delta = FALSE;
    attr_set_t attr_bk;

    if ( !ATTR_MASK_TEST( p_attrs_old, fullpath ) )
    {
        DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Missing mandatory attribute 'fullpath' for restoring entry "DFID, PFID(p_old_id) );
        return RS_ERROR;
    }
    fspath = ATTR( p_attrs_old, fullpath );

    /* if there is no backend path, try to guess */
    if ( !ATTR_MASK_TEST( p_attrs_old, backendpath) )
    {
        rc = entry2backend_path( p_old_id, p_attrs_old, FOR_LOOKUP, bkpath );
        if ( rc == 0 )
        {
            DisplayLog( LVL_EVENT, RBHEXT_TAG,
                        "No backend path is set for '%s', guess it could be '%s'",
                        fspath, bkpath );
            backend_path = bkpath;
        }
        else
        {
            DisplayLog( LVL_MAJOR, RBHEXT_TAG,
                        "Cannot determine backend path for '%s'",
                        fspath );
            return RS_ERROR;
        }
    }
    else
        backend_path = ATTR( p_attrs_old, backendpath );

    /* test if this copy exists */
    if ( lstat( backend_path, &st_bk ) != 0 )
    {
        rc = -errno;
        DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Cannot stat '%s' in backend: %s",
                    backend_path, strerror(-rc) );
        if (rc == -ENOENT )
            return RS_NOBACKUP;
        else
            return RS_ERROR;
    }

    /* merge missing posix attrs to p_attrs_old */
    PosixStat2EntryAttr( &st_bk, &attr_bk, TRUE );
    /* leave attrs unchanged if they are already set in p_attrs_old */
    ListMgr_MergeAttrSets( p_attrs_old, &attr_bk, FALSE );

    /* test if the target does not already exist */
    rc = lstat( ATTR(p_attrs_old, fullpath), &st_dest );
    if ( rc == 0 )
    {
        DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Error: cannot recover '%s': already exists",
                    fspath );
        return RS_ERROR;
    }
    else if ( (rc = -errno) != -ENOENT )
    {
        DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Unexpected error performing lstat(%s): %s",
                    fspath, strerror(-rc) );
        return RS_ERROR;
    }

    /* recursively create the parent directory */
    /* extract dir path */
    strcpy( tmp, fspath );
    destdir = dirname( tmp );
    if ( destdir == NULL )
    {
        DisplayLog( LVL_CRIT, RBHEXT_TAG, "Error extracting directory path of '%s'",
                    fspath );
        return -EINVAL;
    }

    rc = mkdir_recurse( destdir, 0750, TO_FS );
    if (rc)
        return RS_ERROR;

#ifdef _LUSTRE
    /* restripe the file in Lustre */
    if ( ATTR_MASK_TEST( p_attrs_old, stripe_info ) )
        File_CreateSetStripe( fspath, &ATTR( p_attrs_old, stripe_info ) );
#endif

    /* restore entry */
    if ( S_ISREG( st_bk.st_mode ) )
    {
#ifdef HAVE_PURGE_POLICY
    /* @TODO this backend is restore/release capable.
     * Recover the entry in released state (md only),
     * so it will be recovered at first open.
     */
#else
        struct utimbuf utb;

        rc = execute_shell_command( config.action_cmd, 3, "RESTORE",
                                    backend_path, fspath );
        if (rc)
            return RS_ERROR;
        /* TODO: remove partial copy */

        /* set the same mode as in the backend */
        DisplayLog( LVL_FULL, RBHEXT_TAG, "Restoring mode for '%s': mode=%#o",
                    fspath, st_bk.st_mode & 07777 );
        if ( chmod( fspath, st_bk.st_mode & 07777 ) )
            DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Warning: couldn't restore mode for '%s': %s",
                        fspath, strerror(errno) );

        /* set the same mtime as in the backend */
        DisplayLog( LVL_FULL, RBHEXT_TAG, "Restoring times for '%s': atime=%lu, mtime=%lu",
                    fspath, st_bk.st_atime, st_bk.st_mtime );
        utb.actime = st_bk.st_atime;
        utb.modtime = st_bk.st_mtime;
        if ( utime( fspath, &utb ) )
            DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Warning: couldn't restore times for '%s': %s",
                        fspath, strerror(errno) );
#endif
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

            if ( getpwnam_r( ATTR(p_attrs_old, owner ), &pw, buff, 4096, &p_pw ) != 0 )
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
            if ( getgrnam_r( ATTR(p_attrs_old, gr_name ), &gr, buff, 4096, &p_gr ) != 0 )
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
                        fspath, strerror(-rc) );
        }
    }

    if ( lstat( fspath, &st_dest ) )
    {
        rc = -errno;
        DisplayLog( LVL_CRIT, RBHEXT_TAG, "ERROR: lstat() failed on restored entry '%s': %s",
                    fspath, strerror(-rc) );
        return RS_ERROR;
    }

    /* compare restored size and mtime with the one saved in the DB (for warning purpose) */
    if ( ATTR_MASK_TEST(p_attrs_old, size) && ( st_dest.st_size != ATTR(p_attrs_old, size)) )
    {
        DisplayLog( LVL_MAJOR, RBHEXT_TAG, "%s: the restored size (%zu) is "
                    "different from the last known size in filesystem (%"PRIu64"): "
                    "it should have been modified in filesystem after the last backup.",
                    fspath, st_dest.st_size, ATTR(p_attrs_old, size) );
        delta = TRUE;
    }
    if ( ATTR_MASK_TEST( p_attrs_old, last_mod) && (st_dest.st_mtime != ATTR(p_attrs_old, last_mod)) )
    {
        DisplayLog( LVL_MAJOR, RBHEXT_TAG, "%s: the restored mtime (%lu) is "
                    "different from the last time in filesystem (%u): "
                    "it may have been modified in filesystem after the last backup.",
                    fspath, st_dest.st_mtime, ATTR(p_attrs_old, last_mod) );
        delta = TRUE;
    }

    /* set the new attributes */
    ATTR_MASK_INIT( p_attrs_new );
    PosixStat2EntryAttr( &st_dest, p_attrs_new, TRUE );
    strcpy( ATTR( p_attrs_new, fullpath ), fspath );
    ATTR_MASK_SET( p_attrs_new, fullpath );
    /* status is always synchro after a recovery */
    ATTR( p_attrs_new, status ) = STATUS_SYNCHRO; /* @TODO 'released' if only md import */
    ATTR_MASK_SET( p_attrs_new, status );

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

#ifdef _LUSTRE
    /* get the new stripe info */
    if ( File_GetStripeByPath( fspath,
                               &ATTR( p_attrs_new, stripe_info ),
                               &ATTR( p_attrs_new, stripe_items ) ) == 0 )
    {
        ATTR_MASK_SET( p_attrs_new, stripe_info );
        ATTR_MASK_SET( p_attrs_new, stripe_items );
    }
#endif

    /* set the new entry path in backend, according to the new fid */
    rc = entry2backend_path( p_new_id, p_attrs_new,
                             FOR_NEW_COPY,
                             ATTR(p_attrs_new, backendpath ) );
    if (rc)
        return RS_ERROR;
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

    rc = mkdir_recurse( destdir, 0750, TO_BACKEND );
    if (rc)
        return RS_ERROR;

    /* rename the entry in backend */
    if ( strcmp( ATTR(p_attrs_new, backendpath), backend_path ) != 0 )
    {
        DisplayLog( LVL_FULL, RBHEXT_TAG, "Moving the entry in backend: '%s'->'%s'",
                    backend_path, ATTR(p_attrs_new, backendpath) );
        if ( rename( backend_path, ATTR(p_attrs_new, backendpath) ) )
        {
            rc = -errno;
            DisplayLog( LVL_MAJOR, RBHEXT_TAG, "Could not move entry in backend ('%s'->'%s'): %s",
                        backend_path, ATTR(p_attrs_new, backendpath), strerror(-rc) );
            /* keep the old path */
            strcpy( ATTR(p_attrs_new, backendpath), backend_path );
        }
    }

    if (delta)
        return RS_DELTA;
    else
        return RS_OK;
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

    return -1;
#endif
}
