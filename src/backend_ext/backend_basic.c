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


/* options for getsubopt */
enum
{
    OPT_BACKEND_ROOT = 0,
    OPT_COPYTOOL,
    OPT_XATTRS,
    OPT_COPY_TIMEOUT,
};

char * const opts[] =
{
    [OPT_BACKEND_ROOT]  = "root",
    [OPT_COPYTOOL]      = "action_cmd",
    [OPT_XATTRS]        = "xattrs",
    [OPT_COPY_TIMEOUT]  = "timeout",
    NULL
};

static struct backend_config
{
    char root[MAXPATHLEN];
    char action_cmd[MAXPATHLEN];
    unsigned int copy_timeout; /* 0=disabled */
    unsigned int xattr_support:1;
} config = {
    .root = "/backend",
    .action_cmd = "/usr/sbin/rbhext_tool",
    .copy_timeout = 60*15, /* timeout 15min after last file operation */
    .xattr_support = 0
};


/**
 * Initialize the extension module.
 * \param[in] config_string specific config string (e.g path to config file...)
 * \param[out] p_behaviors_flags pointer to output mask that describes extension behavior
 */
int rbhext_init( const char * config_string,
                 unsigned int * p_behaviors_flags )
{
    char subopts[OPT_STRING_MAX];
    char * curr;
    char * value;
    int tmpval;
    int len;

    if ( strlen( config_string ) >= OPT_STRING_MAX )
        return -E2BIG;
    strcpy(subopts, config_string);

    /* the config string is in the getsubopt format: opt1=xxx,opt2,opt3=yyy */
    curr = subopts;
    while (*curr != '\0')
    {
        switch( getsubopt(&curr, opts, &value) )
        {
            case OPT_BACKEND_ROOT:
                if (value == NULL)
                {
                    DisplayLog(LVL_CRIT, RBHEXT_TAG, "Expected value for suboption '%s'",
                    opts[OPT_BACKEND_ROOT] );
                    return -EINVAL;
                }
                strcpy(config.root, value);

                /* remove final slash */
                len = strlen(config.root);
                if ( (len > 1) && (config.root[len-1] == '/' ))
                    config.root[len-1] = '\0';
                break;
            case OPT_COPYTOOL:
                if (value == NULL)
                {
                    DisplayLog(LVL_CRIT, RBHEXT_TAG, "Expected value for suboption '%s'",
                    opts[OPT_COPYTOOL] );
                    return -EINVAL;
                }
                strcpy(config.action_cmd, value);
                break;
            case OPT_COPY_TIMEOUT:
                if (value == NULL)
                {
                    DisplayLog(LVL_CRIT, RBHEXT_TAG, "Expected value for suboption '%s'",
                    opts[OPT_COPY_TIMEOUT] );
                    return -EINVAL;
                }
                tmpval = str2int(value);
                if ( tmpval < 0 )
                {
                    DisplayLog(LVL_CRIT, RBHEXT_TAG, "integer expected for suboption '%s'",
                    opts[OPT_COPY_TIMEOUT] );
                    return -EINVAL;
                }
                config.copy_timeout = tmpval;
                break;

            case OPT_XATTRS:
#ifdef HAVE_ATTR_XATTR_H
                config.xattr_support = 1;
#else
                DisplayLog(LVL_CRIT, RBHEXT_TAG, "Unsupported option %s: recompile robinhood with xattr support",
                           opts[OPT_XATTRS]);
                config.xattr_support = 0;
#endif
                break;
            default:
                /* Unknown suboption. */
                DisplayLog(LVL_CRIT, RBHEXT_TAG, "Unknown suboption '%s'", value);
                return -EINVAL;
        }
    }

    DisplayLog(LVL_DEBUG, RBHEXT_TAG, "Backend extension config:");
    DisplayLog(LVL_DEBUG, RBHEXT_TAG, "%s = '%s'", opts[OPT_BACKEND_ROOT], config.root );
    DisplayLog(LVL_DEBUG, RBHEXT_TAG, "%s = '%s'", opts[OPT_COPYTOOL], config.action_cmd );
    DisplayLog(LVL_DEBUG, RBHEXT_TAG, "%s = %u", opts[OPT_COPY_TIMEOUT], config.copy_timeout );
    DisplayLog(LVL_DEBUG, RBHEXT_TAG, "%s = %s", opts[OPT_XATTRS], bool2str(config.xattr_support));

    /* synchronous archiving and rm support */
    *p_behaviors_flags = RBHEXT_SYNC_ARCHIVE | RBHEXT_RM_SUPPORT;
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

    if ( entry_type == TYPE_FILE )
    {
        /* temporary copy path */
        sprintf( tmp, "%s.%s", bkpath, COPY_EXT );

        /* execute the archive command */
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
                DisplayLog( LVL_EVENT, RBHEXT_TAG, "'%s' not found in backend: assuming backend removal is successfull",
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
