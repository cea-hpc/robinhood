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
#include "RobinhoodConfig.h"
#include "RobinhoodMisc.h"

backend_t	backend;

#define BACKEND_BLOCK "Backend"
#define BKL_TAG "BkLoader"

int SetDefault_Backend_Config( void *module_config, char *msg_out )
{
    backend_config_t *conf = ( backend_config_t * ) module_config;
    msg_out[0] = '\0';

    strcpy( conf->root, "/backend" );
    strcpy( conf->mnt_type, "nfs" );
    strcpy(conf->action_cmd, ""); /* default is built-in */
#ifdef HAVE_SHOOK
    strcpy( conf->shook_cfg, "/etc/shook.cfg" );
#endif
    conf->copy_timeout = 21600; /* =6h (0=disabled) */
    conf->xattr_support = FALSE;
    conf->check_mounted = TRUE;
    conf->archive_symlinks = TRUE;
    conf->sync_archive_data = TRUE;
    conf->compress = FALSE;
    conf->sendfile = FALSE;
    return 0;
}

int Write_Backend_ConfigDefault( FILE * output )
{
    print_begin_block( output, 0, BACKEND_BLOCK, NULL );
    print_line( output, 1, "root          : \"/backend\"" );
    print_line( output, 1, "mnt_type      : nfs ");
    print_line(output, 1, "action_cmd    : <built-in copy>");
    print_line(output, 1, "compress      : FALSE");
    print_line(output, 1, "sendfile      : FALSE");
#ifdef HAVE_SHOOK
     print_line( output, 1, "shook_cfg    : \"/etc/shook.cfg\"" );
#endif
    print_line(output, 1, "copy_timeout  : 6h");
    print_line(output, 1, "xattr_support : FALSE");
    print_line(output, 1, "check_mounted : TRUE");
    print_line(output, 1, "archive_symlinks: TRUE");
    print_line(output, 1, "sync_archive_data: TRUE");
    print_end_block(output, 0);
    return 0;
}

int Read_Backend_Config( config_file_t config, void *module_config, char *msg_out, int for_reload )
{
    int            rc, tmpval;
    backend_config_t *conf = ( backend_config_t * ) module_config;

    static const char *allowed_params[] = {
        "root", "mnt_type", "action_cmd", "copy_timeout",
#ifdef HAVE_SHOOK
        "shook_cfg",
#endif
        "xattr_support", "check_mounted", "archive_symlinks",
        "sync_archive_data", "compress", "sendfile", NULL};

    /* get Backend block */

    config_item_t  block = rh_config_FindItemByName( config, BACKEND_BLOCK );

    if ( block == NULL )
	return 0; /* no error, it is not mandatory */

    if ( rh_config_ItemType( block ) != CONFIG_ITEM_BLOCK )
    {
        strcpy( msg_out, "A block is expected for '" BACKEND_BLOCK "' item" );
        return EINVAL;
    }

    /* retrieve parameters */

    rc = GetStringParam( block, BACKEND_BLOCK, "root",
                         STR_PARAM_ABSOLUTE_PATH | STR_PARAM_REMOVE_FINAL_SLASH |
                         STR_PARAM_NO_WILDCARDS, conf->root, RBH_PATH_MAX,
                         NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

#ifdef HAVE_SHOOK
    rc = GetStringParam( block, BACKEND_BLOCK, "shook_cfg",
                         STR_PARAM_ABSOLUTE_PATH | STR_PARAM_NO_WILDCARDS,
                         conf->shook_cfg, RBH_PATH_MAX,
                         NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
#endif

    rc = GetStringParam( block, BACKEND_BLOCK, "mnt_type", 0,
                         conf->mnt_type, RBH_NAME_MAX, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetStringParam(block, BACKEND_BLOCK, "action_cmd", 0,
                        conf->action_cmd, RBH_PATH_MAX,
                        NULL, NULL, msg_out);
    if (rc == ENOENT) /* use built-in copy */
        conf->action_cmd[0] = '\0';
    else if (rc != 0) /* error */
        return rc;
    else if (EMPTY_STRING(conf->action_cmd))
    /* rc == 0 and cmd is empty: perhaps the user wanted to perform dry-run copy.
       so notify him it will use the built-in copy */
    {
        DisplayLog(LVL_MAJOR, "BkConfig", "Notice: "BACKEND_BLOCK"::action_cmd "
                   "is empty in config file: will use built-in copy");
    }

    rc = GetStringParam( block, BACKEND_BLOCK, "mnt_type", 0,
                         conf->mnt_type, RBH_NAME_MAX, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetDurationParam( block, BACKEND_BLOCK, "copy_timeout",
                           INT_PARAM_POSITIVE,    /* 0 is authorized => no timeout */
                           ( int * ) &conf->copy_timeout, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    /* /!\ xattr_support is part of a bit field, it should not be passed directly: using tmpval instead */
    rc = GetBoolParam( block, BACKEND_BLOCK, "xattr_support",
                       0, &tmpval, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc != ENOENT )
        conf->xattr_support = tmpval;

    /* /!\ check_mounted is part of a bit field, it should not be passed directly: using tmpval instead */
    rc = GetBoolParam( block, BACKEND_BLOCK, "check_mounted",
                       0, &tmpval, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc != ENOENT )
        conf->check_mounted = tmpval;

    /* /!\ archive_symlinks is part of a bit field, it should not be passed directly: using tmpval instead */
    rc = GetBoolParam( block, BACKEND_BLOCK, "archive_symlinks",
                       0, &tmpval, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc != ENOENT )
        conf->archive_symlinks = tmpval;

    /* /!\ sync_archive_data is part of a bit field, it should not be passed directly: using tmpval instead */
    rc = GetBoolParam(block, BACKEND_BLOCK, "sync_archive_data",
                      0, &tmpval, NULL, NULL, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    else if (rc != ENOENT)
        conf->sync_archive_data = tmpval;

    rc = GetBoolParam(block, BACKEND_BLOCK, "compress", 0, &tmpval, NULL,
                      NULL, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    else if (rc != ENOENT)
        conf->compress = tmpval;

    if (conf->compress && !EMPTY_STRING(conf->action_cmd))
    {
        DisplayLog(LVL_MAJOR, BKL_TAG, "Warning: enabling compression is only allowed for built-in copy action: disabling compression");
        conf->compress = 0;
    }

    rc = GetBoolParam(block, BACKEND_BLOCK, "sendfile", 0, &tmpval, NULL,
                      NULL, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    else if (rc != ENOENT)
        conf->sendfile = tmpval;

    CheckUnknownParameters(block, BACKEND_BLOCK, allowed_params);

    return 0;
}
int Reload_Backend_Config( void *module_config )
{
    return 0;
}

int Write_Backend_ConfigTemplate( FILE * output )
{
    print_begin_block( output, 0, BACKEND_BLOCK, NULL );
    print_line( output, 1, "# backend path and type" );
    print_line( output, 1, "root          = \"/backend\";" );
    print_line( output, 1, "mnt_type      = nfs;");
#ifdef HAVE_SHOOK
    print_line( output, 1, "# shook server configuration" );
    print_line( output, 1, "shook_cfg     = \"/etc/shook.cfg\";" );
#endif
    print_line(output, 1, "# By default, the copy is done by a built-in function");
    print_line(output, 1, "# Uncomment the following line to use an external command.");
    print_line(output, 1, "# /!\\ Calling an external command introduce an extra cost.");
    print_line(output, 1, "#action_cmd    = \"/usr/sbin/rbhext_tool\";");
    print_line(output, 1, "# compress data in archive (built-in copy only)");
    print_line(output, 1, "#compress = yes;");
    print_line(output, 1, "copy_timeout  = 6h;");
    print_line(output, 1, "xattr_support = FALSE;");
    print_line(output, 1, "# check if the backend is mounted on startup");
    print_line(output, 1, "check_mounted = TRUE; ");
    print_line(output, 1, "# archive symlinks to the backend?");
    print_line(output, 1, "archive_symlinks = TRUE; ");
    print_line(output, 1, "# flush archive data on close, to make sure the copy");
    print_line(output, 1, "# is really successful (performance impact for small files)");
    print_line(output, 1, "sync_archive_data = TRUE; ");
    print_line(output, 1, "# use sendfile() to efficiently copy files");
    print_line(output, 1, "# Requires fallocate() and file-to-file sendfile()");
    print_line(output, 1, "#sendfile = TRUE; ");
    print_end_block(output, 0);
    return 0;
}

int Backend_Start( backend_config_t * config, int flags )
{
    int rc;
	unsigned int behav_flags, compat_flags;

    DisplayLog(LVL_DEBUG, BKL_TAG, "Backend extension config:");
    DisplayLog(LVL_DEBUG, BKL_TAG, "root             =   \"%s\"", config->root );
    DisplayLog(LVL_DEBUG, BKL_TAG, "mnt_type         =   %s", config->mnt_type );
#ifdef HAVE_SHOOK
    DisplayLog(LVL_DEBUG, BKL_TAG, "shook_cfg        =   \"%s\"", config->shook_cfg );
#endif
    DisplayLog(LVL_DEBUG, BKL_TAG, "check_mounted    =   %s", bool2str(config->check_mounted));
    if (EMPTY_STRING(config->action_cmd))
        DisplayLog(LVL_DEBUG, BKL_TAG, "action_cmd       =   <built-in copy>");
    else
        DisplayLog(LVL_DEBUG, BKL_TAG, "action_cmd       =   \"%s\"", config->action_cmd);
    DisplayLog(LVL_DEBUG, BKL_TAG, "compress         =  %s",  bool2str(config->compress));
    DisplayLog(LVL_DEBUG, BKL_TAG, "copy_timeout     =   %us", config->copy_timeout );
    DisplayLog(LVL_DEBUG, BKL_TAG, "xattr_support    =   %s",  bool2str(config->xattr_support));
    DisplayLog(LVL_DEBUG, BKL_TAG, "archive_symlinks =   %s",  bool2str(config->archive_symlinks));
    DisplayLog(LVL_DEBUG, BKL_TAG, "sync_archive_data =  %s",  bool2str(config->sync_archive_data));
    DisplayLog(LVL_DEBUG, BKL_TAG, "sendfile         =  %s",  bool2str(config->sendfile));

    /* first check compatibility flags */
    compat_flags = rbhext_compat_flags();
    if ( compat_flags & RBHEXT_COMPAT_LUSTRE )
    {
        if (strcmp(global_config.fs_type, "lustre" ))
        {
            DisplayLog( LVL_CRIT, BKL_TAG, "Error: this backend only supports Lustre filesystems" );
            return -ENOTSUP;
        }
    }

	rc = rbhext_init( config, &behav_flags );
	if (rc)
	{
		DisplayLog( LVL_CRIT, BKL_TAG, "Error %d initializing backend: %s",
		            rc, strerror(-rc) );
		return rc;
	}

    if ( behav_flags & RBHEXT_ASYNC_ARCHIVE )
        backend.async_archive = 1;
    else if ( !(behav_flags & RBHEXT_SYNC_ARCHIVE) )
    {
		DisplayLog(LVL_CRIT, BKL_TAG, "Error: this backend does not support any archive method (sync or async)");
        return -EINVAL;
    }
    else
        backend.async_archive = 0;

    if ( behav_flags & RBHEXT_RM_SUPPORT )
        backend.rm_support = 1;
    else
        backend.rm_support = 0;

    if ( behav_flags & RBHEXT_RELEASE_SUPPORT )
        backend.release_support = 1;
    else
        backend.release_support = 0;

    return 0;
}

int Backend_Stop()
{
    /* TODO Backend_Stop() */
    return 0;
}
