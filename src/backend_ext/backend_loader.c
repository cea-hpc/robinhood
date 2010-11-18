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

backend_t	backend;

#define BACKEND_BLOCK "Backend"


int SetDefault_Backend_Config( void *module_config, char *msg_out )
{
    backend_config_t *conf = ( backend_config_t * ) module_config;
    msg_out[0] = '\0';
    conf->opt_string[0] = '\0';

    return 0;
}

int Write_Backend_ConfigDefault( FILE * output )
{
    print_begin_block( output, 0, BACKEND_BLOCK, NULL );
    print_line( output, 1, "opt_string    : \"\"" );
    print_end_block( output, 0 );
    return 0;
}

int Read_Backend_Config( config_file_t config, void *module_config, char *msg_out, int for_reload )
{
    int            rc;
    backend_config_t *conf = ( backend_config_t * ) module_config;

    static const char *allowed_params[] = { "opt_string" };

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

    rc = GetStringParam( block, BACKEND_BLOCK, "opt_string",0,
                         conf->opt_string, OPT_STRING_MAX, NULL, NULL, msg_out );
    if ( rc )
        return rc;

    CheckUnknownParameters( block, BACKEND_BLOCK, allowed_params );

    return 0;
}
int Reload_Backend_Config( void *module_config )
{
    return 0;
}

int Write_Backend_ConfigTemplate( FILE * output )
{

    print_begin_block( output, 0, BACKEND_BLOCK, NULL );
    print_line( output, 1, "# opt_string has the same syntax as opts in mount -o <opts>");
    print_line( output, 1, "# for basic backend, possible options are:" );
    print_line( output, 1, "# root=<root_path>,action_cmd=<cmd_path>,xattrs,timeout=<sec>");
    print_line( output, 1, "opt_string = \"root=/backend\"; " );
    print_end_block( output, 0 );
    return 0;
}
#define BKL_TAG "BkLoader"

int Backend_Start( backend_config_t * config, int flags )
{
    int rc;
	unsigned int behav_flags, compat_flags;

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

	rc = rbhext_init( config->opt_string, &behav_flags );
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

    return 0;
}

int Backend_Stop()
{
    /* TODO Backend_Stop() */
    return 0;
}
