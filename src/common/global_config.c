/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2008, 2009 CEA/DAM
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

#include "global_config.h"
#include "RobinhoodConfig.h"
#include "RobinhoodMisc.h"
#include <errno.h>

#define GLOBAL_CONFIG_BLOCK "General"


/* exported variable available to all modules */
global_config_t global_config;


/* name 2 fskey value */
static inline fs_key_t name2fskey( const char * name )
{
    if (!strcasecmp(name, "fsname"))
        return FSKEY_FSNAME;
    else if (!strcasecmp(name, "fsid"))
        return FSKEY_FSID;
    else if (!strcasecmp(name, "devid"))
        return FSKEY_DEVID;
    else
        return FSKEY_ERROR;
}

int SetDefaultGlobalConfig( void *module_config, char *msg_out )
{
    global_config_t *conf = ( global_config_t * ) module_config;
    msg_out[0] = '\0';

    rh_strncpy(conf->fs_path, "", RBH_PATH_MAX);
#ifdef _HAVE_FID
    rh_strncpy(conf->fs_type, "lustre", FILENAME_MAX);
#else
    rh_strncpy(conf->fs_type, "", FILENAME_MAX);
#endif
    rh_strncpy(conf->lock_file, "/var/locks/robinhood.lock", RBH_PATH_MAX);
    conf->stay_in_fs = TRUE;
    conf->check_mounted = TRUE;
    conf->fs_key = FSKEY_FSNAME;

#if defined( _LUSTRE ) && defined( _MDS_STAT_SUPPORT )
    conf->direct_mds_stat = FALSE;
#endif


    return 0;
}

int WriteGlobalConfigDefault( FILE * output )
{
    print_begin_block( output, 0, GLOBAL_CONFIG_BLOCK, NULL );
    print_line( output, 1, "fs_path       :  [MANDATORY]" );
#ifdef _HAVE_FID
    print_line( output, 1, "fs_type       :  lustre" );
#else
    print_line( output, 1, "fs_type       :  [MANDATORY]" );
#endif
    print_line( output, 1, "fs_key        :  fsname" );
    print_line( output, 1, "lock_file     :  \"/var/locks/robinhood.lock\"" );
    print_line( output, 1, "stay_in_fs    :  TRUE" );
    print_line( output, 1, "check_mounted :  TRUE" );

#if defined( _LUSTRE ) && defined( _MDS_STAT_SUPPORT )
    print_line( output, 1, "direct_mds_stat :   FALSE" );
#endif

    print_end_block( output, 0 );
    return 0;
}

int ReadGlobalConfig( config_file_t config, void *module_config, char *msg_out, int for_reload )
{
    int            rc, tmpval;
    global_config_t *conf = ( global_config_t * ) module_config;

    static const char *allowed_params[] = {
        "fs_path", "fs_type", "lock_file", "stay_in_fs", "check_mounted",
        "direct_mds_stat", "fs_key",
NULL
    };

    /* get GENERAL block */
    config_item_t  general_block = rh_config_FindItemByName( config, GLOBAL_CONFIG_BLOCK );

    if ( general_block == NULL )
    {
        strcpy( msg_out, "Missing configuration block '" GLOBAL_CONFIG_BLOCK "'" );
        return ENOENT;
    }

    if ( rh_config_ItemType( general_block ) != CONFIG_ITEM_BLOCK )
    {
        strcpy( msg_out, "A block is expected for '" GLOBAL_CONFIG_BLOCK "' item" );
        return EINVAL;
    }

    /* retrieve parameters */

    rc = GetStringParam( general_block, GLOBAL_CONFIG_BLOCK, "fs_path",
                         PARAM_MANDATORY | STR_PARAM_ABSOLUTE_PATH |
                         STR_PARAM_REMOVE_FINAL_SLASH |
                         STR_PARAM_NO_WILDCARDS, conf->fs_path, RBH_PATH_MAX,
                         NULL, NULL, msg_out );
    if ( rc )
        return rc;

#ifdef _HAVE_FID
    rc = GetStringParam( general_block, GLOBAL_CONFIG_BLOCK, "fs_type",
                         PARAM_MANDATORY, conf->fs_type, RBH_NAME_MAX, NULL, NULL, msg_out );
    if ( ( rc != ENOENT ) && strcmp( conf->fs_type, "lustre" ) )
    {
#ifdef _LUSTRE_HSM
        strcpy( msg_out, "Only \"lustre\" filesystem type is allowed for Lustre-HSM purpose" );
#else
        strcpy( msg_out, "Robinhood is compiled for Lustre filesystem support only" );
#endif
        return EINVAL;
    }
#else
    rc = GetStringParam( general_block, GLOBAL_CONFIG_BLOCK, "fs_type",
                         PARAM_MANDATORY, conf->fs_type, RBH_NAME_MAX, NULL, NULL, msg_out );
    if ( rc )
        return rc;
#endif

    rc = GetStringParam( general_block, GLOBAL_CONFIG_BLOCK, "lock_file",
                         STR_PARAM_ABSOLUTE_PATH | STR_PARAM_NO_WILDCARDS,
                         conf->lock_file, RBH_PATH_MAX, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    /* /!\ stay_in_fs is a piece of bit field, it should not be passed directly: using tmpval instead */
    rc = GetBoolParam( general_block, GLOBAL_CONFIG_BLOCK, "stay_in_fs",
                       0, &tmpval, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc != ENOENT )
        conf->stay_in_fs = tmpval;

    /* /!\ idem for check_mounted */
    rc = GetBoolParam( general_block, GLOBAL_CONFIG_BLOCK, "check_mounted",
                       0, &tmpval, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc != ENOENT )
        conf->check_mounted = tmpval;

    /* fs_key param */
    char tmpstr[128];
    rc = GetStringParam( general_block, GLOBAL_CONFIG_BLOCK, "fs_key",
                         STR_PARAM_NO_WILDCARDS, tmpstr, 128, NULL, NULL,
                         msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if (rc == 0)
    {
        conf->fs_key = name2fskey(tmpstr);
        if (conf->fs_key == FSKEY_ERROR)
        {
            sprintf( msg_out, "Invalid type for fs_key: '%s' ('fsname', 'devid' or 'fsid' expected)", tmpstr );
            return EINVAL;
        }
    }

#if defined( _LUSTRE ) && defined( _MDS_STAT_SUPPORT )
    rc = GetBoolParam( general_block, GLOBAL_CONFIG_BLOCK,
                       "direct_mds_stat",0, &conf->direct_mds_stat,
                       NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
#endif

    /* check unknown parameters */
    CheckUnknownParameters( general_block, GLOBAL_CONFIG_BLOCK, allowed_params );


    return 0;
}

int ReloadGlobalConfig( void *module_config )
{
    global_config_t *conf = ( global_config_t * ) module_config;

    if ( strcmp( conf->fs_path, global_config.fs_path ) )
        DisplayLog( LVL_MAJOR, "GlobalConfig",
                    GLOBAL_CONFIG_BLOCK
                    "::fs_path changed in config file, but cannot be modified dynamically" );
    if ( strcmp( conf->fs_type, global_config.fs_type ) )
        DisplayLog( LVL_MAJOR, "GlobalConfig",
                    GLOBAL_CONFIG_BLOCK
                    "::fs_type changed in config file, but cannot be modified dynamically" );

    if ( strcmp( conf->lock_file, global_config.lock_file ) )
    {
        DisplayLog( LVL_MAJOR, "GlobalConfig",
                    GLOBAL_CONFIG_BLOCK "::lock_file updated: '%s'->'%s'",
                    global_config.lock_file, conf->lock_file );
        strcpy( global_config.fs_path, conf->lock_file );
    }

    if ( global_config.stay_in_fs != conf->stay_in_fs )
    {
        DisplayLog( LVL_EVENT, "GlobalConfig", GLOBAL_CONFIG_BLOCK "::stay_in_fs updated: %s->%s",
                    bool2str( global_config.stay_in_fs ), bool2str( conf->stay_in_fs ) );
        global_config.stay_in_fs = conf->stay_in_fs;
    }

    if ( global_config.check_mounted != conf->check_mounted )
    {
        DisplayLog( LVL_EVENT, "GlobalConfig",
                    GLOBAL_CONFIG_BLOCK "::check_mounted updated: %s->%s",
                    bool2str( global_config.check_mounted ), bool2str( conf->check_mounted ) );
        global_config.check_mounted = conf->check_mounted;
    }

#if defined( _LUSTRE ) && defined( _MDS_STAT_SUPPORT )
    if ( conf->direct_mds_stat != global_config.direct_mds_stat )
    {
        DisplayLog( LVL_EVENT, "FS_Scan_Config",
                    GLOBAL_CONFIG_BLOCK "::direct_mds_stat updated: %u->%u",
                    global_config.direct_mds_stat, conf->direct_mds_stat );
        global_config.direct_mds_stat = conf->direct_mds_stat;
    }
#endif

    return 0;

}

int WriteGlobalConfigTemplate( FILE * output )
{
    print_begin_block( output, 0, GLOBAL_CONFIG_BLOCK, NULL );

#ifdef _HAVE_FID
    print_line( output, 1, "# filesystem to be monitored" );
    print_line( output, 1, "fs_path = \"/mnt/lustre\" ;" );
    fprintf( output, "\n" );
#else
    print_line( output, 1, "# filesystem to be monitored" );
    print_line( output, 1, "fs_path = \"/tmp\" ;" );
    fprintf( output, "\n" );
    print_line( output, 1, "# filesystem type (as returned by 'df' or 'mount' commands)" );
    print_line( output, 1, "fs_type = \"ext3\" ;" );
    fprintf( output, "\n" );
    print_line( output, 1, "# filesystem property used as FS key: fsname, devid or fsid (fsid NOT recommended)");
    print_line( output, 1, "fs_key = fsname ;" );
    fprintf( output, "\n" );
#endif

    print_line( output, 1, "# file for suspending all actions" );
    print_line( output, 1, "lock_file = \"/var/locks/robinhood.lock\" ;" );
    fprintf( output, "\n" );
    print_line( output, 1, "# check that objects are in the same device as 'fs_path'," );
    print_line( output, 1, "# so it will not traverse mount points" );
    print_line( output, 1, "stay_in_fs = TRUE ;" );
    fprintf( output, "\n" );
    print_line( output, 1, "# check that the filesystem is mounted" );
    print_line( output, 1, "check_mounted = TRUE ;" );

#if defined( _LUSTRE ) && defined( _MDS_STAT_SUPPORT )
    fprintf( output, "\n" );
    print_line( output, 1, "# File info is asked directly to MDS on Lustre filesystems");
    print_line( output, 1, "# (scan faster, but size information is missing)");
    print_line( output, 1, "direct_mds_stat        =    FALSE ;" );
#endif

    print_end_block( output, 0 );
    return 0;
}
