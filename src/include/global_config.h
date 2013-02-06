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

/**
 * \file  global_config.h
 * \brief Global configuration parameters
 */
#ifndef _GENERAL_H
#define _GENERAL_H

#include "rbh_const.h"
#include "config_parsing.h"
#include <sys/param.h>          /* for RBH_PATH_MAX */
#include <stdio.h>

typedef enum {
    FSKEY_ERROR=0,
    FSKEY_FSNAME,
    FSKEY_FSID,
    FSKEY_DEVID
} fs_key_t;

/**
 * General Robinhood configuration
 */
typedef struct global_config_t
{
    /* filesystem description */

    char           fs_path[RBH_PATH_MAX];
    char           fs_type[FILENAME_MAX];

    /* lock file */
    char           lock_file[RBH_PATH_MAX];

    fs_key_t      fs_key;

    /* behavior flags */
    int            stay_in_fs:1;
    int            check_mounted:1;

#if defined( _LUSTRE ) && defined ( _MDS_STAT_SUPPORT )
    /** Direct stat to MDS on Lustre filesystems */
    int            direct_mds_stat;
#endif

} global_config_t;

/**
 * \addtogroup MODULE_CONFIG_FUNCTIONS
 * @{
 */
int            SetDefaultGlobalConfig( void *module_config, char *msg_out );
int            ReadGlobalConfig( config_file_t config, void *module_config,
                                 char *msg_out, int for_reload );
int            ReloadGlobalConfig( void *module_config );
int            WriteGlobalConfigTemplate( FILE * output );
int            WriteGlobalConfigDefault( FILE * output );
/** @} */

/** global config structure available to all modules */
extern global_config_t global_config;



#endif
