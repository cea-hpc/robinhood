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
#ifndef _GLB_CFG_H
#define _GLB_CFG_H

#include "rbh_cfg.h"
#include "rbh_const.h"
#include <sys/param.h>  /* for RBH_PATH_MAX */
#include <stdio.h>
#include <stdbool.h>

typedef enum {
    FSKEY_ERROR = 0,
    FSKEY_FSNAME,
    FSKEY_FSID,
    FSKEY_DEVID
} fs_key_t;

/**
 * General Robinhood configuration
 */
typedef struct global_config_t {
    /* filesystem description */
    char    fs_path[RBH_PATH_MAX];
    char    fs_type[FILENAME_MAX];
    /* Array of filesystem mount points.
     * Contains fs_path first, then alternatives sorted in ascending order.
     */
    char  **fs_mount_points;
    int     fs_mpoints_num;

    /* lock file */
    char    lock_file[RBH_PATH_MAX];

    fs_key_t fs_key;

    /* behavior flags */
    bool    stay_in_fs;
    bool    check_mounted;
    bool    last_access_only_atime;
    bool    uid_gid_as_numbers;

#if defined(_LUSTRE) && defined(_MDS_STAT_SUPPORT)
    /** Direct stat to MDS on Lustre filesystems */
    bool    direct_mds_stat;
#endif

} global_config_t;

/** global config structure available to all modules */
extern global_config_t global_config;

/** handlers for global config */
extern mod_cfg_funcs_t global_cfg_hdlr;

#endif
