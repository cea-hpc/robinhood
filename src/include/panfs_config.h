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
 * \file  panfs_config.h
 * \brief Panasas configuration parameters
 */
#ifndef _PANFS_CFG_H
#define _PANFS_CFG_H

#include "config_parsing.h"
#include "rbh_cfg.h"
#include "rbh_const.h"
#include <sys/param.h>  /* for RBH_PATH_MAX */
#include <stdio.h>
#include <stdbool.h>

/**
 * General Panasas configuration
 */
typedef struct panfs_config_t {
    /* filesystem description */
    char    snap_delta_path[RBH_PATH_MAX];
    char    snap_delta_results_path[RBH_PATH_MAX];
    char    volume[FILENAME_MAX];
    time_t     update_interval;
   
} panfs_config_t;

/** panasas config structure available to all modules */
extern panfs_config_t panfs_config;

/** handlers for panasas config */
extern mod_cfg_funcs_t panfs_cfg_hdlr;

#endif
