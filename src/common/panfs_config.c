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

#include "panfs_config.h"
#include "rbh_cfg_helpers.h"
#include "rbh_misc.h"
#include "rbh_logs.h"
#include <errno.h>

#define PANFS_CONFIG_BLOCK "Panasas"

/* exported variable available to all modules */
panfs_config_t panfs_config;

static void panfs_cfg_set_default(void *module_config)
{
    panfs_config_t *conf = (panfs_config_t *)module_config;
    rh_strncpy(conf->snap_delta_path, "", RBH_PATH_MAX);
    rh_strncpy(conf->snap_delta_results_path, "", RBH_PATH_MAX);
    rh_strncpy(conf->volume, "", FILENAME_MAX);
    conf->update_interval = 600;//10 min
}

static void panfs_cfg_write_default(FILE *output)
{
    print_begin_block(output, 0, PANFS_CONFIG_BLOCK, NULL);
    print_line(output, 1, "snap_delta_path       :  PATH");
    print_line(output, 1, "snap_delta_results_path       :  PATH");
    print_line(output, 1, "volume        :  volumeName");
    print_line(output, 1, "# Interval for updating database");
    print_line(output, 1, "update_interval = 10min ;");
    fprintf(output, "\n");
    print_end_block(output, 0);
}

static int panfs_cfg_read(config_file_t config, void *module_config,
                           char *msg_out)
{
    panfs_config_t *conf = (panfs_config_t *)module_config;
    config_item_t    panfs_block;
    int              rc;

    static const char * const allowed_params[] = {
        "snap_delta_path", "snap_delta_results_path", "volume", "update_interval", NULL
    };
    const cfg_param_t cfg_params[] = {
        {"snap_delta_path", PT_STRING, PFLG_MANDATORY  |
         PFLG_REMOVE_FINAL_SLASH | PFLG_NO_WILDCARDS, conf->snap_delta_path,
         sizeof(conf->snap_delta_path)}
        ,
        {"snap_delta_results_path", PT_STRING, PFLG_MANDATORY  |
         PFLG_REMOVE_FINAL_SLASH | PFLG_NO_WILDCARDS, conf->snap_delta_results_path,
         sizeof(conf->snap_delta_results_path)}
        ,
        {"volume", PT_STRING,
         PFLG_MANDATORY |
         PFLG_NO_WILDCARDS, conf->volume, sizeof(conf->volume)}
        ,
        {"update_interval", PT_DURATION, PFLG_POSITIVE | PFLG_NOT_NULL,
         &conf->update_interval, 0}
        ,

        END_OF_PARAMS
    };

    /* get PANASAS block */
    rc = get_cfg_block(config, PANFS_CONFIG_BLOCK, &panfs_block, msg_out);
    if (rc)
        return rc;

    /* retrieve std parameters */
    rc = read_scalar_params(panfs_block, PANFS_CONFIG_BLOCK, cfg_params,
                            msg_out);
    if (rc)
        return rc;

    /* check unknown parameters */
    CheckUnknownParameters(panfs_block, PANFS_CONFIG_BLOCK, allowed_params);

    return 0;
}

static int panfs_cfg_set(void *module_config, bool reload)
{
    panfs_config_t *conf = (panfs_config_t *) module_config;

    if (!reload) {
        /* copy the whole structure content */
        panfs_config = *conf;
        return 0;
    }

    if (strcmp(conf->snap_delta_path, panfs_config.snap_delta_path)){
        DisplayLog(LVL_MAJOR, "PanasasConfig",
                   PANFS_CONFIG_BLOCK
                   "::Path to pan_snap_delta updated");
	strcpy(panfs_config.snap_delta_path,conf->snap_delta_path);
	
    }
    if (strcmp(conf->snap_delta_results_path, panfs_config.snap_delta_results_path)){
        DisplayLog(LVL_MAJOR, "PanasasConfig",
                   PANFS_CONFIG_BLOCK
                   "::Path to pan_snap_delta results updated");
	strcpy(panfs_config.snap_delta_results_path,conf->snap_delta_results_path);
	
    }
    if (strcmp(conf->volume, panfs_config.volume)){
        DisplayLog(LVL_MAJOR, "PanasasConfig",
                   PANFS_CONFIG_BLOCK
                   "::Volume's name changed in config file, but cannot be modified dynamically. If you need to change it, drop the database and run it one more time.");
        exit(1);
    }
    if (panfs_config.update_interval != conf->update_interval) {
        DisplayLog(LVL_MAJOR, "PanasasConfig",
                   PANFS_CONFIG_BLOCK "::update_interval modified: "
                   "'%" PRI_TT "'->'%" PRI_TT "'",
                   panfs_config.update_interval, conf->update_interval);
        panfs_config.update_interval = conf->update_interval;
    } 
    return 0;
}

static void panfs_cfg_write_template(FILE *output)
{
    print_begin_block(output, 0, PANFS_CONFIG_BLOCK, NULL);
    print_line(output, 1,
               "# Path to pan_snap_delta binary");
    print_line(output, 1, "snap_delta_path =  \"/panfs/\" ;");
    fprintf(output, "\n");
    print_line(output, 1,
               "# Path to pan_snap_delta results");
    print_line(output, 1, "snap_delta_results_path =  \"/tmp/\" ;");
    fprintf(output, "\n");
    print_line(output, 1, "# Name of the volume");
    print_line(output, 1, "volume = home;");
    fprintf(output, "\n");
    print_line(output, 1,
               "# Time between checks to see if there are newer snapshots to process");
   
    print_line(output, 1, " update_interval=10m;");
    print_end_block(output, 0);
}

static void *panfs_cfg_new(void)
{
    return calloc(1, sizeof(panfs_config_t));
}

static void panfs_cfg_free(void *cfg)
{
    if (cfg != NULL) 
        free(cfg);
}

/** structure with config handling functions */
mod_cfg_funcs_t panfs_cfg_hdlr = {
    .module_name = "panasas",
    .new = panfs_cfg_new,
    .free = panfs_cfg_free,
    .set_default = panfs_cfg_set_default,
    .read = panfs_cfg_read,
    .set_config = panfs_cfg_set,
    .write_default = panfs_cfg_write_default,
    .write_template = panfs_cfg_write_template
};
