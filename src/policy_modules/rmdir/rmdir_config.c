/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009 CEA/DAM
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

#include "RobinhoodConfig.h"
#include "rmdir.h"
#include "xplatform_print.h"
#include <errno.h>

#define RMDIR_PARAM_BLOCK   "Rmdir_Parameters"

/* access to module config */
extern rmdir_config_t rmdir_config;

int SetDefault_Rmdir_Config( void *module_config, char *msg_out )
{
    rmdir_config_t *conf = ( rmdir_config_t * ) module_config;
    msg_out[0] = '\0';

    conf->runtime_interval = 6 * 3600;  /* 6h */
    conf->nb_threads_rmdir = 4;
    conf->rmdir_queue_size = 256;

    return 0;
}

int Write_Rmdir_ConfigTemplate( FILE * output )
{
    print_begin_block( output, 0, RMDIR_PARAM_BLOCK, NULL );
    print_line( output, 1, "# Interval for performing empty directory removal" );
    print_line( output, 1, "runtime_interval = 12h ;" );
    fprintf( output, "\n" );
    print_line( output, 1, "# Number of threads for performing rmdir operations" );
    print_line( output, 1, "nb_threads_rmdir = 4 ;" );
    fprintf( output, "\n" );
    print_line( output, 1, "# Queue size (for leveraging rmdir threads load)" );
    print_line( output, 1, "rmdir_queue_size = 256 ;" );
    print_end_block( output, 0 );
    return 0;
}

int Write_Rmdir_ConfigDefault( FILE * output )
{
    print_begin_block( output, 0, RMDIR_PARAM_BLOCK, NULL );
    print_line( output, 1, "runtime_interval      : 6h" );
    print_line( output, 1, "nb_threads_rmdir      : 4" );
    print_line( output, 1, "rmdir_queue_size      : 256" );
    print_end_block( output, 0 );

    return 0;
}

int Read_Rmdir_Config( config_file_t config, void *module_config, char *msg_out, int for_reload )
{
    int            rc;
    int            intval;
    rmdir_config_t *conf = ( rmdir_config_t * ) module_config;

    static const char * rmdir_allowed[] =
    {
        "runtime_interval", "nb_threads_rmdir", "rmdir_queue_size", NULL
    };

    /* get RMDIR_PARAM block */

    config_item_t  param_block = rh_config_FindItemByName( config, RMDIR_PARAM_BLOCK );
    if ( param_block == NULL )
      {
          /* no error, because no parameter is mandatory */
          return 0;
      }

    /* check this is a block... */
    if ( rh_config_ItemType( param_block ) != CONFIG_ITEM_BLOCK )
      {
          strcpy( msg_out, "A block is expected for '" RMDIR_PARAM_BLOCK "' item" );
          return EINVAL;
      }

    /* parse parameters */
    rc = GetDurationParam( param_block, RMDIR_PARAM_BLOCK, "runtime_interval",
                           INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL, &intval, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc != ENOENT )
        conf->runtime_interval = intval;


    rc = GetIntParam( param_block, RMDIR_PARAM_BLOCK, "nb_threads_rmdir",
                      INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL, ( int * ) &conf->nb_threads_rmdir,
                      NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetIntParam( param_block, RMDIR_PARAM_BLOCK, "rmdir_queue_size",
                      INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL, ( int * ) &conf->rmdir_queue_size,
                      NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetBoolParam( param_block, RMDIR_PARAM_BLOCK, "simulation_mode",
                       0, &intval, NULL, NULL, msg_out );
    if ( rc == 0 )
    {
        DisplayLog( LVL_EVENT, "RmdirConfig",
            "WARNING: deprecated parameter 'simulation_mode'. Use '--dry-run' option instead.");
    }

    CheckUnknownParameters( param_block, RMDIR_PARAM_BLOCK, rmdir_allowed );

    return 0;

}

int Reload_Rmdir_Config( void *module_config )
{
    rmdir_config_t *conf = ( rmdir_config_t * ) module_config;

    /* parameters that can't be modified dynamically */

    if (rmdir_config.nb_threads_rmdir != conf->nb_threads_rmdir )
        DisplayLog( LVL_MAJOR, "RmdirConfig",
                    RMDIR_PARAM_BLOCK
                    "::nb_threads_rmdir changed in config file, but cannot be modified dynamically" );

    if (rmdir_config.rmdir_queue_size != conf->rmdir_queue_size )
        DisplayLog( LVL_MAJOR, "RmdirConfig",
                    RMDIR_PARAM_BLOCK
                    "::rmdir_queue_size changed in config file, but cannot be modified dynamically" );


    /* dynamic parameters */

    if (rmdir_config.runtime_interval != conf->runtime_interval )
    {
        DisplayLog( LVL_EVENT, "RmdirConfig", RMDIR_PARAM_BLOCK
                    "::runtime_interval updated: %"PRI_TT"->%"PRI_TT,
                    rmdir_config.runtime_interval, conf->runtime_interval );
        rmdir_config.runtime_interval = conf->runtime_interval;
    }

    return 0;
}
