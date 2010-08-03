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
#include "hsm_rm.h"
#include <errno.h>

#define HSMRM_PARAM_BLOCK   "hsm_remove_parameters"

/* access to module config */
extern hsm_rm_config_t hsm_rm_config;

int SetDefault_HSMRm_Config( void *module_config, char *msg_out )
{
    hsm_rm_config_t *conf = ( hsm_rm_config_t * ) module_config;
    msg_out[0] = '\0';

    conf->runtime_interval = 600;  /* 10 min */
    conf->nb_threads_rm = 4;
    conf->rm_queue_size = 256;
    conf->max_rm  = 0;

    return 0;
}

int Write_HSMRm_ConfigTemplate( FILE * output )
{
    print_begin_block( output, 0, HSMRM_PARAM_BLOCK, NULL );
    print_line( output, 1, "# Interval for checking files to be removed in HSM" );
    print_line( output, 1, "runtime_interval = 10min ;" );
    fprintf( output, "\n" );
    print_line( output, 1, "# Number of threads to perform hsm rm operations" );
    print_line( output, 1, "nb_threads_rm = 4 ;" );
    fprintf( output, "\n" );
    print_line( output, 1, "# Queue size (for leveraging rm threads load)" );
    print_line( output, 1, "rm_queue_size = 256 ;" );
    fprintf( output, "\n" );
    print_line( output, 1, "# maximum number of remove requests per pass" );
    print_line( output, 1, "# (0: unlimited)" );
    print_line( output, 1, "max_rm_count   = 0 ;" );
    print_end_block( output, 0 );
    return 0;
}

int Write_HSMRm_ConfigDefault( FILE * output )
{
    print_begin_block( output, 0, HSMRM_PARAM_BLOCK, NULL );
    print_line( output, 1, "runtime_interval      : 10min" );
    print_line( output, 1, "nb_threads_rm         : 4" );
    print_line( output, 1, "rm_queue_size         : 256" );
    print_line( output, 1, "max_rm_count          : 0 (unlimited)" );
    print_end_block( output, 0 );

    return 0;
}

int Read_HSMRm_Config( config_file_t config, void *module_config, char *msg_out, int for_reload )
{
    int            rc;
    int            intval;
    hsm_rm_config_t *conf = ( hsm_rm_config_t * ) module_config;

    static const char * hsmrm_allowed[] =
    {
        "runtime_interval", "nb_threads_rm", "rm_queue_size", "max_rm_count", NULL
    };

    /* get HSMRM_PARAM block */

    config_item_t  param_block = rh_config_FindItemByName( config, HSMRM_PARAM_BLOCK );
    if ( param_block == NULL )
      {
          /* no error, because no parameter is mandatory */
          return 0;
      }

    /* check this is a block... */
    if ( rh_config_ItemType( param_block ) != CONFIG_ITEM_BLOCK )
      {
          strcpy( msg_out, "A block is expected for '" HSMRM_PARAM_BLOCK "' item" );
          return EINVAL;
      }

    /* parse parameters */
    rc = GetDurationParam( param_block, HSMRM_PARAM_BLOCK, "runtime_interval",
                           INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL, &intval, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc != ENOENT )
        conf->runtime_interval = intval;


    rc = GetIntParam( param_block, HSMRM_PARAM_BLOCK, "nb_threads_rm",
                      INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL, ( int * ) &conf->nb_threads_rm,
                      NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetIntParam( param_block, HSMRM_PARAM_BLOCK, "rm_queue_size",
                      INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL, ( int * ) &conf->rm_queue_size,
                      NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetIntParam( param_block, HSMRM_PARAM_BLOCK, "max_rm_count",
                      INT_PARAM_POSITIVE, ( int * ) &conf->max_rm,
                      NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetBoolParam( param_block, HSMRM_PARAM_BLOCK, "simulation_mode",
                       0, &intval, NULL, NULL, msg_out );
    if ( rc == 0 )
    {
        DisplayLog( LVL_EVENT, "RmdirConfig",
            "WARNING: deprecated parameter 'simulation_mode'. Use '--dry-run' option instead.");
    }

    CheckUnknownParameters( param_block, HSMRM_PARAM_BLOCK, hsmrm_allowed );

    return 0;

}

int Reload_HSMRm_Config( void *module_config )
{
    hsm_rm_config_t *conf = ( hsm_rm_config_t * ) module_config;

    /* parameters that can't be modified dynamically */

    if (hsm_rm_config.nb_threads_rm != conf->nb_threads_rm )
        DisplayLog( LVL_MAJOR, "HSMRmConfig",
                    HSMRM_PARAM_BLOCK
                    "::nb_threads_rm changed in config file, but cannot be modified dynamically" );

    if (hsm_rm_config.rm_queue_size != conf->rm_queue_size )
        DisplayLog( LVL_MAJOR, "HSMRmConfig",
                    HSMRM_PARAM_BLOCK
                    "::rm_queue_size changed in config file, but cannot be modified dynamically" );

    /* dynamic parameters */

    if (hsm_rm_config.runtime_interval != conf->runtime_interval )
    {
        DisplayLog( LVL_EVENT, "HSMRmConfig", HSMRM_PARAM_BLOCK
                    "::runtime_interval updated: %u->%u",
                    (unsigned int)hsm_rm_config.runtime_interval,
                    (unsigned int)conf->runtime_interval );
        hsm_rm_config.runtime_interval = conf->runtime_interval;
    }

    if (hsm_rm_config.max_rm != conf->max_rm )
    {
        DisplayLog( LVL_EVENT, "HSMRmConfig", HSMRM_PARAM_BLOCK
                    "::max_rm_count updated: %u->%u",
                    hsm_rm_config.max_rm, conf->max_rm );
        hsm_rm_config.max_rm = conf->max_rm;
    }

    return 0;
}
