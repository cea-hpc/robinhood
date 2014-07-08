/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009, 2010 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */


/**
 * \file    chglog_reader_config.c
 * \author  Th. Leibovici
 * \brief   Configuration for Lustre MDT Changelog processing module.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "chglog_reader.h"
#include "RobinhoodMisc.h"
#include "RobinhoodConfig.h"
#include <pthread.h>
#include <errno.h>
#include <unistd.h>

#define CHGLOG_CFG_BLOCK    "ChangeLog"
#define MDT_DEF_BLOCK       "MDT"

chglog_reader_config_t chglog_reader_config;

static mdt_def_t default_mdt_def =
    {
        .mdt_name  = "MDT0000",
        .reader_id = "cl1"
    };


/** Set changelog reader default configuration */
int            ChgLogRdr_SetDefaultConfig( void *module_config, char *msg_out )
{
   chglog_reader_config_t * p_config = (chglog_reader_config_t *) module_config;

   msg_out[0] = '\0';

   p_config->mdt_def = &default_mdt_def;
   p_config->mdt_count = 1;
   /* poll until changelog's follow flag is implemented in llapi */
   p_config->force_polling = TRUE;
   p_config->polling_interval = 1; /* 1s */
   p_config->queue_max_size = 1000;
   p_config->queue_max_age = 5; /* 5s */
   p_config->queue_check_interval = 1; /* every second */
   p_config->mds_has_lu543 = FALSE;
   p_config->mds_has_lu1331 = FALSE;
   p_config->dump_file[0] = '\0'; /* no dump file */

   /* acknowledge 100 records at once */
   p_config->batch_ack_count = 1024;

   return 0;
}

/** Write default parameters for changelog readers */
int            ChgLogRdr_WriteDefaultConfig( FILE * output )
{
    print_begin_block( output, 0, CHGLOG_CFG_BLOCK, NULL );
    print_begin_block( output, 1, MDT_DEF_BLOCK, NULL );
    print_line( output, 2, "mdt_name    :  \"%s\"", default_mdt_def.mdt_name );
    print_line( output, 2, "reader_id   :  \"%s\"", default_mdt_def.reader_id );
    print_end_block( output, 1 );

    print_line( output, 1, "batch_ack_count  : 1024" );
    print_line( output, 1, "force_polling    : TRUE" );
    print_line( output, 1, "polling_interval : 1s" );
    print_line( output, 1, "queue_max_size   : 1000" );
    print_line( output, 1, "queue_max_age    : 5s" );
    print_line( output, 1, "queue_check_interval : 1s" );
    print_line(output, 1, "mds_has_lu543    : FALSE");
    print_line(output, 1, "mds_has_lu1331   : FALSE");
    print_line(output, 1, "dump_file        : (none)");

    print_end_block( output, 0 );

    return 0;
}

/** Write a configuration template for changelog readers */
int            ChgLogRdr_WriteConfigTemplate( FILE * output )
{
    print_line( output, 0, "# Parameters for processing MDT changelogs :");
    print_begin_block( output, 0, CHGLOG_CFG_BLOCK, NULL );

	print_line( output, 1, "# 1 MDT block for each MDT :");
    print_begin_block( output, 1, MDT_DEF_BLOCK, NULL );

	print_line( output, 2, "# name of the first MDT");
    print_line( output, 2, "mdt_name  = \"MDT0000\" ;");
    fprintf( output, "\n" );
    print_line( output, 2, "# id of the persistent changelog reader");
    print_line( output, 2, "# as returned by \"lctl changelog_register\" command");
    print_line( output, 2, "reader_id = \"cl1\" ;" );

    print_end_block( output, 1 );

#ifdef HAVE_DNE
    fprintf( output, "\n" );
    print_line( output, 1, "# another MDT");
    print_begin_block( output, 1, MDT_DEF_BLOCK, NULL );
    print_line( output, 2, "mdt_name  = \"MDT0001\" ;");
    print_line( output, 2, "reader_id = \"cl1\" ;" );
    print_end_block( output, 1 );

    fprintf( output, "\n" );
    print_line( output, 1, "# yet another MDT");
    print_begin_block( output, 1, MDT_DEF_BLOCK, NULL );
    print_line( output, 2, "mdt_name  = \"MDT0002\" ;");
    print_line( output, 2, "reader_id = \"cl1\" ;" );
    print_end_block( output, 1 );
#endif

	print_line( output, 1, "# clear changelog every 1024 records:");
	print_line( output, 1, "batch_ack_count = 1024 ;");
    fprintf( output, "\n" );

    print_line( output, 1, "force_polling    = ON ;" );
    print_line( output, 1, "polling_interval = 1s ;" );
    print_line( output, 1, "queue_max_size   = 1000 ;" );
    print_line( output, 1, "queue_max_age    = 5s ;" );
    print_line( output, 1, "queue_check_interval = 1s ;" );
    fprintf( output, "\n" );

    print_line(output, 1, "# uncomment to dump all changelog records to the file");
    print_line(output, 1, "#dump_file = \"/var/log/robinhood/changelog_dump.log\";");

    print_end_block( output, 0 );

    return 0;
}

#define critical_err_check(_ptr_, _blkname_) do { if (!_ptr_) {\
                                        sprintf( msg_out, "Internal error reading %s block in config file", _blkname_); \
                                        return EFAULT; \
                                    }\
                                } while (0)

static int parse_mdt_block( config_item_t config_blk, const char *block_name,
                             mdt_def_t * p_mdt_def, char *msg_out )
{
    char         * str;

    /* 2 variables expected : 'mdt_name' and 'reader_id' */

    static const char * expected_vars[] = { "mdt_name","reader_id", NULL };

    /* get 'mdt_name' value */

    str = rh_config_GetKeyValueByName( config_blk, "mdt_name" );
    if ( str == NULL )
      {
          DisplayLog(LVL_CRIT, "ChgLog config", "WARNING: no 'mdt_name' provided in %s block: using default value '%s'",
                        block_name, default_mdt_def.mdt_name );
          strcpy( p_mdt_def->mdt_name, default_mdt_def.mdt_name );
      }
    else if ( strlen( str ) >= MDT_NAME_MAX )
      {
          sprintf( msg_out, "MDT name '%s' is too long (max length=%u)", str, MDT_NAME_MAX );
                return ENAMETOOLONG;
      }
    else if ( strncmp( "MDT", str, 3 ) != 0 )
      {
          sprintf( msg_out, "Invalid MDT name '%s'. \"MDT<index>\" expected", str );
      }
    else
      {
          strcpy( p_mdt_def->mdt_name, str );
      }

    /* get 'reader_id' value */

    str = rh_config_GetKeyValueByName( config_blk, "reader_id" );
    if ( str == NULL )
      {
          DisplayLog(LVL_CRIT, "ChgLog config", "WARNING: no 'reader_id' provided in %s block: using default value '%s'",
                        block_name, default_mdt_def.reader_id );
          strcpy( p_mdt_def->reader_id, default_mdt_def.reader_id );
      }
    else if ( strlen( str ) >= MDT_NAME_MAX )
      {
          sprintf( msg_out, "Client id '%s' is too long (max length=%u)", str, READER_ID_MAX );
                return ENAMETOOLONG;
      }
    else
      {
          strcpy( p_mdt_def->reader_id, str );
      }

    /* display warnings for unknown parameters */
    CheckUnknownParameters( config_blk, block_name, expected_vars );

    return 0;
}

/** Read configuration for changelog readers */
int            ChgLogRdr_ReadConfig( config_file_t config, void *module_config,
                                  char *msg_out, int for_reload )
{
    chglog_reader_config_t * p_config = (chglog_reader_config_t *) module_config;
    unsigned int   blc_index;
    int            rc;

    /* get ChangeLog  block */
    config_item_t  chglog_block = rh_config_FindItemByName( config, CHGLOG_CFG_BLOCK );

    /* not mandatory */
    if ( chglog_block == NULL )
        return 0;

    /* get scalar params */
    rc = GetBoolParam( chglog_block, CHGLOG_CFG_BLOCK,
                       "force_polling",0, &p_config->force_polling, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetDurationParam( chglog_block, CHGLOG_CFG_BLOCK,
                      "polling_interval", INT_PARAM_NOT_NULL|INT_PARAM_POSITIVE,
                        &p_config->polling_interval, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetIntParam( chglog_block, CHGLOG_CFG_BLOCK,
                      "batch_ack_count", INT_PARAM_NOT_NULL|INT_PARAM_POSITIVE,
                       &p_config->batch_ack_count, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetIntParam( chglog_block, CHGLOG_CFG_BLOCK,
                      "queue_max_size", INT_PARAM_NOT_NULL | INT_PARAM_POSITIVE,
                       &p_config->queue_max_size, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetDurationParam( chglog_block, CHGLOG_CFG_BLOCK,
                      "queue_max_age", INT_PARAM_NOT_NULL|INT_PARAM_POSITIVE,
                        &p_config->queue_max_age, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetDurationParam( chglog_block, CHGLOG_CFG_BLOCK,
                      "queue_check_interval", INT_PARAM_NOT_NULL|INT_PARAM_POSITIVE,
                        &p_config->queue_check_interval, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetBoolParam( chglog_block, CHGLOG_CFG_BLOCK,
                       "mds_has_lu543",0, &p_config->mds_has_lu543, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetBoolParam( chglog_block, CHGLOG_CFG_BLOCK,
                       "mds_has_lu1331",0, &p_config->mds_has_lu1331, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetStringParam(chglog_block, CHGLOG_CFG_BLOCK, "dump_file",
                        STR_PARAM_ABSOLUTE_PATH | STR_PARAM_NO_WILDCARDS,
                       p_config->dump_file, RBH_PATH_MAX, NULL, NULL, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    else if (rc == ENOENT)
        p_config->dump_file[0] = '\0';

    /* browse  the list of MDT blocks */

    for ( blc_index = 0; blc_index < rh_config_GetNbItems( chglog_block ); blc_index++ )
      {
          char          *block_name;
          config_item_t  curr_item = rh_config_GetItemByIndex( chglog_block, blc_index );
          critical_err_check( curr_item, CHGLOG_CFG_BLOCK );

          if ( rh_config_ItemType( curr_item ) != CONFIG_ITEM_BLOCK )
              continue;

          block_name = rh_config_GetBlockName( curr_item );
          critical_err_check( curr_item, CHGLOG_CFG_BLOCK );

          if ( !strcasecmp( block_name, MDT_DEF_BLOCK ) )
          {
                /* allocate a new mdt_definition  */

                if ( (p_config->mdt_def==NULL) || (p_config->mdt_def == &default_mdt_def) )
                {
                    p_config->mdt_count = 1;

                    /* no MDT definition, or MDT definition was the default */
                    p_config->mdt_def = (mdt_def_t*)malloc( sizeof( mdt_def_t ) );
                    if ( !p_config->mdt_def ) return ENOMEM;
                }
                else
                {
                   p_config->mdt_count ++;

                   p_config->mdt_def = (mdt_def_t*)realloc( p_config->mdt_def,
                                         p_config->mdt_count * sizeof( mdt_def_t ) );
                   if ( !p_config->mdt_def ) return ENOMEM;
                }

                /* fill the structure */
                rc = parse_mdt_block( curr_item, MDT_DEF_BLOCK, &p_config->mdt_def[p_config->mdt_count-1], msg_out );
                if ( rc ) return rc;
          }
          else
          {
                sprintf( msg_out, "Unknown sub-block '%s' in " CHGLOG_CFG_BLOCK " block, line %d",
                         block_name, rh_config_GetItemLine( curr_item ) );
                return EINVAL;
          }
    }

#ifdef _DEBUG_CHGLOG
    printf( "%u MDT definitions parsed successfully, ptr = %p\n", p_config->mdt_count,  p_config->mdt_def );
#endif

    return 0;

}

