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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "RobinhoodConfig.h"
#include "resource_monitor.h"
#include "resmon_purge.h"
#include "RobinhoodMisc.h"
#include <errno.h>

#define PURGE_PARAM_BLOCK   "Purge_Parameters"
#define TRIGGER_BLOCK       "Purge_Trigger"

#define RESMONCFG_TAG       "ResMonConfig"

int SetDefault_ResourceMon_Config( void *module_config, char *msg_out )
{
    resource_monitor_config_t *conf = ( resource_monitor_config_t * ) module_config;
    msg_out[0] = '\0';

    conf->nb_threads_purge = 4;
    conf->post_purge_df_latency = 60;   /*1 min */
    conf->purge_queue_size = 4096;
    conf->db_request_limit = 100000;
    conf->sort = true;
#ifdef ATTR_INDEX_status
    conf->check_purge_status_on_startup = TRUE;
#endif
    conf->recheck_ignored_classes = FALSE;

    conf->trigger_list = NULL;
    conf->trigger_count = 0;

#ifdef _TMP_FS_MGR
    conf->purge_command[0] = '\0';
#endif

    return 0;
}

int Write_ResourceMon_ConfigDefault(FILE * output)
{
    print_begin_block(output, 0, PURGE_PARAM_BLOCK, NULL);
    print_line(output, 1, "nb_threads_purge      : 4");
    print_line(output, 1, "post_purge_df_latency : 1min");
    print_line(output, 1, "purge_queue_size      : 4096");
    print_line(output, 1, "db_result_size_max    : 100000");
    print_line(output, 1, "sort                  : TRUE");
#ifdef ATTR_INDEX_status
    print_line(output, 1, "check_purge_status_on_startup: TRUE");
#endif
    print_line(output, 1, "recheck_ignored_classes: FALSE");
#ifdef _TMP_FS_MGR
    print_line(output, 1, "purge_command          : <built-in: unlink>");
#endif
    print_end_block(output, 0);

    fprintf(output, "\n");

    return 0;
}

int Write_ResourceMon_ConfigTemplate( FILE * output )
{
    print_begin_block( output, 0, PURGE_PARAM_BLOCK, NULL );
    print_line( output, 1, "# nbr of thread for performing purge operations" );
    print_line( output, 1, "nb_threads_purge      = 4 ;" );
    fprintf( output, "\n" );
    print_line( output, 1,
                "# Immediately after purging data, 'df'/'ost df' may not return an exact value," );
    print_line( output, 1, "# especially if freeing disk space is asynchronous." );
    print_line( output, 1, "# So, it is necessary to wait for a while after a purge before" );
    print_line( output, 1, "# issuing a new 'df'/'ost df' command." );
    print_line( output, 1, "post_purge_df_latency = 1min ;" );
    fprintf( output, "\n" );
    print_line( output, 1, "# queue size (for leveraging purge threads load)" );
    print_line( output, 1, "purge_queue_size      = 4096 ;" );

    fprintf(output, "\n");
    print_line(output, 1, "# Limit the size of database result sets (save memory)");
    print_line(output, 1, "db_result_size_max    = 100000 ;");

    fprintf(output, "\n");
    print_line(output, 1, "# Enable/disable sorting by access time");
    print_line(output, 1, "#sort = TRUE ;");

#ifdef ATTR_INDEX_status
    fprintf( output, "\n" );
    print_line( output, 1, "# check status of previous purge operations on startup" );
    print_line( output, 1, "check_purge_status_on_startup = TRUE ;" );
#endif
    print_line( output, 1, "# When applying purge policies, recheck entries");
    print_line( output, 1, "# that previously matched ignored classes.");
    print_line( output, 1, "# Enable it after changing fileclass definitions");
    print_line( output, 1, "# or if entries move from one class to another.");
    print_line( output, 1, "# This can significantly slow down policy application.");
    print_line( output, 1, "recheck_ignored_classes = no;" );
    fprintf( output, "\n" );

#ifdef _TMP_FS_MGR
    print_line(output, 1, "# By default, purge action is removing the entry");
    print_line(output, 1, "# from the filesystem. You can define an alternative");
    print_line(output, 1, "# action by specifying a script command.");
    print_line(output, 1, "# The following parameters can be specified:");
    print_line(output, 1, "#    {path}: posix path to the entry");
#ifdef _LUSTRE
#   ifdef _HAVE_FID
    print_line(output, 1, "#    {fid}: fid of the entry");
#   endif
    print_line(output, 1, "#    {fsname}: Lustre fsname");
#endif
    print_line(output, 1, "#purge_command = =\"/usr/bin/move_to_trash.sh {path}\";");
#endif
    fprintf( output, "\n" );

    print_end_block( output, 0 );

    fprintf( output, "\n" );
#ifdef _LUSTRE
    print_line( output, 0, "# Trigger purge on individual OST usage" );
    print_begin_block( output, 0, TRIGGER_BLOCK, NULL );
    print_line( output, 1, "trigger_on         = OST_usage ;" );
    print_line( output, 1, "high_threshold_pct = 85%% ;" );
    print_line( output, 1, "low_threshold_pct  = 80%% ;" );
    print_line( output, 1, "check_interval     = 5min ;" );
    print_end_block( output, 0 );
    fprintf( output, "\n" );
#endif

    print_line( output, 0, "# Trigger purge on filesystem usage" );
    print_begin_block( output, 0, TRIGGER_BLOCK, NULL );
    print_line( output, 1, "trigger_on         = global_usage ;" );
    print_line( output, 1, "high_threshold_pct = 90%% ;" );
    print_line( output, 1, "low_threshold_pct  = 85%% ;" );
    print_line( output, 1, "check_interval     = 5min ;" );
    print_line( output, 1, "# raise an alert when the high threshold is reached" );
    print_line( output, 1, "alert_high         = TRUE ;" );
    print_line( output, 1, "# raise an alert if not enough data can be purged");
    print_line( output, 1, "# to reach the low threshold");
    print_line( output, 1, "alert_low          = TRUE ;" );
    print_end_block( output, 0 );

    fprintf( output, "\n" );

/** @TODO not implemented yet */

#if 0
#ifdef _LUSTRE
    print_line( output, 1, "# Trigger purge on pool usage" );
    print_begin_block( output, 1, TRIGGER_BLOCK );
    print_line( output, 2, "trigger_on         = pool_usage(pool1,pool2) ;" );
    print_line( output, 2, "high_threshold_pct = 85%% ;" );
    print_line( output, 2, "low_threshold_pct  = 80%% ;" );
    print_line( output, 2, "check_interval     = 5min ;" );
    print_end_block( output, 1 );
    fprintf( output, "\n" );
#endif

#endif

    print_line( output, 0, "# Trigger purge of charlie's or foo's files" );
    print_line( output, 0, "# if they use more than a TB (check twice a day)" );
    print_begin_block( output, 0, TRIGGER_BLOCK, NULL );
    print_line( output, 1, "trigger_on         = user_usage(charlie,foo) ;" );
    print_line( output, 1, "high_threshold_vol = 1TB ;" );
    print_line( output, 1, "low_threshold_vol  = 950GB ;" );
    print_line( output, 1, "check_interval     = 12h ;" );
    print_line( output, 1, "# send an alert when the quota is reached" );
    print_line( output, 1, "alert_high         = TRUE ;" );
    print_end_block( output, 0 );

    fprintf( output, "\n" );

    return 0;
}

static int parse_trigger_block( config_item_t config_blk, const char *block_name,
                                trigger_item_t * p_trigger_item, char *msg_out )
{
    int            rc;
    int            rc_hp, rc_lp, rc_hv, rc_lv, rc_hc, rc_lc;
    unsigned int high_count = 0;
    unsigned int low_count = 0;
    double         h_pct, l_pct;
    unsigned long long h_vol, l_vol;
    uint64_t       h_cnt, l_cnt;
    int            tmpval;
    int            i;
    char           tmpstr[1024];
    char         **arg_tab;
    unsigned int   arg_count;

    static const char *trigger_expect[] =
    {
        "trigger_on", "check_interval",
        "high_threshold_pct", "low_threshold_pct",
        "high_threshold_vol", "low_threshold_vol",
        "high_threshold_cnt", "low_threshold_cnt",
        "alert_high", "alert_low",
        /* for backward compatibility */
        "high_watermark_pct", "low_watermark_pct",
        "high_watermark_vol", "low_watermark_vol",
        "high_watermark_cnt", "low_watermark_cnt",
        "notify", "notify_hw", "alert_lw",
        NULL
    };


    /* retrieve parameters */

    rc = GetStringParam( config_blk, block_name, "trigger_on",
                         PARAM_MANDATORY | STR_PARAM_NO_WILDCARDS, tmpstr, 1024, &arg_tab,
                         &arg_count, msg_out );

    if ( rc )                   /* even ENOENT retruns an error because trigger_on is mandatory */
        return rc;

    /* initialize list of optional args */
    p_trigger_item->list = NULL;
    p_trigger_item->list_size = 0;

    /* analyze trigger_on parameter */
    if ( !strcasecmp( tmpstr, "periodic" ) )
    {
        p_trigger_item->type = TRIGGER_ALWAYS;

        /* default: alert enabled if LW cannot be reached */
        p_trigger_item->alert_lw = FALSE;

        /* no arg expected */
        if ( arg_count > 0 )
        {
            sprintf( msg_out,
                     "No extra argument expected for trigger type '%s': %u argument(s) found.",
                     tmpstr, arg_count );
            return EINVAL;
        }
    }
    else if ( !strcasecmp( tmpstr, "global_usage" ) )
    {
        p_trigger_item->type = TRIGGER_GLOBAL_USAGE;

        /* default: alert enabled if LW cannot be reached */
        p_trigger_item->alert_lw = TRUE;

        /* no arg expected */
        if ( arg_count > 0 )
        {
            sprintf( msg_out,
                     "No extra argument expected for trigger type '%s': %u argument(s) found.",
                     tmpstr, arg_count );
            return EINVAL;
        }
    }
    else if ( !strcasecmp( tmpstr, "OST_usage" ) )
    {
        p_trigger_item->type = TRIGGER_OST_USAGE;

        /* default: alert enabled if LW cannot be reached */
        p_trigger_item->alert_lw = TRUE;

        /* no arg expected */
        if ( arg_count > 0 )
        {
            sprintf( msg_out,
                     "No extra argument expected for trigger type '%s': %u argument(s) found.",
                     tmpstr, arg_count );
            return EINVAL;
        }
    }
    else if ( !strcasecmp( tmpstr, "user_usage" ) )
    {
        p_trigger_item->type = TRIGGER_USER_USAGE;

        /* default: alert enabled if LW cannot be reached */
        p_trigger_item->alert_lw = TRUE;

        /* optional arguments: user list */
        if ( arg_count > 0 )
        {
            p_trigger_item->list = ( char ** ) calloc( arg_count, sizeof( char * ) );
            p_trigger_item->list_size = arg_count;
            for ( i = 0; i < arg_count; i++ )
            {
                p_trigger_item->list[i] = ( char * ) malloc( strlen( arg_tab[i] ) + 1 );
                strcpy( p_trigger_item->list[i], arg_tab[i] );
            }
        }
    }
    else if ( !strcasecmp( tmpstr, "group_usage" ) )
    {
        p_trigger_item->type = TRIGGER_GROUP_USAGE;

        /* default: alert enabled if LW cannot be reached */
        p_trigger_item->alert_lw = TRUE;

        /* optional argument: group list */
        if ( arg_count > 0 )
        {
            p_trigger_item->list = ( char ** ) calloc( arg_count, sizeof( char * ) );
            p_trigger_item->list_size = arg_count;
            for ( i = 0; i < arg_count; i++ )
            {
                p_trigger_item->list[i] = ( char * ) malloc( strlen( arg_tab[i] ) + 1 );
                strcpy( p_trigger_item->list[i], arg_tab[i] );
            }
        }
    }
    else if ( !strcasecmp( tmpstr, "pool_usage" ) )
    {
        p_trigger_item->type = TRIGGER_POOL_USAGE;

        /* default: alert enabled if LW cannot be reached */
        p_trigger_item->alert_lw = TRUE;

        /* optional arguments: user list */
        if ( arg_count > 0 )
        {
            p_trigger_item->list = ( char ** ) calloc( arg_count, sizeof( char * ) );
            p_trigger_item->list_size = arg_count;
            for ( i = 0; i < arg_count; i++ )
            {
                p_trigger_item->list[i] = ( char * ) malloc( strlen( arg_tab[i] ) + 1 );
                strcpy( p_trigger_item->list[i], arg_tab[i] );
            }
        }
    }
    else if ( !strcasecmp( tmpstr, "external_command" ) )
    {
        p_trigger_item->type = TRIGGER_CUSTOM_CMD;

        /* default: alert enabled if LW cannot be reached */
        p_trigger_item->alert_lw = TRUE;

        /* single mandatory argument: command */
        if ( arg_count != 1 )
        {
            sprintf( msg_out,
                     "A single mandatory argument is expected for trigger type '%s': %u argument(s) found.",
                     tmpstr, arg_count );
            return EINVAL;
        }

        p_trigger_item->list = ( char ** ) malloc( sizeof( char * ) );
        p_trigger_item->list[0] = ( char * ) malloc( strlen( arg_tab[0] ) + 1 );
        strcpy( p_trigger_item->list[0], arg_tab[0] );
        p_trigger_item->list_size = 1;
    }
    else
    {
        sprintf( msg_out, "Unexpected value for 'trigger_on' parameter: %s.", tmpstr );
        return EINVAL;
    }


    /* retrieve all threshold params and check their compatibility */
    high_count = low_count = 0;

    rc_hp = GetFloatParam( config_blk, block_name, "high_threshold_pct",
                         FLOAT_PARAM_POSITIVE | ALLOW_PCT_SIGN, &h_pct,
                         NULL, NULL, msg_out );
    /* for backward compatibility */
    if ( rc_hp == ENOENT )
        rc_hp = GetFloatParam( config_blk, block_name, "high_watermark_pct",
                             FLOAT_PARAM_POSITIVE | ALLOW_PCT_SIGN, &h_pct,
                             NULL, NULL, msg_out );

    if ( ( rc_hp != 0 ) && ( rc_hp != ENOENT ) )
        return rc_hp;
    else if ( rc_hp != ENOENT )
        high_count++;

    rc_hv = GetSizeParam( config_blk, block_name, "high_threshold_vol",
                        INT_PARAM_POSITIVE, &h_vol, NULL, NULL, msg_out );
    /* for backward compatibility */
    if ( rc_hv == ENOENT )
        rc_hv = GetSizeParam( config_blk, block_name, "high_watermark_vol",
                              INT_PARAM_POSITIVE, &h_vol, NULL, NULL, msg_out );

    if ( ( rc_hv != 0 ) && ( rc_hv != ENOENT ) )
        return rc_hv;
    else if ( rc_hv != ENOENT )
        high_count++;

    rc_hc = GetInt64Param( config_blk, block_name, "high_threshold_cnt",
                           INT_PARAM_POSITIVE, &h_cnt, NULL, NULL, msg_out );
    /* for backward compatibility */
    if ( rc_hc == ENOENT )
            rc_hc = GetInt64Param( config_blk, block_name, "high_watermark_cnt",
                                   INT_PARAM_POSITIVE, &h_cnt, NULL, NULL, msg_out );
    if ( ( rc_hc != 0 ) && ( rc_hc != ENOENT ) )
        return rc_hc;
    else if ( rc_hc != ENOENT )
        high_count++;

    rc_lp = GetFloatParam( config_blk, block_name, "low_threshold_pct",
                         FLOAT_PARAM_POSITIVE | ALLOW_PCT_SIGN, &l_pct,
                         NULL, NULL, msg_out );
    /* for backward compatibility */
    if ( rc_lp == ENOENT )
        rc_lp = GetFloatParam( config_blk, block_name, "low_watermark_pct",
                             FLOAT_PARAM_POSITIVE | ALLOW_PCT_SIGN, &l_pct,
                             NULL, NULL, msg_out );

    if ( ( rc_lp != 0 ) && ( rc_lp != ENOENT ) )
        return rc_lp;
    else if ( rc_lp != ENOENT )
        low_count++;

    rc_lv = GetSizeParam( config_blk, block_name, "low_threshold_vol",
                        INT_PARAM_POSITIVE, &l_vol, NULL, NULL, msg_out );
    /* for backward compatibility */
    if ( rc_lv == ENOENT )
        rc_lv = GetSizeParam( config_blk, block_name, "low_watermark_vol",
                            INT_PARAM_POSITIVE, &l_vol, NULL, NULL, msg_out );
    if ( ( rc_lv != 0 ) && ( rc_lv != ENOENT ) )
        return rc_lv;
    else if ( rc_lv != ENOENT )
        low_count++;

    rc_lc = GetInt64Param( config_blk, block_name, "low_threshold_cnt",
                           INT_PARAM_POSITIVE, &l_cnt, NULL, NULL, msg_out );
    /* for backward compatibility */
    if ( rc_lc == ENOENT )
        rc_lc = GetInt64Param( config_blk, block_name, "low_watermark_cnt",
                               INT_PARAM_POSITIVE, &l_cnt, NULL, NULL, msg_out );
    if ( ( rc_lc != 0 ) && ( rc_lc != ENOENT ) )
        return rc_lc;
    else if ( rc_lc != ENOENT )
        low_count++;

    if ( p_trigger_item->type == TRIGGER_ALWAYS )
    {
        /* in case of 'periodic' trigger, no thresholds are expected */
        if ( (high_count > 0) || (low_count > 0) )
        {
            strcpy( msg_out,
                    "No high/low threshold expected for trigger type 'periodic'" );
            return EINVAL;
        }
    }
    else if ( p_trigger_item->type == TRIGGER_CUSTOM_CMD )
    {
        /* in case of an external command, no thresholds are expected */
        if ( (high_count > 0) || (low_count > 0) )
        {
            strcpy( msg_out,
                    "No high/low thresholds expected for trigger type 'external_command'" );
            return EINVAL;
        }
    }
    else if ( high_count > 1 )
    {
        strcpy( msg_out, "Multiple purge start conditions in trigger." );
        return EINVAL;
    }
    else if ( low_count > 1 )
    {
        strcpy( msg_out, "Multiple purge stop conditions in trigger." );
        return EINVAL;
    }
    else if ( high_count == 0 )
    {
        strcpy( msg_out, "No purge start condition found in trigger "
                         "(mandatory). 'high_threshold_pct', 'high_threshold_vol'"
                         "or 'high_threshold_cnt' expected" );
        return ENOENT;
    }
    else if ( low_count == 0 )
    {
        strcpy( msg_out, "No purge stop condition found in trigger "
                         "(mandatory). 'low_threshold_pct', 'low_threshold_vol'"
                         "or 'low_threshold_cnt' expected" );
        return ENOENT;
    }
    else if ( rc_hc != rc_lc ) /* both 0 or both ENOENT */
    {
        strcpy( msg_out, "Incompatible threshold types: 'high_threshold_cnt' "
                         "must be used with 'low_threshold_cnt'" );
        return ENOENT;
    }

    /* NOTE: count threshold for HSM systems only match online files (not released)*/

    /* count threshold is only on global usage */
    if ( (p_trigger_item->type != TRIGGER_GLOBAL_USAGE)
         && (p_trigger_item->type != TRIGGER_ALWAYS)
         && (p_trigger_item->type != TRIGGER_USER_USAGE)
         && (p_trigger_item->type != TRIGGER_GROUP_USAGE)
         && ( (rc_hc == 0) || (rc_lc == 0) ) )
    {
        strcpy( msg_out, "Threshold on entry count is only supported "
                         "for 'global_usage', 'user_usage', 'group_usage' and 'periodic' triggers" );
        return EINVAL;
    }

    if ( rc_hp == 0 )
    {
        p_trigger_item->hw_type = PCT_THRESHOLD;
        p_trigger_item->hw_percent = h_pct;
    }
    else if ( rc_hv == 0 )
    {
        p_trigger_item->hw_type = VOL_THRESHOLD;
        p_trigger_item->hw_volume = h_vol;
    }
    else if ( rc_hc == 0 )
    {
        p_trigger_item->hw_type = COUNT_THRESHOLD;
        p_trigger_item->hw_count = h_cnt;
    }

    if ( rc_lp == 0 )
    {
        p_trigger_item->lw_type = PCT_THRESHOLD;
        p_trigger_item->lw_percent = l_pct;
    }
    else if ( rc_lv == 0 )
    {
        p_trigger_item->lw_type = VOL_THRESHOLD;
        p_trigger_item->lw_volume = l_vol;
    }
    else if ( rc_lc == 0 )
    {
        p_trigger_item->lw_type = COUNT_THRESHOLD;
        p_trigger_item->lw_count = l_cnt;
    }

    /* retrieve check interval parameter */

    rc = GetDurationParam( config_blk, block_name, "check_interval",
                           INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL
                           | PARAM_MANDATORY, &tmpval, NULL,
                           NULL, msg_out );
    if ( rc )
        return rc;
    p_trigger_item->check_interval = tmpval;

    rc = GetBoolParam( config_blk, block_name, "alert_high", 0,
                       &tmpval, NULL, NULL, msg_out );
    /* for backward compatibility */
    if ( rc == ENOENT )
        rc = GetBoolParam( config_blk, block_name, "notify_hw", 0,
                           &tmpval, NULL, NULL, msg_out );
    if ( rc == ENOENT )
        rc = GetBoolParam( config_blk, block_name, "notify", 0,
                           &tmpval, NULL, NULL, msg_out );

    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc == 0 )
        p_trigger_item->alert_hw = tmpval;

    rc = GetBoolParam( config_blk, block_name, "alert_low", 0,
                       &tmpval, NULL, NULL, msg_out );
    /* for backward compatibility */
    if ( rc == ENOENT )
        rc = GetBoolParam( config_blk, block_name, "alert_lw", 0,
                           &tmpval, NULL, NULL, msg_out );

    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc == 0 )
        p_trigger_item->alert_lw = tmpval;

    CheckUnknownParameters( config_blk, block_name, trigger_expect );

    return 0;

}

#define critical_err_check(_ptr_, _blkname_) do { if (!_ptr_) {\
                                        sprintf( msg_out, "Internal error reading %s block in config file", _blkname_); \
                                        return EFAULT; \
                                    }\
                                } while (0)

int Read_ResourceMon_Config( config_file_t config,
                             void *module_config, char *msg_out, int for_reload )
{
    int            rc;
    int            intval;
    resource_monitor_config_t *conf = ( resource_monitor_config_t * ) module_config;

    unsigned int   blc_index;

    static const char *purge_allowed[] = {
        "nb_threads_purge", "post_purge_df_latency",
        "purge_queue_size", "db_result_size_max",
        "sort",
#ifdef ATTR_INDEX_status
        "check_purge_status_on_startup",
#endif
        "recheck_ignored_classes",
#ifdef _TMP_FS_MGR
        "purge_command",
#endif
        NULL
    };

    /* get PURGE_PARAM block */

    config_item_t  param_block = rh_config_FindItemByName( config, PURGE_PARAM_BLOCK );
    if ( param_block != NULL )
    {
        /* no error, because no parameter is mandatory */

        /* check this is a block... */
        if ( rh_config_ItemType( param_block ) != CONFIG_ITEM_BLOCK )
        {
            strcpy( msg_out, "A block is expected for '" PURGE_PARAM_BLOCK "' item" );
            return EINVAL;
        }

        /* parse parameters */
        rc = GetIntParam( param_block, PURGE_PARAM_BLOCK, "nb_threads_purge",
                          INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL, ( int * ) &conf->nb_threads_purge,
                          NULL, NULL, msg_out );
        if ( ( rc != 0 ) && ( rc != ENOENT ) )
            return rc;

        rc = GetDurationParam( param_block, PURGE_PARAM_BLOCK, "post_purge_df_latency", INT_PARAM_POSITIVE, /* 0 is authorized: no delay */
                               &intval, NULL, NULL, msg_out );
        if ( ( rc != 0 ) && ( rc != ENOENT ) )
            return rc;
        else if ( rc != ENOENT )
            conf->post_purge_df_latency = intval;

        rc = GetIntParam( param_block, PURGE_PARAM_BLOCK, "purge_queue_size",
                          INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL, ( int * ) &conf->purge_queue_size,
                          NULL, NULL, msg_out );
        if ( ( rc != 0 ) && ( rc != ENOENT ) )
            return rc;

        rc = GetIntParam( param_block, PURGE_PARAM_BLOCK, "db_result_size_max",
                          INT_PARAM_POSITIVE, ( int * ) &conf->db_request_limit, NULL, NULL, msg_out );
        if ( ( rc != 0 ) && ( rc != ENOENT ) )
            return rc;

        rc = GetBoolParam(param_block, PURGE_PARAM_BLOCK, "sort",
                          0, &intval, NULL, NULL, msg_out);
        if ((rc != 0) && (rc != ENOENT))
            return rc;
        else if (rc != ENOENT)
            conf->sort = intval;

#ifdef ATTR_INDEX_status
        rc = GetBoolParam( param_block, PURGE_PARAM_BLOCK, "check_purge_status_on_startup",
                           0, &intval, NULL, NULL, msg_out );
        if ( ( rc != 0 ) && ( rc != ENOENT ) )
            return rc;
        else if ( rc != ENOENT )
            conf->check_purge_status_on_startup = intval;
#endif
        rc = GetBoolParam( param_block, PURGE_PARAM_BLOCK, "recheck_ignored_classes",
                           0, &intval, NULL, NULL, msg_out );
        if ( ( rc != 0 ) && ( rc != ENOENT ) )
            return rc;
        else if ( rc != ENOENT )
            conf->recheck_ignored_classes = intval;

        rc = GetBoolParam( param_block, PURGE_PARAM_BLOCK, "simulation_mode",
                           0, &intval, NULL, NULL, msg_out );
        if ( rc == 0 )
        {
            DisplayLog( LVL_CRIT, RESMONCFG_TAG,
                "WARNING: 'simulation_mode' parameter is deprecated. Use '--dry-run' option instead.");
        }

#ifdef _TMP_FS_MGR
        rc = GetStringParam(param_block, PURGE_PARAM_BLOCK, "purge_command",
                         STR_PARAM_ABSOLUTE_PATH, /* can contain wildcards: {path}, {fid}, {fsname} */
                         conf->purge_command, RBH_PATH_MAX, NULL, NULL, msg_out);
        if ((rc != 0) && (rc != ENOENT))
            return rc;
#endif

        CheckUnknownParameters( param_block, PURGE_PARAM_BLOCK, purge_allowed );

    } /* end of purge parameters */

    /* get PURGE_TRIGGER blocks */

    for ( blc_index = 0; blc_index < rh_config_GetNbBlocks( config ); blc_index++ )
    {
        char          *block_name;
        config_item_t  curr_item = rh_config_GetBlockByIndex( config, blc_index );
        critical_err_check( curr_item, "root" );

        if ( rh_config_ItemType( curr_item ) != CONFIG_ITEM_BLOCK )
            continue;

        block_name = rh_config_GetBlockName( curr_item );
        critical_err_check( block_name, "root" );

        if ( !strcasecmp( block_name, TRIGGER_BLOCK ) )
        {
            if ( conf->trigger_count == 0 )
                conf->trigger_list = ( trigger_item_t * ) malloc( sizeof( trigger_item_t ) );
            else
                conf->trigger_list =
                    ( trigger_item_t * ) realloc( conf->trigger_list,
                                                  ( conf->trigger_count +
                                                    1 ) * sizeof( trigger_item_t ) );

            conf->trigger_count++;

            /* analyze trigger block */
            rc = parse_trigger_block( curr_item, block_name,
                                      &conf->trigger_list[conf->trigger_count - 1], msg_out );

            if ( rc )
                return rc;
        }
    }

    return 0;
}


/** Update purge policy triggers */
static void update_triggers( trigger_item_t * trigger_list, unsigned int trigger_count )
{
    unsigned int   i;
    int            check_interval_chgd = FALSE;

    if ( trigger_count != resmon_config.trigger_count )
    {
        /* skip trigger checking & update */
        DisplayLog( LVL_MAJOR, RESMONCFG_TAG,
                    "Trigger count changed in config file but cannot be modified dynamically: trigger update cancelled" );
        return;
    }

    /* check trigger types */
    for ( i = 0; i < trigger_count; i++ )
    {
        if ( trigger_list[i].type != resmon_config.trigger_list[i].type )
        {
            DisplayLog( LVL_MAJOR, RESMONCFG_TAG,
                        "Trigger type changed (%d<>%d) in config file but cannot be modified dynamically: trigger update cancelled",
                        trigger_list[i].type, resmon_config.trigger_list[i].type );
            return;
        }
        else if ( ( trigger_list[i].type != TRIGGER_CUSTOM_CMD ) &&
                  ( trigger_list[i].type != TRIGGER_ALWAYS ) &&
                  ( trigger_list[i].hw_type != resmon_config.trigger_list[i].hw_type ) )
        {
            DisplayLog( LVL_MAJOR, RESMONCFG_TAG,
                        "High threshold type changed (%d<>%d) in config file but cannot be modified dynamically: trigger update cancelled",
                        trigger_list[i].hw_type, resmon_config.trigger_list[i].hw_type );
            return;
        }
        else if ( ( trigger_list[i].type != TRIGGER_CUSTOM_CMD ) &&
                  ( trigger_list[i].type != TRIGGER_ALWAYS ) &&
                  ( trigger_list[i].lw_type != resmon_config.trigger_list[i].lw_type ) )
        {
            DisplayLog( LVL_MAJOR, RESMONCFG_TAG,
                        "Low threshold type changed (%d<>%d) in config file but cannot be modified dynamically: trigger update cancelled",
                        trigger_list[i].lw_type, resmon_config.trigger_list[i].lw_type );
            return;
        }
    }

    /* triggers have the same type: update simple parameters: threshold levels and check interval */
    for ( i = 0; i < trigger_count; i++ )
    {
        if ( trigger_list[i].check_interval != resmon_config.trigger_list[i].check_interval )
        {
            DisplayLog( LVL_EVENT, RESMONCFG_TAG, "check_interval updated for trigger #%u: %lu->%lu",
                        i, resmon_config.trigger_list[i].check_interval,
                        trigger_list[i].check_interval );
            resmon_config.trigger_list[i].check_interval = trigger_list[i].check_interval;
            check_interval_chgd = TRUE;
        }

        if ( trigger_list[i].alert_hw != resmon_config.trigger_list[i].alert_hw )
        {
            DisplayLog( LVL_EVENT, RESMONCFG_TAG, "alert_high updated for trigger #%u: %s->%s",
                        i, bool2str(resmon_config.trigger_list[i].alert_hw),
                        bool2str(trigger_list[i].alert_hw) );
            resmon_config.trigger_list[i].alert_hw = trigger_list[i].alert_hw;
        }

        if ( trigger_list[i].alert_lw != resmon_config.trigger_list[i].alert_lw )
        {
            DisplayLog( LVL_EVENT, RESMONCFG_TAG, "alert_low updated for trigger #%u: %s->%s",
                        i, bool2str(resmon_config.trigger_list[i].alert_lw),
                        bool2str(trigger_list[i].alert_lw) );
            resmon_config.trigger_list[i].alert_lw = trigger_list[i].alert_lw;
        }

        /* no thresholds for custom cmd */
        if ( trigger_list[i].type == TRIGGER_CUSTOM_CMD )
        {
            if ( strcmp( trigger_list[i].list[0], resmon_config.trigger_list[i].list[0] ) )
            {
                DisplayLog( LVL_MAJOR, RESMONCFG_TAG,
                            "External command for trigger #%u changed in config file but cannot be modified dynamically",
                            i );
            }
            /* do nothing in all cases */
            continue;
        } else if ( trigger_list[i].type == TRIGGER_ALWAYS )
            /* no threshold for 'periodic' triggers */
            continue;

        switch ( trigger_list[i].hw_type )
        {
        case PCT_THRESHOLD:
            if ( trigger_list[i].hw_percent != resmon_config.trigger_list[i].hw_percent )
            {
                DisplayLog( LVL_EVENT, RESMONCFG_TAG,
                            "High threshold updated for trigger #%u: %.2f%%->%.2f%%", i,
                            resmon_config.trigger_list[i].hw_percent, trigger_list[i].hw_percent );
                resmon_config.trigger_list[i].hw_percent = trigger_list[i].hw_percent;
            }
            break;

        case VOL_THRESHOLD:
            if ( trigger_list[i].hw_volume != resmon_config.trigger_list[i].hw_volume )
            {
                DisplayLog( LVL_EVENT, RESMONCFG_TAG,
                            "High threshold updated for trigger #%u: %llu bytes->%llu bytes", i,
                            resmon_config.trigger_list[i].hw_volume, trigger_list[i].hw_volume );
                resmon_config.trigger_list[i].hw_volume = trigger_list[i].hw_volume;
            }
            break;

        case COUNT_THRESHOLD:
            if ( trigger_list[i].hw_count != resmon_config.trigger_list[i].hw_count )
            {
                DisplayLog( LVL_EVENT, RESMONCFG_TAG,
                            "High threshold updated for trigger #%u: %llu files->%llu files", i,
                            resmon_config.trigger_list[i].hw_count, trigger_list[i].hw_count );
                resmon_config.trigger_list[i].hw_count = trigger_list[i].hw_count;
            }
            break;
        }

        switch ( trigger_list[i].lw_type )
        {
        case PCT_THRESHOLD:
            if ( trigger_list[i].lw_percent != resmon_config.trigger_list[i].lw_percent )
            {
                DisplayLog( LVL_EVENT, RESMONCFG_TAG,
                            "Low threshold updated for trigger #%u: %.2f%%->%.2f%%", i,
                            resmon_config.trigger_list[i].lw_percent, trigger_list[i].lw_percent );
                resmon_config.trigger_list[i].lw_percent = trigger_list[i].lw_percent;
            }
            break;

        case VOL_THRESHOLD:
            if ( trigger_list[i].lw_volume != resmon_config.trigger_list[i].lw_volume )
            {
                DisplayLog( LVL_EVENT, RESMONCFG_TAG,
                            "Low threshold updated for trigger #%u: %llu bytes->%llu bytes", i,
                            resmon_config.trigger_list[i].lw_volume, trigger_list[i].lw_volume );
                resmon_config.trigger_list[i].lw_volume = trigger_list[i].lw_volume;
            }
            break;

        case COUNT_THRESHOLD:
            if ( trigger_list[i].lw_count != resmon_config.trigger_list[i].lw_count )
            {
                DisplayLog( LVL_EVENT, RESMONCFG_TAG,
                            "Low threshold updated for trigger #%u: %llu files->%llu files", i,
                            resmon_config.trigger_list[i].lw_count, trigger_list[i].lw_count );
                resmon_config.trigger_list[i].lw_count = trigger_list[i].lw_count;
            }
            break;

        }

    }

    /* update global interval check (GCD of all check intervals) if one of them changed */
    if ( check_interval_chgd )
        ResMon_UpdateCheckInterval(  );

    /* triggers have been updated */
    return;

}

static void free_triggers( trigger_item_t * p_triggers, unsigned int count )
{
    unsigned int   i, j;
    for ( i = 0; i < count; i++ )
    {
        if ( ( p_triggers[i].list_size > 0 ) && ( p_triggers[i].list != NULL ) )
        {
            /* free the strings */
            for ( j = 0; j < p_triggers[i].list_size; j++ )
            {
                if ( p_triggers[i].list[j] != NULL )
                    free( p_triggers[i].list[j] );
            }

            /* free the arg list */
            free( p_triggers[i].list );
        }
    }

    /* free the trigger list */
    if ( ( count > 0 ) && ( p_triggers != NULL ) )
        free( p_triggers );
}


int Reload_ResourceMon_Config( void *module_config )
{
    resource_monitor_config_t *conf = ( resource_monitor_config_t * ) module_config;

    /* parameters that can't be modified dynamically */

    if ( resmon_config.nb_threads_purge != conf->nb_threads_purge )
        DisplayLog( LVL_MAJOR, RESMONCFG_TAG, PURGE_PARAM_BLOCK
                    "::nb_threads_purge changed in config file, but cannot be modified dynamically");

    if ( resmon_config.purge_queue_size != conf->purge_queue_size )
        DisplayLog( LVL_MAJOR, RESMONCFG_TAG, PURGE_PARAM_BLOCK
                    "::purge_queue_size changed in config file, but cannot be modified dynamically");

#ifdef _TMP_FS_MGR
    if (strcmp(conf->purge_command, resmon_config.purge_command))
        DisplayLog(LVL_MAJOR, RESMONCFG_TAG, PURGE_PARAM_BLOCK
                   "::purge_command changed in config file, but cannot be modified dynamically");
#endif

    /* dynamic parameters */

    if ( resmon_config.post_purge_df_latency != conf->post_purge_df_latency )
    {
        DisplayLog( LVL_EVENT, RESMONCFG_TAG, PURGE_PARAM_BLOCK
                    "::post_purge_df_latency updated: %lu->%lu",
                    resmon_config.post_purge_df_latency, conf->post_purge_df_latency );
        resmon_config.post_purge_df_latency = conf->post_purge_df_latency;
    }

    if ( resmon_config.db_request_limit != conf->db_request_limit )
    {
        DisplayLog( LVL_EVENT, RESMONCFG_TAG, PURGE_PARAM_BLOCK
                    "::db_result_size_max updated: %u->%u",
                    resmon_config.db_request_limit, conf->db_request_limit );
        resmon_config.db_request_limit = conf->db_request_limit;
    }

#ifdef ATTR_INDEX_status
    if ( resmon_config.check_purge_status_on_startup != conf->check_purge_status_on_startup )
    {
        DisplayLog( LVL_EVENT, RESMONCFG_TAG, PURGE_PARAM_BLOCK
                    "::check_purge_status_on_startup updated: %u->%u",
                    resmon_config.check_purge_status_on_startup,
                    conf->check_purge_status_on_startup );
        resmon_config.check_purge_status_on_startup = conf->check_purge_status_on_startup;
    }
#endif
    if ( resmon_config.recheck_ignored_classes != conf->recheck_ignored_classes )
    {
        DisplayLog( LVL_EVENT, RESMONCFG_TAG, PURGE_PARAM_BLOCK
                    "::recheck_ignored_classes updated: %u->%u",
                    resmon_config.recheck_ignored_classes, conf->recheck_ignored_classes );
        resmon_config.recheck_ignored_classes = conf->recheck_ignored_classes;
    }


    update_triggers( conf->trigger_list, conf->trigger_count );

    free_triggers( conf->trigger_list, conf->trigger_count );

    return 0;
}
