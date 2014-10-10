/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2007, 2008, 2009 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
/**
 * FS scan stop/start routines
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "fs_scan_main.h"
#include "fs_scan.h"
#include "RobinhoodMisc.h"
#include "RobinhoodConfig.h"
#include <pthread.h>
#include <errno.h>
#include <unistd.h>

static pthread_t scan_starter_thread;
static pthread_attr_t starter_attr;
static int     terminate = FALSE;

/* Scan starter thread */
static void   *scan_starter( void *arg )
{
    int            rc;

    DisplayLog( LVL_VERB, FSSCAN_TAG, "Launching FS Scan starter thread" );

    if ( fsscan_flags & FLAG_ONCE )
    {
        rc = Robinhood_CheckScanDeadlines(  );
        if ( rc )
            DisplayLog( LVL_CRIT, FSSCAN_TAG, "Error %d checking FS Scan status", rc );
        pthread_exit( NULL );
        return NULL;
    }

    /* not a one-shot mode */
    while ( !terminate )
    {
        rc = Robinhood_CheckScanDeadlines(  );
        if ( rc )
            DisplayLog( LVL_CRIT, FSSCAN_TAG, "Error %d checking FS Scan status", rc );

        /* attente de la boucle suivante */
        rh_sleep( fs_scan_config.spooler_check_interval );
    }

    return NULL;
}


/** Start FS Scan info collector */
int FSScan_Start( fs_scan_config_t *module_config, int flags, const char * partial_root )
{
    int            rc;
    fs_scan_config = *module_config;
    fsscan_flags = flags;
    partial_scan_root = partial_root;

    if (partial_root)
    {
        /* check that partial_root is under FS root */
        if (strncmp(global_config.fs_path, partial_scan_root, strlen(global_config.fs_path)))
        {
            DisplayLog( LVL_CRIT, FSSCAN_TAG, "ERROR scan root %s is not under fs root %s",
                        partial_scan_root, global_config.fs_path );
            return EINVAL;
        }
    }

    rc = Robinhood_InitScanModule();
    if ( rc )
        return rc;

    /* start a background thread */

    pthread_attr_init( &starter_attr );
    pthread_attr_setscope( &starter_attr, PTHREAD_SCOPE_SYSTEM );

    if ( pthread_create( &scan_starter_thread, &starter_attr, scan_starter, NULL ) )
    {
        return errno;
    }

    return 0;
}

/** Wait for scan termination */
void FSScan_Wait( void )
{
    wait_scan_finished(  );
}


/** Stop FS Scan info collector */
void FSScan_Terminate( void )                         /* @TODO */
{
    terminate = TRUE;

    Robinhood_StopScanModule( );
}

/** Store FS Scan into database */
void FSScan_StoreStats( lmgr_t * lmgr )
{
    robinhood_fsscan_stat_t stats;
    char           tmp_buff[256];

    Robinhood_StatsScan( &stats );

    /* store the number of scanning threads */
    sprintf( tmp_buff, "%i", fs_scan_config.nb_threads_scan );
    ListMgr_SetVar( lmgr, LAST_SCAN_NB_THREADS, tmp_buff );

    if ( stats.scan_running )
    {
        if ( stats.last_action > 0 )
        {
           sprintf( tmp_buff, "%lu", ( unsigned long ) stats.last_action );
           ListMgr_SetVar( lmgr, LAST_SCAN_LAST_ACTION_TIME, tmp_buff);
        }

        if ( stats.scanned_entries )
        {
            sprintf(tmp_buff, "%u", stats.scanned_entries);
            ListMgr_SetVar( lmgr, LAST_SCAN_ENTRIES_SCANNED, tmp_buff);
            sprintf(tmp_buff, "%u", stats.error_count);
            ListMgr_SetVar( lmgr, LAST_SCAN_ERRORS, tmp_buff);
            sprintf(tmp_buff, "%.2f", stats.avg_ms_per_entry);
            ListMgr_SetVar( lmgr, LAST_SCAN_AVGMSPE, tmp_buff);
            sprintf(tmp_buff, "%.2f", stats.curr_ms_per_entry);
            ListMgr_SetVar( lmgr, LAST_SCAN_CURMSPE, tmp_buff);
        }
    }
    sprintf(tmp_buff, "%u", stats.nb_hang);
    ListMgr_SetVar( lmgr, LAST_SCAN_TIMEOUTS, tmp_buff);

}


/** Dump FS Scan stats to log file */
void FSScan_DumpStats( void )
{
    robinhood_fsscan_stat_t stats;
    struct tm      paramtm;
    char           tmp_buff[256];
    char           tmp_buff2[256];

    Robinhood_StatsScan( &stats );

    DisplayLog( LVL_MAJOR, "STATS", "======== FS scan statistics =========" );

    if ( stats.last_fsscan_time != 0 )
    {
        strftime( tmp_buff, 256, "%Y/%m/%d %T", localtime_r( &stats.last_fsscan_time, &paramtm ) );

        DisplayLog( LVL_MAJOR, "STATS", "last scan  = %s", tmp_buff );

        FormatDuration( tmp_buff, 256, stats.last_duration );

        DisplayLog( LVL_MAJOR, "STATS", "duration    = %s (%u s)", tmp_buff, stats.last_duration );
        DisplayLog( LVL_MAJOR, "STATS", "status      = %s",
                    ( stats.scan_complete ? "complete" : "incomplete" ) );
    }

    if ( stats.current_scan_interval != 0 )
    {
        FormatDurationFloat( tmp_buff, 256, stats.current_scan_interval );
        DisplayLog( LVL_MAJOR, "STATS", "current scan interval = %s", tmp_buff );
    }

    if ( stats.scan_running )
    {
        time_t now = time(NULL);

        DisplayLog( LVL_MAJOR, "STATS", "scan is running:" );

        strftime( tmp_buff, 256, "%Y/%m/%d %T", localtime_r( &stats.start_time, &paramtm ) );
        FormatDurationFloat( tmp_buff2, 256, now - stats.start_time );

        DisplayLog( LVL_MAJOR, "STATS", "     started at : %s (%s ago)", tmp_buff, tmp_buff2 );

        strftime( tmp_buff, 256, "%Y/%m/%d %T", localtime_r( &stats.last_action, &paramtm ) );
        FormatDurationFloat( tmp_buff2, 256, now - stats.last_action );

        DisplayLog( LVL_MAJOR, "STATS", "     last action: %s (%s ago)", tmp_buff, tmp_buff2 );

        if ( stats.scanned_entries )
        {
            double         speed;

            DisplayLog(LVL_MAJOR, "STATS", "     progress   : %u entries scanned (%u errors)",
                       stats.scanned_entries, stats.error_count);

            if (stats.curr_ms_per_entry > 0.0)
                speed = ( 1000.0 / stats.curr_ms_per_entry ) * fs_scan_config.nb_threads_scan;
            else
                speed = 0.0;

            DisplayLog(LVL_MAJOR, "STATS",     "     inst. speed (potential): %9.2f entries/sec (%4.2f ms/entry/thread)",
                       speed, stats.curr_ms_per_entry);

            if (now - stats.start_time > 0)
                DisplayLog(LVL_MAJOR, "STATS", "     avg. speed  (effective): %9.2f entries/sec (%4.2f ms/entry/thread)",
                           (float)stats.scanned_entries/(float)(now - stats.start_time),
                           stats.avg_ms_per_entry);
        }
    }

    if (stats.nb_hang > 0)
        DisplayLog( LVL_MAJOR, "STATS", "scan operation timeouts = %u", stats.nb_hang );

}

/* ------------ Config management functions --------------- */

#define FSSCAN_CONFIG_BLOCK  "FS_Scan"
#define IGNORE_BLOCK  "Ignore"

#define MINUTE 60
#define HOUR 3600
#define DAY (24*HOUR)

int FSScan_SetDefaultConfig( void *module_config, char *msg_out )
{
    fs_scan_config_t *conf = ( fs_scan_config_t * ) module_config;
    msg_out[0] = '\0';

#ifdef _LUSTRE_HSM
    conf->min_scan_interval = 24 * HOUR;
    conf->max_scan_interval = 7 * DAY;
#else
    conf->min_scan_interval = 2 * HOUR;
    conf->max_scan_interval = 12 * HOUR;
#endif
    conf->scan_retry_delay = HOUR;
    conf->nb_threads_scan = 2;
    conf->scan_op_timeout = 0;
    conf->exit_on_timeout = FALSE;
    conf->spooler_check_interval = MINUTE;
    conf->nb_prealloc_tasks = 256;

    conf->ignore_list = NULL;
    conf->ignore_count = 0;
    rh_strncpy(conf->completion_command, "", RBH_PATH_MAX);

    return 0;
}

int FSScan_WriteDefaultConfig( FILE * output )
{
    print_begin_block( output, 0, FSSCAN_CONFIG_BLOCK, NULL );
#ifdef _LUSTRE_HSM
    print_line( output, 1, "min_scan_interval      :   24h" );
    print_line( output, 1, "max_scan_interval      :    7d" );
#else
    print_line( output, 1, "min_scan_interval      :    2h" );
    print_line( output, 1, "max_scan_interval      :   12h" );
#endif
    print_line( output, 1, "scan_retry_delay       :    1h" );
    print_line( output, 1, "nb_threads_scan        :     2" );
    print_line( output, 1, "scan_op_timeout        :     0 (disabled)" );
    print_line( output, 1, "exit_on_timeout        : FALSE" );
    print_line( output, 1, "spooler_check_interval :  1min" );
    print_line( output, 1, "nb_prealloc_tasks      :   256" );
    print_line( output, 1, "ignore                 :  NONE" );
    print_line( output, 1, "completion_command     :  NONE" );
    print_end_block( output, 0 );
    return 0;
}


#define critical_err_check(_ptr_, _blkname_) do { if (!_ptr_) {\
                                        sprintf( msg_out, "Internal error reading %s block in config file", _blkname_); \
                                        return EFAULT; \
                                    }\
                                } while (0)



int FSScan_ReadConfig( config_file_t config, void *module_config, char *msg_out, int for_reload )
{
    int            rc, blc_index;
    fs_scan_config_t *conf = ( fs_scan_config_t * ) module_config;
    int scan_intl_set = FALSE;
    int scan_intl = 0;

    static const char * fsscan_allowed[] =
    {
        "scan_interval", "min_scan_interval", "max_scan_interval",
        "scan_retry_delay", "nb_threads_scan", "scan_op_timeout",
        "exit_on_timeout", "spooler_check_interval", "nb_prealloc_tasks",
		"completion_command",
        IGNORE_BLOCK, NULL
    };

    /* get FS Scan block */

    config_item_t  fsscan_block = rh_config_FindItemByName( config, FSSCAN_CONFIG_BLOCK );

    if ( fsscan_block == NULL )
    {
        strcpy( msg_out, "Missing configuration block '" FSSCAN_CONFIG_BLOCK "'" );
        /* No error because no parameter is mandatory  */
        return 0;
    }

    if ( rh_config_ItemType( fsscan_block ) != CONFIG_ITEM_BLOCK )
    {
        strcpy( msg_out, "A block is expected for '" FSSCAN_CONFIG_BLOCK "' item" );
        return EINVAL;
    }

    /* retrieve parameters */
    rc = GetDurationParam( fsscan_block, FSSCAN_CONFIG_BLOCK,
                           "min_scan_interval",
                           INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL,
                           ( int * ) &conf->min_scan_interval, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc == 0 )
        scan_intl_set = TRUE;

    rc = GetDurationParam( fsscan_block, FSSCAN_CONFIG_BLOCK,
                           "max_scan_interval",
                           INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL,
                           ( int * ) &conf->max_scan_interval, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc == 0 )
        scan_intl_set = TRUE;

    rc = GetDurationParam( fsscan_block, FSSCAN_CONFIG_BLOCK,
                           "scan_interval",
                           INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL,
                           &scan_intl, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc == 0 )
    {
        if (scan_intl_set)
        {
            strcpy( msg_out, "scan_interval parameter cannot be used with min/max_scan_interval" );
            return EINVAL;
        }
        conf->min_scan_interval = scan_intl;
        conf->max_scan_interval = scan_intl;
    }

    rc = GetDurationParam( fsscan_block, FSSCAN_CONFIG_BLOCK,
                           "scan_retry_delay",
                           INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL,
                           ( int * ) &conf->scan_retry_delay, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetIntParam( fsscan_block, FSSCAN_CONFIG_BLOCK,
                      "nb_threads_scan",
                      INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL,
                      ( int * ) &conf->nb_threads_scan, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetDurationParam( fsscan_block, FSSCAN_CONFIG_BLOCK, "scan_op_timeout",
                            INT_PARAM_POSITIVE,    /* 0 is authorized => no timeout */
                           ( int * ) &conf->scan_op_timeout, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetBoolParam( fsscan_block, FSSCAN_CONFIG_BLOCK, "exit_on_timeout", 0,
                       (int*)&conf->exit_on_timeout, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetDurationParam( fsscan_block, FSSCAN_CONFIG_BLOCK,
                           "spooler_check_interval",
                           INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL,
                           ( int * ) &conf->spooler_check_interval, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetDurationParam( fsscan_block, FSSCAN_CONFIG_BLOCK,
                           "nb_prealloc_tasks",
                           INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL,
                           ( int * ) &conf->nb_prealloc_tasks, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetStringParam( fsscan_block, FSSCAN_CONFIG_BLOCK, "completion_command",
                         0, /* can contain wildcards: {cfg} or {fspath} */
                         conf->completion_command, RBH_PATH_MAX, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;


    /* Find and parse "ignore" blocks */
    for ( blc_index = 0; blc_index < rh_config_GetNbItems( fsscan_block ); blc_index++ )
    {
        char          *block_name;
        config_item_t  curr_item = rh_config_GetItemByIndex( fsscan_block, blc_index );
        critical_err_check( curr_item, FSSCAN_CONFIG_BLOCK );

        if ( rh_config_ItemType( curr_item ) != CONFIG_ITEM_BLOCK )
            continue;

        block_name = rh_config_GetBlockName( curr_item );
        critical_err_check( curr_item, FSSCAN_CONFIG_BLOCK );

        if ( !strcasecmp( block_name, IGNORE_BLOCK ) )
        {
            if ( conf->ignore_count == 0 )
                conf->ignore_list = ( whitelist_item_t * ) malloc( sizeof( whitelist_item_t ) );
            else
                conf->ignore_list =
                    ( whitelist_item_t * ) realloc( conf->ignore_list,
                                                 ( conf->ignore_count + 1 )
                                                 * sizeof( whitelist_item_t ) );

            conf->ignore_count++;

            /* analyze boolean expression */
            rc = GetBoolExpr( curr_item, block_name,
                              &conf->ignore_list[conf->ignore_count - 1].bool_expr,
                              &conf->ignore_list[conf->ignore_count - 1].attr_mask, msg_out );

            if ( rc )
                return rc;

        }
    }                           /* Loop on subblocks */

    CheckUnknownParameters( fsscan_block, FSSCAN_CONFIG_BLOCK, fsscan_allowed );

    return 0;

}

#define RELOAD_TAG  "FS_Scan_Config"

/** Update ignore rules */
static void update_ignore( whitelist_item_t * old_items, unsigned int old_count,
                      whitelist_item_t * new_items, unsigned int new_count,
                      const char * block_name )
{
   unsigned int i;

   if ( old_count != new_count )
   {
        DisplayLog( LVL_MAJOR, RELOAD_TAG, "Ignore rules count changed in block '%s' but cannot be modified dynamically: ignore update cancelled",
                block_name );
        return;
   }

   /* compare ignore boolean expression structure */
   for (i = 0; i < new_count; i++ )
   {
        if ( (old_items[i].attr_mask != new_items[i].attr_mask)
             || compare_boolexpr( &old_items[i].bool_expr, &new_items[i].bool_expr) )
        {
           DisplayLog( LVL_MAJOR, RELOAD_TAG, "Ignore expression #%u changed in block '%s'. Only numerical values can be modified dynamically. Ignore update cancelled", i, block_name );
           return;
        }
   }

   /* if they are all the same, update/check their values */

   for (i = 0; i < new_count; i++ )
   {
       if ( update_boolexpr( &old_items[i].bool_expr, &new_items[i].bool_expr ) )
       {
            char criteriastr[2048];
            BoolExpr2str( &old_items[i].bool_expr, criteriastr, 2048 );
            DisplayLog( LVL_EVENT, RELOAD_TAG, "Ignore expression #%u in block '%s' has been updated and is now: %s",
                i, block_name, criteriastr );
       }
   }

    /* XXX attr_mask is unchanged, since we keep the same expression structures */
} /* update_ignore */

static void free_ignore( whitelist_item_t * p_items, unsigned int count )
{
    unsigned int i;

    for ( i = 0; i < count; i++ )
        FreeBoolExpr( &p_items[i].bool_expr, FALSE );

    if ( (count > 0) && (p_items!= NULL) )
        free(p_items);
}


int FSScan_ReloadConfig( void *module_config )
{
    fs_scan_config_t *conf = ( fs_scan_config_t * ) module_config;

    /* Parameters that can be modified dynamically */

    if ( conf->min_scan_interval != fs_scan_config.min_scan_interval )
    {
        DisplayLog( LVL_EVENT, "FS_Scan_Config",
                    FSSCAN_CONFIG_BLOCK "::min_scan_interval updated: %u->%u",
                    fs_scan_config.min_scan_interval, conf->min_scan_interval );
        fs_scan_config.min_scan_interval = conf->min_scan_interval;
    }

    if ( conf->max_scan_interval != fs_scan_config.max_scan_interval )
    {
        DisplayLog( LVL_EVENT, "FS_Scan_Config",
                    FSSCAN_CONFIG_BLOCK "::max_scan_interval updated: %u->%u",
                    fs_scan_config.max_scan_interval, conf->max_scan_interval );
        fs_scan_config.max_scan_interval = conf->max_scan_interval;
    }

    if ( conf->scan_retry_delay != fs_scan_config.scan_retry_delay )
    {
        DisplayLog( LVL_EVENT, "FS_Scan_Config",
                    FSSCAN_CONFIG_BLOCK "::scan_retry_delay updated: %u->%u",
                    fs_scan_config.scan_retry_delay, conf->scan_retry_delay );
        fs_scan_config.scan_retry_delay = conf->scan_retry_delay;
    }

    if ( conf->scan_op_timeout != fs_scan_config.scan_op_timeout )
    {
        DisplayLog( LVL_EVENT, "FS_Scan_Config",
                    FSSCAN_CONFIG_BLOCK "::scan_op_timeout updated: %u->%u",
                    fs_scan_config.scan_op_timeout, conf->scan_op_timeout );
        fs_scan_config.scan_op_timeout = conf->scan_op_timeout;
    }

    if ( conf->exit_on_timeout != fs_scan_config.exit_on_timeout )
    {
        DisplayLog( LVL_EVENT, "FS_Scan_Config",
                    FSSCAN_CONFIG_BLOCK "::exit_on_timeout updated: %s->%s",
                    bool2str(fs_scan_config.exit_on_timeout),
                    bool2str(conf->exit_on_timeout) );
        fs_scan_config.exit_on_timeout = conf->exit_on_timeout;
    }

    if ( conf->spooler_check_interval != fs_scan_config.spooler_check_interval )
    {
        DisplayLog( LVL_EVENT, "FS_Scan_Config",
                    FSSCAN_CONFIG_BLOCK "::spooler_check_interval updated: %u->%u",
                    fs_scan_config.spooler_check_interval, conf->spooler_check_interval );
        fs_scan_config.spooler_check_interval = conf->spooler_check_interval;
    }

    if ( strcmp( conf->completion_command, fs_scan_config.completion_command ) )
    {
        DisplayLog( LVL_EVENT, "FS_Scan_Config",
                    FSSCAN_CONFIG_BLOCK "::completion_command updated: '%s'->'%s'",
                    fs_scan_config.completion_command, conf->completion_command );
        strcpy( fs_scan_config.completion_command, conf->completion_command);
    }


    /* Parameters that canNOT be modified dynamically */

    if ( conf->nb_threads_scan != fs_scan_config.nb_threads_scan )
        DisplayLog( LVL_MAJOR, "FS_Scan_Config",
                    FSSCAN_CONFIG_BLOCK
                    "::nb_threads_scan changed in config file, but cannot be modified dynamically" );


    if ( conf->nb_prealloc_tasks != fs_scan_config.nb_prealloc_tasks )
        DisplayLog( LVL_MAJOR, "FS_Scan_Config",
                    FSSCAN_CONFIG_BLOCK
                    "::nb_prealloc_tasks changed in config file, but cannot be modified dynamically" );

    /* compare ignore list */
    update_ignore( fs_scan_config.ignore_list, fs_scan_config.ignore_count,
                   conf->ignore_list, conf->ignore_count, FSSCAN_CONFIG_BLOCK );

    /* free conf structure */
    free_ignore( conf->ignore_list, conf->ignore_count );

    return 0;
}

int FSScan_WriteConfigTemplate( FILE * output )
{
    print_begin_block( output, 0, FSSCAN_CONFIG_BLOCK, NULL );

    print_line( output, 1, "# simple scan interval (fixed)" );
#ifdef HAVE_CHANGELOGS
    print_line( output, 1, "scan_interval      =   2d ;" );
#else
    print_line( output, 1, "scan_interval      =   6h ;" );
#endif
    fprintf( output, "\n" );

    print_line( output, 1, "# min/max for adaptive scan interval:" );
    print_line( output, 1, "# the more the filesystem is full, the more frequently it is scanned." );
#ifdef HAVE_CHANGELOGS
    print_line( output, 1, "#min_scan_interval      =   24h ;" );
    print_line( output, 1, "#max_scan_interval      =    7d ;" );
#else
    print_line( output, 1, "#min_scan_interval      =    2h ;" );
    print_line( output, 1, "#max_scan_interval      =   12h ;" );
#endif
    fprintf( output, "\n" );
    print_line( output, 1, "# number of threads used for scanning the filesystem" );
    print_line( output, 1, "nb_threads_scan        =     2 ;" );
    fprintf( output, "\n" );
    print_line( output, 1, "# when a scan fails, this is the delay before retrying" );
    print_line( output, 1, "scan_retry_delay       =    1h ;" );
    fprintf( output, "\n" );
    print_line( output, 1, "# timeout for operations on the filesystem" );
    print_line( output, 1, "scan_op_timeout        =    1h ;" );
    print_line( output, 1, "# exit if operation timeout is reached?" );
    print_line( output, 1, "exit_on_timeout        =    TRUE ;" );
    print_line( output, 1, "# external command called on scan termination");
    print_line( output, 1, "# special arguments can be specified: {cfg} = config file path,");
    print_line( output, 1, "# {fspath} = path to managed filesystem");
    print_line( output, 1, "#completion_command     =    \"/path/to/my/script.sh -f {cfg} -p {fspath}\" ;" );
    fprintf( output, "\n" );

    print_line( output, 1,
                "# Internal scheduler granularity (for testing and of scan, hangs, ...)" );
    print_line( output, 1, "spooler_check_interval =  1min ;" );
    fprintf( output, "\n" );
    print_line( output, 1, "# Memory preallocation parameters" );
    print_line( output, 1, "nb_prealloc_tasks      =   256 ;" );
    fprintf( output, "\n" );
    print_begin_block( output, 1, IGNORE_BLOCK, NULL );
    print_line( output, 2,
                "# ignore \".snapshot\" and \".snapdir\" directories (don't scan them)" );
    print_line( output, 2, "type == directory" );
    print_line( output, 2, "and" );
    print_line( output, 2, "( name == \".snapdir\" or name == \".snapshot\" )" );
    print_end_block( output, 1 );
    print_end_block( output, 0 );
    return 0;

}
