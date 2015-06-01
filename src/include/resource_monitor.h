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

/**
 * \file resource_monitor.h
 * \brief This module monitors filesystem resource usage
 * (using 'trigger_on' blocks of purge policies)
 * and start purges when needed.
 */
#ifndef _RESOURCE_MONITOR_H
#define _RESOURCE_MONITOR_H

#include "rbh_const.h"
#include "config_parsing.h"
#include <sys/types.h>
#include <stdbool.h>

/**
 *  Trigger type
 */
typedef enum
{
    TRIGGER_OST_USAGE,                           /**< trigger purge on OST usage */
    TRIGGER_GLOBAL_USAGE,                        /**< trigger purge on global filesystem usage */
    TRIGGER_POOL_USAGE,                          /**< trigger purge on OST pool usage */
    TRIGGER_USER_USAGE,                          /**< trigger purge on the space used by a user */
    TRIGGER_GROUP_USAGE,                         /**< trigger purge on the space used by a group */
    TRIGGER_ALWAYS,                              /**< always trigger purge at scheduled interval */
    TRIGGER_CUSTOM_CMD                           /**< trigger purge on a custom command */
} trigger_type_t;

typedef enum
{
    PCT_THRESHOLD,                               /**< threshold is specified as a percentage */
    VOL_THRESHOLD,                               /**< threshold is specified as a (used) size (in Bytes) */
    COUNT_THRESHOLD                              /**< threshold is specified as a number of entries */
} trigger_value_type_t;

typedef struct trigger_item_t
{
    trigger_type_t type;

    char         **list;                         /**< list of groups or user for user/group triggers. NULL=> apply to all.
                                                  * For custom commands, list[0] contains the command line. */
    unsigned int   list_size;

    time_t         check_interval;

    trigger_value_type_t hw_type;
    union
    {
        unsigned long long volume;
        double         percent;
        unsigned long long count;
    } hw_u;

#define hw_volume    hw_u.volume
#define hw_percent   hw_u.percent
#define hw_count     hw_u.count

    trigger_value_type_t lw_type;
    union
    {
        unsigned long long volume;
        double         percent;
        unsigned long long count;
    } lw_u;

#define lw_volume    lw_u.volume
#define lw_percent   lw_u.percent
#define lw_count     lw_u.count

    /* trigger options: */
    /* raise alert when it is triggered */
    unsigned int alert_hw:1;

    /* raise alert when it cannot reach low threshold */
    unsigned int alert_lw:1;

} trigger_item_t;


typedef struct resource_monitor_config_t
{
    unsigned int   nb_threads_purge;
    time_t         post_purge_df_latency;
    unsigned int   purge_queue_size;
    unsigned int   db_request_limit;

    trigger_item_t *trigger_list;
    unsigned int   trigger_count;

#ifdef _TMP_FS_MGR
	char         purge_command[RBH_PATH_MAX];
#endif

    bool recheck_ignored_classes;
    bool sort;

#ifdef ATTR_INDEX_status
    bool check_purge_status_on_startup;
#endif

} resource_monitor_config_t;

int            SetDefault_ResourceMon_Config( void *module_config, char *msg_out );
int            Read_ResourceMon_Config( config_file_t config,
                                        void *module_config, char *msg_out, int for_reload );
int            Reload_ResourceMon_Config( void *module_config );
int            Write_ResourceMon_ConfigTemplate( FILE * output );
int            Write_ResourceMon_ConfigDefault( FILE * output );

/** theses options specify the mode of running/purging */
typedef enum
{
    RESMON_DAEMON,        /**< run as a daemon */
    RESMON_ALL_TRIGGERS,  /**< check all triggers and terminate (one-shot mode) */
    RESMON_PURGE_OST,     /**< purge the specified OST and terminate (one-shot mode) */
    RESMON_PURGE_FS,      /**< purge the filesystem to the target level and terminate (one-shot mode) */
    RESMON_PURGE_CLASS,   /**< purge eligible files of the given fileclass and terminate (one-shot mode) */
} resmon_mode_t;

typedef struct resmon_opt_t
{
    resmon_mode_t  mode;
    int            flags;
    unsigned int   ost_index;  /**< targetted OST for RESMON_PURGE_OST mode */
    double         target_usage; /**< target usage for RESMON_PURGE_OST and RESMON_PURGE_FS */
    char *         fileclass;    /**< target fileclass for  RESMON_PURGE_CLASS */
} resmon_opt_t;


int            Start_ResourceMonitor( resource_monitor_config_t * p_config, resmon_opt_t options );

/* wait or abort a purge */
int            Wait_ResourceMonitor( void );
int            Stop_ResourceMonitor( void );

void           Dump_ResourceMonitor_Stats( void );

/** Recompute trigger check interval as the GCD of all triggers */
void           ResMon_UpdateCheckInterval( void );

#endif
