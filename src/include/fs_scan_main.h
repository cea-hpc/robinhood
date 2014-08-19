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
 * \file    fs_scan_main.h
 * \author  Th. Leibovici
 * \brief   Interface for FS Scan info provider.
 */

/**
 * \addtogroup FS_SCAN
 * @{
 */
#ifndef _FS_SCAN_MAIN_H
#define _FS_SCAN_MAIN_H

#include "config_parsing.h"
#include "policy_rules.h"

/** Configuration for FS scan Module */
typedef struct fs_scan_config_t
{
    /* scan options */

    unsigned int nb_threads_scan;
    time_t       min_scan_interval;
    time_t       max_scan_interval;
    time_t       scan_retry_delay;
    time_t       scan_op_timeout;
    bool         exit_on_timeout;

    /**
     * interval of the spooler (checks for audits to be launched,
     * thread hangs, ...) */
    time_t         spooler_check_interval;

    /** memory management */
    unsigned       nb_prealloc_tasks;

    /** ignore list (bool expr) */
    whitelist_item_t *ignore_list;
    unsigned int   ignore_count;

	char           completion_command[RBH_PATH_MAX];

} fs_scan_config_t;


/** start scanning module */
int            FSScan_Start( fs_scan_config_t * module_config, int flags, const char * partial_root );

/** terminate scanning module */
void           FSScan_Terminate( void );

/** wait for scan termination */
void           FSScan_Wait( void );

/** dump scan stats */
void           FSScan_DumpStats( void );

/** store scan stats in db */
void           FSScan_StoreStats( lmgr_t * lmgr );

/**
 * \addtogroup MODULE_CONFIG_FUNCTIONS
 * @{
 */
int            FSScan_SetDefaultConfig( void *module_config, char *msg_out );
int            FSScan_ReadConfig(config_file_t config, void *module_config,
                                 char *msg_out, bool for_reload);
int            FSScan_ReloadConfig( void *module_config );
int            FSScan_WriteConfigTemplate( FILE * output );
int            FSScan_WriteDefaultConfig( FILE * output );

#endif

/** @} */
/** @} */
