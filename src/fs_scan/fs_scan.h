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
 *  Module for filesystem scans.
 *
 */

#ifndef _FSSCAN_H
#define _FSSCAN_H

/* tag for logs */
#define FSSCAN_TAG "FS_Scan"

#include "fs_scan_types.h"
#include "fs_scan_main.h"

/* defined in fs_scan.c */
extern fs_scan_config_t fs_scan_config;
extern int     fsscan_flags;
extern const char * partial_scan_root;

/* Audit module relative types */

/**
 * Structure of audit statistics.
 */
typedef struct robinhood_fsscan_stat__
{
    /* stats about audits */
    time_t         last_fsscan_time;
    time_t         current_scan_interval;
    unsigned int   last_duration;
    int            scan_complete;
    int            scan_running;
    time_t         start_time;
    time_t         last_action;
    unsigned int   nb_hang;

    /* current scan progression */
    unsigned int   scanned_entries;
    unsigned int   error_count;
    double         avg_ms_per_entry;
    double         curr_ms_per_entry;

} robinhood_fsscan_stat_t;

/**
 * Audit module initialization
 * (called at deamon startup)
 *
 * The function looks at the content of the configuration structure
 * that have been previously parsed.
 *
 * It returns a status code:
 *   0 : initialization successful
 *   -1 : unexpected error at initialization.
 *   EINVAL : a parameter from the config file is invalid.
 */
int            Robinhood_InitScanModule( void );


/**
 * Stop audit module + wait for termination
 */
void           Robinhood_StopScanModule( void );


/**
 * Wait for scan termination (one shot mode).
 */
void           wait_scan_finished( void );


/**
 * Check if audit is to be started and thread hangs.
 * (called by the spooler)
 */
int            Robinhood_CheckScanDeadlines( void );

/**
 * Retrieve some statistics about current and terminated audits.
 * (called by the statistic collector)
 *
 * Take as parameter a structure of statistics to be filled.
 */
void           Robinhood_StatsScan( robinhood_fsscan_stat_t * p_stats );

#endif
