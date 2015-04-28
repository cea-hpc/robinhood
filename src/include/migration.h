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
 * \file migration.h
 * \brief This module triggers migrations to HSM or external storage.
 */
#ifndef _MIGRATION_H
#define _MIGRATION_H

#include "config_parsing.h"
#include <sys/types.h>
#include <stdbool.h>

typedef struct migration_config_t
{
    time_t         runtime_interval;
    unsigned int   nb_threads_migr;
    unsigned int   migr_queue_size;
    unsigned int   db_request_limit;
    unsigned int   max_migr_nbr;
    unsigned long long max_migr_vol;

    time_t         check_copy_status_delay;
    time_t         migration_timeout;

    time_t         pre_maintenance_window;
    time_t         maint_min_migr_delay;

    /* min error percentage to suspend current migration (0=disable) */
    double         suspend_error_pct;
    /* min error count to suspend current migration (0=disable) */
    unsigned int   suspend_error_min;

    /* attr index of the sort order (e.g. last_mod, creation_time, ...) */
    unsigned int   lru_sort_attr;
    bool           sort;

    bool   check_copy_status_on_startup;
    bool   recheck_ignored_classes;

#if defined( _LUSTRE_HSM) || defined(_HSM_LITE)
    bool   backup_new_files;
#endif

} migration_config_t;

int            SetDefault_Migration_Config( void *module_config, char *msg_out );
int            Read_Migration_Config( config_file_t config,
                                      void *module_config, char *msg_out, int for_reload );
int            Reload_Migration_Config( void *module_config );
int            Write_Migration_ConfigTemplate( FILE * output );
int            Write_Migration_ConfigDefault( FILE * output );

/** theses options specify the mode of running */
typedef enum
{
    MIGR_DAEMON,    /**< run as a daemon */
    MIGR_ONCE,      /**< perform a migration pass and terminate (one-shot mode) */
    MIGR_OST,       /**< perform a migration pass for files on a given OST and terminate (one-shot mode) */
    MIGR_USER,      /**< perform a migration pass for files of a given user and terminate (one-shot mode) */
    MIGR_GROUP,     /**< perform a migration pass for files of a given group and terminate (one-shot) */
    MIGR_CLASS,     /**< perform a migration pass for files in the given fileclass and terminate (one-shot) */
} migr_mode_t;


typedef struct migr_opt_t
{
    migr_mode_t    mode;
    int            flags;
    union
    {
        unsigned int   ost_index;  /**< targetted OST for MIGR_OST mode */
        char          *name;       /**< targetted user/group for MIGR_USER and MIGR_GROUP modes */
    } optarg_u;
} migr_opt_t;


int            Start_Migration( migration_config_t * p_config, migr_opt_t options );
int            Stop_Migration( void );
int            Wait_Migration( void );
void           Dump_Migration_Stats( void );

int MigrateSingle( migration_config_t * p_config, const char * file, int flags );

#endif
