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

#ifndef _MIGR_ARCH_H
#define _MIGR_ARCH_H

#include "queue.h"
#include "list_mgr.h"

/* defined in migr_arch.c */
extern migration_config_t migr_config;
extern entry_queue_t migr_queue;

/* Migration status */
typedef enum
{
    MIGR_OK = 0,                                 /* migration has been started correctly */

    MIGR_ENTRY_MOVED,           /* entry has been moved or deleted */
    MIGR_ENTRY_WHITELISTED,     /* entry is ignored for migration */
    MIGR_STATUS_CHGD,           /* HSM status flags has changed */
    MIGR_NO_POLICY,             /* entry matches no policy */
    MIGR_BAD_TYPE,              /* migration policy does not apply to this type of entry */
    MIGR_BUSY,                  /* entry is is use */
    MIGR_ALREADY,               /* entry migration is already running */

    MIGR_PARTIAL_MD,            /* entry metadata is incomplete */
    MIGR_STAT_FAILURE,          /* stat failure */
    MIGR_ERROR,                 /* migration call failed */

    MIGR_ABORT,                 /* migration aborted by signal */

    MIGR_ST_COUNT               /* last status index */
} migr_status_t;

/* Purge status description */
static const char __attribute__(( __unused__ )) *migr_status_descr[MIGR_ST_COUNT] = {
#ifdef _LUSTRE_HSM
    "migration successfully started",   /* in Lustre-HSM, migration is asynchronous */
#else
    "migration successful",     /* other cases */
#endif
    "moved or deleted since last update",
    "whitelisted/ignored",
    "status flags have changed",
    "no matching policy",
    "bad type for migration",
    "entry is in use/busy",
    "migration already running",

    "incomplete metadata",
    "stat failure",
    "migration error",

    "migration aborted"
};

/* feedback from migration queue */
typedef enum
{
    MIGR_FDBK_NBR = 0,
    MIGR_FDBK_VOL,
    MIGR_FDBK_VOL_NOK,

    MIGR_FDBK_COUNT
} migr_fdbk_t;

typedef enum
{
    MIGR_FS = 1,
    MIGR_BY_OST,
    MIGR_BY_USER,
    MIGR_BY_GROUP,
    MIGR_BY_CLASS,
} migr_type_t;

typedef struct migr_param__
{
    migr_type_t    type;
    int            flags;
    union
    {
        unsigned int   ost_index;
        const char    *user_name;
        const char    *group_name;
        const char    *class_name;
    } param_u;
    policy_modifier_t * policy_mod;
} migr_param_t;


int            perform_migration( lmgr_t * lmgr, migr_param_t * p_migr_param,
                                  unsigned int *p_nb_migr, unsigned long long *p_migr_vol );


int            start_migration_threads( unsigned int nb_threads );

int  check_current_migrations( lmgr_t * lmgr, unsigned int *p_nb_reset,
                               unsigned int * p_nb_total,
                               time_t timeout );

void abort_migration( void );

int            migrate_one_file( const char * file, int flags );

#endif
