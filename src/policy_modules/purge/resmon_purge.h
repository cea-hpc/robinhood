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

#ifndef _RESMON_PURGE_H
#define _RESMON_PURGE_H

#include "queue.h"
#include "list_mgr.h"

/* defined in resmon_purge.c */
extern resource_monitor_config_t resmon_config;
extern entry_queue_t purge_queue;

/* Purge status */
typedef enum
{
    PURGE_OK = 0,                                /* entry has been purged correctly */
    PURGE_ENTRY_ACCESSED,       /* entry has been accessed recently */
    PURGE_ENTRY_MOVED,          /* entry has been moved or deleted */
    PURGE_ENTRY_WHITELISTED,    /* entry is whitelisted  */

#ifdef ATTR_INDEX_status
    PURGE_STATUS_CHGD,          /* HSM status flags changed */
#endif

    PURGE_NO_POLICY,            /* entry matches no policy */
    PURGE_PARTIAL_MD,           /* entry metadata was incomplete */
    PURGE_STAT_FAILURE,         /* stat failure */
    PURGE_ERROR,                /* unlink failed */
    PURGE_ABORT,                /* purge aborted by termination signal */

    PURGE_ST_COUNT              /* last status index */
} purge_status_t;

/* Purge status description */
static const char __attribute__(( __unused__ )) *purge_status_descr[PURGE_ST_COUNT] = {
    "successfully purged",
    "accessed since last update",
    "moved or deleted since last update",
    "whitelisted/ignored",

#ifdef _LUSTRE_HSM
    "status flags have changed",
#endif

    "no matching policy",
    "incomplete metadata",
    "stat failure",
    "purge error",
    "purge aborted"
};


/* feedback from purge queue */
typedef enum
{
    PURGE_FDBK_BLOCKS = 0,
    PURGE_SPECIFIC_COUNT,

    PURGE_FDBK_COUNT
} purge_fdbk_t;


/* Purge type definitions */
typedef enum
{
    PURGE_FS = 1,
    PURGE_ALL,
    PURGE_BY_OST,
    PURGE_BY_POOL,
    PURGE_BY_USER,
    PURGE_BY_GROUP,
    PURGE_BY_CLASS
} purge_type_t;

typedef struct purge_param__
{
    purge_type_t   type;
    int            flags;
    union
    {
        unsigned int   ost_index;
        const char    *user_name;
        const char    *group_name;
        const char    *pool_name;
        const char    *class_name;
    } param_u;
    unsigned long  nb_blocks;
    unsigned long long nb_inodes;
} purge_param_t;


int  perform_purge( lmgr_t * lmgr, purge_param_t * p_purge_param,
                              unsigned long long *p_blks_purged,
                              unsigned long long *p_nb_specific );

#ifdef ATTR_INDEX_status
int  check_current_purges( lmgr_t * lmgr, unsigned int *p_nb_reset,
                               unsigned int * p_nb_total,
                               time_t timeout );
#endif

void abort_purge();


int  start_purge_threads( unsigned int nb_threads );

#endif
