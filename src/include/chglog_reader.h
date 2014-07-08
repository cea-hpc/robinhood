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
 * \file    chglog_reader.h
 * \author  Th. Leibovici
 * \brief   Interface for Lustre MDT Changelog processing.
 */

/**
 * \addtogroup CHANGE_LOGS
 * @{
 */
#ifndef _CHGLOG_READER_H
#define _CHGLOG_READER_H

#include "config_parsing.h"
#include "list_mgr.h"

#define MDT_NAME_MAX  32
#define READER_ID_MAX 16

typedef struct mdt_def_t
{
    char           mdt_name[MDT_NAME_MAX];
    char           reader_id[READER_ID_MAX];
} mdt_def_t;

/** Configuration for ChangeLog reader Module */
typedef struct chglog_reader_config_t
{
    /** List of MDTs (used for opening ChangeLogs) */
    mdt_def_t     *mdt_def;
    unsigned int   mdt_count;

    /* nbr of changelog records to be agregated for llapi_changelog_clear() */
    int   batch_ack_count;

    int   force_polling;
    int   polling_interval;

    /* Maximum number of operations to keep in the internal queue. */
    int queue_max_size;

    /* Age of the opration we keep in the internal queue before we
     * push them to thepipeline. */
    int queue_max_age;

    /* Interval at which we have to check whether operation in the
     * internal queue have aged. */
    int queue_check_interval;

    /* Options suported by the MDS. LU-543 and LU-1331 are related to
     * events in changelog, where a rename is overriding a destination
     * file. */
    int mds_has_lu543;
    int mds_has_lu1331;

    /* file to dump all changelog records */
    char dump_file[RBH_PATH_MAX];

} chglog_reader_config_t;

extern chglog_reader_config_t chglog_reader_config;

/** start ChangeLog Readers
 * \param mdt_index -1 for all
 */
int            ChgLogRdr_Start(chglog_reader_config_t * p_config,
                               int flags, int mdt_index);

/** terminate ChangeLog Readers */
int            ChgLogRdr_Terminate( void );

/** wait for ChangeLog Readers termination */
int            ChgLogRdr_Wait( void );

/** Release last changelog records, and dump the final stats. */
int            ChgLogRdr_Done( void );

/** dump changelog processing stats */
int            ChgLogRdr_DumpStats( void );

/** store changelog stats to db */
int            ChgLogRdr_StoreStats( lmgr_t * lmgr );

/**
 * \addtogroup MODULE_CONFIG_FUNCTIONS
 * @{
 */
int            ChgLogRdr_SetDefaultConfig( void *module_config, char *msg_out );
int            ChgLogRdr_ReadConfig( config_file_t config, void *module_config,
                                     char *msg_out, int for_reload );
int            ChgLogRdr_ReloadConfig( void *module_config );
int            ChgLogRdr_WriteConfigTemplate( FILE * output );
int            ChgLogRdr_WriteDefaultConfig( FILE * output );

#endif

/** @} */
/** @} */
