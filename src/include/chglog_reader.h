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
    /** List of MDTs (used for openning ChangeLogs) */
    mdt_def_t     *mdt_def;
    unsigned int   mdt_count;

    /* nbr of changelog records to be agregated for llapi_changelog_clear() */
    int   batch_ack_count;

    int   force_polling;
    int   polling_interval;

} chglog_reader_config_t;

/* type if a log record is a ctime or mtime ? */
#ifdef CL_SPLITTED_TIME
#define CL_TIME_NOACCESS(_t) (((_t)==CL_MTIME)||((_t)==CL_CTIME))
#else
#define CL_TIME_NOACCESS(_t) ((_t)==CL_TIME)
#endif


/** start ChangeLog Readers */
int            ChgLogRdr_Start( chglog_reader_config_t * collector_config, int flags );

/** terminate ChangeLog Readers */
int            ChgLogRdr_Terminate(  );

/** wait for ChangeLog Readers termination */
int            ChgLogRdr_Wait(  );

/** dump changelog processing stats */
int            ChgLogRdr_DumpStats(  );

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
