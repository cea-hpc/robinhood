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
 *  \file RobinhoodLogs.h
 *  \brief Logs management.
 */

#ifndef _ROBINHOOD_LOGS_H
#define _ROBINHOOD_LOGS_H

#include "config_parsing.h"
#include <stdarg.h>
#include <stdio.h>              /* for FILE */
#include <sys/param.h>          /* for MAXPATHLEN */

#define LVL_CRIT          0
#define LVL_MAJOR         2
#define LVL_EVENT         5
#define LVL_VERB         10
#define LVL_DEBUG        50
#define LVL_FULL        100

typedef struct log_config__
{
    int            debug_level;
    char           log_file[MAXPATHLEN];
    char           report_file[MAXPATHLEN];

    char           alert_mail[256];
    char           alert_file[1024];

    time_t         stats_interval;

} log_config_t;

int            SetDefaultLogConfig( void *module_config, char *msg_out );
int            ReadLogConfig( config_file_t config, void *module_config,
                              char *msg_out, int for_reload );
int            ReloadLogConfig( void *module_config );
int            WriteLogConfigTemplate( FILE * output );
int            WriteLogConfigDefault( FILE * output );

/* Converts a debug level string to the associated
 * verbosity level.
 * Returns -1 in case of an error.
 */
int            str2debuglevel( char *str );

/* Open log and report files,
 * Returns -1 and sets error in case of an error.
 */
int            InitializeLogs( char *prog_name, const log_config_t * log_config );

/* flush logs */
void           FlushLogs(  );


/* Displays a log message */
void           DisplayLog_( int debug_level, const char *tag, const char *format, ... );

#ifdef _CHECK_LOCK_ARGS
#define DisplayLog( _l, _t, _fo, ... )	fprintf( stderr, _fo, ## __VA_ARGS__ )
#else
#define DisplayLog DisplayLog_
#endif

/* Displays a line in the report file */
void           DisplayReport( const char *format, ... );

/* Displays a line in the alert file / send a mail */
void           DisplayAlert( const char *title, const char *format, ... );

/* Wait for next stat deadline */
void           WaitStatsInterval(  );


#endif
