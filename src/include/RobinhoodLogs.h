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
#include "rbh_const.h"
#include <stdarg.h>
#include <stdio.h>              /* for FILE */
#include <sys/param.h>          /* for RBH_PATH_MAX */

#define LVL_CRIT          0
#define LVL_MAJOR         2
#define LVL_EVENT         5
#define LVL_VERB         10
#define LVL_DEBUG        50
#define LVL_FULL        100

typedef struct log_config__
{
    int            debug_level;
    char           log_file[RBH_PATH_MAX];
    char           report_file[RBH_PATH_MAX];

    char           alert_mail[256];
    char           alert_file[RBH_PATH_MAX];

    int            syslog_facility;
    int            syslog_priority;

    /* batching of alerts:
     * 0=unlimited, 1=no batching, >1 maximum number of reported alerts per summary
     */
    unsigned int            batch_alert_max;

    time_t         stats_interval;

    /* display entry attributes for each entry in alert reports */
    unsigned int alert_show_attrs:1;
    unsigned int log_process:1; /* display process name in the log line header */
    unsigned int log_host:1;    /* display hostname in the log line header */
    unsigned int log_tag:1;     /* display module name in the log line header */

} log_config_t;

extern log_config_t log_config;

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

/**
 * Indicates if traces of the given level are to be displayed.
 */
int            TestDisplayLevel( int level );

/* Open log and report files,
 * Returns -1 and sets error in case of an error.
 */
int            InitializeLogs( char *prog_name, const log_config_t * log_config );

/* flush logs */
void           FlushLogs(void);


/* Displays a log message */
void           DisplayLogFn( int debug_level, const char *tag, const char *format, ... )
                    __attribute__((format(printf, 3, 4))); /* 3=format 4=params */
#define DisplayLog(dbg_level,tag,format...) \
    do { if (log_config.debug_level >= (dbg_level))   \
            DisplayLogFn((dbg_level), (tag), format); \
    } while(0)

/* Abort due to a bug */
#define RBH_BUG(_msg)   do { DisplayLog(LVL_CRIT, "BUG", "in %s::%s(), line %u: %s", \
                                       __FILE__, __func__, __LINE__, _msg); \
                             FlushLogs(); \
                             abort();   \
                        } while(0)

/* Displays a line in the report file */
void           DisplayReport( const char *format, ... )
                    __attribute__((format(printf, 1, 2))); /* 1=format 2=params */

/* Displays a line in the alert file / send a mail */
void           RaiseAlert( const char *title, const char *format, ... )
                    __attribute__((format(printf, 2, 3))); /* 2=format 3=params */

void RaiseEntryAlert( const char *alert_name, /* alert name (if set) */
                      const char *alert_string, /* alert description */
                      const char *entry_path,   /* entry path */
                      const char *entry_info);  /* alert related attributes */

/* Start grouping several entry alerts in the same email */
void           Alert_StartBatching(void);
void           Alert_EndBatching(void);

/* Wait for next stat deadline */
void           WaitStatsInterval(void);


#endif
