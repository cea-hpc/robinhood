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
 *  \file rbh_logs.h
 *  \brief Logs management.
 */

#ifndef _ROBINHOOD_LOGS_H
#define _ROBINHOOD_LOGS_H

#include "config_parsing.h"
#include "rbh_const.h"
#include "rbh_cfg.h"
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>  /* for FILE */
#include <sys/param.h>  /* for RBH_PATH_MAX */

typedef enum {
    LVL_CRIT    =  0,
    LVL_MAJOR   =  2,
    LVL_EVENT   =  5,
    LVL_VERB    =  10,
    LVL_DEBUG   =  50,
    LVL_FULL    = 100
} log_level;

typedef struct log_config__ {
    log_level   debug_level;
    bool        force_debug_level; /**< do not take the configuration into
                                    *   account, use a value from cmd line */
    char        log_file[RBH_PATH_MAX];
    bool        force_log_file; /**< do not take the configuration into
                                 * account, use a path from cmd line */
    char        report_file[RBH_PATH_MAX];

    char        alert_mail[MAIL_ADDRESS_MAX];
    char        alert_file[RBH_PATH_MAX];

    char        changelogs_file[RBH_PATH_MAX];

    int         syslog_facility;
    int         syslog_priority;

    /* batching of alerts:
     * 0=unlimited, 1=no batching,
     * >1 maximum number of reported alerts per summary
     */
    int         batch_alert_max;

    time_t      stats_interval;

    /* display entry attributes for each entry in alert reports */
    bool        alert_show_attrs;
    bool        log_process;  /* display process name in the log line header */
    bool        log_host;     /* display hostname in the log line header */

} log_config_t;

/* Allow forcing log files etc... */
extern log_config_t log_config;

/** config parsing helpers */
extern mod_cfg_funcs_t log_cfg_hdlr;

/* Converts a debug level string to the associated
 * verbosity level.
 * Returns (log_level)-1 in case of an error.
 */
log_level str2debuglevel(char *str);

/**
 * Force debug level.
 * Won't be overridden by configuration.
 */
void force_debug_level(log_level level);

/**
 * Force log file.
 * Won't be overridden by configuration.
 */
void force_log_file(const char *file);

/**
 * Indicates if traces of the given level are to be displayed.
 */
int TestDisplayLevel(log_level level);

/* Open log and report files,
 * Returns -1 and sets error in case of an error.
 */
int InitializeLogs(const char *prog_name);

/* flush logs */
void FlushLogs(void);

/**
 * Adjust log levels of external components (such as libraries) we get
 * messages from.
 */
void rbh_adjust_log_level_external(void);

/**
 * Display a log message.
 * This should not be called directly but used via the DisplayLog macro below.
 */
void DisplayLogFn(log_level debug_level, const char *tag,
                  const char *format, ...)
    __attribute__ ((format(printf, 3, 4)));

/**
 * Display a log message (variable arguments version).
 * This should not be called directly but used via the vDisplayLog macro below.
 */
void vDisplayLogFn(log_level debug_level, const char *tag, const char *format,
                   va_list ap);

/**
 * Emit a log record if the message is of high enough importance.
 */
#define DisplayLog(dbg_level, tag, ...) \
    do { \
        if (log_config.debug_level >= (dbg_level)) \
            DisplayLogFn((dbg_level), (tag), __VA_ARGS__); \
    } while (0)

/**
 * va_list-version of DisplayLog.
 */
#define vDisplayLog(dbg_level, tag, format, args) \
    do { \
        if (log_config.debug_level >= (dbg_level)) \
            vDisplayLogFn((dbg_level), (tag), (format), args); \
    } while (0)

/* Abort due to a bug */
#define RBH_BUG(_msg)   do { DisplayLog(LVL_CRIT, \
                                 "BUG", "in %s::%s(), line %u: %s",   \
                                 __FILE__, __func__, __LINE__, _msg); \
                             FlushLogs(); \
                             abort();   \
                        } while (0)

/* Displays a line in the report file */
void DisplayReport(const char *format, ...)
    __attribute__ ((format(printf, 1, 2))); /* 1=format 2=params */

#ifdef HAVE_CHANGELOGS
void DisplayChangelogs(const char *format, ...)
    __attribute__ ((format(printf, 1, 2))); /* 1=format 2=params */
#endif

/* Displays a line in the alert file / send a mail */
void RaiseAlert(const char *title, const char *format, ...)
    __attribute__ ((format(printf, 2, 3))); /* 2=format 3=params */

void RaiseEntryAlert(const char *alert_name,    /* alert name (if set) */
                     const char *alert_string,  /* alert description */
                     const char *entry_path,    /* entry path */
                     const char *entry_info);   /* alert related attributes */

/* Start grouping several entry alerts in the same email */
void Alert_StartBatching(void);
void Alert_EndBatching(void);

/* Wait for next stat deadline */
void WaitStatsInterval(void);

#endif
