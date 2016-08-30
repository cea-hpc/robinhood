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
 *  Robinhood logs management.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "rbh_cfg_helpers.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "xplatform_print.h"

#include <stdio.h>
#include <pthread.h>
#include <sys/param.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/utsname.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <ctype.h>
#include <glib.h>

#define SYSLOG_NAMES    /* to get the array of syslog facilities */
#include <syslog.h>

/* test that log file exists every 5min (compliency with log rotation) */
#define TIME_TEST_FILE     300

/* flush log buffer every 30s */
#define TIME_FLUSH_LOG      30

/* maximum log line size */
#define MAX_LINE_LEN      2048
/* maximum mail content size */
#define MAX_MAIL_LEN      4096

static bool log_initialized = false;

log_config_t log_config = {
    .debug_level = LVL_EVENT,   /* used for non-initialized logging */
    .syslog_facility = LOG_LOCAL1,
    .syslog_priority = LOG_INFO
};

/* type for log descriptors */
typedef struct _log_stream_ {
    enum {
        RBH_LOG_DEFAULT,
        RBH_LOG_REGFILE,
        RBH_LOG_STDIO,
        RBH_LOG_SYSLOG
    } log_type;
    pthread_rwlock_t  f_lock;  /* to protect the fields below _and logname_ */
    FILE             *f_log;   /* for regfile and stdio */
    ino_t             f_ino;   /* for regfile */
} log_stream_t;

#define RBH_LOG_INITIALIZER { .log_type = RBH_LOG_DEFAULT, \
                          .f_lock   = PTHREAD_RWLOCK_INITIALIZER, \
                          .f_log    = NULL,        \
                          .f_ino    = -1, }

/* log descriptors for each purpose (log, reports, alerts) */

static log_stream_t log     = RBH_LOG_INITIALIZER;
static log_stream_t report  = RBH_LOG_INITIALIZER;
static log_stream_t alert   = RBH_LOG_INITIALIZER;
#ifdef HAVE_CHANGELOGS
static log_stream_t chglogs = RBH_LOG_INITIALIZER;
#endif

/* syslog info */
static bool syslog_opened = false;

/* Check if the log file has been rotated
 * after a given delay.
 */
static time_t last_time_test = 0;

/* time of last flush */
static time_t last_time_flush_log = 0;

/* mutex for alert list */
static pthread_mutex_t alert_mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct alert_type {
    char            title[MAIL_TITLE_MAX];
    char          **entries;
    char          **info;
    unsigned int    count;

    /* estimated size for mail (not perfectly accurate: add margins to be
     * safe) */
    unsigned int    estim_size;

    struct alert_type *next;
} alert_type_t;

alert_type_t *alert_list = NULL;
bool alert_batching = false;
unsigned int alert_count = 0;

/* log line headers */
static char prog_name[RBH_PATH_MAX];
static char machine_name[RBH_PATH_MAX];

/* assign an index to each thread (displayed as [pid/thread_nbr] in the log) */

#if !HAVE_PTHREAD_GETSEQUENCE_NP
/* threads keys */
static pthread_key_t thread_key;
static pthread_once_t once_key = PTHREAD_ONCE_INIT;
static unsigned int next_index = 1;

/* init check */
static inline void log_init_check(void)
{
    if (!log_initialized) {
        fprintf(stderr, "Log management is not initialized. Aborting.\n");
        exit(1);
    }
}

/* Thread context management */
static void init_keys(void)
{
    pthread_key_create(&thread_key, NULL);
}   /* init_keys */
#endif

/* returns thread index */
static unsigned int GetThreadIndex(void)
{

#if HAVE_PTHREAD_GETSEQUENCE_NP
    return pthread_getsequence_np(pthread_self());
#else
    unsigned int index;

    /* first, we init the keys if this is the first time */
    pthread_once(&once_key, init_keys);

    index = (unsigned long)pthread_getspecific(thread_key);

    if (index == 0) {
        index = next_index++;
        pthread_setspecific(thread_key, (void *)(unsigned long)index);
    }

    return index;

#endif
}

#if _LUSTRE && HAVE_LLAPI_LOG_CALLBACKS
static inline enum llapi_message_level rbh_msg_level_convert(int level)
{
    switch (level) {
    case LVL_CRIT:
        return LLAPI_MSG_ERROR;

    case LVL_MAJOR:
        return LLAPI_MSG_WARN;

    case LVL_EVENT:
        return LLAPI_MSG_NORMAL;

    case LVL_VERB:
        return LLAPI_MSG_INFO;

    case LVL_DEBUG:
    case LVL_FULL:
    default:
        return LLAPI_MSG_DEBUG;
    }
}
#endif

void rbh_adjust_log_level_external(void)
{
#if _LUSTRE && HAVE_LLAPI_LOG_CALLBACKS
    llapi_msg_set_level(rbh_msg_level_convert(log_config.debug_level));
#endif
}

/* initialize a single log descriptor */
static int init_log_descr(const char *logname, log_stream_t *p_log)
{
    struct stat filestat;

    p_log->f_ino = -1;

    if (!strcasecmp(logname, "stdout")) {
        p_log->log_type = RBH_LOG_STDIO;
        p_log->f_log = stdout;
    } else if (!strcasecmp(logname, "stderr")) {
        p_log->log_type = RBH_LOG_STDIO;
        p_log->f_log = stderr;
    } else if (!strcasecmp(logname, "syslog")) {
        p_log->log_type = RBH_LOG_SYSLOG;
        p_log->f_log = NULL;

        /* open syslog once */
        if (!syslog_opened) {
            openlog(prog_name, LOG_PID, log_config.syslog_facility);
            syslog_opened = true;
        }
    } else {    /* log to regular file */

        p_log->log_type = RBH_LOG_REGFILE;
        p_log->f_log = fopen(logname, "a");

        if (p_log->f_log == NULL) {
            fprintf(stderr,
                    "Error opening log file %s: %s. Logging to stderr instead.\n",
                    logname, strerror(errno));
            p_log->log_type = RBH_LOG_STDIO;
            p_log->f_log = stderr;
            return 0;   /* do not propagate error as there is a workaround */
        }

        if (fstat(fileno(p_log->f_log), &filestat) != -1)
            p_log->f_ino = filestat.st_ino;
    }
    return 0;
}

/* check syslog facility name.
 * keep p_level unchanged if not specified.
 */
static int check_syslog_facility(const char *descriptor, int *p_fac,
                                 int *p_level)
{
    char  descr_cp[256];
    char *curr;
    int   i;
    bool  match;

    rh_strncpy(descr_cp, descriptor, 256);
    curr = strchr(descr_cp, '.');
    if (curr != NULL) {
        curr[0] = '\0';
        curr++; /* location of syslog level */
    }

    match = false;
    for (i = 0; facilitynames[i].c_name != NULL; i++) {
        if (!strcasecmp(facilitynames[i].c_name, descr_cp)) {
            if (p_fac)
                *p_fac = facilitynames[i].c_val;
            match = true;
            break;
        }
    }
    if (!match)
        return ENOENT;

    if (curr != NULL) {
        /* now doing the same for priority */
        match = false;
        for (i = 0; prioritynames[i].c_name != NULL; i++) {
            if (!strcasecmp(prioritynames[i].c_name, curr)) {
                if (p_level)
                    *p_level = prioritynames[i].c_val;
                match = true;
                break;
            }
        }
        if (!match)
            return ENOENT;
    }

    return 0;
}

/* Open log files */

int InitializeLogs(const char *program_name)
{
    struct utsname uts;
    char          *tmp;
    int            rc;

    /* get node name */
    if (uname(&uts) == -1)
        strcpy(machine_name, "???");
    else
        rh_strncpy(machine_name, uts.nodename, RBH_PATH_MAX);

    /* if the name is the full machine name (node.subnet.domain.ext),
     * only kief the brief name */
    if ((tmp = strchr(machine_name, '.')) != NULL)
        *tmp = '\0';

    if (program_name == NULL)
        strcpy(prog_name, "???");
    else
        rh_strncpy(prog_name, program_name, RBH_PATH_MAX);

    /* open log files */
    rc = init_log_descr(log_config.log_file, &log);
    if (rc)
        return rc;

    rc = init_log_descr(log_config.report_file, &report);
    if (rc)
        return rc;

    if (!EMPTY_STRING(log_config.alert_file)) {
        rc = init_log_descr(log_config.alert_file, &alert);
        if (rc)
            return rc;
    }
#ifdef HAVE_CHANGELOGS
    if (!EMPTY_STRING(log_config.changelogs_file)) {
        rc = init_log_descr(log_config.changelogs_file, &chglogs);
        if (rc)
            return rc;
    }
#endif

    /* Update log level for external components we get logs from (LLAPI...) */
    rbh_adjust_log_level_external();

    last_time_test = time(NULL);
    log_initialized = true;

    return 0;

}   /* InitializeLogs */

int TestDisplayLevel(log_level level)
{
    return (log_config.debug_level >= level);
}

/* flush a single log descriptor */
static void flush_log_descr(log_stream_t *p_log)
{
    pthread_rwlock_rdlock(&p_log->f_lock);
    if ((p_log->log_type == RBH_LOG_STDIO)
        || (p_log->log_type == RBH_LOG_REGFILE)) {
        if (p_log->f_log != NULL)
            fflush(p_log->f_log);
    }
    pthread_rwlock_unlock(&p_log->f_lock);
}

/* Flush logs (for example, at the end of a purge pass or after dumping
 * stats) */
void FlushLogs(void)
{
    log_init_check();

    flush_log_descr(&log);
    flush_log_descr(&report);
    flush_log_descr(&alert);
#ifdef HAVE_CHANGELOGS
    flush_log_descr(&chglogs);
#endif
}

static void test_log_descr(const char *logname, log_stream_t *p_log)
{
    struct stat filestat;

    /* test log rotation only for regular files */
    if (p_log->log_type != RBH_LOG_REGFILE)
        return;

    /* If the lock is taken (another thread is doing the check)
     * just wait for it to be released and safely continue to
     * log after file may have been closed and switched
     */
    if (pthread_rwlock_trywrlock(&p_log->f_lock) != 0) {
        pthread_rwlock_rdlock(&p_log->f_lock);
        pthread_rwlock_unlock(&p_log->f_lock);
        return;
    }

    if (stat(logname, &filestat) == -1) {
        if (errno == ENOENT) {
            /* the file disappeared, or has been renamed: opening a new one */
            fclose(p_log->f_log);
            p_log->f_log = fopen(logname, "a");

            if (fstat(fileno(p_log->f_log), &filestat) != -1)
                p_log->f_ino = filestat.st_ino;
        }
    } else if (p_log->f_ino != filestat.st_ino) {
        /* the old log file was renamed, and a new one has been created:
         * opening it.
         */
        fclose(p_log->f_log);
        p_log->f_log = fopen(logname, "a");
        p_log->f_ino = filestat.st_ino;
    }

    pthread_rwlock_unlock(&p_log->f_lock);
}

/* check if log file have been renamed */

static void test_file_names(void)
{
    log_init_check();

    test_log_descr(log_config.log_file, &log);
    test_log_descr(log_config.report_file, &report);

    if (!EMPTY_STRING(log_config.alert_file))
        test_log_descr(log_config.alert_file, &alert);

#ifdef HAVE_CHANGELOGS
    if (!EMPTY_STRING(log_config.changelogs_file))
        test_log_descr(log_config.changelogs_file, &chglogs);
#endif
}

/* Convert log level to  string.
 * \return -1 on error.
 */
log_level str2debuglevel(char *str)
{
    if (!strcasecmp(str, "CRIT"))
        return LVL_CRIT;
    if (!strcasecmp(str, "MAJOR"))
        return LVL_MAJOR;
    if (!strcasecmp(str, "EVENT"))
        return LVL_EVENT;
    if (!strcasecmp(str, "VERB"))
        return LVL_VERB;
    if (!strcasecmp(str, "DEBUG"))
        return LVL_DEBUG;
    if (!strcasecmp(str, "FULL"))
        return LVL_FULL;
    return -1;
}

/** replace 'non-printable chars with '?' */
static void clean_str(char *str)
{
    char *c;

    for (c = str; *c != '\0'; c++) {
        if ((*c != '\n') && (*c != '\t') && (!isprint(*c)))
            *c = '?';
    }
}

static void display_line_log(log_stream_t *p_log, const char *tag,
                             const char *format, va_list arglist)
{
    char          line_log[MAX_LINE_LEN];
    int           written;
    time_t        now = time(NULL);
    unsigned int  th = GetThreadIndex();
    struct tm     date;
    int           would_print;

    if (log_initialized) {
        /* periodically check if log files have been renamed */
        if (now - last_time_test > TIME_TEST_FILE) {
            test_file_names();
            last_time_test = now;
        }
    }

    pthread_rwlock_rdlock(&p_log->f_lock);
    /* if logs are not initalized or the log is a NULL FILE*,
     * default logging to stderr */
    if ((!log_initialized) ||
        ((p_log->log_type != RBH_LOG_SYSLOG) && (p_log->f_log == NULL))) {
        localtime_r(&now, &date);
        written =
            snprintf(line_log, MAX_LINE_LEN,
                     "%.4d/%.2d/%.2d %.2d:%.2d:%.2d %s[%lu/%u] %s%s",
                     1900 + date.tm_year, date.tm_mon + 1, date.tm_mday,
                     date.tm_hour, date.tm_min, date.tm_sec,
                     log_config.log_process ? "robinhood" : "",
                     (unsigned long)getpid(), th, tag ? tag : "",
                     tag ? " | " : "");

        would_print =
            vsnprintf(line_log + written, MAX_LINE_LEN - written, format,
                      arglist);
        clean_str(line_log);

        if (would_print >= MAX_LINE_LEN - written)
            fprintf(stderr, "%s... <Line truncated. Original size=%u>\n",
                    line_log, would_print);
        else
            fprintf(stderr, "%s\n", line_log);
    } else if (p_log->log_type == RBH_LOG_SYSLOG) {
        /* add tag to syslog line */
        char new_format[MAX_LINE_LEN];
        if (tag)
            snprintf(new_format, MAX_LINE_LEN, "%s | %s", tag, format);
        else
            rh_strncpy(new_format, format, MAX_LINE_LEN);

        vsyslog(log_config.syslog_priority, new_format, arglist);
    } else {    /* log to a file */

        localtime_r(&now, &date);

        written =
            snprintf(line_log, MAX_LINE_LEN,
                     "%.4d/%.2d/%.2d %.2d:%.2d:%.2d %s%s%s[%lu/%u] %s%s",
                     1900 + date.tm_year, date.tm_mon + 1, date.tm_mday,
                     date.tm_hour, date.tm_min, date.tm_sec,
                     log_config.log_process ? prog_name : "",
                     log_config.log_host ? "@" : "",
                     log_config.log_host ? machine_name : "",
                     (unsigned long)getpid(), th,
                     tag ? tag : "", tag ? " | " : "");

        would_print =
            vsnprintf(line_log + written, MAX_LINE_LEN - written, format,
                      arglist);
        clean_str(line_log);

        if (p_log->f_log != NULL) {
            if (would_print >= MAX_LINE_LEN - written)
                fprintf(p_log->f_log,
                        "%s... <Line truncated. Original size=%u>\n", line_log,
                        would_print);
            else
                fprintf(p_log->f_log, "%s\n", line_log);
        }
    }
    pthread_rwlock_unlock(&p_log->f_lock);
}

static void display_line_log_(log_stream_t *p_log, const char *tag,
                              const char *format, ...)
{
    va_list args;
    va_start(args, format);
    display_line_log(p_log, tag, format, args);
    va_end(args);
}

/** Display a message in the log.
 *  If logs are not initialized, write to stderr.
 */

void DisplayLogFn(log_level debug_level, const char *tag, const char *format,
                  ...)
{
    va_list args;

    va_start(args, format);
    vDisplayLogFn(debug_level, tag, format, args);
    va_end(args);
}

void vDisplayLogFn(log_level debug_level, const char *tag, const char *format,
                   va_list ap)
{
    time_t now = time(NULL);

    if (log_config.debug_level >= debug_level) {
        display_line_log(&log, tag, format, ap);

        /* test if it's time to flush.
         * Also flush major errors, to display it immediately. */
        if ((now - last_time_flush_log) > TIME_FLUSH_LOG
            || debug_level >= LVL_MAJOR) {
            flush_log_descr(&log);
            last_time_flush_log = now;
        }
    }
}

/* Display a message in report file */

void DisplayReport(const char *format, ...)
{
    va_list args;

    va_start(args, format);
    display_line_log(&report, NULL, format, args);
    va_end(args);

    /* always flush reports, because we don't want to lose events */
    flush_log_descr(&report);

}   /* DisplayReport */

#ifdef HAVE_CHANGELOGS
void DisplayChangelogs(const char *format, ...)
{
    va_list args;

    /* Bail out if no file defined */
    if (EMPTY_STRING(log_config.changelogs_file))
        return;

    va_start(args, format);
    display_line_log(&chglogs, NULL, format, args);
    va_end(args);
}
#endif

void Alert_StartBatching()
{
    /* no batching */
    if (log_config.batch_alert_max == 1)
        return;

    P(alert_mutex);
    alert_batching = true;
    V(alert_mutex);
}

/* Flush batched alerts.
 * Must be called under the protection of alert_mutex
 * release mutex ASAP if release_mutex_asap is true,
 * else: don't release it.
 */
static void FlushAlerts(bool release_mutex_asap)
{
    alert_type_t   *pcurr;
    unsigned int    alert_types = 0;
    unsigned int    mail_size = 0;
    char            title[MAIL_TITLE_MAX];
    GString        *contents = NULL;
    time_t          now;
    struct tm       date;

    /* first list scan, to determine the number of alerts, etc... */
    for (pcurr = alert_list; pcurr != NULL; pcurr = pcurr->next) {
        alert_types++;
        mail_size += pcurr->estim_size;
    }

    if (alert_count == 0) {
        if (release_mutex_asap)
            V(alert_mutex);
        return;
    }

    now = time(NULL);
    localtime_r(&now, &date);

    snprintf(title, MAIL_TITLE_MAX,
             "robinhood alert summary (%s on %s): %u alerts",
             global_config.fs_path, machine_name, alert_count);

    contents = g_string_new("");
    g_string_printf(contents, "Date: %.4d/%.2d/%.2d %.2d:%.2d:%.2d\n"
                    "Program: %s (pid %lu)\n"
                    "Host: %s\n"
                    "Filesystem: %s\n",
                    1900 + date.tm_year, date.tm_mon + 1, date.tm_mday,
                    date.tm_hour, date.tm_min, date.tm_sec, prog_name,
                    (unsigned long)getpid(), machine_name,
                    global_config.fs_path);

    g_string_append(contents, "\n===== alert summary ====\n\n");
    g_string_append_printf(contents, "%u alerts:\n", alert_count);

    for (pcurr = alert_list; pcurr != NULL; pcurr = pcurr->next) {
        g_string_append_printf(contents, "\t* %u %s\n", pcurr->count,
                               pcurr->title);
    }

    for (pcurr = alert_list; pcurr != NULL;) {
        unsigned int i;

        g_string_append_printf(contents, "\n==== alert '%s' ====\n\n",
                               pcurr->title);

        for (i = 0; i < pcurr->count; i++) {
            /* print and free */
            if (pcurr->entries[i]) {
                g_string_append_printf(contents, "%s\n", pcurr->entries[i]);
                if (log_config.alert_show_attrs)
                    g_string_append_printf(contents, "Entry info:\n%s\n",
                                           pcurr->info[i]);

                free(pcurr->entries[i]);
                free(pcurr->info[i]);
            }
        }
        /* free the list of entries */
        free(pcurr->entries);
        free(pcurr->info);

        /* set the list to the next item */
        alert_list = pcurr->next;
        /* free the item */
        free(pcurr);
        /* next item */
        pcurr = alert_list;
    }

    /* reset alert count */
    alert_count = 0;

    /* all alerts has been released, we can put the lock */
    if (release_mutex_asap)
        V(alert_mutex);

    /* send the mail and/or write the alert in alert file */
    if (!EMPTY_STRING(log_config.alert_mail))
        SendMail(log_config.alert_mail, title, contents->str);

    if (!EMPTY_STRING(log_config.alert_file)) {
        if (alert.log_type == RBH_LOG_SYSLOG) {
            /* we need to split the content after each '\n' */
            char *curr = contents->str;
            char *next = NULL;
            display_line_log_(&alert, "ALERT", "=== ALERT REPORT ===");
            do {
                next = strchr(curr, '\n');
                if (next != NULL) {
                    next[0] = '\0';
                    next++;
                }
                display_line_log_(&alert, "ALERT", curr);
                curr = next;
            } while (curr != NULL);
            display_line_log_(&alert, "ALERT", "=== END OF ALERT REPORT ===");
        } else {
            display_line_log_(&alert, "ALERT", "=== ALERT REPORT ===\n%s",
                              contents->str);
            display_line_log_(&alert, "ALERT", "=== END OF ALERT REPORT ===");
        }

        /* always flush alerts, because we don't want to lose events */
        flush_log_descr(&alert);
    }

    g_string_free(contents, TRUE);
    /* mutex already released, can go out now */

}   /*  Flush alerts */

static void Alert_Add(const char *title, const char *entry, const char *info)
{
    alert_type_t  *pcurr;
    bool           found = false;
    unsigned int   entrylen = strlen(entry);
    unsigned int   infolen = strlen(info);

    /* look for an alert with the same title */
    P(alert_mutex);
    for (pcurr = alert_list; pcurr != NULL; pcurr = pcurr->next) {
        if (!strcmp(pcurr->title, title)) {
            /* OK, found */
            found = true;
            break;
        }
    }

    /* if not found: add new alert type */
    if (!found) {
        pcurr = (alert_type_t *) malloc(sizeof(alert_type_t));
        if (!pcurr)
            goto out_unlock;

        strcpy(pcurr->title, title);
        pcurr->estim_size = strlen(title);
        pcurr->count = 0;
        pcurr->entries = NULL;
        pcurr->info = NULL;
        pcurr->next = alert_list;
        alert_list = pcurr;
    }

    /* pcurr now points to the appropriate alert type */
    pcurr->count++;

    /* total alert count */
    alert_count++;

    /* realloc manual (3): if ptr is NULL, the call is equivalent to
     * malloc(size) */
    pcurr->entries =
        (char **)realloc(pcurr->entries, pcurr->count * (sizeof(char *)));
    if (!pcurr->entries) {
        pcurr->count = 0;
        goto out_unlock;
    }
    pcurr->entries[pcurr->count - 1] = (char *)malloc(entrylen + 2);
    strcpy(pcurr->entries[pcurr->count - 1], entry);
    pcurr->estim_size += entrylen;

    pcurr->info =
        (char **)realloc(pcurr->info, pcurr->count * (sizeof(char *)));
    if (!pcurr->info) {
        pcurr->count = 0;
        goto out_unlock;
    }
    pcurr->info[pcurr->count - 1] = (char *)malloc(infolen + 2);
    strcpy(pcurr->info[pcurr->count - 1], info);
    pcurr->estim_size += infolen;

    if ((log_config.batch_alert_max > 1) &&
        (alert_count >= log_config.batch_alert_max)) {
        /* this also unlocks the mutex as soon as it is possible */
        FlushAlerts(true);
        return;
    }

 out_unlock:
    V(alert_mutex);
}

void Alert_EndBatching()
{
    if (alert_batching) {
        P(alert_mutex);
        alert_batching = false;
        /* release the mutex too */
        FlushAlerts(true);
    }
}

void RaiseEntryAlert(const char *alert_name,    /* alert name (if set) */
                     const char *alert_string,  /* alert description */
                     const char *entry_path,    /* entry path */
                     const char *entry_info)
{   /* alert related attributes */
    char title[1024];

    /* lockless check (not a big problem if some alerts are sent without
     * being batched).
     */
    if (alert_batching) {
        if (alert_name && !EMPTY_STRING(alert_name))
            strcpy(title, alert_name);
        else {
            if (snprintf(title, 1024, "unnamed alert %s", alert_string) > 80) {
                /* truncate at 80 char: */
                strcpy(title + 77, "...");
            }
        }

        Alert_Add(title, entry_path, entry_info);
    } else {
        if (alert_name && !EMPTY_STRING(alert_name))
            snprintf(title, 1024, "Robinhood alert (%s on %s): %s",
                     global_config.fs_path, machine_name, alert_name);
        else
            snprintf(title, 1024,
                     "Robinhood alert (%s on %s): entry matches alert rule",
                     global_config.fs_path, machine_name);

        if (log_config.alert_show_attrs)
            RaiseAlert(title, "Entry: %s\nAlert condition: %s\n"
                       "Entry info:\n%s", entry_path, alert_string, entry_info);
        else
            RaiseAlert(title, "Entry: %s\nAlert condition: %s\n",
                       entry_path, alert_string);
    }
}

/* Display a message in alert file */

void RaiseAlert(const char *title, const char *format, ...)
{
    va_list     args;
    char        mail[MAX_MAIL_LEN];
    char        title2[1024];
    int         written;
    time_t      now = time(NULL);
    struct tm   date;

    log_init_check();

    /* send alert mail, if an address was specified in config file */
    if (!EMPTY_STRING(log_config.alert_mail)) {
        localtime_r(&now, &date);
        written = snprintf(mail, MAX_MAIL_LEN,
                           "===== %s =====\n"
                           "Date: %.4d/%.2d/%.2d %.2d:%.2d:%.2d\n"
                           "Program: %s (pid %lu)\n"
                           "Host: %s\n"
                           "Filesystem: %s\n",
                           title, 1900 + date.tm_year, date.tm_mon + 1,
                           date.tm_mday, date.tm_hour, date.tm_min, date.tm_sec,
                           prog_name, (unsigned long)getpid(), machine_name,
                           global_config.fs_path);

        va_start(args, format);
        vsnprintf(mail + written, MAX_MAIL_LEN - written, format, args);
        va_end(args);

        snprintf(title2, 1024, "%s (%s on %s)", title, global_config.fs_path,
                 machine_name);
        SendMail(log_config.alert_mail, title2, mail);
    }

    if (!EMPTY_STRING(log_config.alert_file)) {
        display_line_log_(&alert, "ALERT", "%s", title);
        va_start(args, format);
        display_line_log(&alert, "ALERT", format, args);
        va_end(args);

        /* always flush alerts, because we don't want to lose events */
        flush_log_descr(&alert);
    }

}   /* DisplayAlert */

/* Wait for next stat deadline */
void WaitStatsInterval(void)
{
    rh_sleep(log_config.stats_interval > 0 ? log_config.stats_interval : 1);
}

/* ---------------- Config management routines -------------------- */

#define RBH_LOG_CONFIG_BLOCK "Log"

static void log_cfg_set_default(void *module_config)
{
    log_config_t *conf = (log_config_t *) module_config;

    conf->debug_level = LVL_EVENT;
    rh_strncpy(conf->log_file, "/var/log/robinhood.log", RBH_PATH_MAX);
    rh_strncpy(conf->report_file, "/var/log/robinhood_actions.log",
               RBH_PATH_MAX);

    rh_strncpy(conf->alert_file, "/var/log/robinhood_alerts.log", 1024);
    conf->alert_mail[0] = '\0';

#ifdef HAVE_CHANGELOGS
    conf->changelogs_file[0] = '\0';
#endif

    conf->syslog_facility = LOG_LOCAL1;
    conf->syslog_priority = LOG_INFO;

    conf->batch_alert_max = 1;  /* no batching */
    conf->alert_show_attrs = false;

    conf->stats_interval = 900; /* 15min */

    conf->log_process = 0;
    conf->log_host = 0;
}

static void log_cfg_write_default(FILE *output)
{
    print_begin_block(output, 0, RBH_LOG_CONFIG_BLOCK, NULL);
    print_line(output, 1, "debug_level    :   EVENT");
    print_line(output, 1, "log_file       :   \"/var/log/robinhood.log\"");
    print_line(output, 1,
               "report_file    :   \"/var/log/robinhood_actions.log\"");
    print_line(output, 1,
               "alert_file     :   \"/var/log/robinhood_alerts.log\"");
    print_line(output, 1, "syslog_facility:   local1.info");
    print_line(output, 1, "stats_interval :   15min");
    print_line(output, 1, "batch_alert_max:   1 (no batching)");
    print_line(output, 1, "alert_show_attrs: no");
    print_line(output, 1, "log_procname: no");
    print_line(output, 1, "log_hostname: no");
    print_end_block(output, 0);
}

static void log_cfg_write_template(FILE *output)
{
    print_begin_block(output, 0, RBH_LOG_CONFIG_BLOCK, NULL);

    print_line(output, 1, "# Log verbosity level");
    print_line(output, 1,
               "# Possible values are: CRIT, MAJOR, EVENT, VERB, DEBUG, FULL");
    print_line(output, 1, "debug_level = EVENT ;");
    fprintf(output, "\n");
    print_line(output, 1, "# Log file");
    print_line(output, 1, "log_file = \"/var/log/robinhood.log\" ;");
    fprintf(output, "\n");
    print_line(output, 1, "# File for reporting purge events");
    print_line(output, 1, "report_file = \"/var/log/robinhood_actions.log\" ;");
    fprintf(output, "\n");
    print_line(output, 1,
               "# set alert_file, alert_mail or both depending on the alert method you wish");
    print_line(output, 1, "alert_file = \"/var/log/robinhood_alerts.log\" ;");
    print_line(output, 1, "alert_mail = \"root@localhost\" ;");
    fprintf(output, "\n");
#ifdef HAVE_CHANGELOGS
    print_line(output, 1, "# File to dump changelogs into");
    print_line(output, 1, "changelogs_file = \"/var/log/robinhood_cl.log\" ;");
#endif
    fprintf(output, "\n");
    print_line(output, 1, "# Interval for dumping stats (to logfile)");
    print_line(output, 1, "stats_interval = 20min ;");
    fprintf(output, "\n");
    print_line(output, 1,
               "# Alert batching (to send a digest instead of 1 alert per file)");
    print_line(output, 1,
               "# 0: unlimited batch size, 1: no batching (1 alert per file),");
    print_line(output, 1, "# N>1: batch N alerts per digest");
    print_line(output, 1, "batch_alert_max = 5000 ;");
    print_line(output, 1,
               "# Give the detail of entry attributes for each alert?");
    print_line(output, 1, "alert_show_attrs = no ;");
    fprintf(output, "\n");
    print_line(output, 1, "# whether the process name appears in the log line");
    print_line(output, 1, "log_procname = yes;");
    print_line(output, 1, "# whether the host name appears in the log line");
    print_line(output, 1, "log_hostname = yes;");
    print_end_block(output, 0);
}

static int log_cfg_read(config_file_t config, void *module_config,
                        char *msg_out)
{
    int rc, tmpval;
    char tmpstr[1024];
    log_config_t *conf = (log_config_t *) module_config;
    config_item_t log_block;

    /* all allowed parameters names */
    static const char * const allowed_params[] = {
        "debug_level", "log_file", "report_file",
        "alert_file", "alert_mail", "stats_interval", "batch_alert_max",
        "alert_show_attrs", "syslog_facility", "log_procname", "log_hostname",
#ifdef HAVE_CHANGELOGS
        "changelogs_file",
#endif
        NULL
    };

    /* std parameters */
    const cfg_param_t cfg_params[] = {
        {"log_file", PT_STRING,
         PFLG_ABSOLUTE_PATH | PFLG_NO_WILDCARDS | PFLG_STDIO_ALLOWED,
         conf->log_file, sizeof(conf->log_file)}
        ,
        {"report_file", PT_STRING,
         PFLG_ABSOLUTE_PATH | PFLG_NO_WILDCARDS | PFLG_STDIO_ALLOWED,
         conf->report_file, sizeof(conf->report_file)}
        ,
        {"alert_file", PT_STRING,
         PFLG_ABSOLUTE_PATH | PFLG_NO_WILDCARDS | PFLG_STDIO_ALLOWED,
         conf->alert_file, sizeof(conf->alert_file)}
        ,
        {"alert_mail", PT_STRING, PFLG_MAIL,
         conf->alert_mail, sizeof(conf->alert_mail)}
        ,
#ifdef HAVE_CHANGELOGS
        {"changelogs_file", PT_STRING,
         PFLG_ABSOLUTE_PATH | PFLG_NO_WILDCARDS | PFLG_STDIO_ALLOWED,
         conf->changelogs_file, sizeof(conf->changelogs_file)}
        ,
#endif
        /* TODO add cfg flag: clean if not found */
        {"stats_interval", PT_DURATION, PFLG_POSITIVE | PFLG_NOT_NULL,
         &conf->stats_interval, 0}
        ,
        {"batch_alert_max", PT_INT, PFLG_POSITIVE, &conf->batch_alert_max, 0}
        ,
        {"alert_show_attrs", PT_BOOL, 0, &conf->alert_show_attrs, 0}
        ,
        {"log_procname", PT_BOOL, 0, &conf->log_process, 0}
        ,
        {"log_hostname", PT_BOOL, 0, &conf->log_host, 0}
        ,

        {NULL, 0, 0, NULL, 0}
    };

    /* get Log block */
    rc = get_cfg_block(config, RBH_LOG_CONFIG_BLOCK, &log_block, msg_out);
    if (rc)
        return rc == ENOENT ? 0 : rc;   /* not mandatory */

    /* read std parameters */
    rc = read_scalar_params(log_block, RBH_LOG_CONFIG_BLOCK, cfg_params,
                            msg_out);
    if (rc)
        return rc;

    /* read specific parameters */
    rc = GetStringParam(log_block, RBH_LOG_CONFIG_BLOCK, "debug_level",
                        PFLG_NO_WILDCARDS, tmpstr, 1024, NULL, NULL, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    else if (rc != ENOENT) {
        tmpval = str2debuglevel(tmpstr);

        if (tmpval < 0) {
            sprintf(msg_out,
                    "Invalid value for " RBH_LOG_CONFIG_BLOCK
                    "::debug_level: '%s'. CRIT, MAJOR, EVENT, VERB, DEBUG or FULL expected",
                    tmpstr);
            return EINVAL;
        } else
            conf->debug_level = tmpval;
    }

    rc = GetStringParam(log_block, RBH_LOG_CONFIG_BLOCK, "syslog_facility",
                        PFLG_NO_WILDCARDS, tmpstr, 1024, NULL, NULL, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    else if (rc == 0) {
        rc = check_syslog_facility(tmpstr, &conf->syslog_facility,
                                   &conf->syslog_priority);
        if (rc) {
            sprintf(msg_out,
                    "Invalid syslog channel '%s': expected syntax: <facility>[.<priority>]",
                    tmpstr);
            return rc;
        }
    }

    CheckUnknownParameters(log_block, RBH_LOG_CONFIG_BLOCK, allowed_params);

    return 0;
}

static int log_cfg_reload(log_config_t *conf)
{
    if (conf->debug_level != log_config.debug_level) {
        DisplayLog(LVL_MAJOR, "LogConfig",
                   RBH_LOG_CONFIG_BLOCK "::debug_level modified: '%d'->'%d'",
                   log_config.debug_level, conf->debug_level);
        log_config.debug_level = conf->debug_level;
    }

    /* log files can be changed dynamically: this will just be considered as if
     * it was renamed */
    if (strcmp(conf->log_file, log_config.log_file)) {
        DisplayLog(LVL_MAJOR, "LogConfig",
                   RBH_LOG_CONFIG_BLOCK "::log_file modified: '%s'->'%s'",
                   log_config.log_file, conf->log_file);

        /* lock file name to avoid reading inconsistent filenames */
        pthread_rwlock_wrlock(&log.f_lock);
        strcpy(log_config.log_file, conf->log_file);
        pthread_rwlock_wrlock(&log.f_lock);
    }

    if (strcmp(conf->report_file, log_config.report_file)) {
        DisplayLog(LVL_MAJOR, "LogConfig",
                   RBH_LOG_CONFIG_BLOCK "::report_file modified: '%s'->'%s'",
                   log_config.report_file, conf->report_file);

        /* lock file name to avoid reading inconsistent filenames */
        pthread_rwlock_wrlock(&report.f_lock);
        strcpy(log_config.report_file, conf->report_file);
        pthread_rwlock_unlock(&report.f_lock);
    }

    if (strcmp(conf->alert_file, log_config.alert_file)) {
        DisplayLog(LVL_MAJOR, "LogConfig",
                   RBH_LOG_CONFIG_BLOCK "::alert_file modified: '%s'->'%s'",
                   log_config.alert_file, conf->alert_file);

        /* lock file name to avoid reading inconsistent filenames */
        pthread_rwlock_wrlock(&alert.f_lock);
        strcpy(log_config.alert_file, conf->alert_file);
        pthread_rwlock_unlock(&alert.f_lock);
    }

    if (strcmp(conf->alert_mail, log_config.alert_mail))
        DisplayLog(LVL_MAJOR, "LogConfig",
                   RBH_LOG_CONFIG_BLOCK
                   "::alert_mail changed in config file, but cannot be modified dynamically");

#ifdef HAVE_CHANGELOGS
    if (strcmp(conf->changelogs_file, log_config.changelogs_file)) {
        DisplayLog(LVL_MAJOR, "LogConfig",
                   RBH_LOG_CONFIG_BLOCK
                   "::changelogs_file modified: '%s'->'%s'",
                   log_config.changelogs_file, conf->changelogs_file);

        /* lock file name to avoid reading inconsistent filenames */
        pthread_rwlock_wrlock(&chglogs.f_lock);
        strcpy(log_config.changelogs_file, conf->changelogs_file);
        pthread_rwlock_unlock(&chglogs.f_lock);
    }
#endif

    if (conf->stats_interval != log_config.stats_interval) {
        DisplayLog(LVL_MAJOR, "LogConfig",
                   RBH_LOG_CONFIG_BLOCK "::stats_interval modified: "
                   "'%" PRI_TT "'->'%" PRI_TT "'",
                   log_config.stats_interval, conf->stats_interval);
        log_config.stats_interval = conf->stats_interval;
    }

    if (conf->batch_alert_max != log_config.batch_alert_max) {
        DisplayLog(LVL_MAJOR, "LogConfig",
                   RBH_LOG_CONFIG_BLOCK
                   "::batch_alert_max modified: '%u'->'%u'",
                   log_config.batch_alert_max, conf->batch_alert_max);

        /* flush batched alerts first */
        P(alert_mutex);

        if (alert_batching)
            /* don't release mutex */
            FlushAlerts(false);

        log_config.batch_alert_max = conf->batch_alert_max;
        V(alert_mutex);
    }

    if (conf->log_process != log_config.log_process) {
        DisplayLog(LVL_MAJOR, "LogConfig",
                   RBH_LOG_CONFIG_BLOCK "::log_procname modified: '%s'->'%s'",
                   bool2str(log_config.log_process),
                   bool2str(conf->log_process));
        log_config.log_process = conf->log_process;
    }

    if (conf->log_host != log_config.log_host) {
        DisplayLog(LVL_MAJOR, "LogConfig",
                   RBH_LOG_CONFIG_BLOCK "::log_hostname modified: '%s'->'%s'",
                   bool2str(log_config.log_host), bool2str(conf->log_host));
        log_config.log_host = conf->log_host;
    }

    rbh_adjust_log_level_external();
    return 0;
}

static int log_cfg_set(void *cfg, bool reload)
{
    log_config_t *config = (log_config_t *) cfg;

    if (reload)
        return log_cfg_reload(config);

    log_config = *config;
    return 0;
}

static void *log_cfg_new(void)
{
    return calloc(1, sizeof(log_config_t));
}

static void log_cfg_free(void *cfg)
{
    if (cfg != NULL)
        free(cfg);
}

/* export config functions */
mod_cfg_funcs_t log_cfg_hdlr = {
    .module_name = "logs",
    .new = log_cfg_new,
    .free = log_cfg_free,
    .set_default = log_cfg_set_default,
    .read = log_cfg_read,
    .set_config = log_cfg_set,
    .write_default = log_cfg_write_default,
    .write_template = log_cfg_write_template
};
