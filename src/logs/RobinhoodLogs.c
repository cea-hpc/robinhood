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


#include "RobinhoodConfig.h"
#include "RobinhoodLogs.h"
#include "RobinhoodMisc.h"
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

#define SYSLOG_NAMES /* to get the array of syslog facilities */
#include <syslog.h>

/* test that log file exists every 5min (compliency with log rotation) */
#define TIME_TEST_FILE     300

/* flush log buffer every 30s */
#define TIME_FLUSH_LOG      30

/* maximum log line size */
#define MAX_LINE_LEN      2048
/* maximum mail content size */
#define MAX_MAIL_LEN      4096

#ifndef TRUE
#define TRUE (1)
#endif

#ifndef FALSE
#define FALSE (0)
#endif


static int     log_initialized = FALSE;

log_config_t log_config = {
    .debug_level = LVL_EVENT, /* used for non-initialized logging */
    .syslog_facility = LOG_LOCAL1,
    .syslog_priority = LOG_INFO
};

/* type for log descriptors */
typedef struct _log_stream_
{
    enum  {
            RBH_LOG_DEFAULT,
            RBH_LOG_REGFILE,
            RBH_LOG_STDIO,
            RBH_LOG_SYSLOG
    } log_type;
    pthread_rwlock_t     f_lock; /* to protect the fields below _and logname_ */
    FILE                *f_log;  /* for regfile and stdio */
    ino_t                f_ino;  /* for regfile */
} log_stream_t;

#define RBH_LOG_INITIALIZER { .log_type = RBH_LOG_DEFAULT, \
                          .f_lock   = PTHREAD_RWLOCK_INITIALIZER, \
                          .f_log    = NULL,        \
                          .f_ino    = -1, }

/* log descriptors for each purpose (log, reports, alerts) */

static log_stream_t log     = RBH_LOG_INITIALIZER;
static log_stream_t report  = RBH_LOG_INITIALIZER;
static log_stream_t alert   = RBH_LOG_INITIALIZER;

/* syslog info */
static int syslog_opened = FALSE;

/* Check if the log file has been rotated
 * after a given delay.
 */
static time_t  last_time_test = 0;

/* time of last flush */
static time_t  last_time_flush_log = 0;

/* mutex for alert list */
static pthread_mutex_t alert_mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct alert_type
{
   char         title[MAIL_TITLE_MAX];
   char         ** entries;
   char         ** info;
   unsigned int count;

   /* estimated size for mail (not perfectly accurate: add  margins to be safe) */
   unsigned int estim_size;

   struct alert_type * next;
} alert_type_t;

alert_type_t * alert_list = NULL;
int alert_batching = FALSE;
unsigned int alert_count = 0;


/* log line headers */
static char    prog_name[RBH_PATH_MAX];
static char    machine_name[RBH_PATH_MAX];


/* assign an index to each thread (displayed as [pid/thread_nbr] in the log) */

#if ! HAVE_PTHREAD_GETSEQUENCE_NP
/* threads keys */
static pthread_key_t thread_key;
static pthread_once_t once_key = PTHREAD_ONCE_INIT;
static unsigned int next_index = 1;

/* init check */
static inline void log_init_check( void )
{
    if ( !log_initialized )
    {
        fprintf( stderr, "Log management is not initialized. Aborting.\n" );
        exit( 1 );
    }
}


/* Thread context management */
static void init_keys( void )
{
    pthread_key_create( &thread_key, NULL );
}                               /* init_keys */
#endif


/* returns thread index */
static unsigned int GetThreadIndex( void )
{

#if HAVE_PTHREAD_GETSEQUENCE_NP
    return pthread_getsequence_np( pthread_self(  ) );
#else

    unsigned int   index;

    /* first, we init the keys if this is the first time */
    pthread_once( &once_key, init_keys );

    index = ( unsigned long ) pthread_getspecific( thread_key );

    if ( index == 0 )
    {
        index = next_index++;
        pthread_setspecific( thread_key, ( void * ) ( unsigned long ) index );
    }

    return index;

#endif
}

/* initialize a single log descriptor */
static int init_log_descr( const char * logname, log_stream_t * p_log )
{
    struct stat    filestat;

    p_log->f_ino  = -1;

    if ( !strcasecmp( logname, "stdout" ) )
    {
        p_log->log_type = RBH_LOG_STDIO;
        p_log->f_log  = stdout;
    }
    else if ( !strcasecmp( logname, "stderr" ) )
    {
        p_log->log_type = RBH_LOG_STDIO;
        p_log->f_log  = stderr;
    }
    else if ( !strcasecmp( logname, "syslog" ) )
    {
        p_log->log_type = RBH_LOG_SYSLOG;
        p_log->f_log  = NULL;

        /* open syslog once */
        if ( !syslog_opened )
        {
            openlog(prog_name, LOG_PID, log_config.syslog_facility );
            syslog_opened = TRUE;
        }
    }
    else /* log to regular file */
    {
        p_log->log_type = RBH_LOG_REGFILE;
        p_log->f_log = fopen( logname, "a" );

        if ( p_log->f_log == NULL ) {
            fprintf(stderr, "Error opening log file %s: %s. Logging to stderr instead.\n", logname, strerror(errno));
            p_log->log_type = RBH_LOG_STDIO;
            p_log->f_log  = stderr;
            return 0; /* do not propagate error as there is a workaround */
        }

        if ( fstat( fileno( p_log->f_log ), &filestat ) != -1 )
            p_log->f_ino = filestat.st_ino;
    }
    return 0;
}

/* check syslog facility name.
 * keep p_level unchanged if not specified.
 */
static int check_syslog_facility( const char * descriptor, int * p_fac, int *p_level )
{
    char descr_cp[256];
    char * curr;
    int i;
    int match;

    rh_strncpy(descr_cp, descriptor, 256);
    curr = strchr(descr_cp, '.');
    if ( curr != NULL )
    {
        curr[0]='\0';
        curr++; /* location of syslog level */
    }

    match = FALSE;
    for ( i = 0; facilitynames[i].c_name != NULL; i++ )
    {
        if ( !strcasecmp(facilitynames[i].c_name, descr_cp) )
        {
            if ( p_fac ) *p_fac = facilitynames[i].c_val;
            match = TRUE;
            break;
        }
    }
    if ( !match )
        return ENOENT;

    if ( curr != NULL )
    {
        /* now doing the same for priority */
        match = FALSE;
        for ( i = 0; prioritynames[i].c_name != NULL; i++ )
        {
            if ( !strcasecmp(prioritynames[i].c_name, curr) )
            {
                if ( p_level ) *p_level = prioritynames[i].c_val;
                match = TRUE;
                break;
            }
        }
        if ( !match )
            return ENOENT;
    }

    return 0;
}


/* Open log files */

int InitializeLogs( char *program_name, const log_config_t * config )
{
    struct utsname uts;
    char          *tmp;
    int            rc;

    /* store module configuration */
    log_config = *config;

    /* get node name */

    if ( uname( &uts ) == -1 )
        strcpy( machine_name, "???" );
    else
        rh_strncpy(machine_name, uts.nodename, RBH_PATH_MAX);

    /* if the name is the full machine name (node.subnet.domain.ext),
     * only kief the brief name */
    if ( ( tmp = strchr( machine_name, '.' ) ) != NULL )
        *tmp = '\0';

    if ( program_name == NULL )
        strcpy( prog_name, "???" );
    else
        rh_strncpy(prog_name, program_name, RBH_PATH_MAX);

    /* open log files */
    rc = init_log_descr( log_config.log_file, &log );
    if (rc) return rc;

    rc = init_log_descr( log_config.report_file, &report );
    if (rc) return rc;

    if ( !EMPTY_STRING( log_config.alert_file ) )
    {
        rc = init_log_descr( log_config.alert_file, &alert );
        if (rc) return rc;
    }

    last_time_test = time( NULL );


    log_initialized = TRUE;

    return 0;

}   /* InitializeLogs */

int            TestDisplayLevel( int level )
{
    return ( log_config.debug_level >= level );
}


/* flush a single log descriptor */
static void flush_log_descr( log_stream_t * p_log )
{
    pthread_rwlock_rdlock( &p_log->f_lock );
    if ( (p_log->log_type == RBH_LOG_STDIO) || (p_log->log_type == RBH_LOG_REGFILE) )
    {
        if ( p_log->f_log != NULL )
            fflush(p_log->f_log);
    }
    pthread_rwlock_unlock( &p_log->f_lock );
}


/* Flush logs (for example, at the end of a purge pass or after dumping stats) */
void FlushLogs( void )
{
    log_init_check(  );

    flush_log_descr( &log );
    flush_log_descr( &report );
    flush_log_descr( &alert );
}


static void test_log_descr( const char * logname, log_stream_t * p_log )
{
    struct stat    filestat;

    /* test log rotation only for regular files */
    if ( p_log-> log_type != RBH_LOG_REGFILE )
        return;

    /* If the lock is taken (another thread is doing the check)
     * just wait for it to be released and safely continue to
     * log after file may have been closed and switched
     */
    if ( pthread_rwlock_trywrlock( &p_log->f_lock ) != 0 ) {
        pthread_rwlock_rdlock( &p_log->f_lock );
        pthread_rwlock_unlock( &p_log->f_lock );
        return;
    }

    if ( stat( logname, &filestat ) == -1 )
    {
        if ( errno == ENOENT )
        {
            /* the file disapeared, or has been renamed: opening a new one */
            fclose( p_log->f_log );
            p_log->f_log = fopen( logname, "a" );

            if ( fstat( fileno( p_log->f_log ), &filestat ) != -1 )
                p_log->f_ino = filestat.st_ino;
        }
    }
    else if ( p_log->f_ino != filestat.st_ino )
    {
        /* the old log file was renamed, and a new one has been created:
         * opening it.
         */
        fclose( p_log->f_log );
        p_log->f_log = fopen( logname, "a" );
        p_log->f_ino = filestat.st_ino;
    }

    pthread_rwlock_unlock( &p_log->f_lock );
}


/* check if log file have been renamed */

static void test_file_names( void )
{
    log_init_check( );

    test_log_descr( log_config.log_file, &log );
    test_log_descr( log_config.report_file, &report );

    if ( !EMPTY_STRING( log_config.alert_file ) )
        test_log_descr( log_config.alert_file, &alert );
}




/* Convert log level to  string.
 * \return -1 on error.
 */
int str2debuglevel( char *str )
{
    if ( !strcasecmp( str, "CRIT" ) )
        return LVL_CRIT;
    if ( !strcasecmp( str, "MAJOR" ) )
        return LVL_MAJOR;
    if ( !strcasecmp( str, "EVENT" ) )
        return LVL_EVENT;
    if ( !strcasecmp( str, "VERB" ) )
        return LVL_VERB;
    if ( !strcasecmp( str, "DEBUG" ) )
        return LVL_DEBUG;
    if ( !strcasecmp( str, "FULL" ) )
        return LVL_FULL;
    return -1;
}

/** replace 'non-printable chars with '?' */
static void clean_str(char *str)
{
    char *c;

    for (c = str; *c != '\0'; c++)
    {
        if ((*c != '\n') && (!isprint(*c)))
            *c = '?';
    }
}


static void display_line_log( log_stream_t * p_log, const char * tag,
                       const char *format, va_list arglist )
{
    char           line_log[MAX_LINE_LEN];
    int            written;
    time_t         now = time( NULL );
    unsigned int   th = GetThreadIndex(  );
    struct tm      date;
    int            would_print;

    if ( log_initialized )
    {
        /* periodically check if log files have been renamed */
        if ( now - last_time_test > TIME_TEST_FILE )
        {
            test_file_names(  );
            last_time_test = now;
        }
    }

    pthread_rwlock_rdlock( &p_log->f_lock );
    /* if logs are not initalized or the log is a NULL FILE*,
     * default logging to stderr */
    if ((!log_initialized) ||
        ((p_log->log_type != RBH_LOG_SYSLOG) && (p_log->f_log == NULL)))
    {
        localtime_r( &now, &date );
        written =
            snprintf(line_log, MAX_LINE_LEN,
                     "%.4d/%.2d/%.2d %.2d:%.2d:%.2d %s[%lu/%u] %s%s",
                     1900 + date.tm_year, date.tm_mon + 1, date.tm_mday,
                     date.tm_hour, date.tm_min, date.tm_sec,
                     log_config.log_process?"robinhood":"",
                     (unsigned long)getpid(), th,
                     (tag && log_config.log_tag)?tag:"",
                     (tag && log_config.log_tag)?" | ":"");

        would_print = vsnprintf(line_log + written, MAX_LINE_LEN - written, format, arglist);
        clean_str(line_log);

        if (would_print >= MAX_LINE_LEN - written)
            fprintf(stderr, "%s... <Line truncated. Original size=%u>\n", line_log, would_print);
        else
            fprintf(stderr, "%s\n", line_log);
    }
    else if ( p_log->log_type == RBH_LOG_SYSLOG )
    {
        /* add tag to syslog line */
        char new_format[MAX_LINE_LEN];
        if (tag && log_config.log_tag)
            snprintf(new_format, MAX_LINE_LEN, "%s | %s", tag, format);
        else
            rh_strncpy(new_format, format, MAX_LINE_LEN);

        vsyslog(log_config.syslog_priority, new_format, arglist);
    }
    else /* log to a file */
    {
        localtime_r( &now, &date );

        written =
            snprintf(line_log, MAX_LINE_LEN,
                     "%.4d/%.2d/%.2d %.2d:%.2d:%.2d %s%s%s[%lu/%u] %s%s",
                     1900 + date.tm_year, date.tm_mon + 1, date.tm_mday,
                     date.tm_hour, date.tm_min, date.tm_sec,
                     log_config.log_process?prog_name:"",
                     log_config.log_host?"@":"",
                     log_config.log_host?machine_name:"",
                     (unsigned long)getpid(), th,
                     (tag && log_config.log_tag)?tag:"",
                     (tag && log_config.log_tag)?" | ":"");

        would_print = vsnprintf(line_log + written, MAX_LINE_LEN - written, format, arglist);
        clean_str(line_log);

        if ( p_log->f_log != NULL )
        {
        if (would_print >= MAX_LINE_LEN - written)
            fprintf(p_log->f_log, "%s... <Line truncated. Original size=%u>\n", line_log, would_print);
        else
            fprintf(p_log->f_log, "%s\n", line_log);
        }
    }
    pthread_rwlock_unlock( &p_log->f_lock );
}


static void display_line_log_( log_stream_t * p_log, const char * tag,
                       const char *format, ... )
{
        va_list args;
        va_start( args, format );
        display_line_log( p_log, tag, format, args );
        va_end( args );
}

/** Display a message in the log.
 *  If logs are not initialized, write to stderr.
 */

void DisplayLogFn( int debug_level, const char *tag, const char *format, ... )
{
    time_t         now = time( NULL );
    va_list        args;

    if ( log_config.debug_level >= debug_level )
    {
        va_start( args, format );
        display_line_log( &log, tag, format, args );
        va_end( args );

        /* test if it's time to flush. Also flush major errors, to display it immediately. */
        if ( (now - last_time_flush_log > TIME_FLUSH_LOG)
             || (debug_level >= LVL_MAJOR) )
        {
            flush_log_descr( &log );
            last_time_flush_log = now;
        }
    }
}                               /* DisplayLog */



/* Display a message in report file */

void DisplayReport( const char *format, ... )
{
    va_list        args;

    va_start( args, format );
    display_line_log( &report, NULL, format, args );
    va_end( args );

    /* always flush reports, because we don't want to loose events */
    flush_log_descr( &report );

} /* DisplayReport */


void Alert_StartBatching()
{
    /* no batching */
    if ( log_config.batch_alert_max == 1 )
        return;

    P( alert_mutex );
    alert_batching = TRUE;
    V( alert_mutex );
}

#define STR_APPEND_INCR( _pc, _str )  \
      do { strcpy((_pc), (_str)); (_pc)+=strlen(_str); } while(0)

/* Flush batched alerts.
 * Must be called under the protection of alert_mutex
 * release mutex ASAP if release_mutex_asap is true,
 * else: don't release it.
 */
static void FlushAlerts(int release_mutex_asap)
{
    alert_type_t * pcurr;
    unsigned int alert_types = 0;
    unsigned int mail_size = 0;
    char title[MAIL_TITLE_MAX];
    char * content;
    char * pchar;

    /* first list scan, to determine the number of alerts, etc... */
    for ( pcurr = alert_list; pcurr != NULL; pcurr = pcurr->next )
    {
        alert_types ++;
        mail_size += pcurr->estim_size;
    }

    if ( alert_count > 0 )
    {
        time_t         now = time( NULL );
        struct tm      date;
        localtime_r( &now, &date );

        snprintf(title, MAIL_TITLE_MAX, "robinhood alert summary (%s on %s): %u alerts",
                 global_config.fs_path, machine_name, alert_count);
        /* allocate and write the mail content */
        /* header: 1024 + summary: 1024*nb_types + estim_size*2 */
        content = (char*)malloc(1024 + 1024*alert_types + 2*mail_size);
        if ( !content )
            goto out;

        pchar = content;

        pchar += sprintf( pchar, "Date: %.4d/%.2d/%.2d %.2d:%.2d:%.2d\n"
                                 "Program: %s (pid %lu)\n"
                                 "Host: %s\n"
                                 "Filesystem: %s\n",
                                 1900 + date.tm_year, date.tm_mon + 1, date.tm_mday,
                                 date.tm_hour, date.tm_min, date.tm_sec, prog_name,
                                 ( unsigned long ) getpid(  ), machine_name,
                                 global_config.fs_path );

        STR_APPEND_INCR( pchar , "\n===== alert summary ====\n\n" );
        pchar += sprintf(pchar, "%u alerts:\n", alert_count );
        for ( pcurr = alert_list; pcurr != NULL; pcurr = pcurr->next )
        {
            pchar += sprintf(pchar, "\t* %u %s\n", pcurr->count, pcurr->title );
        }

        for ( pcurr = alert_list; pcurr != NULL; )
        {
            unsigned int i;
            pchar += sprintf(pchar, "\n==== alert '%s' ====\n\n",
                             pcurr->title );

            for ( i = 0; i < pcurr->count; i++ )
            {
                /* print and free */
                if ( pcurr->entries[i] )
                {
                    pchar += sprintf( pchar, "%s\n", pcurr->entries[i] );
                    if ( log_config.alert_show_attrs )
                    {
                        pchar += sprintf( pchar, "Entry info:\n%s\n",
                                          pcurr->info[i] );
                    }
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
            free( pcurr );
            /* next item */
            pcurr = alert_list;
        }

        /* reset alert count */
        alert_count = 0;

        /* all alerts has been released, we can put the lock */
        if ( release_mutex_asap )
            V( alert_mutex );

        /* send the mail and/or write the alert in alert file */
        if ( !EMPTY_STRING( log_config.alert_mail ) )
            SendMail( log_config.alert_mail, title, content );

        if ( !EMPTY_STRING( log_config.alert_file ) )
        {
            if ( alert.log_type == RBH_LOG_SYSLOG )
            {
                /* we need to split the content after each '\n' */
                char * curr = content;
                char * next = NULL;
                display_line_log_( &alert, "ALERT", "=== ALERT REPORT ===" );
                do {
                    next = strchr(curr, '\n');
                    if ( next != NULL )
                    {
                        next[0] = '\0';
                        next++;
                    }
                    display_line_log_( &alert, "ALERT", curr );
                    curr = next;
                } while (curr != NULL);
                display_line_log_( &alert, "ALERT", "=== END OF ALERT REPORT ===");
            }
            else
            {
                display_line_log_( &alert, "ALERT", "=== ALERT REPORT ===\n%s", content );
                display_line_log_( &alert, "ALERT", "=== END OF ALERT REPORT ===");
            }

            /* always flush alerts, because we don't want to loose events */
            flush_log_descr( &alert );
        }

        free(content);
        /* mutex already released, can go out now */
        return;
    }
out:
    if ( release_mutex_asap )
        V( alert_mutex );

} /*  Flush alerts */

static void Alert_Add( const char * title, const char * entry, const char * info )
{
    alert_type_t * pcurr;
    int            found = FALSE;
    unsigned int   entrylen = strlen(entry);
    unsigned int   infolen = strlen(info);

    /* look for an alert with the same title */
    P( alert_mutex );
    for ( pcurr = alert_list; pcurr != NULL; pcurr = pcurr->next )
    {
        if ( !strcmp( pcurr->title, title ) )
        {
            /* OK, found */
            found = TRUE;
            break;
        }
    }

    /* if not found: add new alert type */
    if ( !found )
    {
        pcurr = (alert_type_t*)malloc(sizeof(alert_type_t));
        if ( !pcurr )
            goto out_unlock;

        strcpy (pcurr->title, title);
        pcurr->estim_size = strlen(title);
        pcurr->count = 0;
        pcurr->entries = NULL;
        pcurr->info = NULL;
        pcurr->next = alert_list;
        alert_list = pcurr;
    }

    /* pcurr now points to the appropriate alert type */
    pcurr->count ++;

    /* total alert count */
    alert_count ++;

    /* realloc manual (3): if ptr is NULL, the call is equivalent to malloc(size) */
    pcurr->entries = (char**)realloc( pcurr->entries, pcurr->count * (sizeof(char*)) );
    if ( !pcurr->entries )
    {
        pcurr->count = 0;
        goto out_unlock;
    }
    pcurr->entries[pcurr->count-1] = (char*)malloc(entrylen+2); /* +2 safer than +1 :-) */
    strcpy(pcurr->entries[pcurr->count-1], entry );
    pcurr->estim_size += entrylen;

    pcurr->info = (char**)realloc( pcurr->info, pcurr->count * (sizeof(char*)) );
    if ( !pcurr->info )
    {
        pcurr->count = 0;
        goto out_unlock;
    }
    pcurr->info[pcurr->count-1] = (char*)malloc(infolen+2); /* +2 safer than +1 :-) */
    strcpy(pcurr->info[pcurr->count-1], info );
    pcurr->estim_size += infolen;

    if ((log_config.batch_alert_max > 1) &&
        (alert_count >= log_config.batch_alert_max))
    {
        /* this also unlocks the mutex as soon as it is possible */
        FlushAlerts(TRUE);
        return;
    }

out_unlock:
    V(alert_mutex);
}

void Alert_EndBatching()
{
    if ( alert_batching )
    {
        P( alert_mutex );
        alert_batching = FALSE;
        /* release the mutex too */
        FlushAlerts(TRUE);
    }
}


void RaiseEntryAlert( const char *alert_name, /* alert name (if set) */
                      const char *alert_string, /* alert description */
                      const char *entry_path,   /* entry path */
                      const char *entry_info)  /* alert related attributes */
{
   char title[1024];

   /* lockless check (not a big problem if some alerts are sent without
    * being batched).
    */
   if ( alert_batching )
   {
        if ( alert_name && !EMPTY_STRING(alert_name) )
            strcpy(title, alert_name );
        else
        {
            if ( snprintf(title, 1024, "unnamed alert %s", alert_string ) > 80 )
            {
                /* truncate at 80 char: */
                strcpy( title+77, "..." );
            }
        }

        Alert_Add( title, entry_path, entry_info );
   }
   else
   {
        if ( alert_name && !EMPTY_STRING(alert_name) )
            snprintf(title, 1024, "Robinhood alert (%s on %s): %s", global_config.fs_path,
                    machine_name, alert_name );
        else
            snprintf(title, 1024, "Robinhood alert (%s on %s): entry matches alert rule",
                    global_config.fs_path, machine_name );

        if ( log_config.alert_show_attrs )
            RaiseAlert( title, "Entry: %s\nAlert condition: %s\n"
                        "Entry info:\n%s", entry_path, alert_string,
                        entry_info );
        else
            RaiseAlert( title, "Entry: %s\nAlert condition: %s\n",
                        entry_path, alert_string );
    }
}


/* Display a message in alert file */

void RaiseAlert( const char *title, const char *format, ... )
{
    va_list        args;
    char           mail[MAX_MAIL_LEN];
    char           title2[1024];
    int            written;
    time_t         now = time( NULL );
    struct tm      date;

    log_init_check(  );

    /* send alert mail, if an address was specified in config file */
    if ( !EMPTY_STRING( log_config.alert_mail ) )
    {
        localtime_r( &now, &date );
        written = snprintf( mail, MAX_MAIL_LEN,
                          "===== %s =====\n"
                          "Date: %.4d/%.2d/%.2d %.2d:%.2d:%.2d\n"
                          "Program: %s (pid %lu)\n"
                          "Host: %s\n"
                          "Filesystem: %s\n",
                          title, 1900 + date.tm_year, date.tm_mon + 1, date.tm_mday,
                          date.tm_hour, date.tm_min, date.tm_sec, prog_name,
                          ( unsigned long ) getpid(  ), machine_name,
                          global_config.fs_path );

        va_start( args, format );
        vsnprintf( mail + written, MAX_MAIL_LEN - written, format, args );
        va_end( args );

        snprintf(title2, 1024, "%s (%s on %s)", title, global_config.fs_path,
                 machine_name);
        SendMail(log_config.alert_mail, title2, mail);
    }

    if ( !EMPTY_STRING( log_config.alert_file ) )
    {
        display_line_log_( &alert, "ALERT", "%s", title );
        va_start( args, format );
        display_line_log( &alert, "ALERT", format, args );
        va_end( args );

        /* always flush alerts, because we don't want to loose events */
        flush_log_descr( &alert );
    }

}                               /* DisplayAlert */

/* Wait for next stat deadline */
void WaitStatsInterval( void )
{
    rh_sleep( log_config.stats_interval );
}


/* ---------------- Config management routines -------------------- */

#define RBH_LOG_CONFIG_BLOCK "Log"

int SetDefaultLogConfig( void *module_config, char *msg_out )
{
    log_config_t  *conf = ( log_config_t * ) module_config;
    msg_out[0] = '\0';


    conf->debug_level = LVL_EVENT;
    rh_strncpy(conf->log_file, "/var/log/robinhood.log", RBH_PATH_MAX);
    rh_strncpy(conf->report_file, "/var/log/robinhood_reports.log", RBH_PATH_MAX);

    rh_strncpy(conf->alert_file, "/var/log/robinhood_alerts.log", 1024);
    conf->alert_mail[0] = '\0';

    conf->syslog_facility = LOG_LOCAL1;
    conf->syslog_priority = LOG_INFO;

    conf->batch_alert_max = 1; /* no batching */
    conf->alert_show_attrs = FALSE;

    conf->stats_interval = 900; /* 15min */

    conf->log_process = 0;
    conf->log_host = 0;
    conf->log_tag = 1;

    return 0;
}

int WriteLogConfigDefault(FILE * output)
{
    print_begin_block(output, 0, RBH_LOG_CONFIG_BLOCK, NULL);
    print_line(output, 1, "debug_level    :   EVENT");
    print_line(output, 1, "log_file       :   \"/var/log/robinhood.log\"");
    print_line(output, 1, "report_file    :   \"/var/log/robinhood_reports.log\"");
    print_line(output, 1, "alert_file     :   \"/var/log/robinhood_alerts.log\"");
    print_line(output, 1, "syslog_facility:   local1.info");
    print_line(output, 1, "stats_interval :   15min");
    print_line(output, 1, "batch_alert_max:   1 (no batching)");
    print_line(output, 1, "alert_show_attrs: FALSE");
    print_line(output, 1, "log_procname: FALSE");
    print_line(output, 1, "log_hostname: FALSE");
    print_line(output, 1, "log_module:   TRUE");
    print_end_block(output, 0);
    return 0;
}

int ReadLogConfig( config_file_t config, void *module_config, char *msg_out, int for_reload )
{
    int            rc, tmpval;
    char           tmpstr[1024];
    log_config_t  *conf = ( log_config_t * ) module_config;

    static const char *allowed_params[] = { "debug_level", "log_file", "report_file",
        "alert_file", "alert_mail", "stats_interval", "batch_alert_max",
        "alert_show_attrs", "syslog_facility", "log_procname", "log_hostname", "log_module",
        NULL
    };

    /* get Log block */

    config_item_t  log_block = rh_config_FindItemByName( config, RBH_LOG_CONFIG_BLOCK );

    if ( log_block == NULL )
    {
        strcpy( msg_out, "Missing configuration block '" RBH_LOG_CONFIG_BLOCK "'" );
        /* no parameter is mandatory => Not an error */
        return 0;
    }

    if ( rh_config_ItemType( log_block ) != CONFIG_ITEM_BLOCK )
    {
        sprintf( msg_out, "A block is expected for '" RBH_LOG_CONFIG_BLOCK "' item, line %d",
                 rh_config_GetItemLine( log_block ) );
        return EINVAL;
    }

    /* retrieve parameters */

    rc = GetStringParam( log_block, RBH_LOG_CONFIG_BLOCK, "debug_level",
                         STR_PARAM_NO_WILDCARDS, tmpstr, 1024, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc != ENOENT )
    {
        tmpval = str2debuglevel( tmpstr );

        if ( tmpval < 0 )
        {
            sprintf( msg_out,
                     "Invalid value for " RBH_LOG_CONFIG_BLOCK
                     "::debug_level: '%s'. CRIT, MAJOR, EVENT, VERB, DEBUG or FULL expected",
                     tmpstr );
            return EINVAL;
        }
        else
            conf->debug_level = tmpval;
    }

    rc = GetStringParam( log_block, RBH_LOG_CONFIG_BLOCK, "log_file",
                         STR_PARAM_ABSOLUTE_PATH | STR_PARAM_NO_WILDCARDS | STDIO_ALLOWED,
                         conf->log_file, RBH_PATH_MAX, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetStringParam( log_block, RBH_LOG_CONFIG_BLOCK, "report_file",
                         STR_PARAM_ABSOLUTE_PATH | STR_PARAM_NO_WILDCARDS | STDIO_ALLOWED,
                         conf->report_file, RBH_PATH_MAX, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetStringParam( log_block, RBH_LOG_CONFIG_BLOCK, "alert_file",
                         STR_PARAM_ABSOLUTE_PATH | STR_PARAM_NO_WILDCARDS | STDIO_ALLOWED,
                         conf->alert_file, 1024, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc == ENOENT )
        conf->alert_file[0] = '\0';

    rc = GetStringParam( log_block, RBH_LOG_CONFIG_BLOCK, "syslog_facility",
                         STR_PARAM_NO_WILDCARDS,
                         tmpstr, 1024, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc == 0 )
    {
        rc = check_syslog_facility( tmpstr, &conf->syslog_facility,
                                    &conf->syslog_priority );
        if (rc)
        {
            sprintf( msg_out, "Invalid syslog channel '%s': expected syntax: <facility>[.<priority>]",
                     tmpstr );
            return rc;
        }
    }

    rc = GetStringParam( log_block, RBH_LOG_CONFIG_BLOCK, "alert_mail",
                         STR_PARAM_MAIL, conf->alert_mail, 256, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc == ENOENT )
        conf->alert_mail[0] = '\0';

    rc = GetDurationParam( log_block, RBH_LOG_CONFIG_BLOCK, "stats_interval",
                           INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL, &tmpval, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc != ENOENT )
        conf->stats_interval = tmpval;

    rc = GetIntParam( log_block, RBH_LOG_CONFIG_BLOCK, "batch_alert_max",
                      INT_PARAM_POSITIVE, (int *)&conf->batch_alert_max,
                      NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetBoolParam(log_block, RBH_LOG_CONFIG_BLOCK, "alert_show_attrs",
                      0, &tmpval, NULL, NULL, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    else if (rc != ENOENT)
        conf->alert_show_attrs = tmpval;

    rc = GetBoolParam(log_block, RBH_LOG_CONFIG_BLOCK, "log_procname",
                      0, &tmpval, NULL, NULL, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    else if (rc != ENOENT)
        conf->log_process = tmpval;

    rc = GetBoolParam(log_block, RBH_LOG_CONFIG_BLOCK, "log_hostname",
                      0, &tmpval, NULL, NULL, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    else if (rc != ENOENT)
        conf->log_host = tmpval;

    rc = GetBoolParam(log_block, RBH_LOG_CONFIG_BLOCK, "log_module",
                      0, &tmpval, NULL, NULL, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    else if (rc != ENOENT)
        conf->log_tag = tmpval;

    CheckUnknownParameters( log_block, RBH_LOG_CONFIG_BLOCK, allowed_params );

    return 0;
}

int ReloadLogConfig( void *module_config )
{
    log_config_t  *conf = ( log_config_t * ) module_config;

    if ( conf->debug_level != log_config.debug_level )
    {
        DisplayLog( LVL_MAJOR, "LogConfig", RBH_LOG_CONFIG_BLOCK "::debug_level modified: '%d'->'%d'",
                    log_config.debug_level, conf->debug_level );
        log_config.debug_level = conf->debug_level;
    }

    /* log files can be changed dynamically : this will just be considered as if it was renamed */

    if ( strcmp( conf->log_file, log_config.log_file ) )
    {
        DisplayLog( LVL_MAJOR, "LogConfig", RBH_LOG_CONFIG_BLOCK "::log_file modified: '%s'->'%s'",
                    log_config.log_file, conf->log_file );

        /* lock file name to avoid reading inconsistent filenames */
        pthread_rwlock_wrlock( &log.f_lock );
        strcpy( log_config.log_file, conf->log_file );
        pthread_rwlock_wrlock( &log.f_lock );
    }

    if ( strcmp( conf->report_file, log_config.report_file ) )
    {
        DisplayLog( LVL_MAJOR, "LogConfig", RBH_LOG_CONFIG_BLOCK "::report_file modified: '%s'->'%s'",
                    log_config.report_file, conf->report_file );

        /* lock file name to avoid reading inconsistent filenames */
        pthread_rwlock_wrlock( &report.f_lock );
        strcpy( log_config.report_file, conf->report_file );
        pthread_rwlock_unlock( &report.f_lock );
    }

    if ( strcmp( conf->alert_file, log_config.alert_file ) )
    {
        DisplayLog( LVL_MAJOR, "LogConfig", RBH_LOG_CONFIG_BLOCK "::alert_file modified: '%s'->'%s'",
                    log_config.alert_file, conf->alert_file );

        /* lock file name to avoid reading inconsistent filenames */
        pthread_rwlock_wrlock( &alert.f_lock );
        strcpy( log_config.alert_file, conf->alert_file );
        pthread_rwlock_unlock( &alert.f_lock );
    }

    if ( strcmp( conf->alert_mail, log_config.alert_mail ) )
        DisplayLog( LVL_MAJOR, "LogConfig",
                    RBH_LOG_CONFIG_BLOCK
                    "::alert_mail changed in config file, but cannot be modified dynamically" );

    if ( conf->stats_interval != log_config.stats_interval )
    {
        DisplayLog( LVL_MAJOR, "LogConfig",
                    RBH_LOG_CONFIG_BLOCK "::stats_interval modified: "
                    "'%"PRI_TT"'->'%"PRI_TT"'",
                    log_config.stats_interval, conf->stats_interval );
        log_config.stats_interval = conf->stats_interval;
    }

    if ( conf->batch_alert_max != log_config.batch_alert_max )
    {
        DisplayLog( LVL_MAJOR, "LogConfig",
                    RBH_LOG_CONFIG_BLOCK "::batch_alert_max modified: '%u'->'%u'",
                    log_config.batch_alert_max, conf->batch_alert_max );

        /* flush batched alerts first */
        P( alert_mutex );

        if ( alert_batching )
            /* don't release mutex */
            FlushAlerts(FALSE);

        log_config.batch_alert_max = conf->batch_alert_max;
        V( alert_mutex );
    }

    if (conf->log_process != log_config.log_process)
    {
        DisplayLog(LVL_MAJOR, "LogConfig",
                    RBH_LOG_CONFIG_BLOCK "::log_procname modified: '%s'->'%s'",
                    bool2str(log_config.log_process),
                    bool2str(conf->log_process));
        log_config.log_process = conf->log_process;
    }

    if (conf->log_host != log_config.log_host)
    {
        DisplayLog(LVL_MAJOR, "LogConfig",
                    RBH_LOG_CONFIG_BLOCK "::log_hostname modified: '%s'->'%s'",
                    bool2str(log_config.log_host),
                    bool2str(conf->log_host));
        log_config.log_host = conf->log_host;
    }

    if (conf->log_tag != log_config.log_tag)
    {
        DisplayLog(LVL_MAJOR, "LogConfig",
                    RBH_LOG_CONFIG_BLOCK "::log_module modified: '%s'->'%s'",
                    bool2str(log_config.log_tag),
                    bool2str(conf->log_tag));
        log_config.log_tag = conf->log_tag;
    }

    return 0;

}

int WriteLogConfigTemplate(FILE * output)
{
    print_begin_block(output, 0, RBH_LOG_CONFIG_BLOCK, NULL);

    print_line(output, 1, "# Log verbosity level");
    print_line(output, 1, "# Possible values are: CRIT, MAJOR, EVENT, VERB, DEBUG, FULL");
    print_line(output, 1, "debug_level = EVENT ;");
    fprintf(output, "\n");
    print_line(output, 1, "# Log file");
    print_line(output, 1, "log_file = \"/var/log/robinhood.log\" ;");
    fprintf(output, "\n");
    print_line(output, 1, "# File for reporting purge events");
    print_line(output, 1, "report_file = \"/var/log/robinhood_reports.log\" ;");
    fprintf(output, "\n");
    print_line(output, 1, "# set alert_file, alert_mail or both depending on the alert method you wish");
    print_line(output, 1, "alert_file = \"/var/log/robinhood_alerts.log\" ;");
    print_line(output, 1, "alert_mail = \"root@localhost\" ;");
    fprintf(output, "\n");
    print_line(output, 1, "# Interval for dumping stats (to logfile)");
    print_line(output, 1, "stats_interval = 20min ;");
    fprintf(output, "\n");
    print_line(output, 1, "# Alert batching (to send a digest instead of 1 alert per file)");
    print_line(output, 1, "# 0: unlimited batch size, 1: no batching (1 alert per file),");
    print_line(output, 1, "# N>1: batch N alerts per digest");
    print_line(output, 1, "batch_alert_max = 5000 ;");
    print_line(output, 1, "# Give the detail of entry attributes for each alert?");
    print_line(output, 1, "alert_show_attrs = FALSE ;");
    fprintf(output, "\n");
    print_line(output, 1, "# whether the process name appears in the log line");
    print_line(output, 1, "log_procname = TRUE;");
    print_line(output, 1, "# whether the host name appears in the log line");
    print_line(output, 1, "log_hostname = TRUE;");
    print_line(output, 1, "# whether the module name appears in the log line");
    print_line(output, 1, "log_module = TRUE;");
    print_end_block(output, 0);
    return 0;
}
