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

static log_config_t log_config;

static FILE   *f_log = NULL;
static FILE   *f_rapport = NULL;
static FILE   *f_alert = NULL;

static ino_t   log_inode = 0;
static ino_t   rapport_inode = 0;
static ino_t   alert_inode = 0;

/* Check if the log file has been rotated
 * after a given delay.
 */
static time_t  last_time_test = 0;

/* time of last flush */
static time_t  last_time_flush_log = 0;

/* mutex for opening closing log file */
static pthread_mutex_t mutex_reopen = PTHREAD_MUTEX_INITIALIZER;


/* log line headers */
static char    prog_name[MAXPATHLEN];
static char    machine_name[MAXPATHLEN];


/* assign an index to each thread (displayed as [pid/thread_nbr] in the log) */

#if ! HAVE_PTHREAD_GETSEQUENCE_NP
/* threads keys */
static pthread_key_t thread_key;
static pthread_once_t once_key = PTHREAD_ONCE_INIT;
static unsigned int next_index = 1;


/* init check */
static inline void log_init_check(  )
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
static unsigned int GetThreadIndex(  )
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



/* Open log files */

int InitializeLogs( char *program_name, const log_config_t * config )
{
    struct stat    filestat;
    struct utsname uts;
    char          *tmp;

    /* store module configuration */
    log_config = *config;

    /* open log files */

    if ( !strcasecmp( log_config.log_file, "stdout" ) )
        f_log = stdout;
    else if ( !strcasecmp( log_config.log_file, "stderr" ) )
        f_log = stderr;
    else
    {
        f_log = fopen( log_config.log_file, "a" );

        if ( f_log == NULL )
            return -1;

        if ( fstat( fileno( f_log ), &filestat ) != -1 )
            log_inode = filestat.st_ino;
    }

    if ( !strcasecmp( log_config.report_file, "stdout" ) )
        f_rapport = stdout;
    else if ( !strcasecmp( log_config.report_file, "stderr" ) )
        f_rapport = stderr;
    else
    {
        f_rapport = fopen( log_config.report_file, "a" );

        if ( f_rapport == NULL )
        {
            fclose( f_log );
            return -1;
        }

        if ( fstat( fileno( f_rapport ), &filestat ) != -1 )
            rapport_inode = filestat.st_ino;
    }


    if ( !EMPTY_STRING( log_config.alert_file ) )
    {
        if ( !strcasecmp( log_config.alert_file, "stdout" ) )
            f_alert = stdout;
        else if ( !strcasecmp( log_config.alert_file, "stderr" ) )
            f_alert = stderr;
        else
        {

            f_alert = fopen( log_config.alert_file, "a" );
            if ( f_alert == NULL )
            {
                fclose( f_log );
                fclose( f_rapport );
                return -1;
            }

            if ( fstat( fileno( f_alert ), &filestat ) != -1 )
                alert_inode = filestat.st_ino;
        }
    }

    last_time_test = time( NULL );

    /* get node name */

    if ( uname( &uts ) == -1 )
        strcpy( machine_name, "???" );
    else
        strncpy( machine_name, uts.nodename, MAXPATHLEN );

    /* if the name is the full machine name (node.subnet.domain.ext),
     * only kief the brief name */
    if ( ( tmp = strchr( machine_name, '.' ) ) != NULL )
        *tmp = '\0';

    if ( program_name == NULL )
        strcpy( prog_name, "???" );
    else
        strncpy( prog_name, program_name, MAXPATHLEN );

    log_initialized = TRUE;

    return 0;

}                               /* InitializeLogs */


/* Flush logs (for example, at the end of a purge pass or after dumping stats) */
void FlushLogs(  )
{
    log_init_check(  );

    if ( f_log )
        fflush( f_log );

    if ( f_rapport )
        fflush( f_rapport );

    if ( f_alert )
        fflush( f_alert );
}



/* check if log file have been renamed */

static void test_file_names(  )
{
    struct stat    filestat;

    log_init_check(  );

    /* if the lock is taken, return immediately
     * (another thread is doing the check)
     */
    if ( pthread_mutex_trylock( &mutex_reopen ) != 0 )
        return;

    /* test log files (except for std outputs) */
    if ( ( f_log != stdout ) && ( f_log != stderr ) )
    {

        if ( stat( log_config.log_file, &filestat ) == -1 )
        {
            if ( errno == ENOENT )
            {
                /* the file disapeared, or has been renamed: opening a new one */
                fclose( f_log );
                f_log = fopen( log_config.log_file, "a" );

                if ( fstat( fileno( f_log ), &filestat ) != -1 )
                    log_inode = filestat.st_ino;
            }
        }
        else if ( log_inode != filestat.st_ino )
        {
            /* the old log file was renamed, and a new one has been created:
             * opening it.
             */
            fclose( f_log );
            f_log = fopen( log_config.log_file, "a" );
            log_inode = filestat.st_ino;
        }
    }


    /* test report file */
    if ( ( f_rapport != stdout ) && ( f_rapport != stderr ) )
    {

        if ( stat( log_config.report_file, &filestat ) == -1 )
        {
            if ( errno == ENOENT )
            {
                /* the file disapeared, or has been renamed: opening a new one */
                fclose( f_rapport );
                f_rapport = fopen( log_config.report_file, "a" );

                if ( fstat( fileno( f_rapport ), &filestat ) != -1 )
                    rapport_inode = filestat.st_ino;
            }
        }
        else if ( rapport_inode != filestat.st_ino )
        {
            /* the old report file was renamed, and a new one has been created:
             * opening it.
             */
            fclose( f_rapport );
            f_rapport = fopen( log_config.report_file, "a" );
            rapport_inode = filestat.st_ino;
        }
    }

    /* test alert file */
    if ( !EMPTY_STRING( log_config.alert_file ) )
    {

        if ( ( f_alert != stdout ) && ( f_alert != stderr ) )
        {
            if ( stat( log_config.alert_file, &filestat ) == -1 )
            {
                if ( errno == ENOENT )
                {
                    /* the file disapeared, or has been renamed: opening a new one */
                    fclose( f_alert );
                    f_alert = fopen( log_config.alert_file, "a" );

                    if ( fstat( fileno( f_alert ), &filestat ) != -1 )
                        alert_inode = filestat.st_ino;
                }
            }
            else if ( alert_inode != filestat.st_ino )
            {
                /* the old report file was renamed, and a new one has been created:
                 * opening it.
                 */
                fclose( f_alert );
                f_alert = fopen( log_config.alert_file, "a" );
                alert_inode = filestat.st_ino;
            }
        }
    }

    pthread_mutex_unlock( &mutex_reopen );

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


/** Display a message in the log.
 *  If logs are not initialized, write to stderr.
 */

void DisplayLog_( int debug_level, const char *tag, const char *format, ... )
{
    va_list        args;
    char           line_log[MAX_LINE_LEN];
    int            ecrit;
    time_t         now = time( NULL );
    unsigned int   th = GetThreadIndex(  );
    struct tm      date;


    if ( log_initialized )
    {
        log_init_check(  );

        /* periodically check in log files have been renamed */
        if ( now - last_time_test > TIME_TEST_FILE )
        {
            test_file_names(  );
            last_time_test = now;
        }


        if ( ( log_config.debug_level >= debug_level ) && ( f_log != NULL ) )
        {
            localtime_r( &now, &date );
            ecrit =
                snprintf( line_log, MAX_LINE_LEN,
                          "%.4d/%.2d/%.2d %.2d:%.2d:%.2d %s@%s[%lu/%u]: %s | ",
                          1900 + date.tm_year, date.tm_mon + 1, date.tm_mday,
                          date.tm_hour, date.tm_min, date.tm_sec,
                          prog_name, machine_name, ( unsigned long ) getpid(  ), th, tag );


            va_start( args, format );
            vsnprintf( line_log + ecrit, MAX_LINE_LEN - ecrit, format, args );
            va_end( args );

            fprintf( f_log, "%s\n", line_log );

        }


        /* test if its time to flush */
        if ( now - last_time_flush_log > TIME_FLUSH_LOG )
        {
            fflush( f_log );
            last_time_flush_log = now;
        }
    }
    else                        /* not initialized */
    {
        localtime_r( &now, &date );
        ecrit =
            snprintf( line_log, MAX_LINE_LEN,
                      "%.4d/%.2d/%.2d %.2d:%.2d:%.2d robinhood[%lu/%u]: %s | ",
                      1900 + date.tm_year, date.tm_mon + 1, date.tm_mday,
                      date.tm_hour, date.tm_min, date.tm_sec,
                      ( unsigned long ) getpid(  ), th, tag );


        va_start( args, format );
        vsnprintf( line_log + ecrit, MAX_LINE_LEN - ecrit, format, args );
        va_end( args );

        fprintf( stderr, "%s\n", line_log );
    }


}                               /* DisplayLog */




/* Display a message in report file */

void DisplayReport( const char *format, ... )
{
    va_list        args;
    char           line_log[MAX_LINE_LEN];
    int            ecrit;
    time_t         now = time( NULL );
    unsigned int   th = GetThreadIndex(  );
    struct tm      date;

    log_init_check(  );

    /* test if log file has been renamed */
    if ( now - last_time_test > TIME_TEST_FILE )
    {
        test_file_names(  );
        last_time_test = now;
    }

    localtime_r( &now, &date );
    ecrit =
        snprintf( line_log, MAX_LINE_LEN,
                  "%.4d/%.2d/%.2d %.2d:%.2d:%.2d %s@%s[%lu/%u]: ",
                  1900 + date.tm_year, date.tm_mon + 1, date.tm_mday,
                  date.tm_hour, date.tm_min, date.tm_sec, prog_name,
                  machine_name, ( unsigned long ) getpid(  ), th );


    va_start( args, format );
    vsnprintf( line_log + ecrit, MAX_LINE_LEN - ecrit, format, args );
    va_end( args );

    fprintf( f_rapport, "%s\n", line_log );

    /* always flush reports, because we don't want to loose events */
    fflush( f_rapport );


}                               /* DisplayReport */

/* Display a message in alert file */

void DisplayAlert( const char *title, const char *format, ... )
{
    va_list        args;
    char           line_log[MAX_LINE_LEN];
    char           mail[MAX_MAIL_LEN];
    int            ecrit;
    time_t         now = time( NULL );
    unsigned int   th = GetThreadIndex(  );
    struct tm      date;

    log_init_check(  );

    /* send alert mail, if an address was specified in config file */
    if ( !EMPTY_STRING( log_config.alert_mail ) )
    {
        localtime_r( &now, &date );
        ecrit = snprintf( mail, MAX_MAIL_LEN,
                          "===== Alert =====\n"
                          "Date: %.4d/%.2d/%.2d %.2d:%.2d:%.2d\n"
                          "Program: %s (pid %lu)\n"
                          "Host: %s\n"
                          "Message:\n",
                          1900 + date.tm_year, date.tm_mon + 1, date.tm_mday,
                          date.tm_hour, date.tm_min, date.tm_sec, prog_name,
                          ( unsigned long ) getpid(  ), machine_name );

        va_start( args, format );
        vsnprintf( mail + ecrit, MAX_MAIL_LEN - ecrit, format, args );
        va_end( args );
        SendMail( log_config.alert_mail, title, mail );
    }

    if ( !EMPTY_STRING( log_config.alert_file ) )
    {
        /* test if log file has been renamed */
        if ( now - last_time_test > TIME_TEST_FILE )
        {
            test_file_names(  );
            last_time_test = now;
        }

        localtime_r( &now, &date );
        ecrit =
            snprintf( line_log, MAX_LINE_LEN,
                      "%.4d/%.2d/%.2d %.2d:%.2d:%.2d %s@%s[%lu/%u]: ===== Alert ====\n%s\n",
                      1900 + date.tm_year, date.tm_mon + 1, date.tm_mday,
                      date.tm_hour, date.tm_min, date.tm_sec, prog_name,
                      machine_name, ( unsigned long ) getpid(  ), th, title );


        va_start( args, format );
        vsnprintf( line_log + ecrit, MAX_LINE_LEN - ecrit, format, args );
        va_end( args );

        fprintf( f_alert, "%s\n", line_log );

        /* always flush alerts, because we don't want to loose events */
        fflush( f_alert );
    }


}                               /* DisplayReport */

/* Wait for next stat deadline */
void WaitStatsInterval(  )
{
    rh_sleep( log_config.stats_interval );
}


/* ---------------- Config management routines -------------------- */

#define LOG_CONFIG_BLOCK "Log"

int SetDefaultLogConfig( void *module_config, char *msg_out )
{
    log_config_t  *conf = ( log_config_t * ) module_config;
    msg_out[0] = '\0';

    conf->debug_level = LVL_EVENT;
    strncpy( conf->log_file, "/var/log/robinhood.log", MAXPATHLEN );
    strncpy( conf->report_file, "/var/log/robinhood_reports.log", MAXPATHLEN );

    strncpy( conf->alert_file, "/var/log/robinhood_alerts.log", 1024 );
    conf->alert_mail[0] = '\0';

    conf->stats_interval = 900; /* 15min */

    return 0;
}

int WriteLogConfigDefault( FILE * output )
{
    print_begin_block( output, 0, LOG_CONFIG_BLOCK, NULL );
    print_line( output, 1, "debug_level    :   EVENT" );
    print_line( output, 1, "log_file       :   \"/var/log/robinhood.log\"" );
    print_line( output, 1, "report_file    :   \"/var/log/robinhood_reports.log\"" );
    print_line( output, 1, "alert_file     :   \"/var/log/robinhood_alerts.log\"" );
    print_line( output, 1, "stats_interval :   15min" );
    print_end_block( output, 0 );
    return 0;
}

int ReadLogConfig( config_file_t config, void *module_config, char *msg_out, int for_reload )
{
    int            rc, tmpval;
    char           tmpstr[1024];
    log_config_t  *conf = ( log_config_t * ) module_config;

    static const char *allowed_params[] = { "debug_level", "log_file", "report_file",
        "alert_file", "alert_mail", "stats_interval",
        NULL
    };

    /* get Log block */

    config_item_t  log_block = rh_config_FindItemByName( config, LOG_CONFIG_BLOCK );

    if ( log_block == NULL )
    {
        strcpy( msg_out, "Missing configuration block '" LOG_CONFIG_BLOCK "'" );
        /* no parameter is mandatory => Not an error */
        return 0;
    }

    if ( rh_config_ItemType( log_block ) != CONFIG_ITEM_BLOCK )
    {
        sprintf( msg_out, "A block is expected for '" LOG_CONFIG_BLOCK "' item, line %d",
                 rh_config_GetItemLine( log_block ) );
        return EINVAL;
    }

    /* retrieve parameters */

    rc = GetStringParam( log_block, LOG_CONFIG_BLOCK, "debug_level",
                         STR_PARAM_NO_WILDCARDS, tmpstr, 1024, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc != ENOENT )
    {
        tmpval = str2debuglevel( tmpstr );

        if ( tmpval < 0 )
        {
            sprintf( msg_out,
                     "Invalid value for " LOG_CONFIG_BLOCK
                     "::debug_level: '%s'. CRIT, MAJOR, EVENT, VERB, DEBUG or FULL expected",
                     tmpstr );
            return EINVAL;
        }
        else
            conf->debug_level = tmpval;
    }

    rc = GetStringParam( log_block, LOG_CONFIG_BLOCK, "log_file",
                         STR_PARAM_ABSOLUTE_PATH | STR_PARAM_NO_WILDCARDS | STDIO_ALLOWED,
                         conf->log_file, MAXPATHLEN, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetStringParam( log_block, LOG_CONFIG_BLOCK, "report_file",
                         STR_PARAM_ABSOLUTE_PATH | STR_PARAM_NO_WILDCARDS | STDIO_ALLOWED,
                         conf->report_file, MAXPATHLEN, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetStringParam( log_block, LOG_CONFIG_BLOCK, "alert_file",
                         STR_PARAM_ABSOLUTE_PATH | STR_PARAM_NO_WILDCARDS | STDIO_ALLOWED,
                         conf->alert_file, 1024, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc == ENOENT )
        conf->alert_file[0] = '\0';

    rc = GetStringParam( log_block, LOG_CONFIG_BLOCK, "alert_mail",
                         STR_PARAM_MAIL, conf->alert_mail, 256, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc == ENOENT )
        conf->alert_mail[0] = '\0';

    rc = GetDurationParam( log_block, LOG_CONFIG_BLOCK, "stats_interval",
                           INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL, &tmpval, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc != ENOENT )
        conf->stats_interval = tmpval;

    CheckUnknownParameters( log_block, LOG_CONFIG_BLOCK, allowed_params );

    return 0;
}

int ReloadLogConfig( void *module_config )
{
    log_config_t  *conf = ( log_config_t * ) module_config;

    if ( conf->debug_level != log_config.debug_level )
    {
        DisplayLog( LVL_MAJOR, "LogConfig", LOG_CONFIG_BLOCK "::debug_level modified: '%d'->'%d'",
                    log_config.debug_level, conf->debug_level );
        log_config.debug_level = conf->debug_level;
    }

    /* log files can be changed dynamically : this will just be considered as if it was renamed */

    if ( strcmp( conf->log_file, log_config.log_file ) )
    {
        DisplayLog( LVL_MAJOR, "LogConfig", LOG_CONFIG_BLOCK "::log_file modified: '%s'->'%s'",
                    log_config.log_file, conf->log_file );

        /* lock file name to avoid reading inconsistent filenames */
        pthread_mutex_lock( &mutex_reopen );
        strcpy( log_config.log_file, conf->log_file );
        pthread_mutex_unlock( &mutex_reopen );
    }

    if ( strcmp( conf->report_file, log_config.report_file ) )
    {
        DisplayLog( LVL_MAJOR, "LogConfig", LOG_CONFIG_BLOCK "::report_file modified: '%s'->'%s'",
                    log_config.report_file, conf->report_file );

        /* lock file name to avoid reading inconsistent filenames */
        pthread_mutex_lock( &mutex_reopen );
        strcpy( log_config.report_file, conf->report_file );
        pthread_mutex_unlock( &mutex_reopen );
    }

    if ( strcmp( conf->alert_file, log_config.alert_file ) )
    {
        DisplayLog( LVL_MAJOR, "LogConfig", LOG_CONFIG_BLOCK "::alert_file modified: '%s'->'%s'",
                    log_config.alert_file, conf->alert_file );

        /* lock file name to avoid reading inconsistent filenames */
        pthread_mutex_lock( &mutex_reopen );
        strcpy( log_config.alert_file, conf->alert_file );
        pthread_mutex_unlock( &mutex_reopen );
    }

    if ( strcmp( conf->alert_mail, log_config.alert_mail ) )
        DisplayLog_( LVL_MAJOR, "LogConfig",
                    LOG_CONFIG_BLOCK
                    "::alert_mail changed in config file, but cannot be modified dynamically" );

    if ( conf->stats_interval != log_config.stats_interval )
    {
        DisplayLog_( LVL_MAJOR, "LogConfig",
                    LOG_CONFIG_BLOCK "::stats_interval modified: '%u'->'%u'",
                    log_config.stats_interval, conf->stats_interval );
        log_config.stats_interval = conf->stats_interval;
    }

    return 0;

}

int WriteLogConfigTemplate( FILE * output )
{
    print_begin_block( output, 0, LOG_CONFIG_BLOCK, NULL );

    print_line( output, 1, "# Log verbosity level" );
    print_line( output, 1, "# Possible values are: CRIT, MAJOR, EVENT, VERB, DEBUG, FULL" );
    print_line( output, 1, "debug_level = EVENT ;" );
    fprintf( output, "\n" );
    print_line( output, 1, "# Log file" );
    print_line( output, 1, "log_file = \"/var/log/robinhood.log\" ;" );
    fprintf( output, "\n" );
    print_line( output, 1, "# File for reporting purge events" );
    print_line( output, 1, "report_file = \"/var/log/robinhood_reports.log\" ;" );
    fprintf( output, "\n" );
    print_line( output, 1,
                "# set alert_file, alert_mail or both depending on the alert method you wish" );
    print_line( output, 1, "alert_file = \"/var/log/robinhood_alerts.log\" ;" );
    print_line( output, 1, "alert_mail = \"root@localhost\" ;" );
    fprintf( output, "\n" );
    print_line( output, 1, "# Interval for dumping stats (to logfile)" );
    print_line( output, 1, "stats_interval = 20min ;" );


    print_end_block( output, 0 );
    return 0;
}
