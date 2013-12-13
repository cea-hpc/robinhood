/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009, 2010 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#define MIGR_TAG "Migr"

#include "RobinhoodConfig.h"
#include "RobinhoodMisc.h"
#include "migration.h"
#include "migr_arch.h"
#include "queue.h"
#include "Memory.h"
#include "xplatform_print.h"
#include <errno.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/vfs.h>

#ifdef _HSM_LITE
#include "backend_mgr.h"
#include "backend_ext.h"
#endif

/* ------------ Types and global variables ------------ */

/* Running mode (default is daemon) */
static migr_opt_t module_args = {
    .mode = MIGR_DAEMON,
    .flags = 0
};

/** @TODO stats about each policy */

/* indicate if termination signal has been received */
static int     terminate = FALSE;

static time_t  last_migration_check = 0;
static pthread_t main_thread_id = (pthread_t)-1;
static lmgr_t  lmgr;
static dev_t   fsdev = 0;

typedef enum
{
    MS_OFF,                                      /* migration module is not started */
    MS_RUNNING,                                  /* migration is running */
    MS_SLEEPING,                                 /* migration is not running */
    MS_TERMINATED                                /* migration module exited */
} migr_state_t;

static migr_state_t migr_state = MS_OFF;

/**
 * Function for checking that filesystem hasn't been unmounted
 */
static int CheckFSDevice( void )
{
    struct stat    root_md;

    /* retrieve device of filesystem, to compare it to initial device id */

    if (stat( global_config.fs_path, &root_md ) == -1)
    {
        DisplayLog( LVL_CRIT, MIGR_TAG, "Stat on '%s' failed! Error %d: %s",
                    global_config.fs_path, errno, strerror( errno ) );
        return FALSE;
    }
    if (root_md.st_dev != fsdev)
    {
        /* manage dev id change after umount/mount */
        DisplayLog( LVL_MAJOR, MIGR_TAG, "WARNING: Filesystem device id changed (old=%"PRI_DT", new=%"PRI_DT"): "
                    "checking if it has been remounted", fsdev, root_md.st_dev );
        if (ResetFS())
        {
            DisplayLog( LVL_CRIT, MIGR_TAG, "Filesystem was unmounted!!! EXITING!" );
            Exit( 1 );
        }
        /* update current fsdev */
        fsdev = get_fsdev();
    }

    return TRUE;
}

/**
 * @return boolean to indicate if we are in a maintenance window.
 */
static int check_maintenance_mode(policy_modifier_t * p_mod)
{
    struct tm dt;
    time_t  next_maint, now;
    char    varstr[128];
    char    datestr[128];
    char    leftstr[128];

    if (migr_config.pre_maintenance_window == 0)
        return FALSE;

    /* check maintenance mod */
    if ( (ListMgr_GetVar( &lmgr, NEXT_MAINT_VAR, varstr ) != DB_SUCCESS)
         || EMPTY_STRING(varstr) )
        return FALSE;

    next_maint = str2int(varstr);
    if (next_maint <= 0) /* invalid value, or disabled */
        return FALSE;

    /* build maintenance date */
    strftime( datestr, 128, "%Y/%m/%d %T", localtime_r( &next_maint, &dt ) );

    now = time(NULL);
    if (next_maint < now)
    {
        DisplayLog( LVL_DEBUG, MIGR_TAG, "Maintenance time is in the past (%s): no policy modifier", datestr);
        return FALSE;
    }
    else if (now < next_maint - migr_config.pre_maintenance_window)
    {
        FormatDuration( leftstr, 128, next_maint - migr_config.pre_maintenance_window - now );

        DisplayLog( LVL_VERB, MIGR_TAG, "Maintenance time is set (%s): maintenance window will start in %s",
                    datestr, leftstr );
        return FALSE;
    }
    else /* this is the pre maintenance window! */
    {
        /* linear function to compute time modifier */
        p_mod->time_factor = ((double)(next_maint - now)) / (double) migr_config.pre_maintenance_window;
        p_mod->time_min = migr_config.maint_min_migr_delay;

        FormatDuration( leftstr, 128, next_maint - now );

        DisplayLog( LVL_MAJOR, MIGR_TAG, "Currently in maintenance mode (maintenance is in %s): time modifier = %.2f%%",
                    leftstr, 100.0 * p_mod->time_factor );
        return TRUE;
    }
}


static int start_migration_pass( void )
{
    int            rc;
    char           tmpstr[128];
    char           varstr[512];
    migr_param_t   param;
    policy_modifier_t pol_mod;
    unsigned int   nb_files = 0;
    unsigned long long vol = 0;
    const char * action_str;
    time_t start_time;

    /* check if filesystem is still mounted */
    if ( !CheckFSDevice(  ) )
        return ENXIO;

    param.flags = module_args.flags;

    if (check_maintenance_mode(&pol_mod))
        param.policy_mod = &pol_mod;
    else
        param.policy_mod = NULL;

    /* build migration param from module args */
    switch ( module_args.mode )
    {
    case MIGR_DAEMON:
    case MIGR_ONCE:
        param.type = MIGR_FS;
        strcpy(tmpstr,"migrate all matching entries");
        break;
    case MIGR_OST:
        param.type = MIGR_BY_OST;
        param.param_u.ost_index = module_args.optarg_u.ost_index;
        sprintf(tmpstr,"migrate OST #%u", param.param_u.ost_index);
        break;
    case MIGR_USER:
        param.type = MIGR_BY_USER;
        param.param_u.user_name = module_args.optarg_u.name;
        snprintf(tmpstr, 128, "migrate user '%s' files", param.param_u.user_name );
        break;
    case MIGR_GROUP:
        param.type = MIGR_BY_GROUP;
        param.param_u.group_name = module_args.optarg_u.name;
        snprintf(tmpstr, 128, "migrate group '%s' files", param.param_u.group_name );
        break;
    case MIGR_CLASS:
        param.type = MIGR_BY_CLASS;
        param.param_u.class_name = module_args.optarg_u.name;
        snprintf(tmpstr, 128, "migrate fileclass '%s'", param.param_u.class_name );
        break;
    default:
        DisplayLog( LVL_CRIT, MIGR_TAG, "ERROR: unknown migration mode %d", module_args.mode );
        return EINVAL;
    }

    migr_state = MS_RUNNING;

    sprintf( varstr, "%lu", ( unsigned long ) time( NULL ) );
    ListMgr_SetVar( &lmgr, LAST_MIGR_TIME, varstr );
    ListMgr_SetVar( &lmgr, LAST_MIGR_STATUS, "running" );
    ListMgr_SetVar( &lmgr, LAST_MIGR_INFO, tmpstr );

    start_time = time(NULL);
    rc = perform_migration( &lmgr, &param, &nb_files, &vol );

#ifdef _LUSTRE_HSM
    action_str = "started";
#elif defined(_HSM_LITE)
    if ( backend.async_archive )
        action_str = "started";
    else
        action_str = "done";
#endif

    if ( rc == 0 )
    {
        char buf[128];
        unsigned int spent = time(NULL) - start_time;
        if (spent == 0) spent = 1;

        FormatFileSize( tmpstr, 128, vol );
        FormatFileSize( buf, 128, vol/spent );

        DisplayLog( LVL_MAJOR, MIGR_TAG,
                    "Migration summary: %u file migrations %s (%.2f/sec); total volume: %s (%s/sec)",
                    nb_files, action_str, (float)nb_files/(float)spent, tmpstr, buf );

        snprintf( varstr, 512, "nbr migration %s: %u, total volume: %s",
                  action_str, nb_files, tmpstr );

        ListMgr_SetVar( &lmgr, LAST_MIGR_STATUS, "OK" );
        ListMgr_SetVar( &lmgr, LAST_MIGR_INFO, varstr );
    }
    else if ( rc == ENOENT )
    {
        DisplayLog( LVL_MAJOR, MIGR_TAG, "Could not start migration: no list is available." );

        ListMgr_SetVar( &lmgr, LAST_MIGR_STATUS, "No list available" );
        ListMgr_SetVar( &lmgr, LAST_MIGR_INFO, "" );
    }
    else                        /* error */
    {
        FormatFileSize( tmpstr, 128, vol );

        DisplayLog( LVL_MAJOR, MIGR_TAG,
                    "Migration terminated with error %d: %u file migrations %s (total volume: %s)",
                    rc, nb_files, action_str, tmpstr );

        snprintf( varstr, 512, "Error %d", rc );
        ListMgr_SetVar( &lmgr, LAST_MIGR_STATUS, varstr );
        snprintf( varstr, 512, "nbr migration %s: %u, total volume: %s",
                  action_str, nb_files, tmpstr );
        ListMgr_SetVar( &lmgr, LAST_MIGR_INFO, varstr );
    }

    return rc;

}


/**
 * Main loop for running migrations periodically
 */
static void   *migration_thr( void *thr_arg )
{
    int            rc;
    unsigned int nb_reset = 0;
    unsigned int nb_total = 0;

    rc = ListMgr_InitAccess( &lmgr );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, MIGR_TAG,
                    "Could not connect to database (error %d). Migration cannot be started.", rc );
        return NULL;
    }

    /* check previous migration status */
    if ( migr_config.check_copy_status_on_startup )
    {
        DisplayLog( LVL_EVENT, MIGR_TAG, "Checking status of outstanding migrations..." );

        rc = check_current_migrations( &lmgr, &nb_reset, &nb_total, 0 );

        if ( rc != 0 )
            DisplayLog( LVL_CRIT, MIGR_TAG, "Error checking outstanding migration status" );
        else
            DisplayLog( LVL_EVENT, MIGR_TAG, "%u migration finished / %u total", nb_reset, nb_total );
    }

    do
    {
        rc = start_migration_pass(  );

        /* don't update last_check if trigger check failed */
        if ( rc != 0 )
            DisplayLog( LVL_CRIT, MIGR_TAG,
                        "Migration pass terminated with error %d... Will retry later", rc );
        else
            last_migration_check = time( NULL );

        if ( !terminate && ( module_args.mode == MIGR_DAEMON ) )
        {
            migr_state = MS_SLEEPING;
            rh_intr_sleep( migr_config.runtime_interval, terminate );
            if (terminate)
                break;

            /* cancel old migrations */
            DisplayLog( LVL_EVENT, MIGR_TAG, "Checking migration timeouts..." );
            nb_reset = nb_total = 0;
            rc = check_current_migrations( &lmgr, &nb_reset, &nb_total, migr_config.check_copy_status_delay );
            if ( rc != 0 )
                DisplayLog( LVL_CRIT, MIGR_TAG, "Error checking outstanding migration status" );
            else
                DisplayLog( LVL_EVENT, MIGR_TAG, "%u migration canceled / %u total", nb_reset, nb_total );
        }

    }
    while ( !terminate && ( module_args.mode == MIGR_DAEMON ) );

    ListMgr_CloseAccess( &lmgr );
    migr_state = MS_TERMINATED;

    DisplayLog( LVL_FULL, MIGR_TAG, "Main migration thread terminating" );

    pthread_exit( NULL );
    return NULL;
}

/* ------------ Exported functions ------------ */

/**
 * Initialize module and start main thread
 */
int Start_Migration( migration_config_t * p_config, migr_opt_t options )
{
    int            rc;

    fsdev = get_fsdev();

    /* store configuration */
    migr_config = *p_config;
    module_args = options;

    if ( NO_POLICY( &policies.migr_policies ) &&
        !(module_args.flags & FLAG_IGNORE_POL) )
    {
        DisplayLog( LVL_CRIT, MIGR_TAG,
            "No migration policy defined in configuration file... Disabling migrations." );
        return ENOENT;
    }

    /* Display an info message if no default policy is specified */
    if (!has_default_policy(&policies.migr_policies))
        DisplayLog(LVL_EVENT, MIGR_TAG, "Notice: no 'default' migration policy is defined. Unmatched entries will be ignored.");

    /* initialize migration queue */
    rc = CreateQueue( &migr_queue, migr_config.migr_queue_size, MIGR_ST_COUNT - 1,
                      MIGR_FDBK_COUNT );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, MIGR_TAG, "Error %d initializing migration queue", rc );
        return rc;
    }

    /* start migration threads */
    rc = start_migration_threads( migr_config.nb_threads_migr );
    if ( rc )
        return rc;

    rc = pthread_create( &main_thread_id, NULL, migration_thr, NULL );

    if ( rc != 0 )
    {
        rc = errno;
        DisplayLog( LVL_CRIT, MIGR_TAG,
                    "Error %d starting main thread of Migration module: %s", rc,
                    strerror( rc ) );
        return rc;
    }
    else
        DisplayLog( LVL_FULL, MIGR_TAG, "Migration thread %#lx started", main_thread_id );

    return 0;
}

/**
 * Only migrate one file and exit
 */
int MigrateSingle( migration_config_t * p_config, const char * file, int flags )
{
    fsdev = get_fsdev();

    /* store configuration */
    migr_config = *p_config;

    return migrate_one_file( file, flags );
}

int Stop_Migration()
{
    terminate = TRUE;
    abort_migration();
    return 0;
}

static int volatile waiting = 0;

int Wait_Migration()
{
    void          *returned;
    int rc = 0;

    /* /!\ pb: 2 threads cannot join the same other thread.
     * In one shot mode, the main thread is already waiting
     * for migration to end. Thus, the signal manager thread
     * gets an error when trying to join it after abort.
     */
    if (!waiting )
    {
        /* no lock here, we consider the sigterm is not simultaneous with module start */
        if ( main_thread_id != ((pthread_t)-1) )
        {
            waiting = 1;
            rc = pthread_join( main_thread_id, &returned );
            if ( rc != 0 )
                DisplayLog( LVL_MAJOR, MIGR_TAG, "pthread_join() returned error %d", rc );
            else
                waiting = 0;
        }
    }
    else
    {
        /* the second thread that needs to join polls the 'waiting' variable */
        while (waiting)
            rh_sleep(1);
    }
    return rc;
}

void Dump_Migration_Stats(  )
{
    unsigned int   status_tab[MIGR_ST_COUNT];
    unsigned long long feedback_tab[MIGR_FDBK_COUNT];

    unsigned int   nb_waiting, nb_items;
    time_t         last_submitted, last_started, last_ack;

    char           tmp_buff[256];
    time_t         now = time( NULL );
    int            i;
    struct tm      paramtm;


    /* Stats about triggers */

    DisplayLog( LVL_MAJOR, "STATS", "=======    Migration stats     ======" );

    switch ( migr_state )
    {
    case MS_OFF:               /* migration module is not started */
        DisplayLog( LVL_MAJOR, "STATS", "Current status: module not started" );
        break;
    case MS_RUNNING:           /* migration is running */
        DisplayLog( LVL_MAJOR, "STATS", "Current status: migration running" );
        break;
    case MS_SLEEPING:          /* migration is not running */
        DisplayLog( LVL_MAJOR, "STATS", "Current status: sleeping" );
        break;
    case MS_TERMINATED:        /* migration module exited */
        DisplayLog( LVL_MAJOR, "STATS", "Current status: module terminated" );
        break;
    }

    if ( last_migration_check > 0 )
    {
        strftime( tmp_buff, 256, "%Y/%m/%d %T", localtime_r( &last_migration_check, &paramtm ) );
        DisplayLog( LVL_MAJOR, "STATS", "Last migration pass finished at: %s", tmp_buff );
    }
    else
    {
        DisplayLog( LVL_MAJOR, "STATS", "No migration finished" );
    }

    RetrieveQueueStats( &migr_queue, &nb_waiting, &nb_items, &last_submitted, &last_started,
                        &last_ack, status_tab, feedback_tab );

    DisplayLog( LVL_MAJOR, "STATS", "idle migration threads   = %u", nb_waiting );
    DisplayLog( LVL_MAJOR, "STATS", "operations pending       = %u", nb_items );
    DisplayLog( LVL_MAJOR, "STATS", "migration status:" );

    for ( i = 0; i < MIGR_ST_COUNT; i++ )
    {
        /* always display PURGE_OK count and display error only if they have occured */
        if ( ( status_tab[i] > 0 ) || ( i == MIGR_OK ) )
            DisplayLog( LVL_MAJOR, "STATS", "    %-30s = %u", migr_status_descr[i], status_tab[i] );
    }

    DisplayLog( LVL_MAJOR, "STATS", "total number of migration requests = %llu",
                feedback_tab[MIGR_FDBK_NBR] );
    DisplayLog( LVL_MAJOR, "STATS", "total volume of migration requests = %llu (%s)",
                feedback_tab[MIGR_FDBK_VOL], FormatFileSize( tmp_buff, 256,
                                                             feedback_tab[MIGR_FDBK_VOL] ) );

    if ( last_submitted )
        DisplayLog( LVL_DEBUG, "STATS", "last file queued %2d s ago",
                    ( int ) ( now - last_submitted ) );

    if ( last_started )
        DisplayLog( LVL_DEBUG, "STATS", "last file processed %2d s ago",
                    ( int ) ( now - last_started ) );

    if ( last_ack )
        DisplayLog( LVL_DEBUG, "STATS", "last file acknowledged %2d s ago",
                    ( int ) ( now - last_ack ) );

}
