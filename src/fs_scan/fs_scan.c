/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2007, 2008, 2009, 2010 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
/**
 *  Filesystem scan module.
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "global_config.h"
#include "entry_processor.h"
#include "fs_scan.h"
#include "RobinhoodConfig.h"
#include "RW_Lock.h"
#include "RobinhoodMisc.h"
#include "list_mgr.h"

#include "task_stack_mngmt.h"
#include "task_tree_mngmt.h"
#include "xplatform_print.h"

#include <sys/types.h>
#include <string.h>
#include <dirent.h>
#include <sys/time.h>           /* for gettimeofday */
#include <sys/utsname.h>
#include <libgen.h>
#include <unistd.h>


fs_scan_config_t fs_scan_config;
int              fsscan_flags = 0;

#define fsscan_once ( fsscan_flags & FLAG_ONCE )

static int     is_lustre_fs = FALSE;


/* information about scanning thread */

typedef struct thread_scan_info__
{
    unsigned int   index;
    pthread_t      thread_scan;
    time_t         last_action;

    /* NULL if no task is running */
    robinhood_task_t *current_task;

    /* flag for forcing thread scan to stop */
    int force_stop;

    /* entries handled since scan started */
    unsigned int   entries_handled;
    unsigned int   entries_errors;

    /* time consumed for handling entries */
    struct timeval time_consumed;
    struct timeval last_processing_time;

} thread_scan_info_t;


/**
 * internal variables 
 */

static thread_scan_info_t *thread_list = NULL;
/* nb_threads, fs_path, fs_type: from configuration */

static dev_t   fsdev;                            /* for STAY_IN_FS mode */

/* stack of scan tasks */
static task_stack_t tasks_stack;

/* pointer to mother task (NULL if no scan is running) */
robinhood_task_t *root_task = NULL;

/* statistics */
static time_t  last_scan_time = 0;
static unsigned int last_duration = 0;
static int     last_scan_complete = FALSE;
static time_t  scan_start_time = 0;

static struct timeval accurate_start_time = { 0, 0 };

static unsigned int nb_hang_total = 0;

/* used for adaptive scan interval */
static double  usage_max = 50.0;                 /* default: 50% */
static time_t  scan_interval = 0;

/* for accessing persistent variables */
static lmgr_t  lmgr;
static int     lmgr_init = FALSE;

/* lock on scan stats and other information.
 * This lock must always be taken AFTER the list lock
 * at the end of a scan.
 */
static pthread_mutex_t lock_scan;

/* threads behavior */
static pthread_attr_t thread_attrs;

/* condition about DB special operations when starting/terminating FS scan */
static pthread_cond_t special_db_op_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t special_db_op_lock = PTHREAD_MUTEX_INITIALIZER;
static int     waiting_db_op = FALSE;

static inline void set_db_wait_flag(  )
{
    P( special_db_op_lock );
    waiting_db_op = TRUE;
    V( special_db_op_lock );
}

static void wait_for_db_callback(  )
{
    P( special_db_op_lock );
    while ( waiting_db_op )
        pthread_cond_wait( &special_db_op_cond, &special_db_op_lock );
    V( special_db_op_lock );
}

static int db_special_op_callback( lmgr_t *lmgr, struct entry_proc_op_t *p_op, void *arg )
{
    DisplayLog( LVL_VERB, FSSCAN_TAG, "Callback from database for operation '%s'", ( char * ) arg );

    P( special_db_op_lock );
    waiting_db_op = FALSE;
    pthread_cond_signal( &special_db_op_cond );
    V( special_db_op_lock );
    return 0;
}

/* condition about end of 'one-shot' FS_Scan */
static int     scan_finished = FALSE;
static pthread_cond_t one_shot_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t one_shot_lock = PTHREAD_MUTEX_INITIALIZER;

static inline void signal_scan_finished(  )
{
    P( one_shot_lock );
    scan_finished = TRUE;
    pthread_cond_broadcast( &one_shot_cond );
    V( one_shot_lock );
}

static inline int all_threads_idle()
{
    unsigned int i;
    for ( i = 0; i < fs_scan_config.nb_threads_scan ; i++ )
        if ( thread_list[i].current_task )
            return FALSE;

    return TRUE;
}

void wait_scan_finished(  )
{
    P( one_shot_lock );
    while ( !scan_finished )
        pthread_cond_wait( &one_shot_cond, &one_shot_lock );
    V( one_shot_lock );
}


/**
 * Reset Scan thread statistics (before and after a scan)
 */
static void ResetScanStats(  )
{
    int            i;
    for ( i = 0; i < fs_scan_config.nb_threads_scan; i++ )
    {
        thread_list[i].entries_handled = 0;
        thread_list[i].entries_errors = 0;
        timerclear( &thread_list[i].time_consumed );
        timerclear( &thread_list[i].last_processing_time );
    }
}


int ignore_entry( char *fullpath, char *name, unsigned int depth, struct stat *p_stat )
{
    entry_id_t     tmpid;
    attr_set_t     tmpattr;
    unsigned int   i;
    policy_match_t rc = POLICY_NO_MATCH;

    /* build temporary attr set for testing ignore condition */
    ATTR_MASK_INIT( &tmpattr );

    ATTR_MASK_SET( &tmpattr, name );
    strcpy( ATTR( &tmpattr, name ), name );

    ATTR_MASK_SET( &tmpattr, fullpath );
    strcpy( ATTR( &tmpattr, fullpath ), fullpath );

    ATTR_MASK_SET( &tmpattr, depth );
    ATTR( &tmpattr, depth ) = depth;

#if defined( _LUSTRE ) && defined( _MDS_STAT_SUPPORT )
    PosixStat2EntryAttr( p_stat, &tmpattr , !(is_lustre_fs && global_config.direct_mds_stat));
#else
    PosixStat2EntryAttr( p_stat, &tmpattr , TRUE );
#endif

    /* Set entry id */
#ifndef _HAVE_FID
    tmpid.inode = p_stat->st_ino;
    tmpid.device = p_stat->st_dev;
    tmpid.validator = p_stat->st_ctime;
#endif

    rc = POLICY_NO_MATCH;
    for ( i = 0; i < fs_scan_config.ignore_count; i++ )
    {
        switch ( EntryMatches( &tmpid, &tmpattr, &fs_scan_config.ignore_list[i].boolexpr ) )
        {
        case POLICY_MATCH:
            return TRUE;
        case POLICY_MISSING_ATTR:
            DisplayLog( LVL_MAJOR, FSSCAN_TAG, "Attribute is missing for checking ignore rule" );
            if ( rc != POLICY_ERR )
                rc = POLICY_MISSING_ATTR;
            break;
        case POLICY_ERR:
            DisplayLog( LVL_CRIT, FSSCAN_TAG, "An error occured when checking ignore rule" );
            rc = POLICY_ERR;
            break;
        case POLICY_NO_MATCH:
            /* continue testing other ignore rules */
            break;
        }
    }

#ifdef _SHERPA
    /* is the directory locked? */
    if ( S_ISDIR( p_stat->st_mode ) && TestVerrouAsync( fullpath ) )
    {
        DisplayLog( LVL_EVENT, FSSCAN_TAG, "Directory %s is locked: skipping it", fullpath );
        return TRUE;
    }
#endif


    return ( rc != POLICY_NO_MATCH );

}

/* Terminate a filesystem scan (called by the thread
 * that terminates the last task of scan, and merge
 * itself to the mother task).
 * This function invalidates all entries that have not
 * been updated during the scan.
 * It also updates scan dates and root task.
 */
static int TerminateScan( int scan_complete, time_t date_fin )
{
    entry_proc_op_t op;
    int            st;

    /* final DB operation: remove entries with md_update < scan_start_time */
    InitEntryProc_op( &op );
    op.pipeline_stage = STAGE_RM_OLD_ENTRIES;

    /* set callback */
    op.callback_func = db_special_op_callback;
    op.callback_param = ( void * ) "Remove obsolete entries";

    /* set the timestamp of scan in (md_update attribute) */
    ATTR_MASK_INIT( &op.entry_attr );

    /* set update time  */
    ATTR_MASK_SET( &op.entry_attr, md_update );
    ATTR( &op.entry_attr, md_update ) = scan_start_time;

    op.entry_attr_is_set = TRUE;

    /* set wait db flag */
    set_db_wait_flag(  );

    st = EntryProcessor_Push( &op );

    if ( st )
    {
        DisplayLog( LVL_CRIT, FSSCAN_TAG, "CRITICAL ERROR: EntryProcessor_Push returned %d", st );
        return st;
    }
    wait_for_db_callback(  );

    /* reset threads stats */
    ResetScanStats(  );

    /* take a lock of scan info */
    P( lock_scan );

    /* reinitialize scan status */
    last_scan_complete = scan_complete;
    last_scan_time = date_fin;
    last_duration = date_fin - scan_start_time;
    scan_start_time = 0;

    timerclear( &accurate_start_time );

    root_task = NULL;

    /* release the lock */
    V( lock_scan );

    DisplayLog( LVL_EVENT, FSSCAN_TAG, "File list of %s has been updated", global_config.fs_path );

    /* sending batched alerts */
    DisplayLog( LVL_VERB, FSSCAN_TAG, "Sending batched alerts, if any" );
    Alert_EndBatching();

    if ( fsscan_once )
        signal_scan_finished(  );

    FlushLogs(  );

    return 0;

}



/**
 * Function for terminating a task
 * and merging recursively with parent terminated tasks.
 */
static int RecursiveTaskTermination( thread_scan_info_t * p_info,
                                     robinhood_task_t * p_task, int bool_scan_complete )
{
    int            st;
    int            bool_termine;
    robinhood_task_t *current_task = p_task;

    /* notify of current action (for watchdog) */
    p_info->last_action = time( NULL );

    /* tag itself at terminated */
    st = FlagTaskAsFinished( current_task, &bool_termine );

    if ( st )
    {
        DisplayLog( LVL_CRIT, FSSCAN_TAG, "CRITICAL ERROR: FlagTaskAsFinished returned %d", st );
        return st;
    }


    if ( bool_termine )
    {
        robinhood_task_t *maman;
        int            bool_termine_mere;

        do
        {
            DisplayLog( LVL_FULL, FSSCAN_TAG,
                        "%s is finished and has no more child => merging to the parent task",
                        current_task->path );

            /* No chance that another thread has a lock on the current task,
             * because all the child tasks are terminated.
             * We are the last thread to handle it.
             */
            maman = current_task->parent_task;

            if ( maman != NULL )
            {
                /* removes this task from parent's sub-task list */
                st = RemoveChildTask( maman, current_task, &bool_termine_mere );

                if ( st )
                {
                    DisplayLog( LVL_CRIT, FSSCAN_TAG,
                                "CRITICAL ERROR: RemoveChildTask returned %d", st );
                    return st;
                }

            }
            else                /* manage parent task */
            {
                struct timeval fin_precise;
                struct timeval duree_precise;
                unsigned int   i, count, err_count;

                gettimeofday( &fin_precise, NULL );

                timersub( &fin_precise, &accurate_start_time, &duree_precise );

                /* End of mother task, compute and display summary */

                count = 0;
                err_count = 0;

                for ( i = 0; i < fs_scan_config.nb_threads_scan; i++ )
                {
                    count += thread_list[i].entries_handled;
                    err_count += thread_list[i].entries_errors;
                }

                DisplayLog( LVL_MAJOR, FSSCAN_TAG,
                            "Full scan of %s completed, %u entries found (%u errors). Duration = %ld.%02lds",
                            global_config.fs_path, count, err_count, duree_precise.tv_sec,
                            duree_precise.tv_usec/10000 );

                DisplayLog( LVL_EVENT, FSSCAN_TAG, "Flushing pipeline..." );

                /* merge global scan information */
                /** @TODO better completion boolean management: also check child tasks completion */

                st = TerminateScan( bool_scan_complete, time( NULL ) );

                if ( st )
                {
                    DisplayLog( LVL_CRIT, FSSCAN_TAG,
                                "CRITICAL ERROR: TerminateScan returned %d", st );
                    return st;
                }

            }

            /* this thread now manages parent task */
            p_info->current_task = maman;

            /* notify of current activity (for watchdog) */
            p_info->last_action = time( NULL );

            /* free the task */
            DisplayLog( LVL_FULL, FSSCAN_TAG, "Freeing task %s", current_task->path );
            FreeTask( current_task );

            current_task = maman;

        }
        while ( bool_termine_mere && ( current_task != NULL ) );

        /* mission complete! */
        p_info->current_task = NULL;

    }
    else
    {
        /* The thread leave this task running,
         * because there are still sub-tasks.
         * Go and manage another task from taskpool
         */
        p_info->current_task = NULL;
        DisplayLog( LVL_FULL, FSSCAN_TAG,
                    "%s finished, but there are still child tasks", current_task->path );
    }

    /* notify of current activity (for watchdog) */
    p_info->last_action = time( NULL );

    return 0;

}                               /* RecursiveTaskTermination */



/* process a filesystem entry */
static int HandleFSEntry( thread_scan_info_t * p_info, robinhood_task_t * p_task, char *entry_name, DIR * parent )
{
    char           entry_path[RBH_PATH_MAX];
    struct stat    inode;
    int            st;

    /* build absolute path */
    snprintf( entry_path, RBH_PATH_MAX, "%s/%s", p_task->path, entry_name );

    /* retrieve information about the entry (to know if it's a directory or something else) */
#if defined( _LUSTRE ) && defined( _MDS_STAT_SUPPORT )
    if ( is_lustre_fs && global_config.direct_mds_stat )
    {
        st = lustre_mds_stat( entry_path, parent, &inode );
        if ( st ) return st;
        /* device id is not the one seen by client: change it */
        inode.st_dev = fsdev;
    }
    else
#endif
    if ( lstat( entry_path, &inode ) == -1 )
    {
        DisplayLog( LVL_MAJOR, FSSCAN_TAG,
                    "lstat on %s failed: Error %d: %s: entry ignored",
                    entry_path, errno, strerror( errno ) );
        return errno;
    }

    /* Test if entry or directory is ignored */
    if ( ignore_entry( entry_path, entry_name, p_task->depth, &inode ) )
    {
        DisplayLog( LVL_DEBUG, FSSCAN_TAG, "%s matches an 'ignore' rule. Skipped.", entry_path );
        return 0;
    }

    /* Check that the entry is on the same device as the filesystem we manage.
     * (prevent from mountpoint traversal).
     */
    if ( global_config.stay_in_fs )
    {
        if ( inode.st_dev != fsdev )
        {
            DisplayLog( LVL_CRIT, FSSCAN_TAG,
                        "%s (0x%.8"PRI_DT") is in a filesystem different from root (0x%.8"
                        PRI_DT "), entry ignored", entry_path, inode.st_dev, fsdev );
            return -1;
        }
    }

    /* General purpose: Push all entries except dirs to the pipeline.
     * Lustre-HSM: Only push files.
     */
    /* Note: for non-Lustre-HSM purposes, directories are pushed in Thr_scan(),
     * after the closedir() call.
     */
#ifdef _LUSTRE_HSM
    if ( S_ISREG( inode.st_mode ) )
#else
    if ( !S_ISDIR( inode.st_mode ) )
#endif
    {
        entry_proc_op_t op;

        /* init the structure */
        InitEntryProc_op( &op );

#ifdef _HAVE_FID
        /* no need for parsing */
        op.pipeline_stage = STAGE_GET_FID;
#else
        op.pipeline_stage = STAGE_GET_INFO_DB;
#endif
        ATTR_MASK_INIT( &op.entry_attr );

        ATTR_MASK_SET( &op.entry_attr, name );
        strcpy( ATTR( &op.entry_attr, name ), entry_name );

        ATTR_MASK_SET( &op.entry_attr, fullpath );
        strcpy( ATTR( &op.entry_attr, fullpath ), entry_path );

#ifdef ATTR_INDEX_invalid
        ATTR_MASK_SET( &op.entry_attr, invalid );
        ATTR( &op.entry_attr, invalid ) = FALSE;
#endif

        ATTR_MASK_SET( &op.entry_attr, depth );
        ATTR( &op.entry_attr, depth ) = p_task->depth;  /* depth(/<mntpoint>/toto) = 0 */

#if defined( _LUSTRE ) && defined( _MDS_STAT_SUPPORT )
        PosixStat2EntryAttr( &inode, &op.entry_attr, !(is_lustre_fs && global_config.direct_mds_stat) );
#else
        PosixStat2EntryAttr( &inode, &op.entry_attr, TRUE );
#endif
        op.entry_attr_is_set = TRUE;

        /* set update time  */
        ATTR_MASK_SET( &op.entry_attr, md_update );
        ATTR( &op.entry_attr, md_update ) = time( NULL );
#ifdef _HAVE_FID
        ATTR_MASK_SET( &op.entry_attr, path_update );
        ATTR( &op.entry_attr, path_update ) = time( NULL );
#endif

        /* Set entry id */
#ifndef _HAVE_FID
        op.entry_id.inode = inode.st_ino;
        op.entry_id.device = inode.st_dev;
        op.entry_id.validator = inode.st_ctime;
        op.entry_id_is_set = TRUE;
#else
        op.entry_id_is_set = FALSE;
#endif

        op.extra_info_is_set = FALSE;

        /* Push entry to the pipeline */
        st = EntryProcessor_Push( &op );

        if ( st )
        {
            DisplayLog( LVL_CRIT, FSSCAN_TAG,
                        "CRITICAL ERROR: EntryProcessor_Push returned %d", st );
            return st;
        }

    }

    if ( S_ISDIR( inode.st_mode ) )
    {

        robinhood_task_t *p_scan_task;

        /* create a scan task for this directory */
        p_scan_task = CreateTask(  );

        if ( p_scan_task == NULL )
        {
            DisplayLog( LVL_CRIT, FSSCAN_TAG, "CRITICAL ERROR: task creation failed" );
            return -1;
        }

        p_scan_task->parent_task = p_task;
        strncpy( p_scan_task->path, entry_path, RBH_PATH_MAX );
        p_scan_task->directory_md = inode;
        p_scan_task->depth = p_task->depth + 1;
        p_scan_task->task_finished = FALSE;

        /* add the task to the parent's subtask list */
        st = AddChildTask( p_task, p_scan_task );

        if ( st )
            return ( st );

        /* insert task to the stack */
        st = InsertTask_to_Stack( &tasks_stack, p_scan_task );

        if ( st )
        {
            DisplayLog( LVL_CRIT, FSSCAN_TAG, "CRITICAL ERROR: InsertCandidate returned %d", st );
            return st;
        }

    }

    return 0;

}




/**
 * Thr_scan :
 * main routine for handling tasks.
 */
static void   *Thr_scan( void *arg_thread )
{
    int            st;
    robinhood_task_t *p_task;

    struct dirent  entree_rep;
    struct dirent *cookie_rep;
    struct stat    inode_entree;
    int            rc;
    DIR           *dirp;

    struct timeval start_dir;
    struct timeval end_dir;
    struct timeval diff;

    thread_scan_info_t *p_info = ( thread_scan_info_t * ) arg_thread;

    unsigned int   nb_entries = 0;
    unsigned int   nb_errors = 0;

    /* Initialize buddy management */
#ifdef _BUDDY_MALLOC
    if ( BuddyInit( &buddy_config ) )
    {
        DisplayLog( LVL_CRIT, FSSCAN_TAG, "Error Initializing Memory Management" );
        Exit( 1 );
    }
#endif

    while ( !p_info->force_stop )
    {
        DisplayLog( LVL_FULL, FSSCAN_TAG, "ThrScan-%d: Waiting for a task", p_info->index );

        /* take a task from queue */
        p_task = GetTask_from_Stack( &tasks_stack );

        /* skip it if the thread was requested to stop */
        if ( p_info->force_stop )
            break;

        /* ERROR if NULL */
        if ( p_task == NULL )
        {
            DisplayLog( LVL_CRIT, FSSCAN_TAG, "CRITICAL ERROR: GetTask_from_Stack returned NULL" );
            Exit( 1 );
        }

        /* update thread info */
        p_info->current_task = p_task;
        p_info->last_action = time( NULL );

        DisplayLog( LVL_FULL, FSSCAN_TAG,
                    "ThrScan-%d: Processing %s (depth %u)",
                    p_info->index, p_task->path, p_task->depth );

        /* directory processing start time */
        gettimeofday( &start_dir, NULL );

        /* if this is the root task, check that the filesystem is still mounted */
        if ( p_task->depth == 0 )
        {
            /* retrieve filesystem device id */
            if ( stat( p_task->path, &inode_entree ) == -1 )
            {
                DisplayLog( LVL_CRIT, FSSCAN_TAG,
                            "lstat failed on %s. Error %d", p_task->path, errno );
                DisplayLog( LVL_CRIT, FSSCAN_TAG, "Error accessing filesystem: exiting" );
                Exit( 1 );
            }

            if ( fsdev != inode_entree.st_dev )
            {
                DisplayLog( LVL_CRIT, FSSCAN_TAG,
                            "Somebody cut the grass under my feet!!! I suicide me..." );
                Exit( 1 );
            }

            /* test lock before starting scan */
            TestLockFile( &p_info->last_action );

        }


        /* open directory */

        if ( ( dirp = opendir( p_task->path ) ) == NULL )
        {
            DisplayLog( LVL_CRIT, FSSCAN_TAG,
                        "opendir on %s failed: Error %d: %s",
                        p_task->path, errno, strerror( errno ) );

            /* cancel the task */
            st = RecursiveTaskTermination( p_info, p_task, FALSE );

            if ( st )
            {
                DisplayLog( LVL_CRIT, FSSCAN_TAG,
                            "CRITICAL ERROR: RecursiveTaskTermination returned %d", st );
                Exit( 1 );
            }

            /* back to "normal" life :-) */
            continue;
        }                       /* opendir */


        /* scan directory entries */
        nb_entries = 0;
        nb_errors = 0;

        while ( 1 )
        {
            /* notify current activity (for watchdog) */
            p_info->last_action = time( NULL );

            rc = readdir_r( dirp, &entree_rep, &cookie_rep );

            if ( rc == 0 && cookie_rep == NULL )
                /* end of directory */
                break;
            else if ( p_info->force_stop )
            {
                DisplayLog( LVL_EVENT, FSSCAN_TAG, "Stop requested: cancelling directory scan operation" );
                goto end_task;
            }
            else if ( rc != 0 )
            {
                DisplayLog( LVL_CRIT, FSSCAN_TAG, "ERROR %d reading directory '%s': %s",
                            rc, p_task->path, strerror(rc) );
                nb_errors++;
                break;
            }

            /* ignore . and .. */

            if ( !strcmp( entree_rep.d_name, "." ) || !strcmp( entree_rep.d_name, ".." ) )
                continue;

            nb_entries++;

#ifdef SIMUL_HANGS
            /* simulate a hang */
            sleep( 20 * p_task->depth );
#endif

            /* Handle filesystem entry.
             * Don't manage return value (entry is ignored).
             */
            if ( HandleFSEntry( p_info, p_task, entree_rep.d_name, dirp ) != 0 )
                nb_errors++;

        } /* end of dir */

        if ( rc != EBADF )
            closedir( dirp );

/* No directory management for Lustre HSM */
#ifndef _LUSTRE_HSM
        if ( p_task->depth > 0 )
        {
            /* Fill dir info and push it to the pileline for checking alerts on it,
             * and possibly purge it if it is empty for a long time.
             */

            entry_proc_op_t op;

            /* init the structure */
            InitEntryProc_op( &op );

#ifdef _HAVE_FID
            /* retrieve fids from posix path */
            op.pipeline_stage = STAGE_GET_FID;
#else
            /* attributes already retrieved */
            op.pipeline_stage = STAGE_GET_INFO_DB;
#endif
            ATTR_MASK_INIT( &op.entry_attr );

            ATTR_MASK_SET( &op.entry_attr, name );
            strcpy( ATTR( &op.entry_attr, name ), basename( p_task->path ) );

            ATTR_MASK_SET( &op.entry_attr, fullpath );
            strcpy( ATTR( &op.entry_attr, fullpath ), p_task->path );

            ATTR_MASK_SET( &op.entry_attr, invalid );
            ATTR( &op.entry_attr, invalid ) = FALSE;

            ATTR_MASK_SET( &op.entry_attr, depth );
            ATTR( &op.entry_attr, depth ) = p_task->depth - 1;  /* depth(/tmp/toto) = 0 */

            ATTR_MASK_SET( &op.entry_attr, dircount );
            ATTR( &op.entry_attr, dircount ) = nb_entries;

#if defined( _LUSTRE ) && defined( _MDS_STAT_SUPPORT )
            PosixStat2EntryAttr( &p_task->directory_md, &op.entry_attr, !(is_lustre_fs && global_config.direct_mds_stat) );
#else
            PosixStat2EntryAttr( &p_task->directory_md, &op.entry_attr, TRUE );
#endif
            op.entry_attr_is_set = TRUE;

            /* set update time  */
            ATTR_MASK_SET( &op.entry_attr, md_update );
            ATTR( &op.entry_attr, md_update ) = time( NULL );
#ifdef _HAVE_FID
            ATTR_MASK_SET( &op.entry_attr, path_update );
            ATTR( &op.entry_attr, path_update ) = time( NULL );
#endif

            /* Set entry id */
#ifndef _HAVE_FID
            op.entry_id.inode = p_task->directory_md.st_ino;
            op.entry_id.device = p_task->directory_md.st_dev;
            op.entry_id.validator = p_task->directory_md.st_ctime;
            op.entry_id_is_set = TRUE;
#else
            op.entry_id_is_set = FALSE;
#endif

            op.extra_info_is_set = FALSE;

            /* Push directory to the pipeline */
            st = EntryProcessor_Push( &op );

            if ( st )
            {
                DisplayLog( LVL_CRIT, FSSCAN_TAG,
                            "CRITICAL ERROR: EntryProcessor_Push returned %d", st );
                return NULL;
            }
        }
#endif

        gettimeofday( &end_dir, NULL );
        timersub( &end_dir, &start_dir, &diff );

        /* update thread statistics */
        timeradd( &diff, &p_info->time_consumed, &p_info->time_consumed );
        p_info->entries_handled += nb_entries;
        p_info->entries_errors += nb_errors;

        /* make an average on directory entries */
        if ( nb_entries > 0)
        {
            unsigned int rest;
            p_info->last_processing_time.tv_sec = diff.tv_sec/nb_entries;
            rest = diff.tv_sec - (p_info->last_processing_time.tv_sec * nb_entries) ;
            p_info->last_processing_time.tv_usec = ((1000000 * rest ) + diff.tv_usec)/nb_entries;
        }

        /* terminate and free current task */
        st = RecursiveTaskTermination( p_info, p_task, TRUE );
        if ( st )
        {
            DisplayLog( LVL_CRIT, FSSCAN_TAG,
                        "CRITICAL ERROR: RecursiveTaskTermination returned %d", st );
            Exit( 1 );
        }

    }

end_task:
    p_info->current_task = NULL;

    /* check scan termination status */
    if (all_threads_idle())
        signal_scan_finished();

    return NULL;

}




/**
 * Audit module initialization
 * (called at deamon startup)
 *
 * The function looks at the content of the configuration structure
 * that have been previously parsed.
 * 
 * It returns a status code:
 *   0 : initialization sucessful
 *   -1 : unexpected error at initialization.
 *   EINVAL : a parameter from the config file is invalid.
 */
int Robinhood_InitScanModule(  )
{
    int            st;
    int            rc, i;

    /* fill-in be structures with zeros */
    memset( &tasks_stack, 0, sizeof( tasks_stack ) );

    /* initialize module, using configuration info */

    /* preallocation parameters */
    if ( fs_scan_config.nb_prealloc_tasks > 0 )
        SetNbPreallocTasks( fs_scan_config.nb_prealloc_tasks );

    /* initializing task stack */

    st = InitTaskStack( &tasks_stack );
    if ( st )
        return st;

    /* Initialize locks */

    pthread_mutex_init( &lock_scan, NULL );

    /* check device, filesystem type, ... */
    if ( ( rc = CheckFSInfo( global_config.fs_path, global_config.fs_type, &fsdev,
                             global_config.check_mounted, TRUE ) ) != 0 )
        return ( rc );

    if ( !strcmp( global_config.fs_type, "lustre" ) )
        is_lustre_fs = TRUE;

    /* initializing thread attrs */

    pthread_attr_init( &thread_attrs );
    pthread_attr_setscope( &thread_attrs, PTHREAD_SCOPE_SYSTEM );
    pthread_attr_setdetachstate( &thread_attrs, PTHREAD_CREATE_JOINABLE );

    /* dynamic allocation */
    thread_list = MemCalloc( fs_scan_config.nb_threads_scan, sizeof( thread_scan_info_t ) );
    if ( !thread_list )
        return ENOMEM;

    /* creating scanning threads  */

    for ( i = 0; i < fs_scan_config.nb_threads_scan; i++ )
    {

        thread_list[i].index = i;
        thread_list[i].last_action = 0;
        thread_list[i].current_task = NULL;

        thread_list[i].force_stop = FALSE;

        thread_list[i].entries_handled = 0;
        thread_list[i].entries_errors = 0;

        timerclear( &thread_list[i].time_consumed );
        timerclear( &thread_list[i].last_processing_time );

        rc = pthread_create( &( thread_list[i].thread_scan ), &thread_attrs,
                             Thr_scan, &( thread_list[i] ) );

        if ( rc != 0 )
        {
            DisplayLog( LVL_CRIT, FSSCAN_TAG,
                        "ERROR %d CREATING SCANNING THREAD: %s", rc, strerror( rc ) );
            return rc;
        }
    }

    return 0;

}


/**
 * Stop scan module
 */
int Robinhood_StopScanModule(  )
{
    unsigned int   i;
    int            err = 0;

    /* terminate scan threads */

    for ( i = 0; i < fs_scan_config.nb_threads_scan; i++ )
    {
        thread_list[i].force_stop = TRUE;
    }

    DisplayLog( LVL_EVENT, FSSCAN_TAG, "Stop request has been sent to all scan threads" );

    /* if there are still threads doing something, wait for them */
    if ( !all_threads_idle() )
        wait_scan_finished();

    return err;
}



/* Start a scan of the filesystem. 
 * This creates a root task and push it to the stack of tasks.
 * Return EBUSY if a scan is already running.
 */
static int StartScan(  )
{
    robinhood_task_t *p_parent_task;

    /* Lock scanning status */
    P( lock_scan );

    /* is a scan already running ? */
    if ( root_task != NULL )
    {
        V( lock_scan );
        DisplayLog( LVL_MAJOR, FSSCAN_TAG,
                    "An scan is already running on %s", global_config.fs_path );
        return EBUSY;
    }


    /* create a root task */

    p_parent_task = CreateTask(  );

    if ( p_parent_task == NULL )
    {
        V( lock_scan );
        DisplayLog( LVL_CRIT, FSSCAN_TAG,
                    "ERROR creating scan task for %s", global_config.fs_path );
        return -1;
    }

    strcpy( p_parent_task->path, global_config.fs_path );
    p_parent_task->depth = 0;
    p_parent_task->task_finished = FALSE;

    /* set the mother task, and remember start time */
    root_task = p_parent_task;
    scan_start_time = time( NULL );
    gettimeofday( &accurate_start_time, NULL );

    /* reset threads stats */
    ResetScanStats(  );

    /* unlock scanning status */
    V( lock_scan );

    /* start batching alerts */
    Alert_StartBatching();

    /* insert first task in stack */
    InsertTask_to_Stack( &tasks_stack, p_parent_task );

    /* indicates that a scan started in logs */
    FlushLogs();

    return 0;
}


/**
 * Start a new scan thread in case a previous thread has timed-out.
 * The new thread recovers the timed-out task and them become a standard
 * Thr_scan().
 */
static void   *Thr_scan_recovery( void *arg_thread )
{
    int            st;

    thread_scan_info_t *p_info = ( thread_scan_info_t * ) arg_thread;

    p_info->last_action = time( NULL );

    /* Initialize buddy management */
#ifdef _BUDDY_MALLOC
    if ( BuddyInit( &buddy_config ) )
    {
        DisplayLog( LVL_CRIT, FSSCAN_TAG, "Error Initializing Memory Management" );
        Exit( 1 );
    }
#endif

    /* terminate and free current task */
    st = RecursiveTaskTermination( p_info, p_info->current_task, FALSE );

    if ( st )
    {
        DisplayLog( LVL_CRIT, FSSCAN_TAG,
                    "CRITICAL ERROR: RecursiveTaskTermination returned %d", st );
        Exit( 1 );
    }

    return Thr_scan( arg_thread );

}

/**
 * Updates the max usage indicator (used for adaptive scan interval).
 */
static void UpdateMaxUsage(  )
{
    char           tmpval[1024];
    double         val;

    if ( !lmgr_init )
    {
        if ( ListMgr_InitAccess( &lmgr ) != DB_SUCCESS )
            return;
        lmgr_init = TRUE;
    }

    if ( ListMgr_GetVar( &lmgr, USAGE_MAX_VAR, tmpval ) == DB_SUCCESS )
    {
        if ( sscanf( tmpval, "%lf", &val ) == 1 )
            usage_max = val;
    }

    scan_interval = fs_scan_config.min_scan_interval
        + ( 100.0 - usage_max ) * 0.01 * ( fs_scan_config.max_scan_interval -
                                           fs_scan_config.min_scan_interval );

    /* set current scan period, so it is available to client */
    sprintf( tmpval, "%lu", ( unsigned long ) scan_interval );
    ListMgr_SetVar( &lmgr, SCAN_INTERVAL_VAR, tmpval );

}


/**
 * Check thread's activity or start a scan if its time.
 */
int Robinhood_CheckScanDeadlines(  )
{
    int            st;
    char           tmp_buff[256];
    char           tmp_buff2[256];
    struct tm      paramtm;

    time_t         loc_last_scan_time;
    unsigned int   loc_last_duration;
    int            loc_scan_complete;
    int            loc_scan_running;
    time_t         loc_start_time;
    time_t         loc_last_action;
    time_t         now;


    /* Get scan info */

    /* lock on scan status */
    P( lock_scan );

    /* retrieve stats */

    loc_last_scan_time = last_scan_time;
    loc_last_duration = last_duration;
    loc_scan_complete = last_scan_complete;

    if ( root_task != NULL )
    {
        unsigned int   i;
        time_t         last_action = 0;

        loc_scan_running = TRUE;
        loc_start_time = scan_start_time;

        for ( i = 0; i < fs_scan_config.nb_threads_scan; i++ )
        {
            if ( ( thread_list[i].current_task != NULL )
                 && ( thread_list[i].last_action > last_action ) )
            {
                last_action = thread_list[i].last_action;
            }
        }

        loc_last_action = last_action;
    }
    else
    {
        loc_scan_running = FALSE;
        loc_start_time = 0;
        loc_last_action = 0;
    }

    V( lock_scan );


    DisplayLog( LVL_FULL, FSSCAN_TAG, "Verifying scan deadlines for %s", global_config.fs_path );

    now = time( NULL );

    /* debug traces */

    if ( loc_last_scan_time != 0 )
    {
        strftime( tmp_buff, 256, "%Y/%m/%d %T", localtime_r( &loc_last_scan_time, &paramtm ) );

        DisplayLog( LVL_DEBUG, FSSCAN_TAG,
                    "last scan done in %u s: %s (%s)", loc_last_duration,
                    tmp_buff, ( loc_scan_complete ? "complete" : "partial" ) );
    }

    if ( loc_scan_running )
    {
        strftime( tmp_buff, 256, "%Y/%m/%d %T", localtime_r( &loc_start_time, &paramtm ) );
        strftime( tmp_buff2, 256, "%Y/%m/%d %T", localtime_r( &loc_last_action, &paramtm ) );
        DisplayLog( LVL_DEBUG, FSSCAN_TAG,
                    "scan running: started at %s, last action: %s", tmp_buff, tmp_buff2 );
    }

    /* compute scan interval (depending on last usage max) */
    UpdateMaxUsage(  );

    if ( loc_scan_complete && !loc_scan_running && ( now - loc_last_scan_time >= scan_interval ) )
    {
        FormatDuration( tmp_buff, 256, scan_interval );

        /* starting a new scan, if its time */

        DisplayLog( LVL_MAJOR, FSSCAN_TAG,
                    "Starting scan of %s (current scan interval is %s)",
                    global_config.fs_path, tmp_buff );

        st = StartScan(  );

        if ( st == EBUSY )
        {
            DisplayLog( LVL_MAJOR, FSSCAN_TAG,
                        "An scan is already running on %s", global_config.fs_path );
        }
        else if ( st != 0 )
        {
            DisplayLog( LVL_CRIT, FSSCAN_TAG, "Error in StartScan on %s", global_config.fs_path );
            return st;
        }

    }
    else if ( !loc_scan_complete && !loc_scan_running
              && ( now - loc_last_scan_time >= fs_scan_config.scan_retry_delay ) )
    {
        /* retry a scan, if the last was incomplete */

        DisplayLog( LVL_MAJOR, FSSCAN_TAG, "Starting scan of %s", global_config.fs_path );

        st = StartScan(  );

        if ( st == EBUSY )
        {
            DisplayLog( LVL_MAJOR, FSSCAN_TAG,
                        "An scan is already running on %s", global_config.fs_path );
        }
        else if ( st != 0 )
        {
            DisplayLog( LVL_CRIT, FSSCAN_TAG, "Error in StartScan on %s", global_config.fs_path );
            return st;
        }

    }
    else if ( loc_scan_running )
    {
        int            i;

        /* number of threads having a task assigned */
        unsigned int   nb_assigned = 0;

        /* detect and manage hangs */

        /* for each thread, check timeout */
        for ( i = 0; i < fs_scan_config.nb_threads_scan; i++ )
        {
            if ( thread_list[i].current_task != NULL )
                nb_assigned++;

            if ( ( thread_list[i].current_task != NULL ) && (fs_scan_config.scan_op_timeout != 0)
                 && ( time( NULL ) - thread_list[i].last_action > fs_scan_config.scan_op_timeout ) )
            {
                DisplayLog( LVL_VERB, FSSCAN_TAG,
                            "Scan thread #%d looks stuck in %s",
                            i, thread_list[i].current_task->path );

                /* check if the task is waiting for a lock */
                if ( ( thread_list[i].current_task->parent_task == NULL )
                     && ( TestTaskTermination( thread_list[i].current_task ) ) )
                {
                    DisplayLog( LVL_VERB, FSSCAN_TAG,
                                "Actually, thread #%d is currently updating candidate list of %s",
                                i, global_config.fs_path );
                }
                else
                {
                    char tmpbuf[1024];
                    DisplayLog( LVL_MAJOR, FSSCAN_TAG,
                                "Hang of thread #%d while it was scanning %s (inactive for %ld sec)",
                                i, thread_list[i].current_task->path,
                                time( NULL ) - thread_list[i].last_action );

                    snprintf( tmpbuf, 1024, "FS scan is blocked (%s)",
                              global_config.fs_path );
                    RaiseAlert( tmpbuf, "A thread has been inactive for %ld sec\n"
                                "while scanning directory %s",
                                time( NULL ) - thread_list[i].last_action,
                                thread_list[i].current_task->path );

                    /* if the config says to exit on timeout => do it */
                    if ( fs_scan_config.exit_on_timeout )
                    {
                            DisplayLog( LVL_CRIT, FSSCAN_TAG,
                                        "exit_on_timeout is set in config file => EXITING" );
                            Exit( ETIMEDOUT );
                    }

                    /* else restart the hung thread */
                    if ( TerminateThread( thread_list[i].thread_scan ) == 0 )
                    {
                        int            rc;

                        nb_hang_total++;

                        /* increment the error counter */
                        thread_list[i].entries_errors ++;

                        /* the monitoring thread does not terminate the task itself,
                         * to avoid blocking it too. We start a recovery thread for
                         * that. This new thread will then become a standard worker
                         * thread, to replace the terminated one.
                         */
                        rc = pthread_create( &(thread_list[i].thread_scan),
                                             &thread_attrs,
                                             Thr_scan_recovery, &(thread_list[i]) );

                        if ( rc != 0 )
                        {
                            DisplayLog( LVL_CRIT, FSSCAN_TAG,
                                        "ERROR CREATING SCANNING RECOVERY THREAD: %d: %s",
                                        rc, strerror( rc ) );
                            return rc;
                        }
                    } /* end if thread terminated by monitoring thread */

                } /* end if child task */

            } /* end if hang detected */

        } /* end of loop on threads */


        /* if no thread has a task assigned, and an scan is running for a while,
         * there is something anormal: so, terminate the daemon.
         */
        if ( ( nb_assigned == 0 ) && (fs_scan_config.scan_op_timeout != 0)
             && ( now - loc_last_action > fs_scan_config.scan_op_timeout )
             && ( now - loc_start_time > fs_scan_config.scan_op_timeout ) )
        {
            DisplayLog( LVL_CRIT, FSSCAN_TAG,
                        "A scan on %s is supposed to be running, but no threads are active. Anormal situation. Exiting.",
                        global_config.fs_path );
            Exit( 1 );
        }


    } /* scan is running */

    return 0;

}




/** 
 * Retrieve some statistics about current and terminated audits.
 * (called by the statistic collector)
 * 
 * Take as parameter a structure of statistics to be filled.
 *
 * It returns a status code:
 *   0 : statistics collected successfully
 *   -1 : unexpected problem stats collection
 */
int Robinhood_StatsScan( robinhood_fsscan_stat_t * p_stats )
{
    /* lock scan info */
    P( lock_scan );

    p_stats->last_fsscan_time = last_scan_time;
    p_stats->last_duration = last_duration;
    p_stats->scan_complete = last_scan_complete;
    p_stats->current_scan_interval = scan_interval;

    if ( root_task != NULL )
    {
        unsigned int   i;
        time_t         last_action = 0;
        struct timeval total_time = { 0, 0 };
        struct timeval curr_time = { 0, 0 };
        unsigned int   nb_done = 0;

        p_stats->scanned_entries = 0;
        p_stats->error_count = 0;
        p_stats->scan_running = TRUE;
        p_stats->start_time = scan_start_time;

        for ( i = 0; i < fs_scan_config.nb_threads_scan; i++ )
        {
            if ( ( thread_list[i].current_task != NULL )
                 && ( thread_list[i].last_action > last_action ) )
            {
                last_action = thread_list[i].last_action;
            }

            /* entry processing time */
            if ( thread_list[i].entries_handled )
            {
                timeradd( &thread_list[i].time_consumed, &total_time, &total_time );
                timeradd( &thread_list[i].last_processing_time, &curr_time, &curr_time );
                p_stats->scanned_entries += thread_list[i].entries_handled;
                p_stats->error_count += thread_list[i].entries_errors;
                nb_done++;
            }
        }

        p_stats->last_action = last_action;

        /* avg speed */
        if ( p_stats->scanned_entries )
            p_stats->avg_ms_per_entry =
                ( ( 1000.0 * total_time.tv_sec ) +
                  ( 1E-3 * total_time.tv_usec ) ) / ( double ) ( p_stats->scanned_entries );
        else
            p_stats->avg_ms_per_entry = 0.0;

        /* current speed */
        if ( nb_done )
            p_stats->curr_ms_per_entry =
                ( ( 1000.0 * curr_time.tv_sec ) +
                  ( 1E-3 * curr_time.tv_usec ) ) / ( double ) ( nb_done );
        else
            p_stats->curr_ms_per_entry = 0.0;

    }
    else
    {
        p_stats->scan_running = FALSE;
        p_stats->start_time = 0;
        p_stats->last_action = 0;
        p_stats->scanned_entries = 0;
        p_stats->error_count = 0;
        p_stats->avg_ms_per_entry = 0.0;
        p_stats->curr_ms_per_entry = 0.0;
    }

    p_stats->nb_hang = nb_hang_total;

    V( lock_scan );

    return 0;

}
