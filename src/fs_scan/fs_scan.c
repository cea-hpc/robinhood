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
#include <sys/stat.h>
#include <sys/time.h>           /* for gettimeofday */
#include <sys/utsname.h>

#include <unistd.h>
#include <linux/types.h>
#include <linux/unistd.h>
#include <errno.h>
#include <syscall.h>

#include <string.h>
#include <fcntl.h>
#include <libgen.h>

fs_scan_config_t fs_scan_config;
int              fsscan_flags = 0;
const char      *partial_scan_root = NULL;

#define fsscan_once ( fsscan_flags & FLAG_ONCE )
#define fsscan_nogc ( fsscan_flags & FLAG_NO_GC )

static int     is_lustre_fs = FALSE;
static int     is_first_scan = FALSE;


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

static inline void set_db_wait_flag( void )
{
    P( special_db_op_lock );
    waiting_db_op = TRUE;
    V( special_db_op_lock );
}

static void wait_for_db_callback( void )
{
    P( special_db_op_lock );
    while ( waiting_db_op )
        pthread_cond_wait( &special_db_op_cond, &special_db_op_lock );
    V( special_db_op_lock );
}

static int db_special_op_callback( lmgr_t *lmgr, struct entry_proc_op_t *p_op, void *arg )
{
    char timestamp[128];

    DisplayLog( LVL_VERB, FSSCAN_TAG, "Callback from database for operation '%s'", ( char * ) arg );

    /* Update end time for pipeline processing */
    if ( lmgr )
    {
        sprintf( timestamp, "%lu", ( unsigned long ) time ( NULL ) );
        ListMgr_SetVar( lmgr, LAST_SCAN_PROCESSING_END_TIME, timestamp );
    }

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

static inline void signal_scan_finished( void )
{
    P( one_shot_lock );
    scan_finished = TRUE;
    pthread_cond_broadcast( &one_shot_cond );
    V( one_shot_lock );
}

static inline int all_threads_idle( void )
{
    unsigned int i;
    for ( i = 0; i < fs_scan_config.nb_threads_scan ; i++ )
        if ( thread_list[i].current_task )
            return FALSE;

    return TRUE;
}

void wait_scan_finished( void )
{
    P( one_shot_lock );
    while ( !scan_finished )
        pthread_cond_wait( &one_shot_cond, &one_shot_lock );
    V( one_shot_lock );
}


/**
 * Reset Scan thread statistics (before and after a scan)
 */
static void ResetScanStats( void )
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


static int ignore_entry( char *fullpath, char *name, unsigned int depth, struct stat *p_stat )
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
    tmpid.fs_key = get_fskey();
    tmpid.validator = p_stat->st_ctime;
#endif

    rc = POLICY_NO_MATCH;
    for ( i = 0; i < fs_scan_config.ignore_count; i++ )
    {
        switch ( EntryMatches( &tmpid, &tmpattr, &fs_scan_config.ignore_list[i].bool_expr, NULL ) )
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
    int     i;
    time_t  last_action = 0;
    char    timestamp[128];
    char    tmp[1024];
    unsigned int count = 0;
    lmgr_t  lmgr;
    int no_db = 0;

    if ( ListMgr_InitAccess( &lmgr ) != DB_SUCCESS )
    {
        no_db = 1;
        DisplayLog(LVL_MAJOR, FSSCAN_TAG, "WARNING: won't be able to update scan stats");
    }

    /* store the last scan end date */
    if (!no_db) {
        sprintf( timestamp, "%lu", ( unsigned long ) date_fin );
        ListMgr_SetVar( &lmgr, LAST_SCAN_END_TIME, timestamp );
    }

    /* update the last action date and get entry count */
    P( lock_scan );
    for ( i = 0; i < fs_scan_config.nb_threads_scan; i++ )
    {
        if ( ( thread_list[i].current_task != NULL )
             && ( thread_list[i].last_action > last_action ) )
        {
            last_action = thread_list[i].last_action;
            count += thread_list[i].entries_handled;
        }
    }
    V( lock_scan );

    if (!no_db) {
        sprintf( timestamp, "%lu", ( unsigned long ) last_action );
        ListMgr_SetVar( &lmgr, LAST_SCAN_LAST_ACTION_TIME, timestamp );
        /* invoke FSScan_StoreStats, so stats are updated at least once during the scan */

        FSScan_StoreStats( &lmgr ) ;
        /* and update the scan status */
        if (partial_scan_root)
        {
            snprintf(tmp, 1024, "%s (%s)", SCAN_STATUS_PARTIAL, partial_scan_root);
            ListMgr_SetVar( &lmgr, LAST_SCAN_STATUS, tmp );
        }
        else
            ListMgr_SetVar( &lmgr, LAST_SCAN_STATUS,
                            scan_complete?SCAN_STATUS_DONE:SCAN_STATUS_INCOMPLETE );

        /* no other DB actions, close the connection */
        ListMgr_CloseAccess(&lmgr);
    }

    /* if scan is incomplete (aborted or failed), don't remove old entries in DB. */
    if (scan_complete)
    {
        entry_proc_op_t *op;

        /* final DB operation: remove entries with md_update < scan_start_time */
        op = EntryProcessor_Get( );
        if (!op) {
            DisplayLog( LVL_CRIT, FSSCAN_TAG, "CRITICAL ERROR: Failed to allocate a new op" );
            return -1;
        }

        op->pipeline_stage = entry_proc_descr.GC_OLDENT;

        /* set callback */
        op->callback_func = db_special_op_callback;
        op->callback_param = ( void * ) "Remove obsolete entries";

        ATTR_MASK_INIT( &op->fs_attrs );

        /* if this is an initial scan, don't rm old entries (but flush pipeline still) */
        if (fsscan_nogc || (is_first_scan && !partial_scan_root))
        {
            op->gc_entries = FALSE;
            op->gc_names = FALSE;
            op->callback_param = ( void * ) "End of flush";
        }
        else
        {
            op->gc_names = TRUE;

#ifdef HAVE_RM_POLICY
            /* Don't clean old entries for partial scan: dangerous if
             * files have been moved from one part of the namespace to another.
             * Clean names, however.
             */
            if (partial_scan_root)
                op->gc_entries = FALSE;
            else
#endif
            op->gc_entries = TRUE;

            /* set the timestamp of scan in (md_update attribute) */
            ATTR_MASK_SET( &op->fs_attrs, md_update );
            ATTR( &op->fs_attrs, md_update ) = scan_start_time;
        }

        /* set root (if partial scan) */
        if (partial_scan_root)
        {
            ATTR_MASK_SET( &op->fs_attrs, fullpath );
            strcpy(ATTR(&op->fs_attrs, fullpath), partial_scan_root);
        }

        /* set wait db flag */
        set_db_wait_flag(  );

#ifndef _BENCH_SCAN
        /* Push directory to the pipeline */
        EntryProcessor_Push(op);
        wait_for_db_callback();
#else
        EntryProcessor_Release(op);
#endif
    }

    /* reset threads stats */
    ResetScanStats(  );

    /* take a lock on scan info */
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

    if (partial_scan_root)
        DisplayLog( LVL_EVENT, FSSCAN_TAG, "File list of %s has been updated", partial_scan_root);
    else
        DisplayLog( LVL_EVENT, FSSCAN_TAG, "File list of %s has been updated", global_config.fs_path );

    /* sending batched alerts */
    DisplayLog( LVL_VERB, FSSCAN_TAG, "Sending batched alerts, if any" );
    Alert_EndBatching();

    if (scan_complete && !EMPTY_STRING(fs_scan_config.completion_command))
    {
        const char *vars[] = {
            "cfg", process_config_file,
            "fspath", global_config.fs_path,
            NULL, NULL
        };

        /* replace special args in completion command */
        char * cmd = replace_cmd_parameters(fs_scan_config.completion_command, vars);
        if (cmd)
        {
            execute_shell_command(TRUE, cmd, 0);
            free(cmd);
        }
    }

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

    /* tag itself as terminated */
    bool_termine = FlagTaskAsFinished( current_task );

    if ( bool_termine )
    {
        robinhood_task_t *maman;
        int            bool_termine_mere;

        do
        {
            DisplayLog( LVL_FULL, FSSCAN_TAG,
                        "%s is finished and has no child left => merging to the parent task",
                        current_task->path );

            /* No chance that another thread has a lock on the current task,
             * because all the children tasks are terminated.
             * We are the last thread to handle it.
             */
            maman = current_task->parent_task;

            if ( maman != NULL )
            {
                /* removes this task from parent's sub-task list */
                bool_termine_mere = RemoveChildTask( maman, current_task );
            }
            else                /* manage parent task */
            {
                struct timeval fin_precise;
                struct timeval duree_precise;
                unsigned int   i, count, err_count;

                gettimeofday( &fin_precise, NULL );

                timersub( &fin_precise, &accurate_start_time, &duree_precise );

                /* End of mother task, compute and display summary */
                bool_termine_mere = TRUE;
                count = 0;
                err_count = 0;

                for ( i = 0; i < fs_scan_config.nb_threads_scan; i++ )
                {
                    count += thread_list[i].entries_handled;
                    err_count += thread_list[i].entries_errors;
                }

                DisplayLog(LVL_MAJOR, FSSCAN_TAG,
                           "%s of %s %s, %u entries found (%u errors). Duration = %ld.%02lds",
                           bool_scan_complete?"Full scan":"Scan",
                           partial_scan_root?partial_scan_root:global_config.fs_path,
                           bool_scan_complete?"completed":"aborted",
                           count, err_count, duree_precise.tv_sec,
                           duree_precise.tv_usec/10000);

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

static inline int check_entry_dev(dev_t entry_dev, dev_t *root_dev, const char *path, int is_root)
{
    /* Check that the entry is on the same device as the filesystem we manage.
     * (prevent from mountpoint traversal).
     */
    if (entry_dev != *root_dev)
    {
        struct stat root_md;
        /* is the FS root changed: file system may have been remounted.
         * else: the entry is not in the same filesystem
         */
        /* 1) check fs root dev_id (use stat as FS mount point maybe a symlink) */
        if (stat(global_config.fs_path, &root_md) == -1)
        {
            int rc = -errno;
            DisplayLog(LVL_CRIT, FSSCAN_TAG,
                       "stat failed on %s: %s", global_config.fs_path, strerror(-rc));
            DisplayLog(LVL_CRIT, FSSCAN_TAG, "ERROR accessing FileSystem: EXITING.");
            Exit(rc);
        }
        if (root_md.st_dev != *root_dev)
        {
            /* manage dev id change after umount/mount */
            DisplayLog(LVL_MAJOR, FSSCAN_TAG,
                       "WARNING: Filesystem device id changed (old=%"PRI_DT", new=%"PRI_DT"): "
                       "checking if it has been remounted", *root_dev, root_md.st_dev);
            if (ResetFS())
            {
                DisplayLog(LVL_CRIT, FSSCAN_TAG, "Filesystem was unmounted!!! EXITING!");
                Exit(1);
            }
            /* update current root_dev */
            *root_dev = get_fsdev();
        }
        /* else: root is still the same */

        /* entry_dev == *root_dev => OK: the entry is in the root filesystem */
        if (entry_dev != *root_dev)
        {
            /* if new root dev != just retrieved root dev
             * a remount occured while we were checking.
             * Return error so the caller update its dev.
             */
            if (is_root)
                return -1;

            if (global_config.stay_in_fs)
            {
                DisplayLog(LVL_CRIT, FSSCAN_TAG,
                           "%s (0x%.8"PRI_DT") is in a filesystem different from root (0x%.8"
                           PRI_DT "), entry ignored", path, entry_dev, *root_dev);
                return -1;
            }
            else
            {
                /* TODO: what fs_key for this entry??? */
                DisplayLog(LVL_DEBUG, FSSCAN_TAG,
                           "%s (0x%.8"PRI_DT") is in a filesystem different from root (0x%.8"
                           PRI_DT "), but 'stay_in_fs' parameter is disabled: processing entry anyhow",
                           path, entry_dev, *root_dev);
            }
        }
    }
    return 0;
}

#ifndef _NO_AT_FUNC

static int noatime_permitted = TRUE;

static int openat_noatime(int pfd, const char *name, int rddir)
{
    int fd = -1;
    int flags = 0;
    int had_eperm = FALSE;

    /* is it for readdir? */
    if (rddir)
        flags = O_RDONLY | O_DIRECTORY;
    else
        flags = O_RDONLY | O_NONBLOCK | O_NOFOLLOW;

    if (noatime_permitted)
    {
        /* try to open with NOATIME flag */
        fd = openat(pfd, name, flags | O_NOATIME);
        if ((fd < 0) && (errno == EPERM))
            had_eperm = TRUE;
    }
    if (fd < 0)
        fd = openat(pfd, name, flags);

    /* openat successful but not with NOATIME => no longer use this flag */
    if (had_eperm && (fd >= 0))
    {
        DisplayLog(LVL_DEBUG, FSSCAN_TAG, "openat failed with O_NOATIME, and was sucessful without it. Disabling this flag.");
        noatime_permitted = FALSE;
    }

    return fd;
}

static int open_noatime(const char *path, int rddir)
{
    int fd = -1;
    int flags = 0;
    int had_eperm = FALSE;

    /* is it for readdir? */
    if (rddir)
        flags = O_RDONLY | O_DIRECTORY;
    else
        flags = O_RDONLY | O_NONBLOCK | O_NOFOLLOW;

    if (noatime_permitted)
    {
        /* try to open with NOATIME flag */
        fd = open(path, flags | O_NOATIME);
        if ((fd < 0) && (errno == EPERM))
            had_eperm = TRUE;
    }

    if (fd < 0)
        fd = open(path, flags);

    /* open successful but not with NOATIME => no longer use this flag */
    if (had_eperm && (fd >= 0))
    {
        DisplayLog(LVL_DEBUG, FSSCAN_TAG, "open failed with O_NOATIME, and was sucessful without it. Disabling this flag.");
        noatime_permitted = FALSE;
    }

    return fd;
}
#endif

/** disable GC if a transient directory error occured */
static void check_dir_error(int rc)
{
    if (rc != 0 && abs(rc) != ENOENT && abs(rc) != ESTALE)
    {
        /* If we cannot read the directory, we must avoid dropping all
         * its entries from the DB => Switch to NO_GC mode. */
        fsscan_flags |= FLAG_NO_GC;
        DisplayLog(LVL_CRIT, FSSCAN_TAG,
                   "Disabling GC because the namespace can't be fully scanned");
    }
}


static inline int get_dirid(const char *path, struct stat *st, entry_id_t *id)
{
#ifdef _HAVE_FID
    int rc;
    rc = Lustre_GetFidFromPath(path, id);
    if (rc)
        DisplayLog(LVL_CRIT, FSSCAN_TAG,
                   "Skipping %s because its FID couldn't be resolved",
                   path);
    check_dir_error(rc);
    return rc;
#else
    id->inode = st->st_ino;
    id->fs_key = get_fskey();
    id->validator = st->st_ctime;
    return 0;
#endif
}

static int create_child_task(const char *childpath, struct stat *inode, robinhood_task_t *parent)
{
    robinhood_task_t *p_task;
    int rc = 0;

    p_task = CreateTask();

    if (p_task == NULL)
    {
        DisplayLog(LVL_CRIT, FSSCAN_TAG, "CRITICAL ERROR: task creation failed");
        return -1;
    }

    p_task->parent_task = parent;
    rh_strncpy(p_task->path, childpath, RBH_PATH_MAX);

    /* set parent id */
    if ((rc = get_dirid(childpath, inode, &p_task->dir_id)))
        goto out_free;

    p_task->dir_md = *inode;
    p_task->depth = parent->depth + 1;
    p_task->task_finished = FALSE;

    /* add the task to the parent's subtask list */
    AddChildTask(parent, p_task);

    /* insert task to the stack */
    InsertTask_to_Stack(&tasks_stack, p_task);
    return 0;

out_free:
    FreeTask(p_task);
    return rc;
}

static int stat_entry(const char *path, const char *name, int parentfd, struct stat *inode)
{
#ifndef _NO_AT_FUNC
    if (parentfd != -1) /* if called for a directory between root and partial_scan_root */
    {
        if (fstatat(parentfd, name, inode, AT_SYMLINK_NOFOLLOW) == -1)
            return -errno;
    }
    else
#endif
#if defined( _LUSTRE ) && defined( _MDS_STAT_SUPPORT )
    if (is_lustre_fs && global_config.direct_mds_stat)
    {
        int rc;
        rc = lustre_mds_stat(path, parentfd, inode);
        if (!rc)
            /* device id is not the one seen by client: change it */
            inode->st_dev = fsdev;
        return rc;
    }
    else
#endif
    if (lstat(path, inode) == -1)
        return -errno;

    return 0;
}

/* process a filesystem entry */
static int process_one_entry(thread_scan_info_t *p_info,
                             robinhood_task_t *p_task,
                             char *entry_name, int parentfd)
{
    char           entry_path[RBH_PATH_MAX];
    struct stat    inode;
    int            rc = 0;
    int            no_md = 0;

    /* build absolute path */
    snprintf(entry_path, RBH_PATH_MAX, "%s/%s", p_task->path, entry_name);

    /* retrieve information about the entry (to know if it's a directory or something else) */
    rc = stat_entry(entry_path, entry_name, parentfd, &inode);
    if (rc)
    {
#ifdef _LUSTRE
        if (is_lustre_fs && (rc == -ESHUTDOWN))
        {
            /* File can't be stat because it is on a disconnected OST.
             * Still push it to the pipeline, to avoid loosing valid info
             * in the DB.
             */
            DisplayLog(LVL_EVENT, FSSCAN_TAG, "Entry %s is on inactive OST or MDT. "
                       "Cannot get its attributes.", entry_path);
            no_md = 1;
            goto push;
        }
#endif
        DisplayLog(LVL_MAJOR, FSSCAN_TAG,
                   "failed to stat %s (%s): entry ignored",
                   entry_path, strerror(-rc));
        return rc;
    }

    /* Test if entry or directory is ignored */
    if (ignore_entry(entry_path, entry_name, p_task->depth, &inode))
    {
        DisplayLog(LVL_DEBUG, FSSCAN_TAG, "%s matches an 'ignore' rule. Skipped.", entry_path);
        return 0;
    }

    if (check_entry_dev(inode.st_dev, &fsdev, entry_path, FALSE))
        return 0; /* not considered as an error */

    /* Push all entries except dirs to the pipeline.
     * Note: directories are pushed in Thr_scan(), after the closedir() call.
     */
    if (S_ISDIR(inode.st_mode))
    {
        rc = create_child_task(entry_path, &inode, p_task);
        if (rc)
            return rc;
    }
    else
    {
        entry_proc_op_t * op;
push:

        op = EntryProcessor_Get( );
        if (!op) {
            DisplayLog( LVL_CRIT, FSSCAN_TAG, "CRITICAL ERROR: Failed to allocate a new op" );
            return -1;
        }

#ifdef _HAVE_FID
        op->pipeline_stage = entry_proc_descr.GET_ID;
#else
        op->pipeline_stage = entry_proc_descr.GET_INFO_DB;
#endif
        ATTR_MASK_INIT( &op->fs_attrs );

        ATTR_MASK_SET( &op->fs_attrs, parent_id );
        ATTR( &op->fs_attrs, parent_id) = p_task->dir_id;

        ATTR_MASK_SET( &op->fs_attrs, name );
        strcpy( ATTR( &op->fs_attrs, name ), entry_name );

        ATTR_MASK_SET( &op->fs_attrs, fullpath );
        strcpy( ATTR( &op->fs_attrs, fullpath ), entry_path );

#ifdef ATTR_INDEX_invalid
        ATTR_MASK_SET( &op->fs_attrs, invalid );
        ATTR( &op->fs_attrs, invalid ) = FALSE;
#endif

        ATTR_MASK_SET( &op->fs_attrs, depth );
        ATTR( &op->fs_attrs, depth ) = p_task->depth;  /* depth(/<mntpoint>/toto) = 0 */

        if (!no_md)
        {
#if defined( _LUSTRE ) && defined( _MDS_STAT_SUPPORT )
            PosixStat2EntryAttr( &inode, &op->fs_attrs, !(is_lustre_fs && global_config.direct_mds_stat) );
#else
            PosixStat2EntryAttr(&inode, &op->fs_attrs, TRUE);
#endif
            /* set update time  */
            ATTR_MASK_SET( &op->fs_attrs, md_update );
            ATTR( &op->fs_attrs, md_update ) = time( NULL );
        }
        else
        {
            /* must still set it to avoid the entry to be impacted by scan final GC */
            ATTR_MASK_SET(&op->fs_attrs, md_update);
            ATTR(&op->fs_attrs, md_update) = time(NULL);
        }
        ATTR_MASK_SET( &op->fs_attrs, path_update );
        ATTR( &op->fs_attrs, path_update ) = time( NULL );

        /* Set entry id */
#ifndef _HAVE_FID
        if (!no_md)
        {
            op->entry_id.inode = inode.st_ino;
            op->entry_id.fs_key = get_fskey();
            op->entry_id.validator = inode.st_ctime;
            op->entry_id_is_set = TRUE;
        }
        else
            op->entry_id_is_set = FALSE;
#else
        op->entry_id_is_set = FALSE;
#ifndef _NO_AT_FUNC
        /* get fid from fd, using openat on parent fd */
        int fd = openat_noatime(parentfd, entry_name, FALSE);
        if (fd < 0)
            DisplayLog(LVL_DEBUG, FSSCAN_TAG, "openat failed on %d/%s: %s", parentfd, entry_name, strerror(errno));
        else
        {
            rc = Lustre_GetFidByFd(fd, &op->entry_id);
            if (rc)
                DisplayLog(LVL_DEBUG, FSSCAN_TAG, "fd2fid failed on %d/%s: %s", parentfd, entry_name, strerror(errno));
            else
            {
                op->entry_id_is_set = TRUE;
                op->pipeline_stage = entry_proc_descr.GET_INFO_DB;
            }
            close(fd);
        }
#endif
#endif

        op->extra_info_is_set = FALSE;

#ifdef _LUSTRE
#ifdef HAVE_LLAPI_FSWAP_LAYOUTS
        /** since Lustre2.4 release, entry striping can change (have_llapi_fswap_layouts)
         * so scanning must update file stripe information.
         */
        if (no_md || S_ISREG(inode.st_mode))
#else
        if ((no_md || S_ISREG(inode.st_mode)) && is_first_scan)
#endif
        {
            /* Fetch the stripes information now. This is faster than
             * doing it later in the pipeline. However if that fails now,
             * the pipeline will retry.
             * Do it only for initial scan, as most of the stripes information
             * is already known for next scans.
             */
#ifndef _NO_AT_FUNC
            /* have a dir fd */
            rc = File_GetStripeByDirFd( parentfd, entry_name,
                                        &ATTR( &op->fs_attrs, stripe_info ),
                                        &ATTR( &op->fs_attrs, stripe_items ) );
#else
            rc = File_GetStripeByPath( entry_path,
                                       &ATTR( &op->fs_attrs, stripe_info ),
                                       &ATTR( &op->fs_attrs, stripe_items ) );
#endif
            if (rc)
            {
                ATTR_MASK_UNSET( &op->fs_attrs, stripe_info );
                ATTR_MASK_UNSET( &op->fs_attrs, stripe_items );
            }
            else
            {
                ATTR_MASK_SET( &op->fs_attrs, stripe_info );
                ATTR_MASK_SET( &op->fs_attrs, stripe_items );
            }
        }
#endif

#ifndef _BENCH_SCAN
        /* Push entry to the pipeline */
        EntryProcessor_Push(op);
#else
        EntryProcessor_Release(op);
#endif

    }

    return 0;
}

/* directory specific types and accessors */
#ifndef _NO_AT_FUNC
#define GETDENTS_BUF_SZ 4096
#define DIR_T int
#define DIR_FD(_d) (_d)
#define DIR_ERR(_d) ((_d) < 0)
#define OPENDIR_STR "open"
#else
#define DIR_T DIR*
#define DIR_FD(_d) dirfd(_d)
#define DIR_ERR(_d) ((_d) == NULL)
#define OPENDIR_STR "opendir"
#endif

static inline DIR_T dir_open(const char *path)
{
#ifndef _NO_AT_FUNC
    return open_noatime(path, TRUE);
#else
    return opendir(path);
#endif
}

static int process_one_dir(robinhood_task_t *p_task,
                           thread_scan_info_t *p_info,
                           unsigned int *nb_entries,
                           unsigned int *nb_errors)
{
    DIR_T   dirp;
#ifndef _NO_AT_FUNC
    char              dirent_buf[GETDENTS_BUF_SZ];
    struct dirent64  *direntry = NULL;
#else
    struct dirent  direntry;
    struct dirent *cookie_rep;
#endif
    int     rc = 0;

    (*nb_entries) = 0;

    /* hearbeat before opendir */
    p_info->last_action = time(NULL);

    dirp = dir_open(p_task->path);
    if (DIR_ERR(dirp))
    {
        rc = -errno;
        DisplayLog(LVL_CRIT, FSSCAN_TAG,
                   OPENDIR_STR" failed on %s (%s)",
                   p_task->path, strerror(-rc));
        (*nb_errors)++;
        check_dir_error(rc);

        return rc;
    }

    /* hearbeat before first readdir */
    p_info->last_action = time(NULL);

#ifndef _NO_AT_FUNC
    /* scan directory entries by chunk of 4k */
    direntry = (struct dirent64*)dirent_buf;
    while ((rc = syscall(SYS_getdents64, dirp, direntry, GETDENTS_BUF_SZ)) > 0)
    {
       off_t bytepos;
       struct dirent64 *dp;

        /* notify current activity */
        p_info->last_action = time(NULL);

       for(bytepos = 0; bytepos < rc;)
       {
          dp = (struct dirent64 *)(dirent_buf + bytepos);
          bytepos += dp->d_reclen;

          /* break ASAP if requested */
          if (p_info->force_stop)
          {
              DisplayLog(LVL_EVENT, FSSCAN_TAG, "Stop requested: "
                         "cancelling directory scan operation "
                         "(in '%s')", p_task->path);
              return -ECANCELED;
          }

          if (!strcmp(dp->d_name, ".") || !strcmp(dp->d_name, ".."))
                continue;

          (*nb_entries)++;

          /* Handle filesystem entry. */
          if (process_one_entry(p_info, p_task, dp->d_name, DIR_FD(dirp)))
              (*nb_errors)++;
        }
    }
    /* rc == 0 => end of dir */
    if (rc < 0)
    {
        rc = errno;
        DisplayLog(LVL_CRIT, FSSCAN_TAG, "ERROR reading directory %s (%s)",
                   p_task->path, strerror(rc));
        (*nb_errors)++;
    }
    if (rc != EBADF)
        close(dirp);
#else
    /* read entries one by one */
    while (1)
    {
        rc = readdir_r(dirp, &direntry, &cookie_rep);

        /* notify current activity (for watchdog) */
        p_info->last_action = time(NULL);

        if ((rc == 0) && (cookie_rep == NULL))
            /* end of directory */
            break;
        else if (p_info->force_stop)
        {
            DisplayLog(LVL_EVENT, FSSCAN_TAG,"Stop requested: "
                       "cancelling directory scan operation (in '%s')",
                       p_task->path);
            return -ECANCELED;
        }
        else if (rc != 0)
        {
            DisplayLog(LVL_CRIT, FSSCAN_TAG, "ERROR reading directory %s (%s)",
                       p_task->path, strerror(rc));
            (*nb_errors)++;
            break;
        }

        if (!strcmp( direntry.d_name, ".") || !strcmp(direntry.d_name, ".."))
            continue;

        (*nb_entries)++;

#ifdef SIMUL_HANGS
        /* simulate a hang */
        sleep(20 * p_task->depth);
#endif

        /* Handle filesystem entry. */
        if (process_one_entry(p_info, p_task, direntry.d_name, dirfd(dirp)))
            (*nb_errors)++;

    } /* end of dir */

    if (rc != EBADF)
        closedir(dirp);
#endif
    return rc;
}

static int process_one_task(robinhood_task_t *p_task,
                            thread_scan_info_t *p_info,
                            unsigned int *nb_entries,
                            unsigned int *nb_errors)
{
    int rc;
#ifdef _BENCH_DB
    /* to map entry_id_t to an integer  we can increment */
    struct id_map { uint64_t high; uint64_t low; } * volatile fakeid;
    /* level1 tasks: insert 100k entries with root entry id + N. */
    if (p_task->depth > 1)
        return 0;
#endif

    /* if this is the root task, check that the filesystem is still mounted */
    if (p_task->parent_task == NULL)
    {
        /* retrieve filesystem device id */
        if (stat(p_task->path, &p_task->dir_md))
        {
            DisplayLog(LVL_CRIT, FSSCAN_TAG,
                       "stat failed on %s (%s)", p_task->path, strerror(errno));
            DisplayLog(LVL_CRIT, FSSCAN_TAG, "Error accessing filesystem: exiting");
            Exit(1);
        }
        if (check_entry_dev(p_task->dir_md.st_dev, &fsdev, p_task->path, TRUE))
            p_task->dir_md.st_dev = fsdev; /* just updated */

        rc = get_dirid(p_task->path, &p_task->dir_md, &p_task->dir_id);
        if (rc)
        {
            (*nb_errors)++;
            return rc;
        }

        /* test lock before starting scan */
        TestLockFile(&p_info->last_action);
    }

    /* As long as the current task path is (strictly)
     * upper than partial scan root: just lookup, no readdir */ 
    if (partial_scan_root && (strlen(p_task->path) <
                              strlen(partial_scan_root)))
    {
        char name[RBH_NAME_MAX+1];
        const char *next_name, *next_slash;

        /* check path */
        if (strncmp(p_task->path, partial_scan_root, strlen(p_task->path)))
        {
            DisplayLog(LVL_CRIT, FSSCAN_TAG, "ERROR: %s is supposed to be under %s",
                       partial_scan_root, p_task->path);
            (*nb_errors)++;
            return -EINVAL;
        }

        next_name = partial_scan_root + strlen(p_task->path);
        while (*next_name == '/')
            next_name++;
        next_slash = strchr(next_name, '/');
        if (next_slash)
        {
            ptrdiff_t len = next_slash - next_name; /* length without final '\0' */
            strncpy(name, next_name, len);
            name[len] = '\0';
        }
        else
            strcpy(name, next_name);

        DisplayLog(LVL_FULL, FSSCAN_TAG, "Partial scan: processing '%s' in %s", name, p_task->path);

        rc = process_one_entry(p_info, p_task, name, -1);
        if (rc)
        {
            (*nb_errors)++;
            return rc;
        }
    }
#ifndef _BENCH_DB
    else
#else
    else if (p_task->depth == 0)
#endif
    {
        /* read the directory and process each entry */
        rc = process_one_dir(p_task, p_info, nb_entries, nb_errors);
        if (rc)
            return rc;
    }

#ifdef _BENCH_DB
    int i;
#endif

    if (p_task->depth > 0)
#ifdef _BENCH_DB
    for (i = 1; i < 100000 && !p_info->force_stop; i++)
#endif
    {
        /* Fill dir info and push it to the pileline for checking alerts on it,
         * and possibly purge it if it is empty for a long time.
         */
        entry_proc_op_t * op;

        op = EntryProcessor_Get();
        if (!op) {
            DisplayLog(LVL_CRIT, FSSCAN_TAG, "CRITICAL ERROR: Failed to allocate a new op");
            return -ENOMEM;
        }

        ATTR_MASK_INIT(&op->fs_attrs);

        /* set entry ID */
        op->entry_id = p_task->dir_id;
#ifdef _BENCH_DB
        /* add i to the entry id */
        fakeid = (struct id_map*)&op->entry_id;
        fakeid->high += i;
#endif
        op->entry_id_is_set = TRUE;

        /* Id already known */
        op->pipeline_stage = entry_proc_descr.GET_INFO_DB;

#ifndef _BENCH_DB
        if (p_task->parent_task)
        {
            ATTR_MASK_SET(&op->fs_attrs, parent_id);
            ATTR(&op->fs_attrs, parent_id) = p_task->parent_task->dir_id;
        }
#else
        ATTR_MASK_SET(&op->fs_attrs, parent_id);
        ATTR(&op->fs_attrs, parent_id) = p_task->dir_id;
#endif

        ATTR_MASK_SET(&op->fs_attrs, name);
        strcpy(ATTR(&op->fs_attrs, name), basename(p_task->path));
#ifdef _BENCH_DB
        sprintf(ATTR(&op->fs_attrs, name) + strlen(ATTR(&op->fs_attrs, name)), "%d", i);
#endif

        ATTR_MASK_SET(&op->fs_attrs, fullpath);
        strcpy(ATTR(&op->fs_attrs, fullpath), p_task->path);
#ifdef _BENCH_DB
        sprintf(ATTR(&op->fs_attrs, fullpath) + strlen(ATTR(&op->fs_attrs, fullpath)), "%d", i);
#endif

#ifdef ATTR_INDEX_invalid
        ATTR_MASK_SET(&op->fs_attrs, invalid);
        ATTR(&op->fs_attrs, invalid) = FALSE;
#endif

        ATTR_MASK_SET(&op->fs_attrs, depth);
        ATTR(&op->fs_attrs, depth) = p_task->depth - 1;  /* depth(/tmp/toto) = 0 */

        ATTR_MASK_SET(&op->fs_attrs, dircount);
        ATTR(&op->fs_attrs, dircount) = *nb_entries;

#ifndef _BENCH_PIPELINE
#if defined( _LUSTRE ) && defined( _MDS_STAT_SUPPORT )
        PosixStat2EntryAttr(&p_task->dir_md, &op->fs_attrs, !(is_lustre_fs && global_config.direct_mds_stat));
#else
        PosixStat2EntryAttr(&p_task->dir_md, &op->fs_attrs, TRUE);
#endif
#endif

#ifdef _BENCH_DB
        /* generate cyclic owner, group, type, size, ... */
        unsigned int u = (i + 17) % 137;
        sprintf(ATTR(&op->fs_attrs, owner), "user%u", u);
        sprintf(ATTR(&op->fs_attrs, gr_name), "group%u", u/8); /* 8 user per group */
        switch (i % 2)
        {
            case 0: strcpy(ATTR(&op->fs_attrs, type), STR_TYPE_DIR); break;
            case 1: strcpy(ATTR(&op->fs_attrs, type), STR_TYPE_FILE); break;
        }
        ATTR(&op->fs_attrs, size) = ((i % 311) * 1493);

        p_info->entries_handled ++;
#endif
        /* set update time  */
        ATTR_MASK_SET(&op->fs_attrs, md_update);
        ATTR_MASK_SET(&op->fs_attrs, path_update);
        ATTR(&op->fs_attrs, md_update) = ATTR(&op->fs_attrs, path_update)
            = time(NULL);

        op->extra_info_is_set = FALSE;

#ifndef _BENCH_SCAN
        /* Push directory to the pipeline */
        EntryProcessor_Push(op);
#else
        EntryProcessor_Release(op);
#endif
    }
    return 0;
}




/**
 * Thr_scan :
 * main routine for handling tasks.
 */
static void   *Thr_scan(void *arg_thread)
{
    robinhood_task_t *p_task;
    int            rc;

    struct timeval start_dir;
    struct timeval end_dir;
    struct timeval diff;

    thread_scan_info_t *p_info = (thread_scan_info_t *)arg_thread;

    unsigned int   nb_entries = 0;
    unsigned int   nb_errors = 0;

    /* Initialize buddy management */
#ifdef _BUDDY_MALLOC
    if (BuddyInit(&buddy_config))
    {
        DisplayLog(LVL_CRIT, FSSCAN_TAG, "Error Initializing Memory Management");
        Exit(1);
    }
#endif

    while (!p_info->force_stop)
    {
        int task_rc;

        DisplayLog(LVL_FULL, FSSCAN_TAG, "ThrScan-%d: Waiting for a task", p_info->index);

        /* take a task from queue */
        p_task = GetTask_from_Stack(&tasks_stack);

        /* skip it if the thread was requested to stop */
        if (p_info->force_stop)
            break;

        /* ERROR if NULL */
        if (p_task == NULL)
        {
            DisplayLog(LVL_CRIT, FSSCAN_TAG, "CRITICAL ERROR: GetTask_from_Stack returned NULL");
            Exit(1);
        }

        /* update thread info */
        p_info->current_task = p_task;
        p_info->last_action = time(NULL);

        /* initialize error counters for current task */
        nb_entries = 0;
        nb_errors = 0;

        DisplayLog(LVL_FULL, FSSCAN_TAG,
                   "ThrScan-%d: Processing %s (depth %u)",
                   p_info->index, p_task->path, p_task->depth);

        /* measure task processing time */
        gettimeofday(&start_dir, NULL);

        task_rc = process_one_task(p_task, p_info, &nb_entries, &nb_errors);

        gettimeofday( &end_dir, NULL );
        timersub( &end_dir, &start_dir, &diff );

        /* update thread statistics */
        timeradd( &diff, &p_info->time_consumed, &p_info->time_consumed );
        p_info->entries_handled += nb_entries;
        p_info->entries_errors += nb_errors;

        /* make an average on directory entries */
        if (nb_entries > 0)
        {
            unsigned int rest;
            p_info->last_processing_time.tv_sec = diff.tv_sec/nb_entries;
            rest = diff.tv_sec - (p_info->last_processing_time.tv_sec * nb_entries) ;
            p_info->last_processing_time.tv_usec = ((1000000 * rest ) + diff.tv_usec)/nb_entries;
        }

        /* terminate processing of current task */
        rc = RecursiveTaskTermination(p_info, p_task, (task_rc == 0)
                                                      && !p_info->force_stop);
        if (rc)
        {
            DisplayLog(LVL_CRIT, FSSCAN_TAG,
                       "CRITICAL ERROR: RecursiveTaskTermination returned %d", rc);
            Exit(1);
        }
    }

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
 *   0 : initialization successful
 *   -1 : unexpected error at initialization.
 *   EINVAL : a parameter from the config file is invalid.
 */
int Robinhood_InitScanModule( void )
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

    fsdev = get_fsdev();

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
void Robinhood_StopScanModule( void )
{
    unsigned int   i;
    int            running = 0;
    char           timestamp[128];
    lmgr_t  lmgr;

    P( lock_scan );
    /* is a scan really running ? */
    if ( root_task != NULL )
    {
        running = 1;
    }
    V( lock_scan );

    /* terminate scan threads */
    for ( i = 0; i < fs_scan_config.nb_threads_scan; i++ )
    {
        thread_list[i].force_stop = TRUE;
    }

    DisplayLog( LVL_EVENT, FSSCAN_TAG, "Stop request has been sent to all scan threads" );

    /* if there are still threads doing something, wait for them */
    if ( !all_threads_idle() )
        wait_scan_finished();

    /* update scan status in db */
    if ( running )
    {
        if ( ListMgr_InitAccess( &lmgr ) == DB_SUCCESS ) {
            sprintf( timestamp, "%lu", ( unsigned long ) time ( NULL ) );
            ListMgr_SetVar( &lmgr, LAST_SCAN_END_TIME, timestamp );
            ListMgr_SetVar( &lmgr, LAST_SCAN_STATUS, SCAN_STATUS_ABORTED);
            ListMgr_CloseAccess(&lmgr);
        } else {
            DisplayLog(LVL_MAJOR, FSSCAN_TAG, "WARNING: not able to update scan stats");
        }
    }

}

/* Start a scan of the filesystem.
 * This creates a root task and push it to the stack of tasks.
 * @param partial_root NULL for full scan; subdir path for partial scan
 * @retval EBUSY if a scan is already running.
 */
static int StartScan( void )
{
    robinhood_task_t *p_parent_task;
    char              timestamp[128];
    char              value[128];
    lmgr_t  lmgr;
    int no_db = 0;
    uint64_t    count = 0LL;
    int rc;

    /* Lock scanning status */
    P( lock_scan );

    /* is a scan already running ? */
    if ( root_task != NULL )
    {
        V( lock_scan );
        DisplayLog( LVL_MAJOR, FSSCAN_TAG,
                    "An scan is already running on %s",
                    partial_scan_root?partial_scan_root:global_config.fs_path );
        return EBUSY;
    }

    /* create a root task */
    p_parent_task = CreateTask(  );

    if ( p_parent_task == NULL )
    {
        V( lock_scan );
        DisplayLog( LVL_CRIT, FSSCAN_TAG,
                    "ERROR creating scan task for %s",
                    partial_scan_root?partial_scan_root:global_config.fs_path );
        return -1;
    }

    if (partial_scan_root)
    {
        /* check that partial_root is under FS root */
        if (strncmp(global_config.fs_path, partial_scan_root, strlen(global_config.fs_path)))
        {
            V(lock_scan);
            DisplayLog( LVL_CRIT, FSSCAN_TAG, "ERROR scan root %s is not under fs root %s",
                        partial_scan_root, global_config.fs_path );
            return -1;
        }
    }

    /* always start at the root to get info about parent dirs */
    strcpy(p_parent_task->path, global_config.fs_path);
    p_parent_task->depth = 0;
    p_parent_task->task_finished = FALSE;

    /* set the mother task, and remember start time */
    root_task = p_parent_task;
    scan_start_time = time( NULL );
    gettimeofday( &accurate_start_time, NULL );

    if ( ListMgr_InitAccess( &lmgr ) != DB_SUCCESS ) {
        no_db = 1;
        DisplayLog( LVL_MAJOR, FSSCAN_TAG, "WARNING: won't be able to update scan stats");
    }

    if (!no_db)
    {
        /* archive previous scan start/end time */
        if ( ListMgr_GetVar( &lmgr, LAST_SCAN_START_TIME, timestamp ) == DB_SUCCESS )
             ListMgr_SetVar( &lmgr, PREV_SCAN_START_TIME, timestamp );
        if ( ListMgr_GetVar( &lmgr, LAST_SCAN_END_TIME, timestamp ) == DB_SUCCESS )
             ListMgr_SetVar( &lmgr, PREV_SCAN_END_TIME, timestamp );

        /* store current scan start time and status in db */
        sprintf( timestamp, "%lu", ( unsigned long ) scan_start_time );
        ListMgr_SetVar( &lmgr, LAST_SCAN_START_TIME, timestamp );
        ListMgr_SetVar( &lmgr, LAST_SCAN_LAST_ACTION_TIME, timestamp );
        ListMgr_SetVar( &lmgr, LAST_SCAN_STATUS, SCAN_STATUS_RUNNING);
        /* store the number of scanning threads */
        sprintf( value, "%i", fs_scan_config.nb_threads_scan );
        ListMgr_SetVar( &lmgr, LAST_SCAN_NB_THREADS, value );

        /* check if it is the first scan (avoid RM_OLD_ENTRIES in this case) */
        is_first_scan = FALSE;
        rc = ListMgr_EntryCount(&lmgr, &count);

        if ((rc == DB_SUCCESS) && (count == 0)) {
            is_first_scan = TRUE;
            DisplayLog(LVL_EVENT, FSSCAN_TAG, "Notice: this is the first scan (DB is empty)");
        }
        else if (rc)
             DisplayLog(LVL_MAJOR, FSSCAN_TAG, "Failed to retrieve entry count from DB: error %d", rc);
        else
            DisplayLog(LVL_DEBUG, FSSCAN_TAG, "%"PRIu64" entries in DB before starting the scan", count);

        ListMgr_CloseAccess(&lmgr);
    }

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
static void UpdateMaxUsage( void )
{
    char           tmpval[1024];
    double         val;
    lmgr_t  lmgr;

    if ( ListMgr_InitAccess( &lmgr ) != DB_SUCCESS ) {
        DisplayLog( LVL_MAJOR, FSSCAN_TAG, "WARNING: can't update usage stats");
        return;
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

    ListMgr_CloseAccess(&lmgr);
}


/**
 * Check thread's activity or start a scan if it's time.
 */
int Robinhood_CheckScanDeadlines( void )
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

        /* starting a new scan, if it's time */

        DisplayLog( LVL_MAJOR, FSSCAN_TAG,
                    "Starting scan of %s (current scan interval is %s)",
                    partial_scan_root?partial_scan_root:global_config.fs_path, tmp_buff );

        st = StartScan();

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

        DisplayLog( LVL_MAJOR, FSSCAN_TAG, "Starting scan of %s",
                    partial_scan_root?partial_scan_root:global_config.fs_path );

        st = StartScan();

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
                    DisplayLog( LVL_MAJOR, FSSCAN_TAG,
                                "Hang of thread #%d while it was scanning %s (inactive for %ld sec)",
                                i, thread_list[i].current_task->path,
                                time( NULL ) - thread_list[i].last_action );
                    RaiseAlert("FS scan is blocked", "A thread has been inactive for %ld sec\n"
                               "while scanning directory %s",
                               time(NULL) - thread_list[i].last_action,
                               thread_list[i].current_task->path);

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
 */
void Robinhood_StatsScan( robinhood_fsscan_stat_t * p_stats )
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
                nb_done++;
            }
            p_stats->error_count += thread_list[i].entries_errors;
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

}
