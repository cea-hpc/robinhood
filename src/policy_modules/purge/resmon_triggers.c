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

#define RESMON_TAG "ResMonitor"

#include "RobinhoodConfig.h"
#include "RobinhoodMisc.h"
#include "resource_monitor.h"
#include "resmon_purge.h"
#include "queue.h"
#include "Memory.h"
#include "xplatform_print.h"
#include <errno.h>
#include <pthread.h>
#include <unistd.h>

#ifdef __FreeBSD__
#include <sys/param.h>
#include <sys/mount.h>
#else /* Linux */
#include <sys/vfs.h>
#endif

/* ------------ Types and global variables ------------ */

/* Module configuration */
resource_monitor_config_t resmon_config = {0};

/* Running mode (default is daemon) */
static resmon_opt_t module_args =
{
    .mode = RESMON_DAEMON,
    .flags = 0,
    .ost_index = 0,
    .target_usage = 0.0
};

typedef enum
{
    TRIG_NOT_CHECKED,                            /* not checked yet */
    TRIG_BEING_CHECKED,                          /* currently beeing checked */
    TRIG_PURGE_RUNNING,                          /* purge running for this trigger */
    TRIG_OK,                                     /* no purge is needed */
    TRIG_NO_LIST,                                /* no list available */
    TRIG_NOT_ENOUGH,                             /* not enough candidates */
    TRIG_CHECK_ERROR,                            /* Misc Error */
    TRIG_ABORTED,                                /* aborted purge */
    TRIG_UNSUPPORTED                             /* Trigger not supported in this mode */
} trigger_status_t;

/* Info about each trigger */
typedef struct trigger_status__
{
    time_t         last_check;                   /* the last time this trigger was tested */
    trigger_status_t status;

    /* @TODO more stats about tiggers */

    /* its usage, the last time it was checked for OST and global FS triggers */
    double         last_usage;
    /* for inode based thresholds */
    unsigned long long last_count;
} trigger_info_t;

#define IS_COUNT_TRIGGER( _index_ ) (resmon_config.trigger_list[(_index_)].hw_type == COUNT_THRESHOLD)
#define ALERT_LW( _index_ ) (resmon_config.trigger_list[(_index_)].alert_lw)

static trigger_info_t *trigger_status_list = NULL;
static time_t  trigger_check_interval = 1;

static pthread_t trigger_check_thread_id = (pthread_t)-1;
static lmgr_t  lmgr;

static int terminate = FALSE;

static dev_t   fsdev = 0;

static void update_trigger_status( unsigned int i, trigger_status_t state )
{
    trigger_status_list[i].status = state;

    if ( state == TRIG_BEING_CHECKED )
        trigger_status_list[i].last_check = time( NULL );
}


/**
 * Function for checking that filesystem hasn't been unmounted
 */
static int CheckFSDevice( void )
{
    struct stat    root_md;

    /* retrieve device of filesystem, to compare it to initial device id */

    if (stat( global_config.fs_path, &root_md ) == -1)
    {
        DisplayLog( LVL_CRIT, RESMON_TAG, "Stat on '%s' failed! Error %d: %s",
                    global_config.fs_path, errno, strerror( errno ) );
        return FALSE;
    }
    if (root_md.st_dev != fsdev)
    {
        /* manage dev id change after umount/mount */
        DisplayLog( LVL_MAJOR, RESMON_TAG, "WARNING: Filesystem device id changed (old=%"PRI_DT", new=%"PRI_DT"): "
                    "checking if it has been remounted", fsdev, root_md.st_dev );
        if (ResetFS())
        {
            DisplayLog( LVL_CRIT, RESMON_TAG, "Filesystem was unmounted!!! EXITING!" );
            Exit( 1 );
        }
        /* update current fsdev */
        fsdev = get_fsdev();
    }

    return TRUE;
}

static unsigned long FSInfo2Blocs512( unsigned long nb_blocs, unsigned long sz_blocks )
{
    uint64_t       total_sz;
    unsigned long  nb_blocs_512;
    unsigned long  reste;

    /* evite les calculs inutiles */
    if ( sz_blocks == DEV_BSIZE )
        return nb_blocs;

    /* cas ou le nombre de blocs est different */
    total_sz = nb_blocs * sz_blocks;
    nb_blocs_512 = total_sz / DEV_BSIZE;
    reste = total_sz % DEV_BSIZE;

    if ( reste == 0 )
        return nb_blocs_512;
    else
        return nb_blocs_512 + 1;
}

/* ------------ Functions for checking each type of trigger ------------ */


static inline int statfs2usage(const struct statfs *p_statfs,
                               unsigned long long *used_vol,
                               double *used_pct,
                               unsigned long *total_blocks,
                               const char *storage_descr)
{
    /* check df consistency:
     * used = total - free = f_blocks - f_bfree
     * if used + available <= 0, there's something wrong
     */
    if (p_statfs->f_blocks + p_statfs->f_bavail - p_statfs->f_bfree <= 0)
    {
        DisplayLog(LVL_CRIT, RESMON_TAG,
                   "ERROR: statfs on %s returned inconsistent values!!!",
                   storage_descr);
        DisplayLog(LVL_CRIT, RESMON_TAG,
                   "Detail: blks=%" PRIu64 " avail=%" PRIu64 " free=%" PRIu64,
                   p_statfs->f_blocks, p_statfs->f_bavail, p_statfs->f_bfree);
        return -EIO;
    }

    /* number of blocks available to users */
    *total_blocks = (p_statfs->f_blocks + p_statfs->f_bavail
                        - p_statfs->f_bfree);
    *used_pct = 100.0 * ((double)p_statfs->f_blocks - (double)p_statfs->f_bfree)
                 / ((double)(*total_blocks));
    *used_vol = (p_statfs->f_blocks - p_statfs->f_bfree) * p_statfs->f_bsize;

    return 0;
}

/** function for checking thresholds (for global FS, single OST,...)
 * @return negative value on error
 *         0 on success (in this case, to_be_purged gives the number of blocks to be purged)
 */
static int check_thresholds( trigger_item_t * p_trigger, const char *storage_descr,
                             const struct statfs *p_statfs, unsigned long *to_be_purged_512,
                             double *p_used_pct )
{
    unsigned long  total_user_blocks, block_target;
    char           tmp1[128];
    char           tmp2[128];
    double         used_pct;
    unsigned long long used_vol;
    char           buff[1024];
    int rc;

    *to_be_purged_512 = 0;

    if ((rc = statfs2usage(p_statfs, &used_vol, &used_pct, &total_user_blocks,
                           storage_descr)))
        return rc;

    /* return last usage */
    if (p_used_pct)
        *p_used_pct = used_pct;

    /* is this a condition on volume or percentage ? */
    if ( p_trigger->hw_type == VOL_THRESHOLD )
    {
        FormatFileSize( tmp1, 128, used_vol );
        FormatFileSize( tmp2, 128, p_trigger->hw_volume );

        /* compare used volume to threshold */
        DisplayLog( LVL_VERB, RESMON_TAG, "%s usage: %s / high threshold: %s", storage_descr,
                    tmp1, tmp2 );

        if ( used_vol < p_trigger->hw_volume )
        {
            DisplayLog( LVL_DEBUG, RESMON_TAG,
                        "%s usage is under high threshold: nothing to do.", storage_descr );
            return 0;
        }
        else if ( p_trigger->alert_hw )
        {
           snprintf(buff, 1024, "High threshold reached on %s", storage_descr);
           RaiseAlert(buff, "%s\nspaced used: %s (%.2f%%), high threshold: %s",
                      buff, tmp1, used_pct, tmp2);
        }
        else
        {
            DisplayLog( LVL_MAJOR, RESMON_TAG, "High threshold reached on %s (%s): "
                        "spaced used: %s (%.2f%%), high threshold: %s",
                        storage_descr, global_config.fs_path, tmp1, used_pct, tmp2 );
        }
    }
    else if ( p_trigger->hw_type == PCT_THRESHOLD )
    {
        unsigned long  used_hw =
            ( unsigned long ) ( ( p_trigger->hw_percent * total_user_blocks ) / 100.0 );

        DisplayLog( LVL_VERB, RESMON_TAG,
                    "%s usage: %.2f%% (%"PRIu64" blocks) / high threshold: %.2f%% (%lu blocks)",
                    storage_descr, used_pct, p_statfs->f_blocks - p_statfs->f_bfree,
                    p_trigger->hw_percent, used_hw );

        if ( used_pct < p_trigger->hw_percent )
        {
            DisplayLog( LVL_DEBUG, RESMON_TAG,
                        "%s usage is under high threshold: nothing to do.", storage_descr );
            return 0;
        }
        else if ( p_trigger->alert_hw )
        {
           FormatFileSize( tmp1, 128, used_vol );
           snprintf(buff, 1024, "High threshold reached on %s", storage_descr);
           RaiseAlert( buff, "%s\nspaced used: %s (%.2f%%), high threshold: %.2f%%",
                       buff, tmp1, used_pct, p_trigger->hw_percent );
        }
        else
        {
            FormatFileSize( tmp1, 128, used_vol );
            DisplayLog( LVL_MAJOR, RESMON_TAG, "High threshold reached on %s (%s): "
                        "spaced used: %s (%.2f%%), high threshold: %.2f%%",
                        storage_descr, global_config.fs_path,  tmp1, used_pct, p_trigger->hw_percent );
        }
    }

    /* if we reach this point, high threshold is exceeded.
     * compute the amount of data for reaching low threshold */

    if ( p_trigger->lw_type == VOL_THRESHOLD )
    {
        block_target = ( p_trigger->lw_volume / p_statfs->f_bsize );
        if ( p_trigger->lw_volume % p_statfs->f_bsize )
            block_target++;
        DisplayLog( LVL_VERB, RESMON_TAG, "Target usage volume: %s (%lu blocks)",
                    FormatFileSize( tmp1, 128, p_trigger->lw_volume ), block_target );
    }
    else if ( p_trigger->lw_type == PCT_THRESHOLD )
    {
        block_target =
            ( unsigned long ) ( ( p_trigger->lw_percent * ( double ) total_user_blocks ) / 100.0 );
        DisplayLog( LVL_VERB, RESMON_TAG, "Target usage percentage: %.2f%% (%lu blocks)",
                    p_trigger->lw_percent, block_target );
    }
    else
    {
        DisplayLog( LVL_CRIT, RESMON_TAG, "Unexpected Low Threshold type %d. Trigger skipped.",
                    p_trigger->lw_type );
        return -EINVAL;
    }

    if ( p_statfs->f_blocks - p_statfs->f_bfree <= block_target )
    {
        DisplayLog( LVL_EVENT, RESMON_TAG, "Usage is already under low threshold. Do nothing." );
        return 0;
    }

    /* to be purged= blocks used - block_target */
    *to_be_purged_512 =
        FSInfo2Blocs512( ( p_statfs->f_blocks - p_statfs->f_bfree ) - block_target,
                         p_statfs->f_bsize );

    DisplayLog( LVL_EVENT, RESMON_TAG,
                "%lu blocks (x%u) must be purged on %s (used=%"PRIu64", target=%lu, block size=%zu)",
                *to_be_purged_512, DEV_BSIZE, storage_descr, p_statfs->f_blocks - p_statfs->f_bfree,
                block_target, p_statfs->f_bsize );

    return 0;

}

/** function for checking inode count thresholds
 * @return negative value on error
 *         0 on success (in this case, to_be_purged gives the number of entries to be purged)
 */
static int check_count_thresholds( trigger_item_t * p_trigger,
                                   const char *storage_descr,
                                   const struct statfs *p_statfs,
                                   unsigned long long * to_be_purged,
                                   unsigned long long * count_used )
{
    unsigned long long inode_used;

    *to_be_purged = 0;

    /* check df consistency: free < total */
    if ( p_statfs->f_ffree > p_statfs->f_files )
    {
        DisplayLog( LVL_CRIT, RESMON_TAG,
                    "ERROR: statfs on %s returned inconsistent values!!!",
                    storage_descr );
        DisplayLog( LVL_CRIT, RESMON_TAG, "Detail: total=%"PRIu64", free=%"PRIu64,
                    p_statfs->f_files , p_statfs->f_ffree );
        return -EIO;
    }

    /* number of inodes used */
    inode_used = p_statfs->f_files - p_statfs->f_ffree;

    /* return last usage */
    if ( count_used )
        *count_used = inode_used;

    /* check it is a condition on inode count */
    if (( p_trigger->hw_type != COUNT_THRESHOLD ) || ( p_trigger->lw_type != COUNT_THRESHOLD ))
    {
        DisplayLog( LVL_CRIT, RESMON_TAG, "Unexpected threshold types %d, %d. Trigger skipped.",
                    p_trigger->hw_type, p_trigger->lw_type );
        return -EINVAL;
    }

    DisplayLog( LVL_EVENT, RESMON_TAG, "%s entry count: %llu / high threshold: %llu",
                storage_descr, inode_used, p_trigger->hw_count );

    if ( inode_used < p_trigger->hw_count )
    {
        DisplayLog( LVL_VERB, RESMON_TAG,
                    "%s inode count is under high threshold: nothing to do.",
                    storage_descr );
        return 0;
    }
    else if ( p_trigger->alert_hw )
    {
       char buff[1024];
       snprintf(buff, 1024, "High threshold reached on %s", storage_descr);
       RaiseAlert( buff, "%s\nentry count: %Lu, high threshold: %Lu",
                   buff, inode_used, p_trigger->hw_count );
    }

    /* if we reach this point, high threshold is exceeded compute the amount of data for reaching low threshold */
    DisplayLog( LVL_VERB, RESMON_TAG, "Target entry count: %llu",
                p_trigger->lw_count );

    if ( inode_used < p_trigger->lw_count )
    {
        DisplayLog( LVL_EVENT, RESMON_TAG,
                    "Inode count is already under low threshold. Do nothing." );
        return 0;
    }

    *to_be_purged = inode_used - p_trigger->lw_count;

    DisplayLog( LVL_EVENT, RESMON_TAG,
                "%llu entries must be purged in %s (used=%Lu, target=%Lu)",
                *to_be_purged, storage_descr, inode_used, p_trigger->lw_count );
    return 0;
}



/** Periodic trigger */
static int check_periodic_trigger( unsigned trigger_index )
{
    purge_param_t  purge_param;
    int            rc;
    unsigned long long  blocks_purged, nbr_purged;
    char           timestamp[128];
    char           status_str[1024];

    if ( !CheckFSDevice(  ) )
        return ENXIO;

    purge_param.type = PURGE_ALL;
    purge_param.flags = module_args.flags;
    purge_param.nb_blocks = 0; /* unused, apply to all eligible files */
    purge_param.nb_inodes = 0; /* unused, apply to all eligible files */

    /* only purge if check_only is not set */
    if ( module_args.flags & FLAG_CHECK_ONLY )
    {
        ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, "only checked, no purge performed" );
        update_trigger_status( trigger_index, TRIG_OK );
        return 0;
    }
    DisplayLog( LVL_EVENT, RESMON_TAG,
                "Applying purge policy to eligible files in filesystem" );

    update_trigger_status( trigger_index, TRIG_PURGE_RUNNING );

    /* perform the purge */
    blocks_purged = nbr_purged = 0;
    rc = perform_purge( &lmgr, &purge_param, &blocks_purged, &nbr_purged );

    /* update last purge time and target */
    sprintf( timestamp, "%lu", ( unsigned long ) time( NULL ) );
    ListMgr_SetVar( &lmgr, LAST_PURGE_TIME, timestamp );
    ListMgr_SetVar( &lmgr, LAST_PURGE_TARGET, "Filesystem" );

    if ( rc == 0 )
    {
        update_trigger_status( trigger_index, TRIG_OK );
        DisplayLog( LVL_MAJOR, RESMON_TAG,
                    "Purge summary: %Lu entries, %Lu blocks purged",
                    nbr_purged, blocks_purged );

        snprintf(status_str, 1024, "Success (%Lu entries, %Lu blocks released)",
                 nbr_purged, blocks_purged );
        ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
    }
    else if ( rc == ENOENT )
    {
        update_trigger_status( trigger_index, TRIG_NO_LIST );
        DisplayLog( LVL_EVENT, RESMON_TAG,
                    "Could not perform purge on %s: no list is available.",
                    global_config.fs_path );

        snprintf(status_str, 1024, "No list available" );
        ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
    }
    else if ( rc == ECANCELED )
    {
        update_trigger_status( trigger_index, TRIG_ABORTED );
        DisplayLog( LVL_CRIT, RESMON_TAG,
                    "Purge aborted after releasing %Lu entries, %Lu blocks in %s.",
                    nbr_purged, blocks_purged, global_config.fs_path );

        snprintf(status_str, 1024, "Purge on %s aborted by admin (after releasing %Lu entries, %Lu blocks)",
                 global_config.fs_path, nbr_purged, blocks_purged);
        ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
    }
    else
    {
        update_trigger_status( trigger_index, TRIG_CHECK_ERROR );
        DisplayLog( LVL_CRIT, RESMON_TAG,
                    "Error %d performing purge on %s (%s). "
                    "%Lu entries purged, %Lu blocks.", rc,
                    global_config.fs_path, strerror(rc),
                    nbr_purged, blocks_purged );

        RaiseAlert("Error purging data", "Error %d performing purge in %s (%s).\n"
                    "%Lu entries purged, %Lu blocks.", rc,
                    global_config.fs_path, strerror(rc),
                    nbr_purged, blocks_purged );

        snprintf(status_str, 1024, "Error %d after releasing %Lu entries, %Lu blocks released in %s",
                 rc, nbr_purged, blocks_purged, global_config.fs_path );
        ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
    }

    FlushLogs(  );

    if ( (!terminate) && ( blocks_purged > 0 ) && ( resmon_config.post_purge_df_latency > 0 ) )
    {
        DisplayLog( LVL_EVENT, RESMON_TAG,
                    "Waiting %lus before performing 'df' on other storage units.",
                    resmon_config.post_purge_df_latency );
        rh_sleep( resmon_config.post_purge_df_latency );
    }

    return rc;
}


/** Check triggers on global filesystem usage */
static int check_global_trigger( unsigned trigger_index )
{
    struct statfs  statfs_glob;
    char           traverse_path[RBH_PATH_MAX];
    purge_param_t  purge_param;
    int            rc;
    unsigned long long  blks_purged, spec;
    char           timestamp[128];
    char           status_str[1024];
    char           buff[1024];

    snprintf( traverse_path, RBH_PATH_MAX, "%s/.", global_config.fs_path );

    if ( !CheckFSDevice(  ) )
        return ENXIO;

    update_trigger_status( trigger_index, TRIG_BEING_CHECKED );

    /* retrieve filesystem usage info */

    if ( statfs( traverse_path, &statfs_glob ) != 0 )
    {
        int            err = errno;
        DisplayLog( LVL_CRIT, RESMON_TAG, "Could not make a 'df' on %s: error %d: %s",
                    global_config.fs_path, err, strerror( err ) );
        update_trigger_status( trigger_index, TRIG_CHECK_ERROR );
        return err;
    }

    /* initialize counts */
    purge_param.nb_inodes = 0;
    purge_param.nb_blocks = 0;

    if ( IS_COUNT_TRIGGER( trigger_index ) )
        /* inode count */
        rc = check_count_thresholds( &( resmon_config.trigger_list[trigger_index] ),
                               "Filesystem",
                               &statfs_glob, &purge_param.nb_inodes,
                               &trigger_status_list[trigger_index].last_count );
    else
        /* volume threshold */
        rc = check_thresholds( &( resmon_config.trigger_list[trigger_index] ),
                               "Filesystem",
                               &statfs_glob, &purge_param.nb_blocks,
                               &trigger_status_list[trigger_index].last_usage );
    if ( rc )
    {
        update_trigger_status( trigger_index, TRIG_CHECK_ERROR );
        return rc;
    }
    else if ( (purge_param.nb_inodes == 0) && (purge_param.nb_blocks == 0) )
    {
        update_trigger_status( trigger_index, TRIG_OK );
        return 0;
    }

    purge_param.type = PURGE_FS;
    purge_param.flags = module_args.flags;

    /* only purge if check_only is not set */
    if ( module_args.flags & FLAG_CHECK_ONLY )
    {
        if ( IS_COUNT_TRIGGER( trigger_index ) )
            snprintf(status_str, 1024, "High threshold exceeded on inode count: "
                     "%Lu inodes used",
                     trigger_status_list[trigger_index].last_count );
        else
            snprintf(status_str, 1024, "High threshold exceeded on filesystem usage: "
                     "%.2f%% used", trigger_status_list[trigger_index].last_usage );

        ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
        update_trigger_status( trigger_index, TRIG_OK );

        return 0;
    }

    update_trigger_status( trigger_index, TRIG_PURGE_RUNNING );

    /* perform the purge */
    rc = perform_purge(&lmgr, &purge_param, &blks_purged, &spec);

    /* update last purge time and target */
    sprintf( timestamp, "%lu", ( unsigned long ) time( NULL ) );
    ListMgr_SetVar( &lmgr, LAST_PURGE_TIME, timestamp );
    ListMgr_SetVar( &lmgr, LAST_PURGE_TARGET, "Filesystem" );

    if ( IS_COUNT_TRIGGER(trigger_index) )
    {
        if ( rc == 0 )
        {
            DisplayLog(LVL_MAJOR, RESMON_TAG, "Filesystem purge summary: "
                       "%Lu entries purged (%Lu blocks)/%Lu needed in %s",
                       spec, blks_purged, purge_param.nb_inodes,
                       global_config.fs_path);
        }

        if ( spec < purge_param.nb_inodes )
        {
            if ( rc == ENOENT )
            {
                update_trigger_status( trigger_index, TRIG_NO_LIST );
                DisplayLog( LVL_EVENT, RESMON_TAG,
                            "Could not purge %Lu entries in %s: no list is available.",
                            purge_param.nb_inodes, global_config.fs_path );

                snprintf(status_str, 1024, "No list available (%Lu entries need to be purged)",
                         purge_param.nb_inodes );
                ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
            }
            else if ( rc == ECANCELED )
            {
                update_trigger_status(trigger_index, TRIG_ABORTED);
                DisplayLog(LVL_CRIT, RESMON_TAG,
                           "Purge aborted after releasing %Lu entries (%Lu blocks) in %s.",
                           spec, blks_purged, global_config.fs_path);

                snprintf(status_str, 1024, "Purge on %s aborted by admin (after releasing %Lu entries, %Lu blocks)",
                         global_config.fs_path, spec, blks_purged);
                ListMgr_SetVar(&lmgr, LAST_PURGE_STATUS, status_str);
            }
            else
            {
                update_trigger_status(trigger_index, TRIG_NOT_ENOUGH);
                DisplayLog(LVL_CRIT, RESMON_TAG,
                           "Could not purge %Lu entries in %s: not enough eligible files. Only %Lu entries released (%Lu blocks).",
                           purge_param.nb_inodes, global_config.fs_path, spec,
                           blks_purged);

                if ( ALERT_LW( trigger_index ) )
                {
                    sprintf(buff, "cannot purge filesystem %s", global_config.fs_path );
                    RaiseAlert(buff, "Could not purge %Lu entries in filesystem %s: "
                               "not enough eligible files. Only %Lu entries freed (%Lu blocks).",
                               purge_param.nb_inodes, global_config.fs_path, spec, blks_purged);
                }

                snprintf(status_str, 1024, "Not enough eligible files: %Lu/%Lu entries released",
                         spec, purge_param.nb_inodes);
                ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
            }

        }
        else
        {
            snprintf(status_str, 1024, "Success (%Lu/%Lu entries purged)",
                     spec, purge_param.nb_inodes);
            ListMgr_SetVar(&lmgr, LAST_PURGE_STATUS, status_str);

            update_trigger_status( trigger_index, TRIG_OK );
        }
    }
    else /* volume trigger */
    {
        if ( rc == 0 )
        {
            DisplayLog(LVL_MAJOR, RESMON_TAG, "Filesystem purge summary: "
                       "%Lu blocks purged/%lu blocks needed in %s",
                       blks_purged, purge_param.nb_blocks, global_config.fs_path);
        }

        if (blks_purged < purge_param.nb_blocks)
        {
            if ( rc == ENOENT )
            {
                update_trigger_status( trigger_index, TRIG_NO_LIST );
                DisplayLog( LVL_EVENT, RESMON_TAG,
                            "Could not purge %lu blocks in %s: no list is available.",
                            purge_param.nb_blocks, global_config.fs_path );

                snprintf(status_str, 1024, "No list available (%lu blocks need to be released)",
                         purge_param.nb_blocks );
                ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
            }
            else if ( rc == ECANCELED )
            {
                update_trigger_status(trigger_index, TRIG_ABORTED);
                DisplayLog(LVL_CRIT, RESMON_TAG,
                           "Purge aborted after releasing %Lu blocks in %s.",
                           blks_purged, global_config.fs_path);

                snprintf(status_str, 1024, "Purge on %s aborted by admin (after releasing %Lu blocks)",
                         global_config.fs_path, blks_purged);
                ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
            }
            else
            {
                update_trigger_status(trigger_index, TRIG_NOT_ENOUGH);
                DisplayLog(LVL_CRIT, RESMON_TAG,
                           "Could not purge %lu blocks in %s: not enough eligible files. Only %Lu blocks released.",
                           purge_param.nb_blocks, global_config.fs_path,
                           blks_purged);

                if ( ALERT_LW( trigger_index ) )
                {
                    sprintf(buff, "cannot purge filesystem %s", global_config.fs_path);
                    RaiseAlert(buff, "Could not purge %lu blocks in filesystem %s: "
                               "not enough eligible files. Only %Lu blocks freed.",
                               purge_param.nb_blocks, global_config.fs_path,
                               blks_purged);
                }

                snprintf(status_str, 1024, "Not enough eligible files (%Lu/%lu blocks released)",
                         blks_purged, purge_param.nb_blocks);
                ListMgr_SetVar(&lmgr, LAST_PURGE_STATUS, status_str);
            }

        }
        else
        {
            snprintf(status_str, 1024, "Success (%Lu/%lu blocks released)",
                     blks_purged, purge_param.nb_blocks);
            ListMgr_SetVar(&lmgr, LAST_PURGE_STATUS, status_str);
            update_trigger_status(trigger_index, TRIG_OK);
        }
    }

    FlushLogs(  );

    if ((!terminate) && (blks_purged > 0) &&
        (resmon_config.post_purge_df_latency > 0))
    {
        DisplayLog( LVL_EVENT, RESMON_TAG,
                    "Waiting %lus before performing 'df' on other storage units.",
                    resmon_config.post_purge_df_latency );
        rh_sleep( resmon_config.post_purge_df_latency );
    }

    return rc;

}

#ifdef _LUSTRE
struct ost_list {
     unsigned int *list;
     unsigned int count;
};
static inline void ost_list_init(struct ost_list *l)
{
    l->list = NULL;
    l->count = 0;
}
static inline int ost_list_add(struct ost_list *l, unsigned int ost_idx)
{
    l->list = MemRealloc(l->list, (l->count + 1) * sizeof(*l->list));
    if (!l->list)
       return ENOMEM;

    l->list[l->count] = ost_idx;
    l->count++;
    return 0;
}
static inline void ost_list_free(struct ost_list *l)
{
    if (l->list)
        MemFree(l->list);
    l->list = NULL;
    l->count = 0;
}
static inline int ost_list_is_member(struct ost_list *l,
                                     unsigned int test_member)
{
    int i;
    for (i = 0; i < l->count; i++)
    {
        if (l->list[i] == test_member)
            return TRUE;
    }
    return FALSE;
}

static int get_ost_max(struct statfs *df, trigger_value_type_t tr_type,
                       struct ost_list *excluded)
{
    int             ost_index,
                    rc = 0;
    int             ost_max = -1;
    unsigned long   ost_blocks;
    struct statfs   stat_max,
                    stat_tmp;
    double          max_pct = 0.0,
                    curr_pct;
    unsigned long long max_vol = 0LL,
                    curr_vol;
    char           ostname[128];

    for (ost_index = 0;; ost_index++)
    {
        if (ost_list_is_member(excluded, ost_index))
            continue;

        rc = Get_OST_usage(global_config.fs_path, ost_index, &stat_tmp);
        if (rc == ENODEV)     /* end of OST list */
            break;
        else if (rc != 0)
            /* continue with next OSTs */
            continue;

        snprintf(ostname, 128, "OST #%u", ost_index);
        if (statfs2usage(&stat_tmp, &curr_vol, &curr_pct, &ost_blocks, ostname))
            /* continue with next OSTs */
            continue;

        switch (tr_type)
        {
            case VOL_THRESHOLD:
                if (curr_vol > max_vol)
                {
                    ost_max = ost_index;
                    max_vol = curr_vol;
                    stat_max = stat_tmp;
                }
                break;
            case PCT_THRESHOLD:
                if (curr_pct > max_pct)
                {
                    ost_max = ost_index;
                    max_pct = curr_pct;
                    stat_max = stat_tmp;
                }
                break;
            default:
                RBH_BUG("Unexpected OST trigger type");
        }
    }

    if (ost_max == -1)
        /* none found */
        return -ENOENT;

    *df = stat_max;
    return ost_max;
}
#endif

/** Check triggers on OST usage */
static int check_ost_trigger( unsigned trigger_index )
{
    struct statfs  statfs_ost;
    purge_param_t  purge_param;
    int            ost_index;
    int            rc;
    char           ostname[128];
    char           timestamp[128];
    unsigned long long purged, spec;
    double         ost_usage = 0.0;
    char           status_str[1024];
    char           buff[1024];

    trigger_item_t *p_trigger = &resmon_config.trigger_list[trigger_index];

#ifndef _LUSTRE
    DisplayLog( LVL_CRIT, RESMON_TAG,
                "'OST_usage' trigger is not supported: you should rebuild the program with '_LUSTRE' flag enabled." );
    return ENOTSUP;
#else

    /* Only for lustre filesystems */
    if ( strcasecmp( global_config.fs_type, "lustre" ) )
    {
        DisplayLog( LVL_CRIT, RESMON_TAG,
                    "'OST_usage' trigger is only supported on Lustre filesystems: skipped." );

        update_trigger_status( trigger_index, TRIG_UNSUPPORTED );
        return ENOTSUP;
    }

    /* Check that filesystem is mounted before purging entries */
    if ( !CheckFSDevice(  ) )
        return ENXIO;


    /* get the OST with the max usage:
     * - for volume trigger: get the highest volume
     * - for a pct trigger: get the highest usage percentage.
     */
    struct ost_list excl_list;
    ost_list_init(&excl_list);

    /* initialize max usage */
    trigger_status_list[trigger_index].last_usage = 0.0;

    while(!terminate &&
          ((ost_index = get_ost_max(&statfs_ost, p_trigger->hw_type, &excl_list))
              != -ENOENT))
    {
        if (ost_index < 0)
        {
            update_trigger_status(trigger_index, TRIG_CHECK_ERROR);
            rc = -ost_index;
            goto out;
        }
        snprintf(ostname, 128, "OST #%u", ost_index);
        update_trigger_status(trigger_index, TRIG_BEING_CHECKED);

        /* check thresholds */
        rc = check_thresholds(p_trigger, ostname, &statfs_ost,
                              &purge_param.nb_blocks, &ost_usage);

        /* save max OST usage */
        if ((rc == 0) && (ost_usage > trigger_status_list[trigger_index].last_usage))
            trigger_status_list[trigger_index].last_usage = ost_usage;

        if (rc)
        {
            update_trigger_status(trigger_index, TRIG_CHECK_ERROR);
            goto out;
        }
        else if (purge_param.nb_blocks == 0)
        {
            update_trigger_status(trigger_index, TRIG_OK);
            /* If the max is not over the threshold,
             * next won't be either. */
            DisplayLog(LVL_DEBUG, RESMON_TAG, "Most used OSTs are all under high threshold: skipping check of other OSTs");
            break;
        }

        purge_param.type = PURGE_BY_OST;
        purge_param.param_u.ost_index = ost_index;
        purge_param.flags = module_args.flags;

        /* only purge if check_only is not set */
        if (module_args.flags & FLAG_CHECK_ONLY)
        {
            snprintf(status_str, 1024, "High threshold exceeded on %s: "
                     "%.2f%% used", ostname, ost_usage);
            ListMgr_SetVar(&lmgr, LAST_PURGE_STATUS, status_str);
            update_trigger_status(trigger_index, TRIG_OK);

            /* skip this OST, from now */
            if ((rc = ost_list_add(&excl_list, ost_index)))
                goto out;

            continue; /* handle next OSTs */
        }

        update_trigger_status(trigger_index, TRIG_PURGE_RUNNING);

        rc = perform_purge(&lmgr, &purge_param, &purged, &spec);

        /* update last purge time and target */
        sprintf(timestamp, "%lu", (unsigned long)time(NULL));
        ListMgr_SetVar(&lmgr, LAST_PURGE_TIME, timestamp);
        ListMgr_SetVar(&lmgr, LAST_PURGE_TARGET, ostname);

        if (rc == 0)
            DisplayLog(LVL_MAJOR, RESMON_TAG,
                       "OST #%u purge summary: %Lu blocks purged in OST #%u (%Lu total)/%lu blocks needed",
                       ost_index, spec, ost_index, purged, purge_param.nb_blocks);

        if (spec < purge_param.nb_blocks)
        {
            if (rc == ENOENT)
            {
                update_trigger_status(trigger_index, TRIG_NO_LIST);
                DisplayLog(LVL_EVENT, RESMON_TAG,
                           "Could not purge %lu blocks in OST #%u: no list is available.",
                           purge_param.nb_blocks, ost_index);

                snprintf(status_str, 1024, "No list available (%lu blocks need to be released in OST #%u)",
                         purge_param.nb_blocks, ost_index);
                ListMgr_SetVar(&lmgr, LAST_PURGE_STATUS, status_str);
            }
            else if (rc == ECANCELED)
            {
                update_trigger_status(trigger_index, TRIG_ABORTED);
                DisplayLog(LVL_CRIT, RESMON_TAG,
                           "Purge aborted after releasing %Lu blocks in OST #%u.",
                           spec, ost_index);
                snprintf(status_str, 1024, "Purge on OST#%u aborted by admin (after releasing %Lu blocks)",
                         ost_index, spec);
                ListMgr_SetVar(&lmgr, LAST_PURGE_STATUS, status_str);
            }
            else /* other error */
            {
                update_trigger_status(trigger_index, TRIG_NOT_ENOUGH);
                DisplayLog(LVL_CRIT, RESMON_TAG,
                           "Could not purge %lu blocks in OST #%u: not enough eligible files. Only %Lu blocks freed.",
                           purge_param.nb_blocks, ost_index, spec);

                if (ALERT_LW(trigger_index))
                {
                    sprintf(buff, "cannot purge OST#%u", ost_index);
                    RaiseAlert(buff, "Could not purge %lu blocks in OST #%u (filesystem %s):\n"
                                "not enough eligible files. Only %Lu blocks freed.",
                                purge_param.nb_blocks, ost_index,
                                global_config.fs_path, spec);
                }

                snprintf(status_str, 1024, "Not enough eligible files (%Lu/%lu blocks released) in OST #%u",
                         spec, purge_param.nb_blocks, ost_index);
                ListMgr_SetVar(&lmgr, LAST_PURGE_STATUS, status_str);
            }

        }
        else
        {
            snprintf(status_str, 1024, "Success (%Lu/%lu blocks released in OST #%u)",
                     spec, purge_param.nb_blocks, ost_index);
            ListMgr_SetVar(&lmgr, LAST_PURGE_STATUS, status_str);
            update_trigger_status(trigger_index, TRIG_OK);
        }

        /* exclude this OST for next loops */
        if ((rc = ost_list_add(&excl_list, ost_index)))
            goto out;

        FlushLogs();

        if ((!terminate) && (purged > 0) && (resmon_config.post_purge_df_latency > 0))
        {
            DisplayLog(LVL_EVENT, RESMON_TAG,
                        "Waiting %lus before performing 'df' on other storage units.",
                        resmon_config.post_purge_df_latency);
            rh_sleep(resmon_config.post_purge_df_latency);
        }
    }
#endif
    rc = 0;
out:
#ifdef _LUSTRE
    ost_list_free(&excl_list);
#endif
    return rc;
}


/** Check triggers on pool usage */
static int check_pool_trigger( unsigned trigger_index )
{

#ifndef _LUSTRE
    DisplayLog( LVL_CRIT, RESMON_TAG,
                "'pool_usage' trigger is not supported: you should rebuild the program with '_LUSTRE' flag enabled." );
    return ENOTSUP;
#else
#ifndef HAVE_LLAPI_GETPOOL_INFO
    DisplayLog( LVL_CRIT, RESMON_TAG,
                "'pool_usage' trigger is not supported for this lustre release. Consider using a lustre version over 2.x" );
                                                                                                                             /** @TODO */
    return ENOTSUP;
#else
    struct statfs  statfs_pool;
    purge_param_t  purge_param;
    char         **pool_list;
    int            pool_count;
    char           pool_string[128];
    int            rc, i;
    char           timestamp[128];
    unsigned long long purged, spec;
    double         pool_usage = 0.0;
    char           status_str[1024];
    char           buff[1024];

    /* Only for lustre filesystems */
    if ( strcasecmp( global_config.fs_type, "lustre" ) )
    {
        DisplayLog( LVL_CRIT, RESMON_TAG,
                    "'pool_usage' trigger is only supported on Lustre filesystems: skipped." );

        update_trigger_status( trigger_index, TRIG_UNSUPPORTED );
        return ENOTSUP;
    }

    if ( !CheckFSDevice(  ) )
        return ENXIO;

    /* If no list of pool is specified, get the entire list */
    if ( resmon_config.trigger_list[trigger_index].list_size == 0 )
    {
        pool_list = NULL;
        pool_count = 0;
    }
    else
    {
        pool_list = resmon_config.trigger_list[trigger_index].list;
        pool_count = resmon_config.trigger_list[trigger_index].list_size;
    }

    /* first initialize max_pool_usage */
    trigger_status_list[trigger_index].last_usage = 0.0;

    /* check usage for each pool in list */

    for ( i = 0; i < pool_count; i++ )
    {
        rc = Get_pool_usage( pool_list[i], &statfs_pool );
        if ( rc )
        {
            DisplayLog( LVL_CRIT, RESMON_TAG, "Could not retrieve usage info for pool '%s': %s",
                        pool_list[i], strerror( rc ) );
            update_trigger_status( trigger_index, TRIG_CHECK_ERROR );
            continue;
        }

        update_trigger_status( trigger_index, TRIG_BEING_CHECKED );

        sprintf( pool_string, "pool '%s'", pool_list[i] );
        /* check thresholds */
        rc = check_thresholds( &( resmon_config.trigger_list[trigger_index] ), pool_string,
                               &statfs_pool, &purge_param.nb_blocks, &pool_usage );

        /* update max pool usage */
        if ( ( rc == 0 ) && ( pool_usage > trigger_status_list[trigger_index].last_usage ) )
            trigger_status_list[trigger_index].last_usage = pool_usage;

        if ( rc )
        {
            update_trigger_status( trigger_index, TRIG_CHECK_ERROR );
            return rc;
        }
        else if ( purge_param.nb_blocks == 0 )
        {
            update_trigger_status( trigger_index, TRIG_OK );
            continue;
        }

        purge_param.type = PURGE_BY_POOL;
        purge_param.param_u.pool_name = pool_list[i];
        purge_param.flags = module_args.flags;

        /* only purge if check_only is not set */
        if ( module_args.flags & FLAG_CHECK_ONLY )
        {
            snprintf(status_str, 1024, "High threshold exceeded on %s: "
                     "%.2f%% used", pool_string, pool_usage );
            ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
            update_trigger_status( trigger_index, TRIG_OK );

            continue; /* handle next pools */
        }

        update_trigger_status( trigger_index, TRIG_PURGE_RUNNING );

        rc = perform_purge( &lmgr, &purge_param, &purged, &spec );

        /* update last purge time and target */
        sprintf( timestamp, "%lu", ( unsigned long ) time( NULL ) );
        ListMgr_SetVar( &lmgr, LAST_PURGE_TIME, timestamp );
        ListMgr_SetVar( &lmgr, LAST_PURGE_TARGET, pool_string );

        if ( rc == 0 )
            DisplayLog( LVL_MAJOR, RESMON_TAG,
                        "Pool '%s' purge summary: %Lu blocks purged in '%s' (%Lu total)/%lu blocks needed",
                        pool_list[i], spec, pool_list[i], purged, purge_param.nb_blocks );

        if ( spec < purge_param.nb_blocks )
        {
            if ( rc == ENOENT )
            {
                update_trigger_status( trigger_index, TRIG_NO_LIST );
                DisplayLog( LVL_EVENT, RESMON_TAG,
                            "Could not purge %lu blocks in %s: no list is available.",
                            purge_param.nb_blocks, pool_string );

                snprintf(status_str, 1024, "No list available (%lu blocks need to be released in %s)",
                         purge_param.nb_blocks, pool_string );
                ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
            }
            else if ( rc == ECANCELED )
            {
                update_trigger_status( trigger_index, TRIG_ABORTED );
                DisplayLog( LVL_CRIT, RESMON_TAG,
                            "Purge aborted after releasing %Lu blocks in %s.",
                            spec, pool_string );
                snprintf(status_str, 1024, "Purge on %s aborted by admin (after releasing %Lu blocks)",
                         pool_string, spec );
                ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
            }
            else /* other error */
            {
                update_trigger_status( trigger_index, TRIG_NOT_ENOUGH );
                DisplayLog( LVL_CRIT, RESMON_TAG,
                            "Could not purge %lu blocks in %s: not enough eligible files. Only %Lu blocks freed.",
                            purge_param.nb_blocks, pool_string, spec );

                if ( ALERT_LW( trigger_index ) )
                {
                    sprintf(buff, "cannot purge %s", pool_string);
                    RaiseAlert( buff, "Could not purge %lu blocks in %s (%s): not enough eligible files."
                                " Only %Lu blocks freed.",
                                purge_param.nb_blocks, pool_string, global_config.fs_path, spec );
                }

                snprintf(status_str, 1024, "Not enough eligible files (%Lu/%lu blocks released in %s)",
                         spec, purge_param.nb_blocks, pool_string );
                ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
            }
        }
        else
        {
            snprintf(status_str, 1024, "Success (%Lu/%lu blocks released in %s)",
                     spec, purge_param.nb_blocks, pool_string );
            ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );

            update_trigger_status( trigger_index, TRIG_OK );
        }

        FlushLogs(  );

        if ( (!terminate) && ( purged > 0 ) && ( resmon_config.post_purge_df_latency > 0 ) )
        {
            DisplayLog( LVL_EVENT, RESMON_TAG,
                        "Waiting %lus before performing 'df' on other storage units.",
                        resmon_config.post_purge_df_latency );
            rh_sleep( resmon_config.post_purge_df_latency );
        }

    }

    return 0;
#endif
#endif
}

/** Check trigger on user usage or group usage */
static int check_usergroup_trigger( unsigned trigger_index )
{
    unsigned long long max_blk512 = 0;
    unsigned long long low_blk512 = 0;
    report_field_descr_t info[2];
    db_value_t     result[2];
    unsigned int   result_count;
    struct lmgr_report_t *it;
    filter_value_t fv;
    lmgr_filter_t  filter;
    int            rc;
    purge_param_t  purge_param;
    struct statfs  statfs_glob;
    unsigned long  total_blocks = 0;
    char           hw_str[128];
    char           status_str[1024];
    char           buff[1024];

    /* user/group switches */
    char        * what = "";
    char        * what_up = "";
    unsigned int what_index = 0;

    trigger_item_t *p_trigger = &resmon_config.trigger_list[trigger_index];

    if (p_trigger->type == TRIGGER_USER_USAGE)
    {
        what = "user";
        what_up = "User";
        what_index = ATTR_INDEX_owner;
    }
    else
    {
        what = "group";
        what_up = "Group";
        what_index = ATTR_INDEX_gr_name;
    }

    update_trigger_status( trigger_index, TRIG_BEING_CHECKED );

    if ( ( p_trigger->hw_type == PCT_THRESHOLD ) || ( p_trigger->lw_type == PCT_THRESHOLD ) )
    {
        /* if PCT_THRESHOLD is used, statfs is needed */

        char           traverse_path[RBH_PATH_MAX];
        snprintf( traverse_path, RBH_PATH_MAX, "%s/.", global_config.fs_path );

        if ( statfs( traverse_path, &statfs_glob ) != 0 )
        {
            int            err = errno;
            DisplayLog( LVL_CRIT, RESMON_TAG, "Could not make a 'df' on %s: error %d: %s",
                        global_config.fs_path, err, strerror( err ) );
            update_trigger_status( trigger_index, TRIG_CHECK_ERROR );
            return err;
        }

        /* number of blocks available to users */
        total_blocks = ( statfs_glob.f_blocks + statfs_glob.f_bavail - statfs_glob.f_bfree );
    }

    /* compute high threshold, in number of blocks */

    if ( p_trigger->hw_type == VOL_THRESHOLD )
    {
        max_blk512 = p_trigger->hw_volume / DEV_BSIZE;
        FormatFileSize( hw_str, 128, p_trigger->hw_volume );
    }
    else if ( p_trigger->hw_type == PCT_THRESHOLD )
    {
        unsigned long  used_hw =
            ( unsigned long ) ( ( p_trigger->hw_percent * total_blocks ) / 100.0 );
        snprintf( hw_str, 128, "%.2f%%", p_trigger->hw_percent );

        max_blk512 = FSInfo2Blocs512( used_hw, statfs_glob.f_bsize );
    }

    /* compute low threshold */
    if ( p_trigger->lw_type == VOL_THRESHOLD )
        low_blk512 = p_trigger->lw_volume / DEV_BSIZE;
    else if ( p_trigger->hw_type == PCT_THRESHOLD )
    {
        unsigned long  target =
            ( unsigned long ) ( ( p_trigger->lw_percent * total_blocks ) / 100.0 );
        low_blk512 = FSInfo2Blocs512( target, statfs_glob.f_bsize );
    }

    /* build report parameters */

    info[0].attr_index = what_index;
    info[0].report_type = REPORT_GROUP_BY;
    info[0].sort_flag = SORT_NONE;
    info[0].filter = FALSE;

    if ( IS_COUNT_TRIGGER(trigger_index) )
    {
        info[1].attr_index = 0;
        info[1].report_type = REPORT_COUNT;
        info[1].sort_flag = SORT_NONE;
        info[1].filter = TRUE;
        info[1].filter_compar = MORETHAN_STRICT;
        info[1].filter_value.value.val_biguint = p_trigger->hw_count;
    }
    else /* volume based trigger */
    {
        /* select users/groups whose sum(blocks) > high_threshold */
        info[1].attr_index = ATTR_INDEX_blocks;
        info[1].report_type = REPORT_SUM;
        info[1].sort_flag = SORT_NONE;
        info[1].filter = TRUE;
        info[1].filter_compar = MORETHAN_STRICT;
        info[1].filter_value.value.val_biguint = max_blk512;
    }

    /* filtre non-invalid entries */
    lmgr_simple_filter_init( &filter );

#ifdef ATTR_INDEX_status
    /* don't consider released files in quota */
    fv.value.val_int = STATUS_RELEASED;
    lmgr_simple_filter_add( &filter, ATTR_INDEX_status, NOTEQUAL, fv,
                            FILTER_FLAG_ALLOW_NULL);
#endif

#if 0
    /** @TODO if accounting is enabled, don't filter to take benefits of accounting table */
    fv.value.val_bool = TRUE;
#ifdef ATTR_INDEX_invalid
    lmgr_simple_filter_add( &filter, ATTR_INDEX_invalid, NOTEQUAL, fv, 0 );
#endif
#endif

    /* if a specific set of users/groups is specified, make a filter for this */
    if ( p_trigger->list != NULL )
    {
        /* 2 cases: if there is a single user/group, add a simple filter for it:
         * AND owner LIKE ...
         * If there are several users/groups, add a OR sequence:
         * AND (owner LIKE ... OR owner LIKE ...)
         */
        if ( p_trigger->list_size == 1 )
        {
            fv.value.val_str = p_trigger->list[0];
            lmgr_simple_filter_add( &filter, what_index, LIKE, fv, 0 );
        }
        else
        {
            int i;

            fv.value.val_str = p_trigger->list[0];
            lmgr_simple_filter_add( &filter, what_index, LIKE, fv,
                                    FILTER_FLAG_BEGIN );
            for ( i = 1; i < p_trigger->list_size-1; i++ )
            {
                fv.value.val_str = p_trigger->list[i];
                lmgr_simple_filter_add( &filter, what_index, LIKE, fv,
                                        FILTER_FLAG_OR );
            }
            fv.value.val_str = p_trigger->list[i];
            lmgr_simple_filter_add( &filter, what_index, LIKE, fv,
                                    FILTER_FLAG_OR | FILTER_FLAG_END );
        }
    }

    it = ListMgr_Report( &lmgr, info, 2, NULL, &filter, NULL );

    lmgr_simple_filter_free( &filter );

    if ( it == NULL )
    {
        update_trigger_status( trigger_index, TRIG_CHECK_ERROR );
        DisplayLog( LVL_CRIT, RESMON_TAG,
                    "Could not retrieve %s stats from database. Skipping %s_usage trigger check.", what, what );
        return -1;
    }

    result_count = 2;
    while ( (( rc = ListMgr_GetNextReportItem( it, result, &result_count, NULL ) ) == DB_SUCCESS) && !terminate )
    {
        unsigned long long blocks_purged, nb_purged;
        char           desc[128];
        char           timestamp[128];

        if (IS_COUNT_TRIGGER(trigger_index))
        {
            DisplayLog( LVL_EVENT, RESMON_TAG,
                        "%s '%s' exceeds high threshold: used: %Lu inodes / high threshold: %llu inodes.",
                        what_up, result[0].value_u.val_str, result[1].value_u.val_biguint, p_trigger->hw_count );

            if ( p_trigger->alert_hw )
            {
                snprintf(buff, 1024, "Inode quota exceeded for %s '%s'",
                         what, result[0].value_u.val_str);
                RaiseAlert(buff, "%s\n"
                                 "%s:       %s\n"
                                 "quota:      %Lu inodes\n"
                                 "usage:      %Lu inodes",
                           buff, what, result[0].value_u.val_str, p_trigger->hw_count, result[1].value_u.val_biguint);
            }
        }
        else
        {
            DisplayLog( LVL_EVENT, RESMON_TAG,
                        "%s '%s' exceeds high threshold: used: %llu blocks / high threshold: %llu blocks (x%u).",
                        what_up, result[0].value_u.val_str, result[1].value_u.val_biguint, max_blk512,
                        DEV_BSIZE );

            if ( p_trigger->alert_hw )
            {
                char usage_str[128];
                FormatFileSize( usage_str, 128, result[1].value_u.val_biguint * 512 );
                snprintf(buff, 1024, "Volume quota exceeded for %s '%s'",
                         what,  result[0].value_u.val_str);
                RaiseAlert(buff, "%s\n%s:       %s\nquota:      %s\nspace used: %s",
                           buff, what, result[0].value_u.val_str, hw_str, usage_str);
            }
        }

        /* reset result count before continuing */
        result_count = 2;

        purge_param.flags = module_args.flags;
        if (p_trigger->type == TRIGGER_USER_USAGE)
        {
            purge_param.type = PURGE_BY_USER;
            purge_param.param_u.user_name = result[0].value_u.val_str;
        }
        else
        {
            purge_param.type = PURGE_BY_GROUP;
            purge_param.param_u.group_name = result[0].value_u.val_str;
        }

        if (IS_COUNT_TRIGGER(trigger_index))
        {
            purge_param.nb_inodes = result[1].value_u.val_biguint - p_trigger->lw_count;
            purge_param.nb_blocks = 0;


            DisplayLog( LVL_EVENT, RESMON_TAG,
                        "%Lu files must be purged for %s '%s' (used=%Lu, target=%Lu)",
                        purge_param.nb_inodes, what, result[0].value_u.val_str,
                        result[1].value_u.val_biguint, p_trigger->lw_count );
        }
        else
        {
            purge_param.nb_blocks = result[1].value_u.val_biguint - low_blk512;
            purge_param.nb_inodes = 0;

            DisplayLog( LVL_EVENT, RESMON_TAG,
                        "%lu blocks (x%u) must be purged for %s '%s' (used=%Lu, target=%Lu)",
                        purge_param.nb_blocks, DEV_BSIZE, what, result[0].value_u.val_str,
                        result[1].value_u.val_biguint, low_blk512 );
        }

        /* only purge if check_only is not set */
        if ( module_args.flags & FLAG_CHECK_ONLY )
        {
            if (IS_COUNT_TRIGGER(trigger_index))
                snprintf(status_str, 1024, "Inode quota exceeded for %s '%s': "
                         "%Lu inodes", what, result[0].value_u.val_str,
                         result[1].value_u.val_biguint );
            else
                snprintf(status_str, 1024, "Volume quota exceeded for %s '%s': "
                         "%Lu kB used", what, result[0].value_u.val_str,
                         (result[1].value_u.val_biguint*DEV_BSIZE)/1024 );

            ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
            update_trigger_status( trigger_index, TRIG_OK );

            continue; /* handle next users/groups */
        }

        update_trigger_status( trigger_index, TRIG_PURGE_RUNNING );

        /* perform the purge */
        blocks_purged = nb_purged = 0;

        rc = perform_purge( &lmgr, &purge_param, &blocks_purged, &nb_purged );

        /* update last purge time and target */
        sprintf( timestamp, "%lu", ( unsigned long ) time( NULL ) );
        ListMgr_SetVar( &lmgr, LAST_PURGE_TIME, timestamp );
        snprintf( desc, 128, "%s \"%s\"", what, result[0].value_u.val_str );
        ListMgr_SetVar( &lmgr, LAST_PURGE_TARGET, desc );

        if ( IS_COUNT_TRIGGER(trigger_index) )
        {
            if ( rc == 0 )
            {
                DisplayLog( LVL_MAJOR, RESMON_TAG,
                            "%s files purge summary: %Lu entries purged (%Lu blocks)/%Lu needed for %s '%s'",
                            what_up, nb_purged, blocks_purged, purge_param.nb_inodes, what, result[0].value_u.val_str );
            }

            if ( nb_purged < purge_param.nb_inodes )
            {
                if ( rc == ENOENT )
                {
                    update_trigger_status( trigger_index, TRIG_NO_LIST );
                    DisplayLog( LVL_EVENT, RESMON_TAG,
                                "Could not purge %Lu entries for %s '%s': no list is available.",
                                purge_param.nb_inodes, what, result[0].value_u.val_str );

                    snprintf(status_str, 1024, "No list available (%Lu entries need to be released for %s '%s')",
                             purge_param.nb_inodes, what, result[0].value_u.val_str);
                    ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
                }
                else if ( rc == ECANCELED )
                {
                    update_trigger_status( trigger_index, TRIG_ABORTED );
                    DisplayLog( LVL_CRIT, RESMON_TAG,
                                "Purge aborted after releasing %Lu entries (%Lu blocks) for %s %s.",
                                nb_purged, blocks_purged, what, result[0].value_u.val_str );

                    snprintf(status_str, 1024, "Purge on %s %s aborted by admin (after releasing %Lu entries, %Lu blocks)",
                             what, result[0].value_u.val_str, nb_purged, blocks_purged);
                    ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
                }
                else
                {
                    update_trigger_status( trigger_index, TRIG_NOT_ENOUGH );
                    DisplayLog( LVL_CRIT, RESMON_TAG,
                                "Could not purge %Lu entries for %s '%s': not enough eligible files. Only %Lu entries released.",
                                purge_param.nb_inodes, what, result[0].value_u.val_str, nb_purged );

                    if ( ALERT_LW( trigger_index ) )
                    {
                        sprintf(buff, "cannot purge %s '%s' files",
                                what, result[0].value_u.val_str);
                        RaiseAlert( buff,
                                    "Could not purge %Lu entries for %s '%s' in %s:\n"
                                    "not enough eligible files. Only %Lu entries freed.",
                                    purge_param.nb_inodes, what, result[0].value_u.val_str,
                                    global_config.fs_path, nb_purged );
                    }

                    snprintf(status_str, 1024, "Not enough eligible files (%Lu/%Lu entries released for %s '%s')",
                             nb_purged, purge_param.nb_inodes, what, result[0].value_u.val_str);
                    ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
                }
            }
            else
            {
                snprintf(status_str, 1024, "Success (%Lu/%Lu entries released for %s '%s')",
                         nb_purged, purge_param.nb_inodes, what, result[0].value_u.val_str);
                ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
                update_trigger_status( trigger_index, TRIG_OK );

            }
        }
        else /* volume trigger */
        {
            if ( rc == 0 )
            {
                DisplayLog( LVL_MAJOR, RESMON_TAG,
                            "%s files purge summary: %Lu blocks purged/%lu blocks needed for %s '%s'",
                            what_up, blocks_purged, purge_param.nb_blocks, what, result[0].value_u.val_str );
            }

            if ( blocks_purged < purge_param.nb_blocks )
            {
                if (rc == ENOENT)
                {
                    update_trigger_status( trigger_index, TRIG_NO_LIST );
                    DisplayLog( LVL_EVENT, RESMON_TAG,
                                "Could not purge %lu blocks for %s '%s': no list is available.",
                                purge_param.nb_blocks, what, result[0].value_u.val_str );

                    snprintf(status_str, 1024, "No list available (%lu blocks need to be released for %s '%s')",
                             purge_param.nb_blocks, what, result[0].value_u.val_str);
                    ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
                }
                else if ( rc == ECANCELED )
                {
                    update_trigger_status( trigger_index, TRIG_ABORTED );
                    DisplayLog( LVL_CRIT, RESMON_TAG,
                                "Purge aborted after releasing %Lu blocks for %s %s.",
                                blocks_purged, what, result[0].value_u.val_str );
                    snprintf(status_str, 1024, "Purge on %s %s aborted by admin (after releasing %Lu blocks)",
                             what, result[0].value_u.val_str, blocks_purged);
                    ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
                }
                else
                {
                    update_trigger_status( trigger_index, TRIG_NOT_ENOUGH );
                    DisplayLog( LVL_CRIT, RESMON_TAG,
                                "Could not purge %lu blocks for %s '%s': not enough eligible files. Only %Lu blocks freed.",
                                purge_param.nb_blocks, what, result[0].value_u.val_str, blocks_purged );

                    if ( ALERT_LW( trigger_index ) )
                    {
                        sprintf(buff, "cannot purge %s '%s' files",
                                what, result[0].value_u.val_str);
                        RaiseAlert( buff,
                                    "Could not purge %lu blocks for %s '%s' in %s:\n"
                                    "not enough eligible files. Only %Lu blocks freed.",
                                    purge_param.nb_blocks, what, result[0].value_u.val_str,
                                    global_config.fs_path, blocks_purged );
                    }

                    snprintf(status_str, 1024, "Not enough eligible files (%Lu/%lu blocks released for %s '%s')",
                             blocks_purged, purge_param.nb_blocks, what, result[0].value_u.val_str);
                    ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
                }

            }
            else
            {
                snprintf(status_str, 1024, "Success (%Lu/%lu blocks released for %s '%s')",
                         blocks_purged, purge_param.nb_blocks, what, result[0].value_u.val_str);
                ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
                update_trigger_status( trigger_index, TRIG_OK );
            }
        }

        FlushLogs(  );

    }                           /* loop for reading report */

    ListMgr_CloseReport( it );

    if ( trigger_status_list[trigger_index].status == TRIG_BEING_CHECKED )
        update_trigger_status( trigger_index, TRIG_OK );

    return 0;
}

static int check_cmd_trigger( unsigned trigger_index )
{                               /* @TODO */
    return 0;
}

/**
 * This thread is for performing a manual purge on a given OST
 */
static void   *force_ost_trigger_thr( void *arg )
{
    int            rc;
    struct statfs  statfs_ost;
    purge_param_t  purge_param;
    char           ostname[128];
    char           timestamp[128];
    unsigned long long purged, spec;
    double         ost_usage = 0.0;
    trigger_item_t trig;
    char           status_str[1024];
    char           buff[1024];

#ifndef _LUSTRE
    DisplayLog( LVL_CRIT, RESMON_TAG,
                "OST purge is not supported: you must rebuild the program with Lustre support." );
    goto end_of_thread;
#else

    /* Only for lustre filesystems */
    if ( strcasecmp( global_config.fs_type, "lustre" ) )
    {
        DisplayLog( LVL_CRIT, RESMON_TAG,
                    "Purge per OST is only supported for Lustre filesystems: operation cancelled." );
        goto end_of_thread;
    }

    rc = ListMgr_InitAccess( &lmgr );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, RESMON_TAG,
                    "Could not connect to database (error %d). OST usage cannot be checked.", rc );
        goto end_of_thread;
    }

    if ( !CheckFSDevice(  ) )
        goto disconnect;

    rc = Get_OST_usage( global_config.fs_path, module_args.ost_index, &statfs_ost );

    if ( rc == ENODEV )         /* end of OST list */
    {
        DisplayLog( LVL_CRIT, RESMON_TAG,
                    "Cannot purge OST #%u: no such device.", module_args.ost_index );
        goto disconnect;
    }
    else if ( rc != 0 )
    {
        DisplayLog( LVL_CRIT, RESMON_TAG,
                    "Cannot retrieve usage for OST #%u: error %d: %s",
                    module_args.ost_index, rc,
                    strerror(abs(rc)) );
        goto disconnect;
    }

    snprintf( ostname, 128, "OST #%u", module_args.ost_index );

    /* build a custom trigger for this OST */
    trig.type = TRIGGER_OST_USAGE;
    trig.list = NULL;
    trig.list_size = 0;
    trig.check_interval = 0;
    /* in this case, HW=LW=target */
    trig.hw_type = PCT_THRESHOLD;
    trig.lw_type = PCT_THRESHOLD;
    trig.hw_percent = module_args.target_usage;
    trig.lw_percent = module_args.target_usage;

    /* check thresholds */
    rc = check_thresholds( &trig, ostname, &statfs_ost, &purge_param.nb_blocks, &ost_usage );

    if ( rc )
    {
        DisplayLog( LVL_CRIT, RESMON_TAG,
                    "Error %d checking usage for OST #%u.", rc, module_args.ost_index );
        goto disconnect;
    }
    else if ( purge_param.nb_blocks == 0 )
    {
        DisplayLog( LVL_CRIT, RESMON_TAG, "Nothing to do for OST #%u", module_args.ost_index );
        goto disconnect;
    }

    purge_param.type = PURGE_BY_OST;
    purge_param.flags = module_args.flags;
    purge_param.param_u.ost_index = module_args.ost_index;

    /* only purge if check_only is not set */
    if ( module_args.flags & FLAG_CHECK_ONLY )
    {
        snprintf(status_str, 1024, "%s usage is over the specified limit: "
                 "%.2f%% used", ostname, ost_usage );
        ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );

        goto disconnect;
    }

    rc = perform_purge( &lmgr, &purge_param, &purged, &spec );

    /* update last purge time and target */
    sprintf( timestamp, "%lu", ( unsigned long ) time( NULL ) );
    ListMgr_SetVar( &lmgr, LAST_PURGE_TIME, timestamp );
    ListMgr_SetVar( &lmgr, LAST_PURGE_TARGET, ostname );

    if ( rc == 0 )
        DisplayLog( LVL_MAJOR, RESMON_TAG,
                    "OST #%u purge summary: %Lu blocks purged in OST #%u (%Lu total)/%lu blocks needed",
                    module_args.ost_index, spec, module_args.ost_index, purged,
                    purge_param.nb_blocks );

    if ( spec < purge_param.nb_blocks )
    {
        if (rc == ENOENT)
        {
            DisplayLog( LVL_EVENT, RESMON_TAG,
                        "Could not purge %lu blocks in OST #%u: no list is available.",
                        purge_param.nb_blocks, module_args.ost_index );

            snprintf(status_str, 1024, "No list available (admin requested to release %lu blocks in OST #%u)",
                     purge_param.nb_blocks, module_args.ost_index );
            ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
        }
        else if ( rc == ECANCELED )
        {
            DisplayLog( LVL_CRIT, RESMON_TAG,
                        "Purge aborted after releasing %Lu blocks in OST #%u.",
                        spec, module_args.ost_index );
            snprintf(status_str, 1024, "Purge on OST#%u aborted by admin (after releasing %Lu blocks)",
                     module_args.ost_index, spec );
            ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
        }
        else
        {
            DisplayLog( LVL_CRIT, RESMON_TAG,
                        "Could not purge %lu blocks in OST #%u: not enough eligible files. "
                        "Only %Lu blocks freed.",
                        purge_param.nb_blocks, module_args.ost_index, spec );

            sprintf(buff, "cannot purge OST#%u", module_args.ost_index);
            RaiseAlert( buff, "Could not purge %lu blocks in OST #%u (%s): not enough eligible files. "
                        "Only %Lu blocks freed.",
                        purge_param.nb_blocks, module_args.ost_index,
                        global_config.fs_path, spec );

            snprintf(status_str, 1024, "Not enough eligible files (%Lu/%lu blocks released) in OST #%u",
                     spec, purge_param.nb_blocks, module_args.ost_index );
            ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
        }
    }
    else
    {
        snprintf(status_str, 1024, "Success (%Lu/%lu blocks released in OST #%u)",
                 spec, purge_param.nb_blocks, module_args.ost_index );
        ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
    }

    FlushLogs(  );

    if ( ( purged > 0 ) && ( resmon_config.post_purge_df_latency > 0 ) )
    {
        DisplayLog( LVL_EVENT, RESMON_TAG,
                    "It is advised waiting %lus before performing purge on other storage units.",
                    resmon_config.post_purge_df_latency );
    }
#endif
  disconnect:
    ListMgr_CloseAccess( &lmgr );
  end_of_thread:
    pthread_exit( NULL );
    return NULL;
}

/**
 * This thread performs a manual purge on the entire filesystem
 */
static void   *force_fs_trigger_thr( void *arg )
{
    struct statfs  statfs_glob;
    char           traverse_path[RBH_PATH_MAX];
    purge_param_t  purge_param;
    int            rc;
    unsigned long long purged, spec;
    char           timestamp[128];
    trigger_item_t trig = {0};
    double         curr_usage;
    char           status_str[1024];
    char           buff[1024];

    snprintf( traverse_path, RBH_PATH_MAX, "%s/.", global_config.fs_path );

    rc = ListMgr_InitAccess( &lmgr );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, RESMON_TAG,
                    "Could not connect to database (error %d). FS usage cannot be checked.", rc );
        goto end_of_thread;
    }

    if ( !CheckFSDevice(  ) )
        goto disconnect;

    /* retrieve filesystem usage info */

    if ( statfs( traverse_path, &statfs_glob ) != 0 )
    {
        int            err = errno;
        DisplayLog( LVL_CRIT, RESMON_TAG, "Could not make a 'df' on %s: error %d: %s",
                    global_config.fs_path, err, strerror( err ) );
        goto disconnect;
    }

    /* build a custom policy for the FS */
    trig.type = TRIGGER_GLOBAL_USAGE;
    trig.list = NULL;
    trig.list_size = 0;
    trig.check_interval = 0;
    /* in this case, HW=LW=target */
    trig.hw_type = PCT_THRESHOLD;
    trig.lw_type = PCT_THRESHOLD;
    trig.hw_percent = module_args.target_usage;
    trig.lw_percent = module_args.target_usage;

    rc = check_thresholds( &trig, "Filesystem", &statfs_glob, &purge_param.nb_blocks, &curr_usage );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, RESMON_TAG, "Error %d checking usage for Filesystem.", rc );
        goto disconnect;
    }
    else if ( purge_param.nb_blocks == 0 )
    {
        DisplayLog( LVL_CRIT, RESMON_TAG, "Nothing to do for filesystem." );
        goto disconnect;
    }

    purge_param.type = PURGE_FS;
    purge_param.flags = module_args.flags;

    /* only purge if check_only is not set */
    if ( module_args.flags & FLAG_CHECK_ONLY )
    {
        snprintf(status_str, 1024, "Filesystem usage is over the specified limit: "
                 "%.2f%% used", curr_usage );
        ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );

        goto disconnect;
    }

    /* perform the purge */
    rc = perform_purge( &lmgr, &purge_param, &purged, &spec );

    /* update last purge time and target */
    sprintf( timestamp, "%lu", ( unsigned long ) time( NULL ) );
    ListMgr_SetVar( &lmgr, LAST_PURGE_TIME, timestamp );
    ListMgr_SetVar( &lmgr, LAST_PURGE_TARGET, "Filesystem" );

    if ( rc == 0 )
    {
        DisplayLog( LVL_MAJOR, RESMON_TAG,
                    "Filesystem purge summary: %Lu blocks purged "
                    "(initial estimation %Lu)/%lu blocks needed in %s",
                    purged, spec, purge_param.nb_blocks, global_config.fs_path );
    }

    if ( purged < purge_param.nb_blocks )
    {
        if ( rc == ENOENT )
        {
            DisplayLog( LVL_EVENT, RESMON_TAG,
                        "Could not purge %lu blocks in %s: no list is available.",
                        purge_param.nb_blocks, global_config.fs_path );

            snprintf(status_str, 1024, "No list available (admin requested to release %lu blocks in %s)",
                     purge_param.nb_blocks, global_config.fs_path );
            ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
        }
        else if ( rc == ECANCELED )
        {
            DisplayLog( LVL_CRIT, RESMON_TAG,
                        "Purge aborted after releasing %Lu blocks in %s.",
                        purged, global_config.fs_path );

            snprintf(status_str, 1024, "Purge on %s aborted by admin (after releasing %Lu blocks)",
                     global_config.fs_path, purged);
            ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
        }
        else
        {
            DisplayLog( LVL_CRIT, RESMON_TAG,
                        "Could not purge %lu blocks in %s: "
                        "not enough eligible files. Only %Lu blocks freed.",
                        purge_param.nb_blocks, global_config.fs_path, purged );

            sprintf(buff, "cannot purge filesystem");
            RaiseAlert( buff, "Could not purge %lu blocks in filesystem %s: "
                        "not enough eligible files. Only %Lu blocks freed.",
                        purge_param.nb_blocks, global_config.fs_path, purged );

            snprintf(status_str, 1024, "Not enough eligible files (%Lu/%lu blocks released in %s)",
                     purged, purge_param.nb_blocks, global_config.fs_path );
            ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
        }
    }
    else
    {
        snprintf(status_str, 1024, "Success (%Lu/%lu blocks released in %s)",
                 purged, purge_param.nb_blocks, global_config.fs_path );
        ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
    }

    FlushLogs(  );

    if ( ( purged > 0 ) && ( resmon_config.post_purge_df_latency > 0 ) )
    {
        DisplayLog( LVL_EVENT, RESMON_TAG,
                    "It is advised waiting %lus before performing purge on other storage units.",
                    resmon_config.post_purge_df_latency );
    }

disconnect:
    ListMgr_CloseAccess( &lmgr );
end_of_thread:
    pthread_exit( NULL );
    return NULL;
}


/**
 * This thread performs a manual purge on the entire filesystem
 */
static void * force_purge_class_thr( void *arg )
{
    int rc;
    purge_param_t  purge_param;
    unsigned long long blocks_purged = 0;
    unsigned long long nbr_purged = 0;
    char timestamp[128];
    char descr[256];
    char status_str[1024];
    char buff[1024];

    rc = ListMgr_InitAccess( &lmgr );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, RESMON_TAG,
                    "Could not connect to database (error %d). Purge cannot be perfomed.", rc );
        goto end_of_thread;
    }

    if ( !CheckFSDevice(  ) )
        goto disconnect;

    purge_param.type = PURGE_BY_CLASS;
    purge_param.flags = module_args.flags;
    purge_param.param_u.class_name = module_args.fileclass;
    purge_param.nb_blocks = 0; /* unused, apply to all eligible files */
    purge_param.nb_inodes = 0; /* unused, apply to all eligible files */

    if ( WILDCARDS_IN(module_args.fileclass) )
        snprintf( descr, 256, "fileclass(es) '%s'", module_args.fileclass );
    else
        snprintf( descr, 256, "fileclass '%s'", module_args.fileclass );

    DisplayLog( LVL_EVENT, RESMON_TAG,
                "Applying purge policy to eligible files in %s", descr );

    /* perform the purge */
    rc = perform_purge( &lmgr, &purge_param, &blocks_purged, &nbr_purged );

    /* update last purge time and target */
    sprintf( timestamp, "%lu", ( unsigned long ) time( NULL ) );
    ListMgr_SetVar( &lmgr, LAST_PURGE_TIME, timestamp );
    ListMgr_SetVar( &lmgr, LAST_PURGE_TARGET, descr );

    if ( rc == 0 )
    {
        DisplayLog( LVL_MAJOR, RESMON_TAG,
                    "%s purge summary: %Lu entries, %Lu blocks purged",
                    descr, nbr_purged, blocks_purged );

        snprintf(status_str, 1024, "Success (%Lu entries, %Lu blocks released)",
                 nbr_purged, blocks_purged );
        ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
    }
    else if ( rc == ENOENT )
    {
        DisplayLog( LVL_EVENT, RESMON_TAG,
                    "Could not perform purge %s, %s: no list is available.",
                    global_config.fs_path, descr );

        snprintf(status_str, 1024, "No list available (admin requested to release files in %s, %s)",
                 global_config.fs_path, descr );
        ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
    }
    else if ( rc == ECANCELED )
    {
        DisplayLog( LVL_CRIT, RESMON_TAG,
                    "Purge aborted after releasing %Lu entries, %Lu blocks in %s.",
                    nbr_purged, blocks_purged, descr );

        snprintf(status_str, 1024, "Purge on %s aborted by admin (after releasing %Lu entries, %Lu blocks)",
                 descr, nbr_purged, blocks_purged );
        ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
    }
    else
    {
        DisplayLog( LVL_CRIT, RESMON_TAG,
                    "Error %d performing purge in %s, %s (%s). "
                    "%Lu entries purged, %Lu blocks.", rc,
                    global_config.fs_path, descr, strerror(rc),
                    nbr_purged, blocks_purged );

        sprintf(buff, "Error releasing data in %s, %s", global_config.fs_path,
                descr );
        RaiseAlert( buff, "Error %d performing purge in %s, %s (%s).\n"
                    "%Lu entries purged, %Lu blocks.", rc,
                    global_config.fs_path, descr, strerror(rc),
                    nbr_purged, blocks_purged );

        snprintf(status_str, 1024, "Error %d after releasing %Lu entries, %Lu blocks released in %s",
                 rc, nbr_purged, blocks_purged, global_config.fs_path );
        ListMgr_SetVar( &lmgr, LAST_PURGE_STATUS, status_str );
    }

    if ( ( blocks_purged > 0 ) && ( resmon_config.post_purge_df_latency > 0 ) )
    {
        DisplayLog( LVL_EVENT, RESMON_TAG,
                    "It is advised waiting %lus before performing purge on other storage units.",
                    resmon_config.post_purge_df_latency );
    }

disconnect:
    ListMgr_CloseAccess( &lmgr );
end_of_thread:
    FlushLogs(  );
    pthread_exit( NULL );
    return NULL;
}


static inline char *trigger2str( trigger_type_t type )
{
    switch ( type )
    {
    case TRIGGER_GLOBAL_USAGE:
        return "global_usage";
    case TRIGGER_ALWAYS:
        return "periodic";
    case TRIGGER_OST_USAGE:
        return "OST_usage";
    case TRIGGER_POOL_USAGE:
        return "pool_usage";
    case TRIGGER_USER_USAGE:
        return "user_usage";
    case TRIGGER_GROUP_USAGE:
        return "group_usage";
    case TRIGGER_CUSTOM_CMD:
        return "external_command";
    default:
        return "?";
    }
}


/**
 * Main loop for checking triggers periodically
 */
static void   *trigger_check_thr( void *thr_arg )
{
    unsigned int   i;
    int            rc;
    double         max_usage;
    char           tmpstr[128];

    rc = ListMgr_InitAccess( &lmgr );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, RESMON_TAG,
                    "Could not connect to database (error %d). Trigger checking cannot be started.",
                    rc );
        return NULL;
    }

#ifdef ATTR_INDEX_status
    unsigned int nb_reset = 0;
    unsigned int nb_total = 0;

    /* check previous migration status */
    if ( resmon_config.check_purge_status_on_startup )
    {
        DisplayLog( LVL_EVENT, RESMON_TAG, "Checking status of outstanding purges..." );

        rc = check_current_purges( &lmgr, &nb_reset, &nb_total, 0 );

        if ( rc != 0 )
            DisplayLog( LVL_CRIT, RESMON_TAG, "Error checking outstanding purge status" );
        else
            DisplayLog( LVL_EVENT, RESMON_TAG, "%u purges finished / %u total", nb_reset,
                        nb_total );
    }
#endif

    do
    {
        max_usage = 0.0;

        /* check every trigger */
        for ( i = 0; i < resmon_config.trigger_count; i++ )
        {
            if (terminate)
            {
                DisplayLog( LVL_MAJOR, RESMON_TAG, "Stop requested: aborting trigger check" );
                break;
            }

            if ( time( NULL ) - trigger_status_list[i].last_check >=
                 resmon_config.trigger_list[i].check_interval )
            {
                if ( trigger_status_list[i].last_check != 0 )
                    DisplayLog( LVL_DEBUG, RESMON_TAG,
                                "Checking trigger #%u (last check %lus ago)", i,
                                time( NULL ) - trigger_status_list[i].last_check );
                else
                    DisplayLog( LVL_DEBUG, RESMON_TAG, "Checking trigger #%u (never checked)", i );

                /* call the appropriate checking function depending on trigger type */
                switch ( resmon_config.trigger_list[i].type )
                {
                case TRIGGER_GLOBAL_USAGE:
                    rc = check_global_trigger( i );
                    break;
                case TRIGGER_ALWAYS:
                    rc = check_periodic_trigger( i );
                    break;
                case TRIGGER_OST_USAGE:
                    rc = check_ost_trigger( i );
                    break;
                case TRIGGER_POOL_USAGE:
                    rc = check_pool_trigger( i );
                    break;
                case TRIGGER_USER_USAGE:
                case TRIGGER_GROUP_USAGE:
                    rc = check_usergroup_trigger( i );
                    break;
                case TRIGGER_CUSTOM_CMD:
                    rc = check_cmd_trigger( i );
                    break;
                default:
                    DisplayLog( LVL_CRIT, RESMON_TAG,
                                "Unexpected trigger type %d for trigger #%u: ignored.",
                                resmon_config.trigger_list[i].type, i );
                    rc = 0;     /* don't retry immediately */
                }

                /* don't update last_check if trigger check failed */
                if ( rc != 0 )
                    DisplayLog( LVL_CRIT, RESMON_TAG,
                                "Trigger #%u check function returned error %d... Will retry later",
                                i, rc );
                else
                    trigger_status_list[i].last_check = time( NULL );

            }

            /* in any case compute max usage */
            if ( trigger_status_list[i].last_usage > max_usage )
                max_usage = trigger_status_list[i].last_usage;
        }

        /* Finaly update max_usage in persistent stats */
        snprintf( tmpstr, 128, "%.2f", max_usage );
        if ( ListMgr_SetVar( &lmgr, USAGE_MAX_VAR, tmpstr ) != DB_SUCCESS )
            DisplayLog( LVL_CRIT, RESMON_TAG,
                        "Error updating value of " USAGE_MAX_VAR " variable (value = %s)", tmpstr );

        DisplayLog( LVL_EVENT, RESMON_TAG, "Current usage max is %.2f%%", max_usage );

        if ( (module_args.mode == RESMON_DAEMON) && !terminate )
        {
            rh_intr_sleep( trigger_check_interval, terminate );
            if (terminate)
                goto out;
        }
        else
            goto out;

    } while ( 1 );

out:
    ListMgr_CloseAccess( &lmgr );
    pthread_exit( NULL );
    return NULL;

}

/* ------------ Exported functions ------------ */

/** Recompute trigger check interval as the GCD of all triggers */
void ResMon_UpdateCheckInterval( void )
{
    unsigned int   i;

    trigger_check_interval = 1;

    if ( resmon_config.trigger_count == 0 )
        return;

    /* compute GCD of trigger check intervals */

    if ( resmon_config.trigger_count == 1 )
        trigger_check_interval = resmon_config.trigger_list[0].check_interval;
    else if ( resmon_config.trigger_count > 1 )
    {
        trigger_check_interval =
            gcd( resmon_config.trigger_list[0].check_interval,
                 resmon_config.trigger_list[1].check_interval );
        for ( i = 2; i < resmon_config.trigger_count; i++ )
            trigger_check_interval =
                gcd( trigger_check_interval, resmon_config.trigger_list[i].check_interval );
    }

    DisplayLog( LVL_DEBUG, RESMON_TAG, "GCD of trigger check intervals is %us",
                ( unsigned int ) trigger_check_interval );

}

/**
 * Initialize module and start main thread
 */
int Start_ResourceMonitor( resource_monitor_config_t * p_config, resmon_opt_t options )
{
    unsigned int   i;
    int            rc;

    fsdev = get_fsdev();

    /* store configuration */
    resmon_config = *p_config;
    module_args = options;

    if ( (options.mode == RESMON_DAEMON) || (options.mode == RESMON_ALL_TRIGGERS) )
    {
        if ( resmon_config.trigger_count == 0 )
        {
            DisplayLog( LVL_CRIT, RESMON_TAG,
                        "No purge trigger defined in configuration file... Disabling automatic purges." );
            return ENOENT;
        }
    }
    if (NO_POLICY(&policies.purge_policies) && !(module_args.flags & FLAG_IGNORE_POL))
    {
        DisplayLog(LVL_CRIT, RESMON_TAG,
            "No purge policy defined in configuration file... Disabling purge.");
        return ENOENT;
    }

    /* Display an info message if no default policy is specified */
    if (!has_default_policy(&policies.purge_policies))
        DisplayLog(LVL_EVENT, RESMON_TAG, "Notice: no 'default' purge policy is defined. Unmatched entries will be ignored.");

    /* intervals must only be computed for daemon mode */
    if ( options.mode == RESMON_DAEMON )
        ResMon_UpdateCheckInterval(  );
    else
       trigger_check_interval = 1;

    /* alloc and initialize trigger status array (except for FORCE_PURGE modes) */
    if ( ( module_args.mode != RESMON_PURGE_OST )
          && ( module_args.mode != RESMON_PURGE_FS )
          && ( module_args.mode != RESMON_PURGE_CLASS ) )
    {
        trigger_status_list =
            ( trigger_info_t * ) MemCalloc( resmon_config.trigger_count, sizeof( trigger_info_t ) );

        if ( trigger_status_list == NULL )
        {
            DisplayLog( LVL_CRIT, RESMON_TAG, "Memory Error in %s", __FUNCTION__ );
            return ENOMEM;
        }

        for ( i = 0; i < resmon_config.trigger_count; i++ )
        {
            trigger_status_list[i].last_check = 0;      /* not checked yet */
            trigger_status_list[i].status = TRIG_NOT_CHECKED;
            trigger_status_list[i].last_usage = 0.0;
            trigger_status_list[i].last_count = 0;
        }
    }

    /* initialize purge queue */
    rc = CreateQueue( &purge_queue, resmon_config.purge_queue_size, PURGE_ST_COUNT - 1,
                      PURGE_FDBK_COUNT );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, RESMON_TAG, "Error %d initializing purge queue", rc );
        return rc;
    }

    /* start purge threads */
    rc = start_purge_threads( resmon_config.nb_threads_purge );
    if ( rc )
        return rc;

    if ( module_args.mode == RESMON_PURGE_OST )
    {
        rc = pthread_create( &trigger_check_thread_id, NULL, force_ost_trigger_thr, NULL );
    }
    else if ( module_args.mode == RESMON_PURGE_FS )
    {
        rc = pthread_create( &trigger_check_thread_id, NULL, force_fs_trigger_thr, NULL );
    }
    else if ( module_args.mode == RESMON_PURGE_CLASS )
    {
        rc = pthread_create( &trigger_check_thread_id, NULL, force_purge_class_thr, NULL );
    }
    else
    {
        rc = pthread_create( &trigger_check_thread_id, NULL, trigger_check_thr, NULL );
    }

    if ( rc != 0 )
    {
        rc = errno;
        DisplayLog( LVL_CRIT, RESMON_TAG,
                    "Error %d starting main thread of Resource Monitor: %s", rc,
                    strerror( rc ) );
        return rc;
    }

    return 0;
}

int Stop_ResourceMonitor()
{
    terminate = TRUE;
    abort_purge();
    return 0;
}

static int volatile waiting = 0;

int Wait_ResourceMonitor( void )
{
    void          *returned;
    int rc = 0;

    /* /!\ pb: 2 threads cannot join the same other thread.
     * In one shot mode, the main thread is already waiting
     * for purge to end. Thus, the signal manager thread
     * gets an error when trying to join it after abort.
     */
    if (!waiting )
    {
        /* no lock here, we consider the sigterm is not simultaneous with module start */
        if ( trigger_check_thread_id != ((pthread_t)-1) )
        {
            waiting = 1;
            rc = pthread_join( trigger_check_thread_id, &returned );
            if ( rc != 0 )
                DisplayLog( LVL_MAJOR, RESMON_TAG, "pthread_join() returned error %d", rc );
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


void Dump_ResourceMonitor_Stats( void )
{
    unsigned int   status_tab[PURGE_ST_COUNT];
    unsigned long long feedback_tab[PURGE_FDBK_COUNT];

    unsigned int   nb_waiting, nb_items;
    time_t         last_submitted, last_started, last_ack;

    unsigned long long sz;
    char           tmp_buff[256];
    char           trigstr[256];
    time_t         now = time( NULL );
    int            i;
    struct tm      paramtm;


    /* Stats about triggers */

    DisplayLog( LVL_MAJOR, "STATS", "======= Resource Monitor stats ======" );

    /* sanity check */
    if ( trigger_status_list != NULL )
        for ( i = 0; i < resmon_config.trigger_count; i++ )
        {

            snprintf( trigstr, 256, "Trigger #%u (%s)", i,
                      trigger2str( resmon_config.trigger_list[i].type ) );

            switch ( trigger_status_list[i].status )
            {
            case TRIG_NOT_CHECKED:     /* not checked yet */
                DisplayLog( LVL_MAJOR, "STATS", "%-30s: not checked yet.", trigstr );
                break;
            case TRIG_BEING_CHECKED:   /* currently beeing checked */
                DisplayLog( LVL_MAJOR, "STATS", "%-30s: being checked.", trigstr );
                break;
            case TRIG_PURGE_RUNNING:   /* purge running for this trigger */
                DisplayLog( LVL_MAJOR, "STATS", "%-30s: purge running.", trigstr );
                break;
            case TRIG_OK:      /* no purge is needed */
                strftime( tmp_buff, 256, "%Y/%m/%d %T",
                          localtime_r( &trigger_status_list[i].last_check, &paramtm ) );
                DisplayLog( LVL_MAJOR, "STATS", "%-30s: OK (last check: %s).", trigstr, tmp_buff );
                break;
            case TRIG_NO_LIST: /* no list available */
                strftime( tmp_buff, 256, "%Y/%m/%d %T",
                          localtime_r( &trigger_status_list[i].last_check, &paramtm ) );
                DisplayLog( LVL_MAJOR, "STATS", "%-30s: no list available (last check: %s).",
                            trigstr, tmp_buff );
                break;
            case TRIG_NOT_ENOUGH:      /* not enough candidates */
                strftime( tmp_buff, 256, "%Y/%m/%d %T",
                          localtime_r( &trigger_status_list[i].last_check, &paramtm ) );
                DisplayLog( LVL_MAJOR, "STATS",
                            "%-30s: last purge (%s) was incomplete: not enough eligible files.",
                            trigstr, tmp_buff );
                break;

            case TRIG_CHECK_ERROR:     /* Misc Error */
                strftime( tmp_buff, 256, "%Y/%m/%d %T",
                          localtime_r( &trigger_status_list[i].last_check, &paramtm ) );
                DisplayLog( LVL_MAJOR, "STATS", "%-30s: an error occured at last check (%s).",
                            trigstr, tmp_buff );
                break;

            case TRIG_ABORTED:     /*  */
                strftime( tmp_buff, 256, "%Y/%m/%d %T",
                          localtime_r( &trigger_status_list[i].last_check, &paramtm ) );
                DisplayLog( LVL_MAJOR, "STATS", "%-30s: aborted during last check (%s)", trigstr,
                            tmp_buff );
                break;

            case TRIG_UNSUPPORTED:     /* Trigger not supported in this mode */
                DisplayLog( LVL_MAJOR, "STATS", "%-30s: not supported in this mode.", trigstr );
                break;

            }
        }

    /* Purge stats */

    RetrieveQueueStats( &purge_queue, &nb_waiting, &nb_items, &last_submitted, &last_started,
                        &last_ack, status_tab, feedback_tab );

    DisplayLog( LVL_MAJOR, "STATS", "============ Purge stats ============" );
    DisplayLog( LVL_MAJOR, "STATS", "idle purge threads       = %u", nb_waiting );
    DisplayLog( LVL_MAJOR, "STATS", "purge operations pending = %u", nb_items );
    DisplayLog( LVL_MAJOR, "STATS", "purge status:" );

    for ( i = 0; i < PURGE_ST_COUNT; i++ )
    {
        /* always display PURGE_OK count and display error only if they have occured */
        if ( ( status_tab[i] > 0 ) || ( i == PURGE_OK ) )
            DisplayLog( LVL_MAJOR, "STATS", "    %-30s = %u", purge_status_descr[i],
                        status_tab[i] );
    }

    sz = feedback_tab[PURGE_FDBK_BLOCKS] * DEV_BSIZE;

    DisplayLog( LVL_MAJOR, "STATS", "total purged volume = %llu (%s)", sz,
                FormatFileSize( tmp_buff, 256, sz ) );

    if ( last_submitted )
        DisplayLog( LVL_MAJOR, "STATS", "last file submitted %2d s ago",
                    ( int ) ( now - last_submitted ) );

    if ( last_started )
        DisplayLog( LVL_MAJOR, "STATS", "last file handled   %2d s ago",
                    ( int ) ( now - last_started ) );

    if ( last_ack )
        DisplayLog( LVL_MAJOR, "STATS", "last file purged    %2d s ago",
                    ( int ) ( now - last_ack ) );

}
