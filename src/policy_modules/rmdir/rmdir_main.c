/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009 CEA/DAM
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

#define RMDIR_TAG "Rmdir"

#include "RobinhoodConfig.h"
#include "RobinhoodMisc.h"
#include "rmdir.h"
#include "queue.h"
#include "Memory.h"
#include "xplatform_print.h"
#include <errno.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>


/* ----- queue management ----- */

static entry_queue_t rmdir_queue;

/* rmdir status */
#define RMDIR_OK            0
#define RMDIR_DIR_MODIFIED  1
#define RMDIR_DIR_MOVED     2
#define RMDIR_DIR_WHITELISTED  3
#define RMDIR_ERROR         4

#define RMDIR_STATUS_COUNT  5

/* feedback (stats) from rmdir queue */
typedef enum
{
    RMDIR_FDBK_NBR = 0,
    RMDIR_FDBK_VOL,

    RMDIR_FDBK_COUNT
} rmdir_fdbk_t;

/* ----- Module configuration ----- */

rmdir_config_t rmdir_config;
static int     rmdir_flags = 0;

#define one_shot_mode (rmdir_flags & FLAG_ONCE)
#define dry_run (rmdir_flags & FLAG_DRY_RUN)

/* main thread */
static pthread_t rmdir_thr_id;

/* array of rmdir threads */
static pthread_t *thread_ids = NULL;

static time_t  last_rmdir = 0;

/* fs device */
static dev_t fsdev = 0;

#define CHECK_QUEUE_INTERVAL    1

/* ---- Internal functions ---- */

/**
 * Rmdir helper
 * @return posix error code (from errno)
 */

static int Rmdir_ByPath( const char *dir_path )
{
    /* TODO remove entry in database */
    DisplayLog( LVL_FULL, RMDIR_TAG, "Rmdir(%s)", dir_path );
    if ( !dry_run )
      {
          if ( rmdir( dir_path ) != 0 )
              return errno;
      }
    return 0;
}

/**
 * Remove a directory recursively
 */
static int Recursive_Rmdir_ByPath( lmgr_t * lmgr,
                                   const entry_id_t * dir_id,
                                   const attr_set_t * dir_attrs,
                                   unsigned long long * p_blocks,
                                   unsigned int * p_count )
{
    DIR *dirp;
    int rc;
    struct dirent entry;
    struct dirent * cookie;
    int err = 0;
    entry_id_t id;
    const char * dir_path;

    if (!ATTR_MASK_TEST(dir_attrs, fullpath))
    {
        DisplayLog( LVL_CRIT, RMDIR_TAG, "Missing mandatory attribute 'fullpath'" );
        return EINVAL;
    }
    else
        dir_path = ATTR(dir_attrs, fullpath);

    if ((dirp = opendir(dir_path)) == NULL)
    {
        rc = errno;
        DisplayLog( LVL_CRIT, RMDIR_TAG, "opendir failed on %s: %s", dir_path,
                    strerror(rc) );
        return rc;
    }

    do
    {
        struct stat stat_buf;
        attr_set_t  attr_set;

        rc = readdir_r(dirp, &entry, &cookie);

        if ( rc != 0 || ( rc == 0 && cookie == NULL ) )
            /* end of directory */
            break;

        /* ignore . and .. */
        if ( !strcmp( entry.d_name, "." ) || !strcmp( entry.d_name, ".." ) )
            continue;

        ATTR_MASK_INIT( &attr_set );

        sprintf(ATTR(&attr_set, fullpath), "%s/%s", dir_path, entry.d_name);
        ATTR_MASK_SET( &attr_set, fullpath );
        strcpy(ATTR(&attr_set, name), entry.d_name);
        ATTR_MASK_SET( &attr_set, name );
        ATTR(&attr_set, parent_id) = *dir_id;
        ATTR_MASK_SET( &attr_set, parent_id );

        if (lstat(ATTR(&attr_set, fullpath), &stat_buf) == -1)
        {
            rc = errno;
            DisplayLog( LVL_CRIT, RMDIR_TAG,
                        "lstat failed on %s (skipped): %s", ATTR(&attr_set, fullpath),
                        strerror(rc) );
            err = rc;
            continue;
        }

        /* set posix attrs in attr struct */
        PosixStat2EntryAttr( &stat_buf, &attr_set, TRUE );

        if ( fsdev != stat_buf.st_dev )
        {
            /* check if filesystem was remounted */
            struct stat root_md;
            /* is the FS root changed: file system may have been remounted.
             * else: the entry is not in the same filesystem
             */
            /* 1) check fs root dev_id */
            if (stat( global_config.fs_path, &root_md ) == -1)
            {
                int rc = -errno;
                DisplayLog( LVL_CRIT, RMDIR_TAG,
                            "stat failed on %s: %s", global_config.fs_path, strerror(-rc) );
                DisplayLog( LVL_CRIT, RMDIR_TAG, "ERROR accessing FileSystem: EXITING." );
                Exit( rc );
            }
            if (root_md.st_dev != fsdev)
            {
                /* manage dev id change after umount/mount */
                DisplayLog( LVL_MAJOR, RMDIR_TAG, "WARNING: Filesystem device id changed (old=%"PRI_DT", new=%"PRI_DT"): "
                            "checking if it has been remounted", fsdev, root_md.st_dev );
                if (ResetFS())
                {
                    DisplayLog( LVL_CRIT, RMDIR_TAG, "Filesystem was unmounted!!! EXITING!" );
                    Exit( 1 );
                }
                /* update current fsdev */
                fsdev = get_fsdev();
            }
            /* else: root is still the same */

            if ( global_config.stay_in_fs && (fsdev != stat_buf.st_dev))
            {
                DisplayLog( LVL_CRIT, RMDIR_TAG, "%s is not in the same filesystem as %s, skipping it",
                            ATTR(&attr_set, fullpath), global_config.fs_path );
                err = EXDEV;
                continue;
            }
        }

#ifdef _HAVE_FID
       /* get id for this entry */
       rc = Lustre_GetFidFromPath( ATTR(&attr_set, fullpath), &id );
       if (rc)
       {
            DisplayLog( LVL_CRIT, RMDIR_TAG, "Failed to retrieve id for %s, skipping entry: %s",
                        ATTR(&attr_set, fullpath), strerror(-rc) );
            continue;
       }
#else
       id.fs_key = get_fskey();
       id.inode  = stat_buf.st_ino;
       id.validator = stat_buf.st_ctime;
#endif

       if (S_ISDIR(stat_buf.st_mode))
       {
           rc = Recursive_Rmdir_ByPath( lmgr, &id, &attr_set, p_blocks, p_count );
           if (rc)
           {
                DisplayLog( LVL_CRIT, RMDIR_TAG,
                            "recursive rmdir failed on %s (skipped): %s",
                            ATTR(&attr_set, fullpath), strerror(rc) );
                err = rc;
           }
       }
       else /* non directory entry */
       {
            if (!dry_run)
            {
                if (unlink(ATTR(&attr_set, fullpath)))
                {
                    DisplayLog( LVL_CRIT, RMDIR_TAG,
                                "rmdir failed on %s (skipped): %s", ATTR(&attr_set, fullpath),
                                strerror(rc) );
                    err = rc;
                    continue;
                }
            }

           *p_blocks += stat_buf.st_blocks;
           (*p_count) += 1;

           /* this is the last removal if nlink was 1 */
           rc = ListMgr_Remove( lmgr, &id, &attr_set, stat_buf.st_nlink == 1 );
           if (rc)
           {
                DisplayLog( LVL_CRIT, RMDIR_TAG, "Error %d removing entry from database (%s)",
                            rc, ATTR(&attr_set, fullpath) );
           }
       }
    } while( 1 );

   closedir(dirp);

   /* finally delete the directory itself */
   rc = Rmdir_ByPath( dir_path );
   if (rc)
   {
        DisplayLog( LVL_CRIT, RMDIR_TAG, "rmdir failed on %s: %s", dir_path,
                    strerror(rc) );
        /* if an previous error occurs, keep it, because this one
         * will be ENOTEMPTY */
        if (!err)
            err = rc;
   }
   else
   {
       if ( dir_id != NULL )
       {
           /* no hardlinks for directories */
           rc = ListMgr_Remove( lmgr, dir_id, dir_attrs, TRUE );
           if (rc)
           {
                DisplayLog( LVL_CRIT, RMDIR_TAG, "Error %d removing entry from database (%s)",
                            rc, dir_path );
           }
       }

       (*p_count) += 1;
   }

   return err;
}


/**
 * Count entries in directory (excluding . and ..)
 * @return value > 0
 */
static int nb_dir_entries( const char *dirpath )
{
    DIR           *dirp;
    int            rc;
    unsigned int   count = 0;
    struct dirent  entry;
    struct dirent *cookie = NULL;


    if ( ( dirp = opendir( dirpath ) ) == NULL )
      {
          rc = errno;
          DisplayLog( LVL_CRIT, RMDIR_TAG, "opendir on %s failed: Error %d: %s",
                      dirpath, rc, strerror( rc ) );
          return -rc;
      }

    while ( 1 )
      {
          rc = readdir_r( dirp, &entry, &cookie );
          if ( rc != 0 || ( rc == 0 && cookie == NULL ) )
              break;

          /* do not count . or .. */
          if ( !strcmp( entry.d_name, "." ) || !strcmp( entry.d_name, ".." ) )
              continue;

          count++;

      }

    closedir( dirp );

    return count;
}

enum rmdir_type_e { RMDIR_EMPTY, RMDIR_RECURSE };

typedef struct rmdir_item__
{
    entry_id_t     entry_id;
    attr_set_t     entry_attr;
    enum rmdir_type_e   rmdir_type;
} rmdir_item_t;

/**
 *  Alloc a new rmdir item so it can be pushes to the rmdir queue.
 */
static void * Entry2RmdirItem( entry_id_t * p_entry_id, attr_set_t * p_attr_set,
                               enum rmdir_type_e rmdir_type )
{
    rmdir_item_t  *new_entry;

    new_entry = ( rmdir_item_t * ) MemAlloc( sizeof( rmdir_item_t ) );
    if ( !new_entry )
        return NULL;

    new_entry->entry_id = *p_entry_id;
    new_entry->entry_attr = *p_attr_set;
    new_entry->rmdir_type = rmdir_type;

    return new_entry;
}

/**
 * Free a Rmdir Item (and the resources of entry_attr).
 */
static void FreeRmdirItem( rmdir_item_t * item )
{
    ListMgr_FreeAttrs( &item->entry_attr );
    MemFree( item );
}

/**
 *  Sum the number of acks from a status tab
 */
static inline unsigned int ack_count( unsigned int *status_tab )
{
    unsigned int   i, sum;
    sum = 0;

    for ( i = 0; i < RMDIR_STATUS_COUNT; i++ )
        sum += status_tab[i];

    return sum;
}

/**
 * This function selects directories to be removed
 * and submit them to workers.
 */
static int perform_rmdir( unsigned int *p_nb_removed )
{
    int            rc;
    lmgr_t         lmgr;
    struct lmgr_iterator_t *it = NULL;

    lmgr_filter_t  filter;
    filter_value_t fval;
    lmgr_sort_type_t sort_type;

    attr_set_t     attr_set;
    entry_id_t     entry_id;

    unsigned int   status_tab1[RMDIR_STATUS_COUNT];
    unsigned int   status_tab2[RMDIR_STATUS_COUNT];

    unsigned int   submitted_dirs, nb_in_queue, nb_rmdir_pending;

    int            attr_mask_sav;
    int            end_of_list = FALSE;
    char           timestamp[1024];

    if ( p_nb_removed )
        *p_nb_removed = 0;

    /* we assume that purging is a rare event
     * and we don't want to use a DB connection all the time
     * so we connect only at purge time */

    rc = ListMgr_InitAccess( &lmgr );
    if ( rc )
      {
          DisplayLog( LVL_CRIT, RMDIR_TAG,
                      "Could not connect to database (error %d). "
                      "Dir removal cancelled.", rc );
          return rc;
      }

    /**
     * Do nothing if no previous scan was done.
     */
    if ( ListMgr_GetVar( &lmgr, LAST_SCAN_END_TIME, timestamp ) != DB_SUCCESS )
    {
          DisplayLog( LVL_MAJOR, RMDIR_TAG,
                      "Full FS Scan has never been done. "
                      "Cannot build a rmdir list." );
          ListMgr_CloseAccess( &lmgr );
          return ENOENT;
    }
    else
    {
          time_t         last_scan = atoi( timestamp );
          struct tm      date;


          localtime_r( &last_scan, &date );
          DisplayLog( LVL_EVENT, RMDIR_TAG,
                      "Building a rmdir list from last full FS Scan: "
                      "%.4d/%.2d/%.2d %.2d:%.2d:%.2d", 1900 + date.tm_year,
                      date.tm_mon + 1, date.tm_mday, date.tm_hour, date.tm_min,
                      date.tm_sec );
    }

    ATTR_MASK_INIT( &attr_set );

    /* Retrieve at least: fullpath and last_mod
     * because they are used for checking if directory changed.
     * Also retrieve info needed for whitelist rules.
     */
    /* parent_id and name needed for the new ListMgr_Remove prototype */
    ATTR_MASK_SET( &attr_set, parent_id );
    ATTR_MASK_SET( &attr_set, name );

    ATTR_MASK_SET( &attr_set, fullpath );
    ATTR_MASK_SET( &attr_set, last_mod );
    attr_set.attr_mask |= policies.rmdir_policy.global_attr_mask;

    /* always sort by last_mod */
    sort_type.attr_index = ATTR_INDEX_last_mod;
    sort_type.order = SORT_ASC;

    rc = lmgr_simple_filter_init( &filter );
    if ( rc )
      {
          ListMgr_CloseAccess( &lmgr );
          return rc;
      }
    /* do not retrieve 'invalid' entries */
    fval.value.val_bool = FALSE;
    rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_invalid, EQUAL, fval, FILTER_FLAG_ALLOW_NULL );
    if ( rc )
      {
          ListMgr_CloseAccess( &lmgr );
          return rc;
      }

    /* only consider directories */
    fval.value.val_str = STR_TYPE_DIR;
    rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_type, EQUAL, fval, 0 );
    if ( rc )
      {
          ListMgr_CloseAccess( &lmgr );
          return rc;
      }

    /* only consider empty directories */
    fval.value.val_uint = 0;
    rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_dircount, EQUAL, fval, 0 );
    if ( rc )
      {
          ListMgr_CloseAccess( &lmgr );
          return rc;
      }

    /* only consider directories strictly older than (now - age_rm_empty_dirs) */
    fval.value.val_uint = ( time( NULL ) - policies.rmdir_policy.age_rm_empty_dirs );
    rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_last_mod, LESSTHAN_STRICT,
                                 fval, 0 );
    if ( rc )
      {
          ListMgr_CloseAccess( &lmgr );
          return rc;
      }

    DisplayLog( LVL_EVENT, RMDIR_TAG, "Starting empty directory removal" );

    it = ListMgr_Iterator( &lmgr, &filter, &sort_type, NULL );

    lmgr_simple_filter_free( &filter );

    if ( it == NULL )
      {
          DisplayLog( LVL_CRIT, RMDIR_TAG,
                      "Error retrieving list of empty directories from "
                      "database. Operation cancelled." );
          return -1;
      }

    attr_mask_sav = attr_set.attr_mask;

    /* retrieve info before removing dirs, so we can make a delta after */
    RetrieveQueueStats( &rmdir_queue, NULL, NULL, NULL, NULL, NULL,
                        status_tab1, NULL );
    submitted_dirs = 0;

    /* submit all eligible dirs */
    do
      {
          /* reset attr_mask, if it was altered by last ListMgr_GetNext() call */
          attr_set.attr_mask = attr_mask_sav;

          memset( &entry_id, 0, sizeof( entry_id_t ) );

          rc = ListMgr_GetNext( it, &entry_id, &attr_set );

          if ( rc == DB_END_OF_LIST )
            {
                end_of_list = TRUE;
                break;
            }
          else if ( rc != 0 )
            {
                DisplayLog( LVL_CRIT, RMDIR_TAG, "Error %d getting next entry "
                            "of iterator", rc );
                break;
            }

          /* submit directory */
          rc = Queue_Insert( &rmdir_queue, Entry2RmdirItem( &entry_id,
                             &attr_set, RMDIR_EMPTY ) );
          if ( rc )
              return rc;

          submitted_dirs++;

      }
    while ( 1 );                /* until DB_END_OF_LIST or error is returned */

    /* close iterator and db access */
    ListMgr_CloseIterator( it );
    ListMgr_CloseAccess( &lmgr );

    /* wait for end of rmdir pass  */
    do
      {
          RetrieveQueueStats( &rmdir_queue, NULL, &nb_in_queue, NULL, NULL,
                              NULL, status_tab2, NULL );

          /* nb of rmdir operation pending =
           * nb_enqueued - ( nb ack after - nb ack before ) */
          nb_rmdir_pending = submitted_dirs + ack_count( status_tab1 )
                                - ack_count( status_tab2 );

          DisplayLog( LVL_DEBUG, RMDIR_TAG,
                      "Waiting for the end of empty directory removal: "
                      "still %u dirs to be removed "
                      "(%u in queue, %u beeing processed)",
                      nb_rmdir_pending, nb_in_queue,
                      nb_rmdir_pending - nb_in_queue );

          if ( ( nb_in_queue != 0 ) || ( nb_rmdir_pending != 0 ) )
              rh_sleep( CHECK_QUEUE_INTERVAL );

      }
    while ( ( nb_in_queue != 0 ) || ( nb_rmdir_pending != 0 ) );

    if ( p_nb_removed )
        *p_nb_removed = status_tab2[RMDIR_OK] - status_tab1[RMDIR_OK];

    if ( end_of_list )
        return 0;
    else
        return rc;
}

/**
 * build a filter from policies, to optimize DB queries.
 */
static int set_rmdir_optimization_filters(lmgr_filter_t * p_filter)
{
    /** @TODO build a filter for getting the union of all filesets/conditions */

    /* If there is a single policy, try to convert its condition
     * to a simple filter.
     */
    if ( policies.rmdir_policy.recursive_rmdir_count == 1 )
    {
        if ( convert_boolexpr_to_simple_filter(
                &policies.rmdir_policy.recursive_rmdir_rules[0].bool_expr,
                p_filter ) )
        {
            DisplayLog( LVL_FULL, RMDIR_TAG, "Could not convert recursive "
                        "rmdir policy to simple filter." );
        }
    }

    return 0;
}

/**
 * This function selects directories to be removed recursively
 * and submit them to workers.
 */
static int perform_rmdir_recurse( unsigned int *p_nb_top,
                                  unsigned int *p_nb_total,
                                  unsigned long long * p_volume )
{
    int            rc;
    lmgr_t         lmgr;
    struct lmgr_iterator_t *it = NULL;

    lmgr_filter_t  filter;
    filter_value_t fval;
    lmgr_sort_type_t sort_type;

    attr_set_t     attr_set;
    entry_id_t     entry_id;

    unsigned int   status_tab1[RMDIR_STATUS_COUNT];
    unsigned int   status_tab2[RMDIR_STATUS_COUNT];
    unsigned int   submitted_dirs, nb_in_queue, nb_rmdir_pending;

    /* feedback stats */
    unsigned long long stats_before[RMDIR_FDBK_COUNT];
    unsigned long long stats_after[RMDIR_FDBK_COUNT];

    int            attr_mask_sav;
    int            end_of_list = FALSE;
    char           timestamp[1024];

    if ( p_nb_top )
        *p_nb_top = 0;
    if ( p_nb_total )
        *p_nb_total = 0;

    /* we assume that purging is a rare event
     * and we don't want to use a DB connection all the time
     * so we connect only at purge time */

    rc = ListMgr_InitAccess( &lmgr );
    if ( rc )
      {
          DisplayLog( LVL_CRIT, RMDIR_TAG,
                      "Could not connect to database (error %d). "
                      "Dir removal cancelled.", rc );
          return rc;
      }

    /**
     * Do nothing if no previous scan was done.
     */
    if ( ListMgr_GetVar( &lmgr, LAST_SCAN_END_TIME, timestamp ) != DB_SUCCESS )
      {
          DisplayLog( LVL_MAJOR, RMDIR_TAG,
                      "Full FS Scan has never been done. "
                      "Cannot build a rmdir list." );
          ListMgr_CloseAccess( &lmgr );
          return ENOENT;
      }
    else
      {
          time_t         last_scan = atoi( timestamp );
          struct tm      date;


          localtime_r( &last_scan, &date );
          DisplayLog( LVL_EVENT, RMDIR_TAG,
                      "Building a rmdir list from last full FS Scan: "
                      "%.4d/%.2d/%.2d %.2d:%.2d:%.2d",
                      1900 + date.tm_year, date.tm_mon + 1, date.tm_mday,
                      date.tm_hour, date.tm_min, date.tm_sec );
      }

    ATTR_MASK_INIT( &attr_set );

    /* Retrieve at least: fullpath and last_mod
     * because they are used for checking if directory changed.
     * Also retrieve info needed for whitelist rules.
     */
    /* parent_id and name needed for the new ListMgr_Remove prototype */
    ATTR_MASK_SET( &attr_set, parent_id );
    ATTR_MASK_SET( &attr_set, name );

#ifndef _HAVE_FID
    /* not needed for fid adressing */
    ATTR_MASK_SET( &attr_set, fullpath ) ;
#endif

    ATTR_MASK_SET( &attr_set, last_mod );
    attr_set.attr_mask |= policies.rmdir_policy.global_attr_mask;

    /* always sort by last_mod */
    sort_type.attr_index = ATTR_INDEX_last_mod;
    sort_type.order = SORT_ASC;

    rc = lmgr_simple_filter_init( &filter );
    if ( rc )
      {
          ListMgr_CloseAccess( &lmgr );
          return rc;
      }
    /* do not retrieve 'invalid' entries */
    fval.value.val_bool = FALSE;
    rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_invalid, EQUAL, fval, FILTER_FLAG_ALLOW_NULL );
    if ( rc )
      {
          ListMgr_CloseAccess( &lmgr );
          return rc;
      }

    /* only consider directories */
    fval.value.val_str = STR_TYPE_DIR;
    rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_type, EQUAL, fval, 0 );
    if ( rc )
      {
          ListMgr_CloseAccess( &lmgr );
          return rc;
      }

    /* add simple policy filter, if possible */
    set_rmdir_optimization_filters( &filter );

    DisplayLog( LVL_EVENT, RMDIR_TAG, "Starting recursive directory removal" );

    it = ListMgr_Iterator( &lmgr, &filter, &sort_type, NULL );

    lmgr_simple_filter_free( &filter );

    if ( it == NULL )
      {
          DisplayLog( LVL_CRIT, RMDIR_TAG,
                      "Error retrieving list of directories from database. "
                      "Operation cancelled." );
          return -1;
      }

    attr_mask_sav = attr_set.attr_mask;

    /* retrieve info before removing dirs, so we can make a delta after */
    RetrieveQueueStats( &rmdir_queue, NULL, NULL, NULL, NULL, NULL,
                        status_tab1, stats_before );
    submitted_dirs = 0;

    /* submit all eligible dirs */
    do
      {
          /* reset attr_mask, if it was altered by last ListMgr_GetNext() call */
          attr_set.attr_mask = attr_mask_sav;

          memset( &entry_id, 0, sizeof( entry_id_t ) );

          rc = ListMgr_GetNext( it, &entry_id, &attr_set );

          if ( rc == DB_END_OF_LIST )
            {
                end_of_list = TRUE;
                break;
            }
          else if ( rc != 0 )
            {
                DisplayLog( LVL_CRIT, RMDIR_TAG, "Error %d getting next entry of iterator", rc );
                break;
            }

          /* submit directory */
          rc = Queue_Insert( &rmdir_queue, Entry2RmdirItem( &entry_id, &attr_set, RMDIR_RECURSE ) );
          if ( rc )
              return rc;

          submitted_dirs++;

      }
    while ( 1 );                /* until DB_END_OF_LIST or error is returned */

    /* close iterator and db access */
    ListMgr_CloseIterator( it );
    ListMgr_CloseAccess( &lmgr );

    /* wait for end of rmdir pass  */
    do
      {
          RetrieveQueueStats( &rmdir_queue, NULL, &nb_in_queue, NULL, NULL, NULL, status_tab2,
                              stats_after );

          /* nb of rmdir operation pending = nb_enqueued - ( nb ack after - nb ack before ) */
          nb_rmdir_pending = submitted_dirs + ack_count( status_tab1 ) - ack_count( status_tab2 );

          DisplayLog( LVL_DEBUG, RMDIR_TAG,
                      "Waiting for the end of recursive directory removal: still %u dirs to be removed (%u in queue, %u beeing processed)",
                      nb_rmdir_pending, nb_in_queue, nb_rmdir_pending - nb_in_queue );

          if ( ( nb_in_queue != 0 ) || ( nb_rmdir_pending != 0 ) )
              rh_sleep( CHECK_QUEUE_INTERVAL );

      }
    while ( ( nb_in_queue != 0 ) || ( nb_rmdir_pending != 0 ) );

    if ( p_nb_top )
        *p_nb_top = status_tab2[RMDIR_OK] - status_tab1[RMDIR_OK];
    if ( p_nb_total )
        *p_nb_total = stats_after[RMDIR_FDBK_NBR] - stats_before[RMDIR_FDBK_NBR];
    if ( p_volume )
        *p_volume = stats_after[RMDIR_FDBK_VOL] - stats_before[RMDIR_FDBK_VOL];

    if ( end_of_list )
        return 0;
    else
        return rc;
}


/**
 * Tag dir as invalid in database
 */
inline static int invalidate_dir( lmgr_t * lmgr, entry_id_t * p_entry_id )
{
    attr_set_t     new_attr_set;
    int            rc;

    ATTR_MASK_INIT( &new_attr_set );
    ATTR_MASK_SET( &new_attr_set, invalid );
    ATTR( &new_attr_set, invalid ) = TRUE;

    /* update the entry */
    rc = ListMgr_Update( lmgr, p_entry_id, &new_attr_set );
    if ( rc )
        DisplayLog( LVL_CRIT, RMDIR_TAG, "Error %d tagging directory as invalid in database.", rc );

    return rc;
}

inline static int update_dir( lmgr_t * lmgr, entry_id_t * p_entry_id, attr_set_t * p_attr_set )
{
    int            rc;

    p_attr_set->attr_mask &= ~readonly_attr_set;

#ifdef ATTR_INDEX_creation_time
    /* never update creation time */
    ATTR_MASK_UNSET( p_attr_set, creation_time );
#endif

    rc = ListMgr_Update( lmgr, p_entry_id, p_attr_set );
    if ( rc )
        DisplayLog( LVL_CRIT, RMDIR_TAG, "Error %d updating directory in database.", rc );

    return rc;
}

/**
 *  Worker thread that performs rmdir
 */
static void   *Thr_Rmdir( void *arg )
{
    attr_set_t     new_attr_set;

    struct stat    entry_md;
    int            rc;
    lmgr_t         lmgr;

    policy_match_t match;
    void          *p_queue_entry;
    rmdir_item_t  *p_item;

    rc = ListMgr_InitAccess( &lmgr );
    if ( rc )
      {
          DisplayLog( LVL_CRIT, RMDIR_TAG, "Could not connect to database (error %d). Exiting.",
                      rc );
          exit( rc );
      }

    while ( Queue_Get( &rmdir_queue, &p_queue_entry ) == 0 )
      {
          p_item = ( rmdir_item_t * ) p_queue_entry;

          ATTR_MASK_INIT( &new_attr_set );

#ifndef _HAVE_FID
        /* 1) If fullpath is not set, invalidate the entry (except if entries are adrressed by FID) ... */
        if (!ATTR_MASK_TEST( &p_item->entry_attr, fullpath ))
        {
            DisplayLog( LVL_DEBUG, RMDIR_TAG,
                        "Warning: directory fullpath is not set. Tagging it invalid." );
            invalidate_dir( &lmgr, &p_item->entry_id );

            /* Notify that this entry has been processed and is erroneous */
            Queue_Acknowledge( &rmdir_queue, RMDIR_ERROR, NULL, 0 );

            /* free entry resources */
            FreeRmdirItem( p_item );

            /* process next entries */
            continue;
        }
#else
        /* 1) Update full path */
        /* FIXME: we should get all paths of the directory to check if any of them match the policies */
        /* TODO: also update parent_id+name */
        rc = Lustre_GetFullPath( &p_item->entry_id, ATTR(&p_item->entry_attr, fullpath), RBH_PATH_MAX );
        if ( rc )
        {
            if (rc == -ENOENT || rc == -ESTALE)
            {
                DisplayLog( LVL_EVENT, RMDIR_TAG, "Directory with fid "DFID" disappeared (previous name %s), tagging it invalid: (%d) %s",
                            PFID( &p_item->entry_id), ATTR(&p_item->entry_attr, name), rc, strerror(-rc) );
            }
            else
            {
                DisplayLog( LVL_MAJOR, RMDIR_TAG, "Can not get path for fid "DFID", tagging it invalid: (%d) %s",
                            PFID( &p_item->entry_id), rc, strerror(-rc) );
            }
            invalidate_dir( &lmgr, &p_item->entry_id );

            /* Notify that this entry has been processed and is erroneous */
            Queue_Acknowledge( &rmdir_queue, RMDIR_ERROR, NULL, 0 );

           /* free entry resources */
           FreeRmdirItem( p_item );

           /* process next entries */
           continue;
        }
        ATTR_MASK_SET( &p_item->entry_attr, fullpath );
        ATTR_MASK_SET( &p_item->entry_attr, path_update );
        ATTR( &p_item->entry_attr, path_update ) = time( NULL );
#endif

        DisplayLog(LVL_FULL, RMDIR_TAG, "Considering entry %s ("DFID")",
                   ATTR(&p_item->entry_attr, fullpath), PFID(&p_item->entry_id));

        /* 2) Perform lstat on entry */
        if ( lstat( ATTR( &p_item->entry_attr, fullpath ), &entry_md ) != 0 )
        {
            /* If lstat returns an error, invalidate the entry */
            DisplayLog( LVL_DEBUG, RMDIR_TAG, "lstat() failed on %s. Tagging it invalid.",
                        ATTR( &p_item->entry_attr, fullpath ) );
            invalidate_dir( &lmgr, &p_item->entry_id );

            /* Notify that this entry has been processed and has probably moved */
            Queue_Acknowledge( &rmdir_queue, RMDIR_DIR_MOVED, NULL, 0 );

            /* free entry resources */
            FreeRmdirItem( p_item );

            /* process next entries */
            continue;
        }

          /* 3) check entry id */
#ifndef _HAVE_FID
          /* TODO: check against current fs_key */
          if ( ( entry_md.st_ino != p_item->entry_id.inode )
               || ( get_fskey() != p_item->entry_id.fs_key ) )
            {
                /* If it has changed, invalidate the entry
                 * (fullpath does not match entry_id, it will be updated
                 *  or removed at next FS scan). */
                DisplayLog( LVL_DEBUG, RMDIR_TAG,
                            "Inode of %s changed: old=<%llu,%llu>, "
                            "new=<%llu,%llu>. "
                            "Tagging it invalid.",
                            ATTR( &p_item->entry_attr, fullpath ),
                            ( unsigned long long ) p_item->entry_id.inode,
                            ( unsigned long long ) p_item->entry_id.fs_key,
                            ( unsigned long long ) entry_md.st_ino,
                            ( unsigned long long ) get_fskey() );

                invalidate_dir( &lmgr, &p_item->entry_id );

                /* Notify that this entry has been processed and has probably moved */
                Queue_Acknowledge( &rmdir_queue, RMDIR_DIR_MOVED, NULL, 0 );

                /* free entry resources */
                FreeRmdirItem( p_item );

                /* process next entries */
                continue;
            }
#endif
          /* convert posix attributes to internal structure */
          PosixStat2EntryAttr( &entry_md, &new_attr_set, TRUE );

          /* set update time  */
          ATTR_MASK_SET( &new_attr_set, md_update );
          ATTR( &new_attr_set, md_update ) = time(NULL);

          /* Merge missing attrs from database */
          ListMgr_MergeAttrSets( &new_attr_set, &p_item->entry_attr, FALSE );

          /* From here, assume that entry is valid */
          ATTR_MASK_SET( &new_attr_set, invalid );
          ATTR( &new_attr_set, invalid ) = FALSE;

          /* 4) check whitelist rules */

          /* this eventually set release_class to '@ignore@' */
          match = IsWhitelisted( &p_item->entry_id, &new_attr_set,
                                 RMDIR_POLICY );
          if ( match == POLICY_MATCH )
          {
                /* update DB and skip the entry */
                update_dir( &lmgr, &p_item->entry_id, &new_attr_set );

                /* Notify that this entry is whitelisted */
                Queue_Acknowledge( &rmdir_queue, RMDIR_DIR_WHITELISTED,
                                   NULL, 0 );

                /* free entry resources */
                FreeRmdirItem( p_item );
                continue;

          }
          else if ( match == POLICY_NO_MATCH )
          {
                /* set DEFAULT class for non-whitelisted dirs */
                strcpy( ATTR( &new_attr_set, release_class ), CLASS_DEFAULT );
                ATTR_MASK_SET( &new_attr_set, release_class );
                ATTR( &new_attr_set, rel_cl_update ) = time(NULL);
                ATTR_MASK_SET( &new_attr_set, rel_cl_update );
          }
          else
          {
                /* Cannot determine if entry is whitelisted: skip it
                 * (do nothing in database) */
                DisplayLog( LVL_MAJOR, RMDIR_TAG,
                            "Warning: cannot determine if dir %s is "
                            "whitelisted: skip it.",
                            ATTR( &p_item->entry_attr, fullpath ) );

                /* Notify error */
                Queue_Acknowledge( &rmdir_queue, RMDIR_ERROR, NULL, 0 );

                /* free entry resources */
                FreeRmdirItem( p_item );
                continue;
          }

          /* Empty dir removal */
          if ( p_item->rmdir_type == RMDIR_EMPTY )
          {
              /* 5) check that directory has not been modified. */

              if ( !ATTR_MASK_TEST( &p_item->entry_attr, last_mod ) )
                {
                    /* cannot determine if entry has been accessed: update and skip it */
                    DisplayLog( LVL_MAJOR, RMDIR_TAG,
                                "Warning: previous value of 'last_mod' is not available: "
                                "cannot determine if directory has been modified. "
                                "Skipping entry." );
                    update_dir( &lmgr, &p_item->entry_id, &new_attr_set );

                    /* Notify error */
                    Queue_Acknowledge( &rmdir_queue, RMDIR_ERROR, NULL, 0 );

                    /* free entry resources */
                    FreeRmdirItem( p_item );
                    continue;
                }

              if ( ATTR( &p_item->entry_attr, last_mod )
                        < ATTR( &new_attr_set, last_mod ) )
                {
                    DisplayLog( LVL_DEBUG, RMDIR_TAG,
                                "%s has been modified since it was listed. "
                                "Skipping entry.",
                                ATTR( &p_item->entry_attr, fullpath ) );
                    update_dir( &lmgr, &p_item->entry_id, &new_attr_set );

                    Queue_Acknowledge( &rmdir_queue, RMDIR_DIR_MODIFIED,
                                       NULL, 0 );
                    /* free entry resources */
                    FreeRmdirItem( p_item );
                    continue;
                }

              /* 6) check the directory is still empty */
              rc = nb_dir_entries( ATTR( &p_item->entry_attr, fullpath ) );

              if ( rc < 0 )
                {
                    /* probably an error opening in directory... */
                    /* update its info anyway */
                    update_dir( &lmgr, &p_item->entry_id, &new_attr_set );

                    Queue_Acknowledge( &rmdir_queue, RMDIR_ERROR, NULL, 0 );

                    /* free entry resources */
                    FreeRmdirItem( p_item );
                    continue;
                }
              else if ( rc > 0 )
                {
                    /* not empty */
                    DisplayLog( LVL_DEBUG, RMDIR_TAG,
                                "%s directory is not empty (it contains %u entries). "
                                "Skipping it.",
                                ATTR( &p_item->entry_attr, fullpath ), rc );

                    /* update directory info + dircount */

                    ATTR_MASK_SET( &new_attr_set, dircount );
                    ATTR( &new_attr_set, dircount ) = rc;

                    update_dir( &lmgr, &p_item->entry_id, &new_attr_set );

                    Queue_Acknowledge( &rmdir_queue, RMDIR_DIR_MODIFIED,
                                       NULL, 0 );
                    /* free entry resources */
                    FreeRmdirItem( p_item );
                    continue;
                }

              /* 7) rmdir and remove entry from database */
              rc = Rmdir_ByPath( ATTR( &p_item->entry_attr, fullpath ) );

              if ( rc )
                {
                    DisplayLog( LVL_DEBUG, RMDIR_TAG,
                                "Error removing directory %s: %s",
                                ATTR( &p_item->entry_attr, fullpath ),
                                strerror( rc ) );

                    update_dir( &lmgr, &p_item->entry_id, &new_attr_set );

                    Queue_Acknowledge( &rmdir_queue, RMDIR_ERROR, NULL, 0 );

                    /* free entry resources */
                    FreeRmdirItem( p_item );
                    continue;
                }
              else
                {
                    char           strmod[256];

                    /* dir has been successfully deleted */

                    /* report messages */

                    FormatDurationFloat( strmod, 256, time( NULL )
                                            - ATTR( &new_attr_set, last_mod ) );

                    DisplayLog( LVL_DEBUG, RMDIR_TAG,
                                "Removed empty dir %s, last modified %s ago",
                                ATTR( &p_item->entry_attr, fullpath ), strmod );

                    DisplayReport( "Removed empty dir %s unused for %s | "
                                   "last_mod=%" PRI_TT,
                                   ATTR( &p_item->entry_attr, fullpath ),
                                   strmod, (time_t)ATTR( &new_attr_set, last_mod ) );

                    /* remove it from database */
                    rc = ListMgr_Remove( &lmgr, &p_item->entry_id,
                                         &p_item->entry_attr, /* must be based on the DB content = old attrs */
                                         TRUE ); /* no hardlinks for directories */
                    if ( rc )
                        DisplayLog( LVL_CRIT, RMDIR_TAG,
                                   "Error %d removing directory from database.",
                                   rc );

                    /* ack to queue manager */
                    Queue_Acknowledge( &rmdir_queue, RMDIR_OK, NULL, 0 );
                    /* free entry resources */
                    FreeRmdirItem( p_item );

                }
            } /* end of empty dir removal */
            else if ( p_item->rmdir_type == RMDIR_RECURSE )
            {
                int i;
                int match = FALSE;
                unsigned int nb_entries = 0;
                unsigned long long blocks = 0;
                unsigned long long rmdir_stats[RMDIR_FDBK_COUNT];

                /* test if any recursive policy matches */
                for ( i = 0; i < policies.rmdir_policy.recursive_rmdir_count;
                      i++ )
                {
                    if ( EntryMatches( &p_item->entry_id, &new_attr_set,
                            &policies.rmdir_policy.recursive_rmdir_rules[i].bool_expr, NULL )
                            == POLICY_MATCH )
                    {
                         match = TRUE;
                         break;
                    }
                }

                if ( match )
                {
                    DisplayLog( LVL_FULL, RMDIR_TAG,
                                "%s matches recursive rmdir condition",
                                ATTR(&new_attr_set, fullpath) );

                    /* Recursive rmdir (needs lmgr for removing entries) */
                    rc = Recursive_Rmdir_ByPath( &lmgr, &p_item->entry_id,
                                                 &new_attr_set,
                                                 &blocks, &nb_entries );

                    if ( rc )
                    {
                           DisplayLog( LVL_VERB, RMDIR_TAG, "Error removing directory %s recursively: %s",
                                       ATTR( &p_item->entry_attr, fullpath ), strerror( rc ) );

                           update_dir( &lmgr, &p_item->entry_id, &new_attr_set );

                           rmdir_stats[RMDIR_FDBK_NBR] = nb_entries;
                           rmdir_stats[RMDIR_FDBK_VOL] = blocks*DEV_BSIZE;

                           Queue_Acknowledge( &rmdir_queue, RMDIR_ERROR, rmdir_stats, RMDIR_FDBK_COUNT );

                           /* free entry resources */
                           FreeRmdirItem( p_item );
                     }
                     else /* recursive rmdir successfull */
                     {
                        char volstr[256];

                         /* report messages */
                         DisplayLog( LVL_EVENT, RMDIR_TAG,
                                     "Recursively removed directory %s (content: %u entries, volume: %s)",
                                     ATTR( &p_item->entry_attr, fullpath ), nb_entries,
                                     FormatFileSize( volstr, 256, DEV_BSIZE*blocks ) );

                         DisplayReport( "Recursively removed dir %s | nb_entries=%u, volume=%s (%llu blocks)",
                                        ATTR( &p_item->entry_attr, fullpath ), nb_entries, volstr, blocks );

                         rmdir_stats[RMDIR_FDBK_NBR] = nb_entries;
                         rmdir_stats[RMDIR_FDBK_VOL] = blocks*DEV_BSIZE;

                         /* ack to queue manager */
                         Queue_Acknowledge( &rmdir_queue, RMDIR_OK, rmdir_stats, RMDIR_FDBK_COUNT );
                         /* free entry resources */
                         FreeRmdirItem( p_item );
                     } /* success */
                }  /* entry matches */
                else
                {
                    /* no match */
                    Queue_Acknowledge( &rmdir_queue, RMDIR_DIR_WHITELISTED, NULL, 0 );
                    FreeRmdirItem( p_item );
                }
            }
            else /* unknown RMDIR type */
            {
                DisplayLog( LVL_DEBUG, RMDIR_TAG, "Unknown rmdir policy type: %#x", p_item->rmdir_type );
                Queue_Acknowledge( &rmdir_queue, RMDIR_ERROR, NULL, 0 );
                /* free entry resources */
                FreeRmdirItem( p_item );
            }

      }                         /* end of infinite loop en Queue_Get */

    return NULL;
}


static int start_rmdir_threads( unsigned int nb_threads )
{
    unsigned int   i;

    thread_ids = ( pthread_t * ) MemCalloc( nb_threads, sizeof( pthread_t ) );
    if ( !thread_ids )
      {
          DisplayLog( LVL_CRIT, RMDIR_TAG, "Memory error in %s", __FUNCTION__ );
          return ENOMEM;
      }

    for ( i = 0; i < nb_threads; i++ )
      {
          if ( pthread_create( &thread_ids[i], NULL, Thr_Rmdir, NULL ) != 0 )
            {
                int            rc = errno;
                DisplayLog( LVL_CRIT, RMDIR_TAG, "Error %d creating rmdir thread in %s: %s", rc,
                            __FUNCTION__, strerror( rc ) );
                return rc;
            }
      }

    return 0;
}

/**
 * Main loop for directory removal
 */
static void   *remove_dir_thr( void *thr_arg )
{
    int            rc;
    do
      {
          /* empty directory removal */
          if ( policies.rmdir_policy.age_rm_empty_dirs > 0 )
            {
                unsigned int   nb_removed;

                rc = perform_rmdir( &nb_removed );

                if ( rc )
                    DisplayLog( LVL_CRIT, RMDIR_TAG,
                                "perform_rmdir() returned with error %d. %u empty directories removed.",
                                rc, nb_removed );
                else
                    DisplayLog( LVL_MAJOR, RMDIR_TAG,
                                "Empty dir removal summary: %u directories removed.", nb_removed );
            }
          else
            {
                DisplayLog( LVL_EVENT, RMDIR_TAG,
                            "Empty dir removal is disabled (Rmdir_policy::age_rm_empty_dirs = 0)." );
            }

          last_rmdir = time( NULL );

          /* recursive directory removal */
          if ( policies.rmdir_policy.recursive_rmdir_count > 0 )
          {
                unsigned int   nb_top, nb_total = 0;
                unsigned long long volume = 0;
                char strvol[256];

                rc = perform_rmdir_recurse( &nb_top, &nb_total, &volume );
                FormatFileSize( strvol, 256, volume );

                if ( rc )
                    DisplayLog( LVL_CRIT, RMDIR_TAG,
                                "perform_rmdir() returned with error %d. %u top-level directories removed (total: %u entries, volume: %s).",
                                rc, nb_top, nb_total, strvol );
                else
                    DisplayLog( LVL_MAJOR, RMDIR_TAG,
                                "Recursive dir removal summary: %u top-level directories removed (total: %u entries, volume: %s).",
                                nb_top, nb_total, strvol );
          }

          last_rmdir = time( NULL );

          if ( !one_shot_mode )
              rh_sleep( rmdir_config.runtime_interval );
          else
            {
                pthread_exit( NULL );
                return NULL;
            }

      }
    while ( 1 );

    return NULL;

}

/* ------------ Exported functions ------------ */

/**
 * Initialize module and start main thread
 */
int Start_Rmdir( rmdir_config_t * p_config, int flags )
{
    int            rc;

    /* store configuration */
    rmdir_config = *p_config;
    rmdir_flags = flags;

    fsdev = get_fsdev();

    /* does not start rmdir module if nothing is defined in configuration */
    if ( (policies.rmdir_policy.age_rm_empty_dirs == 0)
        && (policies.rmdir_policy.recursive_rmdir_count == 0 ) )
    {
            DisplayLog( LVL_CRIT, RMDIR_TAG,
                        "No rmdir policy defined in configuration file... Disabling." );
            return ENOENT;
    }

    /* initialize rmdir queue */
    rc = CreateQueue( &rmdir_queue, rmdir_config.rmdir_queue_size, RMDIR_STATUS_COUNT - 1, RMDIR_FDBK_COUNT );
    if ( rc )
      {
          DisplayLog( LVL_CRIT, RMDIR_TAG, "Error %d initializing rmdir queue", rc );
          return rc;
      }

    /* start rmdir threads */
    rc = start_rmdir_threads( rmdir_config.nb_threads_rmdir );
    if ( rc )
        return rc;

    /* start main thread */

    rc = pthread_create( &rmdir_thr_id, NULL, remove_dir_thr, NULL );

    if ( rc != 0 )
      {
          rc = errno;
          DisplayLog( LVL_CRIT, RMDIR_TAG,
                      "Error %d starting main thread of Empty Dir Remover: %s", rc,
                      strerror( rc ) );
          return rc;
      }

    return 0;
}


int Wait_Rmdir( void )
{
    void          *returned;
    pthread_join( rmdir_thr_id, &returned );
    return 0;
}


void Dump_Rmdir_Stats( void )
{
    char           tmp_buff[256];
    struct tm      paramtm;

    unsigned int   status_tab[RMDIR_STATUS_COUNT];

    unsigned int   nb_waiting, nb_items;
    time_t         last_submitted, last_started, last_ack;
    time_t         now = time( NULL );

    DisplayLog( LVL_MAJOR, "STATS", "====== Empty Dir Removal Stats ======" );
    if ( last_rmdir )
      {
          strftime( tmp_buff, 256, "%Y/%m/%d %T", localtime_r( &last_rmdir, &paramtm ) );
          DisplayLog( LVL_MAJOR, "STATS", "last_run                 = %s", tmp_buff );
      }
    else
        DisplayLog( LVL_MAJOR, "STATS", "last_run                 = (none)" );

    /* Rmdir stats */

    RetrieveQueueStats( &rmdir_queue, &nb_waiting, &nb_items, &last_submitted, &last_started,
                        &last_ack, status_tab, NULL );

    DisplayLog( LVL_MAJOR, "STATS", "idle rmdir threads       = %u", nb_waiting );
    DisplayLog( LVL_MAJOR, "STATS", "rmdir operations pending = %u", nb_items );
    DisplayLog( LVL_MAJOR, "STATS", "directories removed      = %u", status_tab[RMDIR_OK] );
    DisplayLog( LVL_MAJOR, "STATS", "rmdir cancelled:" );
    DisplayLog( LVL_MAJOR, "STATS", "   whitelisted dir          = %u",
                status_tab[RMDIR_DIR_WHITELISTED] );
    DisplayLog( LVL_MAJOR, "STATS", "   modified since last scan = %u",
                status_tab[RMDIR_DIR_MODIFIED] );
    DisplayLog( LVL_MAJOR, "STATS", "   moved since last scan    = %u",
                status_tab[RMDIR_DIR_MOVED] );
    DisplayLog( LVL_MAJOR, "STATS", "   removal errors           = %u", status_tab[RMDIR_ERROR] );

    if ( last_submitted )
        DisplayLog( LVL_MAJOR, "STATS", "last dir submitted %2d s ago",
                    ( int ) ( now - last_submitted ) );

    if ( last_started )
        DisplayLog( LVL_MAJOR, "STATS", "last dir handled   %2d s ago",
                    ( int ) ( now - last_started ) );

    if ( last_ack )
        DisplayLog( LVL_MAJOR, "STATS", "last dir removed   %2d s ago",
                    ( int ) ( now - last_ack ) );


}
