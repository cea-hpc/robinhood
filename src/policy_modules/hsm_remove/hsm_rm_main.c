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

#define HSMRM_TAG "HSM_rm"

#include "RobinhoodConfig.h"
#include "RobinhoodMisc.h"
#include "hsm_rm.h"
#include "queue.h"
#include "Memory.h"
#include "xplatform_print.h"
#ifdef _HSM_LITE
#include "backend_ext.h"
#endif
#include <errno.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>


/* ----- queue management ----- */

static entry_queue_t hsm_rm_queue;

/* request status */
#define HSMRM_OK            0 /* file removed */
#define HSMRM_NOCOPY        1 /* no copy in backend */
#define HSMRM_ERROR         2 /* rm error */

#define HSMRM_STATUS_COUNT  3

/* ----- Module configuration ----- */

hsm_rm_config_t hsm_rm_config;
static int     hsm_rm_flags = 0;

#define one_shot_mode (hsm_rm_flags & FLAG_ONCE)
#define dry_run (hsm_rm_flags & FLAG_DRY_RUN)

/* main thread */
static pthread_t hsm_rm_thr_id;

/* array of threads */
static pthread_t *thread_ids = NULL;

static time_t  last_rm = 0;

#define CHECK_QUEUE_INTERVAL    1

/* ---- Internal functions ---- */

/**
 * HSM rm helper
 * @return posix error code (from errno)
 */

#ifdef _LUSTRE_HSM
static inline int HSM_rm( const entry_id_t * p_id )
{
    DisplayLog( LVL_FULL, HSMRM_TAG, "HSM_remove("DFID")", PFID(p_id) );
    if ( !dry_run )
          return LustreHSM_Action( HUA_REMOVE, p_id, NULL, 0 );
    return 0;
}
#elif defined(_HSM_LITE)
static inline int HSM_rm( const entry_id_t * p_id, const char * bkpath )
{
    DisplayLog( LVL_FULL, HSMRM_TAG, "HSM_remove("DFID", %s)", PFID(p_id),
                bkpath );
    if ( !dry_run )
          return rbhext_remove( p_id, bkpath );
    return 0;
}
#endif

typedef struct hsm_rm_item__
{
    entry_id_t     entry_id;
#ifdef _HSM_LITE
    char           backendpath[RBH_PATH_MAX];
#endif
} hsm_rm_item_t;

/**
 *  Alloc a new rm item so it can be pushes to the rm queue.
 */
#ifdef _HSM_LITE
static void   *MkRmItem( entry_id_t * p_entry_id, char * bkpath )
#elif defined(_LUSTRE_HSM)
static void   *MkRmItem( entry_id_t * p_entry_id )
#endif
{
    hsm_rm_item_t  *new_entry;

    new_entry = ( hsm_rm_item_t * ) MemAlloc( sizeof( hsm_rm_item_t ) );
    if ( !new_entry )
        return NULL;

    new_entry->entry_id = *p_entry_id;

#ifdef _HSM_LITE
    rh_strncpy(new_entry->backendpath, bkpath, RBH_PATH_MAX);
#endif

    return new_entry;
}

/**
 * Free a Rmdir Item (and the resources of entry_attr).
 */
static void FreeRmItem( hsm_rm_item_t * item )
{
    MemFree( item );
}

/**
 *  Sum the number of acks from a status tab
 */
static inline unsigned int ack_count( unsigned int *status_tab )
{
    unsigned int   i, sum;
    sum = 0;

    for ( i = 0; i < HSMRM_STATUS_COUNT; i++ )
        sum += status_tab[i];

    return sum;
}

static inline int check_rm_limit( unsigned int count )
{
    if ( (hsm_rm_config.max_rm > 0) && (count >= hsm_rm_config.max_rm) )
    {
        DisplayLog( LVL_EVENT, HSMRM_TAG,
                    "max hsm_rm count %u is reached.", hsm_rm_config.max_rm );
        return 1;
    }
    return 0;
}

/**
 * This function retrieve files to be removed from HSM
 * and submit them to workers.
 */
static int perform_hsm_rm( unsigned int *p_nb_removed, unsigned int * p_noop, unsigned int * p_errors)
{
    int            rc = 0;
    lmgr_t         lmgr;
    struct lmgr_rm_list_t *it = NULL;

    entry_id_t     entry_id;

    unsigned int   status_tab1[HSMRM_STATUS_COUNT];
    unsigned int   status_tab2[HSMRM_STATUS_COUNT];

    unsigned int   submitted_files, nb_in_queue, nb_hsm_rm_pending;
    int            end_of_list = FALSE;
#ifdef _HSM_LITE
    char           bkpath[RBH_PATH_MAX];
#endif

    if ( p_nb_removed )
        *p_nb_removed = 0;
    if ( p_noop )
        *p_noop = 0;
    if ( p_errors )
        *p_errors = 0;

    /* we assume that purging is a rare event
     * and we don't want to use a DB connection all the time
     * so we connect only at purge time */

    rc = ListMgr_InitAccess( &lmgr );
    if ( rc )
      {
          DisplayLog( LVL_CRIT, HSMRM_TAG,
                      "Could not connect to database (error %d). Removal cancelled.", rc );
          return rc;
      }

    DisplayLog( LVL_EVENT, HSMRM_TAG, "Start removing files in HSM" );

    it = ListMgr_RmList( &lmgr, TRUE, NULL );

    if ( it == NULL )
      {
          DisplayLog( LVL_CRIT, HSMRM_TAG,
                      "Error retrieving list of removed entries from database. Operation cancelled." );
          return -1;
      }

    /* retrieve info before removing dirs, so we can make a delta after */
    RetrieveQueueStats( &hsm_rm_queue, NULL, NULL, NULL, NULL, NULL, status_tab1, NULL );

    submitted_files = 0;

    /* submit all eligible files up to max_rm */
    do
      {
          memset( &entry_id, 0, sizeof( entry_id_t ) );

          /* @TODO retrieve path and dates for traces */
#ifdef _HSM_LITE
          bkpath[0] = '\0';
          rc = ListMgr_GetNextRmEntry( it, &entry_id, NULL, bkpath, NULL, NULL );
#else
          rc = ListMgr_GetNextRmEntry( it, &entry_id, NULL, NULL, NULL );
#endif

          if ( rc == DB_END_OF_LIST )
            {
                end_of_list = TRUE;
                break;
            }
          else if ( rc != 0 )
            {
                DisplayLog( LVL_CRIT, HSMRM_TAG, "Error %d getting next entry of iterator", rc );
                break;
            }

          /* submit entry for removal */
#ifdef _HSM_LITE
          rc = Queue_Insert( &hsm_rm_queue, MkRmItem( &entry_id, bkpath ) );
#elif defined(_LUSTRE_HSM)
          rc = Queue_Insert( &hsm_rm_queue, MkRmItem( &entry_id ) );
#endif
          if ( rc )
              return rc;

          submitted_files++;
      }
    while ( !check_rm_limit(submitted_files) );
    /* until END_OF_LIST or error is returned or max_rm is reached */

    /* close iterator and db access */
    ListMgr_CloseRmList( it );
    ListMgr_CloseAccess( &lmgr );

    /* wait for end of rm pass  */
    do
      {
          RetrieveQueueStats( &hsm_rm_queue, NULL, &nb_in_queue, NULL, NULL, NULL, status_tab2,
                              NULL );

          /* nb of rm operation pending = nb_enqueued - ( nb ack after - nb ack before ) */
          nb_hsm_rm_pending = submitted_files + ack_count( status_tab1 ) - ack_count( status_tab2 );

          DisplayLog( LVL_DEBUG, HSMRM_TAG,
                      "Waiting for remove request queue: still %u files to be removed "
                      "(%u in queue, %u beeing processed)",
                      nb_hsm_rm_pending, nb_in_queue, nb_hsm_rm_pending - nb_in_queue );

          if ( ( nb_in_queue != 0 ) || ( nb_hsm_rm_pending != 0 ) )
              rh_sleep( CHECK_QUEUE_INTERVAL );

      }
    while ( ( nb_in_queue != 0 ) || ( nb_hsm_rm_pending != 0 ) );

    if ( p_nb_removed )
        *p_nb_removed = status_tab2[HSMRM_OK] - status_tab1[HSMRM_OK];
    if ( p_noop )
        *p_noop = status_tab2[HSMRM_NOCOPY] - status_tab1[HSMRM_NOCOPY];
    if ( p_errors )
        *p_errors = status_tab2[HSMRM_ERROR] - status_tab1[HSMRM_ERROR];

    if ( end_of_list )
        return 0;
    else
        return rc;
}

/**
 *  Worker thread that performs HSM_REMOVE
 */
static void   *Thr_Rm( void *arg )
{
    int            rc;
    lmgr_t         lmgr;

    void          *p_queue_entry;
    hsm_rm_item_t  *p_item;

    rc = ListMgr_InitAccess( &lmgr );
    if ( rc )
      {
          DisplayLog( LVL_CRIT, HSMRM_TAG, "Could not connect to database (error %d). Exiting.",
                      rc );
          exit( rc );
      }

    while ( Queue_Get( &hsm_rm_queue, &p_queue_entry ) == 0 )
    {
          p_item = ( hsm_rm_item_t * ) p_queue_entry;

          DisplayLog( LVL_FULL, HSMRM_TAG, "Considering entry "DFID,
                      PFID( &p_item->entry_id) );

          /* 7) rm and remove entry from database */
#ifdef _HSM_LITE
          rc = HSM_rm( &p_item->entry_id, p_item->backendpath );
#elif defined(_LUSTRE_HSM)
          rc = HSM_rm( &p_item->entry_id );
#endif

          if (rc == -ENOENT)
          {
#ifdef _HSM_LITE
               DisplayLog(LVL_DEBUG, HSMRM_TAG, "%s not in backend",
                          p_item->backendpath);
#else
               DisplayLog(LVL_DEBUG, HSMRM_TAG, DFID" not in backend",
                          PFID(&p_item->entry_id));
#endif
                /* remove it from database */
                rc = ListMgr_SoftRemove_Discard( &lmgr, &p_item->entry_id );
                if ( rc )
                    DisplayLog( LVL_CRIT, HSMRM_TAG, "Error %d removing entry from database.",
                                rc );

               Queue_Acknowledge( &hsm_rm_queue, HSMRM_NOCOPY, NULL, 0 );
               FreeRmItem( p_item );
          }
          else if (rc == -EINVAL)
          {
               DisplayLog( LVL_DEBUG, HSMRM_TAG, "Unknown backend path for "DFID,
                           PFID( &p_item->entry_id) );
                /* remove it from database */
                rc = ListMgr_SoftRemove_Discard( &lmgr, &p_item->entry_id );
                if ( rc )
                    DisplayLog( LVL_CRIT, HSMRM_TAG, "Error %d removing entry from database.",
                                rc );

               Queue_Acknowledge( &hsm_rm_queue, HSMRM_NOCOPY, NULL, 0 );
               FreeRmItem( p_item );
          }
          else if ( rc != 0 )
            {
                DisplayLog( LVL_DEBUG, HSMRM_TAG, "Error removing entry "DFID": %s",
                            PFID( &p_item->entry_id), strerror( abs(rc) ) );

                Queue_Acknowledge( &hsm_rm_queue, HSMRM_ERROR, NULL, 0 );

                /* free entry resources */
                FreeRmItem( p_item );
            }
          else
            {
                /*char           strmod[256];*/

                /* request successfully sent */

                /* report messages */

                DisplayLog( LVL_DEBUG, HSMRM_TAG,
                            "Remove request successful for entry "DFID, PFID(&p_item->entry_id) );
/*                            "Remove request for entry "DFID", removed from Lustre %s ago", ... */

                DisplayReport( "HSM_remove "DFID, PFID(&p_item->entry_id) );
                           /*     " | lustre_rm=%" PRINT_TIME_T,
                               ATTR( &p_item->entry_attr, fullpath ), strmod, ATTR( &new_attr_set,
                                                                                    last_mod ) );   */
                /* remove it from database */
                rc = ListMgr_SoftRemove_Discard( &lmgr, &p_item->entry_id );
                if ( rc )
                    DisplayLog( LVL_CRIT, HSMRM_TAG, "Error %d removing entry from database.",
                                rc );

                /* ack to queue manager */
                Queue_Acknowledge( &hsm_rm_queue, HSMRM_OK, NULL, 0 );

                /* free entry resources */
                FreeRmItem( p_item );
            }
    }                         /* end of infinite loop en Queue_Get */

    return NULL;
}


static int start_hsm_rm_threads( unsigned int nb_threads )
{
    unsigned int   i;

    thread_ids = ( pthread_t * ) MemCalloc( nb_threads, sizeof( pthread_t ) );
    if ( !thread_ids )
      {
          DisplayLog( LVL_CRIT, HSMRM_TAG, "Memory error in %s", __FUNCTION__ );
          return ENOMEM;
      }

    for ( i = 0; i < nb_threads; i++ )
      {
          if ( pthread_create( &thread_ids[i], NULL, Thr_Rm, NULL ) != 0 )
            {
                int            rc = errno;
                DisplayLog( LVL_CRIT, HSMRM_TAG, "Error %d creating HSM_rm thread in %s: %s", rc,
                            __FUNCTION__, strerror( rc ) );
                return rc;
            }
      }

    return 0;
}

/**
 * Main loop for removal
 */
static void   *remove_thr( void *thr_arg )
{
    int            rc;
    unsigned int   nb_removed, nb_noop, nb_err;

    do
      {
          if ( policies.unlink_policy.hsm_remove )
            {
                rc = perform_hsm_rm( &nb_removed, &nb_noop, &nb_err );

                if ( rc )
                    DisplayLog( LVL_CRIT, HSMRM_TAG,
                                "perform_hsm_rm() returned with error %d. %u remove requests sent.",
                                rc, nb_removed );
                else
                    DisplayLog( LVL_MAJOR, HSMRM_TAG,
                               "HSM file removal summary: %u files removed, %u void op, %u errors.",
                                nb_removed, nb_noop, nb_err );
            }
          else
            {
                DisplayLog( LVL_EVENT, HSMRM_TAG,
                            "HSM entry removal is disabled (hsm_remove_policy::hsm_remove = off)." );
            }

          last_rm = time( NULL );

          if ( !one_shot_mode )
              rh_sleep( hsm_rm_config.runtime_interval );
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
int Start_HSMRm( hsm_rm_config_t * p_config, int flags )
{
    int            rc;

    /* store configuration */
    hsm_rm_config = *p_config;
    hsm_rm_flags = flags;

    if ( !policies.unlink_policy.hsm_remove )
    {
            DisplayLog( LVL_CRIT, HSMRM_TAG,
                "HSM removal is disabled in configuration file. Skipping module initialization..." );
            return ENOENT;
    }

    /* initialize rm queue */
    rc = CreateQueue( &hsm_rm_queue, hsm_rm_config.rm_queue_size, HSMRM_STATUS_COUNT - 1, 0 );
    if ( rc )
      {
          DisplayLog( LVL_CRIT, HSMRM_TAG, "Error %d initializing rm queue", rc );
          return rc;
      }
    else
         DisplayLog( LVL_FULL, HSMRM_TAG, "HSM rm queue created (size=%u)", hsm_rm_config.rm_queue_size );

    /* start rm threads */
    rc = start_hsm_rm_threads( hsm_rm_config.nb_threads_rm );
    if ( rc )
        return rc;

    /* start main thread */

    rc = pthread_create( &hsm_rm_thr_id, NULL, remove_thr, NULL );

    if ( rc != 0 )
      {
          rc = errno;
          DisplayLog( LVL_CRIT, HSMRM_TAG,
                      "Error %d starting main thread for HSM removal: %s", rc,
                      strerror( rc ) );
          return rc;
      }

    return 0;
}


int Wait_HSMRm(  )
{
    void          *returned;
    pthread_join( hsm_rm_thr_id, &returned );
    return 0;
}


void Dump_HSMRm_Stats(  )
{
    char           tmp_buff[256];
    struct tm      paramtm;

    unsigned int   status_tab[HSMRM_STATUS_COUNT];

    unsigned int   nb_waiting, nb_items;
    time_t         last_submitted, last_started, last_ack;
    time_t         now = time( NULL );

    DisplayLog( LVL_MAJOR, "STATS", "====== HSM Remove Stats ======" );
    if ( last_rm )
      {
          strftime( tmp_buff, 256, "%Y/%m/%d %T", localtime_r( &last_rm, &paramtm ) );
          DisplayLog( LVL_MAJOR, "STATS", "last_run              = %s", tmp_buff );
      }
    else
        DisplayLog( LVL_MAJOR, "STATS", "last_run              = (none)" );

    /* Rmdir stats */

    RetrieveQueueStats( &hsm_rm_queue, &nb_waiting, &nb_items, &last_submitted, &last_started,
                        &last_ack, status_tab, NULL );

    DisplayLog( LVL_MAJOR, "STATS", "idle rm threads       = %u", nb_waiting );
    DisplayLog( LVL_MAJOR, "STATS", "rm requests pending   = %u", nb_items );
    DisplayLog( LVL_MAJOR, "STATS", "effective rm          = %u", status_tab[HSMRM_OK] );
    DisplayLog( LVL_MAJOR, "STATS", "void rm               = %u", status_tab[HSMRM_NOCOPY] );
    DisplayLog( LVL_MAJOR, "STATS", "rm errors             = %u", status_tab[HSMRM_ERROR] );

    if ( last_submitted )
        DisplayLog( LVL_MAJOR, "STATS", "last entry submitted %2d s ago",
                    ( int ) ( now - last_submitted ) );

    if ( last_started )
        DisplayLog( LVL_MAJOR, "STATS", "last entry handled   %2d s ago",
                    ( int ) ( now - last_started ) );

    if ( last_ack )
        DisplayLog( LVL_MAJOR, "STATS", "last request sent    %2d s ago",
                    ( int ) ( now - last_ack ) );


}
