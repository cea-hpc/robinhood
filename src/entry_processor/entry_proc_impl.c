/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2008, 2009 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
/**
 * Implementation of pipeline management 
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "entry_processor.h"
#include "entry_proc_tools.h"
#include "Memory.h"
#include "RobinhoodLogs.h"
#include "SemN.h"
#include <pthread.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>

/* Dijkstra notation */
#define P(_m_)  pthread_mutex_lock(&(_m_))
#define V(_m_)  pthread_mutex_unlock(&(_m_))

static semaphore_t pipeline_token;

/* each stage of the pipeline consist of the following information: */
typedef struct __list_by_stage__
{
    entry_proc_op_t *last_in_ptr;
    entry_proc_op_t *first_in_ptr;
    unsigned int   nb_threads;                   /* number of threads working on this stage */
    unsigned int   nb_unprocessed_entries;       /* number of entries to be processed in this list */
    unsigned int   nb_processed_entries;         /* number of entries processed in this list */
    unsigned long long total_processed;          /* total number of processed entries since start */
    struct timeval total_processing_time;        /* total amount of time for processing entries at this stage */
    pthread_mutex_t stage_mutex;
} list_by_stage_t;

/* Note1: nb_threads + nb_unprocessed_entries + nb_processed_entries = nb entries at a given step */
/* stages mutex must always be taken from lower stage to upper to avoid deadlocks */

static list_by_stage_t pipeline[PIPELINE_STAGE_COUNT];

static pthread_mutex_t work_avail_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t work_avail_cond = PTHREAD_COND_INITIALIZER;
unsigned int   nb_waiting_threads = 0;


/* termination mecanism  */
static pthread_mutex_t terminate_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t terminate_cond = PTHREAD_COND_INITIALIZER;
static int     terminate_flag = FALSE;
static int     nb_finished_threads = 0;


typedef struct worker_info__
{
    unsigned int   index;
    pthread_t      thread_id;
    lmgr_t         lmgr;
} worker_info_t;

static worker_info_t *worker_params = NULL;

#ifdef _DEBUG_ENTRYPROC
static void dump_entry_op( entry_proc_op_t * p_op )
{
#ifdef _HAVE_FID
    if ( p_op->entry_id_is_set )
        printf( "id="DFID"\n", PFID(&p_op->entry_id) );
#endif
    if ( p_op->entry_attr_is_set && ATTR_MASK_TEST( &p_op->entry_attr, fullpath ) )
        printf("path=%s\n", ATTR( &p_op->entry_attr, fullpath ) );

    printf("stage=%u, being processed=%u, db_exists=%u, id is referenced=%u, db_op_type=%u\n",
            p_op->pipeline_stage, p_op->being_processed, p_op->db_exists,
            p_op->id_is_referenced, p_op->db_op_type );
    printf("start proc time=%u.%06u\n", (unsigned int)p_op->start_processing_time.tv_sec,
            (unsigned int)p_op->start_processing_time.tv_usec );
    printf("next=%p, prev=%p\n", p_op->p_next, p_op->p_prev );
}
#endif


/* worker thread for pipeline */
static void   *entry_proc_worker_thr( void *arg )
{
    entry_proc_op_t *p_op;
    int            rc;
    worker_info_t *myinfo = ( worker_info_t * ) arg;

    DisplayLog( LVL_DEBUG, ENTRYPROC_TAG, "Starting pipeline worker thread #%u", myinfo->index );

    /* create connection to database */
    rc = ListMgr_InitAccess( &myinfo->lmgr );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                    "Pipeline worker #%u could not connect to ListMgr. Exiting.", myinfo->index );
        exit( 1 );
    }

    while ( ( p_op = EntryProcessor_GetNextOp(  ) ) != NULL )
    {
        const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];
        stage_info->stage_function( p_op, &myinfo->lmgr );
    }

    if ( !terminate_flag )
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG,
                    "Error: EntryProcessor_GetNextOp returned NULL but no termination signal has been received!!!" );

    /* All operations have been processed. Now flushing DB operations and closing connection. */
    ListMgr_CloseAccess( &myinfo->lmgr );

    /* notify thread's termination */
    P( terminate_lock );
    nb_finished_threads++;
    /* always notify waiting thread, so it writes a log about the number of waiting threads/ops */
    pthread_cond_signal( &terminate_cond );
    V( terminate_lock );

    DisplayLog( LVL_DEBUG, ENTRYPROC_TAG, "Pipeline worker thread #%u terminated", myinfo->index );

    pthread_exit( NULL );
    return NULL;

}


/**
 *  Initialize entry processor pipeline
 */
int EntryProcessor_Init( const entry_proc_config_t * p_conf, int flags )
{
    int            i;

    entry_proc_conf = *p_conf;
    pipeline_flags = flags;

    if ( entry_proc_conf.match_classes && !is_class_defined() )
    {
        DisplayLog( LVL_EVENT, ENTRYPROC_TAG, "No class defined in policies, disabling class matching." );
        entry_proc_conf.match_classes = FALSE;
    }

    /* If a limit of pending operations is specified, initialize a token */
    if ( entry_proc_conf.max_pending_operations > 0 )
        semaphore_init( &pipeline_token, entry_proc_conf.max_pending_operations );

    for ( i = 0; i < PIPELINE_STAGE_COUNT; i++ )
    {
        pipeline[i].last_in_ptr = NULL;
        pipeline[i].first_in_ptr = NULL;
        pipeline[i].nb_threads = 0;
        pipeline[i].nb_unprocessed_entries = 0;
        pipeline[i].nb_processed_entries = 0;
        pipeline[i].total_processed = 0;
        timerclear( &pipeline[i].total_processing_time );
        pthread_mutex_init( &pipeline[i].stage_mutex, NULL );
    }

    /* init id constraint manager */
    id_constraint_init(  );

    /* start workers */

    worker_params =
        ( worker_info_t * ) MemCalloc( entry_proc_conf.nb_thread, sizeof( worker_info_t ) );
    if ( !worker_params )
        return ENOMEM;

    for ( i = 0; i < entry_proc_conf.nb_thread; i++ )
    {
        worker_params[i].index = i;
        if ( pthread_create( &worker_params[i].thread_id,
                             NULL, entry_proc_worker_thr, &worker_params[i] ) != 0 )
        {
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error: Could not start worker thread" );
            return errno;
        }
    }

    return 0;
}


/**
 * This function adds a new operation to the queue
 */
int EntryProcessor_Push( const entry_proc_op_t * p_new_op )
{
    int            rc, i;
    unsigned int   insert_stage;

    /* allocate a new pipeline entry */
    entry_proc_op_t *p_entry = ( entry_proc_op_t * ) MemAlloc( sizeof( entry_proc_op_t ) );

    if ( !p_entry )
        return ENOMEM;

    if ( ( rc = pthread_mutex_init( &p_entry->entry_lock, NULL ) ) != 0 )
        return rc;

    if ( ( rc = pthread_mutex_init( &p_entry->entry_id_lock, NULL ) ) != 0 )
        return rc;

    /* fill its content (except p_prev because we need a lock) */
    *p_entry = *p_new_op;
    p_entry->p_next = NULL;     /* insert as last */
    p_entry->being_processed = FALSE;
    p_entry->id_is_referenced = FALSE;

    /* if a limit of pending operations is specified, wait for a token */
    if ( entry_proc_conf.max_pending_operations > 0 )
        semaphore_P( &pipeline_token );

    /* We must always insert it in the first stage, to keep
     * the good ordering of entries.
     * Except if all stages between stage0 and insert_stage are empty
     */

    /* by default, insert stage is entry stage
     * except if there is a non empty stage before
     */
    insert_stage = p_entry->pipeline_stage;

    /* take all locks for stage0 to insert_stage or first non empty stage */
    for ( i = 0; i <= p_entry->pipeline_stage; i++ )
    {
        P( pipeline[i].stage_mutex );

        if ( pipeline[i].last_in_ptr != NULL )
        {
            insert_stage = i;
            break;
        }
    }

#ifdef _DEBUG_ENTRYPROC
    if ( insert_stage != p_entry->pipeline_stage )
        printf( "INSERT STAGE (%u) != PUSH STAGE(%u)\n", insert_stage, p_entry->pipeline_stage );
#endif

    /* If the stage has an ID_CONSTRAINT and ID is set, register entry */
    if ( ( entry_proc_pipeline[insert_stage].stage_flags & STAGE_FLAG_ID_CONSTRAINT )
         && p_entry->entry_id_is_set )
    {
        id_constraint_register( p_entry );
    }

    /* insert entry */
    if ( pipeline[insert_stage].last_in_ptr == NULL )
    {
        /* first = last = new entry */
        p_entry->p_prev = NULL;
        pipeline[insert_stage].last_in_ptr = p_entry;
        pipeline[insert_stage].first_in_ptr = p_entry;
    }
    else
    {
        p_entry->p_prev = pipeline[insert_stage].last_in_ptr;
        pipeline[insert_stage].last_in_ptr->p_next = p_entry;
        pipeline[insert_stage].last_in_ptr = p_entry;
    }

    if ( insert_stage < p_entry->pipeline_stage )
        pipeline[insert_stage].nb_processed_entries++;
    else
        pipeline[insert_stage].nb_unprocessed_entries++;

    /* release all lists lock */
    for ( i = 0; i <= insert_stage; i++ )
        V( pipeline[i].stage_mutex );

    /* there is a new entry to be processed ! (signal only if threads are waiting) */
    P( work_avail_lock );
    if ( nb_waiting_threads > 0 )
        pthread_cond_signal( &work_avail_cond );
    V( work_avail_lock );

    return 0;

}                               /* EntryProcessor_Push */



/*
 * Remove an entry from a stage
 */
static inline void remove_entry_from_stage( unsigned int stage_index,
                                            entry_proc_op_t * p_op, int lock_stage )
{
    if ( lock_stage )
        P( pipeline[stage_index].stage_mutex );

    /* update stage info (the enrty is supposed to be in "processed" category) */
    pipeline[stage_index].nb_processed_entries--;

    /* update first_in_ptr, if this entry was first_in */
    if ( p_op == pipeline[stage_index].first_in_ptr )
        pipeline[stage_index].first_in_ptr = p_op->p_next;

    /* update last_in_ptr, if this entry was last_in */
    if ( p_op == pipeline[stage_index].last_in_ptr )
        pipeline[stage_index].last_in_ptr = p_op->p_prev;

    /* update neighbours pointer */
    if ( p_op->p_next )
        p_op->p_next->p_prev = p_op->p_prev;

    if ( p_op->p_prev )
        p_op->p_prev->p_next = p_op->p_next;

    if ( lock_stage )
        V( pipeline[stage_index].stage_mutex );

    /* this entry doesn't point to anything */
    p_op->p_prev = NULL;
    p_op->p_next = NULL;
}

/*
 * Move terminated operations to next stage
 */
static int move_stage_entries( unsigned int source_stage_index, int lock_src_stage )
{
    entry_proc_op_t *p_first = NULL;
    entry_proc_op_t *p_last = NULL;
    entry_proc_op_t *p_curr = NULL;
    int            count = 0;
    int            i;
    unsigned int   pipeline_stage_min;
    unsigned int   insert_stage;

    /* nothing to do if we are already at last step */
    if ( source_stage_index >= PIPELINE_STAGE_COUNT - 1 )
        return 0;

    if ( lock_src_stage )
        P( pipeline[source_stage_index].stage_mutex );

    /* is there at least 1 entry to be moved ? */
    if ( pipeline[source_stage_index].first_in_ptr == NULL )
        goto out;

    p_first = pipeline[source_stage_index].first_in_ptr;
    P( p_first->entry_lock );
    if ( p_first->being_processed || ( p_first->pipeline_stage <= source_stage_index ) )
    {
        V( p_first->entry_lock );
        goto out;
    }
    pipeline_stage_min = p_first->pipeline_stage;
    V( p_first->entry_lock );

    p_last = p_first;
    count = 1;

    /* check next entries  */
    for ( p_curr = p_first->p_next; p_curr != NULL; p_curr = p_curr->p_next )
    {
        P( p_curr->entry_lock );
        if ( !p_curr->being_processed && ( p_curr->pipeline_stage > source_stage_index ) )
        {
            if ( p_curr->pipeline_stage < pipeline_stage_min )
                pipeline_stage_min = p_curr->pipeline_stage;
            V( p_curr->entry_lock );
            count++;
            p_last = p_curr;
        }
        else
        {
            V( p_curr->entry_lock );
            break;
        }
    }

    /* remove entries from current list: */

    /* 1) remove the beginning of the list + update first_in_ptr */
    if ( p_last->p_next )
        p_last->p_next->p_prev = NULL;

    pipeline[source_stage_index].first_in_ptr = p_last->p_next;

    /* cut the list of entries to be moved */
    p_last->p_next = NULL;

    /* 2) update last_in_ptr if the last entry is last_in */
    if ( pipeline[source_stage_index].last_in_ptr == p_last )
        pipeline[source_stage_index].last_in_ptr = NULL;

    /* change entry count */
    pipeline[source_stage_index].nb_processed_entries -= count;

    /* by default, insert stage is pipeline_stage_min 
     * except if there is a non empty stage before
     */
    insert_stage = pipeline_stage_min;

    /* take all locks from next stage to insert_stage or first non empty stage */
    for ( i = source_stage_index + 1; i <= pipeline_stage_min; i++ )
    {
        P( pipeline[i].stage_mutex );

        /* make sure this stage has correctly been flushed */
        if ( pipeline[i].last_in_ptr )
            move_stage_entries( i, FALSE );

        if ( pipeline[i].last_in_ptr != NULL )
        {
            insert_stage = i;
            break;
        }
    }

#ifdef _DEBUG_ENTRYPROC
    if ( insert_stage != pipeline_stage_min )
    {
        printf( "Entries to be moved: %u\n", count );
        printf( "INSERT STAGE (%u) != NEXT STAGE MIN(%u)\n", insert_stage, pipeline_stage_min );
        printf( "STAGE[%u].FIRST=%s, stage=%u\n", insert_stage,
                ATTR( &pipeline[insert_stage].first_in_ptr->entry_attr, fullpath ),
                pipeline[insert_stage].first_in_ptr->pipeline_stage );
        printf( "STAGE[%u].LAST=%s, stage=%u\n", insert_stage,
                ATTR( &pipeline[insert_stage].last_in_ptr->entry_attr, fullpath ),
                pipeline[insert_stage].last_in_ptr->pipeline_stage );
    }
#endif

    /* If the stage has an ID_CONSTRAINT, register entries */
    if ( entry_proc_pipeline[insert_stage].stage_flags & STAGE_FLAG_ID_CONSTRAINT )
    {
        for ( p_curr = p_first; p_curr != NULL; p_curr = p_curr->p_next )
        {
#ifdef _DEBUG_ENTRYPROC
            if ( p_curr->id_is_referenced || (!p_curr->entry_id_is_set && p_curr->pipeline_stage != 6) )
            {
                printf( "moving entry %p "DFID" from stage %u to %u, id is ref? %u, id_is_set? %u\n", p_curr,
                        PFID(&p_curr->entry_id),
                        source_stage_index, insert_stage, p_curr->id_is_referenced?0:1, p_curr->entry_id_is_set?0:1 );
            }
#endif
            if ( !p_curr->id_is_referenced && p_curr->entry_id_is_set )
            {
                id_constraint_register( p_curr );
            }
        }
    }



#ifdef _DEBUG_ENTRYPROC
    DisplayLog( LVL_FULL, ENTRYPROC_TAG, "move_stage_entries: insert stage %u", insert_stage );
#endif

    /* insert entry list */
    if ( pipeline[insert_stage].last_in_ptr == NULL )
    {
        /* list = new entries */
        p_first->p_prev = NULL;
        p_last->p_next = NULL;
        pipeline[insert_stage].last_in_ptr = p_last;
        pipeline[insert_stage].first_in_ptr = p_first;
    }
    else
    {
        /* put this list at the end of the existing one */
        p_first->p_prev = pipeline[insert_stage].last_in_ptr;
        p_last->p_next = NULL;
        pipeline[insert_stage].last_in_ptr->p_next = p_first;
        pipeline[insert_stage].last_in_ptr = p_last;
    }

    /* check update info depending on this list */

    for ( p_curr = p_first; p_curr != NULL; p_curr = p_curr->p_next )
    {
        /* no need to take a lock to check 'p_curr->pipeline_stage',
         * because if we moved this entry, this means it is not in use.
         * An no thread can process in for now because the list is locked.
         */
        if ( insert_stage < p_curr->pipeline_stage )
            pipeline[insert_stage].nb_processed_entries++;
        else
            pipeline[insert_stage].nb_unprocessed_entries++;
    }

    /* release all lists lock (except the source one) */
    for ( i = source_stage_index + 1; i <= insert_stage; i++ )
        V( pipeline[i].stage_mutex );

  out:
    if ( lock_src_stage )
        P( pipeline[source_stage_index].stage_mutex );
    return count;
}                               /* move_stage_entries */


/**
 * Return an entry to be processed.
 * This entry is tagged "being_processed" and stage info is updated. 
 * @param p_empty Output Boolean. In the case no entry is returned,
 *        this indicates if it is because the pipeline is empty.
 */
entry_proc_op_t *next_work_avail( int *p_empty )
{
    entry_proc_op_t *p_curr;
    int            i;

    *p_empty = TRUE;

    /* check every stage from the last to the first */
    for ( i = PIPELINE_STAGE_COUNT - 1; i >= 0; i-- )
    {
        /* entries have not been processed at this stage. */
        P( pipeline[i].stage_mutex );

        if ( pipeline[i].nb_unprocessed_entries == 0 )
        {
            V( pipeline[i].stage_mutex );
#ifdef _DEBUG_ENTRYPROC
            printf( "Stage[%u] - thread %#lx - no waiting entries\n", i, pthread_self(  ) );
#endif
            continue;
        }

        if ( entry_proc_pipeline[i].stage_flags & STAGE_FLAG_SEQUENTIAL )
        {
            /*
             * If there is already an operation being processed,
             * nothing can be done at this stage. 
             */
            if ( pipeline[i].nb_threads != 0 )
            {
                *p_empty = FALSE;
                V( pipeline[i].stage_mutex );
#ifdef _DEBUG_ENTRYPROC
                printf
                    ( "Stage[%u] - thread %#lx - a thread is already working on this sequential stage\n",
                      i, pthread_self(  ) );
#endif
                continue;
            }

            /* in case of a sequential operation, the only entry that can be processed
             * is the first entry in this stage.
             */
            for ( p_curr = pipeline[i].first_in_ptr; p_curr != NULL; p_curr = p_curr->p_next )
            {
                /* the pipeline is not empty */
                *p_empty = FALSE;

                P( p_curr->entry_lock );

                if ( p_curr->pipeline_stage == i )
                {

                    /* satisfies ID_CONSTRAINT ?  */

                    if ( ( entry_proc_pipeline[i].stage_flags & STAGE_FLAG_ID_CONSTRAINT )
                         && p_curr->entry_id_is_set )
                    {
                        if ( p_curr != id_constraint_get_first_op( &p_curr->entry_id ) )
                        {

                            DisplayLog( LVL_FULL, ENTRYPROC_TAG,
                                        "=============== Not the first  unprocessed operation for this id ============" );
                            /* this is not the first unprocessed operation for this id */
                            V( p_curr->entry_lock );
                            V( pipeline[i].stage_mutex );
                            return NULL;
                        }
                        /* entry can be added */
                    }

                    /* sanity check */
                    if ( p_curr->being_processed )
                    {
                        DisplayLog( LVL_MAJOR, ENTRYPROC_TAG,
                                    "Error: INCONSISTENCY: nb_threads running this step is 0 whereas an entry is being processed !!!" );
                        V( p_curr->entry_lock );
                        V( pipeline[i].stage_mutex );
                        return NULL;
                    }

                    /* tag the entry and update stage info */
                    pipeline[i].nb_unprocessed_entries--;
                    pipeline[i].nb_threads++;
                    p_curr->being_processed = TRUE;

                    V( p_curr->entry_lock );
                    V( pipeline[i].stage_mutex );

                    return p_curr;
                }
                V( p_curr->entry_lock );
            }
        }
        /* @TODO check configuration for max threads */
        else if ( ( entry_proc_pipeline[i].stage_flags & STAGE_FLAG_MAX_THREADS )
                  || ( entry_proc_pipeline[i].stage_flags & STAGE_FLAG_PARALLEL ) )
        {
            if ( ( entry_proc_pipeline[i].max_thread_count != 0 )
                 && ( pipeline[i].nb_threads >= entry_proc_pipeline[i].max_thread_count ) )
            {
                *p_empty = FALSE;
                /* thread quota for this stage is at maximum */
                V( pipeline[i].stage_mutex );

#ifdef _DEBUG_ENTRYPROC
                printf( "Stage[%u] - thread %#lx - thread quota reached (%u)\n", i,
                        pthread_self(  ), entry_proc_pipeline[i].max_thread_count );
#endif
                continue;
            }

            /* check entries at this stage */
            for ( p_curr = pipeline[i].first_in_ptr; p_curr != NULL; p_curr = p_curr->p_next )
            {
                /* the pipeline is not empty */
                *p_empty = FALSE;

                P( p_curr->entry_lock );

                /* manage id constraints (except for special operations) */
                if ( ( entry_proc_pipeline[i].stage_flags & STAGE_FLAG_ID_CONSTRAINT )
                     && p_curr->entry_id_is_set )
                {
                    /* skip entries that are already at superior stage (or processed) */
                    if ( ( p_curr->pipeline_stage > i ) || ( p_curr->being_processed ) )
                    {
                        if ( !p_curr->id_is_referenced )
                        {
                            DisplayLog( LVL_MAJOR, ENTRYPROC_TAG,
                                        "WARNING: Unregistered operation at higher stage" );
                            id_constraint_register( p_curr );
                        }

                        V( p_curr->entry_lock );
#ifdef _DEBUG_ENTRYPROC
                        printf
                            ( "Stage[%u] - thread %#lx - entry at higher stage (%u) or is being processed (%s) \n",
                              i, pthread_self(  ), p_curr->pipeline_stage,
                              p_curr->being_processed ? "TRUE" : "FALSE" );
#endif
                        continue;
                    }

                    /* is this the first operation for this id ? */
                    if ( p_curr != id_constraint_get_first_op( &p_curr->entry_id ) )
                    {
#ifdef _DEBUG_ENTRYPROC
                        entry_proc_op_t * other_op = id_constraint_get_first_op( &p_curr->entry_id );
#endif
                        V( p_curr->entry_lock );
#ifdef _DEBUG_ENTRYPROC
                        printf( "Stage[%u] - thread %#lx - not the first operation with this id ("DFID")\n",
                                i, pthread_self(  ), PFID(&p_curr->entry_id) );
                        if ( other_op == NULL )
                        {
                            fprintf(stderr, "ERROR!!! OPERATION NOT REGISTERED FOR THIS STEP!!!\n" );
                            abort();
                        }
                        else
                        {
                            printf( "The first operation for this id is (%p):\n", other_op);
                            dump_entry_op( other_op );
                        }
#endif
                        continue;
                    }

                }
                else if ( p_curr->being_processed || p_curr->pipeline_stage > i )
                {
                    V( p_curr->entry_lock );
                    /* check next entry */
#ifdef _DEBUG_ENTRYPROC
                    printf( "Stage[%u] - thread %#lx - entry being processed or at higher stage\n",
                            i, pthread_self(  ) );
#endif
                    continue;
                }

                /* this entry can be processed */
                /* tag the entry and update stage info */
                pipeline[i].nb_unprocessed_entries--;
                pipeline[i].nb_threads++;
                p_curr->being_processed = TRUE;

                V( p_curr->entry_lock );
                V( pipeline[i].stage_mutex );
                return p_curr;
            }

        }
        else
        {
            /* unspecified stage flag */
            DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "Error: stage flag not specified !!!" );
            V( pipeline[i].stage_mutex );
            return NULL;
        }

        /* end of current stage */
        V( pipeline[i].stage_mutex );

    }                           /*end for */

#ifdef _DEBUG_ENTRYPROC
    printf( "empty=%d\n", *p_empty );
#endif

    /* nothing found */
    return NULL;
}

/**
 * This function returns the next operation to be processed
 * according to pipeline stage/ordering constrains.
 */
entry_proc_op_t *EntryProcessor_GetNextOp(  )
{
    int            is_empty;
    entry_proc_op_t *p_op;

    P( work_avail_lock );
    nb_waiting_threads++;

    while ( ( p_op = next_work_avail( &is_empty ) ) == NULL )
    {
        if ( terminate_flag && is_empty )
        {
            nb_waiting_threads--;

            /* maybe other threads can also terminate ? */
            if ( nb_waiting_threads > 0 )
                pthread_cond_signal( &work_avail_cond );

            V( work_avail_lock );

            return NULL;
        }
#ifdef _DEBUG_ENTRYPROC
        DisplayLog( LVL_FULL, ENTRYPROC_TAG, "Thread %#lx: no work available", pthread_self(  ) );
#endif
        pthread_cond_wait( &work_avail_cond, &work_avail_lock );
    }

    nb_waiting_threads--;

    /* maybe other entries can be processed after this one ? */
    if ( nb_waiting_threads > 0 )
        pthread_cond_signal( &work_avail_cond );

    V( work_avail_lock );

    gettimeofday( &p_op->start_processing_time, NULL );

    return p_op;
}


/**
 * Advise that the entry is ready for next step of the pipeline.
 * @param next_stage The next stage to be performed for this entry
 * @param remove This flag indicates that the entry must be removed
 *        from pipeline (basically after the last step).
 */
int EntryProcessor_Acknowledge( entry_proc_op_t * p_op, unsigned int next_stage, int remove )
{
    unsigned int   curr_stage = p_op->pipeline_stage;
    int            is_first, nb_moved;
    struct timeval now, diff;

    gettimeofday( &now, NULL );
    timersub( &now, &p_op->start_processing_time, &diff );

    /* lock current stage */
    P( pipeline[curr_stage].stage_mutex );

    /* update stats */
    pipeline[curr_stage].nb_processed_entries++;
    pipeline[curr_stage].total_processed++;
    pipeline[curr_stage].nb_threads--;
    timeradd( &diff, &pipeline[curr_stage].total_processing_time,
              &pipeline[curr_stage].total_processing_time );

    /* lock entry */
    P( p_op->entry_lock );

    if ( (!remove) && (p_op->pipeline_stage >= next_stage) )
    {
        DisplayLog( LVL_CRIT, ENTRYPROC_TAG, "CRITICAL: entry is already at a higher pipeline stage %u >= %u !!!",
                    p_op->pipeline_stage, next_stage );
        return -EINVAL;
    }

    /* update its status (even if it's going to be removed) */
    p_op->being_processed = FALSE;
    p_op->pipeline_stage = next_stage;

    /* check if it is the first */
    is_first = ( p_op == pipeline[curr_stage].first_in_ptr );

    /* remove entry, if it must be */
    if ( remove )
    {
        remove_entry_from_stage( curr_stage, p_op, FALSE );

        /* remove entry constraints on this id */
        if ( p_op->id_is_referenced )
        {
            id_constraint_unregister( p_op );
        }
    }

    /* Entry can be unlocked now, because its current list is locked
     * so it won't be accessed anyway.
     * And we have all done with it, so it can be accessed in its future stage.
     */
    V( p_op->entry_lock );

    /* check if entries are to be moved from this stage */
    nb_moved = move_stage_entries( curr_stage, FALSE );

    /* unlock current stage */
    V( pipeline[curr_stage].stage_mutex );

    /* There may have some work to do in any case:
     * - if current entry has been removed (it may block other operations)
     * - if entries have moved (they are available for next stage)
     * - if the current step had a limited number of threads
     * Note: if operation is sequential, it should be the last of its step
     * so it must have been moved.
     */
    /* @TODO check configuration for max_thread_count */
    if ( remove || ( nb_moved > 0 ) || ( entry_proc_pipeline[curr_stage].max_thread_count != 0 ) )
    {
        P( work_avail_lock );
        if ( nb_waiting_threads > 0 )
            pthread_cond_signal( &work_avail_cond );
        V( work_avail_lock );
    }

    /* free entry resources if asked */
    if ( remove )
    {

        /* If a limit of pending operations is specified, release a token */
        if ( entry_proc_conf.max_pending_operations > 0 )
            semaphore_V( &pipeline_token );

        /* @todo free entry_info */

        /* free specific info */

        if ( p_op->extra_info_is_set && ( p_op->extra_info_free_func != NULL ) )
        {
            p_op->extra_info_free_func( &p_op->extra_info );
        }

        if ( p_op->entry_attr_is_set )
        {
            ListMgr_FreeAttrs( &p_op->entry_attr );
        }

        /* destroy the lock and free the memory */
        V( p_op->entry_lock );
        pthread_mutex_destroy( &p_op->entry_lock );
        pthread_mutex_destroy( &p_op->entry_id_lock );
        MemFree( p_op );
    }

    return 0;
}

static const char * entry_status_str( entry_proc_op_t * p_op, unsigned int stage )
{
    if ( p_op->being_processed )
        return "processing";
    else if ( p_op->pipeline_stage < stage )
        return "ERROR: entry at previous stage!!!";
    else if ( p_op->pipeline_stage == stage )
        return "waiting";
    else if ( p_op->pipeline_stage > stage )
        return "done";
    else
        return "ERROR: uncovered case /!\\";
}



void EntryProcessor_DumpCurrentStages(  )
{
    unsigned int   i;
    double         tpe = 0.0;
    int            is_pending_op = FALSE;

    /* no locks here, because it's just for information */

    if ( TestDisplayLevel( LVL_MAJOR ) )
    {

        DisplayLog( LVL_MAJOR, "STATS", "==== EntryProcessor Pipeline Stats ===" );
        DisplayLog( LVL_MAJOR, "STATS", "Threads waiting: %u", nb_waiting_threads );

        id_constraint_dump(  );

        for ( i = 0; i < PIPELINE_STAGE_COUNT; i++ )
        {
            P( pipeline[i].stage_mutex );
            if ( pipeline[i].total_processed )
                tpe =
                    ( ( 1000.0 * pipeline[i].total_processing_time.tv_sec ) +
                      ( 1E-3 * pipeline[i].total_processing_time.tv_usec ) ) /
                    ( double ) ( pipeline[i].total_processed );
            else
                tpe = 0.0;

            DisplayLog( LVL_MAJOR, "STATS",
                        "%2u: %-20s | Wait: %5u | Curr: %3u | Done: %3u | Total: %6llu | ms/op: %.2f",
                        i, entry_proc_pipeline[i].stage_name,
                        pipeline[i].nb_unprocessed_entries,
                        pipeline[i].nb_threads,
                        pipeline[i].nb_processed_entries, pipeline[i].total_processed, tpe );
            V( pipeline[i].stage_mutex );

            if ( pipeline[i].first_in_ptr || pipeline[i].last_in_ptr )
                is_pending_op = TRUE;
        }
    }

    if ( TestDisplayLevel( LVL_EVENT ) )
    {
        if ( is_pending_op )
        {
            DisplayLog( LVL_EVENT, "STATS", "--- Pipeline stage details ---" );
            /* pipeline stage details */
            for ( i = 0; i < PIPELINE_STAGE_COUNT; i++ )
            {
                P( pipeline[i].stage_mutex );
                if ( pipeline[i].first_in_ptr )
                {
    #ifdef HAVE_CHANGELOGS
                    if ( pipeline[i].first_in_ptr->extra_info.is_changelog_record )
                    {
                        DisplayLog( LVL_EVENT, "STATS", "%-20s: first: changelog record #%Lu, fid="DFID", status=%s",
                                    entry_proc_pipeline[i].stage_name,
                                    pipeline[i].first_in_ptr->extra_info.log_record.p_log_rec->cr_index,
                                    PFID(&pipeline[i].first_in_ptr->extra_info.log_record.p_log_rec->cr_tfid),
                                    entry_status_str( pipeline[i].first_in_ptr, i) );
                    }
                    else
    #endif
                    if ( ATTR_MASK_TEST( &pipeline[i].first_in_ptr->entry_attr, fullpath) )
                    {
                        DisplayLog( LVL_EVENT, "STATS", "%-20s: first: %s, status=%s",
                                    entry_proc_pipeline[i].stage_name,
                                    ATTR( &pipeline[i].first_in_ptr->entry_attr, fullpath ),
                                    entry_status_str( pipeline[i].first_in_ptr, i) );
                    }
                    else
                    {
                        DisplayLog( LVL_EVENT, "STATS", "%-20s: first: special op %s",
                                    entry_proc_pipeline[i].stage_name,
                                    entry_proc_pipeline[pipeline[i].first_in_ptr->pipeline_stage].stage_name );
                    }
                }
                if ( pipeline[i].last_in_ptr )
                {
    #ifdef HAVE_CHANGELOGS
                    if ( pipeline[i].last_in_ptr->extra_info.is_changelog_record )
                    {
                        DisplayLog( LVL_EVENT, "STATS", "%-20s: last: changelog record #%Lu, fid="DFID", status=%s",
                                    entry_proc_pipeline[i].stage_name,
                                    pipeline[i].last_in_ptr->extra_info.log_record.p_log_rec->cr_index,
                                    PFID(&pipeline[i].last_in_ptr->extra_info.log_record.p_log_rec->cr_tfid),
                                    entry_status_str( pipeline[i].last_in_ptr, i) );
                    }
                    else
    #endif
                    if ( ATTR_MASK_TEST( &pipeline[i].last_in_ptr->entry_attr, fullpath) )
                    {
                        DisplayLog( LVL_EVENT, "STATS", "%-20s: last: %s, status=%s",
                                    entry_proc_pipeline[i].stage_name,
                                    ATTR_MASK_TEST( &pipeline[i].last_in_ptr->entry_attr, fullpath) ?
                                        ATTR( &pipeline[i].last_in_ptr->entry_attr, fullpath ): "(path not set)",
                                    entry_status_str( pipeline[i].last_in_ptr, i) );
                    }
                    else
                    {
                        DisplayLog( LVL_EVENT, "STATS", "%-20s: last: special op %s",
                                    entry_proc_pipeline[i].stage_name,
                                    entry_proc_pipeline[pipeline[i].last_in_ptr->pipeline_stage].stage_name );
                    }
                }
                V( pipeline[i].stage_mutex );

            } /* end for */
        } /* end if pending op */
    }
}


/*
 * Functions for handling operations
 */
int EntryProcessor_SetEntryId( entry_proc_op_t * p_op, const entry_id_t * p_id )
{

    P( p_op->entry_id_lock );
    p_op->entry_id_is_set = TRUE;
    p_op->entry_id = *p_id;
    V( p_op->entry_id_lock );

    /* @TODO: remember this reference about this entry id (to check id constraints) */

    return 0;

}

void InitEntryProc_op( entry_proc_op_t * p_op )
{
    memset( p_op, 0, sizeof( entry_proc_op_t ) );

    p_op->pipeline_stage = 0;

    p_op->entry_id_is_set = FALSE;
    p_op->entry_attr_is_set = FALSE;
    p_op->extra_info_is_set = FALSE;
    p_op->db_exists = FALSE;
  
    extra_info_init( &p_op->extra_info );

    p_op->being_processed = FALSE;
    p_op->id_is_referenced = FALSE;

    p_op->db_op_type = OP_TYPE_NONE;
    p_op->callback_func = NULL;
    p_op->callback_param = NULL;
    ATTR_MASK_INIT( &p_op->entry_attr );

    p_op->extra_info_free_func = NULL;

}

/* helper for counting the number of operations in pipeline */
unsigned int count_nb_ops()
{
    int i;
    unsigned int total = 0;

    for ( i = 0; i < PIPELINE_STAGE_COUNT; i++ )
    {
        total +=  pipeline[i].nb_threads
                + pipeline[i].nb_unprocessed_entries
                + pipeline[i].nb_processed_entries ;
    }

    return total;
}

/**
 * Terminate EntryProcessor (after the queue has been flushed)
 */
int EntryProcessor_Terminate(  )
{

    P( terminate_lock );

    /* set termination flag */
    terminate_flag = TRUE;

    /* force idle thread to wake up */
    pthread_cond_broadcast( &work_avail_cond );

    /* wait for all workers to process all pipeline entries and terminate */
    while ( nb_finished_threads < entry_proc_conf.nb_thread )
    {
        DisplayLog( LVL_EVENT, ENTRYPROC_TAG,
                    "Waiting for entry processor pipeline flush: still %u operations to be done, %u threads running",
                    count_nb_ops(), entry_proc_conf.nb_thread - nb_finished_threads );
        pthread_cond_wait( &terminate_cond, &terminate_lock );
    }

    V( terminate_lock );

    DisplayLog( LVL_EVENT, ENTRYPROC_TAG, "Pipeline successfully flushed" );

    EntryProcessor_DumpCurrentStages(  );

    return 0;
}
