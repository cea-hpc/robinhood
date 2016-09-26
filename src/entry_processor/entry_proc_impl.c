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
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "list.h"
#include <semaphore.h>
#include <pthread.h>
#include <errno.h>
#include <stdlib.h>

static sem_t pipeline_token;

/* each stage of the pipeline consist of the following information: */
typedef struct __list_by_stage__ {
    struct rh_list_head entries;
    unsigned int nb_threads;    /* number of threads working on this stage */
    unsigned int nb_unprocessed_entries; /**< number of entries to be processed
                                          * in this list */
    unsigned int nb_current_entries;    /**< number of entries being processed
                                          * in the list */
    unsigned int nb_processed_entries;  /**< number of entries processed in this
                                         * list */
    unsigned long long total_processed; /**< total number of processed entries
                                         * since start */
    unsigned long long nb_batches;  /* number of batched steps */
    unsigned long long total_batched_entries; /**< total number of entries
                                               * processed as batches */
    struct timeval total_processing_time;   /**< total amount of time for
                                             * processing entries at this
                                             * stage */
    pthread_mutex_t stage_mutex;
} list_by_stage_t;

/* Note1: nb_current_entries + nb_unprocessed_entries + nb_processed_entries
 *         = nb entries at a given step */
/* stages mutex must always be taken from lower stage to upper to avoid
 * deadlocks */

static list_by_stage_t *pipeline = NULL;

/* EXPORTED VARIABLES: current pipeline in operation */
pipeline_stage_t *entry_proc_pipeline = NULL;
pipeline_descr_t entry_proc_descr = { 0 };

void *entry_proc_arg = NULL;

static pthread_mutex_t work_avail_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t work_avail_cond = PTHREAD_COND_INITIALIZER;
unsigned int nb_waiting_threads = 0;

/* termination mecanism  */
static pthread_mutex_t terminate_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t terminate_cond = PTHREAD_COND_INITIALIZER;

static enum { NONE = 0, FLUSH = 1, BREAK = 2 } terminate_flag = NONE;
static int nb_finished_threads = 0;

/* forward declarations */
static entry_proc_op_t **EntryProcessor_GetNextOp(int *count);
static void print_op_stats(entry_proc_op_t *p_op, unsigned int stage,
                           const char *what);

typedef struct worker_info__ {
    unsigned int index;
    pthread_t thread_id;
    lmgr_t lmgr;
} worker_info_t;

static worker_info_t *worker_params = NULL;

#ifdef _DEBUG_ENTRYPROC
static void dump_entry_op(entry_proc_op_t *p_op)
{
#ifdef _HAVE_FID
    if (p_op->entry_id_is_set)
        printf("id=" DFID "\n", PFID(&p_op->entry_id));
#endif
    /* mask is always set, even if fs/db_attrs is not set */
    if (ATTR_FSorDB_TEST(p_op, fullpath))
        printf("path=%s\n", ATTR_FSorDB(p_op, fullpath));

    if (p_op->extra_info.is_changelog_record)
        printf("log_rec=%s\n",
               changelog_type2str(p_op->extra_info.log_record.p_log_rec->
                                  cr_type));

    printf("stage=%u, being processed=%u, db_exists=%u, "
           "id is referenced=%u, db_op_type=%u\n",
         p_op->pipeline_stage, p_op->being_processed, p_op->db_exists,
         p_op->id_is_referenced, p_op->db_op_type);
    printf("start proc time=%u.%06u\n",
           (unsigned int)p_op->timestamp.start_processing_time.tv_sec,
           (unsigned int)p_op->timestamp.start_processing_time.tv_usec);
    printf("next=%p, prev=%p\n", p_op->list.next, p_op->list.prev);
}
#endif

/* worker thread for pipeline */
static void *entry_proc_worker_thr(void *arg)
{
    entry_proc_op_t **list_op;
    int rc;
    worker_info_t *myinfo = (worker_info_t *) arg;
    int count;

    DisplayLog(LVL_FULL, ENTRYPROC_TAG, "Starting pipeline worker thread #%u",
               myinfo->index);

    /* create connection to database */
    rc = ListMgr_InitAccess(&myinfo->lmgr);
    if (rc) {
        DisplayLog(LVL_CRIT, ENTRYPROC_TAG,
                   "Pipeline worker #%u could not connect to ListMgr. Exiting.",
                   myinfo->index);
        exit(1);
    }

    while ((list_op = EntryProcessor_GetNextOp(&count)) != NULL) {
        const pipeline_stage_t *stage_info =
            &entry_proc_pipeline[list_op[0]->pipeline_stage];
        if (count == 1) {
            /* preferably call single entry function, if it exists */
            if (stage_info->stage_function)
                stage_info->stage_function(list_op[0], &myinfo->lmgr);
            /* else, call batch function if it exists */
            else if (stage_info->stage_batch_function)
                stage_info->stage_batch_function(list_op, count, &myinfo->lmgr);
            else
                /* no function! */
                RBH_BUG("No function is defined for a pipeline step");
        } else if (count > 1) {
            /* call batch function, if it exists */
            if (stage_info->stage_batch_function)
                stage_info->stage_batch_function(list_op, count, &myinfo->lmgr);
            else
                /* no batch function! */
                RBH_BUG("Batched returned whereas no batch function is "
                        "defined for this stage");
        } else
            RBH_BUG("Empty operation list returned");

        MemFree(list_op);
    }

    if (!terminate_flag)
        DisplayLog(LVL_CRIT, ENTRYPROC_TAG,
                   "Error: EntryProcessor_GetNextOp returned NULL but no termination signal has been received!!!");

    /* All operations have been processed. Now flushing DB operations and
     * closing connection. */
    ListMgr_CloseAccess(&myinfo->lmgr);

    /* notify thread's termination */
    P(terminate_lock);
    nb_finished_threads++;
    /* always notify waiting thread, so it writes a log about the number of
     * waiting threads/ops */
    pthread_cond_signal(&terminate_cond);
    V(terminate_lock);

    DisplayLog(LVL_DEBUG, ENTRYPROC_TAG,
               "Pipeline worker thread #%u terminated", myinfo->index);

    pthread_exit(NULL);
    return NULL;

}

#ifdef _BENCH_PIPELINE
static pipeline_descr_t bench_pipeline_descr = { 0 };   /* to be set */

static pipeline_stage_t *bench_pipeline = NULL; /* to be allocated */

static int EntryProc_noop(struct entry_proc_op_t *p_op, lmgr_t *lmgr)
{
    int rc;
    /* last stage ? */
    if (p_op->pipeline_stage < bench_pipeline_descr.stage_count - 1)
        rc = EntryProcessor_Acknowledge(p_op, p_op->pipeline_stage + 1, false);
    else {
        if (p_op->callback_func)
            p_op->callback_func(lmgr, p_op, p_op->callback_param);

        /* last stage, remove from the pipeline */
        rc = EntryProcessor_Acknowledge(p_op, -1, true);
    }
    return rc;
}

static int mk_bench_pipeline(unsigned int stages)
{
    int i;
    bench_pipeline_descr.stage_count = stages;
    bench_pipeline = MemCalloc(stages, sizeof(pipeline_stage_t));
    if (bench_pipeline == NULL)
        return -ENOMEM;
    for (i = 0; i < stages; i++) {
        bench_pipeline[i].stage_index = i;
        bench_pipeline[i].stage_name = "stage_bench";
        bench_pipeline[i].stage_function = EntryProc_noop;
        bench_pipeline[i].stage_batch_function = NULL;
        bench_pipeline[i].test_batchable = NULL;
        bench_pipeline[i].stage_flags = STAGE_FLAG_PARALLEL | STAGE_FLAG_SYNC;
        bench_pipeline[i].max_thread_count = 0; /* unlimited */
    }

    if (stages > 2) {
        bench_pipeline[1].stage_flags |= STAGE_FLAG_ID_CONSTRAINT;
        bench_pipeline_descr.DB_APPLY = stages - 1;
    }
    return 0;
}
#endif

/**
 *  Initialize entry processor pipeline
 */
int EntryProcessor_Init(pipeline_flavor_e flavor, run_flags_t flags, void *arg)
{
    int i;

    pipeline_flags = flags;
    entry_proc_arg = arg;

#ifdef _BENCH_PIPELINE
    int rc;

    /* in this case, arg points to stage count */
    rc = mk_bench_pipeline(*((int *)arg));
    if (rc)
        return rc;
    entry_proc_pipeline = bench_pipeline;   /* pointer */
    entry_proc_descr = bench_pipeline_descr;    /* full copy */
#else
    switch (flavor) {
    case STD_PIPELINE:
        entry_proc_pipeline = std_pipeline; /* pointer */
        entry_proc_descr = std_pipeline_descr;  /* full copy */
        /* arg is a diff_mask */
        break;
    case DIFF_PIPELINE:
        entry_proc_pipeline = diff_pipeline;    /* pointer */
        entry_proc_descr = diff_pipeline_descr; /* full copy */
        /* arg is a diff_arg */
        break;
    default:
        DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "Pipeline flavor not supported");
        return EINVAL;
    }
#endif

    DisplayLog(LVL_FULL, "EntryProc_Config", "nb_threads=%u",
               entry_proc_conf.nb_thread);
    DisplayLog(LVL_FULL, "EntryProc_Config", "max_batch_size=%u",
               entry_proc_conf.max_batch_size);
    for (i = 0; i < entry_proc_descr.stage_count; i++) {
        if (entry_proc_pipeline[i].stage_flags & STAGE_FLAG_SEQUENTIAL)
            DisplayLog(LVL_FULL, "EntryProc_Config", "%s: sequential",
                       entry_proc_pipeline[i].stage_name);
        else if (entry_proc_pipeline[i].stage_flags & STAGE_FLAG_PARALLEL)
            DisplayLog(LVL_FULL, "EntryProc_Config", "%s: parallel",
                       entry_proc_pipeline[i].stage_name);
        else if (entry_proc_pipeline[i].stage_flags & STAGE_FLAG_MAX_THREADS)
            DisplayLog(LVL_FULL, "EntryProc_Config", "%s: %u threads max",
                       entry_proc_pipeline[i].stage_name,
                       entry_proc_pipeline[i].max_thread_count);
    }

    pipeline =
        (list_by_stage_t *) MemCalloc(entry_proc_descr.stage_count,
                                      sizeof(list_by_stage_t));
    if (!pipeline)
        return ENOMEM;

    if (entry_proc_conf.match_classes && policies.fileset_count == 0) {
        DisplayLog(LVL_EVENT, ENTRYPROC_TAG,
                   "No fileclass defined in configuration, disabling fileclass matching.");
        entry_proc_conf.match_classes = false;
    }

    /* If a limit of pending operations is specified, initialize a token */
    if (entry_proc_conf.max_pending_operations > 0)
        sem_init(&pipeline_token, 0, entry_proc_conf.max_pending_operations);

    for (i = 0; i < entry_proc_descr.stage_count; i++) {
        memset(&pipeline[i], 0, sizeof(*pipeline));
        rh_list_init(&pipeline[i].entries);
#ifdef _DEBUG_ENTRYPROC
        printf("entry list for stage %u: list=%p, next=%p, prev=%p\n",
               i, &pipeline[i].entries, pipeline[i].entries.next,
               pipeline[i].entries.prev);
#endif
        timerclear(&pipeline[i].total_processing_time);
        pthread_mutex_init(&pipeline[i].stage_mutex, NULL);
    }

    /* init id constraint manager */
    if (id_constraint_init())
        return -1;

    /* start workers */

    worker_params =
        (worker_info_t *) MemCalloc(entry_proc_conf.nb_thread,
                                    sizeof(worker_info_t));
    if (!worker_params)
        return ENOMEM;

    for (i = 0; i < entry_proc_conf.nb_thread; i++) {
        worker_params[i].index = i;
        if (pthread_create(&worker_params[i].thread_id,
                           NULL, entry_proc_worker_thr, &worker_params[i]) != 0)
        {
            DisplayLog(LVL_CRIT, ENTRYPROC_TAG,
                       "Error: Could not start worker thread");
            return errno;
        }
    }

#ifdef _DEBUG_ENTRYPROC
    EntryProcessor_DumpCurrentStages();
#endif

    return 0;
}

/**
 * This function adds a new operation, allocated through
 * GetNewEntryProc_op(), to the queue. All fields have been set to 0
 * or a proper value.
 */
void EntryProcessor_Push(entry_proc_op_t *p_entry)
{
    int i;
    unsigned int insert_stage;

    /* if a limit of pending operations is specified, wait for a token */
    if (entry_proc_conf.max_pending_operations > 0)
        sem_wait(&pipeline_token);

    /* We must always insert it in the first stage, to keep
     * the good ordering of entries.
     * Except if all stages between stage0 and insert_stage are empty
     */

    /* by default, insert stage is entry stage
     * except if there is a non empty stage before
     */
    insert_stage = p_entry->pipeline_stage;

    /* take all locks for stage0 to insert_stage or first non empty stage */
    for (i = 0; i <= p_entry->pipeline_stage; i++) {
        P(pipeline[i].stage_mutex);

        if (!rh_list_empty(&pipeline[i].entries)) {
            insert_stage = i;
            break;
        }
    }

#ifdef _DEBUG_ENTRYPROC
    if (insert_stage != p_entry->pipeline_stage)
        printf("INSERT STAGE (%u) != PUSH STAGE(%u)\n", insert_stage,
               p_entry->pipeline_stage);
#endif

    /* If the stage has an ID_CONSTRAINT and ID is set, register entry */
    if ((entry_proc_pipeline[insert_stage].
         stage_flags & STAGE_FLAG_ID_CONSTRAINT)
        && p_entry->entry_id_is_set) {
        id_constraint_register(p_entry, false);
    }
#ifdef _DEBUG_ENTRYPROC
    printf("inserting to stage %u: list=%p, next=%p, prev=%p\n",
           insert_stage, &pipeline[insert_stage].entries,
           pipeline[insert_stage].entries.next,
           pipeline[insert_stage].entries.prev);
#endif

    /* insert entry */
    rh_list_add_tail(&p_entry->list, &pipeline[insert_stage].entries);

    if (insert_stage < p_entry->pipeline_stage)
        pipeline[insert_stage].nb_processed_entries++;
    else
        pipeline[insert_stage].nb_unprocessed_entries++;

    /* release all lists lock */
    for (i = 0; i <= insert_stage; i++)
        V(pipeline[i].stage_mutex);

    /* there is a new entry to be processed ! (signal only if threads
     * are waiting) */
    P(work_avail_lock);
    if (nb_waiting_threads > 0)
        pthread_cond_signal(&work_avail_cond);
    V(work_avail_lock);

}   /* EntryProcessor_Push */

/*
 * Move terminated operations to next stage.
 * The source stage is locked.
 */
static int move_stage_entries(const unsigned int source_stage_index)
{
    entry_proc_op_t *p_first = NULL;
    entry_proc_op_t *p_last = NULL;
    entry_proc_op_t *p_curr = NULL;
    int count = 0;
    int i;
    unsigned int pipeline_stage_min;
    unsigned int insert_stage;
    struct rh_list_head rem;
    list_by_stage_t *pl;

    /* nothing to do if we are already at last step */
    if (source_stage_index >= entry_proc_descr.stage_count - 1)
        return 0;

    pl = &pipeline[source_stage_index];

    /* is there at least 1 entry to be moved ? */
    if (rh_list_empty(&pl->entries))
        goto out;

    p_first = rh_list_first_entry(&pl->entries, entry_proc_op_t, list);
    if (p_first->being_processed
        || (p_first->pipeline_stage <= source_stage_index))
        goto out;
    pipeline_stage_min = p_first->pipeline_stage;

    p_last = p_first;
    count = 1;

    /* check next entries  */
    for (p_curr = rh_list_entry(p_first->list.next, entry_proc_op_t, list);
         &p_curr->list != &pl->entries;
         p_curr = rh_list_entry(p_curr->list.next, entry_proc_op_t, list)) {
        if (!p_curr->being_processed
            && (p_curr->pipeline_stage > source_stage_index)) {
            if (p_curr->pipeline_stage < pipeline_stage_min)
                pipeline_stage_min = p_curr->pipeline_stage;
            count++;
            p_last = p_curr;
        } else {
            break;
        }
    }

    /* remove entries from current list */
    rh_list_cut_head(&pl->entries, &p_last->list, &rem);

    /* change entry count */
    pl->nb_processed_entries -= count;

    /* by default, insert stage is pipeline_stage_min
     * except if there is a non empty stage before
     */
    insert_stage = pipeline_stage_min;

    /* take all locks from next stage to insert_stage
     * or first non-empty stage */
    for (i = source_stage_index + 1; i <= pipeline_stage_min; i++) {
        P(pipeline[i].stage_mutex);

        /* make sure this stage has correctly been flushed */
        if (!rh_list_empty(&pipeline[i].entries))
            move_stage_entries(i);

        if (!rh_list_empty(&pipeline[i].entries)) {
            insert_stage = i;
            break;
        }
    }

#ifdef _DEBUG_ENTRYPROC
    if (insert_stage != pipeline_stage_min) {
        printf("Entries to be moved: %u\n", count);
        printf("INSERT STAGE (%u) != NEXT STAGE MIN(%u)\n", insert_stage,
               pipeline_stage_min);
        printf("STAGE[%u].FIRST=%s, stage=%u\n", insert_stage,
               ATTR(&rh_list_first_entry
                    (&pipeline[insert_stage].entries, entry_proc_op_t,
                     list)->fs_attrs, fullpath),
               rh_list_first_entry(&pipeline[insert_stage].entries,
                                   entry_proc_op_t, list)->pipeline_stage);
        printf("STAGE[%u].LAST=%s, stage=%u\n", insert_stage,
               ATTR(&rh_list_last_entry
                    (&pipeline[insert_stage].entries, entry_proc_op_t,
                     list)->fs_attrs, fullpath),
               rh_list_last_entry(&pipeline[insert_stage].entries,
                                  entry_proc_op_t, list)->pipeline_stage);
    }
#endif

    /* If the stage has an ID_CONSTRAINT, register entries */
    if (entry_proc_pipeline[insert_stage].
        stage_flags & STAGE_FLAG_ID_CONSTRAINT) {
        rh_list_for_each_entry(p_curr, &rem, list) {
#ifdef _DEBUG_ENTRYPROC
            if (p_curr->id_is_referenced
                || (!p_curr->entry_id_is_set && p_curr->pipeline_stage != 6)) {
                printf("moving entry %p " DFID
                       " from stage %u to %u, id is ref? %u, id_is_set? %u\n",
                       p_curr, PFID(&p_curr->entry_id), source_stage_index,
                       insert_stage, p_curr->id_is_referenced ? 0 : 1,
                       p_curr->entry_id_is_set ? 0 : 1);
            }
#endif
            if (!p_curr->id_is_referenced && p_curr->entry_id_is_set) {
                id_constraint_register(p_curr, false);
            }
        }
    }

#ifdef _DEBUG_ENTRYPROC
    DisplayLog(LVL_FULL, ENTRYPROC_TAG, "move_stage_entries: insert stage %u",
               insert_stage);
#endif

    /* check update info depending on this list */
    /* TODO: can we merge that with the previous loop ? */
    rh_list_for_each_entry(p_curr, &rem, list) {
        /* no need to take a lock to check 'p_curr->pipeline_stage',
         * because if we moved this entry, this means it is not in use.
         * And no thread can process it for now because the list is locked.
         */
        if (insert_stage < p_curr->pipeline_stage)
            pipeline[insert_stage].nb_processed_entries++;
        else
            pipeline[insert_stage].nb_unprocessed_entries++;
    }

    /* insert entry list */
    rh_list_splice_tail(&pipeline[insert_stage].entries, &rem);

    /* release all lists lock (except the source one) */
    for (i = source_stage_index + 1; i <= insert_stage; i++)
        V(pipeline[i].stage_mutex);

 out:
    return count;
}   /* move_stage_entries */

/**
 * Return an entry to be processed.
 * This entry is tagged "being_processed" and stage info is updated.
 * @param p_empty Output Boolean. In the case no entry is returned,
 *        this indicates if it is because the pipeline is empty.
 */
static entry_proc_op_t **next_work_avail(bool *p_empty, int *op_count)
{
    entry_proc_op_t *p_curr;
    int i;
    int tot_entries = 0;

    if (terminate_flag == BREAK)
        return NULL;

    *p_empty = true;

    /* check every stage from the last to the first */
    for (i = entry_proc_descr.stage_count - 1; i >= 0; i--) {
        list_by_stage_t *pl = &pipeline[i];

        /* entries have not been processed at this stage. */
        P(pl->stage_mutex);

        /* Accumulate the number of entries in the upper stages. */
        tot_entries +=
            pl->nb_current_entries + pl->nb_unprocessed_entries +
            pl->nb_processed_entries;

        if (pl->nb_unprocessed_entries == 0) {
            V(pl->stage_mutex);
#ifdef _DEBUG_ENTRYPROC
            printf("Stage[%u] - thread %#lx - no waiting entries\n", i,
                   pthread_self());
#endif
            continue;
        }

        if (entry_proc_pipeline[i].stage_flags & STAGE_FLAG_SEQUENTIAL) {
            /*
             * If there is already an operation being processed,
             * nothing can be done at this stage.
             */
            if (pl->nb_threads != 0) {
                *p_empty = false;
                V(pl->stage_mutex);
#ifdef _DEBUG_ENTRYPROC
                printf("Stage[%u] - thread %#lx - a thread is already working "
                       "on this sequential stage\n", i, pthread_self());
#endif
                continue;
            }

            /* In case of a sequential operation, the only entry that can be
             * processed is the first entry in this stage.
             */
            rh_list_for_each_entry(p_curr, &pl->entries, list) {
                /* the pipeline is not empty */
                *p_empty = false;

                if (p_curr->pipeline_stage == i) {

                    /* satisfies ID_CONSTRAINT ?  */

                    if ((entry_proc_pipeline[i].
                         stage_flags & STAGE_FLAG_ID_CONSTRAINT)
                        && p_curr->entry_id_is_set) {
                        if (!id_constraint_is_first_op(p_curr)) {
                            DisplayLog(LVL_FULL, ENTRYPROC_TAG,
                                       "=============== Not the first unprocessed operation for this id ============");
                            /* This is not the first unprocessed operation for
                             * this id */
                            V(pl->stage_mutex);
                            return NULL;
                        }
                        /* else: entry can be added */
                    }

                    /* sanity check */
                    if (p_curr->being_processed) {
                        DisplayLog(LVL_MAJOR, ENTRYPROC_TAG,
                                   "Error: INCONSISTENCY: nb_threads running this step is 0 whereas an entry is being processed !!!");
                        V(pl->stage_mutex);
                        return NULL;
                    }

                    /* tag the entry and update stage info */
                    pl->nb_unprocessed_entries--;
                    pl->nb_current_entries++;
                    pl->nb_threads++;
                    p_curr->being_processed = 1;

                    V(pl->stage_mutex);

                    entry_proc_op_t **listop =
                        MemAlloc(sizeof(entry_proc_op_t *));
                    if (listop) {
                        *listop = p_curr;
                        *op_count = 1;
                    }
                    return listop;
                }
            }
        }
        /* @TODO check configuration for max threads */
        else if ((entry_proc_pipeline[i].stage_flags & STAGE_FLAG_MAX_THREADS)
                 || (entry_proc_pipeline[i].stage_flags
                     & STAGE_FLAG_PARALLEL)) {
            if ((entry_proc_pipeline[i].max_thread_count != 0)
                && (pl->nb_threads
                    >= entry_proc_pipeline[i].max_thread_count)) {
                *p_empty = false;
                /* thread quota for this stage is at maximum */
                V(pl->stage_mutex);

#ifdef _DEBUG_ENTRYPROC
                printf("Stage[%u] - thread %#lx - thread quota reached (%u)\n",
                       i, pthread_self(),
                       entry_proc_pipeline[i].max_thread_count);
#endif
                continue;
            }

            if (entry_proc_pipeline[i].stage_flags & STAGE_FLAG_FORCE_SEQ) {
                /* One thread is processing an operation, and that one
                 * must be the only one in this stage. */
                V(pl->stage_mutex);
                continue;
            }

            /* check entries at this stage */
            rh_list_for_each_entry(p_curr, &pl->entries, list) {
                /* the pipeline is not empty */
                *p_empty = false;

                /* Special case when the op doesn't have an ID, but
                 * the stage has a constraint. */
                if (!p_curr->entry_id_is_set &&
                    entry_proc_pipeline[i].
                    stage_flags & STAGE_FLAG_ID_CONSTRAINT) {
                    /* Do not process past this entry, unless it's the
                     * first in list and the rest of the pipeline is
                     * empty. */
                    if (p_curr ==
                        rh_list_first_entry(&pl->entries, entry_proc_op_t, list)
                        && tot_entries - pl->nb_unprocessed_entries == 0) {
                        /* This is the first entry, and there is no
                         * other entry being processed in this or the
                         * upper stages. So we can process it */
                        entry_proc_pipeline[i].stage_flags |=
                            STAGE_FLAG_FORCE_SEQ;
                    } else {
                        break;
                    }
                }

                /* manage id constraints (except for special operations) */
                if ((entry_proc_pipeline[i].
                     stage_flags & STAGE_FLAG_ID_CONSTRAINT)
                    && p_curr->entry_id_is_set) {
                    /* skip entries that are already at superior stage
                     * (or processed) */
                    if ((p_curr->pipeline_stage > i)
                        || (p_curr->being_processed)) {
                        if (!p_curr->id_is_referenced) {
                            DisplayLog(LVL_MAJOR, ENTRYPROC_TAG,
                                       "WARNING: Unregistered operation at higher stage");
                            id_constraint_register(p_curr, false);
                        }
#ifdef _DEBUG_ENTRYPROC
                        printf("Stage[%u] - thread %#lx - "
                               "entry at higher stage (%u) or is being "
                               "processed (%s) \n",
                                i, pthread_self(), p_curr->pipeline_stage,
                                bool2str(p_curr->being_processed));
#endif
                        continue;
                    }

                    /* is this the first operation for this id ? */
                    if (!id_constraint_is_first_op(p_curr))
                        continue;
                } else if (p_curr->being_processed
                           || p_curr->pipeline_stage > i) {
                    /* check next entry */
#ifdef _DEBUG_ENTRYPROC
                    printf("Stage[%u] - thread %#lx - "
                           "entry being processed or at higher stage\n",
                           i, pthread_self());
#endif
                    continue;
                }

                /* this entry can be processed */
                /* tag the entry and update stage info */
                pl->nb_unprocessed_entries--;
                pl->nb_current_entries++;
                pl->nb_threads++;
                p_curr->being_processed = 1;

                entry_proc_op_t **listop =
                    MemCalloc(entry_proc_conf.max_batch_size,
                              sizeof(entry_proc_op_t *));
                if (!listop)
                    return NULL;
                listop[0] = p_curr;
                *op_count = 1;

                /* check if this stage is batchable */
                if (entry_proc_conf.max_batch_size > 1
                    && entry_proc_pipeline[i].test_batchable != NULL
                    && entry_proc_pipeline[i].stage_batch_function != NULL) {
                    entry_proc_op_t *p_next;
                    attr_mask_t batch_mask = p_curr->fs_attrs.attr_mask;

                    rh_list_for_each_entry_after(p_next, &pl->entries, p_curr,
                                                 list) {
                        if (*op_count >= entry_proc_conf.max_batch_size)
                            break;
                        else if (p_next->being_processed
                                 || (p_next->pipeline_stage != i))
                            /* entry is already beeing processed or is at
                             * a different stage */
                            break;

                        if (entry_proc_pipeline[i].
                            test_batchable(p_curr, p_next, &batch_mask)) {
                            pl->nb_unprocessed_entries--;
                            pl->nb_current_entries++;
                            p_next->being_processed = 1;

                            listop[*op_count] = p_next;
                            (*op_count)++;
                        } else
                            /* stop at first non-batchable entry */
                            break;
                    }
                }

                V(pl->stage_mutex);

                return listop;
            }

        } else {
            /* unspecified stage flag */
            DisplayLog(LVL_CRIT, ENTRYPROC_TAG,
                       "Error: stage flag not specified !!!");
            V(pl->stage_mutex);
            return NULL;
        }

        /* end of current stage */
        V(pl->stage_mutex);

    }   /*end for */

#ifdef _DEBUG_ENTRYPROC
    printf("empty=%d\n", *p_empty);
#endif

    /* nothing found */
    return NULL;
}

/**
 * This function returns the next operation to be processed
 * according to pipeline stage/ordering constrains.
 */
static entry_proc_op_t **EntryProcessor_GetNextOp(int *count)
{
    bool is_empty;
    entry_proc_op_t **list_op;
    int i;
    *count = 0;

    P(work_avail_lock);
    nb_waiting_threads++;

    while ((list_op = next_work_avail(&is_empty, count)) == NULL) {
        if ((terminate_flag == BREAK)
            || ((terminate_flag == FLUSH) && is_empty)) {
            nb_waiting_threads--;

            /* maybe other threads can also terminate ? */
            if (nb_waiting_threads > 0)
                pthread_cond_signal(&work_avail_cond);

            V(work_avail_lock);

            return NULL;
        }
#ifdef _DEBUG_ENTRYPROC
        DisplayLog(LVL_FULL, ENTRYPROC_TAG, "Thread %#lx: no work available",
                   pthread_self());
#endif
        pthread_cond_wait(&work_avail_cond, &work_avail_lock);
    }

    nb_waiting_threads--;

    /* maybe other entries can be processed after this one ? */
    if (nb_waiting_threads > 0)
        pthread_cond_signal(&work_avail_cond);

    V(work_avail_lock);

    gettimeofday(&(list_op[0]->timestamp.start_processing_time), NULL);
    for (i = 1; i < *count; i++)
        list_op[i]->timestamp.start_processing_time =
            list_op[0]->timestamp.start_processing_time;

    return list_op;
}

/**
 * Release an entry op.
 */
void EntryProcessor_Release(entry_proc_op_t *p_op)
{
    /* @todo free entry_info */

    /* free specific info */

    if (p_op->extra_info_is_set && (p_op->extra_info_free_func != NULL)) {
        p_op->extra_info_free_func(&p_op->extra_info);
    }

    ListMgr_FreeAttrs(&p_op->fs_attrs);
    ListMgr_FreeAttrs(&p_op->db_attrs);

    /* free the memory */
    MemFree(p_op);
}

/**
 * Acknownledge a batch of operations.
 */
int EntryProcessor_AcknowledgeBatch(entry_proc_op_t **ops, unsigned int count,
                                    unsigned int next_stage, bool remove)
{
    const unsigned int curr_stage = ops[0]->pipeline_stage;
    list_by_stage_t *pl = &pipeline[curr_stage];
    int nb_moved;
    struct timeval now, diff;
    int i;

    gettimeofday(&now, NULL);
    timersub(&now, &ops[0]->timestamp.start_processing_time, &diff);

    /* lock current stage */
    P(pl->stage_mutex);

    /* update stats */
    pl->nb_processed_entries += count;
    pl->nb_current_entries -= count;
    pl->total_processed += count;

    if (count > 1) {
        pl->nb_batches++;
        pl->total_batched_entries += count;
    }
    pl->nb_threads--;
    timeradd(&diff, &pl->total_processing_time, &pl->total_processing_time);

    for (i = 0; i < count; i++) {
        /* sanity check */
        if ((!remove) && (ops[i]->pipeline_stage >= next_stage)) {
            DisplayLog(LVL_CRIT, ENTRYPROC_TAG, "CRITICAL: entry is already"
                       " in a higher pipeline stage %u >= %u !!!",
                       ops[i]->pipeline_stage, next_stage);

            V(pl->stage_mutex);
            RBH_BUG("Entry is already in a higher pipeline stage.");
        }

        /* update their status */
        ops[i]->being_processed = 0;
        ops[i]->pipeline_stage = next_stage;

        /* remove the entry, if it must be */
        if (remove) {
            /* update stage info. */
            pl->nb_processed_entries--;
            rh_list_del_init(&ops[i]->list);

            /* remove entry constraints on this id */
            if (ops[i]->id_is_referenced)
                id_constraint_unregister(ops[i]);
        }
    }

    /* We're done with the entries in that stage. */

    /* check if entries are to be moved from this stage */
    nb_moved = move_stage_entries(curr_stage);

    /* unlock current stage */
    V(pl->stage_mutex);

    /* There may have some work to do in any case:
     * - if current entry has been removed (it may block other operations)
     * - if entries have moved (they are available for next stage)
     * - if the current step had a limited number of threads
     * Note: if operation is sequential, it should be the last of its step
     * so it must have been moved.
     */
    /* @TODO check configuration for max_thread_count */
    if (remove || (nb_moved > 0)
        || (entry_proc_pipeline[curr_stage].max_thread_count != 0)) {
        P(work_avail_lock);
        if (nb_waiting_threads > 0)
            pthread_cond_signal(&work_avail_cond);
        V(work_avail_lock);
    }

    /* free entry resources if asked */
    if (remove) {
        for (i = 0; i < count; i++) {
            /* If a limit of pending operations is specified, release a token */
            if (entry_proc_conf.max_pending_operations > 0)
                sem_post(&pipeline_token);

            EntryProcessor_Release(ops[i]);
        }
    }

    return 0;
}

/**
 * Advise that the entry is ready for next step of the pipeline.
 * @param next_stage The next stage to be performed for this entry
 * @param remove This flag indicates that the entry must be removed
 *        from pipeline (basically after the last step).
 */
int EntryProcessor_Acknowledge(entry_proc_op_t *p_op, unsigned int next_stage,
                               bool remove)
{
    return EntryProcessor_AcknowledgeBatch(&p_op, 1, next_stage, remove);
}

static const char *entry_status_str(entry_proc_op_t *p_op, unsigned int stage)
{
    if (p_op->being_processed)
        return "processing";
    else if (p_op->pipeline_stage < stage)
        return "ERROR: entry at previous stage!!!";
    else if (p_op->pipeline_stage == stage)
        return "waiting";
    else if (p_op->pipeline_stage > stage)
        return "done";
    else
        return "ERROR: uncovered case /!\\";
}

static void print_op_stats(entry_proc_op_t *p_op, unsigned int stage,
                           const char *what)
{
#ifdef HAVE_CHANGELOGS
    if (p_op->extra_info.is_changelog_record) {
        DisplayLog(LVL_EVENT, "STATS",
                   "%-14s: %s: changelog record #%llu, fid=" DFID ", status=%s",
                   strchr(entry_proc_pipeline[stage].stage_name, '_') + 1, what,
                   p_op->extra_info.log_record.p_log_rec->cr_index,
                   PFID(&p_op->extra_info.log_record.p_log_rec->cr_tfid),
                   entry_status_str(p_op, stage));
    } else
#endif
    if (ATTR_FSorDB_TEST(p_op, fullpath)) {
        DisplayLog(LVL_EVENT, "STATS", "%-14s: %s: %s, status=%s",
                   strchr(entry_proc_pipeline[stage].stage_name, '_') + 1, what,
                   ATTR_FSorDB(p_op, fullpath), entry_status_str(p_op, stage));
    } else if (p_op->entry_id_is_set) {
        DisplayLog(LVL_EVENT, "STATS", "%-14s: %s: " DFID ", status=%s",
                   strchr(entry_proc_pipeline[stage].stage_name, '_') + 1, what,
                   PFID(&p_op->entry_id), entry_status_str(p_op, stage));
    } else
        DisplayLog(LVL_EVENT, "STATS", "%-14s: %s: special op, status=%s",
                   strchr(entry_proc_pipeline[stage].stage_name, '_') + 1, what,
                   entry_status_str(p_op, stage));
}

void EntryProcessor_DumpCurrentStages(void)
{
    unsigned int i;
    double tpe = 0.0;
    bool is_pending_op = false;
    unsigned int nb_get, nb_ins, nb_upd, nb_rm;

    if (!entry_proc_pipeline)
        return; /* not initialized */

    /* no locks here, because it's just for information */

    if (TestDisplayLevel(LVL_MAJOR)) {

        DisplayLog(LVL_MAJOR, "STATS",
                   "==== EntryProcessor Pipeline Stats ===");
        DisplayLog(LVL_MAJOR, "STATS", "Idle threads: %u", nb_waiting_threads);

        id_constraint_stats();

        DisplayLog(LVL_MAJOR, "STATS",
                   "%-18s | Wait | Curr | Done |     Total | ms/op |", "Stage");

        for (i = 0; i < entry_proc_descr.stage_count; i++) {
            P(pipeline[i].stage_mutex);

            if (pipeline[i].total_processed != 0)
                tpe =
                    ((1000.0 * pipeline[i].total_processing_time.tv_sec) +
                     (1E-3 * pipeline[i].total_processing_time.tv_usec)) /
                    (double)(pipeline[i].total_processed);
            else
                tpe = 0.0;

            if (pipeline[i].nb_batches > 0)
                DisplayLog(LVL_MAJOR, "STATS", "%2u: %-14s |%5u | %4u | %4u | %9llu | %5.2f | %.2f%% batched (avg batch size: %.1f)",
                           i, strchr(entry_proc_pipeline[i].stage_name, '_') + 1, /* removes STAGE_ */
                           pipeline[i].nb_unprocessed_entries,
                           pipeline[i].nb_current_entries,
                           pipeline[i].nb_processed_entries,
                           pipeline[i].total_processed, tpe,
                           pipeline[i].total_processed ? 100.0 *
                           (float)pipeline[i].total_batched_entries /
                           (float)pipeline[i].total_processed : 0.0,
                           (float)pipeline[i].total_batched_entries /
                           (float)pipeline[i].nb_batches);
            else
                DisplayLog(LVL_MAJOR, "STATS", "%2u: %-14s |%5u | %4u | %4u | %9llu | %5.2f |",
                           i, strchr(entry_proc_pipeline[i].stage_name, '_') + 1, /* removes STAGE_ */
                           pipeline[i].nb_unprocessed_entries,
                           pipeline[i].nb_current_entries,
                           pipeline[i].nb_processed_entries,
                           pipeline[i].total_processed, tpe);
            V(pipeline[i].stage_mutex);

            if (!rh_list_empty(&pipeline[i].entries))
                is_pending_op = true;
        }
        nb_get = nb_ins = nb_upd = nb_rm = 0;
        for (i = 0; i < entry_proc_conf.nb_thread; i++) {
            if (worker_params) {
                nb_get += worker_params[i].lmgr.nbop[OPIDX_GET];
                nb_ins += worker_params[i].lmgr.nbop[OPIDX_INSERT];
                nb_upd += worker_params[i].lmgr.nbop[OPIDX_UPDATE];
                nb_rm += worker_params[i].lmgr.nbop[OPIDX_RM];
            }
        }
        DisplayLog(LVL_MAJOR, "STATS", "DB ops: get=%u/ins=%u/upd=%u/rm=%u",
                   nb_get, nb_ins, nb_upd, nb_rm);
    }

    if (TestDisplayLevel(LVL_EVENT)) {
        if (is_pending_op) {
            DisplayLog(LVL_EVENT, "STATS", "--- Pipeline stage details ---");
            /* pipeline stage details */
            for (i = 0; i < entry_proc_descr.stage_count; i++) {
                P(pipeline[i].stage_mutex);
                if (!rh_list_empty(&pipeline[i].entries)) {
                    entry_proc_op_t *op1, *op2;
                    op1 =
                        rh_list_first_entry(&pipeline[i].entries,
                                            entry_proc_op_t, list);
                    op2 =
                        rh_list_last_entry(&pipeline[i].entries,
                                           entry_proc_op_t, list);

                    if (op1 != op2) {
                        print_op_stats(op1, i, "first");
                        print_op_stats(op2, i, "last");
                    } else
                        print_op_stats(op1, i, "(1 op)");
                }
                V(pipeline[i].stage_mutex);

            }   /* end for */
        }   /* end if pending op */
    }
}

entry_proc_op_t *EntryProcessor_Get(void)
{
    /* allocate a new pipeline entry */
    entry_proc_op_t *p_entry;

    p_entry = (entry_proc_op_t *) MemCalloc(1, sizeof(entry_proc_op_t));

    if (!p_entry)
        return NULL;

    /* nothing is set */
    ATTR_MASK_INIT(&p_entry->db_attrs);
    ATTR_MASK_INIT(&p_entry->fs_attrs);

    extra_info_init(&p_entry->extra_info);

    return p_entry;
}

/* helper for counting the number of operations in pipeline */
static unsigned int count_nb_ops(void)
{
    int i;
    unsigned int total = 0;

    for (i = 0; i < entry_proc_descr.stage_count; i++) {
        total += pipeline[i].nb_current_entries
            + pipeline[i].nb_unprocessed_entries
            + pipeline[i].nb_processed_entries;
    }

    return total;
}

/**
 * Terminate EntryProcessor
 * \param flush_ops: wait the queue to be flushed
 */
int EntryProcessor_Terminate(bool flush_ops)
{

    P(terminate_lock);

    /* set termination flag (if not already set) */
    if (flush_ops) {
        if (terminate_flag < FLUSH)
            terminate_flag = FLUSH;
    } else {
        if (terminate_flag < BREAK)
            terminate_flag = BREAK;
    }

    DisplayLog(LVL_DEBUG, ENTRYPROC_TAG, "EntryProcessor shutdown mode: %s",
               terminate_flag == BREAK ? "BREAK" : "FLUSH");

    /* force idle thread to wake up */
    pthread_cond_broadcast(&work_avail_cond);

    /* wait for all workers to process all pipeline entries and terminate */
    while (nb_finished_threads < entry_proc_conf.nb_thread) {
        if (terminate_flag == FLUSH)
            DisplayLog(LVL_VERB, ENTRYPROC_TAG,
                       "Waiting for entry processor pipeline flush: still %u operations to be done, %u threads running",
                       count_nb_ops(),
                       entry_proc_conf.nb_thread - nb_finished_threads);
        else if (terminate_flag == BREAK)
            DisplayLog(LVL_VERB, ENTRYPROC_TAG,
                       "Waiting for current operations to end: still %u threads running",
                       entry_proc_conf.nb_thread - nb_finished_threads);

        pthread_cond_wait(&terminate_cond, &terminate_lock);
    }

    V(terminate_lock);

    DisplayLog(LVL_EVENT, ENTRYPROC_TAG, "Pipeline successfully flushed");

    EntryProcessor_DumpCurrentStages();

    return 0;
}

/*
 * A stage was blocked waiting for an operation to get its FID. This
 * is now done, so unblock the stage.
 */
void EntryProcessor_Unblock(int stage)
{
    P(pipeline[stage].stage_mutex);

    /* and unset the block. */
    entry_proc_pipeline[stage].stage_flags &= ~STAGE_FLAG_FORCE_SEQ;

    V(pipeline[stage].stage_mutex);
}
