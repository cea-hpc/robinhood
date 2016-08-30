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

/**
 * Module for handling queue of items with feedback management.
 * The algorithm is based on a cyclic queue.
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "queue.h"
#include "rbh_logs.h"
#include "Memory.h"
#include "rbh_misc.h"

#include <pthread.h>

#define QUEUE_TAG "Queue"

/* cyclic queue utilities */
#define is_empty(pq) ((pq)->first_index == (pq)->last_index)
#define is_full(pq)  ((pq)->first_index == \
                      ((pq)->last_index + 1) % (pq)->array_size)
#define nb_items(pq) (((pq)->last_index - (pq)->first_index) % (pq)->array_size)

#define lockq(pq) pthread_mutex_lock(&((pq)->queue_lock))
#define unlockq(pq) pthread_mutex_unlock(&((pq)->queue_lock))

/**
 * Initialize a queue.
 */
int CreateQueue(entry_queue_t *p_queue, unsigned int queue_size,
                unsigned int max_status, unsigned int feedback_count)
{
    int rc;

    if (!p_queue)
        return EFAULT;

    /* number of slots that can be used */
    p_queue->queue_size = queue_size;

    /* array must be 1 slot larger than queue_size because 1 slot is lost
     * in cyclic queue management */
    p_queue->array_size = queue_size + 1;

    p_queue->first_index = 0;
    p_queue->last_index = 0;

    /* allocates array of entries and stats */
    p_queue->queue = MemCalloc(p_queue->array_size, sizeof(void *));
    if (p_queue->queue == NULL)
        return ENOMEM;

    p_queue->status_count = max_status + 1;
    p_queue->status_array = MemCalloc(max_status + 1, sizeof(unsigned int));
    if (p_queue->status_array == NULL)
        return ENOMEM;

    p_queue->feedback_count = feedback_count;
    p_queue->feedback_array =
        MemCalloc(feedback_count, sizeof(unsigned long long));
    if (p_queue->feedback_array == NULL)
        return ENOMEM;

    /* init locks */
    pthread_mutex_init(&p_queue->queue_lock, NULL);

    rc = sem_init(&p_queue->sem_empty, 0, queue_size);
    if (rc)
        return rc;

    rc = sem_init(&p_queue->sem_full, 0, 0);
    if (rc)
        return rc;

    /* init stats */
    p_queue->last_submitted = 0;
    p_queue->last_unqueued = 0;
    p_queue->last_ack = 0;
    p_queue->nb_thr_waiting = 0;

    return 0;
}

/**
 * Reset status info
 */
void Reset_StatusCount(entry_queue_t *p_queue)
{
    unsigned int i;

    lockq(p_queue);

    for (i = 0; i < p_queue->status_count; i++)
        p_queue->status_array[i] = 0;

    unlockq(p_queue);
}

/**
 * Reset feedback info at given index
 */
void Reset_Feedback(entry_queue_t *p_queue, unsigned int feedback_index)
{
    if (feedback_index >= p_queue->feedback_count) {
        DisplayLog(LVL_CRIT, QUEUE_TAG,
                   "Error: feedback_index overflow (feedback_index=%u, max=%u)",
                   feedback_index, p_queue->feedback_count - 1);
        return;
    }

    lockq(p_queue);

    p_queue->feedback_array[feedback_index] = 0;

    unlockq(p_queue);

}

/**
 * Insert an entry to the queue.
 * Can be blocking if the queue is full.
 */
int Queue_Insert(entry_queue_t *p_queue, void *entry)
{

    if (p_queue == NULL)
        return EFAULT;

    sem_wait_safe(&p_queue->sem_empty); /* wait for free places */

    lockq(p_queue); /* enter into the critical section */

    /* The queue should not be full */
    if (is_full(p_queue)) {

        unlockq(p_queue);
        DisplayLog(LVL_CRIT, QUEUE_TAG,
                   "UNEXPECTED ERROR: queue should not be full!");
        return EFAULT;
    } else {

        /* Inserts data into the queue */
        p_queue->queue[p_queue->last_index] = entry;
        p_queue->last_index = (p_queue->last_index + 1) % p_queue->array_size;

    }

    p_queue->last_submitted = time(NULL);

    unlockq(p_queue);

    sem_post_safe(&p_queue->sem_full);  /* increase filled places */

    return 0;

}

/**
 * Get an entry from the queue.
 * The call is blocking until there is an element available
 * in the queue.
 */
int Queue_Get(entry_queue_t *p_queue, void **p_ptr)
{
    lockq(p_queue);
    p_queue->nb_thr_waiting++;
    unlockq(p_queue);

    sem_wait_safe(&p_queue->sem_full);  /* wait for filled places */

    lockq(p_queue); /* enters into the critical section */

    p_queue->nb_thr_waiting--;

    /* The queue should not be empty */
    if (is_empty(p_queue)) {

        unlockq(p_queue);
        DisplayLog(LVL_CRIT, QUEUE_TAG,
                   "UNEXPECTED ERROR: queue should not be empty!");
        return EFAULT;
    } else {
        /* retrieves data into the queue */
        *p_ptr = p_queue->queue[p_queue->first_index];
        p_queue->first_index = (p_queue->first_index + 1) % p_queue->array_size;
    }

    p_queue->last_unqueued = time(NULL);

    unlockq(p_queue);

    sem_post_safe(&p_queue->sem_empty); /* increase free places */

    return 0;

}

/**
 * Acknwoledge when an entry has been handled.
 * Indicates the status and optionnal feedback info
 * (as unsigned long long array).
 * (to be called by the worker thread)
 */
void Queue_Acknowledge(entry_queue_t *p_queue, unsigned int status,
                       unsigned long long *feedback_array,
                       unsigned int feedback_count)
{
    unsigned int i;

    lockq(p_queue);

    if (status >= p_queue->status_count)
        DisplayLog(LVL_CRIT, QUEUE_TAG,
                   "ERROR: status overflow (status=%u, max=%u)", status,
                   p_queue->status_count - 1);
    else
        p_queue->status_array[status]++;

    if (feedback_count > p_queue->feedback_count)
        DisplayLog(LVL_CRIT, QUEUE_TAG,
                   "ERROR: feedback_array overflow (feedback_count=%u, max=%u)",
                   feedback_count, p_queue->feedback_count);

    for (i = 0; i < MIN2(feedback_count, p_queue->feedback_count); i++)
        p_queue->feedback_array[i] += feedback_array[i];

    p_queue->last_ack = time(NULL);

    unlockq(p_queue);
}

void RetrieveQueueStats(entry_queue_t *p_queue, unsigned int *p_nb_thr_wait,
                        unsigned int *p_nb_items, time_t *p_last_submitted,
                        time_t *p_last_unqueued, time_t *p_last_ack,
                        unsigned int *status_array,
                        unsigned long long *feedback_array)
{
    unsigned int i;

    lockq(p_queue);

    if (p_nb_thr_wait)
        *p_nb_thr_wait = p_queue->nb_thr_waiting;
    if (p_nb_items)
        *p_nb_items = nb_items(p_queue);
    if (p_last_submitted)
        *p_last_submitted = p_queue->last_submitted;
    if (p_last_unqueued)
        *p_last_unqueued = p_queue->last_unqueued;
    if (p_last_ack)
        *p_last_ack = p_queue->last_ack;

    if (status_array)
        for (i = 0; i < p_queue->status_count; i++)
            status_array[i] = p_queue->status_array[i];

    if (feedback_array)
        for (i = 0; i < p_queue->feedback_count; i++)
            feedback_array[i] = p_queue->feedback_array[i];

    unlockq(p_queue);
}
