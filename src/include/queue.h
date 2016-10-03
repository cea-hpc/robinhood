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
 * \file queue.h
 * \brief Module for managing the queue of files to be purged.
 */

#include <semaphore.h>
#include <time.h>
#include <pthread.h>

#ifndef _QUEUE_MNGMT_H
#define _QUEUE_MNGMT_H

typedef struct entry_queue_t {
    /* cyclic array of entries */
    void          **queue;

    /* size and indexes */
    unsigned int    array_size;
    unsigned int    queue_size;
    unsigned int    first_index;
    unsigned int    last_index;

    /* mutex for accessing the queue */
    pthread_mutex_t queue_lock;

    /* token for free slots */
    sem_t           sem_empty;
    /* token for filled slots */
    sem_t           sem_full;

    /* ==== stats ==== */

    time_t          last_submitted;
    time_t          last_unqueued;
    time_t          last_ack;

    /* idle threads */
    unsigned int    nb_thr_waiting;

    /* array of status count */
    unsigned int   *status_array;
    unsigned int    status_count;

    /* special fields for counting feedback info */
    unsigned long long *feedback_array;
    unsigned int    feedback_count;

} entry_queue_t;

/**
 * Queue initialization.
 * @param queue_size: buffer size (over this count, inserts are blocking).
 * @param max_status: the max value for status (will keep track of acknowledgments from 0 to this value)
 * @param feedback_count: the number of feedback values from workers
 */
int CreateQueue(entry_queue_t *p_queue, unsigned int queue_size,
                unsigned int max_status, unsigned int feedback_count);

/**
 * Reset status info
 */
void Reset_StatusCount(entry_queue_t *p_queue);

/**
 * Reset feedback info at given index
 */
void Reset_Feedback(entry_queue_t *p_queue, unsigned int feedback_index);

/**
 * Insert an entry to the queue.
 * Can be blocking if the queue is full.
 */
int Queue_Insert(entry_queue_t *p_queue, void *entry);

/**
 * Get an entry from the queue.
 * The call is blocking until there is an element available
 * in the queue.
 */
int Queue_Get(entry_queue_t *p_queue, void **p_ptr);

/**
 * Acknwoledge when an entry has been handled.
 * Indicates the status and optionnal feedback info (as unsigned long long
 * array). To be called by the worker thread.
 */
void Queue_Acknowledge(entry_queue_t *p_queue, unsigned int status,
                       unsigned long long *feedback_array,
                       unsigned int feedback_count);

void RetrieveQueueStats(entry_queue_t *p_queue, unsigned int *p_nb_thr_wait,
                        unsigned int *p_nb_items, time_t *p_last_submitted,
                        time_t *p_last_unqueued, time_t *p_last_ack,
                        unsigned int *status_array,
                        unsigned long long *feedback_array);

#endif
