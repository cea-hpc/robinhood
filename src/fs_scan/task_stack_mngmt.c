/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2007, 2008, 2009 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
/**
 * Module for managing FS scan tasks as a stack
 * with priorities on entry depth.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "fs_scan.h"
#include "task_stack_mngmt.h"
#include "rbh_logs.h"
#include "rbh_misc.h"

/* Initialize a stack of tasks */
int InitTaskStack(task_stack_t *p_stack)
{
    unsigned int index;
    int rc;

    /* initialize each level of the priority stack */
    for (index = 0; index <= MAX_TASK_DEPTH; index++) {
        p_stack->tasks_at_depth[index] = NULL;
    }

    /* no task waiting for now */
    p_stack->max_task_depth = 0;

    /* initialize the lock and the semaphore for accessing the list */
    pthread_mutex_init(&p_stack->stack_lock, NULL);

    /* initially, no task available: sem=0 */
    if ((rc = sem_init(&p_stack->sem_tasks, 0, 0))) {
        pthread_mutex_destroy(&p_stack->stack_lock);
        DisplayLog(LVL_CRIT, FSSCAN_TAG, "ERROR initializing semaphore");
        return rc;
    }

    return 0;

}

/* insert a task in the stack */
void InsertTask_to_Stack(task_stack_t *p_stack, robinhood_task_t *p_task)
{
    unsigned int prof = p_task->depth;

    /* don't distinguish priorities over a given depth */
    if (prof > MAX_TASK_DEPTH)
        prof = MAX_TASK_DEPTH;

    /* take the lock on stack */
    P(p_stack->stack_lock);

    /* insert the task at the good depth */
    p_task->next_task = p_stack->tasks_at_depth[prof];
    p_stack->tasks_at_depth[prof] = p_task;

    /* update max_task_depth, if needed */
    if (prof > p_stack->max_task_depth)
        p_stack->max_task_depth = prof;

    /* release the stack lock */
    V(p_stack->stack_lock);

    /* unblock waiting worker threads */
    sem_post_safe(&p_stack->sem_tasks);

}

/* take a task (blocking until there is a task in the stack) */
robinhood_task_t *GetTask_from_Stack(task_stack_t *p_stack)
{
    robinhood_task_t *p_task;
    int index;

    /* wait for a task */
    sem_wait_safe(&p_stack->sem_tasks);

    /* lock the stack */
    P(p_stack->stack_lock);

    /* The scan is a 'depth first' scan: directly go to the highest depth. */
    p_task = p_stack->tasks_at_depth[p_stack->max_task_depth];

    /* sanity check */
    if (p_task == NULL) {
        V(p_stack->stack_lock);
        DisplayLog(LVL_CRIT, FSSCAN_TAG, "UNEXPECTED ERROR: NO TASK FOUND");
        return NULL;
    }

    /* update the list for this depth */
    p_stack->tasks_at_depth[p_stack->max_task_depth] = p_task->next_task;

    /* if the list at current depth is empty, we need to
     * update max_task_depth.
     */
    if (p_task->next_task == NULL) {
        for (index = p_stack->max_task_depth; index >= 0; index--) {
            if (p_stack->tasks_at_depth[index] != NULL) {
                p_stack->max_task_depth = index;
                break;
            }
        }
        /* no item found */
        if (index < 0)
            p_stack->max_task_depth = 0;
    }

    /* unlock the stack */
    V(p_stack->stack_lock);

    /* returns pointer to the task */
    return p_task;

}
