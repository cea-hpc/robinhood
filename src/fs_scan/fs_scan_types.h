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
#ifndef _FSSCAN_TYPES_H
#define _FSSCAN_TYPES_H

#include "rbh_const.h"
#include "list_mgr.h"

#include <sys/types.h>
#include <semaphore.h>
#include <pthread.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <stdbool.h>

/* a scanning task */

typedef struct robinhood_task__ {
    /* absolute path of the directory to be read */
    char            path[RBH_PATH_MAX];

    /* the relative depth of the directory to be read */
    unsigned int    depth;

    /* id of this directory */
    entry_id_t      dir_id;

    /* metadatas of this directory */
    struct stat     dir_md;

    /* parent task */
    struct robinhood_task__ *parent_task;

    /* lock for protecting the child list
     * and the task_finished boolean.
     */
    pthread_spinlock_t       child_list_lock;

    /* list of child tasks running */

    struct robinhood_task__ *child_list;

    /* this boolean indicates if the task is finished
     * (not including child tasks)
     */
    bool task_finished;

    /* these pointers are used for chaining a (child) task
     * into its parent list of childs.
     */
    struct robinhood_task__ *prev_child;
    struct robinhood_task__ *next_child;

/* This pointer is used in 2 ways, depending
   * on the structure status :
   * - for chaining tasks in the scheduler (in a task_stack_t)
   * - for chaining free structs in the pool manager
   */
    struct robinhood_task__ *next_task;

} robinhood_task_t;

/* We define a maximum value for ordering tasks into the stack,
 * but we however handle cases when it's over.
 * (we will consider that all the tasks over this limit
 * have the same prority)
 */
#define MAX_TASK_DEPTH  255

/* A stack of tasks ordered by depth,
 * handled by 'task_stack_mngmt' routines.
 */
typedef struct tasks_stack__ {
    pthread_mutex_t     stack_lock; /* lock on the stack */
    sem_t               sem_tasks;  /* token for available tasks */

    /* Indicates the depth for the first task available */
    unsigned int        max_task_depth;

    /* list of tasks, ordered by depth */
    robinhood_task_t   *tasks_at_depth[MAX_TASK_DEPTH + 1];

} task_stack_t;

#endif
