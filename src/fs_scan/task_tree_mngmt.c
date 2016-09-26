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
 * Tools for managing the tasks and their tree.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "rbh_logs.h"
#include "Memory.h"
#include "task_tree_mngmt.h"
#include <string.h>
#include <stdio.h>

#define sP(_lock_)  pthread_spin_lock(&(_lock_))
#define sV(_lock_)  pthread_spin_unlock(&(_lock_))

#ifdef _ONE_SHOT
#define TIME_NOT_SET ((unsigned int)-1)
#endif

/* defaut pool size for preallocation tasks */
static size_t nb_tasks_prealloc = 256;
static pthread_mutex_t mutex_spool = PTHREAD_MUTEX_INITIALIZER;
static robinhood_task_t *tasks_pool = NULL;
static mem_stat_t stat_mem_tach = { 0, 0 };

/* Set chunk size for preallocation mechanism */
void SetNbPreallocTasks(size_t nb_prealloc)
{
    nb_tasks_prealloc = nb_prealloc;
}

void TasksMemInfo(mem_stat_t *p_mem_stat)
{
    *p_mem_stat = stat_mem_tach;
}

/* Allocate and initialize a size structure */
robinhood_task_t *CreateTask()
{
    robinhood_task_t *p_task;

    GET_PREALLOC(p_task, tasks_pool, nb_tasks_prealloc,
                 robinhood_task_t, next_task, mutex_spool, stat_mem_tach);

    if (p_task != NULL) {
        /* zero all fields */
        memset(p_task, 0, sizeof(robinhood_task_t));

        /* initialize spin lock */
        pthread_spin_init(&p_task->child_list_lock, 0);
    }

    return p_task;
}

/* Free task resources */
int FreeTask(robinhood_task_t *p_task)
{
    pthread_spin_destroy(&p_task->child_list_lock);

    /* put it back to the allocation pool */
    RELEASE_PREALLOC(p_task, tasks_pool, next_task, mutex_spool, stat_mem_tach);

    return 0;
}

/* Add a child task (must be called by parent task) */
void AddChildTask(robinhood_task_t *p_parent_task,
                  robinhood_task_t *p_child_task)
{
    /* set parent task for this sub-task */
    p_child_task->parent_task = p_parent_task;

    /* lock the sub-task list of parent */
    sP(p_parent_task->child_list_lock);

    /* add the sub-task as first list item */
    if (p_parent_task->child_list == NULL) {
        p_child_task->prev_child = NULL;
        p_child_task->next_child = NULL;
        p_parent_task->child_list = p_child_task;
    } else {
        p_child_task->prev_child = NULL;
        p_child_task->next_child = p_parent_task->child_list;
        p_parent_task->child_list->prev_child = p_child_task;
        p_parent_task->child_list = p_child_task;
    }
    sV(p_parent_task->child_list_lock);

}

/* Remove a child task from a parent task.
 * This is called by a child task, when it finishes.
 * Return TRUE if the parent task and all its children are completed,
 * FALSE otherwise.
 */
int RemoveChildTask(robinhood_task_t *p_parent_task,
                    robinhood_task_t *p_child_task)
{
    int done;

    /* take the lock on sub-task list */
    sP(p_parent_task->child_list_lock);

    /* remove child task from list */
    if (p_child_task->next_child != NULL)
        p_child_task->next_child->prev_child = p_child_task->prev_child;

    if (p_child_task->prev_child != NULL)
        p_child_task->prev_child->next_child = p_child_task->next_child;
    else
        /* it was the head item */
        p_parent_task->child_list = p_child_task->next_child;

    done = (p_parent_task->child_list == NULL)
        && (p_parent_task->task_finished);

    sV(p_parent_task->child_list_lock);

    /* this task is now orphan */
    p_child_task->parent_task = NULL;

    return done;

}

/* Tag a task as completed i.e. finished its own work
 * (but it can possibly still have sub-tasks running).
 * Return TRUE if all sub-tasks are terminated too,
 * FALSE otherwise.
 */
bool FlagTaskAsFinished(robinhood_task_t *p_task)
{
    bool done;

    /* take the lock */
    sP(p_task->child_list_lock);

    p_task->task_finished = true;

    done = (p_task->child_list == NULL);

    sV(p_task->child_list_lock);

    return done;
}

/* Test is a task is completely done
 * (its own work + its sub-tasks).
 */
bool TestTaskTermination(robinhood_task_t *p_task)
{
    bool done;

    /* take the lock */
    sP(p_task->child_list_lock);

    done = (p_task->task_finished && (p_task->child_list == NULL));

    sV(p_task->child_list_lock);

    return done;
}
