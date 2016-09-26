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
 * Tools for managing tasks and their tree organisation.
 */

#ifndef _TREE_TACHES_H
#define _TREE_TACHES_H

#include "fs_scan_types.h"
#include "Memory.h"

/* Set the number of preallocated tasks for the pool.
 */
void SetNbPreallocTasks(size_t nb_prealloc);

/* Create and initialize a task */
robinhood_task_t *CreateTask(void);

/* Free the resources of a task */
int FreeTask(robinhood_task_t *p_task);

/* Add a child task (called by the parent) */
void AddChildTask(robinhood_task_t *p_parent_task,
                  robinhood_task_t *p_child_task);

/* Remove a child task from a parent task.
 * This is called by a child task, when it finishes.
 * Return TRUE if the parent task and all its children are completed,
 * FALSE otherwise.
 */
int RemoveChildTask(robinhood_task_t *p_parent_task,
                    robinhood_task_t *p_child_task);

/**
 * Tag a task as completed i.e. finished its own work
 * (but it can possibly still have sub-tasks running).
 * \retval true if all sub-tasks are terminated too.
 * \retval false otherwise.
 */
bool FlagTaskAsFinished(robinhood_task_t *p_task);

/* Test if a task is totally finished (i.e. the parent task
 * is finished, and all its children too )
 */
bool TestTaskTermination(robinhood_task_t *p_task);

void TasksMemInfo(mem_stat_t *p_mem_stat);

#endif
