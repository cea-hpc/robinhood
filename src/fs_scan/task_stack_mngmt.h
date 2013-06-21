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
 * Module for managing task stack.
 *
 */


#ifndef _TASK_STACK_MNGMT_H
#define _TASK_STACK_MNGMT_H


#include "fs_scan_types.h"

/* initialize a task stack */
int            InitTaskStack( task_stack_t * p_stack );

/* insert a task in the stack */
void           InsertTask_to_Stack( task_stack_t * p_stack, robinhood_task_t * p_task );

/* take a task in the stack (block until there is a task available) */
robinhood_task_t *GetTask_from_Stack( task_stack_t * p_stack );


#endif
