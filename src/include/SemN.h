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
 *
 * \file    SemN.h
 * \brief   Portable system tools.
 *
 * Definies system utilities (like semaphores)
 * so that they are POSIX and work on
 * most plateforms.
 *
 */

#ifndef _SEMN_H
#define _SEMN_H

#include "config.h"
#include <pthread.h>
/*#include <signal.h>*/

/*
 *  Synchronisation features
 */


typedef struct semaphore
{

    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int            count;

} semaphore_t;

/** Initializes a semaphore. */
int            semaphore_init( semaphore_t * sem, int value );

/** Destroys a semaphore. */
int            semaphore_destroy( semaphore_t * sem );

/** Takes a token. */
int            semaphore_P( semaphore_t * sem );

/** Release a token. */
int            semaphore_V( semaphore_t * sem );


#endif
