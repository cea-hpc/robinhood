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
 * \file    $RCSfile: SemN.c,v $
 * \author  $Author: leibovic $ 
 * \date    $Date: 2008/02/15 10:37:38 $
 * \brief   Portable system tools.
 *
 * Implements system utilities (like semaphores)
 * so that they are POSIX and work on
 * most plateforms.
 *
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <errno.h>
#include "SemN.h"
#include <stdio.h>


int semaphore_init( semaphore_t * sem, int value )
{

    int            retval;

    if ( !sem )
        return EINVAL;

    if ( ( retval = pthread_mutex_init( &sem->mutex, NULL ) ) != 0 )
        return retval;

    if ( ( retval = pthread_cond_init( &sem->cond, NULL ) ) != 0 )
        return retval;

    sem->count = value;

    return 0;

}


int semaphore_destroy( semaphore_t * sem )
{

    if ( !sem )
        return EINVAL;

    pthread_cond_destroy( &sem->cond );
    pthread_mutex_destroy( &sem->mutex );

    return 0;

}

int semaphore_P( semaphore_t * sem )
{

    if ( !sem )
        return EINVAL;

    /* enters into the critical section */
    pthread_mutex_lock( &sem->mutex );

    sem->count--;
    /* If there are no more tokens : wait */
    if ( sem->count < 0 )
        pthread_cond_wait( &sem->cond, &sem->mutex );

    /* leaves the critical section */
    pthread_mutex_unlock( &sem->mutex );

    return 0;

}

int semaphore_V( semaphore_t * sem )
{

    /* enters into the critical section */
    pthread_mutex_lock( &sem->mutex );

    sem->count++;

    /* If a thread was waiting, gives it a token */
    if ( sem->count <= 0 )
        pthread_cond_signal( &sem->cond );

    /* leaves the critical section */
    pthread_mutex_unlock( &sem->mutex );

    return 0;
}
