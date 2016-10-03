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
 * \file RW_Lock.h
 * \brief This file contains the defintions of the functions and types for the
 *        RW lock management.
 */

#ifndef _RW_LOCK_H
#define _RW_LOCK_H

#include <pthread.h>
/* My habit with mutex */
#ifndef P
#ifndef DEBUG
#define P(mutex) pthread_mutex_lock(&mutex)
#define V(mutex)  pthread_mutex_unlock(&mutex)
#else
#define P(mutex){ int rc ; \
        if((rc = pthread_mutex_lock(&mutex)) != 0) \
            printf("  --> Erreur P: %d %d\n", rc, errno) ;}
#define V(mutex){ int rc ; \
        if((rc = pthread_mutex_unlock(&mutex)) != 0) \
            printf("  --> Erreur V: %d %d\n", rc, errno) ;}
#endif
#endif

/* Type representing the lock itself */
typedef struct _RW_LOCK {
    unsigned int    nbr_active;
    unsigned int    nbr_waiting;
    unsigned int    nbw_active;
    unsigned int    nbw_waiting;
    pthread_mutex_t mutexProtect;
    pthread_cond_t  condWrite;
    pthread_cond_t  condRead;
    pthread_mutex_t mcond;
} rw_lock_t;

int rw_lock_init(rw_lock_t *plock);
int rw_lock_destroy(rw_lock_t *plock);
int P_w(rw_lock_t *plock);
int V_w(rw_lock_t *plock);
int P_r(rw_lock_t *plock);
int V_r(rw_lock_t *plock);
int rw_lock_downgrade(rw_lock_t *plock);

#endif /* _RW_LOCK */
