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
 *  \file  Memory.h
 *  \brief Definition of memory management routines.
 */

#ifndef _MEMORY_H
#define _MEMORY_H

#include <stdlib.h>
#include <errno.h>

#ifdef _BUDDY_MALLOC

#include "BuddyMalloc.h"

#define MemAlloc(_a)       BuddyMallocExit(_a)
#define MemCalloc(_s1, _s2) BuddyCalloc(_s1, _s2)
#define MemRealloc(_p, _s)   BuddyRealloc((caddr_t)(_p), _s)
#define MemFree(_a)        BuddyFree((caddr_t) (_a))
#define MemErrno            BuddyErrno

#define GetPreferedPool(_n, _s)  BuddyPreferedPoolCount(_n, _s)

#else

#define MemAlloc(s)       malloc(s)
#define MemCalloc(_n, _s)   calloc((_n), (_s))
#define MemRealloc(p, s)  realloc(p, s)
#define MemFree(p)        free(p)
#define MemErrno            errno

#define GetPreferedPool(_n, _s)  (_n)

#endif

/** memory pool stats */
typedef struct mem_stat_t {
    unsigned int nb_prealloc;
    unsigned int nb_used;
} mem_stat_t;

#ifndef _DEBUG_MEMORY

/**
 *
 * STUFF_PREALLOC: Allocates a pool of pre-allocated entries.
 *
 * This macro Allocates a pool of pre-allocated entries. It calls
 * malloc to get the spool as an arry and then chains all the
 * entries together. Each entry is supposed to have a specific
 * 'next' field, a pointer to an object of its own type, to be
 * used as a pointer to the next entry in the pool.
 *
 * If BuddyMalloc is used, it is supposed to be already initialised.
 *
 * @param pool the preallocted pool that we want to init.
 * @param nb the number of entries to be allocated.
 * @param type the type of the entries to be allocated.
 * @param name_next the name of the field, in structure of type 'type' which pointer to the next entry.
 *
 * @return  nothing (this is a macro), but pool will be NULL if an error occures.
 *
 */

#define STUFF_PREALLOC(_pool, _nb, _type, name_next, memstats)            \
do {                                                                      \
  int _i = 0 ;                                                            \
  unsigned int _prefered = 0 ;                                            \
                                                                          \
  _prefered = GetPreferedPool(_nb, sizeof(_type));                        \
  _pool= NULL ;                                                           \
                                                                          \
  if ((_pool = (_type *)MemCalloc(_prefered, sizeof(_type))) != NULL) {   \
      memstats.nb_prealloc += _prefered ;                                 \
      for (_i = 0 ; _i < _prefered ; _i++) {                              \
          if (_i != _prefered - 1)                                        \
            _pool[_i].name_next = &(_pool[_i+1]) ;                        \
          else                                                            \
            _pool[_i].name_next = NULL ;                                  \
      }                                                                   \
  }                                                                       \
} while (0)

/**
 *
 * GET_PREALLOC: Gets an entry in a preallocated pool.
 *
 * This macro is used to get an entry from a pre-allocated pool. If the pool is
 * empty, the macro STUFF_PREALLOC will be called with the same last four
 * arguments to extend the pool. If nb is set to zero during this call,
 * STUFF_PREALLOC is not called and no entry is get from the pool that is empty.
 *
 * @param entry the entry we need.
 * @param pool the preallocted pool that we want to init.
 * @param nb the number of entries to be allocated.
 * @param type the type of the entries to be allocated.
 * @param name_next the name of the field, in structure of type 'type' which
 * pointer to the next entry.
 *
 * @return  nothing (this is a macro), but entry will be NULL if an error
 * occures.
 *
 */
#define GET_PREALLOC(entry, pool, nb, type, name_next, mutex, memstats) \
do {                                                                    \
  pthread_mutex_lock(&mutex);                                           \
                                                                        \
  if ((pool == NULL) && (nb != 0))                                      \
    STUFF_PREALLOC(pool, nb, type, name_next, memstats) ;               \
                                                                        \
  if (pool != NULL) {                                                   \
      memstats.nb_used += 1 ;                                           \
      entry = pool ;                                                    \
      pool = entry->name_next ;                                         \
  }                                                                     \
  else                                                                  \
   entry = NULL ;                                                       \
                                                                        \
  pthread_mutex_unlock(&mutex);                                         \
} while (0)

/**
 *
 * RELEASE_PREALLOC: Releases an entry and puts it back to the pool.
 *
 * When an entry is no used any more, this macro is used to put it
 * back to the pool, so that it could be reuse later. The released
 * entry is chained to the pool, through the 'name_next' field.
 *
 * @param entry the entry to be released.
 * @param pool the pool to which the entry belongs.
 * @param name_next the name of the field, in structure of type 'type' which pointer to the next entry.
 *
 * @return nothing (this is a macro).
 *
 */
#define RELEASE_PREALLOC(entry, pool, name_next, mutex , memstats)      \
do {                                                                    \
      pthread_mutex_lock(&mutex);                                       \
      entry->name_next = pool ;                                         \
      pool = entry ;                                                    \
      memstats.nb_used -= 1 ;                                           \
      pthread_mutex_unlock(&mutex);                                     \
} while (0)

#else

#define GET_PREALLOC(entry, pool, nb, type, name_next, mutex, memstats) \
        (entry = (type *)malloc(sizeof(type)))

#define RELEASE_PREALLOC(entry, pool, name_next, mutex , memstats)      \
        (free(entry))

#endif

#endif
