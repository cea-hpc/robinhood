/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2013 CEA/DAM
 * Copyright (C) 2013 Cray Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
/**
 * Misc tools for managing entry processor pipeline
 */

#ifndef _ENTRY_PROC_HASH_H
#define _ENTRY_PROC_HASH_H

#include <glib.h>

/* A hash table slot. */
struct id_hash_slot {
    pthread_mutex_t      lock;
    struct rh_list_head  list;   /* list of ops */
    unsigned int         count;
};

/* A hash table. */
struct id_hash {
    unsigned int         hash_size;
    struct id_hash_slot  slot[];
};

/**
 * Creates a new hash table for operation entries.
 * @return the new hash table.
 */
struct id_hash *id_hash_init(const unsigned int hash_size, bool use_lock);

/* display stats about the hash */
void id_hash_stats(struct id_hash *id_hash, const char *log_str);

/* dump all values in the hash */
void id_hash_dump(struct id_hash *id_hash, bool parent);

/**
 * Murmur3 uint64 finalizer
 * from: https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
 */
static inline uint64_t __hash64(uint64_t k)
{
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdLLU;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53LLU;
    k ^= k >> 33;
    return k;
}

static inline uint64_t id_hash64(const entry_id_t *p_id)
{
#ifdef FID_PK
    return __hash64(p_id->f_seq ^ p_id->f_oid);
#else
    return __hash64(p_id->fs_key ^ p_id->inode);
#endif
}

static inline unsigned int hash_id(const entry_id_t *p_id, unsigned int modulo)
{
    return id_hash64(p_id) % modulo;
}

static inline unsigned int hash_name(const entry_id_t *p_id,
                                     const char *name, unsigned int modulo)
{
    return (id_hash64(p_id) ^ g_str_hash(name)) % modulo;
}

/* return a slot pointer. */
static inline struct id_hash_slot *get_hash_slot(struct id_hash *id_hash,
                                                 const entry_id_t *p_id)
{
    return &id_hash->slot[hash_id(p_id, id_hash->hash_size)];
}

/* return a slot pointer. */
static inline struct id_hash_slot *get_name_hash_slot(struct id_hash *name_hash,
                                                      const entry_id_t *
                                                      parent_id,
                                                      const char *name)
{
    return &name_hash->slot[hash_name(parent_id, name, name_hash->hash_size)];
}

#endif
