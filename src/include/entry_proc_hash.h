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

#include "entry_processor.h"

/* A hash table slot. */
struct id_hash_slot
{
    pthread_mutex_t lock;
    struct list_head list;      /* list of ops */
    unsigned int count;
};

/* A hash table. */
struct id_hash
{
    unsigned int hash_size;
	struct id_hash_slot slot[0];
};

/**
 * Creates a new hash table for operation entries.
 * @return the new hash table.
 */
struct id_hash * id_hash_init( const unsigned int hash_size, int use_lock );


/* display stats about the hash */
void id_hash_stats(struct id_hash *id_hash, const char *log_str);

/* dump all values in the hash */
void id_hash_dump(struct id_hash *id_hash, int parent);

/**
 * @TODO use better hash functions:
 *       - http://code.google.com/p/smhasher/wiki/MurmurHash3
 *    or - http://burtleburtle.net/bob/c/lookup3.c
 */
static inline uint64_t id_hash64(const entry_id_t * p_id)
{
#ifdef FID_PK
	uint64_t       val = 1;
    char          *buffer;
    unsigned int   index;

	buffer = (char *) &(p_id->f_seq);

	for (index = 0; index < sizeof(p_id->f_seq); index++)
		val = (val << 5) - val + (unsigned int)(buffer[index]);

	buffer = (char *) &(p_id->f_oid);

	for (index = 0; index < sizeof(p_id->f_oid); index++)
		val = (val << 5) - val + (unsigned int)(buffer[index]);

    return val;
#else
    /* polynom of prime numbers */
	return 1873 * p_id->fs_key + 3511 * p_id->inode + 10267;
#endif
}

static inline unsigned int hash_id(const entry_id_t * p_id, unsigned int modulo)
{
    return id_hash64(p_id) % modulo;
}

static inline unsigned int hash_name(const entry_id_t * p_id,
                                     const char *name, unsigned int modulo)
{
    uint64_t val = id_hash64(p_id);
    const char *c;

    for (c = name; *c != '\0'; c++)
        val = (val << 5) - val + (unsigned int)(*c);

    return val % modulo;
}

/* return a slot pointer. */
static inline struct id_hash_slot * get_hash_slot( struct id_hash * id_hash,
												   const entry_id_t * p_id)
{
	return &id_hash->slot[hash_id( p_id, id_hash->hash_size )];
}

/* return a slot pointer. */
static inline struct id_hash_slot *get_name_hash_slot(struct id_hash *name_hash,
                                                   const entry_id_t *parent_id,
                                                   const char *name)
{
    return &name_hash->slot[hash_name(parent_id, name, name_hash->hash_size)];
}

#endif
