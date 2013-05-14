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


/**
 * This is called to register the operation (with the ordering of pipeline) 
 * @return ID_OK if the entry can be processed.
 *         ID_MISSING if the ID is not set in p_op structure
 */
int id_hash_register( struct id_hash * id_hash, entry_proc_op_t * p_op );


/**
 * This removes the current reference to an id when the operation is removed.
 */
int id_hash_unregister( struct id_hash * id_hash, entry_proc_op_t * p_op );


/* display info about id hash management */
void id_hash_dump( struct id_hash * id_hash, const char * log_str );

/**
 * @TODO use better hash functions (see http://burtleburtle.net/bob/c/lookup3.c)
 */
static inline unsigned int hash_id( const entry_id_t * p_id, unsigned int modulo )
{
#ifdef FID_PK
	unsigned int   val = 1;
    char          *buffer;
    unsigned int   index;

	buffer = ( char * ) &( p_id->f_seq );

	for ( index = 0; index < sizeof( p_id->f_seq ); index++ )
		val = ( val << 5 ) - val + ( unsigned int ) ( buffer[index] );

	buffer = ( char * ) &( p_id->f_oid );

	for ( index = 0; index < sizeof( p_id->f_oid ); index++ )
		val = ( val << 5 ) - val + ( unsigned int ) ( buffer[index] );

	return val % modulo;

#else
	unsigned long long lval;
    /* polynom of prime numbers */
	lval = 1873 * p_id->fs_key + 3511 * p_id->inode + 10267;

	lval = lval % modulo;

	return lval;

#endif
}

/* return a slot pointer. */
static inline struct id_hash_slot * get_hash_slot( struct id_hash * id_hash,
												   const entry_id_t * p_id)
{
	return &id_hash->slot[hash_id( p_id, id_hash->hash_size )];
}

#endif
