/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009, 2010 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */


#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

/* useless functions is prepared statements are disabled */
#ifndef _DISABLE_PREP_STMT

#include "listmgr_prep_stmt.h"
#include "Memory.h"
#include "database.h"
#include "RobinhoodLogs.h"

/**
 * Initilize an empty cache of prepared statements
 */
void init_prep_stmt_cache( lmgr_t * lmgr )
{
    int            i;

    for ( i = 0; i < PREP_STMT_HASH_SIZE; i++ )
        lmgr->prep_cache[i] = NULL;
}

/** hash a  <op_type, db_table, attr_mask> triplet */
static inline unsigned int hash_operation( db_op_type_t op, db_tables_t table, int attr_mask )
{
    unsigned int   val = 1;
    char          *buffer;
    unsigned int   index;

    buffer = ( char * ) &op;

    for ( index = 0; index < sizeof( op ); index++ )
        val = ( val << 5 ) - val + ( unsigned int ) ( buffer[index] );

    buffer = ( char * ) &table;

    for ( index = 0; index < sizeof( table ); index++ )
        val = ( val << 5 ) - val + ( unsigned int ) ( buffer[index] );

    buffer = ( char * ) &attr_mask;

    for ( index = 0; index < sizeof( attr_mask ); index++ )
        val = ( val << 5 ) - val + ( unsigned int ) ( buffer[index] );

    return val % PREP_STMT_HASH_SIZE;
}

/**
 * get a prepared statement from cache
 * @return NULL if not found
 */
prep_stmt_t prep_stmt_get( lmgr_t * lmgr, db_op_type_t op, db_tables_t table, int attr_mask )
{
    unsigned int   hash = hash_operation( op, table, attr_mask );

    stmt_cache_item_t *curr;

    for ( curr = lmgr->prep_cache[hash]; curr != NULL; curr = curr->p_next )
    {
        if ( ( attr_mask == curr->attr_mask ) && ( table == curr->db_table )
             && ( op == curr->op_type ) )
            return curr->stmt;
    }
    return NULL;
}

/**
 * insert a prepared statement to cache
 * @return a listmgr code on error
 */
int prep_stmt_insert( lmgr_t * lmgr, db_op_type_t op, db_tables_t table, int attr_mask,
                      prep_stmt_t stmt )
{
    unsigned int   hash = hash_operation( op, table, attr_mask );

    if ( stmt == NULL )
        return DB_INVALID_ARG;

    /* allocate a new slot */
    stmt_cache_item_t *new = MemAlloc( sizeof( stmt_cache_item_t ) );

    if ( new == NULL )
        return DB_NO_MEMORY;

    new->attr_mask = attr_mask;
    new->db_table = table;
    new->op_type = op;
    new->stmt = stmt;

    new->p_next = lmgr->prep_cache[hash];
    lmgr->prep_cache[hash] = new;

    return DB_SUCCESS;
}


prep_stmt_t prep_stmt_build_or_get( lmgr_t * lmgr, db_op_type_t op, db_tables_t table,
                                    int attr_mask, const char *query )
{
    prep_stmt_t    stmt = prep_stmt_get( lmgr, op, table, attr_mask );

    /* if this prepared statement does not exist, create it and put it to stmt cache */
    if ( stmt == NULL )
    {
#ifdef _DEBUG_DB
        DisplayLog( LVL_FULL, LISTMGR_TAG, "Prepared statement cache miss for request: %s", query );
#endif
        stmt = db_create_prepared( &lmgr->conn, query );
        if ( !stmt )
            return NULL;

        if ( prep_stmt_insert( lmgr, op, table, attr_mask, stmt ) )
        {
            db_destroy_prepared( stmt );
            return NULL;
        }
    }
    return stmt;

}



void dump_prep_stmt_stats( unsigned int index, lmgr_t * pmgr )
{
    int            i;
    stmt_cache_item_t *curr;
    unsigned int   min, max, select, update, delete, insert, count;
    double         avg;

    min = max = select = update = delete = insert = count = 0;

    /* No lock is needed because prepared statement are not removed */

    for ( i = 0; i < PREP_STMT_HASH_SIZE; i++ )
    {
        unsigned int   curr_nb = 0;

        for ( curr = pmgr->prep_cache[i]; curr != NULL; curr = curr->p_next )
        {
            count++;
            curr_nb++;

            if ( curr->op_type == OP_SELECT )
                select++;
            else if ( curr->op_type == OP_UPDATE )
                update++;
            else if ( curr->op_type == OP_DELETE )
                delete++;
            else if ( curr->op_type == OP_INSERT )
                insert++;
        }
        if ( i == 0 )
            min = max = curr_nb;

        if ( curr_nb < min )
            min = curr_nb;
        if ( curr_nb > max )
            max = curr_nb;
    }

    avg = ( double ) count / ( 0.0 + PREP_STMT_HASH_SIZE );
    DisplayLog( LVL_DEBUG, "STATS",
                "Thread #%u: Statements: %u (select: %u, updt: %u, del: %u, ins: %u). Hash min: %u, max: %u, avg: %.1f",
                index, count, select, update, delete, insert, min, max, avg );
}



void destroy_statements( lmgr_t * lmgr )
{
    stmt_cache_item_t *curr;
    stmt_cache_item_t *next = NULL;
    int            i;

    for ( i = 0; i < PREP_STMT_HASH_SIZE; i++ )
    {
        for ( curr = lmgr->prep_cache[i]; curr != NULL; curr = next )
        {
            /* remember next */
            next = curr->p_next;

            db_destroy_prepared( curr->stmt );
            MemFree( curr );
        }
        /* reset the slot */
        lmgr->prep_cache[i] = NULL;
    }
}

#endif
