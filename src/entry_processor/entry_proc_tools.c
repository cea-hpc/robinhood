/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2008, 2009, 2010 CEA/DAM
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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "entry_proc_tools.h"
#include "Memory.h"
#include "RobinhoodLogs.h"
#include "RobinhoodConfig.h"
#include "RobinhoodMisc.h"
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>

/* configuration for this module */
entry_proc_config_t entry_proc_conf;
int                 pipeline_flags = 0;

#define ID_HASH_SIZE 7919

typedef struct id_constraint_item__
{
    /* operation associated to this id */
    entry_proc_op_t *op_ptr;

    /* for chained list */
    struct id_constraint_item__ *p_next;

} id_constraint_item_t;

typedef struct id_constraint_slot__
{
    pthread_mutex_t lock;
    id_constraint_item_t *id_list_first;
    id_constraint_item_t *id_list_last;
    unsigned int   count;
} id_constraint_slot_t;

/* hash table for storing references to ids */
static id_constraint_slot_t id_hash[ID_HASH_SIZE];

static inline unsigned int hash_id( entry_id_t * p_id, unsigned int modulo )
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
    lval = 1873 * p_id->device + 3511 * p_id->inode + 10267;

    lval = lval % modulo;

    return lval;

#endif
}


/** initialize id constraint manager */
int id_constraint_init(  )
{
    unsigned int   i;
    for ( i = 0; i < ID_HASH_SIZE; i++ )
    {
        pthread_mutex_init( &id_hash[i].lock, NULL );
        id_hash[i].id_list_first = NULL;
        id_hash[i].id_list_last = NULL;
        id_hash[i].count = 0;
    }
    return 0;
}

/**
 * This is called to register the operation (with the ordering of pipeline)
 * @return ID_OK if the entry can be processed.
 *         ID_MISSING if the ID is not set in p_op structure
 *         ID_ALREADY if the op_structure has already been registered
 */
int id_constraint_register( entry_proc_op_t * p_op )
{
    unsigned int   hash_index;
    id_constraint_item_t *p_new;

    if ( !p_op->entry_id_is_set )
        return ID_MISSING;

    /* compute id hash value */
    hash_index = hash_id( &p_op->entry_id, ID_HASH_SIZE );

    P( id_hash[hash_index].lock );

    /* no constraint violation detected, register the entry */
    p_new = ( id_constraint_item_t * ) MemAlloc( sizeof( id_constraint_item_t ) );

    p_new->op_ptr = p_op;

    /* always insert in queue */
    p_new->p_next = NULL;

    if ( id_hash[hash_index].id_list_last )
        id_hash[hash_index].id_list_last->p_next = p_new;
    else
        id_hash[hash_index].id_list_first = p_new;

    id_hash[hash_index].id_list_last = p_new;
    id_hash[hash_index].count++;

    p_op->id_is_referenced = TRUE;

    V( id_hash[hash_index].lock );
    return ID_OK;

}


/**
 * Get the first operation for a given id.
 * @return an operation to be processed when it is possible.
 *         NULL else. 
 *        
 */
entry_proc_op_t *id_constraint_get_first_op( entry_id_t * p_id )
{
    unsigned int   hash_index;
    id_constraint_item_t *p_curr;
    entry_proc_op_t *p_op = NULL;

    /* compute id hash value */
    hash_index = hash_id( p_id, ID_HASH_SIZE );

    P( id_hash[hash_index].lock );

    for ( p_curr = id_hash[hash_index].id_list_first; p_curr != NULL; p_curr = p_curr->p_next )
    {
        if ( entry_id_equal( p_id, &p_curr->op_ptr->entry_id ) )
        {
            p_op = p_curr->op_ptr;
            break;
        }
    }
#ifdef _DEBUG_ID_CONSTRAINT
    if ( p_op )
        DisplayLog( LVL_FULL, ENTRYPROC_TAG,
                    "first op on id [%llu, %u] is record %u at stage %u (list %u)",
                    p_op->entry_id.f_seq, p_op->entry_id.f_oid,
                    p_op->entry_id.f_ver, p_op->pipeline_stage, hash_index );
    else
    {

        DisplayLog( LVL_FULL, ENTRYPROC_TAG,
                    "no registered operation on id [%llu, %u]?", p_id->f_seq, p_id->f_oid );
        DisplayLog( LVL_FULL, ENTRYPROC_TAG, "etat de la file %u:", hash_index );
        for ( p_curr = id_hash[hash_index].id_list_first; p_curr != NULL; p_curr = p_curr->p_next )
            DisplayLog( LVL_FULL, ENTRYPROC_TAG, "record #%u id [%llu, %u]",
                        p_curr->op_ptr->entry_id.f_ver,
                        p_curr->op_ptr->entry_id.f_seq, p_curr->op_ptr->entry_id.f_oid );
    }
#endif
    V( id_hash[hash_index].lock );
    return p_op;

}


/**
 * This removes the current reference to an id when the operation is removed.
 */
int id_constraint_unregister( entry_proc_op_t * p_op )
{
    unsigned int   hash_index;
    id_constraint_item_t *p_curr;
    id_constraint_item_t *p_prev;

    if ( !p_op->entry_id_is_set )
        return ID_MISSING;

    if ( !p_op->id_is_referenced )
        return ID_NOT_EXISTS;

    /* compute id hash value */
    hash_index = hash_id( &p_op->entry_id, ID_HASH_SIZE );

    /* check if the entry id exists and is a stage >= pipeline_stage */
    P( id_hash[hash_index].lock );

    for ( p_curr = id_hash[hash_index].id_list_first, p_prev = NULL;
          p_curr != NULL; p_prev = p_curr, p_curr = p_curr->p_next )
    {
        if ( p_curr->op_ptr == p_op )
        {
            /* found */
            if ( p_prev == NULL )
                id_hash[hash_index].id_list_first = p_curr->p_next;
            else
                p_prev->p_next = p_curr->p_next;

            /* was it the last ? */
            if ( id_hash[hash_index].id_list_last == p_curr )
                id_hash[hash_index].id_list_last = p_prev;

            p_curr->op_ptr->id_is_referenced = FALSE;

            id_hash[hash_index].count--;

            V( id_hash[hash_index].lock );

            /* free the slot */
            MemFree( p_curr );

            return ID_OK;
        }
    }

    V( id_hash[hash_index].lock );
#ifdef _HAVE_FID
    DisplayLog( LVL_MAJOR, ENTRYPROC_TAG,
                "id_constraint_unregister: op not found (list %u): id [%llu, %u] record %u",
                hash_index, p_op->entry_id.f_seq, p_op->entry_id.f_oid, p_op->entry_id.f_ver );
#else
    DisplayLog( LVL_MAJOR, ENTRYPROC_TAG,
                "id_constraint_unregister: op not found (list %u): id [dev %llu, ino %llu]",
                hash_index, ( unsigned long long ) p_op->entry_id.device,
                ( unsigned long long ) p_op->entry_id.inode );
#endif
    return ID_NOT_EXISTS;

}


void id_constraint_dump(  )
{
    unsigned int   i, total, min, max;
    double         avg;

    total = 0;
    min = id_hash[0].count;
    max = id_hash[0].count;

    for ( i = 0; i < ID_HASH_SIZE; i++ )
    {
        total += id_hash[i].count;

        if ( id_hash[i].count < min )
            min = id_hash[i].count;
        if ( id_hash[i].count > max )
            max = id_hash[i].count;
    }

    avg = ( double ) total / ( 0.0 + ID_HASH_SIZE );
    DisplayLog( LVL_MAJOR, "STATS",
                "Id constraints count: %u (Hash list min: %u, max: %u, avg: %.1f)", total, min, max,
                avg );

#ifdef _DEBUG_HASH
    /* more than 50% of difference between hash lists ! Dump all values. */
    if ( ( max - min ) > ( ( max + 1 ) / 2 ) )
    {
        unsigned int   nb_min = 0;
        unsigned int   nb_max = 0;

        for ( i = 0; i < ID_HASH_SIZE; i++ )
        {
            if ( id_hash[i].count == min )
                nb_min++;
            else if ( id_hash[i].count == max )
                nb_max++;
        }
        DisplayLog( LVL_MAJOR, "DebugHash", "nb slots with min/max count: %u/%u (total=%u)", nb_min,
                    nb_max, ID_HASH_SIZE );
    }
#endif

}

/* ------------ Config management functions --------------- */

#define ENTRYPROC_CONFIG_BLOCK  "EntryProcessor"
#define ALERT_BLOCK "Alert"

int SetDefault_EntryProc_Config( void *module_config, char *msg_out )
{
    entry_proc_config_t *conf = ( entry_proc_config_t * ) module_config;
    msg_out[0] = '\0';

    conf->nb_thread = 8;
    conf->max_pending_operations = 10000;

    conf->alert_list = NULL;
    conf->alert_count = 0;
    conf->alert_attr_mask = 0;

    return 0;
}

int Write_EntryProc_ConfigDefault( FILE * output )
{
    print_begin_block( output, 0, ENTRYPROC_CONFIG_BLOCK, NULL );
    print_line( output, 1, "nb_threads             :  8" );
    print_line( output, 1, "max_pending_operations :  10000" );
    print_line( output, 1, "alert                  :  NONE" );
    print_end_block( output, 0 );
    return 0;
}

#define critical_err_check(_ptr_, _blkname_) do { if (!_ptr_) {\
                                        sprintf( msg_out, "Internal error reading %s block in config file", _blkname_); \
                                        return EFAULT; \
                                    }\
                                } while (0)


int Read_EntryProc_Config( config_file_t config, void *module_config,
                           char *msg_out, int for_reload )
{
    int            rc, blc_index, i;
    int            tmpval;
    entry_proc_config_t *conf = ( entry_proc_config_t * ) module_config;

    char           pipeline_names[PIPELINE_STAGE_COUNT][256];
    char          *entry_proc_allowed[PIPELINE_STAGE_COUNT + 4];

    entry_proc_allowed[0] = "nb_threads";
    entry_proc_allowed[1] = "max_pending_operations";
    entry_proc_allowed[2] = ALERT_BLOCK;

    entry_proc_allowed[PIPELINE_STAGE_COUNT + 3] = NULL;        /* PIPELINE_STAGE_COUNT+3 = last slot */

    /* get EntryProcessor block */

    config_item_t  entryproc_block = rh_config_FindItemByName( config, ENTRYPROC_CONFIG_BLOCK );

    if ( entryproc_block == NULL )
    {
        strcpy( msg_out, "Missing configuration block '" ENTRYPROC_CONFIG_BLOCK "'" );
        /* No error because no parameter is mandatory  */
        return 0;
    }

    if ( rh_config_ItemType( entryproc_block ) != CONFIG_ITEM_BLOCK )
    {
        strcpy( msg_out, "A block is expected for '" ENTRYPROC_CONFIG_BLOCK "' item" );
        return EINVAL;
    }

    /* retrieve parameters */

    rc = GetIntParam( entryproc_block, ENTRYPROC_CONFIG_BLOCK, "nb_threads",
                      INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL,
                      ( int * ) &conf->nb_thread, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetIntParam( entryproc_block, ENTRYPROC_CONFIG_BLOCK, "max_pending_operations",
                      INT_PARAM_POSITIVE,
                      ( int * ) &conf->max_pending_operations, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    /* look for '<stage>_thread_max' parameters */
    for ( i = 0; i < PIPELINE_STAGE_COUNT; i++ )
    {
        char           varname[256];

        snprintf( varname, 256, "%s_threads_max", entry_proc_pipeline[i].stage_name );

        strncpy( pipeline_names[i], varname, 256 );
#ifdef HAVE_CHANGELOGS
        entry_proc_allowed[i + 4] = pipeline_names[i];
#else
        entry_proc_allowed[i + 3] = pipeline_names[i];
#endif

        rc = GetIntParam( entryproc_block, ENTRYPROC_CONFIG_BLOCK, varname,
                          INT_PARAM_POSITIVE, &tmpval, NULL, NULL, msg_out );

        if ( ( rc != 0 ) && ( rc != ENOENT ) )
            return rc;
        else if ( ( rc != ENOENT ) && ( tmpval > 0 ) )  /* 0: keep default */
        {
            if ( entry_proc_pipeline[i].stage_flags & STAGE_FLAG_MAX_THREADS )
                entry_proc_pipeline[i].max_thread_count =
                    MIN2( entry_proc_pipeline[i].max_thread_count, tmpval );
            else if ( entry_proc_pipeline[i].stage_flags & STAGE_FLAG_PARALLEL )
            {
                /* the stqge is no more parallel, it has a limited number of threads */
                entry_proc_pipeline[i].stage_flags &= ~STAGE_FLAG_PARALLEL;
                entry_proc_pipeline[i].stage_flags |= STAGE_FLAG_MAX_THREADS;
                entry_proc_pipeline[i].max_thread_count = tmpval;
            }
            else if ( ( entry_proc_pipeline[i].stage_flags & STAGE_FLAG_SEQUENTIAL )
                      && ( tmpval != 1 ) )
            {
                sprintf( msg_out, "%s is sequential. Cannot use %u threads at this stage.",
                         entry_proc_pipeline[i].stage_name, tmpval );
                return EINVAL;
            }
        }

    }

    /* Find and parse "Alert" blocks */
    for ( blc_index = 0; blc_index < rh_config_GetNbItems( entryproc_block ); blc_index++ )
    {
        char          *block_name;
        config_item_t  curr_item = rh_config_GetItemByIndex( entryproc_block, blc_index );
        critical_err_check( curr_item, ENTRYPROC_CONFIG_BLOCK );

        if ( rh_config_ItemType( curr_item ) != CONFIG_ITEM_BLOCK )
            continue;

        block_name = rh_config_GetBlockName( curr_item );
        critical_err_check( curr_item, ENTRYPROC_CONFIG_BLOCK );

        if ( !strcasecmp( block_name, ALERT_BLOCK ) )
        {
            char * alert_title = NULL;

            if ( conf->alert_count == 0 )
                conf->alert_list = ( alert_item_t * ) malloc( sizeof( alert_item_t ) );
            else
                conf->alert_list =
                    ( alert_item_t * ) realloc( conf->alert_list,
                                                ( conf->alert_count + 1 )
                                                * sizeof( alert_item_t ) );

            conf->alert_count++;

            alert_title = rh_config_GetBlockId( curr_item );
            if ( alert_title != NULL )
                strncpy( conf->alert_list[conf->alert_count - 1].title,
                         alert_title, ALERT_TITLE_MAX );
            else
                conf->alert_list[conf->alert_count - 1].title[0] = '\0';

            /* analyze boolean expression */
            rc = GetBoolExpr( curr_item, block_name,
                              &conf->alert_list[conf->alert_count - 1].boolexpr,
                              &conf->alert_list[conf->alert_count - 1].attr_mask, msg_out );

            if ( rc )
                return rc;

            conf->alert_attr_mask |= conf->alert_list[conf->alert_count - 1].attr_mask;
        }
    }                           /* Loop on subblocks */

    CheckUnknownParameters( entryproc_block, ENTRYPROC_CONFIG_BLOCK,
                            ( const char ** ) entry_proc_allowed );

    return 0;
}

/** check alert rules and update values */
static void update_alerts( alert_item_t * old_items, unsigned int old_count,
                           alert_item_t * new_items, unsigned int new_count,
                           const char *block_name )
{
    unsigned int   i;

    if ( old_count != new_count )
    {
        DisplayLog( LVL_MAJOR, "EntryProc_Config",
                    "Alert rules count changed in block '%s' but cannot be modified dynamically: alert update cancelled",
                    block_name );
        return;
    }

    /* compare alert boolean expression structure */
    for ( i = 0; i < new_count; i++ )
    {
        if ( ( old_items[i].attr_mask != new_items[i].attr_mask )
             || compare_boolexpr( &old_items[i].boolexpr, &new_items[i].boolexpr ) )
        {
            DisplayLog( LVL_MAJOR, "EntryProc_Config",
                        "Alert expression #%u changed in block '%s'. Only numerical values can be modified dynamically. Alert update cancelled",
                        i, block_name );
            return;
        }
    }

    /* if they are all the same, update/check their values */

    for ( i = 0; i < new_count; i++ )
    {
        if ( update_boolexpr( &old_items[i].boolexpr, &new_items[i].boolexpr ) )
        {
            char           criteriastr[2048];
            BoolExpr2str( &old_items[i].boolexpr, criteriastr, 2048 );
            DisplayLog( LVL_EVENT, "EntryProc_Config",
                        "Alert expression #%u in block '%s' has been updated and is now: %s", i,
                        block_name, criteriastr );
        }
    }

    /* XXX attr_mask is unchanged, since we keep the same expression structures */

}

static void free_alert( alert_item_t * p_items, unsigned int count )
{
    unsigned int   i;

    for ( i = 0; i < count; i++ )
        FreeBoolExpr( &p_items[i].boolexpr, FALSE );

    if ( ( count > 0 ) && ( p_items != NULL ) )
        free( p_items );
}


int Reload_EntryProc_Config( void *module_config )
{
    entry_proc_config_t *conf = ( entry_proc_config_t * ) module_config;

    if ( conf->nb_thread != entry_proc_conf.nb_thread )
        DisplayLog( LVL_MAJOR, "EntryProc_Config",
                    ENTRYPROC_CONFIG_BLOCK
                    "::nb_threads changed in config file, but cannot be modified dynamically" );

    if ( conf->max_pending_operations != entry_proc_conf.max_pending_operations )
        DisplayLog( LVL_MAJOR, "EntryProc_Config",
                    ENTRYPROC_CONFIG_BLOCK
                    "::max_pending_operations changed in config file, but cannot be modified dynamically" );

    /* Check alert rules  */
    update_alerts( entry_proc_conf.alert_list, entry_proc_conf.alert_count,
                   conf->alert_list, conf->alert_count, ENTRYPROC_CONFIG_BLOCK );


    free_alert( conf->alert_list, conf->alert_count );

    return 0;
}

int Write_EntryProc_ConfigTemplate( FILE * output )
{
    int            i;

    print_begin_block( output, 0, ENTRYPROC_CONFIG_BLOCK, NULL );

    print_line( output, 1, "# nbr of worker threads for processing pipeline tasks" );
    print_line( output, 1, "nb_threads = 8 ;" );
    fprintf( output, "\n" );
    print_line( output, 1, "# Max number of operations in the Entry Processor pipeline." );
    print_line( output, 1, "# If the number of pending operations exceeds this limit, " );
    print_line( output, 1, "# info collectors are suspended until this count decreases" );
#ifdef _SQLITE
    print_line( output, 1, "max_pending_operations = 1000 ;" );
#else
    print_line( output, 1, "max_pending_operations = 10000 ;" );
#endif
    fprintf( output, "\n" );

    print_line( output, 1,
                "# Optionnaly specify a maximum thread count for each stage of the pipeline:" );
    print_line( output, 1, "# <stagename>_threads_max = <n> (0: use default)" );
    for ( i = 0; i < PIPELINE_STAGE_COUNT; i++ )
    {
        if ( entry_proc_pipeline[i].stage_flags & STAGE_FLAG_PARALLEL )
            print_line( output, 1, "# %s_threads_max\t= 8 ;", entry_proc_pipeline[i].stage_name );
        else if ( entry_proc_pipeline[i].stage_flags & STAGE_FLAG_MAX_THREADS )
            print_line( output, 1, "%s_threads_max\t= %u ;", entry_proc_pipeline[i].stage_name,
                        entry_proc_pipeline[i].max_thread_count );
    }
    fprintf( output, "\n" );

#ifndef _LUSTRE_HSM
    print_line( output, 1, "# Raise alerts for directories with too many entries" );
    print_begin_block( output, 1, ALERT_BLOCK, "Too_many_entries_in_directory" );
    print_line( output, 2, "type == directory" );
    print_line( output, 2, "and" );
    print_line( output, 2, "dircount > 10000" );
    print_end_block( output, 1 );
    fprintf( output, "\n" );
#endif
    print_line( output, 1, "# Raise alerts for large files" );
    print_begin_block( output, 1, ALERT_BLOCK, "Large_file" );
#ifndef _LUSTRE_HSM
    print_line( output, 2, "type == file" );
    print_line( output, 2, "and" );
#endif
    print_line( output, 2, "size > 100GB" );
    print_end_block( output, 1 );


    print_end_block( output, 0 );
    return 0;
}
