/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009 CEA/DAM
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

#include "entry_processor.h"
#include "list_mgr.h"
#include "RobinhoodConfig.h"
#include "RobinhoodLogs.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>

#define NB_OP_ENQUEUE	100000
#define NB_WORKER 8

/*#define DEBUG 1*/

static inline unsigned int myrand( unsigned int range )
{
    return ( ( unsigned int ) rand(  ) ) % range;
}

/* processing functions */
#ifdef _LUSTRE_HSM
int EntryProc_parse_changelog( struct entry_proc_op_t *p_op, lmgr_t * p_lmgr )
#else
int EntryProc_get_attr( struct entry_proc_op_t *p_op, lmgr_t * p_lmgr )
#endif
{
    int            rc;
    entry_id_t     id;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];

    /* set id */
#ifdef _HAVE_FID
    id.f_seq = myrand( 1000 );
    id.f_oid = myrand( 1000 );
    id.f_ver = p_op->extra_info.log_record.record_id;
#else
    id.device = myrand( 1000 );
    id.inode = myrand( 1000 );
#endif
    EntryProcessor_SetEntryId( p_op, &id );

#ifdef DEBUG
    printf( "stage %s - record #%u - id=[%llu,%u] - thread %lx\n",
            stage_info->stage_name, ( unsigned int ) p_op->entry_id.f_ver,
            p_op->entry_id.f_seq, p_op->entry_id.f_oid, pthread_self(  ) );

    EntryProcessor_DumpCurrentStages(  );
#endif

    /* acknowledge */
    rc = EntryProcessor_Acknowledge( p_op, p_op->pipeline_stage + 1, FALSE );
    if ( rc )
        printf( "Error acknowledging stage %s\n", stage_info->stage_name );

    /* if processing is async, simulate a long operation */
    if ( stage_info->stage_flags & STAGE_FLAG_ASYNC )
    {
        /* do this for 1 entry out of 1000 */
        if ( myrand( 1000 ) == 0 )
        {
            usleep( 10000 );
            DisplayLog( LVL_DEBUG, "CheckExist", "End of async processing for stage %s",
                        stage_info->stage_name );
        }
    }

    return 0;
}

int EntryProc_check_exist( struct entry_proc_op_t *p_op, lmgr_t * p_lmgr )
{
    int            rc;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];
#ifdef DEBUG
    printf( "stage %s - record #%u - id=[%llu,%u] - thread %lx\n",
            stage_info->stage_name, ( unsigned int ) p_op->entry_id.f_ver,
            p_op->entry_id.f_seq, p_op->entry_id.f_oid, pthread_self(  ) );

    EntryProcessor_DumpCurrentStages(  );
#endif

    /* acknowledge */
    /* and randomly return 1 entry out of 100 */
    if ( myrand( 100 ) == 0 )
    {
        rc = EntryProcessor_Acknowledge( p_op, p_op->pipeline_stage + 1, TRUE );
        if ( rc )
            printf( "Error acknowledging stage %s\n", stage_info->stage_name );
        return rc;
    }
    else
    {
        rc = EntryProcessor_Acknowledge( p_op, p_op->pipeline_stage + 1, FALSE );
        if ( rc )
            printf( "Error acknowledging stage %s\n", stage_info->stage_name );
    }

    /* if processing is async, simulate a long operation */
    if ( stage_info->stage_flags & STAGE_FLAG_ASYNC )
    {
        /* do this for 1 entry out of 1000 */
        if ( myrand( 1000 ) == 0 )
        {
            usleep( 100000 );
            DisplayLog( LVL_DEBUG, "CheckExist", "End of async processing for stage %s",
                        stage_info->stage_name );
        }
    }

    return 0;
}

#ifdef _LUSTRE_HSM
int EntryProc_get_info( struct entry_proc_op_t *p_op, lmgr_t * p_lmgr )
#else
int EntryProc_get_extra_info( struct entry_proc_op_t *p_op, lmgr_t * p_lmgr )
#endif
{
    /* same function for all stages */
    return EntryProc_check_exist( p_op, p_lmgr );
}

#ifdef _TMP_FS_MGR
int EntryProc_infer_attrs( struct entry_proc_op_t *p_op, lmgr_t * p_lmgr )
{
    /* same function for all stages */
    return EntryProc_check_exist( p_op, p_lmgr );
}
#endif

int EntryProc_reporting( struct entry_proc_op_t *p_op, lmgr_t * p_lmgr )
{
    /* same function for all stages */
    return EntryProc_check_exist( p_op, p_lmgr );
}


int EntryProc_db_apply( struct entry_proc_op_t *p_op, lmgr_t * p_lmgr )
{
    int            rc;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];
#ifdef DEBUG
    printf( "stage %s - record #%u - id=[%llu,%u] - thread %lx\n",
            stage_info->stage_name, ( unsigned int ) p_op->entry_id.f_ver,
            p_op->entry_id.f_seq, p_op->entry_id.f_oid, pthread_self(  ) );
#endif

#ifdef _HAVE_FID
    if ( p_op->entry_id.f_ver % ( NB_OP_ENQUEUE / 10 ) == 0 )
        EntryProcessor_DumpCurrentStages(  );
#endif

    /* simulate slow DB query */
    usleep( 1000 );

#ifdef _HAVE_FID
    if ( ( unsigned int ) p_op->entry_id.f_ver == NB_OP_ENQUEUE - 1 )
        exit( 0 );
#endif

    /* acknowledge */
    rc = EntryProcessor_Acknowledge( p_op, 0, TRUE );
    if ( rc )
        printf( "Error acknowledging stage %s\n", stage_info->stage_name );

    return 0;
}

#ifdef _TMP_FS_MGR
int EntryProc_db_flag_op( struct entry_proc_op_t *p_op, lmgr_t * p_lmgr )
{

}
#endif


void          *enqueue_thread( void *arg )
{
    int            rc, i;
    entry_proc_op_t *new_op;

    for ( i = 0; i < NB_OP_ENQUEUE; i++ )
    {
        new_op = EntryProcessor_Get( );
        if ( !new_op )
        {
            printf( "Error in EntryProcessor_Get\n");
            return NULL;
        }

        /* initial stage */
        new_op->pipeline_stage = entry_proc_pipeline[0].stage_index;
#ifdef _LUSTRE_HSM
        new_op->extra_info.log_record.record_id = i;
        DisplayLog( LVL_FULL, "EnqueueThr", "Enqueuing record #%u", i );
#else
        sprintf( ATTR( &new_op->entry_attr, fullpath ), "/dir%u/file%d",
                 ( unsigned int ) time( NULL ), i );
        DisplayLog( LVL_FULL, "EnqueueThr", "Enqueuing file %s",
                    ATTR( &new_op->entry_attr, fullpath ) );
#endif


        EntryProcessor_Push( new_op );
    }
}



int main( int argc, char **argv )
{
    int            i, rc;
    pthread_t      enqueuer;
    void          *dummy;
    pthread_attr_t attr;
    robinhood_config_t config;
    char           err_msg[2048];

    srand( time( NULL ) + getpid(  ) );

    if ( argc < 2 )
    {
        fprintf( stderr, "Usage : test_entry_proc <config_file>\n" );
        exit( 1 );
    }
    /* only parse config for mandatory module */
    if ( ReadRobinhoodConfig( MODULE_MASK_ENTRY_PROCESSOR, argv[1], err_msg, &config ) )
    {
        fprintf( stderr, "Error reading configuration:\n%s\n", err_msg );
        exit( 1 );
    }

    InitializeLogs( "test_entry_proc", &config.log_config );

    rc = ListMgr_Init( &config.lmgr_config );
    if ( rc )
        exit( rc );

    /* this starts work threads */
    if ( EntryProcessor_Init( &config.entry_proc_config ) )
    {
        printf( "init error" );
        return -1;
    }

    pthread_attr_init( &attr );
    pthread_attr_setstacksize( &attr, 32 * 4096 );
    pthread_attr_setscope( &attr, PTHREAD_SCOPE_SYSTEM );

    /* create enqueue thread */
    pthread_create( &enqueuer, &attr, enqueue_thread, NULL );

    while ( 1 )                 /* exit is done by db_apply thread */
    {
        sleep( 2 );
        EntryProcessor_DumpCurrentStages(  );
    }

    /* wait for enqueuer thread */
    pthread_join( enqueuer, &dummy );

    return 0;
}
