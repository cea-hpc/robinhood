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

#include "RobinhoodConfig.h"
#include "RobinhoodLogs.h"
#include "entry_processor.h"
#include "info_collector.h"
#include "uidgidcache.h"
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
int EntryProc_parse_changelog( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;
    entry_id_t     id;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];

    /* set id */
    id.f_seq = myrand( 1000 );
    id.f_oid = myrand( 1000 );
    id.f_ver = p_op->extra_info.log_record.record_id;
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
        if ( myrand( 10000 ) == 0 )
        {
            usleep( 100 );
            /*printf( "End of async processing for stage %s\n",
               stage_info->stage_name ); */
        }
    }

    return 0;
}
#endif

#ifdef _LUSTRE_HSM
int EntryProc_get_info( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];
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
            usleep( 100 );
            /*printf( "End of async processing for stage %s\n",
               stage_info->stage_name ); */
        }
    }

    return 0;
}
#endif


int EntryProc_reporting( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;
    const pipeline_stage_t *stage_info = &entry_proc_pipeline[p_op->pipeline_stage];
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
            usleep( 100 );
            /*printf( "End of async processing for stage %s\n",
               stage_info->stage_name ); */
        }
    }

    return 0;

}


int EntryProc_db_apply( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
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

    /* insert to DB */
    switch ( p_op->db_op_type )
    {
    case OP_TYPE_INSERT:
        rc = ListMgr_Insert( lmgr, &p_op->entry_id, &p_op->entry_attr );
        break;
    case OP_TYPE_UPDATE:
        rc = ListMgr_Update( lmgr, &p_op->entry_id, &p_op->entry_attr );
        break;
    default:
        printf( "unhandled\n" );
        rc = -1;
    }
    if ( rc )
        printf( "ERROR: ListMgr operation returned %d\n", rc );


#ifdef _HAVE_FID
    if ( ( unsigned int ) p_op->entry_id.f_ver == NB_OP_ENQUEUE - 1 )
        exit( 0 );
#endif

    if ( p_op->callback_func )
        p_op->callback_func( lmgr, p_op, p_op->callback_param );

    /* acknowledge */
    rc = EntryProcessor_Acknowledge( p_op, 0, TRUE );
    if ( rc )
        printf( "Error acknowledging stage %s\n", stage_info->stage_name );


    return 0;
}

#ifdef _TMP_FS_MGR
int EntryProc_db_flag_op( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    int            rc;

    /* insert to DB */
    switch ( p_op->db_op_type )
    {
        attr_set_t     attr_chg;
        lmgr_filter_t  filter;
        filter_value_t val;

    case OP_TYPE_UPDATE:
        /* set previous_scan flag */
        ATTR_MASK_INIT( &attr_chg );
        ATTR_MASK_SET( &attr_chg, previous_scan );
        ATTR( &attr_chg, previous_scan ) = TRUE;

        rc = ListMgr_MassUpdate( lmgr, NULL, &attr_chg );
        break;
    case OP_TYPE_REMOVE:
        val.val_bool = TRUE;
        lmgr_simple_filter_init( &filter );
        lmgr_simple_filter_add( &filter, ATTR_INDEX_previous_scan, EQUAL, val );

        /* remove entries with previous_scan flag */
        rc = ListMgr_MassRemove( lmgr, &filter );

        lmgr_simple_filter_free( &filter );
        break;
    default:
        printf( "unhandled\n" );
        rc = -1;
    }
    if ( rc )
        printf( "ERROR: ListMgr mass operation returned %d\n", rc );

    if ( p_op->callback_func )
        p_op->callback_func( lmgr, p_op, p_op->callback_param );

    rc = EntryProcessor_Acknowledge( p_op, 0, TRUE );

    return rc;
}
#endif



int main( int argc, char **argv )
{
    int            i;
    pthread_t      worker[NB_WORKER];
    void          *dummy;
    pthread_attr_t attr;
    robinhood_config_t config;
    char           err_msg[2048];

    srand( time( NULL ) + getpid(  ) );

    if ( argc < 2 )
    {
        fprintf( stderr, "Usage : test_fs_scan <config_file>\n" );
        exit( 1 );
    }

    /* Initilize uidgid cache */
    if ( InitUidGid_Cache(  ) )
    {
        fprintf( stderr, "Error initializing uid/gid cache\n" );
        exit( 1 );
    }

    /* only parse config for mandatory module */
    if ( ReadRobinhoodConfig
         ( MODULE_MASK_ENTRY_PROCESSOR | MODULE_MASK_INFO_COLLECTOR, argv[1], err_msg, &config ) )
    {
        fprintf( stderr, "Error reading configuration:\n%s\n", err_msg );
        exit( 1 );
    }



    /* set global configuration */
    global_config = config.global_config;

    /* set policies */
    purge_policy = config.purge_policy;


    InitializeLogs( "test_fs_scan", &config.log_config );

    /* Initialise List Mgr */
    if ( ListMgr_Init( &config.lmgr_config ) )
    {
        printf( "List Mgr init error" );
        return -1;
    }

    /* Initialise Pipeline */
    if ( EntryProcessor_Init( &config.entry_proc_config ) )
    {
        printf( "Pipeline init error" );
        return -1;
    }

    /* Start info collectors */
    if ( Start_Info_Collectors( &config.info_collect_config ) )
    {
        printf( "Info collector init error" );
        return -1;
    }


    pthread_attr_init( &attr );
    pthread_attr_setstacksize( &attr, 32 * 4096 );
    pthread_attr_setscope( &attr, PTHREAD_SCOPE_SYSTEM );

    while ( 1 )                 /* exit is done by db_apply thread */
    {
        sleep( 30 );
        EntryProcessor_DumpCurrentStages(  );
        Dump_Info_Collectors_Stats(  );
    }

    for ( i = 0; i < NB_WORKER; i++ )
        pthread_join( worker[i], &dummy );
    return 0;
}
