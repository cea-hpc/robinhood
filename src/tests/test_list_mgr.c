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

#include "list_mgr.h"
#include "RobinhoodConfig.h"
#include "RobinhoodLogs.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <sys/param.h>

static inline unsigned int myrand( unsigned int range )
{
    return ( ( unsigned int ) rand(  ) ) % range;
}

void random_attrset( attr_set_t * p_set )
{
    /* insert entry */
    ATTR_MASK_INIT( p_set );

#ifdef _TMP_FS_MGR

    p_set->attr_mask |=
        ATTR_MASK_fullpath | ATTR_MASK_depth | ATTR_MASK_owner | ATTR_MASK_gr_name |
        ATTR_MASK_size | ATTR_MASK_blocks | ATTR_MASK_blksize |
        ATTR_MASK_last_access | ATTR_MASK_last_mod;

    sprintf( ATTR( p_set, fullpath ), "/tutu/toto%u/titi%u", myrand( 10000 ), myrand( 10000 ) );
    ATTR( p_set, depth ) = myrand( 256 );
    sprintf( ATTR( p_set, owner ), "titi%u", myrand( 1000 ) );
    sprintf( ATTR( p_set, gr_name ), "titi%u", myrand( 1000 ) );
    ATTR( p_set, size ) = myrand( 1024 * 1024 );
    ATTR( p_set, blocks ) = ATTR( p_set, size ) / DEV_BSIZE;
    ATTR( p_set, blksize ) = DEV_BSIZE;
    ATTR( p_set, last_access ) = time( NULL ) - myrand( 80000 );
    ATTR( p_set, last_mod ) = time( NULL ) - myrand( 80000 );
    ATTR( p_set, penalty ) = myrand( 3600 );
#else
    p_set->attr_mask |=
        ATTR_MASK_fullpath | ATTR_MASK_owner | ATTR_MASK_gr_name |
        ATTR_MASK_creation_time | ATTR_MASK_size |
        ATTR_MASK_last_access | ATTR_MASK_last_mod | ATTR_MASK_status;

    sprintf( ATTR( p_set, fullpath ), "/tutu/toto%u/titi%u", myrand( 10000 ), myrand( 10000 ) );
    strcpy( ATTR( p_set, owner ), "root" );
    strcpy( ATTR( p_set, gr_name ), "gpocre" );
    ATTR( p_set, size ) = myrand( 1024 * 1024 );
    ATTR( p_set, last_access ) = time( NULL ) - myrand( 80000 );
    ATTR( p_set, last_mod ) = time( NULL ) - myrand( 80000 );
    ATTR( p_set, creation_time ) = time( NULL ) - 80000 - myrand( 80000 );
    ATTR( p_set, status ) = ( file_status_t ) myrand( 5 );

#endif

}


void random_stripe_units( stripe_items_t * p_stripe_it )
{
    int            i;

    for ( i = 0; i < p_stripe_it->count; i++ )
    {
        p_stripe_it->stripe_units[i] = myrand( 230 );
    }
}


#define test_rc( _rc ) do { if ( _rc ) { printf("call failed with error %d line %d\n", _rc, __LINE__ ); return(_rc); }} while(0)

int main( int argc, char **argv )
{

    lmgr_t         lmgr;
    int            rc;
    entry_id_t     id;
    attr_set_t     set;
    int            exist = 0;
    int            i;
    lmgr_filter_t  filter;
    filter_value_t fv;
    struct lmgr_iterator_t *it;
    lmgr_sort_type_t sorttype;
    time_t         t1, t2;
    char           err_msg[2048];
    robinhood_config_t conf;

    storage_unit_id_t strlist[] = { 1, 3, 5, 7 };
    stripe_items_t stritems = { 4, strlist };

    stripe_info_t  strinfo = { 1024 * 1024, 4, "da14" };

    srand( time( NULL ) + getpid(  ) );

    if ( argc < 2 )
    {
        fprintf( stderr, "Usage : test_list_mgr <config_file>\n" );
        exit( 1 );
    }
    /* only parse config for mandatory module */
    if ( ReadRobinhoodConfig( 0, argv[1], err_msg, &conf ) )
    {
        fprintf( stderr, "Error reading configuration:\n%s\n", err_msg );
        exit( 1 );
    }
    conf.log_config.debug_level = LVL_FULL;
    InitializeLogs( "test_list_mgr", &conf.log_config );

    rc = ListMgr_Init( &conf.lmgr_config );
    test_rc( rc );

    rc = ListMgr_InitAccess( &lmgr );
    test_rc( rc );


#ifdef FID_PK
    id.f_seq = 12345;
    id.f_oid = 231;
#else
    id.inode = 12345;
    id.device = 231;
    id.validator = time( NULL );
#endif
    rc = ListMgr_Exists( &lmgr, &id );
    if ( rc == 0 )
    {
        printf( "Entry does not exist\n" );
        exist = 0;
    }
    else if ( rc == 1 )
    {
        exist = 1;
        printf( "Entry exists\n" );
    }
    else
        test_rc( -rc );

/* insert entry */
    random_attrset( &set );
    ATTR_MASK_SET( &set, stripe_info );
    ATTR( &set, stripe_info ) = strinfo;

    ATTR_MASK_SET( &set, stripe_items );
    ATTR( &set, stripe_items ) = stritems;

    printf( "attr mask (before insert/update)=%#X\n", set.attr_mask );

    if ( !exist )
    {
        rc = ListMgr_Insert( &lmgr, &id, &set, FALSE );
        test_rc( rc );
    }
    else
    {
        rc = ListMgr_Update( &lmgr, &id, &set );
        test_rc( rc );
    }

    printf( "attr mask (after insert/update)=%#X\n", set.attr_mask );

    rc = ListMgr_Exists( &lmgr, &id );
    if ( rc == 0 )
        printf( "Entry does not exist\n" );
    else if ( rc == 1 )
        printf( "Entry exists\n" );
    else
        test_rc( -rc );

    memset( &set.attr_values, 0, sizeof( entry_info_t ) );

#ifdef ATTR_INDEX_penalized_access
    ATTR_MASK_SET( &set, penalized_access );
#endif

    printf( "attr mask (before)=%#X\n", set.attr_mask );

    rc = ListMgr_Get( &lmgr, &id, &set );
    test_rc( rc );

    printf( "attr mask (after)=%#X\nstripe: ", set.attr_mask );

    for ( i = 0; i < ATTR( &set, stripe_items ).count; i++ )
    {
        printf( "%u ", ATTR( &set, stripe_items ).stripe_units[i] );
    }
    printf( "\n" );

#ifdef ATTR_INDEX_penalized_access
    printf( "penalized_access=%u\n", ( unsigned int ) ATTR( &set, penalized_access ) );
#endif

    rc = ListMgr_CheckStripe( &lmgr, &id );
    printf( "check stripe = %d\n", rc );
    if ( rc )
        rc = ListMgr_SetStripe( &lmgr, &id, &strinfo, &stritems );
    printf( "set stripe = %d\n", rc );

    /* remove */
    rc = ListMgr_Remove( &lmgr, &id );
    test_rc( rc );

    /* reinsert again */
    rc = ListMgr_Insert( &lmgr, &id, &set, FALSE );
    test_rc( rc );

    /* soft remove */
    rc = ListMgr_SoftRemove( &lmgr, &id, NULL,
#ifdef _HSM_LITE
    NULL,
#endif
        time(NULL)+3600 );
    test_rc( rc );

    printf( "starting test in 3s...\n" );
    sleep( 3 );

    /* insert sevaral entries, to perform mass, mass updates & remove */
    t1 = time( NULL );
    for ( i = 1; i < 20000; i++ )
    {
        random_attrset( &set );
        ATTR_MASK_SET( &set, stripe_info );
        ATTR( &set, stripe_info ) = strinfo;
        ATTR_MASK_SET( &set, stripe_items );
        ATTR( &set, stripe_items ) = stritems;
        random_stripe_units( &ATTR( &set, stripe_items ) );
        sprintf( ATTR( &set, stripe_info ).pool_name, "da%u", myrand( 20 ) );
#ifdef FID_PK
        id.f_seq = i;
        id.f_oid = myrand( 1000000 );
#else
        id.inode = i;
        id.device = myrand( 525 );
        id.validator = time( NULL );
#endif

        if ( !ListMgr_Exists( &lmgr, &id ) )
        {
            rc = ListMgr_Insert( &lmgr, &id, &set, FALSE );
            test_rc( rc );
        }

        if ( i % 10000 == 0 )
        {
            t2 = time( NULL );
            printf( "%u entries inserted in %us (%u entries/s)\n", i,
                    ( unsigned int ) ( t2 - t1 ), ( unsigned int ) ( i / ( t2 - t1 ) ) );
        }
    }
    t2 = time( NULL );
    printf( "%u entries inserted in %us (%u entries/s)\n", i,
            ( unsigned int ) ( t2 - t1 ), ( unsigned int ) ( i / ( t2 - t1 ) ) );

    /* */
    db_create_trigger();


    exit(0);


    /* SELECT entries whose status is not STATUS_MODIFIED, mtime <= now - 20000, on OST 100 */

    lmgr_simple_filter_init( &filter );
#ifdef ATTR_INDEX_status
    fv.val_uint = ( int ) STATUS_SYNCHRO;
    lmgr_simple_filter_add( &filter, ATTR_INDEX_status, NOTEQUAL, fv, 0 );
#endif

    fv.val_uint = time( NULL ) - 40000;
    lmgr_simple_filter_add( &filter, ATTR_INDEX_last_mod, LESSTHAN, fv, 0 );

    fv.val_str = "da14";
    lmgr_simple_filter_add( &filter, ATTR_INDEX_stripe_info, EQUAL, fv, 0 );

    fv.val_uint = 100;
    lmgr_simple_filter_add( &filter, ATTR_INDEX_stripe_items, EQUAL, fv, 0 );

    sorttype.attr_index = ATTR_INDEX_last_access;
    sorttype.order = SORT_ASC;


    t1 = time( NULL );
    it = ListMgr_Iterator( &lmgr, &filter, &sorttype, NULL );
    t2 = time( NULL );

    lmgr_simple_filter_free( &filter );

    printf( "ListMgr_Iterator: %us, it=%p\n", ( unsigned int ) ( t2 - t1 ), it );

    i = 0;
#if defined(_LUSTRE_HSM) || defined(_HSM_LITE)
    set.attr_mask =
        ATTR_MASK_fullpath | ATTR_MASK_owner | ATTR_MASK_gr_name |
        ATTR_MASK_creation_time | ATTR_MASK_size | ATTR_MASK_stripe_info |
        ATTR_MASK_last_access | ATTR_MASK_last_mod | ATTR_MASK_status ;
#else
    set.attr_mask =
        ATTR_MASK_fullpath | ATTR_MASK_depth | ATTR_MASK_owner | ATTR_MASK_gr_name |
        ATTR_MASK_size | ATTR_MASK_blocks | ATTR_MASK_stripe_info |
        ATTR_MASK_last_access | ATTR_MASK_last_mod;
#endif
    if ( it )
    {

        while ( ( rc = ListMgr_GetNext( it, &id, &set ) ) == DB_SUCCESS )
        {
            i++;
#ifdef _HAVE_FID
            printf( "seq=%llu, last_access=%u, pool_name=%s\n", id.f_seq,
                    ( unsigned int ) ATTR( &set, last_access ),
                    ATTR( &set, stripe_info ).pool_name );
#else
            printf( "inode=%llu, last_access=%u, penalized_access=%u, pool_name=%s\n",
                    ( unsigned long long ) id.inode, ( unsigned int ) ATTR( &set, last_access ),
                    ( unsigned int ) ATTR( &set, penalized_access ),
                    ATTR( &set, stripe_info ).pool_name );
#endif
            if ( i % 1000 == 0 )
            {
                t2 = time( NULL );
                if ( t2 > t1 )
                    printf( "%u entries listed in %us (%u entries/s)\n",
                            i, ( unsigned int ) ( t2 - t1 ), ( unsigned int ) ( i / ( t2 - t1 ) ) );
            }
        }

        printf( "ListMgr_GetNext terminated after %u entries with status %d\n", i, rc );

        ListMgr_CloseIterator( it );
    }

    printf( "resuming test in 3s...\n" );
    sleep( 3 );

    /* soft remove of a given set of entries */
    lmgr_simple_filter_init( &filter );
    fv.val_uint = time( NULL ) - 40000;
    lmgr_simple_filter_add( &filter, ATTR_INDEX_last_mod, MORETHAN, fv, 0 );

    rc = ListMgr_MassSoftRemove( &lmgr, &filter, time(NULL)+3600 );
    test_rc( rc );

    lmgr_simple_filter_free( &filter );

    /* soft remove all */
    rc = ListMgr_MassSoftRemove( &lmgr, NULL, time(NULL)+3600 );
    test_rc( rc );

#if 0
    /* Test ListMgr_Report */
    {
#define FIELD_COUNT  5
        struct lmgr_report_t *report;
        report_field_descr_t descr[FIELD_COUNT];
        lmgr_iter_opt_t opt;
        opt.list_count_max = 50;

        /* user, type, count, sum_size, max_dircount */

        descr[0].attr_index = ATTR_INDEX_owner;
        descr[0].report_type = REPORT_GROUP_BY;
        descr[0].sort_flag = SORT_ASC;
        descr[0].filter = FALSE;

        descr[1].attr_index = ATTR_INDEX_type;
        descr[1].report_type = REPORT_GROUP_BY;
        descr[1].sort_flag = SORT_ASC;
        descr[1].filter = TRUE;
        descr[1].filter_compar = NOTEQUAL;
        descr[1].filter_value.val_str = "sock";

        descr[2].attr_index = 0;        /* not used */
        descr[2].report_type = REPORT_COUNT;
        descr[2].sort_flag = SORT_NONE;
        descr[2].filter = TRUE;
        descr[2].filter_compar = MORETHAN;
        descr[2].filter_value.val_uint = 10;

        descr[3].attr_index = ATTR_INDEX_size;
        descr[3].report_type = REPORT_SUM;
        descr[3].sort_flag = SORT_NONE;
        descr[3].filter = FALSE;

        descr[4].attr_index = ATTR_INDEX_dircount;
        descr[4].report_type = REPORT_MAX;
        descr[4].sort_flag = SORT_NONE;
        descr[4].filter = FALSE;


        lmgr_simple_filter_init( &filter );

        fv.val_str = "leibovi";
        lmgr_simple_filter_add( &filter, ATTR_INDEX_owner, EQUAL, fv );

        report = ListMgr_Report( &lmgr, descr, FIELD_COUNT, &filter, &opt );

        lmgr_simple_filter_free( &filter );

        if ( report )
        {
            unsigned int   count = 0;
            db_value_t     report_item[FIELD_COUNT];
            unsigned int   report_count = FIELD_COUNT;

            while ( ( rc =
                      ListMgr_GetNextReportItem( report, report_item,
                                                 &report_count ) ) == DB_SUCCESS )
            {
                for ( i = 0; i < report_count; i++ )
                {
                    switch ( report_item[i].type )
                    {
                    case DB_TEXT:
                        if ( report_item[i].value_u.val_str != NULL )
                            printf( "%20s", report_item[i].value_u.val_str );
                        else
                            printf( "%20s", " " );
                        break;
                    case DB_INT:
                        printf( "%20d", report_item[i].value_u.val_int );
                        break;
                    case DB_UINT:
                        printf( "%20u", report_item[i].value_u.val_uint );
                        break;
                    case DB_BIGINT:
                        printf( "%20lld", report_item[i].value_u.val_bigint );
                        break;
                    case DB_BIGUINT:
                        printf( "%20llu", report_item[i].value_u.val_biguint );
                        break;
                    case DB_BOOL:
                        printf( "%20s", report_item[i].value_u.val_bool ? "TRUE" : "FALSE" );
                        break;
                    default:
                        printf( "%20s", "?" );
                    }
                    if ( i != report_count - 1 )
                        printf( " | " );
                }
                printf( "\n" );

                report_count = FIELD_COUNT;
            }

            printf( "ListMgr_GetNextReportItem terminated after %u entries with status %d\n", count,
                    rc );

            ListMgr_CloseReport( report );
        }


    }

    exit( 0 );

    /* impact all */
    ATTR_MASK_INIT( &set );
    ATTR_MASK_SET( &set, last_mod );
    ATTR( &set, last_mod ) = time( NULL );

    t1 = time( NULL );
    rc = ListMgr_MassUpdate( &lmgr, NULL, &set );
    test_rc( rc );

    t2 = time( NULL );
    printf( "ListMgr_MassUpdate(update mtime for all entries): %us\n",
            ( unsigned int ) ( t2 - t1 ) );

    /* filter on stripe */
    lmgr_simple_filter_init( &filter );
    fv.val_uint = 3;
    lmgr_simple_filter_add( &filter, ATTR_INDEX_stripe_info, EQUAL, fv );

    ATTR_MASK_INIT( &set );
    ATTR_MASK_SET( &set, penalty );
    ATTR( &set, penalty ) = 3600;
    ATTR_MASK_SET( &set, uid );
    ATTR( &set, uid ) = 2345;

    t1 = time( NULL );
    rc = ListMgr_MassUpdate( &lmgr, &filter, &set );
    test_rc( rc );

    t2 = time( NULL );
    printf
        ( "ListMgr_MassUpdate (update 'penalty' and 'uid', with condition on OST): %us\n",
          ( unsigned int ) ( t2 - t1 ) );
    lmgr_simple_filter_free( &filter );


#endif

    ListMgr_CloseAccess( &lmgr );
    return 0;
}
