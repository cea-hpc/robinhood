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

#include "resource_monitor.h"
#include "uidgidcache.h"
#include "list_mgr.h"
#include "RobinhoodConfig.h"
#include "RobinhoodLogs.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>

/* @TODO remove this when they will be implemented */
int EntryProc_reporting( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    return 0;
}
int EntryProc_db_apply( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    return 0;
}
int EntryProc_db_flag_op( struct entry_proc_op_t *p_op, lmgr_t * lmgr )
{
    return 0;
}

int main( int argc, char **argv )
{
    int            rc;
    robinhood_config_t config;
    char           err_msg[2048];

    srand( time( NULL ) + getpid(  ) );

    if ( argc < 2 )
    {
        fprintf( stderr, "Usage : test_entry_proc <config_file>\n" );
        exit( 1 );
    }
    /* only parse config for mandatory and resource monitor module */
    if ( ReadRobinhoodConfig( MODULE_MASK_RES_MONITOR, argv[1], err_msg, &config ) )
    {
        fprintf( stderr,
                 "***** Error reading configuration: *****\n%s\n****************************************\n",
                 err_msg );
        exit( 1 );
    }

    /* set global configuration */
    global_config = config.global_config;

    /* set policies */
    purge_policy = config.purge_policy;


    InitializeLogs( "test_resource_monitor", &config.log_config );

    InitUidGid_Cache(  );

    rc = ListMgr_Init( &config.lmgr_config );
    if ( rc )
        exit( rc );

    rc = Start_ResourceMonitor( &config.res_mon_config );
    while ( 1 )
    {
        sleep( 10 );
        /*      Dump_ResourceMonitor_Stats(); */
    }

    return 0;
}
