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

/**
 * Command for retrieving stats about filesystem.
 */

#define TAG "gen_lov_objid"

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "RobinhoodConfig.h"
#include "RobinhoodLogs.h"
#include "RobinhoodMisc.h"
#include <unistd.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include "../robinhood/cmd_helpers.h"
#include "../list_mgr/database.h"

#define OPT_STRING    "l:f:o:m:"

static const char *help_string =
    _B "Usage:" B_ " %s [-f <cfg_file>][-l <lvl_debug>][-o <output_file>][-m <margin>]\n"
    "\n"
    "Generate a lov_objid file for MDT according to max stripe object indexes.\n"
    "Increment each index by the safety "_U"margin"U_".\n";

static inline void display_help(char *bin_name)
{
    printf(help_string, bin_name);
}

#define MAX_OPT_LEN 1024

/**
 * Main daemon routine
 */
int main( int argc, char **argv )
{
    int            c = 0;
    char          *bin = basename( argv[0] );
    int            rc;
    char           err_msg[4096];
    robinhood_config_t config;
    int chgd = 0;

    /* options */
    char           config_file[MAX_OPT_LEN] = "";
    char           badcfg[RBH_PATH_MAX];
    int            force_log_level = FALSE;
    int            log_level = 0;
    int            margin = 0;
    char           output_file[MAX_OPT_LEN] = "/tmp/lov_objid";

    lmgr_t         lmgr;
    FILE         * out;

    /* parse command line options */
    while ((c = getopt(argc, argv, OPT_STRING)) != -1)
    {
        switch (c)
        {
            case 'l':
                force_log_level = TRUE;
                log_level = str2debuglevel(optarg);
                if (log_level == -1)
                {
                    fprintf( stderr,
                             "Unsupported log level '%s'. CRIT, MAJOR, EVENT, VERB, DEBUG or FULL expected.\n",
                             optarg );
                    exit(1);
                }
                break;
            case 'f':
                rh_strncpy(config_file, optarg, MAX_OPT_LEN);
                break;
            case 'o':
                rh_strncpy(output_file, optarg, MAX_OPT_LEN);
                break;
            case 'm':
                margin = str2int(optarg);
                if (margin < 0)
                {
                    fprintf( stderr,
                             "Invalid parameter '%s' for '-m' option: positive integer expected\n",
                             optarg );
                    exit(1);
                }
                break;
            case ':':
            case '?':
            default:
                display_help(bin);
                exit( 1 );
                break;
        }
    }

    /* get default config file, if not specified */
    if (SearchConfig(config_file, config_file, &chgd, badcfg, MAX_OPT_LEN) != 0)
    {
        fprintf(stderr, "No config file (or too many) found matching %s\n", badcfg);
        exit(2);
    }
    else if (chgd)
    {
        fprintf(stderr, "Using config file '%s'.\n", config_file );
    }

    /* only read ListMgr config */

    if ( ReadRobinhoodConfig( 0, config_file, err_msg, &config, FALSE ) )
    {
        fprintf( stderr, "Error reading configuration file '%s': %s\n", config_file, err_msg );
        exit( 1 );
    }
    process_config_file = config_file;

    /* set global configuration */
    global_config = config.global_config;

    if ( force_log_level )
        config.log_config.debug_level = log_level;
    else
        config.log_config.debug_level = LVL_MAJOR; /* no event message */

    /* set logging to stderr for this tool */
    strcpy( config.log_config.log_file, "stderr" );
    strcpy( config.log_config.report_file, "stderr" );
    strcpy( config.log_config.alert_file, "stderr" );

    /* Initialize logging */
    rc = InitializeLogs( bin, &config.log_config );
    if ( rc )
    {
        fprintf( stderr, "Error opening log files: rc=%d, errno=%d: %s\n",
                 rc, errno, strerror( errno ) );
        exit( rc );
    }

    /* Initialize list manager */
    rc = ListMgr_Init( &config.lmgr_config, TRUE );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, TAG, "Error %d initializing list manager", rc );
        exit( rc );
    }
    else
        DisplayLog( LVL_DEBUG, TAG, "ListManager successfully initialized" );

    if ( CheckLastFS(  ) != 0 )
        exit( 1 );

    /* Create database access */
    rc = ListMgr_InitAccess( &lmgr );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, TAG, "Error %d: cannot connect to database", rc );
        exit( rc );
    }

    out = fopen(output_file, "w");
    if (!out)
    {
        DisplayLog(LVL_CRIT, TAG, "Failed to open '%s' for writting: %s", output_file,
                   strerror(errno));
        return errno;
    }

    /* direct SQL request to retrieve the max object index from DB */
    result_handle_t res;
    /* FIXME max on the low weight 32bits of the 'objid' 64bits value */
    rc = db_exec_sql(&lmgr.conn, "SELECT ostidx, max(hex(cast(reverse(cast(details as binary(8))) as binary(4)))) "
                     "FROM "STRIPE_ITEMS_TABLE" GROUP BY ostidx ORDER BY ostidx", &res);
    if (rc)
        goto db_error;

    int index = -1;
    do
    {
        char *resstr[2];
        unsigned int ostidx;
        unsigned int objid;
        unsigned long long objid_long;
        resstr[0] = resstr[1] = NULL;

        rc = db_next_record( &lmgr.conn, &res, resstr, 2 );
        if (rc == DB_END_OF_LIST)
            break;
        else if (rc != DB_SUCCESS)
            goto db_error;

        index ++;

        if (resstr[0] == NULL || resstr[1] == NULL)
        {
            DisplayLog(LVL_MAJOR, TAG, "ERROR: got NULL record from DB at index %u", index);
            rc = EINVAL;
            goto out;
        }

        /* resstr[0] is ost_idx */
        if (sscanf(resstr[0], "%u", &ostidx) != 1)
        {
            DisplayLog(LVL_MAJOR, TAG, "ERROR: cannot parse OST index '%s' at index %u", resstr[0], index);
            rc = EINVAL;
            goto out;
        }
        else if (ostidx != index)
        {
            DisplayLog(LVL_MAJOR, TAG, "Warning: OST index %u not found in database, assuming current objid=1",
                       index);
            objid_long = 1 + margin;
            printf("ostidx=%u, max objid=%016LX\n", ostidx, objid_long);
            fwrite(&objid_long, sizeof(objid_long), 1, out);
            continue;
        }

        /* resstr[1] is objid (hexa) */
        if (sscanf(resstr[1], "%X", &objid) != 1)
        {
            DisplayLog(LVL_MAJOR, TAG, "ERROR: cannot parse objid '%s' at index %u", resstr[1], index);
            rc = EINVAL;
            goto out;
        }

        objid_long = objid + margin;
        printf("ostidx=%u, objid=%016LX\n", ostidx, objid_long);
        fwrite(&objid_long, sizeof(objid_long), 1, out);

    } while(rc == 0);

    fclose(out);
    ListMgr_CloseAccess( &lmgr );
    return 0;

db_error:
    DisplayLog( LVL_CRIT, TAG, "Database error %d\n", rc);
out:
    ListMgr_CloseAccess( &lmgr );
    return rc;
}
