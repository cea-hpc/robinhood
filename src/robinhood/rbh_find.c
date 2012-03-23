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
 * Command for recovering filesystem content after a disaster (backup flavor)
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "list_mgr.h"
#include "cmd_helpers.h"
#include "RobinhoodConfig.h"
#include "RobinhoodLogs.h"
#include "RobinhoodMisc.h"
#include "Memory.h"
#include "xplatform_print.h"

#include <unistd.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <libgen.h>
#include <pthread.h>

#define FIND_TAG "find"

static struct option option_tab[] =
{
    {"user", required_argument, NULL, 'u'},
    {"group", required_argument, NULL, 'g'},
    {"type", required_argument, NULL, 't'},
    {"status", required_argument, NULL, 'S'},

    /* config file options */
    {"config-file", required_argument, NULL, 'f'},

    /* log options */
    {"log-level", required_argument, NULL, 'l'},

    /* miscellaneous options */
    {"help", no_argument, NULL, 'h'},
    {"version", no_argument, NULL, 'V'},

    {NULL, 0, NULL, 0}

};

#define SHORT_OPT_STRING    "u:g:t:S:f:l:hV"

/* global variables */

static lmgr_t  lmgr;

/* special character sequences for displaying help */

/* Bold start character sequence */
#define _B "[1m"
/* Bold end charater sequence */
#define B_ "[m"

/* Underline start character sequence */
#define _U "[4m"
/* Underline end character sequence */
#define U_ "[0m"

static const char *help_string =
    _B "Usage:" B_ " %s [options] [path|fid]\n"
    "\n"
    _B "Filters:" B_ "\n"
    "    " _B "-u" B_ " " _U "user" U_ "\n"
    "    " _B "-g" B_ " " _U "group" U_ "\n"
    "    " _B "-t" B_ " " _U "type" U_ "\n"
    "    " _B "-S" B_ " " _U "status" U_ "\n"
    "\n"
    _B "Program options:" B_ "\n"
    "    " _B "-f" B_ " " _U "config_file" U_ "\n"
    "    " _B "-l" B_ " " _U "log_level" U_ "\n"
    "    " _B "-h" B_ ", " _B "--help" B_ "\n"
    "        Display a short help about command line options.\n"
    "    " _B "-V" B_ ", " _B "--version" B_ "\n"
    "        Display version info\n";

static inline void display_help( char *bin_name )
{
    printf( help_string, bin_name );
}

static inline void display_version( char *bin_name )
{
    printf( "\n" );
    printf( "Product:         " PACKAGE_NAME " 'find' command\n" );
    printf( "Version:         " PACKAGE_VERSION "-"RELEASE"\n" );
    printf( "Build:           " COMPIL_DATE "\n" );
    printf( "\n" );
    printf( "Compilation switches:\n" );

/* purpose of this daemon */
#ifdef _LUSTRE_HSM
    printf( "    Lustre-HSM Policy Engine\n" );
#elif defined(_TMP_FS_MGR)
    printf( "    Temporary filesystem manager\n" );
#elif defined(_SHERPA)
    printf( "    SHERPA cache zapper\n" );
#elif defined(_HSM_LITE)
    printf( "    Backup filesystem to external storage\n" );
#else
#error "No purpose was specified"
#endif

/* Access by Fid ? */
#ifdef _HAVE_FID
    printf( "    Address entries by FID\n" );
#else
    printf( "    Address entries by path\n" );
#endif

#ifdef HAVE_CHANGELOGS
    printf( "    MDT Changelogs supported\n" );
#else
    printf( "    MDT Changelogs disabled\n" );
#endif


    printf( "\n" );
#ifdef _LUSTRE
#ifdef LUSTRE_VERSION
    printf( "Lustre Version: " LUSTRE_VERSION "\n" );
#else
    printf( "Lustre FS support\n" );
#endif
#else
    printf( "No Lustre support\n" );
#endif

#ifdef _MYSQL
    printf( "Database binding: MySQL\n" );
#elif defined(_SQLITE)
    printf( "Database binding: SQLite\n" );
#else
#error "No database was specified"
#endif
    printf( "\n" );
    printf( "Report bugs to: <" PACKAGE_BUGREPORT ">\n" );
    printf( "\n" );
}

/* directory callback */
static int dircb(entry_id_t * id_list, attr_set_t * attr_list,
                       unsigned int entry_count )
{
    /* retrieve child entries for all directories */
    int i, rc;

    for (i = 0; i < entry_count; i++)
    {
        entry_id_t * chids = NULL;
        attr_set_t * chattrs = NULL;
        unsigned int chcount = 0;
        int j;

        /* @TODO create filter according to program options */
        rc = ListMgr_GetChild( &lmgr, NULL, id_list+i, 1,
                               ATTR_MASK_fullpath | ATTR_MASK_type,
                               &chids, &chattrs, &chcount );
        if (rc)
        {
            DisplayLog(LVL_MAJOR, FIND_TAG, "ListMgr_GetChild() failed with error %d", rc);
            return rc;
        }

        for (j = 0; j < chcount; j++)
        {
            char * post = "";
            if (ATTR_MASK_TEST(&chattrs[j], type))
            {
                if (!strcasecmp( ATTR(&chattrs[j], type), STR_TYPE_DIR))
                    post="/";
            }
            if (ATTR_MASK_TEST(&chattrs[j], fullpath))
                printf("%s%s\n", ATTR(&chattrs[j], fullpath), post);
            else
                printf(DFID"%s\n", PFID(&chids[j]), post);
        }
    }
    return 0;
}

static int Path2Id(const char *path, entry_id_t * id)
{
    int rc;
#ifndef _HAVE_FID
    struct stat inode;
    if (lstat(path, &inode))
        return -errno;

    id->inode = inode.st_ino;
    id->device = inode.st_dev;
    id->validator = inode.st_ctime;
    return 0;
#else
    /* perform path2fid */
    rc = Lustre_GetFidFromPath(path, id);
    return rc;
#endif
}

/**
 * List the content of the given id/path list
 */
static int list_content(char ** id_list, int id_count)
{
    entry_id_t * ids;
    int i, rc;

    ids = MemCalloc(id_count, sizeof(entry_id_t));
    if (!ids)
        return -ENOMEM;

    for (i = 0; i < id_count; i++)
    {
        /* is it a path or fid? */
        if (sscanf(id_list[i], SFID, RFID(&ids[i])) != 3)
        {
            /* take it as a path */
            rc = Path2Id(id_list[i], &ids[i]);
            if (rc)
            {
                DisplayLog(LVL_MAJOR, FIND_TAG, "Invalid parameter: %s: %s",
                           id_list[i], strerror(-rc));
                goto out;
            }
        }
    }

    rc = rbh_scrub(&lmgr, ids, id_count, 0, dircb);

out:
    /* ids have been processed, free them */
    MemFree(ids);
    return rc;
}


#define MAX_OPT_LEN 1024

/**
 * Main daemon routine
 */
int main( int argc, char **argv )
{
    int            c, option_index = 0;
    char          *bin = basename( argv[0] );

    char           config_file[MAX_OPT_LEN] = "";
    int            force_log_level = FALSE;
    int            log_level = 0;
    int            rc;
    char           err_msg[4096];
    robinhood_config_t config;


#define SHORT_OPT_STRING    "u:g:t:S:f:l:hV"
    /* parse command line options */
    while ((c = getopt_long(argc, argv, SHORT_OPT_STRING, option_tab,
                            &option_index )) != -1)
    {
        switch ( c )
        {
        case 'u':
            break;
        case 'g':
            break;
        case 't':
            break;
        case 'S':
            break;
        case 'f':
            strncpy( config_file, optarg, MAX_OPT_LEN );
            break;
        case 'l':
            force_log_level = TRUE;
            log_level = str2debuglevel( optarg );
            if ( log_level == -1 )
            {
                fprintf(stderr,
                        "Unsupported log level '%s'. CRIT, MAJOR, EVENT, VERB, DEBUG or FULL expected.\n",
                        optarg);
                exit(1);
            }
            break;
        case 'h':
            display_help( bin );
            exit( 0 );
            break;
        case 'V':
            display_version( bin );
            exit( 0 );
            break;
        case ':':
        case '?':
        default:
            display_help( bin );
            exit( 1 );
            break;
        }
    }

    /* analyse arguments */
    /* @TODO if no path is specified, list all */

    /* get default config file, if not specified */
    if ( EMPTY_STRING( config_file ) )
    {
        if ( SearchConfig( config_file ) != 0 )
        {
            fprintf(stderr, "No config file found in '/etc/robinhood.d/"PURPOSE_EXT"'\n" );
            exit(2);
        }
        else
        {
            fprintf(stderr, "Using config file '%s'.\n", config_file );
        }
    }

    /* only read ListMgr config */

    if ( ReadRobinhoodConfig( 0, config_file, err_msg, &config, FALSE ) )
    {
        fprintf( stderr, "Error reading configuration file '%s': %s\n", config_file, err_msg );
        exit( 1 );
    }

    /* set global configuration */
    global_config = config.global_config;

    /* set policies info */
    policies = config.policies;

    if ( force_log_level )
        config.log_config.debug_level = log_level;
    else
        config.log_config.debug_level = LVL_MAJOR; /* no event message */

    /* Set logging to stderr */
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
    rc = ListMgr_Init( &config.lmgr_config, FALSE );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, FIND_TAG, "Error %d initializing list manager", rc );
        exit( rc );
    }
    else
        DisplayLog( LVL_DEBUG, FIND_TAG, "ListManager successfully initialized" );

    if (CheckLastFS(  ) != 0)
        exit(1);

    /* Create database access */
    rc = ListMgr_InitAccess(&lmgr);
    if (rc)
    {
        DisplayLog( LVL_CRIT, FIND_TAG, "Error %d: cannot connect to database", rc );
        exit(rc);
    }

    rc = list_content(argv+optind, argc-optind);

    ListMgr_CloseAccess( &lmgr );

    return rc;

}
