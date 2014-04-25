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
 * Command for rebinding a backend entry to a new fid.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "list_mgr.h"
#include "RobinhoodConfig.h"
#include "RobinhoodLogs.h"
#include "RobinhoodMisc.h"
#include "xplatform_print.h"
#include "backend_ext.h"
#include "cmd_helpers.h"

#include <unistd.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <libgen.h>
#include <pthread.h>
#include <signal.h>

#define LOGTAG "Rebind"

static struct option option_tab[] =
{
    /* config file options */
    {"config-file", required_argument, NULL, 'f'},

    /* log options */
    {"log-level", required_argument, NULL, 'l'},

    /* miscellaneous options */
    {"help", no_argument, NULL, 'h'},
    {"version", no_argument, NULL, 'V'},

    {NULL, 0, NULL, 0}

};

#define SHORT_OPT_STRING    "f:l:hV"

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
    _B "Usage:" B_ " %s [options] <old_backend_path> <new_fs_path> [new_fid]\n"
    _B "By default, new_fid is taken as the current fid of new_fs_path but it might be different\n"
    "\n"
    _B "Config file options:" B_ "\n"
    "    " _B "-f" B_ " " _U "file" U_ ", " _B "--config-file=" B_ _U "file" U_ "\n"
    "        Path to configuration file (or short name).\n"
    "\n"
    _B "Miscellaneous options:" B_ "\n"
    "    " _B "-l" B_ " " _U "level" U_ ", " _B "--log-level=" B_ _U "level" U_ "\n"
    "        Force the log verbosity level (overides configuration value).\n"
    "        Allowed values: CRIT, MAJOR, EVENT, VERB, DEBUG, FULL.\n"
    "    " _B "-h" B_ ", " _B "--help" B_ "\n"
    "        Display a short help about command line options.\n"
    "    " _B "-V" B_ ", " _B "--version" B_ "\n" "        Display version info\n";


static inline void display_help( char *bin_name )
{
    printf( help_string, bin_name );
}

static inline void display_version( char *bin_name )
{
    printf( "\n" );
    printf( "Product:         " PACKAGE_NAME " rebind tool\n" );
    printf( "Version:         " PACKAGE_VERSION "-"RELEASE"\n" );
    printf( "Build:           " COMPIL_DATE "\n" );
    printf( "\n" );
    printf( "Compilation switches:\n" );

/* purpose of this daemon */
#ifdef _LUSTRE_HSM
    printf( "    Lustre-HSM Policy Engine\n" );
#elif defined(_TMP_FS_MGR)
    printf( "    Temporary filesystem manager\n" );
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

static inline int rebind_helper(const char       *old_backend_path,
                                const char       *new_fs_path,
                                const char       *new_fid_str)
{
    int rc;
    char rp[RBH_PATH_MAX];
    char new_backend_path[RBH_PATH_MAX];
    entry_id_t new_fid;
    char * tmp;

    /* full path required */
    tmp = realpath( new_fs_path, NULL );
    if (tmp == NULL)
    {
        rc = -errno;
        DisplayLog(LVL_CRIT, LOGTAG, "Error in realpath(%s): %s",
                   new_fs_path, strerror(-rc));
        return rc;
    }
    if (strlen(tmp) >= RBH_PATH_MAX)
    {
        DisplayLog( LVL_CRIT, LOGTAG, "Path length is too long!" );
        return -ENAMETOOLONG;
    }
    /* safe because of previous check */
    strcpy(rp, tmp);
    /* now can release tmp path */
    free(tmp);

    if (new_fid_str)
    {
        int nb_read;

        /* parse fid */
        if (new_fid_str[0] == '[')
            nb_read = sscanf(new_fid_str, "["SFID"]", RFID(&new_fid));
        else
            nb_read = sscanf(new_fid_str, SFID, RFID(&new_fid));

        if (nb_read != FID_SCAN_CNT)
        {
            DisplayLog( LVL_CRIT, LOGTAG, "Unexpected format for fid %s", new_fid_str );
            return -EINVAL;
        }

        printf("Binding "DFID" to '%s'...\n", PFID(&new_fid), old_backend_path);
    }
    else
    {
        /* get fid for the given file */
        rc = Path2Id(new_fs_path, &new_fid);
        if (rc)
            return rc;

        printf("Binding '%s' ("DFID") to '%s'...\n", new_fs_path, PFID(&new_fid),
               old_backend_path);
    }

    /* build the new backend path for the entry */
    rc = rbhext_rebind(rp, old_backend_path, new_backend_path, &new_fid);
    if (rc) {
        fprintf(stderr,"rebind failed for '%s': %s\n", rp, strerror(-rc));
        return rc;
    }
    else
    {
        printf("'%s' sucessfully rebound to '%s'\n", rp, new_backend_path);
        return 0;
    }
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
    int chgd = 0;
    char badcfg[RBH_PATH_MAX];

    /* parse command line options */
    while ( ( c = getopt_long( argc, argv, SHORT_OPT_STRING, option_tab,
                               &option_index ) ) != -1 )
    {
        switch ( c )
        {
        case 'f':
            rh_strncpy(config_file, optarg, MAX_OPT_LEN);
            break;
        case 'l':
            force_log_level = TRUE;
            log_level = str2debuglevel( optarg );
            if ( log_level == -1 )
            {
                fprintf( stderr,
                         "Unsupported log level '%s'. CRIT, MAJOR, EVENT, VERB, DEBUG or FULL expected.\n",
                         optarg );
                exit( 1 );
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

    /* 2 expected argument: old backend path, new path is FS */
    if ( optind > argc - 2 )
    {
        fprintf( stderr, "Error: missing arguments on command line.\n" );
        display_help(bin);
        exit( 1 );
    }
    else if  ( optind < argc - 3 )
    {
        fprintf( stderr, "Error: too many arguments on command line.\n" );
        display_help(bin);
        exit( 1 );
    }

    /* get default config file, if not specified */
    if (SearchConfig(config_file, config_file, &chgd, badcfg, MAX_OPT_LEN) != 0)
    {
        fprintf(stderr, "No config file (or too many) found matching %s\n", badcfg );
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

    /* set policies info */
    policies = config.policies;

    if ( force_log_level )
        config.log_config.debug_level = log_level;

    /* XXX HOOK: Set logging to stderr */
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

    /* Initialize Filesystem access */
    rc = InitFS();
    if (rc)
        exit(rc);

#ifdef _HSM_LITE
    rc = Backend_Start( &config.backend_config, 0 );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, LOGTAG, "Error initializing backend" );
        exit( 1 );
    }
#endif

    if (optind == argc - 2)
        rc = rebind_helper(argv[optind], argv[optind+1], NULL);
    else if (optind == argc - 3)
        rc = rebind_helper(argv[optind], argv[optind+1], argv[optind+2]);

    return rc;
}
