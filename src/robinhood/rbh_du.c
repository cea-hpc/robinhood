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

#define DU_TAG "du"
#define DU_TAG "du"

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
    printf( "Product:         " PACKAGE_NAME " 'du' command\n" );
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

/* initially empty array */
entry_id_t  * dir_array = NULL;
unsigned int array_len = 0;

/* first/last+1 set entry ids in array */
unsigned int array_first = 0;
unsigned int array_next = 0;
#define array_used ((int)array_next-(int)array_first)

#define LS_CHUNK    50

static size_t what_2_power(size_t s)
{
    size_t c = 1;
    while (c < s)
        c <<= 1;
    return c;
}

/** add a list of ids to the main array */
static inline void add_id_list(entry_id_t  * list, unsigned int count)
{
    /* always add at the beginning to have LIFO behavior */

    /* is there enough room before the first item ? */
    if (count <= array_first)
    {
        /* copy it just before 'first' (entries must be consecutive) */
        memcpy(&dir_array[array_first-count], list, count * sizeof(entry_id_t));
        array_first -= count;

#ifdef _DEBUG_ID_LIST
        printf("1)...<new_ids:%u-%u><ids:%u-%u>...(len=%Lu)\n", array_first,
                array_first+count-1, array_first+count, array_next-1, array_len);
#endif
    }
    /* is the array empty ?*/
    else if ((array_used == 0) && (count <= array_len))
    {
        /* copy from the begginning */
        memcpy(dir_array, list, count * sizeof(entry_id_t));
        array_first = 0;
        array_next = count;

#ifdef _DEBUG_ID_LIST
        printf("2) <new_ids:%u-%u>...(len=%Lu)\n", array_first, array_next - 1, array_len);
#endif
    }
    else /* increase array size */
    {
        entry_id_t  * dir_array_new;
        size_t new_len = what_2_power(array_len + count);
        dir_array_new = MemAlloc(new_len * sizeof(entry_id_t));
        /* first copy new ids */
        memcpy(dir_array_new, list, count * sizeof(entry_id_t));
        if (dir_array && (array_used > 0))
        {
            /* then copy current ids */
            memcpy(&dir_array_new[count+1], &dir_array[array_first],
                   array_used * sizeof(entry_id_t));

#ifdef _DEBUG_ID_LIST
            printf("3) <new_ids:%u-%u><ids:%u-%u>...(len=%Lu)\n", 0, count - 1,
                   count+1, array_next-1, new_len);
#endif
        }
#ifdef _DEBUG_ID_LIST
        else
            printf("4) <new_ids:%u-%u>...(len=%Lu)\n", 0, count - 1,
                   new_len);
#endif

        /* free old array */
        if (dir_array)
            MemFree(dir_array);

        /* update array info */
        dir_array = dir_array_new;
        array_next = array_used + count;
        array_first = 0;
        array_len = new_len;
    }
}

/** release a list of ids from the array */
static inline void release_list(unsigned int first, unsigned int count)
{
    if (first != array_first)
        DisplayLog(LVL_CRIT, DU_TAG, "IMPLEMENTATION ISSUE: array_first was %u, is now %u\n",
                   first, array_first);
    array_first += count;

#ifdef _DEBUG_ID_LIST
    printf("released %u-%u\n", array_first - count, array_first - 1);
#endif
}


/* scan a set of directories */
static int process_dir_set()
{
    entry_id_t  * curr_array;
    unsigned int count;
    lmgr_filter_t  filter;
    filter_value_t fv;
    int i, rc;

    /* only get subdirs */
    fv.val_str = STR_TYPE_DIR;
    lmgr_simple_filter_init( &filter );
    lmgr_simple_filter_add( &filter, ATTR_INDEX_type, EQUAL, fv, 0 );

    /* while the array is not empty */
    while (array_used > 0)
    {
        unsigned int res_count = 0;
        entry_id_t * child_ids;
        attr_set_t * child_attrs;

        /* get a set of entry_ids */
        if (array_used < LS_CHUNK)
        {
            /* get all available dirs */
            curr_array = &dir_array[array_first];
            count = array_used;
        }
        else
        {
            /* get a constant chunk */
            curr_array = &dir_array[array_first];
            count = LS_CHUNK;
        }

#ifdef _DEBUG_ID_LIST
        printf("processing %u-%u\n", array_first, array_first+count-1);
#endif

        /* read childs */
        res_count = 0;
        child_ids = NULL;
        child_attrs = NULL;
        rc = ListMgr_GetChild(&lmgr, &filter, curr_array, count, ATTR_MASK_fullpath, /* XXX for debug */
                              &child_ids, &child_attrs, &res_count);

        if (rc)
        {
            DisplayLog(LVL_CRIT, DU_TAG, "ListMgr_GetChild() terminated with error %d", rc);
            /* @TODO free allocated resources */
            break;
        }

        for (i = 0; i < res_count; i++)
            printf(DFID": %s\n", PFID(&child_ids[i]),
                   ATTR_MASK_TEST(&child_attrs[i], fullpath) ? ATTR(&child_attrs[i], fullpath): "[n/a]");

        /* attributes no more needed */
        MemFree(child_attrs);
        /* can release the list of input ids */
        release_list(array_first, count);

        /* copy entry ids before freeing them */
        add_id_list(child_ids, res_count);

        /* free the returned array */
        MemFree(child_ids);
    }

    return 0;
}



/**
 * List the content of the given id list
 */
int list_content(char ** id_list, int id_count)
{
    entry_id_t * ids;
    int i;

    ids = MemCalloc(id_count, sizeof(entry_id_t));
    if (!ids)
        return -ENOMEM;

    for (i = 0; i < id_count; i++)
    {
        if (sscanf(id_list[i], SFID, RFID(&ids[i])) != 3)
        {
            DisplayLog(LVL_CRIT, DU_TAG, "Invalid id: %s", id_list[i]);
            MemFree(ids);
            return -EINVAL;
        }
    }

    add_id_list(ids, id_count);
    /* ids have been copied, free them */
    MemFree(ids);
    return process_dir_set();
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
    int            rc, i;
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

    /* print extra arguments */
    for (i = optind; i < argc; i++)
        printf( "arg: %s\n", argv[i]);

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
        DisplayLog( LVL_CRIT, DU_TAG, "Error %d initializing list manager", rc );
        exit( rc );
    }
    else
        DisplayLog( LVL_DEBUG, DU_TAG, "ListManager successfully initialized" );

    if (CheckLastFS(  ) != 0)
        exit(1);

    /* Create database access */
    rc = ListMgr_InitAccess(&lmgr);
    if (rc)
    {
        DisplayLog( LVL_CRIT, DU_TAG, "Error %d: cannot connect to database", rc );
        exit(rc);
    }

    list_content(argv+optind, argc-optind);

    ListMgr_CloseAccess( &lmgr );

    return rc;

}
