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

#include "list_mgr.h"
#include "RobinhoodConfig.h"
#include "RobinhoodMisc.h"
#include <errno.h>

#define LMGR_CONFIG_BLOCK "ListManager"

#define MYSQL_CONFIG_BLOCK "MySQL"
#define SQLITE_CONFIG_BLOCK "SQLite"

/* exported variable available to list_mgr modules */
lmgr_config_t  lmgr_config;                      /* module configuration */


int SetDefaultLmgrConfig( void *module_config, char *msg_out )
{
    lmgr_config_t *conf = ( lmgr_config_t * ) module_config;
    msg_out[0] = '\0';

    conf->commit_behavior = 1; /* transaction */
    conf->connect_retry_min = 1;
    conf->connect_retry_max = 30;

#ifdef _MYSQL
    strcpy( conf->db_config.server, "localhost" );
    conf->db_config.db[0] = '\0';
    strcpy( conf->db_config.user, "robinhood" );
    conf->db_config.password[0] = '\0';
    conf->db_config.port = 0;
    conf->db_config.socket[0] = '\0';
    strcpy(conf->db_config.engine, "InnoDB");
#elif defined (_SQLITE)
    strcpy( conf->db_config.filepath, "/var/robinhood/robinhood_sqlite_db" );
    conf->db_config.retry_delay_microsec = 1000;        /* 1ms */
#endif

     conf->user_acct = TRUE;
     conf->group_acct = TRUE;

    return 0;
}

int WriteLmgrConfigDefault( FILE * output )
{
    print_begin_block( output, 0, LMGR_CONFIG_BLOCK, NULL );
    print_line( output, 1, "commit_behavior             : transaction" );
    print_line( output, 1, "connect_retry_interval_min  : 1s" );
    print_line( output, 1, "connect_retry_interval_max  : 30s" );
    print_line( output, 1, "user_acct  : enabled" );
    print_line( output, 1, "group_acct : enabled" );
    fprintf( output, "\n" );

#ifdef _MYSQL
    print_begin_block( output, 1, MYSQL_CONFIG_BLOCK, NULL );
    print_line( output, 2, "server  :   localhost" );
    print_line( output, 2, "db      :   [MANDATORY]" );
    print_line( output, 2, "user    :   robinhood" );
    print_line( output, 2, "password|password_file : [MANDATORY]" );
    print_line( output, 2, "port    :   (MySQL default)" );
    print_line( output, 2, "socket  :   NONE" );
    print_line( output, 2, "engine  :   InnoDB" );
    print_end_block( output, 1 );
#elif defined (_SQLITE)
    print_begin_block( output, 1, SQLITE_CONFIG_BLOCK, NULL );
    print_line( output, 2, "db_file              :  \"/var/robinhood/robinhood_sqlite_db\"" );
    print_line( output, 2, "retry_delay_microsec :  1000 (1 millisec)" );
    print_end_block( output, 1 );
#endif

    print_end_block( output, 0 );
    return 0;
}

int ReadLmgrConfig( config_file_t config, void *module_config, char *msg_out, int for_reload )
{
    int            rc;
    lmgr_config_t *conf = ( lmgr_config_t * ) module_config;
    char         **options = NULL;
    unsigned int   nb_options = 0;
    char           tmpstr[1024];

    config_item_t  db_block;

    static const char *lmgr_allowed[] = {
        "commit_behavior",
        "connect_retry_interval_min",
        "connect_retry_interval_max",
        "user_acct",
        "group_acct",
        MYSQL_CONFIG_BLOCK,
        SQLITE_CONFIG_BLOCK,
        NULL
    };


#ifdef _MYSQL
    static const char *db_allowed[] = {
        "server", "db", "user", "password", "password_file", "port", "socket",
        "innodb", "engine", NULL
    };
#elif defined (_SQLITE)
    static const char *db_allowed[] = {
        "db_file", "retry_delay_microsec",
        NULL
    };
#endif

    /* get ListManager block */

    config_item_t  lmgr_block = rh_config_FindItemByName( config, LMGR_CONFIG_BLOCK );

    if ( lmgr_block == NULL )
    {
        strcpy( msg_out, "Missing configuration block '" LMGR_CONFIG_BLOCK "'" );
        return ENOENT;
    }

    if ( rh_config_ItemType( lmgr_block ) != CONFIG_ITEM_BLOCK )
    {
        strcpy( msg_out, "A block is expected for '" LMGR_CONFIG_BLOCK "' item" );
        return EINVAL;
    }

    /* retrieve parameters */

    /* 1) commit_behavior */

    rc = GetStringParam( lmgr_block, LMGR_CONFIG_BLOCK, "commit_behavior",
                         STR_PARAM_NO_WILDCARDS, tmpstr, 1024, &options, &nb_options, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc != ENOENT )
    {
        if ( !strcasecmp( tmpstr, "autocommit" ) )
            conf->commit_behavior = 0;
        else if ( !strcasecmp( tmpstr, "transaction" ) )
            conf->commit_behavior = 1;
        else if ( !strcasecmp( tmpstr, "periodical" )  /* for backward compatibility */
                  || !strcasecmp( tmpstr, "periodic" ) )
        {
            if ( ( nb_options != 1 ) || !options || !options[0] )
            {
                strcpy( msg_out,
                        "A single argument is expected for periodic commit behavior. Eg: commit_behavior = periodic(1000)" );
                return EINVAL;
            }

            conf->commit_behavior = atoi( options[0] );
            if ( conf->commit_behavior == 0 )
            {
                strcpy( msg_out,
                        "The argument for \"" LMGR_CONFIG_BLOCK
                        "::commit_behavior = periodical\" must be a positive integer. Eg: commit_behavior = periodic(1000)" );
                return EINVAL;
            }
        }
        else
        {
            sprintf( msg_out, "Invalid commit behavior '%s' (expected: autocommit, "
                     "transaction, periodic(<count>))", tmpstr );
            return EINVAL;
        }
    }

    /* 2) connect_retry_interval_min  and connect_retry_interval_max */

    rc = GetDurationParam( lmgr_block, LMGR_CONFIG_BLOCK,
                           "connect_retry_interval_min",
                           INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL,
                           ( int * ) &conf->connect_retry_min, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetDurationParam( lmgr_block, LMGR_CONFIG_BLOCK,
                           "connect_retry_interval_max",
                           INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL,
                           ( int * ) &conf->connect_retry_max, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    /* 3) ACCT configuration*/

    rc = GetBoolParam( lmgr_block, LMGR_CONFIG_BLOCK,
                       "user_acct", 0, &conf->user_acct, NULL, NULL, msg_out);
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetBoolParam( lmgr_block, LMGR_CONFIG_BLOCK,
                       "group_acct", 0, &conf->group_acct, NULL, NULL, msg_out);
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    CheckUnknownParameters( lmgr_block, LMGR_CONFIG_BLOCK, lmgr_allowed );

    /* Database specific parameters */
#ifdef _MYSQL

    /* get MySQL block */

    db_block = rh_config_GetItemByName( lmgr_block, MYSQL_CONFIG_BLOCK );

    if ( db_block == NULL )
    {
        strcpy( msg_out,
                "Missing configuration block '" LMGR_CONFIG_BLOCK "::" MYSQL_CONFIG_BLOCK "'" );
        return ENOENT;
    }

    if ( rh_config_ItemType( db_block ) != CONFIG_ITEM_BLOCK )
    {
        sprintf( msg_out,
                 "A block is expected for '" LMGR_CONFIG_BLOCK "::" MYSQL_CONFIG_BLOCK
                 "' item, line %d", rh_config_GetItemLine( db_block ) );
        return EINVAL;
    }

    rc = GetStringParam( db_block, MYSQL_CONFIG_BLOCK, "server",
                         STR_PARAM_NO_WILDCARDS, conf->db_config.server, 256, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetStringParam( db_block, MYSQL_CONFIG_BLOCK, "db",
                         PARAM_MANDATORY | STR_PARAM_NO_WILDCARDS,
                         conf->db_config.db, 256, NULL, NULL, msg_out );
    if ( rc )
        return rc;

    rc = GetStringParam( db_block, MYSQL_CONFIG_BLOCK, "user",
                         STR_PARAM_NO_WILDCARDS, conf->db_config.user, 256, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetStringParam( db_block, MYSQL_CONFIG_BLOCK, "password",
                         0, conf->db_config.password, 256, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc == ENOENT )
    {
        FILE          *passfile;
        char           errstr[1024];

        rc = GetStringParam( db_block, MYSQL_CONFIG_BLOCK,
                             "password_file",
                             STR_PARAM_ABSOLUTE_PATH |
                             STR_PARAM_NO_WILDCARDS, tmpstr, 1024, NULL, NULL, msg_out );
        if ( ( rc != 0 ) && ( rc != ENOENT ) )
            return rc;
        else if ( rc == ENOENT )
        {
            strcpy( msg_out,
                    MYSQL_CONFIG_BLOCK "::password or "
                    MYSQL_CONFIG_BLOCK "::password_file must be provided" );
            return ENOENT;
        }

        /* read password file and @TODO check its rights */
        passfile = fopen( tmpstr, "r" );
        if ( !passfile )
        {
            rc = errno;
            sprintf( msg_out, "Error opening password file %s : %s", tmpstr, strerror(errno) );
            return rc;
        }
        fscanf( passfile, "%1024s", tmpstr );
        if ( ferror( passfile ) )
        {
            rc = errno;
            strerror_r( rc, errstr, 1024 );
            sprintf( msg_out, "Error reading password file %s : %s", tmpstr, errstr );
            return rc;
        }
        fclose( passfile );
        rh_strncpy(conf->db_config.password, tmpstr, 256);
    }

    rc = GetIntParam( db_block, MYSQL_CONFIG_BLOCK, "port",
                         INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL,
                         (int*)&conf->db_config.port, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetStringParam( db_block, MYSQL_CONFIG_BLOCK, "socket",
                         STR_PARAM_NO_WILDCARDS | STR_PARAM_ABSOLUTE_PATH,
                         conf->db_config.socket, sizeof(conf->db_config.socket), NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;


    rc = GetStringParam(db_block, MYSQL_CONFIG_BLOCK, "engine",
                        STR_PARAM_NO_WILDCARDS | STR_PARAM_NOT_EMPTY,
                        conf->db_config.engine, sizeof(conf->db_config.engine),
                        NULL, NULL, msg_out);
    if (rc == ENOENT)
    {
        int dummy = TRUE;

        /* try to get innodb parameter for backward compat */
        rc = GetBoolParam(db_block, MYSQL_CONFIG_BLOCK, "innodb", 0,
                          &dummy, NULL, NULL, msg_out);
        if ((rc != 0) && (rc != ENOENT))
            return rc;
        else if (rc == 0)
        {
            if (dummy)
                rh_strncpy(conf->db_config.engine, "InnoDB", sizeof(conf->db_config.engine));
            else
                rh_strncpy(conf->db_config.engine, "MyISAM", sizeof(conf->db_config.engine));

            DisplayLog(LVL_CRIT, "LmgrConfig",
                       "WARNING: 'innodb' parameter is deprecated (specify \"engine = %s\" instead)",
                       conf->db_config.engine);
        }
    }
    else if (rc != 0)/* other error */
        return rc;

    CheckUnknownParameters( db_block, MYSQL_CONFIG_BLOCK, db_allowed );

#elif defined (_SQLITE)
    /* get SQLite block */

    db_block = rh_config_GetItemByName( lmgr_block, SQLITE_CONFIG_BLOCK );

    if ( db_block == NULL )
    {
        strcpy( msg_out,
                "Missing configuration block '" LMGR_CONFIG_BLOCK "::" SQLITE_CONFIG_BLOCK "'" );
        return ENOENT;
    }

    if ( rh_config_ItemType( db_block ) != CONFIG_ITEM_BLOCK )
    {
        sprintf( msg_out,
                 "A block is expected for '" LMGR_CONFIG_BLOCK "::" SQLITE_CONFIG_BLOCK
                 "' item, line %d", rh_config_GetItemLine( db_block ) );
        return EINVAL;
    }


    rc = GetStringParam( db_block, SQLITE_CONFIG_BLOCK, "db_file",
                         STR_PARAM_ABSOLUTE_PATH |
                         STR_PARAM_NO_WILDCARDS,
                         conf->db_config.filepath, RBH_PATH_MAX, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetIntParam( db_block, SQLITE_CONFIG_BLOCK,
                      "retry_delay_microsec",
                      INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL,
                      (int*)&conf->db_config.retry_delay_microsec, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    CheckUnknownParameters( db_block, SQLITE_CONFIG_BLOCK, db_allowed );
#endif


    return 0;
}


int ReloadLmgrConfig( void *module_config )
{
    lmgr_config_t *conf = ( lmgr_config_t * ) module_config;

    if ( conf->commit_behavior != lmgr_config.commit_behavior )
        DisplayLog( LVL_MAJOR, "LmgrConfig",
                    LMGR_CONFIG_BLOCK
                    "::commit_behavior changed in config file, but cannot be modified dynamically" );

    if ( conf->connect_retry_min != lmgr_config.connect_retry_min )
    {
        DisplayLog( LVL_EVENT, "LmgrConfig",
                    LMGR_CONFIG_BLOCK
                    "::connect_retry_interval_min updated: %u->%u",
                    lmgr_config.connect_retry_min, conf->connect_retry_min );
        lmgr_config.connect_retry_min = conf->connect_retry_min;
    }

    if ( conf->connect_retry_max != lmgr_config.connect_retry_max )
    {
        DisplayLog( LVL_EVENT, "LmgrConfig",
                    LMGR_CONFIG_BLOCK
                    "::connect_retry_interval_max updated: %u->%u",
                    lmgr_config.connect_retry_max, conf->connect_retry_max );
        lmgr_config.connect_retry_max = conf->connect_retry_max;
    }

#ifdef _MYSQL

    if ( strcmp( conf->db_config.server, lmgr_config.db_config.server ) )
        DisplayLog( LVL_MAJOR, "LmgrConfig",
                    MYSQL_CONFIG_BLOCK
                    "::server changed in config file, but cannot be modified dynamically" );
    if ( strcmp( conf->db_config.db, lmgr_config.db_config.db ) )
        DisplayLog( LVL_MAJOR, "LmgrConfig",
                    MYSQL_CONFIG_BLOCK
                    "::db changed in config file, but cannot be modified dynamically" );
    if ( strcmp( conf->db_config.user, lmgr_config.db_config.user ) )
        DisplayLog( LVL_MAJOR, "LmgrConfig",
                    MYSQL_CONFIG_BLOCK
                    "::user changed in config file, but cannot be modified dynamically" );
    if ( strcmp( conf->db_config.password, lmgr_config.db_config.password ) )
        DisplayLog( LVL_MAJOR, "LmgrConfig",
                    MYSQL_CONFIG_BLOCK
                    "::password changed in config file, but cannot be modified dynamically" );
#elif defined (_SQLITE)
    if ( strcmp( conf->db_config.filepath, lmgr_config.db_config.filepath ) )
        DisplayLog( LVL_MAJOR, "LmgrConfig",
                    SQLITE_CONFIG_BLOCK
                    "::db_file changed in config file, but cannot be modified dynamically" );

    if ( conf->db_config.retry_delay_microsec != lmgr_config.db_config.retry_delay_microsec )
    {
        DisplayLog( LVL_EVENT, "LmgrConfig",
                    SQLITE_CONFIG_BLOCK
                    "::retry_delay_microsec updated: %u->%u",
                    lmgr_config.db_config.retry_delay_microsec,
                    conf->db_config.retry_delay_microsec );
        lmgr_config.db_config.retry_delay_microsec = conf->db_config.retry_delay_microsec;
    }

#endif

    return 0;
}


int WriteLmgrConfigTemplate( FILE * output )
{
    print_begin_block( output, 0, LMGR_CONFIG_BLOCK, NULL );

    print_line( output, 1, "# Method for committing information to database." );
    print_line( output, 1, "# Possible values are:" );
    print_line( output, 1,
                "# - \"autocommit\": weak transactions (more efficient, but database inconsistencies may occur)" );
    print_line( output, 1,
                "# - \"transaction\": manage operations in transactions (best consistency, lower performance)" );
    print_line( output, 1,
                "# - \"periodic(<nb_transaction>)\": periodically commit (every <n> transactions)." );
    print_line( output, 1, "commit_behavior = transaction ;" );
    fprintf( output, "\n" );
    print_line( output, 1,
                "# Minimum time (in seconds) to wait before trying to reestablish a lost connection." );
    print_line( output, 1,
                "# Then this time is multiplied by 2 until reaching connect_retry_interval_max" );
    print_line( output, 1, "connect_retry_interval_min = 1 ;" );
    print_line( output, 1, "connect_retry_interval_max = 30 ;" );

    print_line( output, 1, "# disable the following options if you are not interested in" );
    print_line( output, 1, "# user or group stats (to speed up scan)" );
    print_line( output, 1, "user_acct  = enabled ;" );
    print_line( output, 1, "group_acct = enabled ;" );
    fprintf( output, "\n" );
#ifdef _MYSQL
    print_begin_block( output, 1, MYSQL_CONFIG_BLOCK, NULL );
    print_line( output, 2, "server = \"localhost\" ;" );
    print_line( output, 2, "db     = \"robinhood_db\" ;" );
    print_line( output, 2, "user   = \"robinhood\" ;" );
    print_line( output, 2, "password_file = \"/etc/robinhood.d/.dbpassword\" ;" );
    print_line( output, 2, "# port   = 3306 ;" );
    print_line( output, 2, "# socket = \"/tmp/mysql.sock\" ;" );
    print_line( output, 2, "engine = InnoDB ;" );
    print_end_block( output, 1 );
#elif defined (_SQLITE)
    print_begin_block( output, 1, SQLITE_CONFIG_BLOCK, NULL );
    print_line( output, 2, "db_file = \"/var/robinhood/robinhood_sqlite_db\" ;" );
    print_line( output, 2, "retry_delay_microsec = 1000 ;" );
    print_end_block( output, 1 );
#endif

    print_end_block( output, 0 );

    return 0;
}


bool lmgr_parallel_batches(void)
{
    return !(lmgr_config.user_acct || lmgr_config.group_acct);
}
