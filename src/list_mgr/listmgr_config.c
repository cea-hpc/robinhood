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
#include "rbh_cfg_helpers.h"
#include "rbh_misc.h"
#include "rbh_logs.h"
#include <errno.h>

#define LMGR_CONFIG_BLOCK "ListManager"

#define MYSQL_CONFIG_BLOCK "MySQL"
#define SQLITE_CONFIG_BLOCK "SQLite"

/* tag for logging */
#define TAG "LmgrConfig"

/** exported variable available for list_mgr internals */
lmgr_config_t lmgr_config;

static void lmgr_cfg_set_default(void *module_config)
{
    lmgr_config_t *conf = (lmgr_config_t *) module_config;

    conf->commit_behavior = 1;  /* transaction */
    conf->connect_retry_min = 1;
    conf->connect_retry_max = 30;

#ifdef _MYSQL
    strcpy(conf->db_config.server, "localhost");
    conf->db_config.db[0] = '\0';
    strcpy(conf->db_config.user, "robinhood");
    conf->db_config.password[0] = '\0';
    conf->db_config.port = 0;
    conf->db_config.socket[0] = '\0';
    strcpy(conf->db_config.engine, "InnoDB");

    /* Depending on the MariaDB version, the TokuDB compression
     * default can be either no compression or zlib compression. See
     * https://mariadb.com/kb/en/mariadb/tokudb-differences. Lets pick
     * no compression, as zlib compression appears to slow database
     * inserts when used by robinhood. */
    strcpy(conf->db_config.tokudb_compression, "tokudb_uncompressed");
#elif defined(_SQLITE)
    strcpy(conf->db_config.filepath, "/var/robinhood/robinhood_sqlite_db");
    conf->db_config.retry_delay_microsec = 1000;    /* 1ms */
#endif

    conf->acct = true;
}

static void lmgr_cfg_write_default(FILE *output)
{
    print_begin_block(output, 0, LMGR_CONFIG_BLOCK, NULL);
    print_line(output, 1, "commit_behavior             : transaction");
    print_line(output, 1, "connect_retry_interval_min  : 1s");
    print_line(output, 1, "connect_retry_interval_max  : 30s");
    print_line(output, 1, "accounting  : enabled");
    fprintf(output, "\n");

#ifdef _MYSQL
    print_begin_block(output, 1, MYSQL_CONFIG_BLOCK, NULL);
    print_line(output, 2, "server  :   localhost");
    print_line(output, 2, "db      :   [MANDATORY]");
    print_line(output, 2, "user    :   robinhood");
    print_line(output, 2, "password|password_file : [MANDATORY]");
    print_line(output, 2, "port    :   (MySQL default)");
    print_line(output, 2, "socket  :   NONE");
    print_line(output, 2, "engine  :   InnoDB");
    print_end_block(output, 1);
#elif defined(_SQLITE)
    print_begin_block(output, 1, SQLITE_CONFIG_BLOCK, NULL);
    print_line(output, 2,
               "db_file              :  \"/var/robinhood/robinhood_sqlite_db\"");
    print_line(output, 2, "retry_delay_microsec :  1000 (1 millisec)");
    print_end_block(output, 1);
#endif

    print_end_block(output, 0);
}

static int lmgr_cfg_read(config_file_t config, void *module_config,
                         char *msg_out)
{
    int rc;
    bool bval;
    lmgr_config_t *conf = (lmgr_config_t *) module_config;
    char **options = NULL;
    unsigned int nb_options = 0;
    char tmpstr[1024];
    config_item_t lmgr_block;
    config_item_t db_block;

    static const char *lmgr_allowed[] = {
        "commit_behavior", "connect_retry_interval_min",
        "connect_retry_interval_max", "accounting",
        MYSQL_CONFIG_BLOCK, SQLITE_CONFIG_BLOCK,
        "user_acct", "group_acct",  /* deprecated => accounting */
        NULL
    };

    const cfg_param_t cfg_params[] = {
        {"connect_retry_interval_min", PT_DURATION, PFLG_POSITIVE |
         PFLG_NOT_NULL, &conf->connect_retry_min, 0},
        {"connect_retry_interval_max", PT_DURATION, PFLG_POSITIVE |
         PFLG_NOT_NULL, &conf->connect_retry_max, 0},
        {"accounting", PT_BOOL, 0, &conf->acct, 0},
        END_OF_PARAMS
    };

#ifdef _MYSQL
    static const char *db_allowed[] = {
        "server", "db", "user", "password", "password_file", "port", "socket",
        "engine", "tokudb_compression", NULL
    };

    const cfg_param_t db_params[] = {
        {"server", PT_STRING, PFLG_NO_WILDCARDS,
         conf->db_config.server, sizeof(conf->db_config.server)}
        ,
        {"db", PT_STRING, PFLG_MANDATORY | PFLG_NO_WILDCARDS,
         conf->db_config.db, sizeof(conf->db_config.db)}
        ,
        {"user", PT_STRING, PFLG_NO_WILDCARDS, conf->db_config.user,
         sizeof(conf->db_config.user)}
        ,
        {"port", PT_INT, PFLG_POSITIVE | PFLG_NOT_NULL,
         (int *)&conf->db_config.port, 0},
        {"socket", PT_STRING, PFLG_NO_WILDCARDS | PFLG_ABSOLUTE_PATH,
         conf->db_config.socket, sizeof(conf->db_config.socket)}
        ,
        {"engine", PT_STRING, PFLG_NO_WILDCARDS | PFLG_NOT_EMPTY,
         conf->db_config.engine, sizeof(conf->db_config.engine)}
        ,
        {"tokudb_compression", PT_STRING, PFLG_NO_WILDCARDS,
         conf->db_config.tokudb_compression,
         sizeof(conf->db_config.tokudb_compression)}
        ,
        END_OF_PARAMS
    };
#elif defined(_SQLITE)
    static const char *db_allowed[] = {
        "db_file", "retry_delay_microsec",
        NULL
    };
    const cfg_param_t db_params[] = {
        {"db_file", PT_STRING, PFLG_ABSOLUTE_PATH | PFLG_NO_WILDCARDS,
         conf->db_config.filepath, sizeof(conf->db_config.filepath)}
        ,
        {"retry_delay_microsec", PT_INT, PFLG_POSITIVE | PFLG_NOT_NULL,
         (int *)&conf->db_config.retry_delay_microsec, 0},
        END_OF_PARAMS
    };
#endif

    /* get ListManager block */
    rc = get_cfg_block(config, LMGR_CONFIG_BLOCK, &lmgr_block, msg_out);
    if (rc)
        return rc;

    /* retrieve std parameters */
    rc = read_scalar_params(lmgr_block, LMGR_CONFIG_BLOCK, cfg_params, msg_out);
    if (rc)
        return rc;

    /* commit_behavior */
    rc = GetStringParam(lmgr_block, LMGR_CONFIG_BLOCK, "commit_behavior",
                        PFLG_NO_WILDCARDS, tmpstr, sizeof(tmpstr), &options,
                        &nb_options, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    else if (rc != ENOENT) {
        if (!strcasecmp(tmpstr, "autocommit"))
            conf->commit_behavior = 0;
        else if (!strcasecmp(tmpstr, "transaction"))
            conf->commit_behavior = 1;
        else if (!strcasecmp(tmpstr, "periodic")) {
            if ((nb_options != 1) || !options || !options[0]) {
                strcpy(msg_out,
                       "A single argument is expected for periodic commit behavior. Eg: commit_behavior = periodic(1000)");
                return EINVAL;
            }

            conf->commit_behavior = atoi(options[0]);
            if (conf->commit_behavior == 0) {
                strcpy(msg_out,
                       "The argument for \"" LMGR_CONFIG_BLOCK
                       "::commit_behavior = periodic\" must be a positive integer. Eg: commit_behavior = periodic(1000)");
                return EINVAL;
            }
        } else {
            sprintf(msg_out,
                    "Invalid commit behavior '%s' (expected: autocommit, "
                    "transaction, periodic(<count>))", tmpstr);
            return EINVAL;
        }
    }

    /* manage deprecated parameters */
    rc = GetBoolParam(lmgr_block, LMGR_CONFIG_BLOCK, "user_acct", 0, &bval,
                      NULL, NULL, msg_out);
    if (rc == 0) {
        DisplayLog(LVL_CRIT, TAG,
                   "WARNING: parameter %s::%s' is deprecated. Specify 'accounting = yes/no' instead.",
                   LMGR_CONFIG_BLOCK, "user_acct");
        DisplayLog(LVL_MAJOR, TAG,
                   "Setting 'accounting = %s' for compatibility.",
                   bool2str(bval));
        conf->acct = bval;
    }

    rc = GetBoolParam(lmgr_block, LMGR_CONFIG_BLOCK, "group_acct", 0, &bval,
                      NULL, NULL, msg_out);
    if (rc == 0) {
        DisplayLog(LVL_CRIT, TAG,
                   "WARNING: parameter %s::%s' is deprecated. Specify 'accounting = yes/no' instead.",
                   LMGR_CONFIG_BLOCK, "group_acct");
        DisplayLog(LVL_MAJOR, TAG,
                   "Setting 'accounting = %s' for compatibility.",
                   bool2str(bval));
        conf->acct = bval;
    }

    CheckUnknownParameters(lmgr_block, LMGR_CONFIG_BLOCK, lmgr_allowed);

    /* Database parameters */
#ifdef _MYSQL
    /* get MySQL block */
    rc = get_cfg_block(config, LMGR_CONFIG_BLOCK "::" MYSQL_CONFIG_BLOCK,
                       &db_block, msg_out);
    if (rc)
        return rc;

    /* DB std params */
    rc = read_scalar_params(db_block, MYSQL_CONFIG_BLOCK, db_params, msg_out);
    if (rc)
        return rc;

    /* DB params with specific type */
    rc = GetStringParam(db_block, MYSQL_CONFIG_BLOCK, "password",
                        0, conf->db_config.password, 256, NULL, NULL, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    else if (rc == ENOENT) {
        FILE *passfile;
        char errstr[1024];

        rc = GetStringParam(db_block, MYSQL_CONFIG_BLOCK,
                            "password_file",
                            PFLG_ABSOLUTE_PATH | PFLG_NO_WILDCARDS,
                            tmpstr, sizeof(tmpstr), NULL, NULL, msg_out);
        if ((rc != 0) && (rc != ENOENT))
            return rc;
        else if (rc == ENOENT) {
            strcpy(msg_out,
                   MYSQL_CONFIG_BLOCK "::password or "
                   MYSQL_CONFIG_BLOCK "::password_file must be provided");
            return ENOENT;
        }

        /* read password file and @TODO check its rights */
        passfile = fopen(tmpstr, "r");
        if (!passfile) {
            rc = errno;
            sprintf(msg_out, "Error opening password file %s : %s", tmpstr,
                    strerror(errno));
            return rc;
        }
        fscanf(passfile, "%1024s", tmpstr);
        if (ferror(passfile)) {
            rc = errno;
            strerror_r(rc, errstr, 1024);
            sprintf(msg_out, "Error reading password file %s : %s", tmpstr,
                    errstr);
            return rc;
        }
        fclose(passfile);
        rh_strncpy(conf->db_config.password, tmpstr, 256);
    }

    CheckUnknownParameters(db_block, MYSQL_CONFIG_BLOCK, db_allowed);

#elif defined(_SQLITE)
    /* get SQLite block */
    rc = get_cfg_block(config, LMGR_CONFIG_BLOCK "::" SQLITE_CONFIG_BLOCK,
                       &db_block, msg_out);
    if (rc)
        return rc;

    rc = read_scalar_params(db_block, SQLITE_CONFIG_BLOCK, db_params, msg_out);
    if (rc)
        return rc;

    CheckUnknownParameters(db_block, SQLITE_CONFIG_BLOCK, db_allowed);
#endif

    return 0;
}

static int lmgr_cfg_reload(lmgr_config_t *conf)
{
    if (conf->commit_behavior != lmgr_config.commit_behavior)
        DisplayLog(LVL_MAJOR, TAG,
                   LMGR_CONFIG_BLOCK
                   "::commit_behavior changed in config file, but cannot be modified dynamically");

    if (conf->acct != lmgr_config.acct)
        DisplayLog(LVL_MAJOR, TAG,
                   LMGR_CONFIG_BLOCK
                   "::accounting changed in config file, but cannot be modified dynamically");

    if (conf->connect_retry_min != lmgr_config.connect_retry_min) {
        DisplayLog(LVL_EVENT, TAG,
                   LMGR_CONFIG_BLOCK
                   "::connect_retry_interval_min updated: %ld->%ld",
                   lmgr_config.connect_retry_min, conf->connect_retry_min);
        lmgr_config.connect_retry_min = conf->connect_retry_min;
    }

    if (conf->connect_retry_max != lmgr_config.connect_retry_max) {
        DisplayLog(LVL_EVENT, TAG,
                   LMGR_CONFIG_BLOCK
                   "::connect_retry_interval_max updated: %ld->%ld",
                   lmgr_config.connect_retry_max, conf->connect_retry_max);
        lmgr_config.connect_retry_max = conf->connect_retry_max;
    }
#ifdef _MYSQL

    if (strcmp(conf->db_config.server, lmgr_config.db_config.server))
        DisplayLog(LVL_MAJOR, TAG,
                   MYSQL_CONFIG_BLOCK
                   "::server changed in config file, but cannot be modified dynamically");
    if (strcmp(conf->db_config.db, lmgr_config.db_config.db))
        DisplayLog(LVL_MAJOR, TAG,
                   MYSQL_CONFIG_BLOCK
                   "::db changed in config file, but cannot be modified dynamically");
    if (strcmp(conf->db_config.user, lmgr_config.db_config.user))
        DisplayLog(LVL_MAJOR, TAG,
                   MYSQL_CONFIG_BLOCK
                   "::user changed in config file, but cannot be modified dynamically");
    if (strcmp(conf->db_config.password, lmgr_config.db_config.password))
        DisplayLog(LVL_MAJOR, TAG,
                   MYSQL_CONFIG_BLOCK
                   "::password changed in config file, but cannot be modified dynamically");
#elif defined(_SQLITE)
    if (strcmp(conf->db_config.filepath, lmgr_config.db_config.filepath))
        DisplayLog(LVL_MAJOR, TAG,
                   SQLITE_CONFIG_BLOCK
                   "::db_file changed in config file, but cannot be modified dynamically");

    if (conf->db_config.retry_delay_microsec !=
        lmgr_config.db_config.retry_delay_microsec) {
        DisplayLog(LVL_EVENT, TAG,
                   SQLITE_CONFIG_BLOCK "::retry_delay_microsec updated: %u->%u",
                   lmgr_config.db_config.retry_delay_microsec,
                   conf->db_config.retry_delay_microsec);
        lmgr_config.db_config.retry_delay_microsec =
            conf->db_config.retry_delay_microsec;
    }
#endif

    return 0;
}

static int lmgr_cfg_set(void *cfg, bool reload)
{
    lmgr_config_t *conf = (lmgr_config_t *) cfg;

    if (reload)
        return lmgr_cfg_reload(conf);

    lmgr_config = *conf;
    return 0;
}

static void lmgr_cfg_write_template(FILE *output)
{
    print_begin_block(output, 0, LMGR_CONFIG_BLOCK, NULL);

    print_line(output, 1, "# Method for committing information to database.");
    print_line(output, 1, "# Possible values are:");
    print_line(output, 1,
               "# - \"autocommit\": weak transactions (more efficient, but database inconsistencies may occur)");
    print_line(output, 1,
               "# - \"transaction\": manage operations in transactions (best consistency, lower performance)");
    print_line(output, 1,
               "# - \"periodic(<nb_transaction>)\": periodically commit (every <n> transactions).");
    print_line(output, 1, "commit_behavior = transaction ;");
    fprintf(output, "\n");
    print_line(output, 1,
               "# Minimum time (in seconds) to wait before trying to reestablish a lost connection.");
    print_line(output, 1,
               "# Then this time is multiplied by 2 until reaching connect_retry_interval_max");
    print_line(output, 1, "connect_retry_interval_min = 1 ;");
    print_line(output, 1, "connect_retry_interval_max = 30 ;");

    print_line(output, 1,
               "# disable the following options if you are not interested in");
    print_line(output, 1, "# user or group stats (to speed up scan)");
    print_line(output, 1, "accounting  = enabled ;");
    fprintf(output, "\n");
#ifdef _MYSQL
    print_begin_block(output, 1, MYSQL_CONFIG_BLOCK, NULL);
    print_line(output, 2, "server = \"localhost\" ;");
    print_line(output, 2, "db     = \"robinhood_db\" ;");
    print_line(output, 2, "user   = \"robinhood\" ;");
    print_line(output, 2, "password_file = \"/etc/robinhood.d/.dbpassword\" ;");
    print_line(output, 2, "# port   = 3306 ;");
    print_line(output, 2, "# socket = \"/tmp/mysql.sock\" ;");
    print_line(output, 2, "engine = InnoDB ;");
    print_end_block(output, 1);
#elif defined(_SQLITE)
    print_begin_block(output, 1, SQLITE_CONFIG_BLOCK, NULL);
    print_line(output, 2, "db_file = \"/var/robinhood/robinhood_sqlite_db\" ;");
    print_line(output, 2, "retry_delay_microsec = 1000 ;");
    print_end_block(output, 1);
#endif

    print_end_block(output, 0);
}

static void *lmgr_cfg_new(void)
{
    return calloc(1, sizeof(lmgr_config_t));
}

static void lmgr_cfg_free(void *cfg)
{
    if (cfg != NULL)
        free(cfg);
}

mod_cfg_funcs_t lmgr_cfg_hdlr = {
    .module_name = "list manager",
    .new = lmgr_cfg_new,
    .free = lmgr_cfg_free,
    .set_default = lmgr_cfg_set_default,
    .read = lmgr_cfg_read,
    .set_config = lmgr_cfg_set,
    .write_default = lmgr_cfg_write_default,
    .write_template = lmgr_cfg_write_template
};

bool lmgr_parallel_batches(void)
{
    return !lmgr_config.acct;
}
