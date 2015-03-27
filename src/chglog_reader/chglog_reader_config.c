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
 * \file    cl_reader_config.c
 * \author  Th. Leibovici
 * \brief   Configuration for Lustre MDT Changelog processing module.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "chglog_reader.h"
#include "rbh_misc.h"
#include "rbh_cfg.h"
#include "rbh_cfg_helpers.h"
#include "rbh_logs.h"
#include <pthread.h>
#include <errno.h>
#include <unistd.h>

#define TAG "ChgLogCfg" /* for logs */

#define CHGLOG_CFG_BLOCK    "ChangeLog"
#define MDT_DEF_BLOCK       "MDT"

chglog_reader_config_t cl_reader_config;

static mdt_def_t default_mdt_def =
    {
        .mdt_name  = "MDT0000",
        .reader_id = "cl1"
    };


/** Set changelog reader default configuration */
static void cl_reader_set_default_cfg(void *module_config)
{
   chglog_reader_config_t * p_config = (chglog_reader_config_t *) module_config;

   p_config->mdt_def = &default_mdt_def;
   p_config->mdt_count = 1;
   /* poll until changelog's follow flag is implemented in llapi */
   p_config->force_polling = true;
   p_config->polling_interval = 1; /* 1s */
   p_config->queue_max_size = 1000;
   p_config->queue_max_age = 5; /* 5s */
   p_config->queue_check_interval = 1; /* every second */
   p_config->mds_has_lu543 = false;
   p_config->mds_has_lu1331 = false;
   p_config->dump_file[0] = '\0'; /* no dump file */

   /* acknowledge 1024 records at once */
   p_config->batch_ack_count = 1024;
}

/** Write default parameters for changelog readers */
static void cl_reader_write_default(FILE *output)
{
    print_begin_block(output, 0, CHGLOG_CFG_BLOCK, NULL);
    print_begin_block(output, 1, MDT_DEF_BLOCK, NULL);
    print_line(output, 2, "mdt_name    :  \"%s\"", default_mdt_def.mdt_name);
    print_line(output, 2, "reader_id   :  \"%s\"", default_mdt_def.reader_id);
    print_end_block(output, 1);

    print_line(output, 1, "batch_ack_count  : 1024");
    print_line(output, 1, "force_polling    : yes");
    print_line(output, 1, "polling_interval : 1s");
    print_line(output, 1, "queue_max_size   : 1000");
    print_line(output, 1, "queue_max_age    : 5s");
    print_line(output, 1, "queue_check_interval : 1s");
    print_line(output, 1, "mds_has_lu543    : no");
    print_line(output, 1, "mds_has_lu1331   : no");
    print_line(output, 1, "dump_file        : (none)");

    print_end_block(output, 0);
}

/** Write a configuration template for changelog readers */
static void cl_reader_write_template(FILE *output)
{
    print_line( output, 0, "# Parameters for processing MDT changelogs :");
    print_begin_block( output, 0, CHGLOG_CFG_BLOCK, NULL );

	print_line( output, 1, "# 1 MDT block for each MDT :");
    print_begin_block( output, 1, MDT_DEF_BLOCK, NULL );

	print_line( output, 2, "# name of the first MDT");
    print_line( output, 2, "mdt_name  = \"MDT0000\" ;");
    fprintf( output, "\n" );
    print_line( output, 2, "# id of the persistent changelog reader");
    print_line( output, 2, "# as returned by \"lctl changelog_register\" command");
    print_line( output, 2, "reader_id = \"cl1\" ;" );

    print_end_block( output, 1 );

#ifdef HAVE_DNE
    fprintf( output, "\n" );
    print_line( output, 1, "# another MDT");
    print_begin_block( output, 1, MDT_DEF_BLOCK, NULL );
    print_line( output, 2, "mdt_name  = \"MDT0001\" ;");
    print_line( output, 2, "reader_id = \"cl1\" ;" );
    print_end_block( output, 1 );

    fprintf( output, "\n" );
    print_line( output, 1, "# yet another MDT");
    print_begin_block( output, 1, MDT_DEF_BLOCK, NULL );
    print_line( output, 2, "mdt_name  = \"MDT0002\" ;");
    print_line( output, 2, "reader_id = \"cl1\" ;" );
    print_end_block( output, 1 );
#endif

	print_line( output, 1, "# clear changelog every 1024 records:");
	print_line( output, 1, "batch_ack_count = 1024 ;");
    fprintf( output, "\n" );

    print_line(output, 1, "force_polling    = yes ;");
    print_line(output, 1, "polling_interval = 1s ;");
    print_line(output, 1, "queue_max_size   = 1000 ;");
    print_line(output, 1, "queue_max_age    = 5s ;");
    print_line(output, 1, "queue_check_interval = 1s ;");
    fprintf(output, "\n");

    print_line(output, 1, "# uncomment to dump all changelog records to the file");
    print_line(output, 1, "#dump_file = \"/var/log/robinhood/changelog_dump.log\";");

    print_end_block( output, 0 );
}

#define critical_err_check(_ptr_, _blkname_) do { if (!_ptr_) {\
                                        sprintf( msg_out, "Internal error reading %s block in config file", _blkname_); \
                                        return EFAULT; \
                                    }\
                                } while (0)

static int parse_mdt_block( config_item_t config_blk, const char *block_name,
                             mdt_def_t * p_mdt_def, char *msg_out )
{
    char *str;
    bool  unique;

    /* 2 variables expected : 'mdt_name' and 'reader_id' */
    static const char *expected_vars[] = { "mdt_name","reader_id", NULL };

    /* get 'mdt_name' value */
    unique = true;
    str = rh_config_GetKeyValueByName(config_blk, "mdt_name", &unique);
    if (str == NULL)
    {
        DisplayLog(LVL_CRIT, "ChgLog config", "WARNING: no 'mdt_name' provided in %s block: using default value '%s'",
                   block_name, default_mdt_def.mdt_name);
        strcpy(p_mdt_def->mdt_name, default_mdt_def.mdt_name);
    }
    else if (!unique)
    {
        sprintf(msg_out, "Found duplicate parameter '%s' in %s.\n", "mdt_name", block_name);
        return EEXIST;
    }
    else if (strlen(str) >= MDT_NAME_MAX)
    {
        sprintf(msg_out, "MDT name '%s' is too long (max length=%u)", str, MDT_NAME_MAX);
        return ENAMETOOLONG;
    }
    else if (strncmp("MDT", str, 3) != 0)
    {
        sprintf(msg_out, "Invalid MDT name '%s'. \"MDT<index>\" expected", str);
        return EINVAL;
    }
    else
    {
        strcpy(p_mdt_def->mdt_name, str);
    }

    /* get 'reader_id' value */
    unique = true;
    str = rh_config_GetKeyValueByName(config_blk, "reader_id", &unique);
    if (str == NULL)
    {
        DisplayLog(LVL_CRIT, "ChgLog config", "WARNING: no 'reader_id' provided in %s block: using default value '%s'",
                   block_name, default_mdt_def.reader_id);
        strcpy(p_mdt_def->reader_id, default_mdt_def.reader_id);
    }
    else if (!unique)
    {
        sprintf(msg_out, "Found duplicate parameter '%s' in %s.\n", "reader_id", block_name);
        return EEXIST;
    }
    else if (strlen(str) >= MDT_NAME_MAX)
    {
        sprintf(msg_out, "Client id '%s' is too long (max length=%u)", str, READER_ID_MAX);
        return ENAMETOOLONG;
    }
    else
    {
        strcpy(p_mdt_def->reader_id, str);
    }

    /* display warnings for unknown parameters */
    CheckUnknownParameters(config_blk, block_name, expected_vars);

    return 0;
}

/** Read configuration for changelog readers */
static int cl_reader_read_cfg(config_file_t config, void *module_config,
                              char *msg_out)
{
    chglog_reader_config_t *p_config = (chglog_reader_config_t *)module_config;
    config_item_t  chglog_block;
    unsigned int   blc_index;
    int            rc;

    static const char *cl_cfg_allow[] =
    {
        "force_polling", "polling_interval", "batch_ack_count",
        "queue_max_size", "queue_max_age", "queue_check_interval",
        "mds_has_lu543", "mds_has_lu1331", MDT_DEF_BLOCK,
        NULL
    };

    const cfg_param_t cfg_params[] = {
        {"force_polling", PT_BOOL, 0, &p_config->force_polling, 0},
        {"polling_interval", PT_DURATION, PFLG_NOT_NULL|PFLG_POSITIVE,
            &p_config->polling_interval, 0},
        {"batch_ack_count",     PT_INT, PFLG_NOT_NULL|PFLG_POSITIVE,
            &p_config->batch_ack_count, 0},
        {"queue_max_size",      PT_INT, PFLG_NOT_NULL|PFLG_POSITIVE,
            &p_config->queue_max_size, 0},
        {"queue_max_age",       PT_DURATION, PFLG_NOT_NULL|PFLG_POSITIVE,
            &p_config->queue_max_age, 0},
        {"queue_check_interval",PT_DURATION, PFLG_NOT_NULL|PFLG_POSITIVE,
            &p_config->queue_check_interval, 0},
        {"mds_has_lu543", PT_BOOL, 0, &p_config->mds_has_lu543, 0},
        {"mds_has_lu1331", PT_BOOL, 0, &p_config->mds_has_lu1331, 0},
        {"dump_file", PT_STRING, PFLG_ABSOLUTE_PATH | PFLG_NO_WILDCARDS,
            p_config->dump_file, sizeof(p_config->dump_file)},
        END_OF_PARAMS
    };


    /* get ChangeLog  block */
    rc = get_cfg_block(config, CHGLOG_CFG_BLOCK, &chglog_block, msg_out);
    if (rc)
        return rc == ENOENT ? 0 : rc; /* not mandatory */

    /* get scalar params */
    rc = read_scalar_params(chglog_block, CHGLOG_CFG_BLOCK, cfg_params, msg_out);
    if (rc)
        return rc;

    /* browse  the list of MDT blocks */
    for (blc_index = 0; blc_index < rh_config_GetNbItems(chglog_block); blc_index++)
    {
        char          *block_name;
        config_item_t  curr_item = rh_config_GetItemByIndex(chglog_block, blc_index);
        critical_err_check(curr_item, CHGLOG_CFG_BLOCK);

        if (rh_config_ItemType(curr_item) != CONFIG_ITEM_BLOCK)
            continue;

        block_name = rh_config_GetBlockName(curr_item);
        critical_err_check(curr_item, CHGLOG_CFG_BLOCK);

        if (!strcasecmp(block_name, MDT_DEF_BLOCK))
        {
            /* allocate a new mdt_definition  */

            if ((p_config->mdt_def==NULL) || (p_config->mdt_def == &default_mdt_def))
            {
                p_config->mdt_count = 1;

                /* no MDT definition, or MDT definition was the default */
                p_config->mdt_def = (mdt_def_t*)malloc(sizeof(mdt_def_t));
                if (!p_config->mdt_def) return ENOMEM;
            }
            else
            {
               p_config->mdt_count ++;

               p_config->mdt_def = (mdt_def_t*)realloc(p_config->mdt_def,
                                     p_config->mdt_count * sizeof(mdt_def_t));
               if (!p_config->mdt_def) return ENOMEM;
            }

            /* fill the structure */
            rc = parse_mdt_block(curr_item, MDT_DEF_BLOCK, &p_config->mdt_def[p_config->mdt_count-1], msg_out);
            if (rc) return rc;
        }
        else
        {
            sprintf(msg_out, "Unknown sub-block '%s' in " CHGLOG_CFG_BLOCK " block, line %d",
                     block_name, rh_config_GetItemLine(curr_item));
            return EINVAL;
        }
    }

    CheckUnknownParameters(chglog_block, CHGLOG_CFG_BLOCK, cl_cfg_allow);

#ifdef _DEBUG_CHGLOG
    printf("%u MDT definitions parsed successfully, ptr = %p\n", p_config->mdt_count,  p_config->mdt_def);
#endif

    return 0;
}

#define NO_PARAM_UPDT_MSG(_blk, _name) DisplayLog(LVL_MAJOR, TAG, "%s::%s"     \
                " changed in config file, but cannot be modified dynamically", \
                 _blk, _name)
#define PARAM_UPDT_MSG(_blk, _name, _format, _v1, _v2) DisplayLog(LVL_EVENT,  \
                TAG, "%s::%s updated: "_format"->"_format, _blk, _name, _v1, _v2)

#define SCALAR_PARAM_UPDT(_cfg, _val_field, _blk, _name, _format, _format_func) do { \
        if ((_cfg)->_val_field != cl_reader_config._val_field) { \
            PARAM_UPDT_MSG(_blk, _name, _format, _format_func(cl_reader_config._val_field), \
                          _format_func((_cfg)->_val_field)); \
            cl_reader_config._val_field = (_cfg)->_val_field; \
        } \
} while(0)

/** reload parameters for a single policy */
static int cl_reader_reload_cfg(chglog_reader_config_t *cfg)
{
    SCALAR_PARAM_UPDT(cfg, force_polling, CHGLOG_CFG_BLOCK, "force_polling", "%s", bool2str);
    SCALAR_PARAM_UPDT(cfg, polling_interval, CHGLOG_CFG_BLOCK, "polling_interval", "%ld", );
    SCALAR_PARAM_UPDT(cfg, batch_ack_count, CHGLOG_CFG_BLOCK, "batch_ack_count", "%u", );
    SCALAR_PARAM_UPDT(cfg, queue_max_size, CHGLOG_CFG_BLOCK, "queue_max_size", "%u", );
    SCALAR_PARAM_UPDT(cfg, queue_max_age, CHGLOG_CFG_BLOCK, "queue_max_age", "%ld", );
    SCALAR_PARAM_UPDT(cfg, queue_check_interval, CHGLOG_CFG_BLOCK, "queue_check_interval", "%ld", );

    if (cfg->mds_has_lu543 != cl_reader_config.mds_has_lu543)
        NO_PARAM_UPDT_MSG(CHGLOG_CFG_BLOCK, "mds_has_lu543");
    if (cfg->mds_has_lu1331 != cl_reader_config.mds_has_lu1331)
        NO_PARAM_UPDT_MSG(CHGLOG_CFG_BLOCK, "mds_has_lu1331");

    if (cfg->mdt_count != cl_reader_config.mdt_count)
        NO_PARAM_UPDT_MSG(CHGLOG_CFG_BLOCK, MDT_DEF_BLOCK" count");
    else
    {
        int i;

        for (i = 0; i < cfg->mdt_count; i++)
        {
            if (strcmp(cfg->mdt_def[i].mdt_name, cl_reader_config.mdt_def[i].mdt_name))
                NO_PARAM_UPDT_MSG(CHGLOG_CFG_BLOCK"::"MDT_DEF_BLOCK, "mdt_name");
            if (strcmp(cfg->mdt_def[i].reader_id, cl_reader_config.mdt_def[i].reader_id))
                NO_PARAM_UPDT_MSG(CHGLOG_CFG_BLOCK"::"MDT_DEF_BLOCK, "reader_id");
        }
    }

    return 0;
}



static int cl_reader_cfg_set(void *arg,  bool reload)
{
    chglog_reader_config_t *cfg = (chglog_reader_config_t*)arg;

    if (reload)
        return cl_reader_reload_cfg(cfg);
    else
        cl_reader_config = *cfg;
    return 0;
}

static void *cl_reader_cfg_new(void)
{
    return calloc(1, sizeof(chglog_reader_config_t));
}

static void cl_reader_cfg_free(void *arg)
{
    chglog_reader_config_t *cfg = (chglog_reader_config_t *)arg;

    if ((cfg->mdt_def != NULL) && (cfg->mdt_def != &default_mdt_def))
        free(cfg->mdt_def);
    free(cfg);
}

/** config handling functions */
mod_cfg_funcs_t cl_reader_cfg_hdlr = {
    .module_name = "changelog reader",
    .new = cl_reader_cfg_new,
    .free = cl_reader_cfg_free,
    .set_default = cl_reader_set_default_cfg,
    .read = cl_reader_read_cfg,
    .set_config = cl_reader_cfg_set,
    .write_default = cl_reader_write_default,
    .write_template = cl_reader_write_template
};
