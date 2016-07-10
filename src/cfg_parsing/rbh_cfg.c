/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2008, 2009, 2010 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
/**
 *  Module for configuration management and parsing.
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "rbh_cfg.h"
#include "rbh_misc.h"
#include "rbh_logs.h"
#include "analyze.h"
#include <errno.h>

/* get config handlers definition */
#include "global_config.h"
#include "update_params.h"
#include "entry_processor.h"
#include "fs_scan_main.h"
#include "chglog_reader.h"
#include "policy_rules.h"
#include "policy_run.h"
#include "status_manager.h"

char config_file[RBH_PATH_MAX] = "";

struct mod_cfgs {
    mod_cfg_funcs_t *funcs;
    int flags;
} cfglist[] = {
    {&global_cfg_hdlr,     MODULE_MASK_ALWAYS},
    {&log_cfg_hdlr,        MODULE_MASK_ALWAYS},
    {&updt_params_hdlr,    MODULE_MASK_ALWAYS},
    {&lmgr_cfg_hdlr,       MODULE_MASK_ALWAYS},
    {&entry_proc_cfg_hdlr, MODULE_MASK_ENTRY_PROCESSOR},
    {&fs_scan_cfg_hdlr,    MODULE_MASK_FS_SCAN},
#ifdef HAVE_CHANGELOGS
    {&cl_reader_cfg_hdlr,  MODULE_MASK_EVENT_HDLR},
#endif
    /* TODO manage fileclasses separately? */
    /* must always be called before smi_cfg, run_cfg */
    {&policies_cfg_hdlr,   MODULE_MASK_ALWAYS},
    {&smi_cfg_hdlr,        MODULE_MASK_ALWAYS},
    {&policy_run_cfg_hdlr, MODULE_MASK_POLICY_RUN},

    {NULL, 0}
};

#define RELOAD_TAG "ReloadCfg"

static int rbh_cfg_read_set(int module_mask, char *file_path, char *err_msg_out,
                            bool reload)
{
    config_file_t syntax_tree;
    int           rc, rc_final = 0;
    char          msg_buf[2048] = "";
    const struct mod_cfgs *p_curr;

    /* First, Parse the configuration file */
    syntax_tree = rh_config_ParseFile(file_path);

    if (syntax_tree == NULL) {
        strcpy(err_msg_out, rh_config_GetErrorMsg());
        return EINVAL;
    }
#ifdef _DEBUG_PARSING
    rh_config_Print(stdout, syntax_tree);
#endif

    /* Set defaults to the structure, then load values from syntax tree */
    for (p_curr = &cfglist[0]; p_curr->funcs != NULL; p_curr++) {
        void *cfg;

        /* Only initialize modules with flag MODULE_MASK_ALWAYS
         * or matching 'module_mask' parameter. */
        if ((p_curr->flags != MODULE_MASK_ALWAYS)
            && !(p_curr->flags & module_mask))
            continue;

        cfg = p_curr->funcs->new();
        if (cfg == NULL) {
            rc_final = ENOMEM;
            sprintf(err_msg_out,
                    "Not enough memory to allocate configuration for %s",
                    p_curr->funcs->module_name);
            if (reload) {
                DisplayLog(LVL_CRIT, RELOAD_TAG, "%s", err_msg_out);
                continue;
            } else
                goto config_free;
        }

        p_curr->funcs->set_default(cfg);

        DisplayLog(LVL_DEBUG, "CfgLoader", "Loading %s config",
                   p_curr->funcs->module_name);

        rc = p_curr->funcs->read(syntax_tree, cfg, msg_buf);
        if (rc != 0) {
            rc_final = rc;
            sprintf(err_msg_out,
                    "Error %d reading %s configuration:\n%s",
                    rc, p_curr->funcs->module_name, msg_buf);

            if (reload) {
                DisplayLog(LVL_CRIT, RELOAD_TAG, "%s", err_msg_out);
                continue;
            } else
                goto config_free;
        }

        rc = p_curr->funcs->set_config(cfg, reload);
        if (rc != 0) {
            rc_final = rc;
            sprintf(err_msg_out,
                    "Error %d setting %s configuration:\n%s",
                    rc, p_curr->funcs->module_name, msg_buf);
            p_curr->funcs->free(cfg);

            if (reload) {
                DisplayLog(LVL_CRIT, RELOAD_TAG, "%s", err_msg_out);
                continue;
            } else
                goto config_free;
        }
        /* When reloading, the configuration can be freed
         * as each module copy new values in the old config structure.
         */
        if (reload)
            p_curr->funcs->free(cfg);
        else
            /* just free the top level handler */
            free(cfg);
    }

 config_free:
    /* free config file resources */
    rh_config_Free(syntax_tree);

    return rc_final;
}

/**
 * Read robinhood's configuration file and fill config struct.
 * if everything is OK, returns 0 and fills the structure
 * else, returns an error code and sets a contextual error message
 * in err_msg_out.
 */
int rbh_cfg_load(int module_mask, char *file_path, char *err_msg_out)
{
    rh_strncpy(config_file, file_path, sizeof(config_file));
    return rbh_cfg_read_set(module_mask, file_path, err_msg_out, false);
}

/**
 * Reload robinhood's configuration file (the one used for last call to rbh_cfg_load())
 * and change only parameters that can be modified on the fly.
 */
int rbh_cfg_reload(int curr_module_mask)
{
    char tmp[2048];

    return rbh_cfg_read_set(curr_module_mask, config_file, tmp, true);
}

/* returns the path to process config file */
const char *config_file_path(void)
{
    return config_file;
}

/**
 * Write a documented template of configuration file.
 * returns 0 on success, else it returns a posix error code.
 */
int rbh_cfg_write_template(FILE *stream)
{
    const struct mod_cfgs *p_module;

    fprintf(stream, "##########################################\n");
    fprintf(stream, "# Robinhood configuration file template  #\n");
    fprintf(stream, "##########################################\n\n");

    for (p_module = &cfglist[0]; p_module->funcs != NULL; p_module++) {
        fprintf(stream, "# %s configuration\n", p_module->funcs->module_name);
        p_module->funcs->write_template(stream);
        fprintf(stream, "\n");
    }

    return 0;
}

int rbh_cfg_write_default(FILE *stream)
{
    const struct mod_cfgs *p_module;

    fprintf(stream, "# Default configuration values\n");

    for (p_module = &cfglist[0]; p_module->funcs != NULL; p_module++) {
        p_module->funcs->write_default(stream);
        fprintf(stream, "\n");
    }

    return 0;
}

#define INDENT_STEP 4
void print_begin_block(FILE *output, unsigned int indent,
                       const char *blockname, const char *id)
{
    char *indent_char = (indent ? " " : "");

    if (id)
        fprintf(output, "%*s%s\t%s\n", indent * INDENT_STEP, indent_char,
                blockname, id);
    else
        fprintf(output, "%*s%s\n", indent * INDENT_STEP, indent_char,
                blockname);
    fprintf(output, "%*s{\n", indent * INDENT_STEP, indent_char);
}

void print_end_block(FILE *output, unsigned int indent)
{
    char *indent_char = (indent ? " " : "");
    fprintf(output, "%*s}\n", indent * INDENT_STEP, indent_char);
}

void print_line(FILE *output, unsigned int indent, const char *format, ...)
{
    va_list arglist;
    char *indent_char = (indent ? " " : "");

    fprintf(output, "%*s", indent * INDENT_STEP, indent_char);

    va_start(arglist, format);
    vfprintf(output, format, arglist);
    va_end(arglist);

    fprintf(output, "\n");
}
