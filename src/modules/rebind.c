/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2017 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

/**
 * \file rebind.c
 * \brief Rebind files to HPSS entries, assign a FUID and set the right UDA.
 *
 * The purpose of this module is to help transitioning from a system where files
 * are not identified by a FUID to one where they are.
 *
 * It maintains a simple {ok/invalid} status for each file.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "status_manager.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "mod_internal.h"

#include <lhsm_hpss_api.h>

#define MOD_NAME    "rebind"


/** set of managed status */
typedef enum {
    STATUS_OK       = 0,    /* checked and access mode is OK */
    STATUS_INVALID  = 1,    /* checked and access mode is invalid */

    STATUS_COUNT    = 2,    /* number of possible statuses */
} rebind_status_t;

static const char *rebind_status_list[] = {
    [STATUS_OK]      = "ok",
    [STATUS_INVALID] = "invalid",
};

#define CT_CONFIG_PARAM "hpss_copytool_config"

#define REBIND_DEFAULT_HPSS_CT_CONFIG   "/var/hpss/etc/lhsm.conf"

/** XX Consider making this a tunable */
#define MAX_AUTH_LIFE_SEC   3600


typedef struct {
    char *hpss_ct_config;
} rebind_config_t;


/** Global configuration shared between instances */
static rebind_config_t Config;

/**
 * Thread-local variable to indicate whether we acquired HPSS credentials.
 */
struct hpss_ct_context {
    void            *handle;
    struct timespec  birthdate;
};

static __thread struct hpss_ct_context *HpssAuthCtx;


static bool context_is_obsolete(void)
{
    struct timespec now;
    int rc;

    assert(HpssAuthCtx);

    rc = clock_gettime(CLOCK_MONOTONIC_COARSE, &now);
    assert(rc == 0); /* see man page for why we can assume zero */

    return (now.tv_sec - HpssAuthCtx->birthdate.tv_sec > MAX_AUTH_LIFE_SEC);
}

static bool context_is_valid(void)
{
    return (HpssAuthCtx && !context_is_obsolete());
}

/**
 * Destroy local context so as to let a new one be instanciated 
 */
static void hpss_deauthenticate(void)
{
    if (!HpssAuthCtx)
        return;

    lhsm_hpss_cleanup(HpssAuthCtx->handle);
    HpssAuthCtx = NULL;
}

/**
 * Get a fresh auth context.
 */
static int hpss_authenticate(void)
{
    struct lhsm_hpss_config *ct_cfg;
    struct hpss_ct_context *new_ctx;
    int rc;

    if (context_is_valid())
        return 0;

    if (context_is_obsolete())
        hpss_deauthenticate();

    new_ctx = malloc(sizeof(*new_ctx));
    if (!new_ctx) {
        DisplayLog(LVL_MAJOR, MOD_NAME, "Cannot allocate memory for HPSS auth");
        return -ENOMEM;
    }

    rc = lhsm_hpss_read_cfg(Config.hpss_ct_config, &ct_cfg);
    if (rc) {
        DisplayLog(LVL_MAJOR, MOD_NAME,
               "Cannot load HPSS copytool config file at '%s': %s",
               Config.hpss_ct_config, strerror(-rc));
        goto err_ctx;
    }

    rc = lhsm_hpss_init(&new_ctx->handle, ct_cfg, get_fsname());
    if (rc) {
        DisplayLog(LVL_MAJOR, MOD_NAME,
                   "Cannot initialize HPSS copytool API: %s", strerror(-rc));
        goto err_cfg;
    }

    rc = clock_gettime(CLOCK_MONOTONIC_COARSE, &new_ctx->birthdate);
    assert(rc == 0);

    HpssAuthCtx = new_ctx;

err_ctx:
    if (rc)
        free(new_ctx);

err_cfg:
    if (rc)
        lhsm_hpss_free_cfg(ct_cfg); /* else: let it leak and be freed at exit */

    return rc;
}

/** the only action supported by rebind */
static int rebind_set_fuid(const entry_id_t *p_entry_fid, attr_set_t *p_attrs,
                           const action_params_t *params, post_action_e *after,
                           db_cb_func_t db_cb_fn, void *db_cb_arg)
{
    const char *path;
    const char *hpss_path;
    int rc;

    if (!ATTR_MASK_TEST(p_attrs, fullpath)) {
        DisplayLog(LVL_MAJOR, MOD_NAME,
                   DFID ": missing 'fullpath' attr for checking entry status",
                   PFID(p_entry_fid));
        return -EINVAL;
    }

    rc = hpss_authenticate();
    if (rc)
        return rc;

    path = ATTR(p_attrs, fullpath);
    hpss_path = rbh_param_get(params, "backend_path");

    rc = lhsm_hpss_bind(HpssAuthCtx->handle, hpss_path, path, 0);
    if (rc)
        DisplayLog(LVL_MAJOR, MOD_NAME,
                   "Unable to rebind file '%s' ("DFID") to backend path '%s'",
                   path, PFID(p_entry_fid), hpss_path);

    return rc;
}

static void *rebind_cfg_new(void)
{
    return calloc(1, sizeof(rebind_config_t));
}

static void rebind_cfg_free(void *ptr)
{
    rebind_config_t *cfg = ptr;

    if (!cfg)
        return;

    free(cfg->hpss_ct_config);
    free(cfg);
}

static void rebind_cfg_set_default(void *module_config)
{
    rebind_config_t *conf = module_config;

    conf->hpss_ct_config = strdup(REBIND_DEFAULT_HPSS_CT_CONFIG);
}

static int rebind_cfg_set(void *cfg, bool reload)
{
    rebind_config_t *new = cfg;

    if (!reload) {
        Config = *new;
        return 0;
    }

    if (strcmp(Config.hpss_ct_config, new->hpss_ct_config))
        DisplayLog(LVL_MAJOR, MOD_NAME,
                   "%s::hpss_ct_config cannot be changed once set", MOD_NAME);
    return 0;
}

static void rebind_cfg_write_default(FILE *output)
{
    print_begin_block(output, 0, MOD_NAME, NULL);
    print_line(output, 1, "hpss_ct_config: \"%s\"",
               REBIND_DEFAULT_HPSS_CT_CONFIG);
    print_end_block(output, 0);
}

#define MAX_PARAMS_LEN  1024
static int rebind_cfg_read(config_file_t config, void *module_config, char *out)
{
    rebind_config_t *conf = module_config;
    char ct_config_buff[MAX_PARAMS_LEN] = "";
    config_item_t block;
    int rc;

    const cfg_param_t rebind_params[] = {
        {CT_CONFIG_PARAM, PT_STRING, 0, ct_config_buff, sizeof(ct_config_buff)},
        END_OF_PARAMS
    };

    static const char *allowed_params[] = {CT_CONFIG_PARAM, NULL};

    /* get rebind_config block */
    rc = get_cfg_block(config, MOD_NAME, &block, out);
    if (rc)
        return rc == ENOENT ? 0 : rc;

    /* read parameters */
    rc = read_scalar_params(block, MOD_NAME, rebind_params, out);
    if (rc)
        return rc;

    CheckUnknownParameters(block, MOD_NAME, allowed_params);

    if (*ct_config_buff)
        conf->hpss_ct_config = strdup(ct_config_buff);

    /* Simple check to make sure the file exists and is readable - do not abort
     * if it's not but issue a warning accordingly */
    rc = access(conf->hpss_ct_config, R_OK);
    if (rc == -1)
        DisplayLog(LVL_EVENT, MOD_NAME, "Cannot access config file at '%s'",
                   conf->hpss_ct_config);

    return 0;
}


static const mod_cfg_funcs_t rebind_cfg_hdlr = {
    .module_name    = MOD_NAME,
    .new            = rebind_cfg_new,
    .free           = rebind_cfg_free,
    .set_default    = rebind_cfg_set_default,
    .read           = rebind_cfg_read,
    .set_config     = rebind_cfg_set,
    .write_default  = rebind_cfg_write_default,
    .write_template = rebind_cfg_write_default,
};

/** Status manager for rebind */
static status_manager_t rebind_sm = {
    .name         = MOD_NAME,
    .status_enum  = rebind_status_list,
    .status_count = STATUS_COUNT,
    .cfg_funcs    = &rebind_cfg_hdlr,
};

/* ======= PUBLIC FUNCTIONS ======= */
const char *mod_get_name(void)
{
    return rebind_sm.name;
}

status_manager_t *mod_get_status_manager(void)
{
    return &rebind_sm;
}

action_func_t mod_get_action(const char *action_name)
{
    if (strcmp(action_name, "rebind.set_fuid") == 0)
        return rebind_set_fuid;
    else
        return NULL;
}
