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

#define MOD_NAME            "hpss_rebind"

#define HPSS_REBIND_BLOCK   "hpss_rebind_config"

int hpss_clnt_thread_init(void);

/** set of managed status */
typedef enum {
    STATUS_OK     = 0,
    STATUS_FAILED = 1,

    STATUS_COUNT  = 2,
} rebind_status_t;

static const char *rebind_status_list[] = {
    [STATUS_OK]     = "ok",
    [STATUS_FAILED] = "failed",
};

#define CT_CONFIG_PARAM     "hpss_copytool_config"
#define REBIND_DEFAULT_CT_CONFIG    "/var/hpss/etc/lhsm.conf"

#define MAX_PARAMS_LEN  1024
typedef struct {
    char hpss_ct_config[MAX_PARAMS_LEN];
} rebind_config_t;


/** Global configuration shared between instances */
static rebind_config_t Config;

/**
 * Global (shared) HPSS client state.
 */
struct hpss_global_ctx {
    void            *handle;
};

static struct hpss_global_ctx   HpssGlobalHandle;
static __thread bool            ThreadIsInitialized;


/**
 * Initialize current thread if needed.
 */
static int hpss_authenticate(void)
{
    int rc;

    if (ThreadIsInitialized)
        return 0;

    rc = hpss_clnt_thread_init();
    if (rc) {
        DisplayLog(LVL_MAJOR, MOD_NAME, "Cannot authenticate to HPSS: %s",
                   strerror(-rc));
        return rc;
    }

    ThreadIsInitialized = true;
    return 0;
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
                   DFID ": missing 'fullpath' attr for setting FUID",
                   PFID(p_entry_fid));
        return -EINVAL;
    }

    rc = hpss_authenticate();
    if (rc)
        return rc;

    path = ATTR(p_attrs, fullpath);

    hpss_path = rbh_param_get(params, "backend_path");
    if (!hpss_path) {
        DisplayLog(LVL_MAJOR, MOD_NAME, "Missing parameter 'backend_path'");
        return -EINVAL;
    }

    /* bind in-place (no symlink or rename)*/
    rc = lhsm_hpss_bind(HpssGlobalHandle.handle, hpss_path, path,
                        IMPORT_FLG_NO_RENAME);
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

    strncpy(conf->hpss_ct_config, REBIND_DEFAULT_CT_CONFIG,
            sizeof(conf->hpss_ct_config));
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
    print_begin_block(output, 0, HPSS_REBIND_BLOCK, NULL);
    print_line(output, 1, "%s: \"%s\"",
               CT_CONFIG_PARAM, REBIND_DEFAULT_CT_CONFIG);
    print_end_block(output, 0);
}

static int rebind_cfg_read(config_file_t config, void *module_config, char *out)
{
    rebind_config_t *conf = module_config;
    char ct_config_buf[MAX_PARAMS_LEN] = "";
    config_item_t block;
    int rc;

    const cfg_param_t rebind_params[] = {
        {CT_CONFIG_PARAM, PT_STRING, 0, ct_config_buf, sizeof(ct_config_buf)},
        END_OF_PARAMS
    };

    static const char *allowed_params[] = {
        CT_CONFIG_PARAM,
        NULL
    };

    /* get rebind_config block */
    rc = get_cfg_block(config, HPSS_REBIND_BLOCK, &block, out);
    if (rc)
        return rc == ENOENT ? 0 : rc;

    /* read parameters */
    rc = read_scalar_params(block, HPSS_REBIND_BLOCK, rebind_params, out);
    if (rc)
        return rc;

    CheckUnknownParameters(block, HPSS_REBIND_BLOCK, allowed_params);

    if (*ct_config_buf)
        strncpy(conf->hpss_ct_config, ct_config_buf,
                sizeof(conf->hpss_ct_config));

    /* Simple check to make sure the file exists and is readable - do not abort
     * if it's not but issue a warning accordingly */
    rc = access(conf->hpss_ct_config, R_OK);
    if (rc == -1)
        DisplayLog(LVL_EVENT, MOD_NAME, "Cannot access config file at '%s'",
                   conf->hpss_ct_config);

    return 0;
}

static int rebind_init(struct sm_instance *smi, run_flags_t flags)
{
    struct lhsm_hpss_config *ct_cfg;
    int rc;

    rc = lhsm_hpss_read_cfg(Config.hpss_ct_config, &ct_cfg);
    if (rc) {
        DisplayLog(LVL_MAJOR, MOD_NAME,
               "Cannot load HPSS copytool config file at '%s': %s",
               Config.hpss_ct_config, strerror(-rc));
        return rc;
    }

    rc = lhsm_hpss_init(&HpssGlobalHandle.handle, ct_cfg, get_fsname());
    if (rc) {
        DisplayLog(LVL_MAJOR, MOD_NAME,
                   "Cannot initialize HPSS copytool API: %s", strerror(-rc));
        goto err_cfg;
    }

err_cfg:
    if (rc)
        lhsm_hpss_free_cfg(ct_cfg); /* else: let it live and be freed at exit */

    return rc;
}

/** set status according to action return status */
static int rebind_action_cb(struct sm_instance *smi, const char *implements,
                            int action_status, const entry_id_t *id,
                            attr_set_t *p_attrs, post_action_e *what_after)
{
    if (action_status == 0)
        set_status_attr(smi, p_attrs, rebind_status_list[STATUS_OK]);
    else
        set_status_attr(smi, p_attrs, rebind_status_list[STATUS_FAILED]);

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
    .init_func    = rebind_init,
    .action_cb    = rebind_action_cb,
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
    if (strcmp(action_name, "hpss_rebind.set_fuid") == 0)
        return rebind_set_fuid;
    else
        return NULL;
}
