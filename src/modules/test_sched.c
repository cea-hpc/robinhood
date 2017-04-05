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
 * This module contains dummy schedulers for testing.
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "mod_internal.h"
#include "policy_run.h"

#include <unistd.h>

/** scheduler configuration */
typedef struct sched_sleep_config {
    unsigned int    sleep_time;
} sched_sleep_config_t;

struct sched_sleep_state {
    sched_sleep_config_t cfg;
};

static int sched_sleep_init(void *config, void **p_sched_data)
{
    struct sched_sleep_state *state = calloc(1, sizeof(*state));
    sched_sleep_config_t *cfg = config;

    if (!config)
        return -EINVAL;

    if (!state)
        return -ENOMEM;

    state->cfg = *cfg;
    *p_sched_data = state;
    return 0;
}

static int sched_sleep_reset(void *sched_data)
{
    return 0;
}

static int sched_sleep_schedule(void *sched_data, const entry_id_t *id,
                              const attr_set_t *attrs, sched_cb_t cb,
                              void *udata)
{
    struct sched_sleep_state *state = sched_data;

    if (state->cfg.sleep_time > 0)
        sleep(state->cfg.sleep_time);

    cb(udata, SCHED_OK);
    return 0;
}

/* ------------- configuration management functions ---------- */

/** configuration block name for max_per_run scheduler */
#define SCHED_SLEEP_BLOCK "sleeper"

static void *sched_sleep_cfg_new(void)
{
    return calloc(1, sizeof(sched_sleep_config_t));
}

static void sched_sleep_cfg_free(void *cfg)
{
    free(cfg);
}

static void sched_sleep_cfg_set_default(void *module_config)
{
    sched_sleep_config_t *conf = module_config;

    conf->sleep_time = 1;
}

static void sched_sleep_cfg_write_default(int indent, FILE *output)
{
    print_begin_block(output, indent, SCHED_SLEEP_BLOCK, NULL);
    print_line(output, indent + 1, "sleep_time: 1s");
    print_end_block(output, indent);
}

static void sched_sleep_cfg_write_template(int indent, FILE *output)
{
    print_begin_block(output, indent, SCHED_SLEEP_BLOCK, NULL);
    print_line(output, indent + 1, "# sleep time (sec)");
    print_line(output, indent + 1, "sleep_time = 1;");
    print_end_block(output, indent);
}

/** get a 'sleeper' sublock from the policy parameters */
static int sched_sleep_cfg_read_from_block(config_item_t parent, void *cfg,
                                         char *msg_out)
{
    int rc;
    sched_sleep_config_t *conf = cfg;
    config_item_t       block;

    const cfg_param_t sleep_params[] = {
        {"sleep_time", PT_DURATION, PFLG_POSITIVE, &conf->sleep_time, 0},
        END_OF_PARAMS
    };

    static const char * const allowed_params[] = {
        "sleep_time", NULL
    };

    /* get 'sleeper' subblock */
    rc = get_cfg_subblock(parent, SCHED_SLEEP_BLOCK, &block, msg_out);
    if (rc)
        return rc == ENOENT ? 0 : rc;   /* not mandatory */

    /* read std parameters */
    rc = read_scalar_params(block, SCHED_SLEEP_BLOCK, sleep_params, msg_out);
    if (rc)
        return rc;

    CheckUnknownParameters(block, SCHED_SLEEP_BLOCK, allowed_params);

    return 0;
}

static int sched_sleep_cfg_update(void *sched_data, void *cfg)
{
    sched_sleep_config_t *new = cfg;
    struct sched_sleep_state *state = sched_data;

    state->cfg = *new;
    return 0;
}

/** configuration handlers for "max_per_run" scheduler */
static const ctx_cfg_funcs_t sched_sleep_cfg_funcs = {
    .module_name     = "sleeper scheduler",
    .new             = sched_sleep_cfg_new,
    .free            = sched_sleep_cfg_free,
    .set_default     = sched_sleep_cfg_set_default,
    .read_from_block = sched_sleep_cfg_read_from_block,
    .update          = sched_sleep_cfg_update,
    .write_default   = sched_sleep_cfg_write_default,
    .write_template  = sched_sleep_cfg_write_template,
};

/** "max_per_run" scheduler definition */
static action_scheduler_t sched_sleep = {
    .sched_name         = "sleeper",
    .sched_cfg_funcs    = &sched_sleep_cfg_funcs,
    .sched_init_func    = sched_sleep_init,
    .sched_reset_func   = sched_sleep_reset,
    .sched_attr_mask    = {0},
    .sched_schedule     = sched_sleep_schedule,
};

/** get a common scheduler by name */
action_scheduler_t *mod_get_scheduler(const char *sched_name)
{
    if (strcmp(sched_name, "test.sleeper") == 0)
        return &sched_sleep;

    return NULL;
}
