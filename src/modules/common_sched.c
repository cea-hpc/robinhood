/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2016 CEA/DAM
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

#include "mod_internal.h"
#include "policy_run.h"

/** max-per-run scheduler configuration */
typedef struct sched_mpr_config {
    ull_t   max_count;
    ull_t   max_vol;
} sched_mpr_config_t;

/** internal state for max-per-run scheduler (LPR) */
struct sched_mpr_state {
    sched_mpr_config_t cfg;
    ull_t   count;
    ull_t   vol;
};

static int sched_mpr_init(void *config, void **p_sched_data)
{
    struct sched_mpr_state *state = calloc(1, sizeof(*state));
    sched_mpr_config_t *cfg = config;

    if (!config)
        return -EINVAL;

    if (!state)
        return -ENOMEM;

    state->cfg = *cfg;
    *p_sched_data = state;
    return 0;
}

static int sched_mpr_reset(void *sched_data)
{
    struct sched_mpr_state *state = sched_data;

    state->count = state->vol = 0;
    return 0;
}

/* atomic increment */
#define ATOMIC_INCR(_var, _val) (__sync_fetch_and_add(&(_var), (_val)))

static int sched_mpr_schedule(void *sched_data, const entry_id_t *id,
                              const attr_set_t *attrs, sched_cb_t cb,
                              void *udata)
{
    struct sched_mpr_state *state = sched_data;

    /* check if a previous entry reached the limit */

    /* limit reached stop submitting actions */
    if (state->cfg.max_count != 0 && state->count >= state->cfg.max_count) {
        DisplayLog(LVL_VERB, "max_per_run",
                   "Max count reached (%Lu): stopping policy run",
                   state->cfg.max_count);
        return SCHED_STOP_RUN;
    }

    if (state->cfg.max_vol != 0 && state->vol >= state->cfg.max_vol) {
        DisplayLog(LVL_VERB, "max_per_run",
                   "Max volume reached (%Lu): stopping policy run",
                   state->cfg.max_vol);
        return SCHED_STOP_RUN;
    }

    ATOMIC_INCR(state->count, 1);

    if (attrs != NULL && ATTR_MASK_TEST(attrs, size))
        ATOMIC_INCR(state->vol, ATTR(attrs, size));

    /* if the limits are not reached, directly call the action calback */
    DisplayLog(LVL_DEBUG, "max_per_run", "Scheduling next step "
               "(curr counters: count=%Lu, vol=%Lu)", state->count, state->vol);
    return cb(udata, SCHED_OK);
}

/* ------------- configuration management functions ---------- */

/** configuration block name for max_per_run scheduler */
#define SCHED_LPR_BLOCK "max_per_run"

static void *sched_mpr_cfg_new(void)
{
    return calloc(1, sizeof(sched_mpr_config_t));
}

static void sched_mpr_cfg_free(void *cfg)
{
    free(cfg);
}

static void sched_mpr_cfg_set_default(void *module_config)
{
    sched_mpr_config_t *conf = module_config;

    conf->max_count = conf->max_vol = 0;
}

static void sched_mpr_cfg_write_default(int indent, FILE *output)
{
    print_begin_block(output, indent, SCHED_LPR_BLOCK, NULL);
    print_line(output, indent + 1, "max_count: 0 (unlimited)");
    print_line(output, indent + 1, "max_vol:   0 (unlimited)");
    print_end_block(output, indent);
}

static void sched_mpr_cfg_write_template(int indent, FILE *output)
{
    print_begin_block(output, indent, SCHED_LPR_BLOCK, NULL);
    print_line(output, indent + 1, "# max actions per run");
    print_line(output, indent + 1, "max_count = 10000;");
    print_line(output, indent + 1, "# max volume per run");
    print_line(output, indent + 1, "max_vol   = 100GB;");
    print_end_block(output, indent);
}

/** get a 'max_per_run' sublock from the policy parameters */
static int sched_mpr_cfg_read_from_block(config_item_t parent, void *cfg,
                                         char *msg_out)
{
    int rc;
    sched_mpr_config_t *conf = cfg;
    config_item_t       block;

    const cfg_param_t mpr_params[] = {
        {"max_count", PT_INT64, PFLG_POSITIVE, &conf->max_count, 0},
        {"max_vol",   PT_SIZE,  PFLG_POSITIVE, &conf->max_vol,   0},
        END_OF_PARAMS
    };

    static const char *allowed_params[] = {
        "max_count", "max_vol", NULL
    };

    /* get 'max_per_run' subblock */
    rc = get_cfg_subblock(parent, SCHED_LPR_BLOCK, &block, msg_out);
    if (rc)
        return rc == ENOENT ? 0 : rc;   /* not mandatory */

    /* read std parameters */
    rc = read_scalar_params(block, SCHED_LPR_BLOCK, mpr_params, msg_out);
    if (rc)
        return rc;

    CheckUnknownParameters(block, SCHED_LPR_BLOCK, allowed_params);

    return 0;
}

static int sched_mpr_cfg_update(void *sched_data, void *cfg)
{
    sched_mpr_config_t *new = cfg;
    struct sched_mpr_state *state = sched_data;

    state->cfg = *new;
    return 0;
}

/** configuration handlers for "max_per_run" scheduler */
static const ctx_cfg_funcs_t sched_mpr_cfg_funcs = {
    .module_name     = "max_per_run scheduler",
    .new             = sched_mpr_cfg_new,
    .free            = sched_mpr_cfg_free,
    .set_default     = sched_mpr_cfg_set_default,
    .read_from_block = sched_mpr_cfg_read_from_block,
    .update          = sched_mpr_cfg_update,
    .write_default   = sched_mpr_cfg_write_default,
    .write_template  = sched_mpr_cfg_write_template,
};

/** "max_per_run" scheduler definition */
static action_scheduler_t sched_mpr = {
    .sched_name         = "max_per_run",
    .sched_cfg_funcs    = &sched_mpr_cfg_funcs,
    .sched_init_func    = sched_mpr_init,
    .sched_reset_func   = sched_mpr_reset,
    .sched_attr_mask    = { .std = ATTR_MASK_size, },
    .sched_schedule     = sched_mpr_schedule,
};

/** get a common scheduler by name */
action_scheduler_t *mod_get_scheduler(const char *sched_name)
{
    if (strcmp(sched_name, "common.max_per_run") == 0)
        return &sched_mpr;

    return NULL;
}
