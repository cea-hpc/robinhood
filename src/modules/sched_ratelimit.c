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
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "mod_internal.h"
#include "policy_run.h"

/**
 * Default # of tokens (operations) granted per time interval.
 */
#define TBF_DEFAULT_CAPACITY 100

/**
 * Default period (in milliseconds) between two consecutive refills.
 */
#define TBF_DEFAULT_REFILL_PERIOD_MS 1000


/**
 * Token Bucket Filter (TBF) based rate-limit scheduler configuration
 */
typedef struct sched_tbf_config {
    int capacity;
    int refill_period;
} sched_tbf_config_t;

/** internal state for TBF rate limiter */
struct sched_tbf_state {
    sched_tbf_config_t  cfg;
    pthread_rwlock_t    rwlock;
    struct timespec     refill;
    int                 tokens;
};

/**
 * This is the function used everywhere in this module to retrieve current time.
 * We are looking for something monotonic and as fast as possible.
 */
static int getclock(struct timespec *clk)
{
    int rc;

    rc = clock_gettime(CLOCK_MONOTONIC_COARSE, clk);
    if (rc)
        return -errno;

    return 0;
}

static int sched_tbf_init(void *config, void **p_sched_data)
{
    struct sched_tbf_state *state;
    sched_tbf_config_t *cfg = config;
    int rc;

    if (!config)
        return -EINVAL;

    state = malloc(sizeof(*state));
    if (!state)
        return -ENOMEM;

    pthread_rwlock_init(&state->rwlock, NULL);

    rc = getclock(&state->refill);
    if (rc)
        goto out_free;

    state->cfg = *cfg;
    state->tokens = TBF_DEFAULT_CAPACITY;

    *p_sched_data = state;

out_free:
    if (rc)
        free(state);

    return rc;
}

static int sched_tbf_reset(void *sched_data)
{
    struct sched_tbf_state *state = sched_data;
    struct timespec now;

    pthread_rwlock_wrlock(&state->rwlock);
    state->tokens = state->cfg.capacity;
    getclock(&now);
    state->refill = now;
    pthread_rwlock_unlock(&state->rwlock);
    return 0;
}

/**
 * Return the number of milliseconds elapsed between stop and start.
 * Can be negative if stop < start (but you won't do that).
 */
static long timediff(const struct timespec *start, const struct timespec *stop)
{
    long res;

    res = (stop->tv_sec - start->tv_sec) * 1000;
    res += (stop->tv_nsec - start->tv_nsec) / 1000000;
    return res;
}

#define ATOMIC_DEC(_x)   (__sync_fetch_and_sub(&(_x), 1))

static int sched_tbf_schedule(void *sched_data, const entry_id_t *id,
                              const attr_set_t *attrs, sched_cb_t cb,
                              void *udata)
{
    struct sched_tbf_state *state = sched_data;
    struct timespec now;
    long diff;

    getclock(&now);

    pthread_rwlock_rdlock(&state->rwlock);
    diff = timediff(&state->refill, &now);
    pthread_rwlock_unlock(&state->rwlock);

    if (diff < state->cfg.refill_period)
        goto proceed;

    /* lock and recheck in case someone else did it while we were waiting */
    pthread_rwlock_wrlock(&state->rwlock);
    getclock(&now);
    diff = timediff(&state->refill, &now);
    if (diff >= state->cfg.refill_period) {
        state->tokens = state->cfg.capacity;
        state->refill = now;
    }
    pthread_rwlock_unlock(&state->rwlock);

proceed:
    if (state->tokens > 0) {
        /* Enough credits, invoke the action callback */
        ATOMIC_DEC(state->tokens);
        cb(udata, SCHED_OK);
        return SCHED_OK;
    }

    DisplayLog(LVL_DEBUG, "rate_limit",
               "Throttling after %d actions per %dms happened",
               state->cfg.capacity, state->cfg.refill_period);

    return SCHED_DELAY;
}

/* ------------- configuration management functions ---------- */

/** configuration block name for rate_limit scheduler */
#define SCHED_TBF_BLOCK "rate_limit"

static void *sched_tbf_cfg_new(void)
{
    return calloc(1, sizeof(sched_tbf_config_t));
}

static void sched_tbf_cfg_free(void *cfg)
{
    free(cfg);
}

static void sched_tbf_cfg_set_default(void *module_config)
{
    sched_tbf_config_t *conf = module_config;

    conf->capacity = TBF_DEFAULT_CAPACITY;
    conf->refill_period = TBF_DEFAULT_REFILL_PERIOD_MS;
}

static void sched_tbf_cfg_write_default(int indent, FILE *output)
{
    print_begin_block(output, indent, SCHED_TBF_BLOCK, NULL);
    print_line(output, indent + 1, "capacity: %d", TBF_DEFAULT_CAPACITY);
    print_line(output, indent + 1, "refill: %d", TBF_DEFAULT_REFILL_PERIOD_MS);
    print_end_block(output, indent);
}

static void sched_tbf_cfg_write_template(int indent, FILE *output)
{
    print_begin_block(output, indent, SCHED_TBF_BLOCK, NULL);
    print_line(output, indent + 1, "# max actions per refill period");
    print_line(output, indent + 1, "capacity = 100;");
    print_line(output, indent + 1, "# refill period in milliseconds");
    print_line(output, indent + 1, "refill  = 5000;");
    print_end_block(output, indent);
}

/** get a 'rate_limit' sublock from the policy parameters */
static int sched_tbf_cfg_read_from_block(config_item_t parent, void *cfg,
                                         char *msg_out)
{
    sched_tbf_config_t *conf = cfg;
    static const char *allowed_params[] = { "capacity", "refill", NULL };
    const cfg_param_t tbf_params[] = {
        {"capacity", PT_INT64, PFLG_POSITIVE, &conf->capacity, 0},
        {"refill",   PT_DURATION,  PFLG_POSITIVE, &conf->refill_period, 0},
        END_OF_PARAMS
    };
    config_item_t block;
    int rc;

    /* get 'rate_limit' subblock */
    rc = get_cfg_subblock(parent, SCHED_TBF_BLOCK, &block, msg_out);
    if (rc)
        return rc == ENOENT ? 0 : rc;   /* not mandatory */

    /* read std parameters */
    rc = read_scalar_params(block, SCHED_TBF_BLOCK, tbf_params, msg_out);
    if (rc)
        return rc;

    CheckUnknownParameters(block, SCHED_TBF_BLOCK, allowed_params);
    return 0;
}

static int sched_tbf_cfg_update(void *sched_data, void *cfg)
{
    struct sched_tbf_state *state = sched_data;
    sched_tbf_config_t *new = cfg;

    state->cfg = *new;
    return 0;
}

/** configuration handlers for "rate_limit" scheduler */
static const ctx_cfg_funcs_t sched_tbf_cfg_funcs = {
    .module_name     = "rate_limit scheduler",
    .new             = sched_tbf_cfg_new,
    .free            = sched_tbf_cfg_free,
    .set_default     = sched_tbf_cfg_set_default,
    .read_from_block = sched_tbf_cfg_read_from_block,
    .update          = sched_tbf_cfg_update,
    .write_default   = sched_tbf_cfg_write_default,
    .write_template  = sched_tbf_cfg_write_template,
};

/** "rate_limit" scheduler definition */
static action_scheduler_t sched_tbf = {
    .sched_name         = "rate_limit",
    .sched_cfg_funcs    = &sched_tbf_cfg_funcs,
    .sched_init_func    = sched_tbf_init,
    .sched_reset_func   = sched_tbf_reset,
    .sched_schedule     = sched_tbf_schedule,
};

/** get a common scheduler by name */
action_scheduler_t *mod_get_scheduler(const char *sched_name)
{
    if (strcmp(sched_name, "common.rate_limit") == 0)
        return &sched_tbf;

    return NULL;
}
