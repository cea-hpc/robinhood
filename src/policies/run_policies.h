/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009-2014 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

/**
 * \file run_policies.h
 * \brief This module triggers migrations to HSM or external storage.
 */
#ifndef _RUN_POLICIES_H
#define _RUN_POLICIES_H

#include "policy_run.h"

typedef struct policy_runs_t {
    policy_info_t *runs;
    unsigned int count;
} policy_runs_t;

/* defined in policy_triggers.c */
extern policy_runs_t policy_runs;

/* Action status */
typedef enum {
    AS_OK = 0,  /* action successful */

/* skipped */
    AS_ACCESSED,    /* entry has been accessed recently */
    AS_MOVED,   /* entry has been moved or deleted */
    AS_WHITELISTED, /* entry is whitelisted  */
    AS_OUT_OF_SCOPE,    /* entry is no longer in policy scope */
    AS_NO_POLICY,   /* entry matches no policy */
    AS_BAD_TYPE,    /* policy does not apply to this type of entry */
    AS_BUSY,    /* entry is is use */
    AS_ALREADY, /* action is already running */

/* errors */
    AS_MISSING_MD,  /* entry metadata is incomplete */
    AS_STAT_FAILURE,    /* stat failure */
    AS_ERROR,   /* action failed */

    AS_ABORT,   /* action aborted by termination signal */

    AS_ENUM_COUNT   /* last status index + 1 */
} action_status_t;

/* Action status description */
static const char __attribute__ ((__unused__))
    *action_status_descr[AS_ENUM_COUNT] = {
        "action successful", "accessed since last update",
        "moved or deleted since last update", "whitelisted/ignored",
        "out of scope", "no matching rule", "entry type out of scope",
        "entry is in use/busy", "action already running",
        "incomplete metadata", "stat failure", "action error",
        "action aborted"};

/* feedback from action queue (count, volume, ...) */
typedef enum {
    AF_NBR_OK,
    AF_NBR_NOK,

    AF_VOL_OK,
    AF_VOL_NOK,

    AF_TARGETED_OK,
    AF_TARGETED_NOK,

    AF_BLOCKS_OK,
    AF_BLOCKS_NOK,

    AF_ENUM_COUNT   /* last status index + 1 */
} action_feedback_t;

typedef struct policy_param_t {
    policy_target_t target;
// XXX no not differ from policy_info->flags? */
//    int             flags;
//
    target_u optarg_u;
    counters_t target_ctr;
    time_modifier_t *time_mod;

    const action_params_t *action_params;

} policy_param_t;

int run_policy(policy_info_t *p_pol_info, const policy_param_t *p_param,
               action_summary_t *p_summary, lmgr_t *lmgr);

/* Note: the number of threads is in p_pol_info->config */
int start_worker_threads(policy_info_t *p_pol_info);

/* Note: the timeout is in p_pol_info->config */
int check_current_actions(policy_info_t *p_pol_info, lmgr_t *lmgr,
                          unsigned int *p_nb_reset, unsigned int *p_nb_total);

#endif
