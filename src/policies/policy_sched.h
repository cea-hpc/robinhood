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

#include "policy_run.h"

#include <glib.h>

/**
 * Scheduler resources.
 */
struct sched_res_t {
    /** Items waiting to be submitted to the schduler */
    GAsyncQueue *sched_queue;
    /** Thread to submit entries to the scheduler */
    pthread_t    sched_thread;
    /** Scheduler description */
    const action_scheduler_t *sched_desc;
    /** Scheduler private context */
    void        *sched_data;
    /** termination state */
    bool         terminate;
    /** pointer to policy run configuration */
    policy_run_config_t *run_cfg;
};

/**
 * Initialize scheduler resources.
 */
int sched_init(struct sched_res_t *sched_res,
               const action_scheduler_t *sched_desc,
               void *sched_cfg, policy_run_config_t *run_cfg);

/** Reinitialize scheduling for a new policy run */
int sched_reinit(struct sched_res_t *sched_res);

/**
 * Submit a new entry for scheduling.
 */
int sched_push(struct sched_res_t *sched_res,
               const entry_id_t *id, const attr_set_t *attrs,
               sched_cb_t cb, void *udata);

/**
 * Drop any entry from the scheduler and the wait queue.
 */
int sched_flush(struct sched_res_t *sched_res);
