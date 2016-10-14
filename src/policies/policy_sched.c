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

#include "policy_sched.h"
#include "rbh_misc.h"

struct sched_q_item {
    const entry_id_t   *id;
    const attr_set_t   *attrs;
    sched_cb_t          cb;
    void               *udata;
};

/** Thread to submit entries to the scheduler */
static void *enqueue_thr(void *arg)
{
    struct sched_res_t *sched_res = arg;

    while (1) {
        struct sched_q_item *item;
        int rc;

        item = g_async_queue_pop(sched_res->sched_queue);

        if (unlikely(item == NULL))
            RBH_BUG("Dequeued item is not supposed to be NULL");

retry:
        /* push to the scheduler */
        rc = sched_res->sched_desc->sched_schedule(sched_res->sched_data,
                                                   item->id, item->attrs,
                                                   item->cb, item->udata);
        switch (rc) {
        case SCHED_OK:
            /* OK, continue enqueuing other entries */
            break;

        case SCHED_DELAY_ALL:
            /* Wait a while before submitting any new entry */
            /* XXX This must be configurable. Globally or per scheduler? */
            rh_usleep(100000); /* 100ms */
            goto retry;

        case SCHED_DELAY_ENTRY:
            /* We must delay this entry and continue with others.
             * In this case, we have to make sure not to loop on these
             * delayed entries... TODO Think about it.
             */
            RBH_BUG("Case not implemented");
            continue;

        case SCHED_SKIP_ENTRY:
            /* skip the entry for the current run */
            item->cb(item->udata, SCHED_SKIP_ENTRY);
            break;

        case SCHED_STOP_RUN:
        case SCHED_KILL_RUN:
            /* stop submitting entries:
             * - acknowledge/free all entries
             *   and signal 'STOP' of the policy run
             * - free current item
             */

            /* no more push */
            sched_res->terminate = true;

            /* unqueue and acknowledge all */
            do {
                item->cb(item->udata, rc);
                free(item);
            } while ((item = g_async_queue_try_pop(sched_res->sched_queue)) != NULL);

            DisplayLog(LVL_VERB, __func__, "Stop submitting entries for "
                   "current policy run");
            break;

        default:
            /* Error or unhandled return code */
            DisplayLog(LVL_VERB, __func__, "Unexpected error %d", rc);
            item->cb(item->udata, rc);
            break;
        }
        free(item);
    }
    UNREACHED();
}

/**
 * Initialize scheduler resources.
 */
int sched_init(struct sched_res_t *sched_res,
               const action_scheduler_t *sched_desc,
               void *sched_cfg)
{
    int rc;

    if (!sched_res)
        return -EINVAL;

    sched_res->sched_desc = sched_desc;
    sched_res->sched_data = NULL;
    sched_res->terminate = false;

    /* initialize the scheduler */
    if (sched_desc->sched_init_func != NULL) {
        rc = sched_desc->sched_init_func(sched_cfg, &sched_res->sched_data);
        if (rc)
            return rc;
    }

    sched_res->sched_queue = g_async_queue_new_full(free);

    /* start the enqueuer thread now everything is initialized */
    if (pthread_create(&sched_res->sched_thread, NULL, enqueue_thr,
                       sched_res) != 0) {
        rc = -errno;
        goto err_free;
    }

    return 0;

err_free:
    /* TODO destroy scheduler resources */
    g_async_queue_unref(sched_res->sched_queue);
    return rc;
}

/** Reinitialize scheduling for a new policy run */
int sched_reinit(struct sched_res_t *sched_res)
{
    /* error if queue is not empty */
    if (g_async_queue_length(sched_res->sched_queue) > 0) {
        DisplayLog(LVL_MAJOR, __func__, "Trying to reinitialize a non-empty queue");
        return -EINVAL;
    }

    sched_res->terminate = false;

    return 0;
}

/**
 * Submit a new entry for scheduling.
 */
int sched_push(struct sched_res_t *sched_res,
               const entry_id_t *id, const attr_set_t *attrs,
               sched_cb_t cb, void *udata)
{
    struct sched_q_item *item;
    int rc;

    /* if the queue is empty, directly push to the scheduler */
    if (g_async_queue_length(sched_res->sched_queue) <= 0) {
        rc = sched_res->sched_desc->sched_schedule(sched_res->sched_data,
                                                   id, attrs, cb, udata);

        /* if the entry must be delayed, then we have to push it to the queue */
        if (rc != SCHED_DELAY_ALL && rc != SCHED_DELAY_ENTRY)
            return rc;
    }

    /* if the entry is to be delayed, or if queue is not empty, push to the queue */
    item = calloc(1, sizeof (*item));
    if (!item)
        return -ENOMEM;

    item->id = id;
    item->attrs = attrs;
    item->cb = cb;
    item->udata = udata;

    DisplayLog(LVL_DEBUG, __func__, "Waiting to be submit entry to the scheduler");
    g_async_queue_push(sched_res->sched_queue, item);
    return 0;
}

/**
 * Flush all pending entries.
 */
int sched_flush(struct sched_res_t *sched_res);

/**
 * Drop any pending entry from the scheduler and the wait queue.
 */
int sched_reset(struct sched_res_t *sched_res);
