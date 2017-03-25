/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2017 Stephane Thiell <sthiell@stanford.edu>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

/**
 * \file deferpurge.c
 * \brief Instead of directly purging files, the deferpurge module marks files
 * as purgeable (deferpurge_check policy) and then performs the cleaning
 * of these files after some predefined time (deferpurge_purge policy).
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "status_manager.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "mod_internal.h"

#define TAG "deferpurge"

/** set of managed status */
typedef enum {
    STATUS_OK,      	/* currently not really used in practice */
    STATUS_PURGEABLE,   /* checked and file can be purged */

    STATUS_COUNT,       /* number of possible statuses */
} deferpurge_status_t;

static const char *deferpurge_status_list[] = {
    [STATUS_OK] = "ok",
    [STATUS_PURGEABLE] = "purgeable",
};

static const char *deferpurge_status2str(deferpurge_status_t st)
{
    switch (st) {
    case STATUS_OK:
    case STATUS_PURGEABLE:
        return deferpurge_status_list[st];
    default:
        return NULL;
    }
}

/** enum of specific attributes */
enum deferpurge_info_e {
    ATTR_LAST_CHECK = 0,    /* time of last command run */
};

/** definition of specific info  */
static sm_info_def_t deferpurge_info[] = {
    [ATTR_LAST_CHECK] =
        {"last_check", "lstchk", DB_UINT, 0, {.val_uint = 0},
         .crit_type = PT_DURATION},
};

/** check this is a supported action */
static bool deferpurge_check_action_name(const char *name)
{
    if (strcasecmp(name, "check") && strcasecmp(name, "purge"))
        return false;

    return true;
}

static int deferpurge_executor(struct sm_instance *smi,
                               const char *implements,
                               const policy_action_t *action,
                               /* arguments for the action : */
                               const entry_id_t *p_id, attr_set_t *p_attrs,
                               const action_params_t *params,
                               post_action_e *what_after, db_cb_func_t db_cb_fn,
                               void *db_cb_arg)
{
    const char *val;
    const char *status_str = NULL;
    int rc;

    if (params == NULL) {
        DisplayLog(LVL_MAJOR, TAG,
                   "Missing action parameters for 'deferpurge' status manager");
        return -EINVAL;
    }

    val = rbh_param_get(params, "purgeable");
    if (val == NULL) {
        DisplayLog(LVL_MAJOR, TAG,
                   "Missing action parameter 'purgeable = yes/clear' for "
                   "'deferpurge' status manager");
        return -EINVAL;
    }

    if (!strcasecmp(val, "clear")) {
        /* if the action succeed new status will be: ok */
        status_str = deferpurge_status2str(STATUS_OK);
    } else if (!strcasecmp(val, "yes")) {
        /* if the action succeed new status will be: purgeable */
        status_str = deferpurge_status2str(STATUS_PURGEABLE);
    } else {
        DisplayLog(LVL_MAJOR, TAG,
                   "Invalid value for 'deferpurge' action parameter: "
                   "'yes' or 'clear' expected");
        return -EINVAL;
    }

    *what_after = PA_UPDATE;

    rc = action_helper(action, "deferpurge", p_id, p_attrs, params, smi,
                       NULL, what_after, db_cb_fn, db_cb_arg);
    if (rc)
        return rc;

    /* update the value of last_check */
    set_uint_info(smi, p_attrs, ATTR_LAST_CHECK, (unsigned int)time(NULL));

    return set_status_attr(smi, p_attrs, status_str);
}

/** Status manager for deferpurge */
static status_manager_t deferpurge_sm = {
    .name = "deferpurge",
    .flags = 0,
    .status_enum = deferpurge_status_list,
    .status_count = STATUS_COUNT,
    .nb_info = G_N_ELEMENTS(deferpurge_info),
    .info_types = deferpurge_info,
    .check_action_name = deferpurge_check_action_name,

    /* note: no get_status support */

    .executor = deferpurge_executor,
};

/* ======= PUBLIC FUNCTIONS ======= */
const char *mod_get_name(void)
{
    return deferpurge_sm.name;
}

status_manager_t *mod_get_status_manager(void)
{
    return &deferpurge_sm;
}

action_func_t mod_get_action(const char *action_name)
{
    return NULL;
}
