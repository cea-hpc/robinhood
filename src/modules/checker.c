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

/**
 * \file checker.c
 * \brief file checking module.
 *
 * The purpose of this module is to run actions
 * on filesystem entries (checksum, virus scan, ...)
 * and store the result of this action in the DB,
 * so it can be later compared when executing the action
 * a next time.
 * It implement a 'last_check' criteria, to allow defining
 * a check interval in policy rules (e.g. last_check > 7d).
 * It implement a 'last_success' criteria, to report
 * last successful command execution.
 * It also maintain a status (ok/failed), which is the
 * status of the last command run on the entry.
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "status_manager.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "mod_internal.h"

#define TAG "checker"

/** set of managed status */
typedef enum {
    STATUS_OK,  /* last executed command exited with status 0 */
    STATUS_FAILED,  /* last executed command exited with status != 0 */

    STATUS_COUNT,   /* number of possible statuses */
} check_status_t;

static const char *check_status_list[] = {
    [STATUS_OK] = "ok",
    [STATUS_FAILED] = "failed",
};

static const char *check_status2str(check_status_t st)
{
    switch (st) {
    case STATUS_OK:
    case STATUS_FAILED:
        return check_status_list[st];
    default:
        return NULL;
    }
}

/** enum of specific attributes */
enum check_info_e {
    ATTR_LAST_CHECK = 0,    /* time of last command run */
    ATTR_LAST_SUCCESS,  /* time of last command success */
    ATTR_OUTPUT,    /* command output (if commands succeeds) */
};

/** definition of specific info  */
static sm_info_def_t check_info[] = {
    [ATTR_LAST_CHECK] =
        {"last_check", "lstchk", DB_UINT, 0, {.val_uint = 0},
         .crit_type = PT_DURATION},
    [ATTR_LAST_SUCCESS] =
        {"last_success", "lstsuc", DB_UINT, 0, {.val_uint = 0},
         .crit_type = PT_DURATION},
/** Define a limited output size to reduce de DB footprint. If the user needs to attach more
 * information for each file, he can still store it as an xattr, or in an annex database. */
    [ATTR_OUTPUT] = {"output", "out", DB_TEXT, 255, {.val_str = NULL},
                     .crit_type = PT_STRING},
};

static int check_executor(struct sm_instance *smi,
                          const char *implements,
                          const policy_action_t *action,
                          /* arguments for the action : */
                          const entry_id_t *p_id, attr_set_t *p_attrs,
                          const action_params_t *params,
                          post_action_e *what_after, db_cb_func_t db_cb_fn,
                          void *db_cb_arg)
{
    int rc = 0;
    time_t t;
    bool use_str = false;
    GString *out = NULL;

    *what_after = PA_UPDATE;

    /* Run the action.
     * Functions (defined in modules):
     * o As input, a function action should use 'output' attribute to compare
     *   the result of the last execution.
     * o As output, a function action can store its result to 'output'
     *   attribute.
     * Commands:
     * o As input, a command can retrieve the last output by using '{output}'
     *   placeholder.
     * o As output, output will be set as the contents of stdout
     *   (truncated to 255 char).
     */
    out = g_string_new("");
    rc = action_helper(action, "check", p_id, p_attrs, params, smi, out,
                       what_after, db_cb_fn, db_cb_arg);

    /* update the value of last_check */
    t = time(NULL);
    set_uint_info(smi, p_attrs, ATTR_LAST_CHECK, (unsigned int)t);

    /* depending on the action status, update the value of last_success */
    if (rc == 0) {
        set_status_attr(smi, p_attrs, check_status2str(STATUS_OK));
        set_uint_info(smi, p_attrs, ATTR_LAST_SUCCESS, (unsigned int)t);

        /* set output if the action was a successful command */
        if (action->type == ACTION_COMMAND) {
            int rc2;

            DisplayLog(LVL_DEBUG, "check_exec", "check command output='%s'",
                       out->str);
            rc2 = set_sm_info(smi, p_attrs, ATTR_OUTPUT, out->str);
            if (rc2 == 0)
                /* str is now owner by p_attrs */
                use_str = true;
        }
    } else {
        set_status_attr(smi, p_attrs, check_status2str(STATUS_FAILED));
        DisplayLog(LVL_EVENT, "check_exec",
                   "check command FAILED on: " DFID_NOBRACE " (%s)",
                   PFID(p_id), ATTR(p_attrs, fullpath));
    }

    g_string_free(out, use_str ? FALSE : TRUE);
    return rc;
}

/** Status manager for file check management */
status_manager_t checker_sm = {
    .name = "checker",
    .flags = 0,
    .status_enum = check_status_list, /* initial state is empty status (unset) */
    .status_count = STATUS_COUNT,
    .nb_info = G_N_ELEMENTS(check_info),
    .info_types = check_info,

    /* note: no get_status support */

    .executor = check_executor,
};

/* ======= PUBLIC FUNCTIONS ======= */
const char *mod_get_name(void)
{
    return checker_sm.name;
}

status_manager_t *mod_get_status_manager(void)
{
    return &checker_sm;
}

action_func_t mod_get_action_by_name(const char *action_name)
{
    /* no specific action implemented */
    return NULL;
}
