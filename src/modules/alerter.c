/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2015,2016 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

/**
 * \file alerter.c
 * \brief manage alerts on filesystem entries
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "status_manager.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "mod_internal.h"

#define TAG "alerter"

/** set of managed status */
typedef enum {
  STATUS_CLEAR,                 /* checked, no alert raised */
  STATUS_ALERT,                 /* alert raised */

  STATUS_COUNT,                 /* number of possible file status */
} alert_status_t;

static const char *alerter_status_list[] = {
    [STATUS_CLEAR] = "clear",
    [STATUS_ALERT] = "alert",
};

static const char *alerter_status2str(alert_status_t st)
{
    switch (st)
    {
        case STATUS_CLEAR:
        case STATUS_ALERT:
            return alerter_status_list[st];
        default:
            return NULL;
    }
}

/** enum of specific attributes */
enum alerter_info_e
{
    ATTR_LAST_CHECK = 0,
    ATTR_LAST_ALERT,
};

/** size of specific info to be stored in DB:
 * last_check: unix epoch
 * last_alert: unix epoch
 */
static sm_info_def_t alerter_info[] = {
    [ATTR_LAST_CHECK] = { "last_check", "lstchk",  DB_UINT, 0, {.val_uint = 0}, PT_DURATION },
    [ATTR_LAST_ALERT] = { "last_alert", "lstalrt", DB_UINT, 0, {.val_uint = 0}, PT_DURATION },
};

static int alerter_executor(struct sm_instance *smi,
                            const char *implements,
                            const policy_action_t *action,
 /* arguments for the action : */ const entry_id_t *p_id, attr_set_t *p_attrs,
                            const action_params_t *params,
                            post_action_e *what_after,
                            db_cb_func_t db_cb_fn, void *db_cb_arg)
{
    const char *val;
    const char *status_str = NULL;
    int         rc = 0;
    bool        alert = false;

    if (params == NULL)
    {
        DisplayLog(LVL_MAJOR, TAG, "Missing action parameters for 'alerter' status manager");
        return -EINVAL;
    }

    val = rbh_param_get(params, "alert");
    if (val == NULL)
    {
        DisplayLog(LVL_MAJOR, TAG, "Missing action parameter 'alert = yes/clear' for 'alerter' status manager");
        return -EINVAL;
    }

    if (!strcasecmp(val, "clear"))
    {
        /* if the action succeed new status will be: clear */
        status_str = alerter_status2str(STATUS_CLEAR);
    }
    else if (!strcasecmp(val, "raise"))
    {
        /* if the action succeed new status will be: alert */
        status_str = alerter_status2str(STATUS_ALERT);
        alert = true;
    }
    else
    {
        DisplayLog(LVL_MAJOR, TAG, "Invalid value for 'alert' action parameter: 'raise' or 'clear' expected");
        return -EINVAL;
    }

    /* set it now, at it may be modified by the specified function */
    *what_after = PA_UPDATE;

    rc = action_helper(action, "alert", p_id, p_attrs, params, smi,
                       NULL, what_after, db_cb_fn, db_cb_arg);
    if (rc)
        return rc;

    set_uint_info(smi, p_attrs, ATTR_LAST_CHECK, (unsigned int)time(NULL));
    if (alert)
        set_uint_info(smi, p_attrs, ATTR_LAST_ALERT, (unsigned int)time(NULL));

    return set_status_attr(smi, p_attrs, status_str);
}

static int alerter_alert(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                         const action_params_t *params, post_action_e *after,
                         db_cb_func_t db_cb_fn, void *db_cb_arg)
{
    char *str_id = NULL;
    bool  str_is_alloc = false;
    GString *gs = g_string_new(NULL);

    /* build alert string */
    if (ATTR_MASK_TEST(p_attrs, fullpath))
    {
        str_id = ATTR(p_attrs, fullpath);
    }
    else
    {
        asprintf(&str_id, DFID, PFID(p_entry_id));
        str_is_alloc = true;
    }

    /** TODO build specific parameter that represents the alert rule */
    #if 0
    rc = BoolExpr2str(&entry_proc_conf.alert_list[i].boolexpr, stralert, 2*RBH_PATH_MAX);
    if ( rc < 0 )
        strcpy( stralert, "Error building alert string" );
    #endif

    /** TODO build specific parameter that represents attr mask for the rule (alert mask) */

    print_attrs(gs, p_attrs, null_mask, 0);

    /* title: alert rule name */
    RaiseEntryAlert(rbh_param_get(params, "title"), "entry matches alert rule",
                    str_id, gs->str);

    g_string_free(gs, TRUE);
    if (str_is_alloc)
        free(str_id);

    return 0;
}

/** Status manager for alerts management */
status_manager_t alerter_sm = {
    .name = "alerter",
    /* TODO can possibly raise alerts on file deletion? if so, must set a softrm_* fields */
    .flags = 0,
    .status_enum = alerter_status_list, /* initial state is empty(unset) status */
    .status_count = STATUS_COUNT,
    .nb_info = G_N_ELEMENTS(alerter_info),
    .info_types = alerter_info,

    /* note: no get_status support */

    .executor = alerter_executor,
};

/* ======= PUBLIC FUNCTIONS ======= */
const char *mod_get_name(void)
{
    return alerter_sm.name;
}

status_manager_t *mod_get_status_manager(void)
{
    return &alerter_sm;
}

action_func_t mod_get_action_by_name(const char *action_name)
{
    if (strcmp(action_name, "alerter.alert") == 0)
        return alerter_alert;
    else
        return NULL;
}
