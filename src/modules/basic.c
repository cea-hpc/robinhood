/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2015-2017 CEA/DAM
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

#include "status_manager.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "mod_internal.h"


/* -------------- Basic status manager implementation ------------------- */

#define BASIC_ST_COUNT 2
static const char *basic_status_list[] = { "ok", "failed" };    /* + not set */

/** set status according to action return status */
static int basic_sm_action_cb(struct sm_instance *smi, const char *implements,
                              int action_status, const entry_id_t *id,
                              attr_set_t *p_attrs, post_action_e *what_after)
{
    if (action_status == 0)
        set_status_attr(smi, p_attrs, basic_status_list[0]);
    else
        set_status_attr(smi, p_attrs, basic_status_list[1]);

    return 0;
}

static status_manager_t basic_sm = {
    .name         = "basic",
    .status_enum  = basic_status_list,
    .status_count = BASIC_ST_COUNT,
    .action_cb    = basic_sm_action_cb,
};

/* ======= PUBLIC FUNCTIONS ======= */
const char *mod_get_name(void)
{
    return basic_sm.name;
}

status_manager_t *mod_get_status_manager(void)
{
    return &basic_sm;
}
