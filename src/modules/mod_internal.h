/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2010-2014 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

#ifndef UTILITY_H
#define UTILITY_H

#include <stdlib.h>
#include <stdbool.h>
#include "rbh_modules.h"

/* log tag for built-in copy */
#define CP_TAG "cp"

#define TARGET_PATH_PARAM "targetpath"

typedef enum {
    CP_COMPRESS     = (1 << 0),
    CP_USE_SENDFILE = (1 << 1),
    CP_NO_SYNC      = (1 << 2),
    CP_COPYBACK     = (1 << 3)  /* retrieve a copy */
} copy_flags_e;

/** These functions are shared by several modules (namely common & backup). */
int builtin_copy(const char *src, const char *dst, int dst_oflags,
                 bool save_attrs, copy_flags_e flags);

/** set copy flags from a parameter set */
copy_flags_e params2flags(const action_params_t *params);

/** helper to set the entry status for the given SMI */
static inline int set_status_attr(const sm_instance_t *smi,
                                  attr_set_t *pattrs, const char *str_st)
{
    int rc;

    if (str_st == NULL) {
        rc = -EINVAL;
        goto clean_status;
    }

    /* check allocation of sm_status array */
    sm_status_ensure_alloc(&pattrs->attr_values.sm_status);
    if (pattrs->attr_values.sm_status == NULL) {
        rc = -ENOMEM;
        goto clean_status;
    }

    STATUS_ATTR(pattrs, smi->smi_index) = str_st;
    ATTR_MASK_STATUS_SET(pattrs, smi->smi_index);

    return 0;

 clean_status:
    if (pattrs->attr_values.sm_status != NULL)
        /* don't free it as it contains a const char* */
        STATUS_ATTR(pattrs, smi->smi_index) = NULL;

    /* Clean the status from the mask */
    ATTR_MASK_STATUS_UNSET(pattrs, smi->smi_index);

    return rc;
}

/** helper to set bool attr */
static inline int set_bool_info(const sm_instance_t *smi, attr_set_t *pattrs,
                                unsigned int attr_index, bool val)
{
    bool *info;
    int rc;

    info = malloc(sizeof(bool));
    if (info == NULL)
        return -ENOMEM;

    *info = val;

    rc = set_sm_info(smi, pattrs, attr_index, info);
    if (rc)
        free(info);

    return rc;
}

/** helper to set uint attr */
static inline int set_uint_info(const sm_instance_t *smi, attr_set_t *pattrs,
                                unsigned int attr_index, unsigned int val)
{
    unsigned int *info;
    int rc;

    info = malloc(sizeof(unsigned int));
    if (info == NULL)
        return -ENOMEM;

    *info = val;

    rc = set_sm_info(smi, pattrs, attr_index, info);
    if (rc)
        free(info);

    return rc;
}

/** Helper to run a configurable action. */
int action_helper(const policy_action_t *action, const char *name,
                  const entry_id_t *p_id, attr_set_t *p_attrs,
                  const action_params_t *params, struct sm_instance *smi,
                  GString *out, post_action_e *after,
                  db_cb_func_t db_cb_fn, void *db_cb_arg);

/* ---- Public module interface ---- */

const char *mod_get_name(void);

status_manager_t *mod_get_status_manager(void);

action_func_t mod_get_action_by_name(const char *action_name);
#endif
