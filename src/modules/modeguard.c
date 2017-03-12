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
 * \file modeguard.c
 * \brief modeguard to enforce some file/directory access mode
 *
 * The purpose of this module is to enforce useful permission bits,
 * like directory setgid bit on project folders.
 *
 * It maintains a status (ok/invalid).
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "status_manager.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "mod_internal.h"

#define TAG "modeguard"

/** By default modeguard will enforce both directory setgid and
 * sticky bits. Directory setgid bit is inherited from parent for new
 * files and directories but users might remove it by mistake or some
 * copy tools might override this bit anyway.
 * The sticky bit is not inherited.
 *
 * Enforcing these bits is very useful for shared project folders on
 * a POSIX filesystem.
 */
#define MG_SET_MODE_MASK   03000

/** set of managed status */
typedef enum {
    STATUS_OK,      /* checked and access mode is OK */
    STATUS_INVALID, /* checked and access mode is invalid */

    STATUS_COUNT,   /* number of possible statuses */
} modeguard_status_t;

static const char *modeguard_status_list[] = {
    [STATUS_OK] = "ok",
    [STATUS_INVALID] = "invalid",
};

static const char *modeguard_status2str(modeguard_status_t st)
{
    switch (st) {
    case STATUS_OK:
    case STATUS_INVALID:
        return modeguard_status_list[st];
    default:
        return NULL;
    }
}

/** helper to set the entry status for the given SMI */
static inline int set_modeguard_status(sm_instance_t *smi, attr_set_t *pattrs,
                                       modeguard_status_t st)
{
    return set_status_attr(smi, pattrs, modeguard_status2str(st));
}

/**
 * Get the status for an entry.
 * \param[in] p_id pointer to entry id
 * \param[in] attrs_in pointer to entry attributes
 * \param[out] p_attrs_changed changed/retrieved attributes
 */
static int modeguard_status(struct sm_instance *smi,
                         const entry_id_t *p_id, const attr_set_t *p_attrs_in,
                         attr_set_t *p_attrs_changed)
{
    mode_t mode;
    int status;

    /* check if mode is provided (mandatory) */
    if (!ATTR_MASK_TEST(p_attrs_in, mode)) {
        DisplayLog(LVL_MAJOR, TAG,
                   "Missing mandatory mode attribute for checking entry status");
        return -EINVAL;
    }

    mode = ATTR(p_attrs_in, mode);

    DisplayLog(LVL_DEBUG, TAG, "status path=%s mode=%o",
               ATTR(p_attrs_in, fullpath), mode);

    if ((mode & MG_SET_MODE_MASK) == MG_SET_MODE_MASK)
        status = STATUS_OK;
    else
        status = STATUS_INVALID;

    return set_modeguard_status(smi, p_attrs_changed, status);
}

/** the only action supported by modeguard to restore permissions */
static int modeguard_action(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                            const action_params_t *params, post_action_e *after,
                            db_cb_func_t db_cb_fn, void *db_cb_arg)
{
    mode_t mode;
    const char *path;

    *after = PA_NONE;

    if (ATTR_MASK_TEST(p_attrs, type) &&
        strcmp(ATTR(p_attrs, type), STR_TYPE_DIR) != 0) {
        DisplayLog(LVL_EVENT, "modeguard_action", "non-dir ignored");
        /* not a directory: not supported */
        return 0;
    }
    /* check if mode is provided (mandatory) */
    if (!(ATTR_MASK_TEST(p_attrs, mode) && ATTR_MASK_TEST(p_attrs, fullpath))) {
        DisplayLog(LVL_MAJOR, TAG,
                   "Missing mandatory attribute for checking entry status");
        return -EINVAL;
    }

    mode = ATTR(p_attrs, mode); /* in robinhood we trust */
    path = ATTR(p_attrs, fullpath);

    if ((mode & MG_SET_MODE_MASK) != MG_SET_MODE_MASK) {
        DisplayLog(LVL_EVENT, TAG,
                   "Fixing invalid mode %o to %o for %s", mode,
                   mode | MG_SET_MODE_MASK, path);
        if (chmod(path, mode | MG_SET_MODE_MASK) != 0 && errno != ENOENT) {
            DisplayLog(LVL_MAJOR, TAG, "chmod failed with error %d (%s)", errno,
                       strerror(errno));
            return -errno;
        }
    }
    return 0;
}


/** changelog callback */
static int modeguard_cl_cb(struct sm_instance *smi, const CL_REC_TYPE *logrec,
                           const entry_id_t *id, const attr_set_t *attrs,
                           attr_set_t *refreshed_attrs, bool *getit,
                           proc_action_e *rec_action)
{
    if (ATTR_MASK_TEST(attrs, type) &&
        strcmp(ATTR(attrs, type), STR_TYPE_DIR) != 0) {
        /* not a directory: not supported */
        return 0;
    }
    if (logrec->cr_type == CL_SETATTR) {
        *getit = true; /* update status */
    }
    return 0;
}

/** Status manager for modeguard */
status_manager_t modeguard_sm = {
    .name = "modeguard",
    .flags = 0,
    .status_enum = modeguard_status_list,
    .status_count = STATUS_COUNT,
    /* not sure those are needed, I didn't notice any difference */
    .status_needs_attrs_fresh = { .std = ATTR_MASK_mode,
                                  .status = ATTR_MASK_mode },
    .get_status_func = modeguard_status,
    .changelog_cb = modeguard_cl_cb,
};

/* ======= PUBLIC FUNCTIONS ======= */
const char *mod_get_name(void)
{
    return modeguard_sm.name;
}

status_manager_t *mod_get_status_manager(void)
{
    return &modeguard_sm;
}

action_func_t mod_get_action(const char *action_name)
{
    if (strcmp(action_name, "modeguard.action") == 0)
        return modeguard_action;
    else
        return NULL;
}
