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

/* config block name */
#define MODEGUARD_BLOCK "modeguard_config"
#define SET_MASK_PARAM "set_mask"
#define CLEAR_MASK_PARAM "clear_mask"

typedef struct modeguard_config_t {
    mode_t set_mask;
    mode_t clear_mask;
} modeguard_config_t;

/**
 * modeguard config is global for now
 * TODO: config per SM instance (policy)
 */
static modeguard_config_t config;

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

    if (((mode & config.set_mask) == config.set_mask) &&
        ((mode & config.clear_mask) == 0))
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
    mode_t mode, fixed_mode;
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

    fixed_mode = (mode | config.set_mask) & ~config.clear_mask;
    if (mode != fixed_mode) {
        DisplayLog(LVL_EVENT, TAG,
                   "Fixing invalid mode %04o to %04o for %s", mode, fixed_mode,
                   path);
        if (chmod(path, fixed_mode) != 0 && errno != ENOENT) {
            DisplayLog(LVL_MAJOR, TAG, "chmod %04o failed on %s with error "
                       "%d (%s)", fixed_mode, path, errno, strerror(errno));
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

/** modeguard_config */

static int parse_param_octal(const char *str, const char *param_name,
                                       mode_t *val_out)
{
    char *tmp;

    /* interpret octal notations (base 8) */
    errno = 0;
    *val_out = strtol(str, &tmp, 8);
    if (tmp == str || *tmp != '\0' || errno == ERANGE) {
        DisplayLog(LVL_MAJOR, MODEGUARD_BLOCK, "Error: invalid value for %s",
                   param_name);
        return EINVAL;
    }
    return 0;
}

static void *modeguard_cfg_new(void)
{
    DisplayLog(LVL_EVENT, TAG, "modeguard_cfg_new");
    return calloc(1, sizeof(modeguard_config_t));
}

static void modeguard_cfg_free(void *cfg)
{
    DisplayLog(LVL_EVENT, TAG, "modeguard_cfg_free");
    if (cfg != NULL)
        free(cfg);
}

static void modeguard_cfg_set_default(void *module_config)
{
    modeguard_config_t *conf = module_config;
    conf->set_mask = 0;
    conf->clear_mask = 0;
}

static int modeguard_cfg_set(void *cfg, bool reload)
{
    DisplayLog(LVL_EVENT, TAG, "modeguard_cfg_set");
    modeguard_config_t *new = cfg;
    config = *new;
    return 0;
}

static int modeguard_cfg_read(config_file_t config, void *module_config,
                              char *msg_out)
{
    DisplayLog(LVL_EVENT, TAG, "modeguard_cfg_read");
    modeguard_config_t *conf = module_config;
    char set_mask_str[8];
    char clear_mask_str[8];
    config_item_t block;
    int rc;

    const cfg_param_t modeguard_params[] = {
        {SET_MASK_PARAM, PT_STRING, 0, &set_mask_str, sizeof(set_mask_str)},
        {CLEAR_MASK_PARAM, PT_STRING, 0, &clear_mask_str,
         sizeof(clear_mask_str)},
        END_OF_PARAMS
    };

    static const char *allowed_params[] = {
        SET_MASK_PARAM, CLEAR_MASK_PARAM, NULL
    };

    /* get modeguard_config block */
    rc = get_cfg_block(config, MODEGUARD_BLOCK, &block, msg_out);
    if (rc)
        return rc == ENOENT ? 0 : rc;   /* not mandatory */

    /* read parameters */
    rc = read_scalar_params(block, MODEGUARD_BLOCK, modeguard_params, msg_out);
    if (rc)
        return rc;

    CheckUnknownParameters(block, MODEGUARD_BLOCK, allowed_params);

    if (*set_mask_str && !parse_param_octal(set_mask_str, SET_MASK_PARAM,
                                            &conf->set_mask))
        DisplayLog(LVL_DEBUG, MODEGUARD_BLOCK, SET_MASK_PARAM" set to %04o",
                   conf->set_mask);
    else
        DisplayLog(LVL_DEBUG, MODEGUARD_BLOCK, SET_MASK_PARAM" not set");

    if (*clear_mask_str && !parse_param_octal(clear_mask_str, CLEAR_MASK_PARAM,
                                              &conf->clear_mask))
        DisplayLog(LVL_DEBUG, MODEGUARD_BLOCK, CLEAR_MASK_PARAM" set to %04o",
                   conf->clear_mask);
    else
        DisplayLog(LVL_DEBUG, MODEGUARD_BLOCK, CLEAR_MASK_PARAM" not set");

    return 0;
}


static const mod_cfg_funcs_t modeguard_cfg_hdlr = {
    .module_name = "modeguard",
    .new = modeguard_cfg_new,
    .free = modeguard_cfg_free,
    .set_default = modeguard_cfg_set_default,
    .read = modeguard_cfg_read,
    .set_config = modeguard_cfg_set,
};

/** Status manager for modeguard */
static status_manager_t modeguard_sm = {
    .name = "modeguard",
    .flags = 0,
    .status_enum = modeguard_status_list,
    .status_count = STATUS_COUNT,
    .status_needs_attrs_fresh = { .std = ATTR_MASK_mode },
    .get_status_func = modeguard_status,
    .changelog_cb = modeguard_cl_cb,
    .cfg_funcs = &modeguard_cfg_hdlr,
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
