/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2008, 2009, 2010, 2014 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

/**
 * \file  update_params.h
 * \brief Db attribute update strategy
 */

#ifndef _UPDT_PARAMS_H
#define _UPDT_PARAMS_H

#include "config_parsing.h"
#include "list_mgr.h"

/**
 * Update behaviors
 * \addtogroup Update management
 * @{
 */

/**
 * path and metadata update strategies
 */
enum updt_when {
    UPDT_NEVER,     /* get info once, and never refresh it */
    UPDT_ALWAYS,    /* always update info when processing an entry */
    UPDT_ON_EVENT,  /* get info on related event */
    UPDT_ON_EVENT_PERIODIC, /* default: get info on related event, with a
                             * min interval + periodic update (max interval) */
    UPDT_PERIODIC   /* update info periodically */
};

/**
 * update policy item
 */
typedef struct updt_param_item_t {
    enum updt_when      when;
    unsigned int        period_min;    /* 0=no min */
    unsigned int        period_max;    /* 0=no periodic update */
} updt_param_item_t;

/**
 *  update parameters
 */
typedef struct updt_params_t {
    updt_param_item_t   md;
#ifdef _HAVE_FID
    updt_param_item_t   path;
#endif
    updt_param_item_t   fileclass; /* only never/always/periodic allowed */
} updt_params_t;

/**
 *  Check if the fileclass needs to be updated
 */
bool need_fileclass_update(const attr_set_t *p_attrs);

/**
 *  Check if path or metadata needs to be updated
 *  \param p_allow_event [out] if set to true, the path
 *         must be updated on related event.
 */
typedef enum { UPDT_PATH, UPDT_MD } type_info_t;
bool need_info_update(const attr_set_t *p_attrs, bool *update_on_event,
                      type_info_t type_info);

#define need_path_update(_pa, _pu)    need_info_update((_pa), (_pu), UPDT_PATH)
#define need_md_update(_pa, _pu)      need_info_update((_pa), (_pu), UPDT_MD)

/** config handlers */
extern mod_cfg_funcs_t updt_params_hdlr;

/** make parameters available to all modules */
extern updt_params_t updt_params;
/** @} */

#endif
