/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009-2014 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

/**
 * \file lhsm.h
 * \brief Lustre/HSM specific functions.
 */
#ifndef _LHSM_H
#define _LHSM_H

#include "status_manager.h"

/** @TODO to be loaded as a dynamic module */
extern status_manager_t lhsm_sm;

/** Lustre/HSM actions.
    @TODO load them from a dynamic module */
int lhsm_archive(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                 const char *hints, post_action_e *after,
                 db_cb_func_t db_cb_fn, void *db_cb_arg);
int lhsm_release(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                 const char *hints, post_action_e *after,
                 db_cb_func_t db_cb_fn, void *db_cb_arg);
int lhsm_remove(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                const char *hints, post_action_e *after,
                db_cb_func_t db_cb_fn, void *db_cb_arg);
#endif
