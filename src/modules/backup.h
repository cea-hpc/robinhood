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

/**
 * \file   backup.h
 * \author Th. Leibovici
 * \brief  Interface to the backup module (status manager and actions).
 */
#ifndef _MOD_BACKUP_H
#define _MOD_BACKUP_H

#include "status_manager.h"

extern status_manager_t backup_sm;

/* exported actions of the backup module */
int backup_remove(const entry_id_t *id, attr_set_t *attrs,
                  const char *hints, post_action_e *what_after,
                  db_cb_func_t db_cb_fn, void *db_cb_arg);
#endif
