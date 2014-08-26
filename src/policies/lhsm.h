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

#include <stdbool.h>
#include "list_mgr.h" /* for common robinhood types: entry_id_t, stripe_info_t... */

typedef enum {
  STATUS_UNKNOWN = 0,           /* undetermined status */
  STATUS_NEW,                   /* file has no HSM flags (just created) */
  STATUS_MODIFIED,              /* file must be archived */
  STATUS_RESTORE_RUNNING,       /* file is being retrieved */
  STATUS_ARCHIVE_RUNNING,       /* file is being archived */
  STATUS_SYNCHRO,               /* file has been synchronized in HSM, file can be purged */
  STATUS_RELEASED,              /* file is released (nothing to do). XXX should not be in DB? */
  STATUS_RELEASE_PENDING,      /* file is being released */

  STATUS_COUNT                  /* number of possible file status */
} hsm_status_t;

int lhsm_get_status(const char *path, hsm_status_t *p_status,
                    bool *no_release, bool *no_archive);

/** Trigger a HSM action
 * \param action HUA_ARCHIVE, HUA_RESTORE, HUA_RELEASE, HUA_REMOVE, HUA_CANCEL
 */
int lhsm_action(enum hsm_user_action action, const entry_id_t *p_id,
                const char *hints);

#endif
