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

typedef enum {
    CP_COMPRESS        = (1 << 0),
    CP_USE_SENDFILE    = (1 << 1),
    CP_NO_SYNC         = (1 << 2),
    CP_COPYBACK        = (1 << 3) /* retrieve a copy */
} copy_flags_e;

/** These functions are shared by several modules (namely common & backup). */
int builtin_copy(const char *src, const char *dst, int dst_oflags,
                 bool save_attrs, copy_flags_e flags);

/** set copy flags from a parameter set */
copy_flags_e params2flags(const action_params_t *params);

/* Public module interface */
const char *mod_get_name(void);

status_manager_t *mod_get_status_manager(void);

action_func_t mod_get_action_by_name(const char *action_name);

#endif
