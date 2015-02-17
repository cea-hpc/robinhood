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

#define COMPRESS        (1 << 0)
#define USE_SENDFILE    (1 << 1)
#define NO_SYNC         (1 << 2)
#define COPYBACK        (1 << 3) /* retrieve a copy */

#define NOSYNC_HINT   "nosync"
#define CPBACK_HINT   "copyback"
#define COMPRESS_HINT "compress"

/* These functions are shared by several modules (namely common & backup). */
int builtin_copy(const char *src, const char *dst, int dst_flags,
                 bool save_attrs, int flags);

int hints2flags(const char *hints);


/* Public module interface */
const char *mod_get_name(void);

status_manager_t *mod_get_status_manager(void);

action_func_t mod_get_action_by_name(const char *action_name);

#endif
