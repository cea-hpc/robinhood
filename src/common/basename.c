/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2015 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
/**
 * Ensure we use the GNU version of basename.
 */
#define _GNU_SOURCE
#include <string.h>

#include "rbh_basename.h"

/* GNU basename never modifies input argument */
const char *rh_basename(const char *path)
{
    return basename(path);
}
