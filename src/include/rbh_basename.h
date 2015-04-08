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
#ifndef _RBH_BASENAME_H
#define _RBH_BASENAME_H

/**
 * Ensure to use the GNU version of basename
 * that does not modify its input argument.
 */
const char *rh_basename(const char *path);

#endif
