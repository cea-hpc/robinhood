/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

#ifndef _RBH_FIND_H
#define _RBH_FIND_H

#define FIND_TAG "find"

extern attr_mask_t disp_mask;

const char *type2char(const char *type);
const char type2onechar(const char *type);

GArray *prepare_printf_format(const char *format);
void printf_entry(GArray *chunks, const wagon_t *id, const attr_set_t *attrs);
void free_printf_formats(GArray *chunks);

#endif
