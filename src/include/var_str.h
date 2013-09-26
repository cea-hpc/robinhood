/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2013 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL-C License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL-C license (http://www.cecill.info) and that you
 * accept its terms.
 */

/**
 * \file  var_str.h
 * \brief manage strings of variable size
 */

#ifndef _VAR_STR_H
#define _VAR_STR_H

#include "Memory.h"
#include <string.h>

typedef struct var_str {
    char *ptr;
    char *last_char;
    unsigned int buff_len;
} var_str;

#define VAR_STR_NULL {NULL, NULL, 0}
#define VAR_STR_START(_v) (_v).ptr
#define VAR_STR_CURR(_v) (_v).last_char
static void inline var_str_free(var_str *pv)
{
    if (pv->ptr != NULL)
        MemFree(pv->ptr);
}
static void inline var_str_reset(var_str *pv)
{
    if (pv->ptr)
        pv->ptr[0] = '\0';
    pv->last_char = pv->ptr;
}

static int inline var_str_append(var_str *pv, const char *str)
{
    ptrdiff_t written = (ptrdiff_t)pv->last_char - (ptrdiff_t)pv->ptr;
    ptrdiff_t remaining = pv->buff_len - written;
    size_t add = strlen(str);

    if (add + 1 > remaining)
    {
        /* next power of 2 (min is 256, to fit with most requests) */
        size_t newsz = (pv->buff_len == 0 ? 256 : pv->buff_len << 1);
        while (newsz < written + add + 1)
            newsz <<= 1;

        /* realloc */
        pv->ptr = MemRealloc(pv->ptr, newsz);
        if (pv->ptr == NULL)
            return -ENOMEM;
        /* update last char according to the new pointer location */
        pv->last_char = pv->ptr + written;
        pv->buff_len = newsz;
    }
    /* append the string */
    strcpy(pv->last_char, str);
    pv->last_char += add;
    return 0;
}

#endif
