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
 * @file   rbh_params.c
 * @author Thomas Leibovici
 * @author Henri Doreau
 * @brief  Handling a generic list of key/values.
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "rbh_params.h"
#include "rbh_misc.h"
#include "rbh_logs.h"

#include <malloc.h>
#include <string.h>
#include <errno.h>

#define PARAMS_TAG "rbh_params"

void rbh_params_free(struct rbh_params *params)
{
    if (params->param_set == NULL)
        return;

    g_hash_table_destroy(params->param_set);
    params->param_set = NULL;
}

const char *rbh_param_get(const struct rbh_params *params, const char *key)
{
    if (params == NULL || params->param_set == NULL)
        return NULL;

    return g_hash_table_lookup(params->param_set, key);
}

int rbh_param_set(struct rbh_params *params, const char *key,
                  const char *value, bool override)
{
    if (params->param_set == NULL) {
        params->param_set = g_hash_table_new_full(g_str_hash, g_str_equal, free,
                                                  free);
        if (params->param_set == NULL)
            return -ENOMEM;
    }

    if (!override && (g_hash_table_lookup(params->param_set, key) != NULL))
        return -EEXIST;

    /* use ght_replace, so that previous key and values are freed */
    g_hash_table_replace(params->param_set, strdup(key), strdup(value));

    return 0;
}

int rbh_list2params(struct rbh_params *params, const char **list,
                    bool key_values)
{
    const char **c;

    if (unlikely(params == NULL || list == NULL))
        return -EINVAL;

    /* allowate the hash table if necessary */
    if (params->param_set == NULL) {
        /* don't provide a free function for value as we only push
         * static const strings to it */
        params->param_set = g_hash_table_new_full(g_str_hash, g_str_equal, free,
                                                  key_values ? free : NULL);
        if (params->param_set == NULL)
            return -ENOMEM;
    }

    if (key_values)
        for (c = list; *c != NULL; c += 2)
            g_hash_table_replace(params->param_set, strdup(c[0]), strdup(c[1]));
    else
        for (c = list; *c != NULL; c++)
            /* Value must not be NULL */
            g_hash_table_replace(params->param_set, strdup(*c), "");

    return 0;
}

/** argument type for serialization functions */
struct serialize_args {
    GString                 *out_str;
    const struct rbh_params *exclude_set;
    rbh_param_flags_e        flags;
};

/** escape the given delimiter in source string */
static char *escape_delim(char *str, char delim, bool *free_it)
{
    char *c;

    if (strchr(str, delim) == NULL) {
        *free_it = false;
        return str;
    }

    c = malloc(2 * strlen(str) + 1);
    strcpy(c, str);
    str = c;
    *free_it = true;

    while ((c = strchr(c, delim)) != NULL) {
        /* shift the end of the string (including delimiter) */
        memmove(c + 1, c, strlen(c) + 1);
        /* escape it */
        *c = '\\';
        c += 2;
    }

    return str;
}

/** append a parameter to a CSV parameter serialization */
static int param2csv(const char *key, const char *val, void *udata)
{
    struct serialize_args *args = (struct serialize_args *)udata;
    bool free_key = false;
    bool free_val = false;

    /* skip ignored attrs */
    if (rbh_param_get(args->exclude_set, key) != NULL)
        return 0;

    /* if there is a comma in key or value, escape it */
    key = escape_delim((char *)key, ',', &free_key);
    val = escape_delim((char *)val, ',', &free_val);

    /* add comma delimiter if needed */
    if (!GSTRING_EMPTY(args->out_str))
        g_string_append(args->out_str,
                        (args->flags & RBH_PARAM_COMPACT) ? "," : ", ");

    /* append key=value */
    g_string_append_printf(args->out_str, "%s=%s", key, val);

    if (free_key)
        free((char *)key);
    if (free_val)
        free((char *)val);

    return 0;
}

int rbh_params_serialize(const struct rbh_params *params,
                         GString                 *str,
                         const struct rbh_params *exclude_set,
                         rbh_param_flags_e        flags)
{
    struct serialize_args args = {
        .out_str        = str,
        .exclude_set    = exclude_set,
        .flags          = flags
    };

    if (!(flags & RBH_PARAM_CSV))
        return -ENOTSUP;

    return rbh_params_foreach(params, param2csv, (void *)&args);
}

int rbh_params_foreach(const struct rbh_params *params, rbh_params_iter_t cb,
                       void *udata)
{
    GHashTableIter  iter;
    gpointer        key;
    gpointer        value;
    int             rc = 0;

    if (params == NULL)
        return -EINVAL;

    if (params->param_set == NULL)
        return 0;

    g_hash_table_iter_init(&iter, params->param_set);
    while (g_hash_table_iter_next(&iter, &key, &value)) {
        rc = cb((const char *)key, (const char *)value, udata);
        if (rc != 0)
            break;
    }

    return rc;
}

/** helper callback to duplicate a parameter set */
static int add_cb(const char *key, const char *val, void *udata)
{
    return rbh_param_set((action_params_t *)udata, key, val, true);
}

int rbh_params_copy(struct rbh_params *tgt, const struct rbh_params *src)
{
    return rbh_params_foreach(src, add_cb, tgt);
}
