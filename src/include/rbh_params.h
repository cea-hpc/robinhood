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
 * @file   rbh_params.h
 * @author Thomas Leibovici
 * @author Henri Doreau
 * @brief  Handling a generic list of key/values.
 */

#ifndef _RBH_PARAMS_H
#define _RBH_PARAMS_H

#include <glib.h>
#include <stdbool.h>

struct rbh_params {
    GHashTable *param_set;
};

typedef int (*rbh_params_iter_t)(const char *key, const char *val, void *udata);

/**
 * Create or update a key-value item in the param set
 * @param override (bool) allow overriding a previous value.
 */
int rbh_param_set(struct rbh_params *params, const char *key,
                  const char *value, bool override);

/** get a key-value item by key name */
const char *rbh_param_get(const struct rbh_params *params, const char *key);

/** empty the parameter list and release memory */
void rbh_params_free(struct rbh_params *params);

/** behavior flags for rbh_params_serialize() */
typedef enum {
    RBH_PARAM_CSV       = (1 << 0), /**< dump attrs to CSV format */
    RBH_PARAM_COMPACT   = (1 << 1)  /**< generate a compact output
                                         (e.g. eliminates superfluous spaces) */
} rbh_param_flags_e;

/**
 * Convert a list of variables to a parameter set that can be used
 * for instance as exclude_set parameter of rbh_params_serialize().
 * This allows more efficient searches in this set.
 * @param[out]  params Output parameter set.
 * @param[in]   list   List of parameter names (list terminated by NULL).
 * @param[in]   key_values List items are {key1, value1, key2, value2, ...}
 */
int rbh_list2params(struct rbh_params *params, const char **list,
                    bool key_values);

/**
 * Serialize a parameter list to a string.
 *
 * @param[in]     params The parameter list to serialize.
 * @param[in/out] str    Allocated GString to write the parameters to.
 * @param[in]     exclude_set Set of parameters to exclude from output
 * @param[in]     flags  Behavior flags for the serialization.
 *
 * @return 0 on success, non-zero value on failure.
 */
int rbh_params_serialize(const struct rbh_params *params, GString *str,
                         const struct rbh_params *exclude_set,
                         rbh_param_flags_e flags);

/**
 * Invoke a callback on all items of the parameter set.
 * Iteration stops if the callback returns a non-zero value,
 * which is then propagated back to the caller.
 *
 * @param[in]       params  The parameter set to iterate over.
 * @param[in]       cb      The processing callback.
 * @param[in,out]   udata   User data to be passed in to the callback.
 *
 * @return 0 on success or first non-zero value returned by the callback.
 */
int rbh_params_foreach(const struct rbh_params *params, rbh_params_iter_t cb,
                       void *udata);


/**
 * copy source parameters to a target parameter set.
 */
int rbh_params_copy(struct rbh_params *tgt, const struct rbh_params *src);

#endif
