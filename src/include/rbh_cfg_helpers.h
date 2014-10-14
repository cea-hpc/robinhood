/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2004-2009 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

/**
 *  \file rbh_cfg_helpers.h
 *  \brief Helper functions for parsing configuration values.
 *
 * \addtogroup CONFIG_PARSING
 * @{
 */

#ifndef _RBH_CFG_HLPR_H
#define _RBH_CFG_HLPR_H

#include "rbh_cfg.h"

/* parameter flags */
#define PFLG_MANDATORY             (1 << 0)

#define PFLG_ABSOLUTE_PATH         (1 << 1)
#define PFLG_REMOVE_FINAL_SLASH    (1 << 2)
#define PFLG_NO_WILDCARDS          (1 << 3)
#define PFLG_MAIL                  (1 << 4)
#define PFLG_STDIO_ALLOWED         (1 << 5)

/* for int and float params */
#define PFLG_POSITIVE              (1 << 6)
#define PFLG_NOT_NULL              (1 << 7)

/* float params only */
#define PFLG_ALLOW_PCT_SIGN        (1 << 8)

/* extra flags for values in policy expressions */
#define PFLG_ALLOW_ANY_DEPTH       (1 << 9) /* allow ** */
#define PFLG_NO_SLASH              (1 << 10)
#define PFLG_COMPARABLE            (1 << 11)
#define PFLG_NOT_EMPTY             PFLG_NOT_NULL
#define PFLG_XATTR                 (1 << 12)
#define PFLG_STATUS                (1 << 13) /* only allowed in some particular context (policy scope) */

/* ==== Tools for retrieving parameters from conf and checking them ==== */

/* constraint flags on parameters */

/**
 *  Retrieve a string parameter and check its format
 *  @return 0 on success
 *          ENOENT if the parameter does not exist in the block
 *          EINVAL if the parameter does not satisfy restrictions
 */
int            GetStringParam(config_item_t block, const char *block_name,
                              const char *var_name, int flags, char *target,
                              unsigned int target_size, char ***extra_args_tab,
                              unsigned int *nb_extra_args, char *err_msg);
/**
 *  Retrieve a boolean parameter and check its format
 *  @return 0 on success
 *          ENOENT if the parameter does not exist in the block
 *          EINVAL if the parameter does not satisfy restrictions
 */
int            GetBoolParam(config_item_t block, const char *block_name,
                            const char *var_name, int flags, bool *target,
                            char ***extra_args_tab, unsigned int *nb_extra_args,
                            char *err_msg );

/**
 *  Retrieve a duration parameter and check its format
 *  @return 0 on success
 *          ENOENT if the parameter does not exist in the block
 *          EINVAL if the parameter does not satisfy restrictions
 */
int            GetDurationParam(config_item_t block, const char *block_name,
                                const char *var_name, int flags, time_t *target,
                                char ***extra_args_tab,
                                unsigned int *nb_extra_args, char *err_msg);
/**
 *  Retrieve a size parameter and check its format
 *  @return 0 on success
 *          ENOENT if the parameter does not exist in the block
 *          EINVAL if the parameter does not satisfy restrictions
 */
int            GetSizeParam(config_item_t block, const char *block_name,
                            const char *var_name, int flags,
                            unsigned long long *target, char ***extra_args_tab,
                            unsigned int *nb_extra_args, char *err_msg);



/**
 *  Retrieve an integer parameter and check its format
 *  @return 0 on success
 *          ENOENT if the parameter does not exist in the block
 *          EINVAL if the parameter does not satisfy restrictions
 */
int            GetIntParam(config_item_t block, const char *block_name,
                           const char *var_name, int flags, int *target,
                           char ***extra_args_tab, unsigned int *nb_extra_args,
                           char *err_msg);

/**
 *  Retrieve a 64 bits integer parameter and check its format.
 *  (a suffix can be used in config file).
 *  @return 0 on success
 *          ENOENT if the parameter does not exist in the block
 *          EINVAL if the parameter does not satisfy restrictions
 */
int GetInt64Param(config_item_t block, const char *block_name,
                  const char *var_name, int flags, uint64_t *target,
                  char ***extra_args_tab, unsigned int *nb_extra_args,
                  char *err_msg);

/**
 *  Retrieve a float parameter and check its format
 *  @return 0 on success
 *          ENOENT if the parameter does not exist in the block
 *          EINVAL if the parameter does not satisfy restrictions
 */
int           GetFloatParam(config_item_t block, const char *block_name,
                            const char *var_name, int flags, double *target,
                            char ***extra_args_tab, unsigned int *nb_extra_args,
                            char *err_msg);



/**
 * Types and function to parse a list of simple scalar configuration variables (with no extra args).
 */
typedef enum {
    PT_STRING,
    PT_BOOL,
    PT_DURATION,
    PT_SIZE,
    PT_INT,
    PT_INT64,
    PT_FLOAT,
    PT_TYPE
} cfg_param_type;

/** generic config parsing using structure {type, name, flags, tgtptr} */

typedef struct cfg_param_t {
    const char     *name; /* NULL for last name */
    cfg_param_type  type;
    int             flags;
    void           *ptr;
    size_t          ptrsize;
} cfg_param_t;
#define END_OF_PARAMS {NULL, 0, 0, NULL, 0}

int read_scalar_params(config_item_t block, const char *block_name, 
                       const cfg_param_t * params, char *msgout);

/**
 * Build a policy boolean expression from the given block
 * \param smi(in) when specifying a policy scope, indicate the
 *                related status manager ('status' criteria is policy dependant).
 */
#include "rbh_boolexpr.h"
struct sm_instance;
int            GetBoolExpr(config_item_t block, const char *block_name,
                           bool_node_t *p_bool_node, uint64_t *p_attr_mask,
                           char *err_msg, const struct sm_instance *smi);

/**
 * Build a policy boolean expression from a union/intersection of filesets
 */
struct policies_t;
int            GetSetExpr(config_item_t block, const char *block_name,
                          bool_node_t * p_bool_node,
                          uint64_t *p_attr_mask, const struct policies_t *policies,
                          char *err_msg);

/**
 * Check that no unknown parameter or block is found.
 * @param param_array NULL terminated array of allowed parameters.
 */
void           CheckUnknownParameters( config_item_t block, const char *block_name,
                                       const char **param_array );


#endif


/** @} */
