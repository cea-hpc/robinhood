/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2015-2016 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

/**
 * \file   modules.h
 * \author Henri Doreau
 * \brief  Dynamic modules management
 */

#ifndef _RBH_MODULES_H
#define _RBH_MODULES_H

#include "policy_rules.h"
#include "policy_run.h"
#include "status_manager.h"
#include "chglog_postproc.h"


/**
 * Operations exposed by robinhood dynamic modules. These should be invoked
 * using the static inline wrappers defined below.
 *
 * 'mod_get_name' and 'mod_get_version' are mandatory.
 * Other operations are optional.
 */
struct rbh_module_operations {
    const char         *(*mod_get_name)(void);
    int                 (*mod_get_version)(void);
    status_manager_t   *(*mod_get_status_manager)(void);
    action_func_t       (*mod_get_action)(const char *);
    action_scheduler_t *(*mod_get_scheduler)(const char *);
};

/** current version of modules */
#define RBH_MODULE_VERSION  1

typedef struct rbh_module {
    const char                      *name;      /**< Module name */
    int                              version;   /**< Module version */
    void                            *sym_hdl;   /**< Private dlsym handle */
    struct rbh_module_operations     mod_ops;   /**< Module operation vector */
} rbh_module_t;


/**
 * Get the status manager associated to a robinhood dynamic module. This
 * function will dlopen() the appropriate module if need be. The library handle
 * will then remain cached until module_unload_all() is called.
 *
 * \param[in] name Module name from which to acquire the SM.
 *
 * \return The status manager or NULL on error
 */
status_manager_t *module_get_status_manager(const char *name);

/**
 * Get the changelog post-processor associated to a robinhood dynamic module.
 * This function will dlopen() the appropriate module if necessary. The library
 * handle will then remain cached until module_unload_all() is called.
 *
 * \param[in] name Module name from which to acquire the Chnagelog
 *                 post-processor.
 * \param[out] sym_addr Address of the Changelog post-processor.
 *
 * \return 0 on success, -EINVAL on failure
 */
int module_get_chglog_postproc(const char *name, void **sym_addr);

/**
 * Get an action function from a robinhood dynamic module. Actions function
 * names are of the form <module_name>.<action>. We expect the whole string
 * here. This function will dlopen() the appropriate module if need be. The
 * library handle will then remain cached until module_unload_all() is called.
 *
 * \param[in] name  The function name, <module_name>.<action>
 *
 * \return A pointer to the desired function or NULL if no suitable action
 *         of this name was found.
 */
action_func_t module_get_action(const char *name);

/**
 * Get an action scheduler from a robinhood dynamic module.
 * Scheduler are names of the form <module_name>.<sched_name>.
 * We expect the whole string here.
 * This function will dlopen() the appropriate module if needed.
 * The library handle will then remain cached until module_unload_all() is
 * called.
 *
 * \param[in] name  The scheduler name: <module_name>.<sched_name>.
 *
 * \return A pointer to the desired scheduler or NULL if no item matches this
 *         name.
 */
action_scheduler_t *module_get_scheduler(const char *name);

/**
 * Release resources associated to robinhood dynamic modules.
 *
 * \return 0 on success, appropriate negative error code on failure
 */
int module_unload_all(void);

#endif
