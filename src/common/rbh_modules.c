/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2014-2015 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

/**
 * \file   rbh_modules.c
 * \author Henri Doreau
 * \brief  Dynamic modules management
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "rbh_modules.h"
#include "global_config.h"
#include "rbh_logs.h"
#include "rbh_misc.h"

#include <ctype.h>
#include <dlfcn.h>
#include <errno.h>
#include <string.h>
#include <assert.h>

/**
 * Logging domain tag
 */
#define MODULE_TAG  "mod_mgt"

/**
 * Maximum module name, including final null terminator.
 */
#define MAX_MOD_NAMELEN 128

/**
 * Global module list and associated size.
 * Re-allocated when extended.
 */
static rbh_module_t *mod_list;
static int           mod_count;

static const char *module_get_name(const rbh_module_t *mod)
{
    if (mod == NULL || mod->mod_ops.mod_get_name == NULL)
        return NULL;

    return mod->mod_ops.mod_get_name();
}

static int module_get_version(const rbh_module_t *mod)
{
    if (mod == NULL || mod->mod_ops.mod_get_version == NULL)
        return 0;

    return mod->mod_ops.mod_get_version();
}

/**
 * Load a given symbol from dlopened library.
 *
 * \param[in]  mod         Module descriptor.
 * \param[in]  sym_name    Name of symbol to be loaded.
 * \param[out] sym_addr    Points to an address where the symbol is to be
 *                         stored.
 * \param[in]  required    Indicate if the symbol definition is required.
 *
 * \return 0 on success, negative error code on failure
 */
static int module_sym_load(rbh_module_t *mod, const char *sym_name,
                           void **sym_addr, bool required)
{
    char *errstr;

    *sym_addr = dlsym(mod->sym_hdl, sym_name);

    errstr = dlerror();
    if (errstr != NULL) {
        if (required) {
            DisplayLog(LVL_CRIT, MODULE_TAG,
                       "Cannot load %s from module %s: %s",
                       sym_name, mod->name, errstr);
            return -EINVAL;
        }
        /* not mandatory, only display in DEBUG level */
        DisplayLog(LVL_DEBUG, MODULE_TAG,
                   "Module '%s': optional symbol '%s' not found: %s",
                   mod->name, sym_name, errstr);
    }

    return 0;
}

/**
 * dlopen() library and initialize module descriptor accordingly
 *
 * \param[in]   libfile Path to dlopen
 * \param[out]  mod     Robinhood module descriptor to initialize
 *
 * \return 0 on success, negative error code on failure
 */
static int module_load_from_file(const char *libfile, rbh_module_t *mod)
{
    int rc;
    int i;
    struct symbol_descr {
      const char  *sym_name; /**< Exposed symbol name */
      void        *sym_addr; /**< Destination address */
      bool         sym_reqd; /**< Whether the symbol is required */
    } mod_symbols[] = {
      {"mod_get_name",           &mod->mod_ops.mod_get_name,           true},
      {"mod_get_version",        &mod->mod_ops.mod_get_version,        true},
      {"mod_get_status_manager", &mod->mod_ops.mod_get_status_manager, false},
      {"mod_get_action",         &mod->mod_ops.mod_get_action,         false},
      {"mod_get_scheduler",      &mod->mod_ops.mod_get_scheduler,      false},
    };

    if (libfile == NULL)
        return -EINVAL;

    memset(mod, 0, sizeof(*mod));

    mod->sym_hdl = dlopen(libfile, RTLD_NOW | RTLD_LOCAL | RTLD_DEEPBIND);
    if (mod->sym_hdl == NULL) {
        DisplayLog(LVL_CRIT, MODULE_TAG, "Cannot dlopen() '%s': %s",
                   libfile, dlerror());
        return -EINVAL;
    }

    /* Use the filename as module name until loading is done and successful */
    mod->name = libfile;

    for (i = 0; i < ARRAY_SIZE(mod_symbols); i++) {
        struct symbol_descr *sym = &mod_symbols[i];

        rc = module_sym_load(mod, sym->sym_name, sym->sym_addr, sym->sym_reqd);
        if (rc)
            goto err_out;

    }

    /* Get direct reference to the module name for faster accesses */
    mod->name = module_get_name(mod);
    mod->version = module_get_version(mod);
    if (mod->version != RBH_MODULE_VERSION) {
        DisplayLog(LVL_CRIT, MODULE_TAG, "Module '%s': incompatible version. "
                   "version %d != expected version %d", mod->name, mod->version,
                   RBH_MODULE_VERSION);
        rc = -EPROTO;
        goto err_out;
    }

    DisplayLog(LVL_DEBUG, MODULE_TAG, "Successfully loaded module '%s'",
               mod->name);

    return 0;

err_out:
    dlclose(mod->sym_hdl);
    mod->sym_hdl = NULL;
    return rc;
}

/**
 * Build module library name from its reduced form.
 * "lhsm" -> "librbh_mod_lhsm.so".
 * Directory handling is to be defined by the LD_LIBRARY_PATH if needed.
 *
 * \param[out]  dst     Destination buffer, of at least MAX_MOD_NAMELEN bytes
 * \param[in]   name    Module short name
 *
 * \return 0 on success, negative error code on failure
 */
static int module_fullname_build(char *dst, const char *name)
{
    int rc;
    int i;

    if (name == NULL)
        return -EINVAL;

    rc = snprintf(dst, MAX_MOD_NAMELEN - 1, "librbh_mod_%s.so", name);
    if (rc >= MAX_MOD_NAMELEN)
        return -ENAMETOOLONG;

    for (i = 0; i < rc; i++)
        dst[i] = tolower(dst[i]);

    return 0;
}

/**
 * Resize module list and invoke module initialization code
 *
 * \param[in]   name    Module short name
 *
 * \return 0 on success, negative error code on failure
 */
static int module_load(const char *name)
{
    char            mod_name[MAX_MOD_NAMELEN];
    rbh_module_t   *new_objects;
    int             rc;

    rc = module_fullname_build(mod_name, name);
    if (rc < 0)
        return rc;

    assert(mod_count >= 0);

    new_objects = (rbh_module_t *) calloc(mod_count + 1, sizeof(rbh_module_t));
    if (new_objects == NULL)
        return -ENOMEM;

    rc = module_load_from_file(mod_name, &new_objects[0]);
    if (rc < 0)
        goto err_out;

    memcpy(&new_objects[1], mod_list, mod_count * sizeof(rbh_module_t));
    mod_count++;

    free(mod_list);
    mod_list = new_objects;

    return 0;

 err_out:
    free(new_objects);
    return rc;
}

/**
 * Release resources associated to a single module. Note that the mod_list
 * is not resized.
 *
 * \param[in, out]  mod Module descriptor to release
 *
 * \return 0 on success, negative error code on failure
 */
static int module_unload(rbh_module_t *mod)
{
    if (mod->name != NULL)
        DisplayLog(LVL_DEBUG, MODULE_TAG, "Unloading module %s", mod->name);

    if (mod->sym_hdl == NULL) {
        DisplayLog(LVL_VERB, MODULE_TAG, "Module already unloaded, ignoring");
        return 0;   /* -EALREADY ? */
    }

    dlclose(mod->sym_hdl);
    mod->sym_hdl = NULL;
    return 0;
}

/**
 * Release all resources associated to dynamic modules
 *
 * \return 0 on success, negative error code on failure
 */
int module_unload_all(void)
{
    int i;
    int rc;
    int rc_save = 0;

    assert(mod_count >= 0);

    for (i = 0; i < mod_count; i++) {
        rc = module_unload(&mod_list[i]);
        if (rc != 0 && rc_save == 0)
            rc_save = rc;
    }

    free(mod_list);
    mod_list = NULL;
    mod_count = 0;

    return rc_save;
}

/**
 * Get a module descriptor, load the corresponding module if needed.
 *
 * \param[in]   mod_name    Module short name
 *
 * \return NULL on error, pointer to an allocated/initialized module
 *         descriptor on success.
 */
static rbh_module_t *module_get(const char *mod_name)
{
    int i;
    int rc;

    assert(mod_count >= 0);

 again:
    for (i = 0; i < mod_count; i++) {
        if (strcasecmp(mod_list[i].name, mod_name) == 0)
            return &mod_list[i];
    }

    rc = module_load(mod_name);
    if (rc == 0)
        goto again;

    return NULL;
}

action_func_t module_get_action(const char *name)
{
    char             mod_name[MAX_MOD_NAMELEN];
    char            *prefix;
    rbh_module_t    *mod;

    prefix = strchr(name, '.');
    if (prefix == NULL)
        return NULL;

    memcpy(mod_name, name, prefix - name);
    mod_name[prefix - name] = '\0';

    mod = module_get(mod_name);
    if (mod == NULL || mod->mod_ops.mod_get_action == NULL)
        return NULL;

    return mod->mod_ops.mod_get_action(name);
}

status_manager_t *module_get_status_manager(const char *name)
{
    rbh_module_t    *mod;

    mod = module_get(name);
    if (mod == NULL || mod->mod_ops.mod_get_status_manager == NULL)
        return NULL;

    return mod->mod_ops.mod_get_status_manager();
}

action_scheduler_t *module_get_scheduler(const char *name)
{
    char             mod_name[MAX_MOD_NAMELEN];
    char            *prefix;
    rbh_module_t    *mod;

    prefix = strchr(name, '.');
    if (prefix == NULL)
        return NULL;

    memcpy(mod_name, name, prefix - name);
    mod_name[prefix - name] = '\0';

    mod = module_get(mod_name);
    if (mod == NULL || mod->mod_ops.mod_get_scheduler == NULL)
        return NULL;

    return mod->mod_ops.mod_get_scheduler(name);
}

int module_get_chglog_postproc(const char *name, void **sym_addr)
{
    rbh_module_t    *mod;
    chglog_postproc_t *(*mod_get_changelog_postproc)(void);
    char *errstr;

    mod = module_get(name);
    if (mod == NULL)
        return -EINVAL;

    /* Clear any errors */
    (void) dlerror();

    mod_get_changelog_postproc = dlsym(mod->sym_hdl,
                                       "mod_get_changelog_postproc");
    errstr = dlerror();
    if (errstr != NULL)
        return -EINVAL;

    *sym_addr = mod_get_changelog_postproc;

    return 0;
}
