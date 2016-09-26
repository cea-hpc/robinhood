/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2014 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "mod_internal.h"
#include "list_mgr.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "rbh_modules.h"
#include "policy_rules.h"
#include "status_manager.h"
#include "Memory.h"
#include "rbh_params.h"
#include <unistd.h>
#include <utime.h>
#include <fcntl.h>
#include <sys/sendfile.h>
#include <zlib.h>

/** perform a standard unlink() action */
static int common_unlink(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                         const action_params_t *params, post_action_e *after,
                         db_cb_func_t db_cb_fn, void *db_cb_arg)
{
    const char *path = NULL;

    *after = PA_UPDATE;

    if (ATTR_MASK_TEST(p_attrs, fullpath))
        path = ATTR(p_attrs, fullpath);
    else
        return EINVAL;

    if (unlink(path) != 0 && errno != ENOENT)
        return errno;

    /* 1 less link */
    *after = PA_RM_ONE;
    return 0;
}

/** perform a standard rmdir() action */
static int common_rmdir(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                        const action_params_t *params, post_action_e *after,
                        db_cb_func_t db_cb_fn, void *db_cb_arg)
{
    const char *path = NULL;

    *after = PA_UPDATE;

    if (ATTR_MASK_TEST(p_attrs, fullpath))
        path = ATTR(p_attrs, fullpath);
    else
        return EINVAL;

    if (rmdir(path) != 0 && errno != ENOENT)
        return errno;

    /* no hardlink for dirs */
    *after = PA_RM_ALL;
    return 0;
}

/** just log it! */
static int common_log(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                      const action_params_t *params, post_action_e *after,
                      db_cb_func_t db_cb_fn, void *db_cb_arg)
{
    GString *params_str = g_string_new("");

    if (rbh_params_serialize(params, params_str, NULL,
                             RBH_PARAM_CSV | RBH_PARAM_COMPACT))
        /* ignore (just for logging) */
        g_string_assign(params_str, "ERROR");

    DisplayLog(LVL_MAJOR, "LogAction", "fid=" DFID ", path=%s, params={%s}",
               PFID(p_entry_id),
               ATTR_MASK_TEST(p_attrs, fullpath) ? ATTR(p_attrs, fullpath) : "",
               params_str->str);
    g_string_free(params_str, TRUE);

    *after = PA_UPDATE;
    return 0;
}

/** standard copy of file contents and its attributes */
static int common_copy(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                       const action_params_t *params, post_action_e *after,
                       db_cb_func_t db_cb_fn, void *db_cb_arg)
{
    int rc;
    copy_flags_e flags = params2flags(params);
    const char *targetpath = rbh_param_get(params, TARGET_PATH_PARAM);

    /* flags for restore vs. flags for archive */
    int oflg = (flags & CP_COPYBACK) ? O_WRONLY : O_WRONLY | O_CREAT | O_TRUNC;

    /* actions expect to get a source path in 'fullpath' and a targetpath
     * in params */
    if (!ATTR_MASK_TEST(p_attrs, fullpath) || (targetpath == NULL)) {
        DisplayLog(LVL_MAJOR, CP_TAG,
                   "Missing mandatory attribute to perform file copy "
                   "(fullpath or backendpath)");
        return -EINVAL;
    }

    rc = builtin_copy(ATTR(p_attrs, fullpath), targetpath,
                      oflg, !(flags & CP_COPYBACK), flags);
    *after = PA_UPDATE;
    return rc;
}

/** copy file contents using sendfile() */
static int common_sendfile(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                           const action_params_t *params,
                           post_action_e *after, db_cb_func_t db_cb_fn,
                           void *db_cb_arg)
{
    int rc;
    copy_flags_e flags = params2flags(params);
    const char *targetpath = rbh_param_get(params, TARGET_PATH_PARAM);

    /* flags for restore vs. flags for archive */
    int oflg = (flags & CP_COPYBACK) ? O_WRONLY : O_WRONLY | O_CREAT | O_TRUNC;

    /* actions expect to get a source path in 'fullpath' and a targetpath in
     * params */
    if (!ATTR_MASK_TEST(p_attrs, fullpath) || (targetpath == NULL)) {
        DisplayLog(LVL_MAJOR, CP_TAG,
                   "Missing mandatory attribute to perform file copy (fullpath or backendpath)");
        return -EINVAL;
    }

    rc = builtin_copy(ATTR(p_attrs, fullpath), targetpath, oflg,
                      !(flags & CP_COPYBACK), flags | CP_USE_SENDFILE);
    *after = PA_UPDATE;
    return rc;
}

/** copy and compress file contents */
static int common_gzip(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                       const action_params_t *params, post_action_e *after,
                       db_cb_func_t db_cb_fn, void *db_cb_arg)
{
    int rc;
    copy_flags_e flags = params2flags(params);
    const char *targetpath = rbh_param_get(params, TARGET_PATH_PARAM);

    /* flags for restore vs. flags for archive */
    int oflg = (flags & CP_COPYBACK) ? O_WRONLY : O_WRONLY | O_CREAT | O_TRUNC;

    /* actions expect to get a source path in 'fullpath' and a targetpath in
     * params */
    if (!ATTR_MASK_TEST(p_attrs, fullpath) || (targetpath == NULL)) {
        DisplayLog(LVL_MAJOR, CP_TAG,
                   "Missing mandatory attribute to perform file copy "
                   "(fullpath or backendpath)");
        return -EINVAL;
    }

    rc = builtin_copy(ATTR(p_attrs, fullpath), targetpath, oflg,
                      !(flags & CP_COPYBACK), flags | CP_COMPRESS);
    *after = PA_UPDATE;
    return rc;
}

const char *mod_get_name(void)
{
    return "common";
}

status_manager_t *mod_get_status_manager(void)
{
    return NULL;
}

action_func_t mod_get_action_by_name(const char *action_name)
{
    if (strcmp(action_name, "common.unlink") == 0)
        return common_unlink;
    else if (strcmp(action_name, "common.rmdir") == 0)
        return common_rmdir;
    else if (strcmp(action_name, "common.log") == 0)
        return common_log;
    else if (strcmp(action_name, "common.copy") == 0)
        return common_copy;
    else if (strcmp(action_name, "common.sendfile") == 0)
        return common_sendfile;
    else if (strcmp(action_name, "common.gzip") == 0)
        return common_gzip;
    else
        return NULL;
}
