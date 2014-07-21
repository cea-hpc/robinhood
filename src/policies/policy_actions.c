/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009, 2010 CEA/DAM
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

#include "list_mgr.h"
#include "policy_run.h"
#include "RobinhoodLogs.h"
#include "RobinhoodMisc.h"
#ifdef _HSM_LITE
#include "backend_mgr.h"
#endif

#ifdef _LUSTRE_HSM
/* XXX to be included to LHSM action module */

static int lhsm_release(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                        const char *hints, post_action_e *after)
{
    int rc = LustreHSM_Action(HUA_RELEASE, p_entry_id, hints);
    //    if (rc == 0)
    //{
    /* TODO set new status */
    //    ATTR_MASK_SET( &new_attr_set, status );
    //    ATTR( &new_attr_set, status ) = STATUS_ARCHIVE_RUNNING;
    //}

    *after = PA_UPDATE;
    return rc;
}

static int lhsm_archive(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                        const char *hints, post_action_e *after)
{
    int rc = LustreHSM_Action(HUA_ARCHIVE, p_entry_id, hints);
    *after = PA_UPDATE;
    return rc;
}
#endif

#ifdef _HSM_LITE
#ifdef HAVE_SHOOK
/* XXX to be included to shook module */
static int shook_release(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                         const char *hints, post_action_e *after)
{
    int rc = rbhext_release(p_entry_id, p_attrs);
    *after = PA_UPDATE;
    return rc;
}
#endif

/* XXX shook_archive == backup_archive ? */

/* XXX to be included to backup module */
static int backup_archive(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                          const char *hints, post_action_e *after)
{
    *after = PA_UPDATE;
    return rbhext_archive(p_entry_id, p_attrs, hints);
}
#endif

/* XXX to be included to common module */
static int common_unlink(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                         const char *hints, post_action_e *after)
{
    const char *path = NULL;

    *after = PA_UPDATE;

    if (ATTR_MASK_TEST(p_attrs,fullpath))
        path = ATTR(p_attrs,fullpath);
    else
        return EINVAL;

    if (unlink(path) != 0)
        return errno;

    *after = PA_RM_ONE;
    return 0;
}

/* just log! */
static int common_log(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                      const char *hints, post_action_e *after)
{
    DisplayLog(LVL_MAJOR, "LogAction", "fid="DFID", path=%s, hints=%s",
               PFID(p_entry_id),
               ATTR_MASK_TEST(p_attrs,fullpath)?ATTR(p_attrs,fullpath):"",
               hints?hints:"");

    *after = PA_UPDATE;
    return 0;
}

static struct fn_names_t
{
    const char    *name;
    action_func_t  func;
} fn_names[] = {
    {"common.unlink", common_unlink},
    {"common.log",    common_log},
#ifdef _LUSTRE_HSM
    {"lhsm.archive", lhsm_archive},
    {"lhsm.release", lhsm_release},
#endif
#ifdef _HSM_LITE
    {"backup.archive", backup_archive},
#ifdef HAVE_SHOOK
    {"shook.archive", backup_archive},
    {"shook.release", shook_release},
#endif
#endif
    {NULL, NULL}
};

action_func_t action_name2function(const char *fname)
{
    struct fn_names_t *curr;
    for (curr = fn_names; curr->name != NULL; curr++)
    {
        if (!strcasecmp(fname, curr->name))
            return curr->func;
    }
    return NULL;
}
