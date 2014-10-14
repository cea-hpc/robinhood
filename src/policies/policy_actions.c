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

#include "policy_run.h"

#include "../modules/common_actions.h" /** FIXME drop this when dynamic module management is implemented (for actions) */

#ifdef _LUSTRE_HSM
#include "../modules/lhsm.h" /** FIXME drop this when dynamic module management is implemented (for status managers) */
#endif
#ifdef _HSM_LITE
#include "../modules/backup.h" /** FIXME drop this when dynamic module management is implemented (for status managers) */
#endif

static struct fn_names_t
{
    const char    *name;
    action_func_t  func;
} fn_names[] = {
    {"common.unlink", common_unlink},
    {"common.log",    common_log},
    {"common.copy",   common_copy}, /* allow hint: nosync, copyback */
    {"common.sendfile", common_sendfile}, /* allow hint: nosync, copyback */
    {"common.gzip",   common_gzip}, /* mandatory if backend.compress is enabled. allow hint: nosync, copyback (uncompress) */
#ifdef _LUSTRE_HSM
    {"lhsm.archive", lhsm_archive},
    {"lhsm.release", lhsm_release},
    {"lhsm.hsm_remove", lhsm_remove},
#endif
#ifdef HAVE_SHOOK
    {"shook.release", rbh_shook_release},
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
