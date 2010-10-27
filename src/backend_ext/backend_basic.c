/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2010 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

/**
 * \file   backend_basic.c
 * \author Th. Leibovici
 * \brief  basic backend implementation
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "backend_ext.h"
#include "RobinhoodLogs.h"
#include <stdlib.h>

#define RBHEXT_TAG "Ext"

/**
 * Get compatibility information,
 * to check compatibility with the current FS.
 */
int rbhext_compat_flags()
{
    int compat_flags = 0;

    /* if entry id is fid, this module is only compatible with Lustre filesystems */
#ifdef FID_PK
    compat_flags |= RBHEXT_COMPAT_LUSTRE;
#endif

    return compat_flags;
}


/* options for getsubopt */
enum
{
    OPT_BACKEND_ROOT,
    OPT_COPYTOOL,
};

const char *opts[] =
{
    [OPT_BACKEND_ROOT] = "root",
    [OPT_COPYTOOL] = "action_cmd",
    NULL
};

static struct backend_config
{
    char root[MAXPATHLEN];
    char action_cmd[MAXPATHLEN];
} config = {
    .root = "/backend",
    .action_cmd = "/usr/sbin/rbhext_tool"
};


/**
 * Initialize the extension module.
 * \param[in] config_string specific config string (e.g path to config file...)
 * \param[out] p_behaviors_flags pointer to output mask that describes extension behavior
 */
int rbhext_init( const char * config_string,
                 unsigned int * p_behaviors_flags )
{
    char subopts[1024];
    char * curr;
    char * value;

    if ( strlen( config_string ) >= 1024 )
        return -E2BIG;
    strcpy(subopts, config_string);

    /* the config string is in the getsubopt format: opt1=xxx,opt2,opt3=yyy */
    curr = subopts;
    while (curr != '\0')
    {
        switch( getsubopt(&curr, opts, &value) )
        {
            case OPT_BACKEND_ROOT:
                if (value == NULL)
                {
                    DisplayLog(LVL_CRIT, RBHEXT_TAG, "Expected value for suboption '%s'",
                    opts[OPT_BACKEND_ROOT] );
                    return -EINVAL;
                }
                strcpy(config.root, value);
                break;
            case OPT_COPYTOOL:
                if (value == NULL)
                {
                    DisplayLog(LVL_CRIT, RBHEXT_TAG, "Expected value for suboption '%s'",
                    opts[OPT_BACKEND_ROOT] );
                    return -EINVAL;
                }
                strcpy(config.action_cmd, value);
                break;
            default:
                /* Unknown suboption. */
                DisplayLog(LVL_CRIT, RBHEXT_TAG, "Unknown suboption '%s'", value);
                return -EINVAL;
        }
    }

    /* synchronous archiving and rm support */
    *p_behaviors_flags = RBHEXT_SYNC_ARCHIVE | RBHEXT_RM_SUPPORT;
    return 0;
}

/**
 * Determine attributes to be provided for rbhext_get_status().
 * \param[in] entry_type type of entry to check status.
 * \param[out] p_attr_allow_cached list of attributes needed for determining status
 *                                 that can be retrieved from DB (cached)
 * \param[out] p_attr_need_fresh list of attributes needed for determining status
 *                                 that need to be up-to-date.
 * \retval 0 on success
 * \retval <0 on error
 * \retval -ENOTSUP backup is not implemented for this type of entry.
 */
int rbhext_status_needs( obj_type_t   entry_type,
                         unsigned int * p_attr_allow_cached,
                         unsigned int * p_attr_need_fresh );

/**
 * Get the status for an entry.
 * \param[in] p_id pointer to entry id
 * \param[in] p_attrs_in pointer to entry attributes
 * \param[out] p_attrs_changed changed/retrieved attributes
 */
int rbhext_get_status( const entry_id_t * p_id,
                       const attr_set_t * p_attrs_in,
                       attr_set_t * p_attrs_changed );

/**
 * Performs an archiving operation.
 * \param[in] arch_meth archiving method (sync or async)
 * \param[in] p_id pointer to id of entry to be archived
 * \param[in,out] p_attrs pointer to entry attributes
 *        function must update at least the entry status
 *        and the path in the backend.
 */
int rbhext_archive( rbhext_arch_meth arch_meth,
                    const entry_id_t * p_id,
                    attr_set_t * p_attrs,
                    const char * hints );

/**
 * Performs entry removal in the backend
 * \param[in] p_id pointer to id of entry to be archived
 * \param[in,out] p_attrs pointer to entry attributes
 *                        must be updated even on failure
 */
int rbhext_remove( const entry_id_t * p_id, attr_set_t * p_attrs );
