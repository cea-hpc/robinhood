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
 * \file   backend_ext.h
 * \author Th. Leibovici
 * \brief  This file defines backend extension interface.
 */
#ifndef _RBH_BACKEND_EXT_H
#define _RBH_BACKEND_EXT_H

/* includes all types definitions */
#include "list_mgr.h"
#include "backend_mgr.h"
/**
 * \addtogroup EXTENSIONS
 * @{
 */

/* extension compatibility flags */
#define RBHEXT_COMPAT_LUSTRE    0x00000001  /* only supports lustre filesystems */

/* extension behavior flags */
#define RBHEXT_SYNC_ARCHIVE     0x00000001 /* copy command is synchronous */
#define RBHEXT_ASYNC_ARCHIVE    0x00000002 /* copy command is asynchronous */
#define RBHEXT_RM_SUPPORT       0x00000004 /* backend supports entry removal operation */

/**
 * Get compatibility information,
 * to check compatibility with the current FS.
 */
int rbhext_compat_flags();

/**
 * Initialize the extension module.
 * \param[in] config_string specific config string (e.g path to config file...)
 * \param[out] p_behaviors_flags pointer to output mask that describes extension behavior
 */
int rbhext_init( const backend_config_t * conf,
                 unsigned int * p_behaviors_flags );

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

typedef enum {
    RBHEXT_SYNC  = 0,
    RBHEXT_ASYNC = 1
} rbhext_arch_meth;

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
int rbhext_remove( const entry_id_t * p_id, const char * backend_path );


/* recover a file from the backend after formatting FS */
recov_status_t rbhext_recover( const entry_id_t * p_old_id,
                               attr_set_t * p_attrs_old,
                               entry_id_t * p_new_id,
                               attr_set_t * p_attrs_new );

#endif
