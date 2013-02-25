/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2004-2010 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
/**
 * \file  RobinhoodMisc.h
 * \brief Common tools for parsing, converting, checking FS status...
 */
#ifndef _RBH_CMD_HELPERS_H
#define _RBH_CMD_HELPERS_H

#include "list_mgr.h"

/* special character sequences for displaying help */

/* Bold start character sequence */
#define _B "[1m"
/* Bold end charater sequence */
#define B_ "[m"

/* Underline start character sequence */
#define _U "[4m"
/* Underline end character sequence */
#define U_ "[0m"

/** The caller's function to be called for scanned entries */
typedef int    ( *scrub_callback_t ) ( entry_id_t * id_list,
                                       attr_set_t * attr_list,
                                       unsigned int entry_count,
                                       void * arg );


/** scan sets of directories
 * \param cb_func, callback function for each set of directory
 */
int rbh_scrub(lmgr_t   * p_mgr, entry_id_t * id_list,
              unsigned int id_count, int dir_attr_mask,
              scrub_callback_t cb_func,
              void * arg);


int Path2Id(const char *path, entry_id_t * id);

#ifdef ATTR_INDEX_status
/** status conversion functions */
const char * db_status2str( file_status_t status, int csv );
file_status_t status2dbval( char * status_str );
const char * allowed_status();
#endif


/* parse attrset for --diff option */
int parse_diff_mask(char * arg, int * diff_mask, char * msg);

#endif
