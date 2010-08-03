/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

#include "list_mgr.h"

#ifndef _LISTMGR_PREP_STMT_H
#define _LISTMGR_PREP_STMT_H

void           init_prep_stmt_cache( lmgr_t * lmgr );

void           destroy_statements( lmgr_t * lmgr );

/* invalidate all statements is simply cleaning the cache */
#define invalidate_statements( __l )    destroy_statements( __l )


/**
 * get a prepared statement from cache
 * @return NULL if not found
 */
prep_stmt_t    prep_stmt_get( lmgr_t * lmgr, db_op_type_t op, db_tables_t table, int attr_mask );


/**
 * insert a prepared statement to cache
 * @return a listmgr code on error
 */
int            prep_stmt_insert( lmgr_t * lmgr, db_op_type_t op, db_tables_t table, int attr_mask,
                                 prep_stmt_t stmt );

/**
 * This wraps the 2 following calls:
 * Try to get a statement from cache and create the statement if it doesnot exist
 */
prep_stmt_t    prep_stmt_build_or_get( lmgr_t * lmgr, db_op_type_t op, db_tables_t table,
                                       int attr_mask, const char *query );



#endif
