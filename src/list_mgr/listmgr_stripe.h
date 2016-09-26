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
#include "listmgr_internal.h"

#ifndef _LISTMGR_STRIPE_H
#define _LISTMGR_STRIPE_H

int insert_stripe_info(lmgr_t *p_mgr, PK_ARG_T pk,
                       int validator, const stripe_info_t *p_stripe,
                       const stripe_items_t *p_items, bool update_if_exists);
int update_stripe_info(lmgr_t *p_mgr, PK_ARG_T pk,
                       int validator, const stripe_info_t *p_stripe,
                       const stripe_items_t *p_items, bool insert_if_absent);
int batch_insert_stripe_info(lmgr_t *p_mgr, pktype *pklist, int *validators,
                             attr_set_t **p_attrs, unsigned int count,
                             bool update_if_exists);

int get_stripe_info(lmgr_t *p_mgr, PK_ARG_T pk, stripe_info_t *p_stripe,
                    stripe_items_t *p_items);

/** duplicate stripe information */
int dup_stripe_items(stripe_items_t *p_stripe_out,
                     const stripe_items_t *p_stripe_in);

void free_stripe_items(stripe_items_t *p_stripe_items);

#endif
