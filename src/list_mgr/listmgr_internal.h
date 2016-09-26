/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2008, 2009 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
#ifndef _LMGR_INTERNAL_H
#define _LMGR_INTERNAL_H

#include "list_mgr.h"

#define STRINGIFY(_x) #_x
#define TOSTRING(_x) STRINGIFY(_x)
/* example:
 * #define FOO 10
 * STRINGIFY(FOO)   => "FOO"
 * TOSTRING(FOO)    => "10"
 */

#ifdef _MYSQL
/* The length can be specified as a value from 0 to 255 before MySQL 5.0.3,
*  and 0 to 65,535 in 5.0.3 and later versions */
#if MYSQL_VERSION_ID >= 50003
#define MAX_VARBINARY 65535
#else
#define MAX_VARBINARY 255
#endif
#else
#define MAX_VARBINARY 255
#endif

/* primary key utils */
#ifndef FID_PK

#define PK_LEN 64
#define PK_ARG_T  char *
#define PK_PARG_T char *
#define PTR_PK(_p) (_p)
#define DEF_PK(_p) char _p[PK_LEN]
typedef DEF_PK(pktype);
#define PK_DB_TYPE DB_TEXT
#define DPK      "'%s'"
#define SPK      "%s"
#define VALID(_p) ((_p)->validator)
#define PK_TYPE   "VARBINARY(" TOSTRING(PK_LEN) ")"

#else
#define DB_FID_LEN 64
#ifdef FID_LEN
#if FID_LEN > DB_FID_LEN
#error "Lustre FID are bigger than expected, fix DB_FID_LEN"
#endif
#endif

#define PK_LEN DB_FID_LEN
#define PK_ARG_T char *
#define PK_PARG_T char *
#define PTR_PK(_p) (_p)
#define DEF_PK(_p) char _p[DB_FID_LEN]
typedef DEF_PK(pktype);
#define PK_DB_TYPE DB_TEXT
#define DPK      "'%s'"
#define SPK      "%s"
#define VALID(_p) (0)
#define PK_TYPE   "VARBINARY(" TOSTRING(DB_FID_LEN) ")"

#endif

#define HNAME_DEF  "sha1(CONCAT(parent_id,'/',name))"
#define HNAME_FMT   "sha1(CONCAT("DPK",'/','%s'))"

int listmgr_get_by_pk(lmgr_t *p_mgr, PK_ARG_T pk, attr_set_t *p_info);
int listmgr_get_dirattrs(lmgr_t *p_mgr, PK_ARG_T dir_pk, attr_set_t *p_attrs);
int listmgr_get_funcattrs(lmgr_t *p_mgr, PK_ARG_T pk, attr_set_t *p_attrs);

int listmgr_batch_insert_no_tx(lmgr_t *p_mgr, entry_id_t **p_ids,
                               attr_set_t **p_attrs, unsigned int count,
                               bool update_if_exists);

int listmgr_remove_no_tx(lmgr_t *p_mgr, const entry_id_t *p_id,
                         const attr_set_t *p_attr_set, bool last);

typedef struct lmgr_iterator_t {
    lmgr_t          *p_mgr;
    lmgr_iter_opt_t  opt;
    result_handle_t  select_result;
    unsigned int     opt_is_set:1;
} lmgr_iterator_t;

#ifdef _LUSTRE
/* see stripe_item_t structure in list_mgr.h */
#define OSTGEN_SZ   4
#define OBJID_SZ    8
#define OBJSEQ_SZ   8
#define STRIPE_DETAIL_SZ (OBJID_SZ+OBJSEQ_SZ+OSTGEN_SZ)
#endif

static inline int buf2hex(char *out, size_t out_sz, const unsigned char *in,
                          size_t in_sz)
{
    /* Convert the input buffer into an hex */
    int i;
    const unsigned char *src = in;
    char *dst = out;

    if (out_sz < 2 * in_sz + 1)
        return -1;

    for (i = 0; i < in_sz; i++) {
        dst += sprintf(dst, "%02x", *src);
        src++;
    }
    *dst = '\0';
    return (int)(dst - out);
}

#endif
