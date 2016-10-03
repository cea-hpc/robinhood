/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2008-2014 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
#ifndef _RBH_TYPE_H
#define _RBH_TYPE_H

#include <stdint.h>

/** shorter alias for "unsigned long long" */
typedef unsigned long long ull_t;

#ifdef _LUSTRE
/* stripe info for Lustre */

typedef struct stripe_item_t {
    unsigned int ost_idx; /* ost index */
    unsigned int ost_gen; /* always 0 ? */
    uint64_t obj_id;      /* object index on OST */
    uint64_t obj_seq;     /* sequence from object fid */
} stripe_item_t;

typedef struct stripe_info_t {
    uint64_t       stripe_size;
    unsigned int   stripe_count;
    char           pool_name[MAX_POOL_LEN];
#ifdef HAVE_LLAPI_FSWAP_LAYOUTS
    int            validator;
#endif
} stripe_info_t;

typedef struct stripe_items_t {
    unsigned int   count;
    stripe_item_t *stripe;   /* list of stripe pieces */
} stripe_items_t;
#else
typedef int stripe_items_t; /* dummy type */
typedef int stripe_info_t; /* dummy type */
#endif

/** type of fields in database */
typedef enum {
    DB_ID = 0,       /**< entry id */
    DB_STRIPE_INFO,  /**< stripe info */
    DB_STRIPE_ITEMS,  /**< stripe items */
    DB_TEXT,    /**< string/text        */
    DB_INT,     /**< signed integer     */
    DB_UINT,    /**< unsigned integer   */
    DB_SHORT,   /**< short integer     */
    DB_USHORT,  /**< short unsigned integer   */
    DB_BIGINT,  /**< 64 bits integer    */
    DB_BIGUINT, /**< 64 bits unsigned integer */
    DB_BOOL,    /**< boolean            */
    DB_ENUM_FTYPE, /**< file type enumeration */
    DB_UIDGID, /**< type depending on uid/gid format (configuration driven) */
} db_type_e;

/** generic function for generating fields:
 * 1st parameter points to the field to be generated.
 * 2nd parameter is the source field.
 */
typedef int (*gen_func_t)(void *, const void *);


/** generic field definition for all applications */
typedef struct field_info_t {
    char          *field_name;
    db_type_e      db_type;
    unsigned int   db_type_size; /**< size for strings */
    int            flags;
    off_t          offset;
    int            gen_index;   /* source attr index for generating this info */
    gen_func_t     gen_func;    /* function for automatic generation */
} field_info_t;

/* access pattern for fields in database */
#define INIT_ONLY    0x00000001 /* set at insert only: stored in an annex table (can't be modified) */
#define ANNEX_INFO   0x00000002 /* annex information, rarely accessed: stored in an annex table */
#define FREQ_ACCESS  0x00000004 /* frequently updated, or used as select filter: stored in the main table */
#define REMOVED      0x00000008 /* this attribute only applies to removed entries */
#define SEPD_LIST    0x00000010 /* list with separators (text) */
#define DNAMES       0x01000000 /* field in DNAMES table. */
#define FUNC_ATTR    0x02000000 /* special attr built using a DB function */
#define GENERATED    0x10000000 /* field not stored in database: generated in SELECT requests (read-only) */
#define INDEXED      0x20000000 /* this field must be indexed */
#define DIR_ATTR     0x40000000 /* need to aggregate directory info (specific DB request) */
#define SLINK_ATTR   0x80000000 /* specific attr for symlinks */

/** type of operation to be performed on database */
typedef enum operation_type_e {
    OP_TYPE_NONE = 0,
    OP_TYPE_INSERT,
    OP_TYPE_UPDATE,
    OP_TYPE_REMOVE_ONE,         /* remove name only; inode still exists */
    OP_TYPE_REMOVE_LAST,        /* remove last name to inode and inode */
    OP_TYPE_SOFT_REMOVE
} operation_type_e;

#endif
