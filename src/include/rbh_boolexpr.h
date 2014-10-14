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

/**
 * \file  rbh_boolexpr.h
 * \brief condition types.
 */

#ifndef _BOOLEXPR_H
#define _BOOLEXPR_H

#include "rbh_const.h"
#include <strings.h>
#include <stdbool.h>
#include <time.h>

/* define ATTR_INDEX_ ... */
#include "db_schema.h"

typedef enum {
    COMP_NONE = 0,              /**<     not set */
    COMP_GRTHAN,                /**<     > */
    COMP_GRTHAN_EQ,             /**<     >= */
    COMP_LSTHAN,                /**<     < */
    COMP_LSTHAN_EQ,             /**<     <= */
    COMP_EQUAL,                 /**<     == */
    COMP_DIFF,                  /**<     != */
    COMP_LIKE,                  /**<     regexp matching */
    COMP_UNLIKE                 /**<     regexp not matching */
} compare_direction_t;

typedef enum {
    CRITERIA_TREE = 0,
    CRITERIA_PATH,
    CRITERIA_FILENAME,
    CRITERIA_TYPE,
    CRITERIA_OWNER,
    CRITERIA_GROUP,
    CRITERIA_SIZE,
    CRITERIA_DEPTH,
    CRITERIA_DIRCOUNT,
    CRITERIA_LAST_ACCESS,
    CRITERIA_LAST_MOD,
#ifdef ATTR_INDEX_last_restore
    CRITERIA_LAST_RESTORE,
#endif
#ifdef ATTR_INDEX_last_archive
    CRITERIA_LAST_ARCHIVE,
#endif
#ifdef ATTR_INDEX_creation_time
    CRITERIA_CREATION,
#endif
#ifdef ATTR_INDEX_rm_time
    CRITERIA_RMTIME,
#endif
#ifdef _LUSTRE
    CRITERIA_POOL,
    CRITERIA_OST,
#endif
    CRITERIA_STATUS,
    /* /!\ str2criteria relies on the fact that CRITERIA_XATTR is the last criteria */
    CRITERIA_XATTR,
} compare_criteria_t;

#define NO_CRITERIA ((compare_criteria_t)-1)

#define XATTR_PREFIX    "xattr"

/* /!\ str2criteria relies on the fact that CRITERIA_XATTR is the last criteria */
#define MAX_CRITERIA CRITERIA_XATTR

const char *criteria2str(compare_criteria_t crit);
compare_criteria_t str2criteria(const char *str);

#define ALLOWED_LRU_ATTRS_STR "creation, last_access, last_mod, last_archive, rm_time"
int str2lru_attr(const char *str);

typedef enum {
    BOOL_ERR = 0,
    BOOL_NOT,
    BOOL_OR,
    BOOL_AND
} bool_op_t;

typedef enum {
    TYPE_NONE = 0,
    TYPE_LINK,
    TYPE_DIR,
    TYPE_FILE,
    TYPE_CHR,
    TYPE_BLK,
    TYPE_FIFO,
    TYPE_SOCK
} obj_type_t;

/** string representation in policies */
static const char *type_cfg_name[] = {
    "?",
    "symlink",
    "directory",
    "file",
    "char",
    "block",
    "fifo",
    "socket"
};

static inline const char *type2str(obj_type_t type)
{
    if (type > TYPE_SOCK)
        return type_cfg_name[TYPE_NONE];

    return type_cfg_name[type];
}

static inline obj_type_t str2type(const char *str)
{
    obj_type_t i;

    for (i = TYPE_NONE; i <= TYPE_SOCK; i++)
    {
        if (!strcasecmp(str, type_cfg_name[i]))
            return i;
    }
    return TYPE_NONE;
}

typedef union
{
    char               str[RBH_PATH_MAX]; /* for all conditions based on a string */
    unsigned long long size;              /* for size-based conditions */
    unsigned int       integer;           /* for int base conditions */
    time_t             duration;          /* for last access and last mod condition */
    obj_type_t         type;              /* for conditions based on object type */
} compare_value_t;

/* indicates that the compare triplet is for mathcing any level
 * of directories.
 */
#define CMP_FLG_ANY_LEVEL 0x00000001

/* whitelist rules are defined by a tree of comparators */

/** <attribute> <comparator> <value> triplet */
typedef struct compare_triplet_t
{
    int flags;
    compare_criteria_t crit;
    char               xattr_name[RBH_NAME_MAX]; /* for xattrs */
    compare_direction_t op;
    compare_value_t val;
} compare_triplet_t;

/** Type of boolean expression: unary, binary or criteria */
typedef enum {
    NODE_CONDITION,
    NODE_UNARY_EXPR,
    NODE_BINARY_EXPR
} node_type_t;

/** Recursive definition of a Boolean expression */
typedef struct bool_node_t
{
    node_type_t    node_type;
    union
    {
        compare_triplet_t *condition;            /**< for final condition on any field */
        struct
        {
            bool_op_t      bool_op;              /**< boolean operator */
            struct bool_node_t *expr1;           /**< for unary or binary operators */
            struct bool_node_t *expr2;           /**< for binary operators */

            /* this tag indicates if expressions 1 and 2
             * are allocated by the owner of this structure
             * (boolean expression or set of classes) */
            unsigned int owner:1;
        } bool_expr;
    } content_u;
} bool_node_t;

/** give the  string for a compare oparation */
const char    *op2str(compare_direction_t comp);

/** Create a boolean condition */
int CreateBoolCond(bool_node_t * p_out_node, compare_direction_t compar,
                   compare_criteria_t  crit, compare_value_t val);

/** Append a boolean condition with bool op = AND */
int AppendBoolCond(bool_node_t * p_in_out_node, compare_direction_t compar,
                   compare_criteria_t  crit, compare_value_t val);
/**
 * Free a boolean expression structure
 */
int FreeBoolExpr(bool_node_t * p_expr, bool free_top_node);

/**
 * Print a boolean expression to a string.
 */
int BoolExpr2str(bool_node_t *p_bool_node, char *out_str, size_t str_size);

#endif
