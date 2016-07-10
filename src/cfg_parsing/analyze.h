/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2004-2009 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
/**
 * \file    analyze.h
 * \author  $Author: leibovic $
 * \date    $Date: 2008/07/04 07:57:35 $
 * \version	$Revision: 1.2 $
 * \brief   Building the syntax tree.
 *
 * Build the structure that represents a config file.
 *
 */

#ifndef CONFPARSER_H
#define CONFPARSER_H

#include <stdio.h>
#include <glib.h>

#ifndef rh_strncpy
#define rh_strncpy(_s1, _s2, _sz) do { \
    strncpy(_s1, _s2, _sz-1);          \
    if (_sz > 0) (_s1)[_sz-1] = '\0';  \
} while (0)
#endif

#define MAXSTRLEN   1024

extern GString *current_file;

/* A program consists of several blocks,
 * each block consists of variables definitions
 * and subblocks.
 */

/* forward declaration of generic item */
struct _generic_item_;

typedef enum {
    TYPE_BLOCK,
    TYPE_BOOL_EXPR,
    TYPE_SET,
    TYPE_AFFECT
} type_item;

typedef enum {
    OP_EQUAL,
    OP_DIFF,
    OP_GT,
    OP_GT_EQ,
    OP_LT,
    OP_LT_EQ,
    OP_CMD
} operator_t;

typedef struct _arg_list_ {
    unsigned int nb_args;
    char **args;
} arg_list_t;

typedef struct _type_key_value_ {
    operator_t  op_type;
    char        varname[MAXSTRLEN];
    char        varvalue[MAXSTRLEN];
    arg_list_t *arg_list;
} type_key_value;

typedef struct _type_block_ {
    char block_name[MAXSTRLEN];
    char block_id[MAXSTRLEN];
    struct _generic_item_ *block_content;
} type_block;

typedef enum {
    BOOL_CONDITION,
    BOOL_UNARY,
    BOOL_BINARY
} expr_type_t;

typedef enum {
    BOOL_OP_IDENTITY,
    BOOL_OP_NOT,
    BOOL_OP_AND,
    BOOL_OP_OR
} bool_operator_t;

typedef enum {
    SET_OP_NOT,
    SET_OP_UNION,
    SET_OP_INTER
} set_operator_t;

typedef struct _type_bool_expr_ {
    expr_type_t type;
    bool_operator_t oper;

    union {
        struct {
            struct _type_bool_expr_ *expr1;
            struct _type_bool_expr_ *expr2;
        } members;
        type_key_value key_value;
    } expr_u;

} type_bool_expr;

/* describes unions/intersections of classes */
typedef struct _type_set_ {
    enum { SET_SINGLETON, SET_NEGATION, SET_BINARY } set_type;

    union {
        char name[MAXSTRLEN];   /* for singleton set */
        struct {
            set_operator_t      oper;
            struct _type_set_  *set1;
            struct _type_set_  *set2;
        } op;   /* for union or insection set */
    } set_u;

} type_set;

typedef struct _generic_item_ {
    type_item type;

    union {
        type_block      block;
        type_bool_expr  bool_expr;
        type_key_value  affect;
        type_set        set;
    } item;

    /* next item in the list */
    struct _generic_item_ *next;

    /* Line of this item */
    unsigned int line;

} generic_item;

typedef generic_item *list_items;

/**
 *  create a list of items
 */
list_items *rh_config_CreateItemsList(void);

/**
 *  Create a block item with the given content
 */
generic_item *rh_config_CreateBlock(char *blockname, char *blockid,
                                    list_items *list);

/**
 *  Create an affectation (key=value)
 */
generic_item *rh_config_CreateAffect(char *varname, char *varval);

/*  ------ Bool expr management functions -------- */

/**
 *  Create a key <op> value peer (condition)
 */
generic_item *rh_config_CreateKeyValueExpr(char *varname, operator_t op,
                                           char *varval);

/**
 * Create a block with a boolean expression
 */
generic_item *rh_config_CreateBoolExpr(char *blockname, char *title,
                                       generic_item *item);

/**
 * Create unary boolean expression from key/value or other boolean expression
 */
generic_item *rh_config_CreateBoolExpr_Unary(bool_operator_t op,
                                             generic_item *item);

/**
 * Create binary boolean expression from 2 expressions
 */
generic_item *rh_config_CreateBoolExpr_Binary(bool_operator_t op,
                                              generic_item *expr1,
                                              generic_item *expr2);

/*  ------ Sets management functions -------- */

generic_item *rh_config_CreateSet(char *blockname, char *label,
                                  generic_item *set);
generic_item *rh_config_CreateSet_Unary(set_operator_t op, generic_item *set);
generic_item *rh_config_CreateSet_Binary(set_operator_t op,
                                         generic_item *set1,
                                         generic_item *set2);
generic_item *rh_config_CreateSet_Singleton(char *set_name);

/*  ------ Arglist management functions -------- */

arg_list_t *rh_config_CreateArgList(void);
void rh_config_AddArg(arg_list_t *arglist, char *arg);
void rh_config_SetArglist(generic_item *item, arg_list_t *arglist);

/**
 *  Add an item to a list
 */
void rh_config_AddItem(list_items *list, generic_item *item);

/**
 *  Displays the content of a list of blocks.
 */
void rh_config_print_list(FILE *output, list_items *list);

/**
 * config_free_list:
 * Free ressources for a list
 */
void rh_config_free_list(list_items *list);

/**
 * Resolve an environment variable.
 */
void rh_config_resolv_var(char *dstvalue, char *var);

#endif
