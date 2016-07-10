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
 * \file    analyze.c
 * \author  Thomas Leibovici, CEA/DAM
 * \date    2008/07/04
 * \brief   Building the syntax tree.
 */

#include "config.h"
#include "analyze.h"
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>

#if HAVE_STRING_H
#   include <string.h>
#endif

extern int yylineno;

/**
 *  create a list of items
 */
list_items *rh_config_CreateItemsList(void)
{
    list_items *new = (list_items *)malloc(sizeof(list_items));

    (*new) = NULL;
    return new;
}

/**
 *  Create a block item with the given content
 */
generic_item *rh_config_CreateBlock(char *blockname, char *blockid,
                                    list_items *list)
{
    generic_item *new = (generic_item *)malloc(sizeof(generic_item));

    new->type = TYPE_BLOCK;
    new->line = yylineno;

    rh_strncpy(new->item.block.block_name, blockname, MAXSTRLEN);
    if (blockid)
        rh_strncpy(new->item.block.block_id, blockid, MAXSTRLEN);
    else
        new->item.block.block_id[0] = '\0';

    if (list) {
        new->item.block.block_content = *list;
        free(list);
    } else
        new->item.block.block_content = NULL;

    new->next = NULL;

    return new;

}

/**
 *  Add an item to a list as first element
 */
void rh_config_AddItem(list_items *list, generic_item *item)
{
    if (!item)
        fprintf(stderr, "Item expected\n");

    if ((*list) == NULL) {
        (*list) = item;
    } else {
        item->next = (*list);
        (*list) = item;
    }
}

/**
 *  Create a key <op> value peer (boolean)
 */
generic_item *rh_config_CreateKeyValueExpr(char *varname, operator_t op,
                                           char *varval)
{

    generic_item *new = (generic_item *)malloc(sizeof(generic_item));
    new->type = TYPE_BOOL_EXPR;
    new->line = yylineno;
    new->next = NULL;
    new->item.bool_expr.type = BOOL_CONDITION;
    new->item.bool_expr.oper = BOOL_OP_IDENTITY;
    rh_strncpy(new->item.bool_expr.expr_u.key_value.varname, varname,
               MAXSTRLEN);
    rh_strncpy(new->item.bool_expr.expr_u.key_value.varvalue, varval,
               MAXSTRLEN);
    new->item.bool_expr.expr_u.key_value.op_type = op;
    new->item.bool_expr.expr_u.key_value.arg_list = NULL;

    return new;

}

/**
 *  Create a key=value peer (assignment or condition)
 */
generic_item *rh_config_CreateAffect(char *varname, char *varval)
{

    generic_item *new = (generic_item *)malloc(sizeof(generic_item));
    new->type = TYPE_AFFECT;
    new->line = yylineno;
    new->next = NULL;
    rh_strncpy(new->item.affect.varname, varname, MAXSTRLEN);
    rh_strncpy(new->item.affect.varvalue, varval, MAXSTRLEN);
    new->item.affect.op_type = 0;
    new->item.affect.arg_list = NULL;

    return new;

}

/**
 * Create a block with a boolean expression
 */
generic_item *rh_config_CreateBoolExpr(char *blockname, char *title,
                                       generic_item *item)
{
    generic_item *new = (generic_item *)malloc(sizeof(generic_item));

    new->type = TYPE_BLOCK;
    new->line = yylineno;

    rh_strncpy(new->item.block.block_name, blockname, MAXSTRLEN);

    if (title)
        rh_strncpy(new->item.block.block_id, title, MAXSTRLEN);
    else
        new->item.block.block_id[0] = '\0';

    new->item.block.block_content = item;

    new->next = NULL;

    return new;
}

/**
 * Create unary boolean expression from other boolean expression
 */
generic_item *rh_config_CreateBoolExpr_Unary(bool_operator_t op,
                                             generic_item *item)
{
    if (item->type != TYPE_BOOL_EXPR) {
        // boolean expression expected
        fprintf(stderr, "Boolean expression expected\n");
        return NULL;
    }

    if (op == BOOL_OP_IDENTITY)
        return item;

    if (op != BOOL_OP_NOT) {
        /* unary operator expected */
        fprintf(stderr, "Unary boolean expression expected\n");
        return NULL;
    }

    generic_item *new = (generic_item *)malloc(sizeof(generic_item));

    new->type = TYPE_BOOL_EXPR;
    new->line = yylineno;
    new->next = NULL;
    new->item.bool_expr.type = BOOL_UNARY;
    new->item.bool_expr.oper = op;

    /* create a bool expr and free the generic item */
    new->item.bool_expr.expr_u.members.expr1 =
        (type_bool_expr *)malloc(sizeof(type_bool_expr));
    *new->item.bool_expr.expr_u.members.expr1 = item->item.bool_expr;
    free(item);

    new->item.bool_expr.expr_u.members.expr2 = NULL;

    return new;
}

/**
 * Create binary boolean expression from 2 expressions
 */
generic_item *rh_config_CreateBoolExpr_Binary(bool_operator_t op,
                                              generic_item *expr1,
                                              generic_item *expr2)
{
    if ((expr1->type != TYPE_BOOL_EXPR) || (expr2->type != TYPE_BOOL_EXPR)) {
        // boolean expressions expected
        fprintf(stderr, "Boolean expression expected\n");
        return NULL;
    }

    if (op != BOOL_OP_AND && op != BOOL_OP_OR) {
        /* binary operator expected */
        fprintf(stderr, "Binary boolean expression expected\n");
        return NULL;
    }

    generic_item *new = (generic_item *)malloc(sizeof(generic_item));

    new->type = TYPE_BOOL_EXPR;
    new->line = yylineno;
    new->next = NULL;
    new->item.bool_expr.type = BOOL_BINARY;
    new->item.bool_expr.oper = op;

    new->item.bool_expr.expr_u.members.expr1 =
        (type_bool_expr *)malloc(sizeof(type_bool_expr));
    new->item.bool_expr.expr_u.members.expr2 =
        (type_bool_expr *)malloc(sizeof(type_bool_expr));
    *new->item.bool_expr.expr_u.members.expr1 = expr1->item.bool_expr;
    *new->item.bool_expr.expr_u.members.expr2 = expr2->item.bool_expr;
    free(expr1);
    free(expr2);

    return new;

}

generic_item *rh_config_CreateSet(char *blockname, char *label,
                                  generic_item *set)
{
    generic_item *new = (generic_item *)malloc(sizeof(generic_item));

    new->type = TYPE_BLOCK;
    new->line = yylineno;

    rh_strncpy(new->item.block.block_name, blockname, MAXSTRLEN);

    if (label)
        rh_strncpy(new->item.block.block_id, label, MAXSTRLEN);
    else
        new->item.block.block_id[0] = '\0';

    new->item.block.block_content = set;

    new->next = NULL;

    return new;
}

generic_item *rh_config_CreateSet_Unary(set_operator_t op, generic_item *set)
{
    if (set->type != TYPE_SET) {
        // sets expected
        fprintf(stderr, "Set is expected\n");
        return NULL;
    }

    if (op != SET_OP_NOT) {
        /* unary operator expected */
        fprintf(stderr, "Unary set operator expected (not)\n");
        return NULL;
    }

    generic_item *new = malloc(sizeof(generic_item));
    if (!new) {
        fprintf(stderr, "Not enough memory\n");
        return NULL;
    }

    new->type = TYPE_SET;
    new->line = yylineno;
    new->next = NULL;
    new->item.set.set_type = SET_NEGATION;

    new->item.set.set_u.op.oper = op;

    new->item.set.set_u.op.set1 = (type_set *)malloc(sizeof(type_set));
    new->item.set.set_u.op.set2 = NULL;

    if (new->item.set.set_u.op.set1 == NULL) {
        free(new);
        fprintf(stderr, "Missing memory\n");
        return NULL;
    }

    *new->item.set.set_u.op.set1 = set->item.set;

    free(set);

    return new;
}

generic_item *rh_config_CreateSet_Binary(set_operator_t op,
                                         generic_item *set1,
                                         generic_item *set2)
{
    if ((set1->type != TYPE_SET) || (set2->type != TYPE_SET)) {
        // boolean setessions expected
        fprintf(stderr, "Sets are expected\n");
        return NULL;
    }

    if (op != SET_OP_UNION && op != SET_OP_INTER) {
        /* binary operator expected */
        fprintf(stderr, "Binary set operators expected (union, inter)\n");
        return NULL;
    }

    generic_item *new = malloc(sizeof(generic_item));
    if (!new) {
        fprintf(stderr, "Not enough memory\n");
        return NULL;
    }

    new->type = TYPE_SET;
    new->line = yylineno;
    new->next = NULL;
    new->item.set.set_type = SET_BINARY;

    new->item.set.set_u.op.oper = op;

    new->item.set.set_u.op.set1 = (type_set *)malloc(sizeof(type_set));
    new->item.set.set_u.op.set2 = (type_set *)malloc(sizeof(type_set));

    if ((new->item.set.set_u.op.set1 == NULL) ||
        (new->item.set.set_u.op.set2 == NULL)) {
        free(new);
        fprintf(stderr, "Missing memory\n");
        return NULL;
    }

    *new->item.set.set_u.op.set1 = set1->item.set;
    *new->item.set.set_u.op.set2 = set2->item.set;

    free(set1);
    free(set2);

    return new;
}

generic_item *rh_config_CreateSet_Singleton(char *set_name)
{
    generic_item *new = (generic_item *)malloc(sizeof(generic_item));

    new->type = TYPE_SET;
    new->line = yylineno;
    new->next = NULL;
    new->item.set.set_type = SET_SINGLETON;
    rh_strncpy(new->item.set.set_u.name, set_name, MAXSTRLEN);

    return new;
}

arg_list_t *rh_config_CreateArgList(void)
{
    arg_list_t *p_list = (arg_list_t *)malloc(sizeof(arg_list_t));
    p_list->nb_args = 0;
    p_list->args = NULL;
    return p_list;
}

void rh_config_AddArg(arg_list_t *p_list, char *arg)
{
    if (p_list->args != NULL)
        p_list->args =
            (char **)realloc(p_list->args,
                             (p_list->nb_args + 1) * sizeof(char *));
    else
        p_list->args = (char **)malloc(sizeof(char *));

    p_list->nb_args++;
    p_list->args[p_list->nb_args - 1] = (char *)malloc(strlen(arg) + 1);
    strcpy(p_list->args[p_list->nb_args - 1], arg);
}

void rh_config_SetArglist(generic_item *item, arg_list_t *arglist)
{
    if (item->type == TYPE_BOOL_EXPR)
        item->item.bool_expr.expr_u.key_value.arg_list = arglist;
    else if (item->type == TYPE_AFFECT)
        item->item.affect.arg_list = arglist;
}

static const char *op2str(operator_t op)
{
    switch (op) {
    case OP_EQUAL:
        return "==";
    case OP_DIFF:
        return "!=";
    case OP_GT:
        return ">";
    case OP_GT_EQ:
        return ">=";
    case OP_LT:
        return "<";
    case OP_LT_EQ:
        return "<=";
    case OP_CMD:
        return ":";
    default:
        return "";
    }
}

static void print_bool_expr(FILE *output, type_bool_expr *bool_expr)
{
    int i;

    switch (bool_expr->type) {
    case BOOL_CONDITION:
        fprintf(output, "%s", bool_expr->expr_u.key_value.varname);
        fprintf(output, " %s ", op2str(bool_expr->expr_u.key_value.op_type));
        fprintf(output, "%s", bool_expr->expr_u.key_value.varvalue);

        if ((bool_expr->expr_u.key_value.arg_list != NULL)
            && (bool_expr->expr_u.key_value.arg_list->nb_args > 0)) {
            fprintf(output, " (");
            for (i = 0; i < bool_expr->expr_u.key_value.arg_list->nb_args;
                 i++) {
                if (i == 0)
                    fprintf(output, "%s",
                            bool_expr->expr_u.key_value.arg_list->args[i]);
                else
                    fprintf(output, ", %s",
                            bool_expr->expr_u.key_value.arg_list->args[i]);
            }
            fprintf(output, ")");
        }
        break;

    case BOOL_UNARY:
        if (bool_expr->oper == BOOL_OP_NOT)
            fprintf(output, "NOT (");
        else
            fprintf(output, "(");

        print_bool_expr(output, bool_expr->expr_u.members.expr1);
        fprintf(output, ")");
        break;

    case BOOL_BINARY:
        fprintf(output, "(");

        print_bool_expr(output, bool_expr->expr_u.members.expr1);

        if (bool_expr->oper == BOOL_OP_AND)
            fprintf(output, ") AND (");
        else if (bool_expr->oper == BOOL_OP_OR)
            fprintf(output, ") OR (");

        print_bool_expr(output, bool_expr->expr_u.members.expr2);

        fprintf(output, ")");
        break;
    }
}

static void print_set(FILE *output, type_set *set)
{
    if (set->set_type == SET_SINGLETON)
        fprintf(output, "{%s}", set->set_u.name);
    else if (set->set_type == SET_NEGATION) {
        fprintf(output, "NOT (");
        print_set(output, set->set_u.op.set1);
        fprintf(output, ")");
    } else {
        fprintf(output, "(");
        print_set(output, set->set_u.op.set1);
        if (set->set_u.op.oper == SET_OP_UNION)
            fprintf(output, ") UNION (");
        else if (set->set_u.op.oper == SET_OP_INTER)
            fprintf(output, ") INTER (");
        print_set(output, set->set_u.op.set2);
        fprintf(output, ")");
    }
}

union {
    char name[MAXSTRLEN];   /* for singleton set */
    struct {
        set_operator_t oper;
        struct _type_set_ *set1;
        struct _type_set_ *set2;
    } op;   /* for union or insection set */
} set_u;

/**
 *  Displays the content of a list of blocks.
 */
static void print_list_ident(FILE *output, list_items *list,
                             unsigned int indent)
{

    generic_item *curr_item;
    unsigned int i;

    /* sanity check */
    if (!list)
        return;

    curr_item = (*list);

    while (curr_item) {

        if (curr_item->type == TYPE_BLOCK) {
            fprintf(output, "%*s<BLOCK '%s'>\n", indent, " ",
                    curr_item->item.block.block_name);
            print_list_ident(output, &curr_item->item.block.block_content,
                             indent + 3);
            fprintf(output, "%*s</BLOCK '%s'>\n", indent, " ",
                    curr_item->item.block.block_name);
        } else if (curr_item->type == TYPE_AFFECT) {
            fprintf(output, "%*s<AFFECT> %s='%s'", indent, " ",
                    curr_item->item.affect.varname,
                    curr_item->item.affect.varvalue);

            if (curr_item->item.affect.arg_list) {
                for (i = 0; i < curr_item->item.affect.arg_list->nb_args; i++) {
                    if (i == 0)
                        fprintf(output, "('%s'",
                                curr_item->item.affect.arg_list->args[i]);
                    else
                        fprintf(output, ", '%s'",
                                curr_item->item.affect.arg_list->args[i]);
                }
                fprintf(output, ")");
            }
            fprintf(output, "</AFFECT>\n");

        } else if (curr_item->type == TYPE_SET) {
            fprintf(output, "%*s", indent, " ");
            /* class set */
            print_set(output, &curr_item->item.set);
            fprintf(output, "\n");
        } else if (curr_item->type == TYPE_BOOL_EXPR) {
            fprintf(output, "%*s", indent, " ");
            /* boolean expression */
            print_bool_expr(output, &curr_item->item.bool_expr);
            fprintf(output, "\n");
        } else {
            fprintf(output, "/!\\ UNKNOWN ITEM TYPE %d\n", curr_item->type);
        }

        curr_item = curr_item->next;
    }

}

/**
 *  Displays the content of a list of blocks.
 */
void rh_config_print_list(FILE *output, list_items *list)
{

    print_list_ident(output, list, 0);

}

static void free_key_value(type_key_value *p_keyval)
{
    if (p_keyval->arg_list != NULL) {
        int i;
        if (p_keyval->arg_list->args) {
            for (i = 0; i < p_keyval->arg_list->nb_args; i++)
                free(p_keyval->arg_list->args[i]);

            free(p_keyval->arg_list->args);
        }
        free(p_keyval->arg_list);
        p_keyval->arg_list = NULL;
    }
}

static void free_bool_expr_recurse(type_bool_expr *p_expr)
{
    if (p_expr->type == BOOL_CONDITION) {
        free_key_value(&p_expr->expr_u.key_value);
    } else if (p_expr->type == BOOL_UNARY) {
        if (p_expr->expr_u.members.expr1) {
            free_bool_expr_recurse(p_expr->expr_u.members.expr1);
            free(p_expr->expr_u.members.expr1);
        }

    } else if (p_expr->type == BOOL_BINARY) {
        if (p_expr->expr_u.members.expr1) {
            free_bool_expr_recurse(p_expr->expr_u.members.expr1);
            free(p_expr->expr_u.members.expr1);
        }
        if (p_expr->expr_u.members.expr2) {
            free_bool_expr_recurse(p_expr->expr_u.members.expr2);
            free(p_expr->expr_u.members.expr2);
        }
    }

}

static void free_set_recurse(type_set *p_set)
{
    if (p_set->set_type == SET_BINARY) {
        if (p_set->set_u.op.set1) {
            free_set_recurse(p_set->set_u.op.set1);
            free(p_set->set_u.op.set1);
        }
        if (p_set->set_u.op.set2) {
            free_set_recurse(p_set->set_u.op.set2);
            free(p_set->set_u.op.set2);
        }
    } else if (p_set->set_type == SET_NEGATION) {
        if (p_set->set_u.op.set1) {
            free_set_recurse(p_set->set_u.op.set1);
            free(p_set->set_u.op.set1);
        }
    }
}

static void free_list_items_recurse(list_items *list)
{
    generic_item *curr_item;
    generic_item *next_item;

    /* sanity check */
    if (!list)
        return;

    curr_item = (*list);

    while (curr_item) {

        next_item = curr_item->next;

        if (curr_item->type == TYPE_BLOCK) {
            free_list_items_recurse(&curr_item->item.block.block_content);
        } else if (curr_item->type == TYPE_BOOL_EXPR) {
            free_bool_expr_recurse(&curr_item->item.bool_expr);
        } else if (curr_item->type == TYPE_SET) {
            free_set_recurse(&curr_item->item.set);
        } else if (curr_item->type == TYPE_AFFECT) {
            free_key_value(&curr_item->item.affect);
        }
        free(curr_item);
        curr_item = next_item;

    }
    return;
}

/**
 * config_free_list:
 * Free ressources for a list
 */
void rh_config_free_list(list_items *list)
{

    free_list_items_recurse(list);
    free(list);
    return;
}

/**
 * Resolve an environment variable.
 */
void rh_config_resolv_var(char *dstvalue, char *var)
{
    char *val = getenv(var + 1);    /* skip '$' */
    if (val == NULL) {
        fprintf(stderr, "WARNING: environment variable %s is not defined.\n",
                var + 1);
        dstvalue[0] = '\0';
    } else {
        rh_strncpy(dstvalue, val, MAXSTRLEN);
    }
}
