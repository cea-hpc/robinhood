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
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "config_parsing.h"
#include "analyze.h"
#include <stdio.h>
#include <errno.h>

#if HAVE_STRING_H
#include <string.h>
#endif

/* case unsensitivity */
#define STRNCMP   strncasecmp

typedef struct config_struct_t {
    /* Syntax tree */
    list_items *syntax_tree;

} config_struct_t;

/***************************************
 * ACCESS TO EXTERNAL VARIABLES
 ***************************************/

/* lexer input file */
extern FILE *yyin;

/* main parsing function */
int yyparse();

/* parser reinitialization */
void yyreset(void);

/* returns the file currently parsed (for tracing in case of error) */
void yy_set_current_file(char *file);

/* free parser resources */
int yylex_destroy(void);

/* the global program structure, set after parsing */
extern list_items *program_result;

/* error message */
extern char extern_errormsg[1024];

/* config_ParseFile:
 * Reads the content of a configuration file and
 * stores it in a memory structure.
 */
config_file_t rh_config_ParseFile(char *file_path)
{
    FILE *configuration_file;
    config_struct_t *output_struct;

    /* Inits error message */

    extern_errormsg[0] = '\0';

    /* Sanity check */

    if (!file_path || !file_path[0]) {
        strcpy(extern_errormsg, "Invalid arguments");
        return NULL;
    }

    /* First, opens the file. */

    configuration_file = fopen(file_path, "r");

    if (!configuration_file) {
        strcpy(extern_errormsg, strerror(errno));
        return NULL;
    }

    /* Then, parse the file. */
    program_result = NULL;

    yyreset();

    yy_set_current_file(file_path);
    yyin = configuration_file;

    if (yyparse()) {
        yylex_destroy();
        fclose(configuration_file);
        return NULL;
    }

    /* Finally, build the output struct. */

    output_struct = (config_struct_t *)malloc(sizeof(config_struct_t));

    if (!output_struct) {
        strcpy(extern_errormsg, strerror(errno));
        yylex_destroy();
        fclose(configuration_file);
        return NULL;
    }

    output_struct->syntax_tree = program_result;

    yylex_destroy();
    fclose(configuration_file);
    return (config_file_t) output_struct;
}

/* If config_ParseFile returns a NULL pointer,
 * config_GetErrorMsg returns a detailed message
 * to indicate the reason for this error.
 */
char *rh_config_GetErrorMsg(void)
{
    return extern_errormsg;
}

/**
 * config_Print:
 * Print the content of the syntax tree
 * to a file.
 */
void rh_config_Print(FILE *output, config_file_t config)
{
    /* sanity check */
    if (!config)
        return;

    rh_config_print_list(output, ((config_struct_t *)config)->syntax_tree);
}

/**
 * config_Free:
 * Free the memory structure that store the configuration.
 */

void rh_config_Free(config_file_t config)
{
    config_struct_t *config_struct = (config_struct_t *)config;

    if (!config_struct)
        return;

    rh_config_free_list(config_struct->syntax_tree);

    free(config_struct);

    return;
}

/**
 * config_GetNbBlocks:
 * Indicates how many blocks are defined into the config file.
 */
int rh_config_GetNbBlocks(config_file_t config)
{

    config_struct_t *config_struct = (config_struct_t *)config;

    if (!config_struct)
        return -EFAULT;

    /* is list empty? */
    if (!(*config_struct->syntax_tree)) {
        return 0;
    }
    /* count how many items are in the list */
    else {
        /* there is at least one item: the first */
        generic_item *curr_block = (*config_struct->syntax_tree);
        int           nb = 1;

        while ((curr_block = curr_block->next) != NULL) {
            nb++;
        }

        return nb;
    }
}

/* retrieves a given block from the config file, from its index */
config_item_t rh_config_GetBlockByIndex(config_file_t config,
                                        unsigned int block_no)
{
    config_struct_t *config_struct = (config_struct_t *)config;
    generic_item    *curr_block;
    unsigned int     i;

    if (!config_struct->syntax_tree || !(*config_struct->syntax_tree))
        return NULL;

    for (i = 0, curr_block = (*config_struct->syntax_tree);
         curr_block != NULL; curr_block = curr_block->next, i++) {
        if (i == block_no)
            return (config_item_t) curr_block;
    }

    /* not found */
    return NULL;
}

/* Return the name of a block */
char *rh_config_GetBlockName(config_item_t block)
{
    generic_item *curr_block = (generic_item *)block;

    if (!curr_block || (curr_block->type != TYPE_BLOCK))
        return NULL;

    return curr_block->item.block.block_name;
}

/**
 * Return the block identifier, if it exists
 */
char *rh_config_GetBlockId(config_item_t block)
{
    generic_item *curr_block = (generic_item *)block;

    if (!curr_block || (curr_block->type != TYPE_BLOCK))
        return NULL;

    return curr_block->item.block.block_id;
}

/* Indicates how many items are defines in a block */
int rh_config_GetNbItems(config_item_t block)
{
    generic_item *the_block = (generic_item *)block;

    if (!the_block || (the_block->type != TYPE_BLOCK))
        return -1;

    /* check if list is empty */
    if (!(the_block->item.block.block_content)) {
        return 0;
    }
    /* count the number of items in the list */
    else {
        /* there is at least one item: the first */
        generic_item *curr_block = the_block->item.block.block_content;
        int nb = 1;

        while ((curr_block = curr_block->next) != NULL) {
            nb++;
        }

        return nb;
    }

}

/**
 * Count how many items with the given name are defined in a block
 */
int rh_config_CountItemNames(config_item_t block, const char *name)
{
    generic_item *the_block = (generic_item *)block;

    if (!the_block || (the_block->type != TYPE_BLOCK))
        return -1;

    /* is the list empty */
    if (!(the_block->item.block.block_content)) {
        return 0;
    } else {
        /* count items */
        generic_item *curr_item;
        int           nb = 0;

        for (curr_item = the_block->item.block.block_content;
             curr_item != NULL; curr_item = curr_item->next) {
            switch (rh_config_ItemType((config_item_t) curr_item)) {
            case CONFIG_ITEM_BLOCK:
                if (!STRNCMP(curr_item->item.block.block_name, name, MAXSTRLEN))
                    nb++;
                break;
            case CONFIG_ITEM_VAR:
                if (!STRNCMP(curr_item->item.affect.varname, name, MAXSTRLEN))
                    nb++;
                break;
            default:
                return -1;
            }
        }

        return nb;
    }

}

/**
 * Count how many blocks with the given name are in config file
 */
int rh_config_CountBlockNames(config_file_t cfg, const char *name)
{
    config_struct_t *config_struct = (config_struct_t *)cfg;
    generic_item    *curr_block;
    unsigned int     i;
    unsigned int     count = 0;

    if (!config_struct->syntax_tree || !(*config_struct->syntax_tree))
        return -1;

    for (i = 0, curr_block = (*config_struct->syntax_tree);
         curr_block != NULL; curr_block = curr_block->next, i++) {
        if (!strcasecmp(curr_block->item.block.block_name, name))
            count++;
    }

    return count;
}

/* retrieves a given block from the config file, from its index */
config_item_t rh_config_GetItemByIndex(config_item_t block,
                                       unsigned int item_no)
{
    generic_item *the_block = (generic_item *)block;
    generic_item *curr_item;
    unsigned int i;

    if (!the_block || (the_block->type != TYPE_BLOCK))
        return NULL;

    for (i = 0, curr_item = the_block->item.block.block_content;
         curr_item != NULL; curr_item = curr_item->next, i++) {
        if (i == item_no)
            return (config_item_t) curr_item;
    }

    /* not found */
    return NULL;
}

/* indicates which type of item it is */
config_item_type rh_config_ItemType(config_item_t item)
{
    generic_item *the_item = (generic_item *)item;

    switch (the_item->type) {
    case TYPE_BLOCK:
        return CONFIG_ITEM_BLOCK;
    case TYPE_AFFECT:
        return CONFIG_ITEM_VAR;
    case TYPE_BOOL_EXPR:
        return CONFIG_ITEM_BOOL_EXPR;
    case TYPE_SET:
        return CONFIG_ITEM_SET;
    default:
        return 0;
    }
}

/* indicates which type of block content */
config_item_type rh_config_ContentType(config_item_t block)
{
    generic_item *item = (generic_item *)block;

    if (item->type != TYPE_BLOCK)
        return 0;

    if (item->item.block.block_content == NULL)
        return 0;

    return rh_config_ItemType((config_item_t)item->item.block.block_content);
}

/* Retrieves a key-value peer from a CONFIG_ITEM_VAR */
int rh_config_GetKeyValue(config_item_t item,
                          char **var_name, char **var_value,
                          int *have_extra_args)
{
    generic_item *var = (generic_item *)item;

    if (rh_config_ItemType(item) != CONFIG_ITEM_VAR) {
        strcpy(extern_errormsg,
               "Expression needs to be interpreted as a <key>=<value> expression, but it is not.");
        return -1;
    }

    *var_name = var->item.affect.varname;
    *var_value = var->item.affect.varvalue;

    *have_extra_args =
        ((var->item.affect.arg_list != NULL)
         && (var->item.affect.arg_list->nb_args > 0));

    return 0;
}

/* Returns the number of arguments */
int rh_config_GetExtraArgs(config_item_t item, char ***p_extra_arg_array)
{
    generic_item *var = (generic_item *)item;
    arg_list_t   *arglist = NULL;

    if (!var)
        return -1;

    if ((var->type == TYPE_BOOL_EXPR)
        && (var->item.bool_expr.type == BOOL_CONDITION)) {
        arglist = var->item.bool_expr.expr_u.key_value.arg_list;
    } else if (var->type == TYPE_AFFECT) {
        arglist = var->item.affect.arg_list;
    } else {
        return -1;
    }

    if (arglist) {
        *p_extra_arg_array = arglist->args;
        return arglist->nb_args;
    } else {
        *p_extra_arg_array = NULL;
        return 0;
    }
}

static const char *cfg_item_name(generic_item *item)
{
    switch (rh_config_ItemType((config_item_t)item)) {
    case CONFIG_ITEM_BLOCK:
        return item->item.block.block_name;
    case CONFIG_ITEM_VAR:
        return item->item.affect.varname;
    default:
        return NULL;
    }
}

/* get an item from a list with the given name */
static generic_item *GetItemFromList(generic_item *list, const char *name,
                                     bool *ensure_unique)
{
    generic_item *curr;
    generic_item *save = NULL;

    for (curr = list; curr != NULL; curr = curr->next) {
        const char *item_name = cfg_item_name(curr);

        if (item_name == NULL)  /*unnamed item */
            continue;

        if (!STRNCMP(item_name, name, MAXSTRLEN)) {
            if (!(*ensure_unique))
                /* return first match */
                return curr;

            /* must check unicity */
            if (save != NULL) {
                *ensure_unique = false;
                return curr;    /* return conflicting item */
            } else
                save = curr;
        }
    }
    return save;
}

/* Returns the block with the specified name.
 * This name can be "BLOCK::SUBBLOCK::SUBBLOCK" */
config_item_t rh_config_FindItemByName(config_file_t config, const char *name,
                                       bool *ensure_unique)
{
    config_struct_t *config_struct = (config_struct_t *)config;
    generic_item    *block;
    generic_item    *list;
    char            *separ;
    char            *current;
    char             tmp_name[MAXSTRLEN];

    /* cannot be found if empty */
    if (!config_struct->syntax_tree || !(*config_struct->syntax_tree))
        return NULL;

    list = *config_struct->syntax_tree;

    rh_strncpy(tmp_name, name, MAXSTRLEN);
    current = tmp_name;

    while (current) {
        /* first, split the name into BLOCK/SUBBLOC/SUBBLOC */
        separ = strstr(current, "::");

        /* it is a whole name */
        if (!separ)
            return (config_item_t) GetItemFromList(list, current,
                                                   ensure_unique);
        else {
            /* split the name */
            *separ = '\0';

            if ((separ - tmp_name) < MAXSTRLEN - 2)
                separ += 2;
            else
                return NULL;    /* overflow */

            block = GetItemFromList(list, current, ensure_unique);

            /* not found or not a block ? */
            if (!block || (block->type != TYPE_BLOCK))
                return NULL;

            list = block->item.block.block_content;

            /* "::" was found, must have something after */
            current = separ;
        }
    }

    /* not found */
    return NULL;

}

/* Directly returns the value of the key with the specified name.
 * This name can be "BLOCK::SUBBLOCK::SUBBLOCK::VARNAME"
 */
char *rh_config_FindKeyValueByName(config_file_t config, const char *key_name,
                                   bool *ensure_unique)
{
    generic_item *var;

    var = (generic_item *)rh_config_FindItemByName(config, key_name,
                                                   ensure_unique);

    if (!var || (rh_config_ItemType((config_item_t) var) != CONFIG_ITEM_VAR))
        return NULL;
    else
        return var->item.bool_expr.expr_u.key_value.varvalue;

}

/* Returns a block or variable with the specified name from the given block" */
config_item_t rh_config_GetItemByName(config_item_t block, const char *name,
                                      bool *ensure_unique)
{
    generic_item  *curr_block = (generic_item *)block;
    generic_item  *list;
    char          *separ;
    char          *current;
    char           tmp_name[MAXSTRLEN];

    /* cannot be found if empty or non block */
    if (!curr_block || (curr_block->type != TYPE_BLOCK))
        return NULL;

    list = curr_block->item.block.block_content;

    rh_strncpy(tmp_name, name, MAXSTRLEN);
    current = tmp_name;

    while (current) {
        /* first, split the name into BLOCK/SUBBLOC/SUBBLOC */
        separ = strstr(current, "::");

        /* it is a whole name */
        if (!separ)
            return (config_item_t) GetItemFromList(list, current,
                                                   ensure_unique);
        else {
            /* split the name */
            *separ = '\0';

            if ((separ - tmp_name) < MAXSTRLEN - 2)
                separ += 2;
            else
                return NULL;    /* overflow */

            curr_block = GetItemFromList(list, current, ensure_unique);

            /* not found or not a block ? */
            if (!curr_block || (curr_block->type != TYPE_BLOCK))
                return NULL;

            list = curr_block->item.block.block_content;

            /* "::" was found, must have something after */
            current = separ;
        }
    }

    /* not found */
    return NULL;

}

/* Directly returns the value of the key with the specified name
 * relative to the given block.
 */
char *rh_config_GetKeyValueByName(config_item_t block, const char *key_name,
                                  bool *ensure_unique)
{
    generic_item *var;

    var = (generic_item *)rh_config_GetItemByName(block, key_name,
                                                  ensure_unique);

    if (!var || (rh_config_ItemType((config_item_t) var) != CONFIG_ITEM_VAR)) {
        strcpy(extern_errormsg, "item not found (or is not a parameter)");
        return NULL;
    }

    return var->item.affect.varvalue;

}

/* Get item line */
int rh_config_GetItemLine(config_item_t item)
{
    generic_item *curr = (generic_item *)item;

    return curr->line;
}
