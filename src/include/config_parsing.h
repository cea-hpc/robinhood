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
 * \file config_parsing.h
 * \author Th. Leibovici
 *
 * \brief High-level functions for browsing syntax tree.
 */
/**
 * \addtogroup CONFIG_PARSING
 * @{
 */
#ifndef _CONFIG_PARSING_H
#define _CONFIG_PARSING_H

#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>

/* opaque type */
typedef caddr_t config_file_t;
typedef caddr_t config_item_t;

typedef enum {
    CONFIG_ITEM_BLOCK = 1,
    CONFIG_ITEM_VAR,
    CONFIG_ITEM_BOOL_EXPR,
    CONFIG_ITEM_SET
} config_item_type;

/**
 * Reads the content of a configuration file and
 * stores it in a memory structure.
 * \return NULL on error.
 */
config_file_t  rh_config_ParseFile( char *file_path );


/**
 * If config_ParseFile returns a NULL pointer,
 * config_GetErrorMsg returns a detailled message
 * to indicate the reason for this error.
 */
char          *rh_config_GetErrorMsg( void );

/**
 * config_Print:
 * Print the content of the syntax tree
 * to a file.
 */
void           rh_config_Print( FILE * output, config_file_t config );


/** Free the memory structure that store the configuration. */
void           rh_config_Free( config_file_t config );


/**
 * Indicates how many main blocks are defined into the config file.
 * \return A positive value if no error.
 *         Else return a negative error code.
 */
int            rh_config_GetNbBlocks( config_file_t config );


/**
 * Retrieves a given block from the config file, from its index
 */
config_item_t  rh_config_GetBlockByIndex( config_file_t config, unsigned int block_no );


/**
 * Return the name of a block
 */
char          *rh_config_GetBlockName( config_item_t block );

/**
 * Return the block identifier, if it exists
 */
char          *rh_config_GetBlockId( config_item_t block );


/**
 * Indicates how many items are defined in a block
 */
int            rh_config_GetNbItems( config_item_t block );

/**
 * Count how many items with the given name are defined in a block
 */
int            rh_config_CountItemNames( config_item_t block, const char *name );

/**
 * Count how many blocks with the given name are in config file
 */
int rh_config_CountBlockNames( config_file_t cfg, const char *name );


/**
 * Retrieves an item from a given block and the subitem index.
 */
config_item_t  rh_config_GetItemByIndex( config_item_t block, unsigned int item_no );

/**
 * Indicates which type of item it is
 */
config_item_type rh_config_ItemType( config_item_t item );

/* indicates which type of block content */
config_item_type rh_config_ContentType( config_item_t block );

/**
 * Retrieves a key-value peer from a CONFIG_ITEM_VAR
 */
int          rh_config_GetKeyValue( config_item_t item,
                                   char **var_name, char **var_value, int *have_extra_args );

/**
 * Returns the number of arguments
 */
int          rh_config_GetExtraArgs( config_item_t item, char ***p_extra_arg_array );


/**
 * Returns a block or variable with the specified name. This name can be "BLOCK::SUBBLOCK::SUBBLOCK"
 */
config_item_t  rh_config_FindItemByName( config_file_t config, const char *name );

/**
 * Directly returns the value of the key with the specified name.
 * This name can be "BLOCK::SUBBLOCK::SUBBLOCK::VARNAME"
 */
char          *rh_config_FindKeyValueByName( config_file_t config, const char *key_name );


/**
 * Returns a block or variable with the specified name from the given block"
 */
config_item_t  rh_config_GetItemByName( config_item_t block, const char *name );

/**
 * Directly returns the value of the key with the specified name
 * relative to the given block.
 */
char          *rh_config_GetKeyValueByName( config_item_t block, const char *key_name );

/**
 * Get item line
 */
int            rh_config_GetItemLine( config_item_t item );

#endif

/**
 * @}
 */
