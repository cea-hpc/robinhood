/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2008, 2009, 2010 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
/**
 *  Module for configuration management and parsing.
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "RobinhoodConfig.h"
#include "RobinhoodMisc.h"
#include "analyze.h"
#include "xplatform_print.h"
#include <errno.h>
#include <stdarg.h>

#define XATTR_PREFIX    "xattr"

char *process_config_file = "";

/**
 * Read robinhood's configuration file and fill config struct.
 * if everything is OK, returns 0 and fills the structure
 * else, returns an error code and sets a contextual error message in err_msg_out.
 */
int ReadRobinhoodConfig( int module_mask, char *file_path, char *err_msg_out,
                         robinhood_config_t * config_struct, int for_reload )
{
    config_file_t  syntax_tree;
    int            rc = 0;
    char           msg_buf[2048] = "";
    const module_config_def_t *p_curr;

    /* First, Parse the configuration file */
    syntax_tree = rh_config_ParseFile( file_path );

    if ( syntax_tree == NULL )
    {
        strcpy( err_msg_out, rh_config_GetErrorMsg(  ) );
        return EINVAL;
    }

#ifdef _DEBUG_PARSING
    rh_config_Print( stdout, syntax_tree );
#endif

    /* Set defaults to the structure, then load values from syntax tree  */

    for ( p_curr = &robinhood_module_conf[0]; p_curr->module_name != NULL; p_curr++ )
    {
        void          *module_config;

        /* only initialize modules with flag MODULE_MASK_ALWAYS or matching 'module_mask' parameter */
        if ( ( p_curr->flags != MODULE_MASK_ALWAYS ) && !( p_curr->flags & module_mask ) )
            continue;

        module_config = ( void * ) config_struct + p_curr->module_config_offset;
        rc = p_curr->set_default_func( module_config, msg_buf );

        if ( rc )
        {
            sprintf( err_msg_out,
                     "Error %d setting default configuration for module '%s':\n%s",
                     rc, p_curr->module_name, msg_buf );
            goto config_free;
        }

        rc = p_curr->read_func( syntax_tree, module_config, msg_buf, for_reload );

        if ( rc != 0 )
        {
            sprintf( err_msg_out,
                     "Error %d reading configuration for module '%s':\n%s",
                     rc, p_curr->module_name, msg_buf );
            goto config_free;
        }
    }


  config_free:

    /* free config file resources */
    rh_config_Free( syntax_tree );

    return rc;
}

/**
 * Reload robinhood's configuration file (the one used for last call to ReadRobinhoodConfig),
 * and change only parameters that can be modified on the fly.
 */
int ReloadRobinhoodConfig( int module_mask, robinhood_config_t * new_config )
{
    int            rc = 0;
    const module_config_def_t *p_curr;

#define RELOAD_TAG "ReloadConfig"

    for ( p_curr = &robinhood_module_conf[0]; p_curr->module_name != NULL; p_curr++ )
    {
        void          *module_config;
        int rc_temp = 0;

        /* only initialize modules with flag MODULE_MASK_ALWAYS or matching 'module_mask' parameter */
        if ( ( p_curr->flags != MODULE_MASK_ALWAYS ) && !( p_curr->flags & module_mask ) )
            continue;

        module_config = ( void * ) new_config + p_curr->module_config_offset;

        /* finally reload the configuration */
        rc_temp = p_curr->reload_func( module_config );
        if ( rc_temp )
        {
            DisplayLog( LVL_CRIT, RELOAD_TAG, "Error %d reloading configuration for module '%s'",
                        rc_temp, p_curr->module_name );
            rc = rc_temp;
        }
        else
            DisplayLog( LVL_EVENT, RELOAD_TAG, "Configuration of module '%s' successfully reloaded",
                        p_curr->module_name );
    }

    return rc;
}

/**
 * Write a documented template of configuration file.
 * returns 0 on success, else it returns a posix error code.
 */
int WriteConfigTemplate( FILE * stream )
{
    const module_config_def_t *p_module;
    int            rc;

    fprintf( stream, "##########################################\n" );
    fprintf( stream, "# Robinhood configuration file template  #\n" );
    fprintf( stream, "##########################################\n\n" );

    for ( p_module = &robinhood_module_conf[0]; p_module->module_name != NULL; p_module++ )
    {
        fprintf( stream, "# %s configuration\n", p_module->module_name );
        rc = p_module->write_template_func( stream );
        fprintf( stream, "\n" );
        if ( rc )
            return rc;
    }
    return 0;
}

/**
 * Write all default configuration values,
 * to the given file path.
 * returns 0 on success, else it returns a posix error code.
 */
int WriteConfigDefault( FILE * stream )
{
    const module_config_def_t *p_module;
    int            rc;

    fprintf( stream, "# Default configuration values\n" );

    for ( p_module = &robinhood_module_conf[0]; p_module->module_name != NULL; p_module++ )
    {
        rc = p_module->write_default_func( stream );
        fprintf( stream, "\n" );
        if ( rc )
            return rc;
    }
    return 0;

}

/**
 *  For debugging.
 */
void DisplayConfiguration( FILE * stream, robinhood_config_t * config )
{
}

#define INDENT_STEP 4
void print_begin_block( FILE * output, unsigned int indent, const char *blockname, const char *id )
{
    char          *indent_char = ( indent ? " " : "" );

    if ( id )
        fprintf( output, "%*s%s\t%s\n", indent * INDENT_STEP, indent_char, blockname, id );
    else
        fprintf( output, "%*s%s\n", indent * INDENT_STEP, indent_char, blockname );
    fprintf( output, "%*s{\n", indent * INDENT_STEP, indent_char );
}

void print_end_block( FILE * output, unsigned int indent )
{
    char          *indent_char = ( indent ? " " : "" );
    fprintf( output, "%*s}\n", indent * INDENT_STEP, indent_char );
}

void print_line( FILE * output, unsigned int indent, const char *format, ... )
{
    va_list        arglist;
    char          *indent_char = ( indent ? " " : "" );

    fprintf( output, "%*s", indent * INDENT_STEP, indent_char );

    va_start( arglist, format );
    vfprintf( output, format, arglist );
    va_end( arglist );

    fprintf( output, "\n" );
}


/**
 * Misc. tools for config parsing
 */
int GetStringParam( config_item_t block,
                    const char *block_name, char *var_name, int flags,
                    char *target, unsigned int target_size,
                    char ***extra_args_tab, unsigned int *nb_extra_args, char *err_msg )
{
    config_item_t  curr_item;
    int            rc;
    int            extra = 0;
    char          *name;
    char          *value;

    err_msg[0] = '\0';

    if ( nb_extra_args )
        *nb_extra_args = 0;
    if ( extra_args_tab )
        *extra_args_tab = NULL;


    curr_item = rh_config_GetItemByName( block, var_name );
    if ( !curr_item )
    {
        if ( flags & PARAM_MANDATORY )
            sprintf( err_msg, "Missing mandatory parameter '%s' in block '%s', line %d", var_name,
                     block_name, rh_config_GetItemLine( block ) );
        /* return ENOENT in any case */
        return ENOENT;
    }

    rc = rh_config_GetKeyValue( curr_item, &name, &value, &extra );
    if ( rc )
    {
        sprintf( err_msg, "Error retrieving parameter value for '%s::%s', line %d:\n%s", block_name,
                 var_name, rh_config_GetItemLine( curr_item ), rh_config_GetErrorMsg(  ) );
        return rc;
    }

    rh_strncpy(target, value, target_size);

    if ( extra )
    {
        if ( !extra_args_tab || !nb_extra_args )
        {
            sprintf( err_msg, "Unexpected options for parameter '%s::%s', line %d", block_name,
                     var_name, rh_config_GetItemLine( curr_item ) );
            return EINVAL;
        }
        else
        {
            *nb_extra_args = rh_config_GetExtraArgs( curr_item, extra_args_tab );
        }
    }

    /* checks */

    if (flags & STR_PARAM_NOT_EMPTY)
    {
        if (EMPTY_STRING(target))
        {
            sprintf(err_msg, "Unexpected empty parameter '%s::%s', line %d", block_name,
                    var_name, rh_config_GetItemLine(curr_item));
            return EINVAL;
        }
    }

    /* are stdio names allowed ? */
    if ( flags & STDIO_ALLOWED )
    {
        if ( !strcasecmp( target, "stdout" ) || !strcasecmp( target, "stderr" )
             || !strcasecmp( target, "syslog") )
            return 0;
    }

    if ( flags & STR_PARAM_ABSOLUTE_PATH )
    {
        /* check this is an absolute path */
        if ( !IS_ABSOLUTE_PATH( target ) )
        {
            sprintf( err_msg, "Absolute path expected for parameter '%s::%s', line %d", block_name,
                     var_name, rh_config_GetItemLine( curr_item ) );
            return EINVAL;
        }
    }

    if ( flags & STR_PARAM_NO_WILDCARDS )
    {
        if ( WILDCARDS_IN( target ) )
        {
            sprintf( err_msg, "Wildcards are not allowed in '%s::%s', line %d", block_name,
                     var_name, rh_config_GetItemLine( curr_item ) );
            return EINVAL;
        }
    }

    if ( flags & STR_PARAM_MAIL )
    {
        char          *arob = strchr( target, '@' );

        /* check there is an arobase, and this arobase has text before and after */
        if ( ( arob == NULL ) || ( arob == target ) || ( *( arob + 1 ) == '\0' ) )
        {
            sprintf( err_msg, "Invalid mail address in '%s::%s', line %d", block_name, var_name,
                     rh_config_GetItemLine( curr_item ) );
            return EINVAL;
        }
    }

    if ( flags & STR_PARAM_REMOVE_FINAL_SLASH )
    {
        /* remove final slash */
        if ( FINAL_SLASH( target ) )
            REMOVE_FINAL_SLASH( target );
    }

    return 0;

}

int GetBoolParam( config_item_t block,
                  const char *block_name, char *var_name, int flags, int *target,
                  char ***extra_args_tab, unsigned int *nb_extra_args, char *err_msg )
{
    config_item_t  curr_item;
    int            rc, extra;
    char          *name;
    char          *value;

    err_msg[0] = '\0';

    curr_item = rh_config_GetItemByName( block, var_name );
    if ( !curr_item )
    {
        if ( flags & PARAM_MANDATORY )
            sprintf( err_msg, "Missing mandatory parameter '%s' in block '%s', line %d", var_name,
                     block_name, rh_config_GetItemLine( block ) );
        /* return ENOENT in any case */
        return ENOENT;
    }

    rc = rh_config_GetKeyValue( curr_item, &name, &value, &extra );
    if ( rc )
    {
        sprintf( err_msg, "Error retrieving parameter value for '%s::%s', line %d:\n%s", block_name,
                 var_name, rh_config_GetItemLine( curr_item ), rh_config_GetErrorMsg(  ) );
        return rc;
    }

    *target = str2bool( value );
    if ( *target == -1 )
    {
        sprintf( err_msg,
                 "Invalid value for '%s::%s', line %d: boolean expected (0, 1, TRUE, FALSE, ENABLED, DISABLED)",
                 block_name, var_name, rh_config_GetItemLine( curr_item ) );
        return EINVAL;
    }

    if ( extra )
    {
        if ( !extra_args_tab || !nb_extra_args )
        {
            sprintf( err_msg, "Unexpected options for parameter '%s::%s', line %d", block_name,
                     var_name, rh_config_GetItemLine( curr_item ) );
            return EINVAL;
        }
        else
        {
            *nb_extra_args = rh_config_GetExtraArgs( curr_item, extra_args_tab );
        }
    }

    return 0;

}


/**
 *  Retrieve a duration parameter and check its format
 *  @return 0 on success
 *          ENOENT if the parameter does not exist in the block
 *          EINVAL if the parameter does not satisfy restrictions
 */
int GetDurationParam( config_item_t block,
                      const char *block_name, char *var_name, int flags, int *target,
                      char ***extra_args_tab, unsigned int *nb_extra_args, char *err_msg )
{
    config_item_t  curr_item;
    int            rc, extra, timeval;
    char          *name;
    char          *value;

    err_msg[0] = '\0';

    if ( nb_extra_args )
        *nb_extra_args = 0;
    if ( extra_args_tab )
        *extra_args_tab = NULL;

    curr_item = rh_config_GetItemByName( block, var_name );
    if ( !curr_item )
    {
        if ( flags & PARAM_MANDATORY )
            sprintf( err_msg, "Missing mandatory parameter '%s' in block '%s', line %d", var_name,
                     block_name, rh_config_GetItemLine( block ) );
        /* return ENOENT in any case */
        return ENOENT;
    }

    rc = rh_config_GetKeyValue( curr_item, &name, &value, &extra );
    if ( rc )
    {
        sprintf( err_msg, "Error retrieving parameter value for '%s::%s', line %d:\n%s", block_name,
                 var_name, rh_config_GetItemLine( curr_item ), rh_config_GetErrorMsg(  ) );
        return rc;
    }

    timeval = str2duration( value );
    if ( timeval == -1 )
    {
        sprintf( err_msg, "Invalid value for '%s::%s', line %d: duration expected. Eg: 10s",
                 block_name, var_name, rh_config_GetItemLine( curr_item ) );
        return EINVAL;
    }

    if ( ( flags & INT_PARAM_POSITIVE ) && ( timeval < 0 ) )
    {
        sprintf( err_msg, "Positive value expected for '%s::%s', line %d.", block_name, var_name,
                 rh_config_GetItemLine( curr_item ) );
        return EINVAL;
    }
    if ( ( flags & INT_PARAM_NOT_NULL ) && ( timeval == 0 ) )
    {
        sprintf( err_msg, "'%s::%s' must not be null, line %d.", block_name, var_name,
                 rh_config_GetItemLine( curr_item ) );
        return EINVAL;
    }

    *target = timeval;

    if ( extra )
    {
        if ( !extra_args_tab || !nb_extra_args )
        {
            sprintf( err_msg, "Unexpected options for parameter '%s::%s', line %d", block_name,
                     var_name, rh_config_GetItemLine( curr_item ) );
            return EINVAL;
        }
        else
        {
            *nb_extra_args = rh_config_GetExtraArgs( curr_item, extra_args_tab );
        }
    }

    return 0;
}

/**
 *  Retrieve a size parameter and check its format
 *  @return 0 on success
 *          ENOENT if the parameter does not exist in the block
 *          EINVAL if the parameter does not satisfy restrictions
 */
int GetSizeParam( config_item_t block,
                  const char *block_name, char *var_name, int flags, unsigned long long *target,
                  char ***extra_args_tab, unsigned int *nb_extra_args, char *err_msg )
{
    config_item_t  curr_item;
    int            rc;
    int            extra = 0;
    unsigned long long sizeval;
    char          *name;
    char          *value;

    err_msg[0] = '\0';

    if ( nb_extra_args )
        *nb_extra_args = 0;
    if ( extra_args_tab )
        *extra_args_tab = NULL;

    curr_item = rh_config_GetItemByName( block, var_name );
    if ( !curr_item )
    {
        if ( flags & PARAM_MANDATORY )
            sprintf( err_msg, "Missing mandatory parameter '%s' in block '%s', line %d", var_name,
                     block_name, rh_config_GetItemLine( block ) );
        /* return ENOENT in any case */
        return ENOENT;
    }

    rc = rh_config_GetKeyValue( curr_item, &name, &value, &extra );
    if ( rc )
    {
        sprintf( err_msg, "Error retrieving parameter value for '%s::%s', line %d:\n%s", block_name,
                 var_name, rh_config_GetItemLine( curr_item ), rh_config_GetErrorMsg(  ) );
        return rc;
    }

    sizeval = str2size( value );
    if ( sizeval == ( unsigned long long ) -1 )
    {
        sprintf( err_msg, "Invalid value for '%s::%s', line %d: size expected. Eg: 10MB",
                 block_name, var_name, rh_config_GetItemLine( curr_item ) );
        return EINVAL;
    }

    if ( ( flags & INT_PARAM_NOT_NULL ) && ( sizeval == 0 ) )
    {
        sprintf( err_msg, "'%s::%s' must not be null, line %d.", block_name, var_name,
                 rh_config_GetItemLine( curr_item ) );
        return EINVAL;
    }

    *target = sizeval;

    if ( extra )
    {
        if ( !extra_args_tab || !nb_extra_args )
        {
            sprintf( err_msg, "Unexpected options for parameter '%s::%s', line %d", block_name,
                     var_name, rh_config_GetItemLine( curr_item ) );
            return EINVAL;
        }
        else
        {
            *nb_extra_args = rh_config_GetExtraArgs( curr_item, extra_args_tab );
        }
    }

    return 0;
}


/**
 *  Retrieve an integer parameter and check its format
 *  @return 0 on success
 *          ENOENT if the parameter does not exist in the block
 *          EINVAL if the parameter does not satisfy restrictions
 */
int GetIntParam( config_item_t block,
                 const char *block_name, char *var_name, int flags, int *target,
                 char ***extra_args_tab, unsigned int *nb_extra_args, char *err_msg )
{
    config_item_t  curr_item;
    int            rc, extra, intval, nb_read;
    char          *name;
    char          *value;
    char           tmpbuf[256];

    err_msg[0] = '\0';

    if ( nb_extra_args )
        *nb_extra_args = 0;
    if ( extra_args_tab )
        *extra_args_tab = NULL;

    curr_item = rh_config_GetItemByName( block, var_name );
    if ( !curr_item )
    {
        if ( flags & PARAM_MANDATORY )
            sprintf( err_msg, "Missing mandatory parameter '%s' in block '%s', line %d", var_name,
                     block_name, rh_config_GetItemLine( block ) );
        /* return ENOENT in any case */
        return ENOENT;
    }

    rc = rh_config_GetKeyValue( curr_item, &name, &value, &extra );
    if ( rc )
    {
        sprintf( err_msg, "Error retrieving parameter value for '%s::%s', line %d:\n%s", block_name,
                 var_name, rh_config_GetItemLine( curr_item ), rh_config_GetErrorMsg(  ) );
        return rc;
    }

    nb_read = sscanf( value, "%d%256s", &intval, tmpbuf );
    if ( nb_read < 1 )
    {
        sprintf( err_msg, "Invalid value for '%s::%s', line %d: integer expected.", block_name,
                 var_name, rh_config_GetItemLine( curr_item ) );
        return EINVAL;
    }
    if ( ( nb_read > 1 ) && ( tmpbuf[0] != '\0' ) )
    {
        sprintf( err_msg,
                 "Invalid value for '%s::%s', line %d: extra characters '%s' found after integer %d.",
                 block_name, var_name, rh_config_GetItemLine( curr_item ), tmpbuf, intval );
        return EINVAL;
    }

    if ( ( flags & INT_PARAM_POSITIVE ) && ( intval < 0 ) )
    {
        sprintf( err_msg, "Positive value expected for '%s::%s', line %d.", block_name, var_name,
                 rh_config_GetItemLine( curr_item ) );
        return EINVAL;
    }
    if ( ( flags & INT_PARAM_NOT_NULL ) && ( intval == 0 ) )
    {
        sprintf( err_msg, "'%s::%s' must not be null, line %d.", block_name, var_name,
                 rh_config_GetItemLine( curr_item ) );
        return EINVAL;
    }

    *target = intval;

    if ( extra )
    {
        if ( !extra_args_tab || !nb_extra_args )
        {
            sprintf( err_msg, "Unexpected options for parameter '%s::%s', line %d", block_name,
                     var_name, rh_config_GetItemLine( curr_item ) );
            return EINVAL;
        }
        else
        {
            *nb_extra_args = rh_config_GetExtraArgs( curr_item, extra_args_tab );
        }
    }

    return 0;
}

/**
 *  Retrieve a long integer parameter and check its format.
 *  (a suffix can be used in config file).
 *  @return 0 on success
 *          ENOENT if the parameter does not exist in the block
 *          EINVAL if the parameter does not satisfy restrictions
 */
int GetInt64Param( config_item_t block,
                   const char *block_name, char *var_name, int flags,
                   uint64_t *target,
                   char ***extra_args_tab, unsigned int *nb_extra_args,
                   char *err_msg )
{
    config_item_t  curr_item;
    int            rc, extra, nb_read;
    uint64_t	   intval;
    char          *name;
    char          *value;
    char           tmpbuf[256];

    err_msg[0] = '\0';

    if ( nb_extra_args )
        *nb_extra_args = 0;
    if ( extra_args_tab )
        *extra_args_tab = NULL;

    curr_item = rh_config_GetItemByName( block, var_name );
    if ( !curr_item )
    {
        if ( flags & PARAM_MANDATORY )
            sprintf( err_msg, "Missing mandatory parameter '%s' in block '%s', line %d", var_name,
                     block_name, rh_config_GetItemLine( block ) );
        /* return ENOENT in any case */
        return ENOENT;
    }

    rc = rh_config_GetKeyValue( curr_item, &name, &value, &extra );
    if ( rc )
    {
        sprintf( err_msg, "Error retrieving parameter value for '%s::%s', line %d:\n%s", block_name,
                 var_name, rh_config_GetItemLine( curr_item ), rh_config_GetErrorMsg(  ) );
        return rc;
    }

    nb_read = sscanf( value, "%"SCNu64"%256s", &intval, tmpbuf );
    if ( nb_read < 1 )
    {
        sprintf( err_msg, "Invalid value for '%s::%s', line %d: integer expected.", block_name,
                 var_name, rh_config_GetItemLine( curr_item ) );
        return EINVAL;
    }
    if ( ( nb_read > 1 ) && ( tmpbuf[0] != '\0' ) )
    {
        /* check suffix */
        if ( !strcasecmp( tmpbuf, "k" ) )
            intval *= 1000ULL; /* thousand */
        else if ( !strcasecmp( tmpbuf, "M" ) )
            intval *= 1000000ULL; /* million */
        else if ( !strcasecmp( tmpbuf, "G" ) )
            intval *= 1000000000ULL; /* billion */
        else if ( !strcasecmp( tmpbuf, "T" ) )
            intval *= 1000000000000ULL; /* trillion */
        else
        {
            sprintf( err_msg, "Invalid suffix for '%s::%s', line %d: '%s'. "
                     "Only 'k', 'M', 'G' or 'T' are allowed.",
                     block_name, var_name, rh_config_GetItemLine( curr_item ),
                     tmpbuf );
            return EINVAL;
        }
    }

    if ( ( flags & INT_PARAM_NOT_NULL ) && ( intval == 0 ) )
    {
        sprintf( err_msg, "'%s::%s' must not be null, line %d.", block_name, var_name,
                 rh_config_GetItemLine( curr_item ) );
        return EINVAL;
    }

    *target = intval;

    if ( extra )
    {
        if ( !extra_args_tab || !nb_extra_args )
        {
            sprintf( err_msg, "Unexpected options for parameter '%s::%s', line %d", block_name,
                     var_name, rh_config_GetItemLine( curr_item ) );
            return EINVAL;
        }
        else
        {
            *nb_extra_args = rh_config_GetExtraArgs( curr_item, extra_args_tab );
        }
    }

    return 0;
}

/**
 *  Retrieve a float parameter and check its format
 *  @return 0 on success
 *          ENOENT if the parameter does not exist in the block
 *          EINVAL if the parameter does not satisfy restrictions
 */
int GetFloatParam( config_item_t block,
                   const char *block_name, char *var_name, int flags, double *target,
                   char ***extra_args_tab, unsigned int *nb_extra_args, char *err_msg )
{
    config_item_t  curr_item;
    int            rc, extra, nb_read;
    double         val;
    char          *name;
    char          *value;
    char           tmpbuf[256];

    err_msg[0] = '\0';

    if ( nb_extra_args )
        *nb_extra_args = 0;
    if ( extra_args_tab )
        *extra_args_tab = NULL;

    curr_item = rh_config_GetItemByName( block, var_name );
    if ( !curr_item )
    {
        if ( flags & PARAM_MANDATORY )
            sprintf( err_msg, "Missing mandatory parameter '%s' in block '%s', line %d", var_name,
                     block_name, rh_config_GetItemLine( block ) );
        /* return ENOENT in any case */
        return ENOENT;
    }

    rc = rh_config_GetKeyValue( curr_item, &name, &value, &extra );
    if ( rc )
    {
        sprintf( err_msg, "Error retrieving parameter value for '%s::%s', line %d:\n%s", block_name,
                 var_name, rh_config_GetItemLine( curr_item ), rh_config_GetErrorMsg(  ) );
        return rc;
    }

    nb_read = sscanf( value, "%lf%256s", &val, tmpbuf );
    if ( nb_read < 1 )
    {
        sprintf( err_msg, "Invalid value for '%s::%s', line %d: float expected.", block_name,
                 var_name, rh_config_GetItemLine( curr_item ) );
        return EINVAL;
    }
    if ( nb_read > 1 )
    {
        if ( ( !( flags & ALLOW_PCT_SIGN ) && ( tmpbuf[0] != '\0' ) )   /* no sign allowed */
             || ( ( flags & ALLOW_PCT_SIGN ) && ( strcmp( tmpbuf, "%" ) != 0 ) ) )      /* '%' allowed */
        {
            sprintf( err_msg,
                     "Invalid value for '%s::%s', line %d: extra characters '%s' found after float %.2f.",
                     block_name, var_name, rh_config_GetItemLine( curr_item ), tmpbuf, val );
            return EINVAL;
        }
    }

    if ( ( flags & FLOAT_PARAM_POSITIVE ) && ( val < 0.0 ) )
    {
        sprintf( err_msg, "Positive value expected for '%s::%s', line %d.", block_name, var_name,
                 rh_config_GetItemLine( curr_item ) );
        return EINVAL;
    }
    if ( ( flags & FLOAT_PARAM_NOT_NULL ) && ( val == 0.0 ) )
    {
        sprintf( err_msg, "'%s::%s' must not be null, line %d.", block_name, var_name,
                 rh_config_GetItemLine( curr_item ) );
        return EINVAL;
    }

    *target = val;

    if ( extra )
    {
        if ( !extra_args_tab || !nb_extra_args )
        {
            sprintf( err_msg, "Unexpected options for parameter '%s::%s', line %d", block_name,
                     var_name, rh_config_GetItemLine( curr_item ) );
            return EINVAL;
        }
        else
        {
            *nb_extra_args = rh_config_GetExtraArgs( curr_item, extra_args_tab );
        }
    }

    return 0;

}




/**
 *  convert the syntaxic code for comparator to the configuration equivalent code
 */
static inline compare_direction_t syntax2conf_comparator( operator_t op )
{
    switch ( op )
    {
    case OP_EQUAL:
        return COMP_EQUAL;
    case OP_DIFF:
        return COMP_DIFF;
    case OP_GT:
        return COMP_GRTHAN;
    case OP_GT_EQ:
        return COMP_GRTHAN_EQ;
    case OP_LT:
        return COMP_LSTHAN;
    case OP_LT_EQ:
        return COMP_LSTHAN_EQ;
    case OP_CMD:
    default:
        return COMP_NONE;

    }
}

/**
 *  convert the syntaxic code for unary boolean operator to the configuration equivalent code
 */
static inline bool_op_t syntax2conf_boolop( bool_operator_t boolop )
{
    switch ( boolop )
    {
    case BOOL_OP_NOT:
        return BOOL_NOT;
    case BOOL_OP_AND:
        return BOOL_AND;
    case BOOL_OP_OR:
        return BOOL_OR;
    default:
        return BOOL_ERR;
    }
}

static int process_any_level_condition( char * regexpr, char *err_msg )
{
    char * curr = strstr( regexpr, "**" );
    size_t len = strlen(regexpr);

    /* characters before and after '**' can only be '/' */
    for ( curr = strstr( regexpr, "**" ); curr != NULL;
          curr = strstr( curr+2, "**") )
    {
        if ( curr > regexpr )
        {
            char * prev = curr-1;
            /* check character before '**' */
            if ( *prev != '/')
            {
                sprintf( err_msg,
                         "Character before and after '**' must be a '/' in '%s'",
                         regexpr );
                return EINVAL;
            }
        }
        /* - last char is 'regexpr + len - 1'
         * - curr + 2 is the first char after '**'
         */
        if ( (curr + 2) <= (regexpr + len - 1) )
        {
            /* check the character after '**' */
            if (curr[2] != '/')
            {
                sprintf( err_msg,
                         "Character before and after '**' must be a '/' in '%s'",
                         regexpr );
                return EINVAL;
            }
        }
    }

    /* leave single '*' (unfortunatly, they will be interpreted like '**') */
#if 0
    for ( curr = strchr(regexpr, '*'); curr != NULL; curr = strchr(curr+2,'*') )
    {
        if ( curr[1] != '*' )
        {
                sprintf( err_msg,
                        "Single wildcard '*' cannot be used in the same expression as double wildcard '**' in '%s'",
                         regexpr );
                return EINVAL;
        }
    }
#endif
    /* non escaped '?' must be replaced by '[!/]'
     * '**' must be replaced by '*'
     */
    str_replace(regexpr, "?", "[!/]");
    str_replace(regexpr, "**", "*");

    return 0;
}



/* test for a criteria name */
#define TEST_CRIT( _str_, _crit_code ) ( !strcasecmp( _str_, criteria2str(_crit_code)  ) )


/**
 *  interpret and check a condition.
 */
static int interpret_condition( type_key_value * key_value, compare_triplet_t * p_triplet,
                                int *p_attr_mask, char *err_msg )
{
    p_triplet->flags = 0;

    /* check the name for the condition */
    if ( TEST_CRIT( key_value->varname, CRITERIA_TREE ) )
    {
        p_triplet->crit = CRITERIA_TREE;
        *p_attr_mask |= ATTR_MASK_fullpath;

        /* looking for a non-empty absolute path or starting with '*''/' */
        if ( EMPTY_STRING( key_value->varvalue ) )
        {
            strcpy( err_msg,
                    "a path is expected for 'tree' criteria (wildcards allowed)" );
            return EINVAL;
        }

        /* 3 possible values: aboslute path, relative path,
         * path with special wildcard '**' that match any number
         * of directory levels */
        rh_strncpy(p_triplet->val.str, key_value->varvalue, RBH_PATH_MAX);

        if ( ANY_LEVEL_MATCH(p_triplet->val.str) )
        {
            int rc;

            /* check the expression and adapt it to fnmatch */
            rc = process_any_level_condition( p_triplet->val.str, err_msg );
            if ( rc ) return rc;
            p_triplet->flags |= CMP_FLG_ANY_LEVEL;
        }

        /* allowed coparators are = and != */
        p_triplet->op = syntax2conf_comparator( key_value->op_type );

        if ( ( p_triplet->op != COMP_EQUAL ) && ( p_triplet->op != COMP_DIFF ) )
        {
            sprintf( err_msg, "Illegal comparator for 'tree' criteria: == or != expected" );
            return EINVAL;
        }

        /* in case the string contains regexpr, those comparators are changed to LIKE / UNLIKE */
        if ( WILDCARDS_IN( p_triplet->val.str ) )
        {
            if ( p_triplet->op == COMP_EQUAL )
                p_triplet->op = COMP_LIKE;
            else if ( p_triplet->op == COMP_DIFF )
                p_triplet->op = COMP_UNLIKE;
        }
    }
    else if ( TEST_CRIT( key_value->varname, CRITERIA_PATH ) )
    {
        /* check for a non empty path */
        p_triplet->crit = CRITERIA_PATH;
        *p_attr_mask |= ATTR_MASK_fullpath;

        /* looking for a non-empty absolute path or starting with * and / */
        if ( EMPTY_STRING( key_value->varvalue ) )
        {
            strcpy( err_msg,
                    "a path is expected for 'path' criteria (wildcards allowed)" );
            return EINVAL;
        }

        /* 3 possible values: aboslute path, relative path,
         * path with special wildcard '**' that match any number
         * of directory levels */
        rh_strncpy(p_triplet->val.str, key_value->varvalue, RBH_PATH_MAX);

        if ( ANY_LEVEL_MATCH(p_triplet->val.str) )
        {
            int rc;

            /* check the expression and adapt it to fnmatch */
            rc = process_any_level_condition( p_triplet->val.str, err_msg );
            if ( rc ) return rc;
            p_triplet->flags |= CMP_FLG_ANY_LEVEL;
        }

        /* allowed comparators are = and != */
        p_triplet->op = syntax2conf_comparator( key_value->op_type );

        if ( ( p_triplet->op != COMP_EQUAL ) && ( p_triplet->op != COMP_DIFF ) )
        {
            strcpy( err_msg, "Illegal comparator for 'tree' criteria: == or != expected" );
            return EINVAL;
        }

        /* in case the string contains regexpr, those comparators are changed to LIKE / UNLIKE */
        if ( WILDCARDS_IN( p_triplet->val.str ) )
        {
            if ( p_triplet->op == COMP_EQUAL )
                p_triplet->op = COMP_LIKE;
            else if ( p_triplet->op == COMP_DIFF )
                p_triplet->op = COMP_UNLIKE;
        }
    }
    else if ( TEST_CRIT( key_value->varname, CRITERIA_FILENAME ) )
    {
        p_triplet->crit = CRITERIA_FILENAME;
        *p_attr_mask |= ATTR_MASK_name;

        /* non-empty string expected with no slash */
        if ( EMPTY_STRING( key_value->varvalue ) || SLASH_IN( key_value->varvalue ) )
        {
            strcpy( err_msg,
                    "A name with no slash is expected in 'name' criteria (wildcards allowed)" );
            return EINVAL;
        }

        /* special wildcard '**' not expected here */
        if ( ANY_LEVEL_MATCH(key_value->varvalue) )
        {
            strcpy( err_msg,
                    "Special wildcard '**' not expected in 'name' regular expression" );
            return EINVAL;
        }

        rh_strncpy(p_triplet->val.str, key_value->varvalue, RBH_PATH_MAX);

        /* allowed comparators are = and != */
        p_triplet->op = syntax2conf_comparator( key_value->op_type );

        if ( ( p_triplet->op != COMP_EQUAL ) && ( p_triplet->op != COMP_DIFF ) )
        {
            strcpy( err_msg, "Illegal comparator for 'tree' criteria: == or != expected" );
            return EINVAL;
        }

        /* in case the string contains regexpr, those comparators are changed to LIKE / UNLIKE */
        if ( WILDCARDS_IN( p_triplet->val.str ) )
        {
            if ( p_triplet->op == COMP_EQUAL )
                p_triplet->op = COMP_LIKE;
            else if ( p_triplet->op == COMP_DIFF )
                p_triplet->op = COMP_UNLIKE;
        }

    }
#ifdef ATTR_INDEX_type
    else if ( TEST_CRIT( key_value->varname, CRITERIA_TYPE ) )
    {
        p_triplet->crit = CRITERIA_TYPE;
        *p_attr_mask |= ATTR_MASK_type;

        /* value expected : file, symlink, directory */
        if ( !strcasecmp( key_value->varvalue, "file" ) )
            p_triplet->val.type = TYPE_FILE;
        else if ( !strcasecmp( key_value->varvalue, "directory" )
                  || !strcasecmp( key_value->varvalue, "dir" ) )
            p_triplet->val.type = TYPE_DIR;
        else if ( !strcasecmp( key_value->varvalue, "symlink" ) )
            p_triplet->val.type = TYPE_LINK;
        else if ( !strcasecmp( key_value->varvalue, "chr" ) )
            p_triplet->val.type = TYPE_CHR;
        else if ( !strcasecmp( key_value->varvalue, "blk" ) )
            p_triplet->val.type = TYPE_BLK;
        else if ( !strcasecmp( key_value->varvalue, "fifo" ) )
            p_triplet->val.type = TYPE_FIFO;
        else if ( !strcasecmp( key_value->varvalue, "sock" ) )
            p_triplet->val.type = TYPE_SOCK;
        else
        {
            strcpy( err_msg,
                    "Illegal condition on type: file, directory, symlink, chr, blk, fifo or sock expected" );
            return EINVAL;
        }

        /* allowed comparators are = and != */
        p_triplet->op = syntax2conf_comparator( key_value->op_type );

        if ( ( p_triplet->op != COMP_EQUAL ) && ( p_triplet->op != COMP_DIFF ) )
        {
            strcpy( err_msg, "Illegal comparator for 'type' criteria: == or != expected" );
            return EINVAL;
        }

    }
#endif
    else if ( TEST_CRIT( key_value->varname, CRITERIA_OWNER ) )
    {
        /* user name, possibly with wildcards */
        p_triplet->crit = CRITERIA_OWNER;
        *p_attr_mask |= ATTR_MASK_owner;

        rh_strncpy(p_triplet->val.str, key_value->varvalue, RBH_PATH_MAX);
        p_triplet->op = syntax2conf_comparator( key_value->op_type );

        if ( ( p_triplet->op != COMP_EQUAL ) && ( p_triplet->op != COMP_DIFF ) )
        {
            strcpy( err_msg, "Illegal comparator for 'owner' criteria: == or != expected" );
            return EINVAL;
        }

        /* in case the string contains regexpr, those comparators are changed to LIKE / UNLIKE */
        if ( WILDCARDS_IN( p_triplet->val.str ) )
        {
            if ( p_triplet->op == COMP_EQUAL )
                p_triplet->op = COMP_LIKE;
            else if ( p_triplet->op == COMP_DIFF )
                p_triplet->op = COMP_UNLIKE;
        }


    }
    else if ( TEST_CRIT( key_value->varname, CRITERIA_GROUP ) )
    {
        /* same thing for group */
        p_triplet->crit = CRITERIA_GROUP;
        *p_attr_mask |= ATTR_MASK_gr_name;

        rh_strncpy(p_triplet->val.str, key_value->varvalue, RBH_PATH_MAX);
        p_triplet->op = syntax2conf_comparator( key_value->op_type );

        if ( ( p_triplet->op != COMP_EQUAL ) && ( p_triplet->op != COMP_DIFF ) )
        {
            strcpy( err_msg, "Illegal comparator for 'group' criteria: == or != expected" );
            return EINVAL;
        }

        /* in case the string containts regexpr, those comparators are changed to LIKE / UNLIKE */
        if ( WILDCARDS_IN( p_triplet->val.str ) )
        {
            if ( p_triplet->op == COMP_EQUAL )
                p_triplet->op = COMP_LIKE;
            else if ( p_triplet->op == COMP_DIFF )
                p_triplet->op = COMP_UNLIKE;
        }

    }
    else if ( TEST_CRIT( key_value->varname, CRITERIA_SIZE ) )
    {
        p_triplet->crit = CRITERIA_SIZE;
        *p_attr_mask |= ATTR_MASK_size;

        /* a size is expected */
        p_triplet->val.size = str2size( key_value->varvalue );

        if ( p_triplet->val.size == ( unsigned long long ) -1 )
        {
            sprintf( err_msg, "Invalid format for size: '%s'", key_value->varvalue );
            return EINVAL;
        }

        /* any comparator is allowed */
        p_triplet->op = syntax2conf_comparator( key_value->op_type );

    }
    else if ( TEST_CRIT( key_value->varname, CRITERIA_DEPTH ) )
    {
        p_triplet->crit = CRITERIA_DEPTH;
        *p_attr_mask |= ATTR_MASK_depth;

        /* a size is expected */
        p_triplet->val.integer = str2int( key_value->varvalue );

        if ( p_triplet->val.integer == -1 )
        {
            sprintf( err_msg, "Invalid format for depth: '%s'", key_value->varvalue );
            return EINVAL;
        }

        /* any comparator is allowed */
        p_triplet->op = syntax2conf_comparator( key_value->op_type );

    }
#ifdef ATTR_INDEX_dircount
    else if ( TEST_CRIT( key_value->varname, CRITERIA_DIRCOUNT ) )
    {
        p_triplet->crit = CRITERIA_DIRCOUNT;
        *p_attr_mask |= ATTR_MASK_dircount;

        /* a size is expected */
        p_triplet->val.integer = str2int( key_value->varvalue );

        if ( p_triplet->val.integer == -1 )
        {
            sprintf( err_msg, "Invalid format for dircount: '%s'", key_value->varvalue );
            return EINVAL;
        }

        /* any comparator is allowed */
        p_triplet->op = syntax2conf_comparator( key_value->op_type );

    }
#endif
    else if ( TEST_CRIT( key_value->varname, CRITERIA_LAST_ACCESS ) )
    {
        p_triplet->crit = CRITERIA_LAST_ACCESS;
        *p_attr_mask |= ATTR_MASK_last_access;

        /* a duration is expected */
        p_triplet->val.duration = str2duration( key_value->varvalue );

        if ( p_triplet->val.duration == -1 )
        {
            sprintf( err_msg, "Invalid format for duration in 'last_access' criteria: '%s'",
                     key_value->varvalue );
            return EINVAL;
        }

        /* any comparator is allowed */
        p_triplet->op = syntax2conf_comparator( key_value->op_type );

    }
    else if ( TEST_CRIT( key_value->varname, CRITERIA_LAST_MOD ) )
    {
        p_triplet->crit = CRITERIA_LAST_MOD;
        *p_attr_mask |= ATTR_MASK_last_mod;

        /* a duration is expected */
        p_triplet->val.duration = str2duration( key_value->varvalue );

        if ( p_triplet->val.duration == -1 )
        {
            sprintf( err_msg, "Invalid format for duration in 'last_mod' criteria: '%s'",
                     key_value->varvalue );
            return EINVAL;
        }

        /* any comparator is allowed */
        p_triplet->op = syntax2conf_comparator( key_value->op_type );


    }
#ifdef ATTR_INDEX_last_archive
    else if ( TEST_CRIT( key_value->varname, CRITERIA_LAST_ARCHIVE ) )
    {
        p_triplet->crit = CRITERIA_LAST_ARCHIVE;
        *p_attr_mask |= ATTR_MASK_last_archive;

        /* a duration is expected */
        p_triplet->val.duration = str2duration( key_value->varvalue );

        if ( p_triplet->val.duration == -1 )
        {
            sprintf( err_msg, "Invalid format for duration in 'last_archive' criteria: '%s'",
                     key_value->varvalue );
            return EINVAL;
        }

        /* any comparator is allowed */
        p_triplet->op = syntax2conf_comparator( key_value->op_type );

    }
#endif
#ifdef ATTR_INDEX_last_restore
    else if ( TEST_CRIT( key_value->varname, CRITERIA_LAST_RESTORE ) )
    {
        p_triplet->crit = CRITERIA_LAST_RESTORE;
        *p_attr_mask |= ATTR_MASK_last_restore;

        /* a duration is expected */
        p_triplet->val.duration = str2duration( key_value->varvalue );

        if ( p_triplet->val.duration == -1 )
        {
            sprintf( err_msg, "Invalid format for duration in 'last_restore' criteria: '%s'",
                     key_value->varvalue );
            return EINVAL;
        }

        /* any comparator is allowed */
        p_triplet->op = syntax2conf_comparator( key_value->op_type );

    }
#endif
#ifdef ATTR_INDEX_creation_time
    else if ( TEST_CRIT( key_value->varname, CRITERIA_CREATION ) )
    {
        p_triplet->crit = CRITERIA_CREATION;
        *p_attr_mask |= ATTR_MASK_creation_time;

        /* a duration is expected */
        p_triplet->val.duration = str2duration( key_value->varvalue );

        if ( p_triplet->val.duration == -1 )
        {
            sprintf( err_msg, "Invalid format for duration in 'creation' criteria: '%s'",
                     key_value->varvalue );
            return EINVAL;
        }

        /* any comparator is allowed */
        p_triplet->op = syntax2conf_comparator( key_value->op_type );

    }
#endif
    else if ( TEST_CRIT( key_value->varname, CRITERIA_POOL ) )
    {
        /* same thing for group */
        p_triplet->crit = CRITERIA_POOL;
        *p_attr_mask |= ATTR_MASK_stripe_info;

        rh_strncpy(p_triplet->val.str, key_value->varvalue, RBH_PATH_MAX);
        p_triplet->op = syntax2conf_comparator( key_value->op_type );

        if ( ( p_triplet->op != COMP_EQUAL ) && ( p_triplet->op != COMP_DIFF ) )
        {
            strcpy( err_msg, "Illegal comparator for 'ost_pool' criteria: == or != expected" );
            return EINVAL;
        }

        /* in case the string contains regexpr, those comparators are changed to LIKE / UNLIKE */
        if ( WILDCARDS_IN( p_triplet->val.str ) )
        {
            if ( p_triplet->op == COMP_EQUAL )
                p_triplet->op = COMP_LIKE;
            else if ( p_triplet->op == COMP_DIFF )
                p_triplet->op = COMP_UNLIKE;
        }
    }
    else if ( TEST_CRIT( key_value->varname, CRITERIA_OST ) )
    {
        p_triplet->crit = CRITERIA_OST;
        *p_attr_mask |= ATTR_MASK_stripe_items;

        /* a index is expected */
        p_triplet->val.integer = str2int( key_value->varvalue );

        if ( p_triplet->val.integer == -1 )
        {
            sprintf( err_msg, "Invalid format for ost index: '%s'", key_value->varvalue );
            return EINVAL;
        }

        /* any comparator is allowed */
        p_triplet->op = syntax2conf_comparator( key_value->op_type );

        if ( ( p_triplet->op != COMP_EQUAL ) && ( p_triplet->op != COMP_DIFF ) )
        {
            strcpy( err_msg, "Illegal comparator for 'ost_index' criteria: == or != expected" );
            return EINVAL;
        }
    }
    else if ( TEST_CRIT( key_value->varname, CRITERIA_CUSTOM_CMD ) )
    {
        p_triplet->crit = CRITERIA_CUSTOM_CMD;
        *p_attr_mask |= ATTR_MASK_fullpath;

        /* non empty string expected */
        if ( EMPTY_STRING( key_value->varvalue ) )
        {
            strcpy( err_msg, "Empty external_command" );
            return EINVAL;
        }

        /* no comparator is allowed */
        if ( key_value->op_type != OP_CMD )
        {
            sprintf( err_msg, "Unexpected operator %d for external_command", key_value->op_type );
            return EINVAL;
        }
    }
    else if ( !strncasecmp( key_value->varname, XATTR_PREFIX".", strlen(XATTR_PREFIX".") ) )
    {
        char * p_xattr = strchr( key_value->varname, '.' );
        p_xattr ++;

        p_triplet->crit = CRITERIA_XATTR;

#if (!defined (_LUSTRE) || !defined(_HAVE_FID))
         /* fullpath needed to get xattr, except if fids are supported */
        *p_attr_mask |= ATTR_MASK_fullpath;
#endif

        rh_strncpy(p_triplet->val.str, key_value->varvalue, RBH_PATH_MAX);
        rh_strncpy(p_triplet->xattr_name, p_xattr, RBH_NAME_MAX);
        p_triplet->op = syntax2conf_comparator( key_value->op_type );

        if ( ( p_triplet->op != COMP_EQUAL ) && ( p_triplet->op != COMP_DIFF ) )
        {
            sprintf( err_msg, "Illegal comparator for 'xattr.%s' criteria: == or != expected",
                     p_triplet->xattr_name  );
            return EINVAL;
        }
    }
    else
    {
        sprintf( err_msg, "Unknown or unsupported criteria on '%s'", key_value->varname );
        return EINVAL;
    }

    return 0;
} /* interpret_condition */


/**
 *  Recursive function for building boolean expression.
 */
static int build_bool_expr( type_bool_expr * p_in_bool_expr, bool_node_t * p_out_node,
                            int *p_attr_mask, char *err_msg )
{
    int            rc;

    switch ( p_in_bool_expr->type )
    {
    case BOOL_CONDITION:
        p_out_node->node_type = NODE_CONDITION;
        p_out_node->content_u.condition =
            ( compare_triplet_t * ) malloc( sizeof( compare_triplet_t ) );
        if ( !p_out_node->content_u.condition )
            goto errmem;

        rc = interpret_condition( &p_in_bool_expr->expr_u.key_value,
                                  p_out_node->content_u.condition, p_attr_mask, err_msg );
        if ( rc )
            goto freecondition;
        return 0;

        break;

    case BOOL_UNARY:

        /* in case of identity, directly return sub expression */
        if ( p_in_bool_expr->oper == BOOL_OP_IDENTITY )
            return build_bool_expr( p_in_bool_expr->expr_u.members.expr1, p_out_node, p_attr_mask,
                                    err_msg );

        p_out_node->node_type = NODE_UNARY_EXPR;
        p_out_node->content_u.bool_expr.bool_op = syntax2conf_boolop( p_in_bool_expr->oper );
        if ( p_out_node->content_u.bool_expr.bool_op == BOOL_ERR )
        {
            strcpy( err_msg, "Unexpected boolean operator in expression" );
            return EINVAL;
        }

        p_out_node->content_u.bool_expr.owner = 1;
        p_out_node->content_u.bool_expr.expr1 = ( bool_node_t * ) malloc( sizeof( bool_node_t ) );
        if ( !p_out_node->content_u.bool_expr.expr1 )
            goto errmem;
        p_out_node->content_u.bool_expr.expr2 = NULL;

        rc = build_bool_expr( p_in_bool_expr->expr_u.members.expr1,
                              p_out_node->content_u.bool_expr.expr1, p_attr_mask, err_msg );
        if ( rc )
            goto free_expr1;
        return 0;

        break;

    case BOOL_BINARY:

        p_out_node->node_type = NODE_BINARY_EXPR;
        p_out_node->content_u.bool_expr.bool_op = syntax2conf_boolop( p_in_bool_expr->oper );

        if ( p_out_node->content_u.bool_expr.bool_op == BOOL_ERR )
        {
            strcpy( err_msg, "Unexpected boolean operator in expression" );
            return EINVAL;
        }

        p_out_node->content_u.bool_expr.owner = 1;
        p_out_node->content_u.bool_expr.expr1 = ( bool_node_t * ) malloc( sizeof( bool_node_t ) );
        if ( !p_out_node->content_u.bool_expr.expr1 )
            goto errmem;
        rc = build_bool_expr( p_in_bool_expr->expr_u.members.expr1,
                              p_out_node->content_u.bool_expr.expr1, p_attr_mask, err_msg );

        if ( rc )
            goto free_expr1;

        p_out_node->content_u.bool_expr.expr2 = ( bool_node_t * ) malloc( sizeof( bool_node_t ) );
        if ( !p_out_node->content_u.bool_expr.expr2 )
            goto errmem;
        rc = build_bool_expr( p_in_bool_expr->expr_u.members.expr2,
                              p_out_node->content_u.bool_expr.expr2, p_attr_mask, err_msg );

        if ( rc )
            goto free_expr2;

        return 0;


        break;

    default:
        sprintf( err_msg, "Invalid boolean node type %d while parsing", p_in_bool_expr->type );
        return EINVAL;
    }

  errmem:
    sprintf( err_msg, "Could not allocate memory" );
    return ENOMEM;

  freecondition:
    free( p_out_node->content_u.condition );
    return rc;

  free_expr2:
    free( p_out_node->content_u.bool_expr.expr2 );
  free_expr1:
    free( p_out_node->content_u.bool_expr.expr1 );
    return rc;


}


/** Create a boolean condition */
int CreateBoolCond(bool_node_t * p_out_node, compare_direction_t compar,
                   compare_criteria_t  crit, compare_value_t val)
{
    p_out_node->node_type = NODE_CONDITION;
    p_out_node->content_u.condition = (compare_triplet_t*)malloc(sizeof(compare_triplet_t));
    if (!p_out_node->content_u.condition)
        return -ENOMEM;
    memset(p_out_node->content_u.condition, 0, sizeof(compare_triplet_t));
    p_out_node->content_u.condition->flags = 0;
    p_out_node->content_u.condition->crit = crit;
    p_out_node->content_u.condition->op = compar;
    p_out_node->content_u.condition->val = val;
    return 0;
}

/** Append a boolean condition with bool op = AND */
int AppendBoolCond(bool_node_t * p_in_out_node, compare_direction_t compar,
                   compare_criteria_t  crit, compare_value_t val)
{
    bool_node_t copy_prev = *p_in_out_node;
    int rc = 0;

    p_in_out_node->node_type = NODE_BINARY_EXPR;
    p_in_out_node->content_u.bool_expr.bool_op = BOOL_AND;

    /* bool expr will be allocated */
    p_in_out_node->content_u.bool_expr.owner = 1;

    /* first expression = the previous expression */
    p_in_out_node->content_u.bool_expr.expr1 = (bool_node_t *)malloc(sizeof(bool_node_t));
    if (!p_in_out_node->content_u.bool_expr.expr1)
        return -ENOMEM;
    *p_in_out_node->content_u.bool_expr.expr1 = copy_prev;

    /* second expression = the appended value */
    p_in_out_node->content_u.bool_expr.expr2 = (bool_node_t *)malloc(sizeof(bool_node_t));
    if (!p_in_out_node->content_u.bool_expr.expr2)
    {
        rc = -ENOMEM;
        goto free_expr1;
    }

    /* expr2 is a triplet */
    rc = CreateBoolCond(p_in_out_node->content_u.bool_expr.expr2, compar,
                        crit, val);
    if (rc)
        goto free_expr2;

    return 0;

free_expr2:
    free(p_in_out_node->content_u.bool_expr.expr2);
free_expr1:
    FreeBoolExpr(p_in_out_node->content_u.bool_expr.expr1, TRUE);
    return rc;
}


/**
 * Build a policy boolean expression from the given block
 */
int GetBoolExpr( config_item_t block, const char *block_name,
                 bool_node_t * p_bool_node, int *p_attr_mask, char *err_msg )
{
    generic_item  *curr_block = ( generic_item * ) block;
    generic_item  *subitem;
    int            rc;

    /* initialize attr mask */
    *p_attr_mask = 0;

    /* check it is a block */
    if ( !curr_block || ( curr_block->type != TYPE_BLOCK ) )
    {
        sprintf( err_msg, "'%s' is expected to be a block", block_name );
        return EINVAL;
    }

    /* Check the block contains something  */
    if ( !curr_block->item.block.block_content )
    {
        sprintf( err_msg, "'%s' block is empty, line %d", block_name, rh_config_GetItemLine( block ) );
        return ENOENT;
    }

    /* check bloc content */
    subitem = curr_block->item.block.block_content;

    if ( subitem->type != TYPE_BOOL_EXPR )
    {
        sprintf( err_msg, "Boolean expression expected in block '%s', line %d", block_name,
                 rh_config_GetItemLine( ( config_item_t ) subitem ) );
        return EINVAL;
    }

    if ( subitem->next )
    {
        sprintf( err_msg, "A single boolean expression is expected in block '%s', line %d",
                 block_name, rh_config_GetItemLine( ( config_item_t ) subitem ) );
        return EINVAL;
    }

    /* now we can analyze the boolean expression */
    rc = build_bool_expr( &subitem->item.bool_expr, p_bool_node, p_attr_mask, err_msg );
    if ( rc )
        sprintf( err_msg + strlen( err_msg ), ", line %d",
                 rh_config_GetItemLine( ( config_item_t ) subitem ) );

    return rc;

}

/**
 *  Recursive function for freeing boolean expression.
 *  TODO: check these functions, in particular the 'owner'
 *        system, when an expression is a sub-part of another.
 */
int FreeBoolExpr( bool_node_t * p_expr, int free_top_node )
{
    if ( p_expr == NULL )
        return -EFAULT;

    switch ( p_expr->node_type )
    {
    case NODE_CONDITION:
        free( p_expr->content_u.condition );
        break;

    case NODE_UNARY_EXPR:
        if ( p_expr->content_u.bool_expr.owner )
            FreeBoolExpr( p_expr->content_u.bool_expr.expr1, TRUE );
        break;

    case NODE_BINARY_EXPR:
        if ( p_expr->content_u.bool_expr.owner )
        {
            FreeBoolExpr( p_expr->content_u.bool_expr.expr1, TRUE );
            FreeBoolExpr( p_expr->content_u.bool_expr.expr2, TRUE );
        }
        break;
    }

    if ( free_top_node )
        free( p_expr );

    return 0;
}

/**
 *  Recursive function for building boolean expression, from a union/intersection
 *  of defined classes.
 */
static int build_set_expr( type_set * p_in_set,
                           bool_node_t * p_out_node, int *p_attr_mask,
                           const fileset_list_t * list, char *err_msg )
{
    int i, rc;

    if ( p_in_set->set_type == SET_SINGLETON )
    {
       /* get class from its name */
       for ( i = 0; i < list->fileset_count; i++ )
       {
            if ( !strcasecmp( list->fileset_list[i].fileset_id,
                              p_in_set->set_u.name ) )
            {
                /* found */
                *p_out_node = list->fileset_list[i].definition;
                (*p_attr_mask) |= list->fileset_list[i].attr_mask;
                /* top level expression is not owner of the content */
                p_out_node->content_u.bool_expr.owner = 0;
                return 0;
            }
       }
       sprintf( err_msg, "FileClass '%s' is undefined", p_in_set->set_u.name );
       return ENOENT;
    }
    else if ( p_in_set->set_type == SET_NEGATION )
    {
        p_out_node->node_type = NODE_UNARY_EXPR;

        if ( p_in_set->set_u.op.oper != SET_OP_NOT )
        {
            strcpy( err_msg, "Unexpected set operator in unary expression" );
            return EINVAL;
        }
        p_out_node->content_u.bool_expr.bool_op = BOOL_NOT;

        p_out_node->content_u.bool_expr.owner = 1;
        p_out_node->content_u.bool_expr.expr1
                = ( bool_node_t * ) malloc( sizeof( bool_node_t ) );
        if ( !p_out_node->content_u.bool_expr.expr1 )
            goto errmem;

        p_out_node->content_u.bool_expr.expr2 = NULL;

        rc = build_set_expr( p_in_set->set_u.op.set1,
                             p_out_node->content_u.bool_expr.expr1,
                             p_attr_mask, list, err_msg );
        if ( rc )
            goto free_set1;
    }
    else /* not a singleton: Union or Inter or Negation */
    {
        p_out_node->node_type = NODE_BINARY_EXPR;

        if ( p_in_set->set_u.op.oper == SET_OP_UNION )
            /* entry matches one class OR the other */
            p_out_node->content_u.bool_expr.bool_op = BOOL_OR;
        else if ( p_in_set->set_u.op.oper == SET_OP_INTER )
            /* entry matches one class AND the other */
            p_out_node->content_u.bool_expr.bool_op = BOOL_AND;
        else
        {
            strcpy( err_msg, "Unexpected set operator in expression" );
            return EINVAL;
        }

        p_out_node->content_u.bool_expr.owner = 1;
        p_out_node->content_u.bool_expr.expr1
                = ( bool_node_t * ) malloc( sizeof( bool_node_t ) );
        if ( !p_out_node->content_u.bool_expr.expr1 )
            goto errmem;
        rc = build_set_expr( p_in_set->set_u.op.set1,
                             p_out_node->content_u.bool_expr.expr1,
                             p_attr_mask, list, err_msg );

        if ( rc )
            goto free_set1;

        p_out_node->content_u.bool_expr.expr2
                = ( bool_node_t * ) malloc( sizeof( bool_node_t ) );
        if ( !p_out_node->content_u.bool_expr.expr2 )
            goto errmem;
        rc = build_set_expr( p_in_set->set_u.op.set2,
                             p_out_node->content_u.bool_expr.expr2,
                             p_attr_mask, list, err_msg );
        if ( rc )
            goto free_set2;
    }

    return 0;

errmem:
    sprintf( err_msg, "Could not allocate memory" );
    return ENOMEM;

free_set2:
    free( p_out_node->content_u.bool_expr.expr2 );
free_set1:
    free( p_out_node->content_u.bool_expr.expr1 );
return rc;

}


/**
 * Build a policy boolean expression from a union/intersection of fileclasses
 */
int GetSetExpr( config_item_t block, const char *block_name,
                bool_node_t * p_bool_node, int *p_attr_mask,
                const fileset_list_t * list, char *err_msg )
{
    generic_item  *curr_block = ( generic_item * ) block;
    generic_item  *subitem;
    int            rc;

    /* initialize attr mask */
    *p_attr_mask = 0;

    /* check it is a block */
    if ( !curr_block || ( curr_block->type != TYPE_BLOCK ) )
    {
        sprintf( err_msg, "'%s' is expected to be a block", block_name );
        return EINVAL;
    }

    /* Check the block contains something  */
    if ( !curr_block->item.block.block_content )
    {
        sprintf( err_msg, "'%s' block is empty, line %d", block_name,
                 rh_config_GetItemLine( block ) );
        return ENOENT;
    }

    /* check bloc content */
    subitem = curr_block->item.block.block_content;

    if ( subitem->type != TYPE_SET )
    {
        sprintf( err_msg, "Union/intersection/negation of classes expected in block '%s', line %d",
                 block_name, rh_config_GetItemLine( ( config_item_t ) subitem ) );
        return EINVAL;
    }

    if ( subitem->next )
    {
        sprintf( err_msg, "A single expression is expected in block '%s', line %d",
                 block_name, rh_config_GetItemLine( ( config_item_t ) subitem ) );
        return EINVAL;
    }

    /* now we can analyze the union/intersection */
    rc = build_set_expr( &subitem->item.set, p_bool_node, p_attr_mask,
                         list, err_msg );
    if ( rc )
        sprintf( err_msg + strlen( err_msg ), ", line %d",
                 rh_config_GetItemLine( ( config_item_t ) subitem ) );

    return rc;

}


const char    *op2str( compare_direction_t comp )
{
    switch ( comp )
    {
    case COMP_GRTHAN:
        return ">";
    case COMP_GRTHAN_EQ:
        return ">=";
    case COMP_LSTHAN:
        return "<";
    case COMP_LSTHAN_EQ:
        return "<=";
    case COMP_EQUAL:
        return "==";
    case COMP_DIFF:
        return "<>";
    case COMP_LIKE:
        return " =~ ";
    case COMP_UNLIKE:
        return " !~ ";
    default:
        return "?";
    }
}                               /* op2str */

const char    *type2str( obj_type_t type )
{
    switch ( type )
    {
    case TYPE_LINK:
        return "symlink";
    case TYPE_DIR:
        return "directory";
    case TYPE_FILE:
        return "file";
    case TYPE_CHR:
        return "chr";
    case TYPE_BLK:
        return "blk";
    case TYPE_FIFO:
        return "fifo";
    case TYPE_SOCK:
        return "sock";
    default:
        return "?";
    }
}

const char    *criteria2str( compare_criteria_t crit )
{
    switch ( crit )
    {
    case CRITERIA_TREE:
        return "tree";
    case CRITERIA_PATH:
        return "path";
    case CRITERIA_FILENAME:
        return "name";
#ifdef ATTR_INDEX_type
    case CRITERIA_TYPE:
        return "type";
#endif
    case CRITERIA_OWNER:
        return "owner";
    case CRITERIA_GROUP:
        return "group";
    case CRITERIA_SIZE:
        return "size";
    case CRITERIA_DEPTH:
        return "depth";
#ifdef ATTR_INDEX_dircount
    case CRITERIA_DIRCOUNT:
        return "dircount";
#endif
    case CRITERIA_LAST_ACCESS:
        return "last_access";
    case CRITERIA_LAST_MOD:
        return "last_mod";
#ifdef ATTR_INDEX_last_archive
    case CRITERIA_LAST_ARCHIVE:
        return "last_archive";
#endif
#ifdef ATTR_INDEX_last_restore
    case CRITERIA_LAST_RESTORE:
        return "last_restore";
#endif
#ifdef ATTR_INDEX_creation_time
    case CRITERIA_CREATION:
        return "creation";
#endif
    case CRITERIA_POOL:
        return "ost_pool";
    case CRITERIA_OST:
        return "ost_index";
    case CRITERIA_CUSTOM_CMD:
        return "external_cmd";
    case CRITERIA_XATTR:
        return XATTR_PREFIX;
    default:
        return "?";
    }
}

static int print_condition( const compare_triplet_t * p_triplet, char *out_str, size_t str_size )
{
    char           tmp_buff[256];

    switch ( p_triplet->crit )
    {
        /* str values */
    case CRITERIA_TREE:
    case CRITERIA_PATH:
    case CRITERIA_FILENAME:
    case CRITERIA_OWNER:
    case CRITERIA_GROUP:
    case CRITERIA_POOL:
        return snprintf( out_str, str_size, "%s %s \"%s\"", criteria2str( p_triplet->crit ),
                         op2str( p_triplet->op ), p_triplet->val.str );

#ifdef ATTR_INDEX_type
    case CRITERIA_TYPE:
        return snprintf( out_str, str_size, "%s %s \"%s\"", criteria2str( p_triplet->crit ),
                         op2str( p_triplet->op ), type2str( p_triplet->val.type ) );
#endif

        /* int values */
    case CRITERIA_DEPTH:
    case CRITERIA_OST:
#ifdef ATTR_INDEX_dircount
    case CRITERIA_DIRCOUNT:
#endif
        return snprintf( out_str, str_size, "%s %s %d", criteria2str( p_triplet->crit ),
                         op2str( p_triplet->op ), p_triplet->val.integer );

    case CRITERIA_SIZE:
        FormatFileSize( tmp_buff, 256, p_triplet->val.size );
        return snprintf( out_str, str_size, "%s %s %s", criteria2str( p_triplet->crit ),
                         op2str( p_triplet->op ), tmp_buff );

        /* duration values */

    case CRITERIA_LAST_ACCESS:
    case CRITERIA_LAST_MOD:
#ifdef ATTR_INDEX_last_archive
    case CRITERIA_LAST_ARCHIVE:
#endif
#ifdef ATTR_INDEX_last_restore
    case CRITERIA_LAST_RESTORE:
#endif
#ifdef ATTR_INDEX_creation_time
    case CRITERIA_CREATION:
#endif
        FormatDurationFloat( tmp_buff, 256, p_triplet->val.duration );
        return snprintf( out_str, str_size, "%s %s %s", criteria2str( p_triplet->crit ),
                         op2str( p_triplet->op ), tmp_buff );

    case CRITERIA_CUSTOM_CMD:
        return snprintf( out_str, str_size, "external_cmd(%s)", p_triplet->val.str );

    case CRITERIA_XATTR:
        return snprintf( out_str, str_size, XATTR_PREFIX".%s %s %s",
                         p_triplet->xattr_name, op2str( p_triplet->op ),
                         p_triplet->val.str );
    default:
        return -EINVAL;
    }
}

/**
 * Print a boolean expression to a string.
 * @return a negative value on error
 *         else, the number of chars written.
 */
int BoolExpr2str( bool_node_t * p_bool_node, char *out_str, size_t str_size )
{
    size_t         written = 0;
    int            rc;

    switch ( p_bool_node->node_type )
    {
    case NODE_UNARY_EXPR:

        /* only BOOL_NOT is supported as unary operator */
        if ( p_bool_node->content_u.bool_expr.bool_op != BOOL_NOT )
            return -EINVAL;
        written = snprintf( out_str, str_size, "NOT (" );
        rc = BoolExpr2str( p_bool_node->content_u.bool_expr.expr1, out_str + written,
                           str_size - written );
        if ( rc < 0 )
            return rc;
        written += rc;
        written += snprintf( out_str + written, str_size - written, ")" );
        return written;

    case NODE_BINARY_EXPR:
        written = snprintf( out_str, str_size, "(" );
        rc = BoolExpr2str( p_bool_node->content_u.bool_expr.expr1, out_str + written,
                           str_size - written );
        if ( rc < 0 )
            return rc;
        written += rc;
        if ( p_bool_node->content_u.bool_expr.bool_op == BOOL_OR )
            written += snprintf( out_str + written, str_size - written, ") OR (" );
        else
            written += snprintf( out_str + written, str_size - written, ") AND (" );

        rc = BoolExpr2str( p_bool_node->content_u.bool_expr.expr2, out_str + written,
                           str_size - written );
        if ( rc < 0 )
            return rc;
        written += rc;

        written += snprintf( out_str + written, str_size - written, ")" );
        return written;

    case NODE_CONDITION:
        return print_condition( p_bool_node->content_u.condition, out_str, str_size );
    }

    return -EINVAL;
}


/**
 * Check that no unknown parameter or block is found.
 * @param param_array NULL terminated array of allowed parameters.
 */
void CheckUnknownParameters( config_item_t block, const char *block_name, const char **param_array )
{
    int            i, j;

    for ( i = 0; i < rh_config_GetNbItems( block ); i++ )
    {
        config_item_t  curr_item = rh_config_GetItemByIndex( block, i );

        if ( rh_config_ItemType( curr_item ) == CONFIG_ITEM_VAR )
        {
            char          *name;
            char          *value;
            int            args_flg;
            int            found = FALSE;

            if ( rh_config_GetKeyValue( curr_item, &name, &value, &args_flg ) == 0 )
            {
                for ( j = 0; param_array[j] != NULL; j++ )
                {
                    if ( !strcasecmp( param_array[j], name ) )
                    {
                        found = TRUE;
                        break;
                    }
                }

                if ( !found )
                    DisplayLog( LVL_CRIT, "Config Check",
                                "WARNING: unknown parameter '%s' in block '%s' line %d", name,
                                block_name, rh_config_GetItemLine( curr_item ) );
            }
        }
        else if ( rh_config_ItemType( curr_item ) == CONFIG_ITEM_BLOCK )
        {
            char          *name;
            int            found = FALSE;

            name = rh_config_GetBlockName( curr_item );

            if ( name != NULL )
            {
                for ( j = 0; param_array[j] != NULL; j++ )
                {
                    if ( !strcasecmp( param_array[j], name ) )
                    {
                        found = TRUE;
                        break;
                    }
                }

                if ( !found )
                    DisplayLog( LVL_CRIT, "Config Check",
                                "WARNING: unknown block '%s' as sub-block of '%s' line %d", name,
                                block_name, rh_config_GetItemLine( curr_item ) );
            }

        }

    }
}
