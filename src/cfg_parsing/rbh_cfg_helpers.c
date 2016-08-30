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

#include "rbh_cfg_helpers.h"
#include "rbh_misc.h"
#include "rbh_logs.h"

static inline bool is_stdname(const char *name)
{
    return (!strcasecmp(name, "stdout")
           || !strcasecmp(name, "stderr")
           || !strcasecmp(name, "syslog"));
}

/** get param with the given name and check for existence and unicity */
static int get_cfg_param(config_item_t block, const char *block_name,
                         const char *var_name, param_flags_t flags,
                         char **pname, char **pvalue, int *pextra,
                         config_item_t *pitem, char *err_msg)
{
    bool          unique = true;
    int           rc;

    *pitem = rh_config_GetItemByName(block, var_name, &unique);
    if (!*pitem)
    {
        if (flags & PFLG_MANDATORY)
            sprintf(err_msg, "Missing mandatory parameter '%s' in block '%s', line %d", var_name,
                    block_name, rh_config_GetItemLine(block));
        /* return ENOENT in any case */
        return ENOENT;
    }
    else if (!unique)
    {
        sprintf(err_msg, "Duplicate definition of parameter '%s' found in block '%s', line %d.",
                var_name, block_name, rh_config_GetItemLine(*pitem));
        return EEXIST;
    }

    rc = rh_config_GetKeyValue(*pitem, pname, pvalue, pextra);
    if (rc)
        sprintf(err_msg, "Error retrieving parameter value for '%s::%s', line %d:\n%s",
                block_name, var_name, rh_config_GetItemLine(*pitem),
                rh_config_GetErrorMsg());
    return rc;
}

/**
 * Misc. tools for config parsing
 */
int            GetStringParam(config_item_t block, const char *block_name,
                              const char *var_name, param_flags_t flags, char *target,
                              unsigned int target_size, char ***extra_args_tab,
                              unsigned int *nb_extra_args, char *err_msg)
{
    config_item_t  curr_item;
    int            rc;
    int            extra = 0;
    char          *name;
    char          *value;
    gsize          sz;

    err_msg[0] = '\0';

    if ( nb_extra_args )
        *nb_extra_args = 0;
    if ( extra_args_tab )
        *extra_args_tab = NULL;

    rc = get_cfg_param(block, block_name, var_name, flags, &name, &value,
                       &extra, &curr_item, err_msg);
    if (rc)
        return rc;

    sz = g_strlcpy(target, value, target_size);
    if (sz >= target_size) {
        sprintf(err_msg, "Option too long for parameter '%s::%s', line %d", block_name,
                var_name, rh_config_GetItemLine(curr_item));
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

    /* checks */

    /* empty string? */
    if ((flags & PFLG_NOT_EMPTY) && EMPTY_STRING(target))
    {
        sprintf(err_msg, "Unexpected empty parameter '%s::%s', line %d",
                block_name, var_name, rh_config_GetItemLine(curr_item));
        return EINVAL;
    }

    /* are stdio names allowed ? */
    if ((flags & PFLG_STDIO_ALLOWED) && is_stdname(target))
            return 0;

    if ((flags & PFLG_ABSOLUTE_PATH) && !IS_ABSOLUTE_PATH(target))
    {
        sprintf( err_msg, "Absolute path expected for parameter '%s::%s', line %d", block_name,
                 var_name, rh_config_GetItemLine( curr_item ) );
        return EINVAL;
    }

    if ((flags & PFLG_NO_WILDCARDS) && WILDCARDS_IN(target))
    {
        sprintf( err_msg, "Wildcards are not allowed in '%s::%s', line %d", block_name,
                 var_name, rh_config_GetItemLine( curr_item ) );
        return EINVAL;
    }

    if (flags & PFLG_MAIL)
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

    if ((flags & PFLG_REMOVE_FINAL_SLASH) && FINAL_SLASH(target))
         REMOVE_FINAL_SLASH(target);

    return 0;

}

int GetCommandParam(config_item_t block, const char *block_name,
                    const char *var_name, param_flags_t flags, char ***target,
                    char ***extra_args_tab, unsigned int *nb_extra_args,
                    char *err_msg)
{
    config_item_t  curr_item;
    int            rc, ac;
    int            extra = 0;
    char          *name;
    char          *value;
    GError        *err_desc;

    err_msg[0] = '\0';

    if (nb_extra_args)
        *nb_extra_args = 0;
    if (extra_args_tab)
        *extra_args_tab = NULL;

    rc = get_cfg_param(block, block_name, var_name, flags, &name, &value,
                       &extra, &curr_item, err_msg);
    if (rc)
        return rc;

    /* Early check */
    if ((flags & PFLG_NO_WILDCARDS) && WILDCARDS_IN(value))
    {
        sprintf( err_msg, "Wildcards are not allowed in '%s::%s', line %d", block_name,
                 var_name, rh_config_GetItemLine( curr_item ) );
        return EINVAL;
    }

    /* free previous array */
    if (*target != NULL)
    {
        g_strfreev(*target);
        *target = NULL;
    }

    /* Split argv */
    if (value[0] != '\0')
    {
        rc = g_shell_parse_argv(value, &ac, target, &err_desc);
        if (!rc)
        {
            sprintf(err_msg, "Cannot parse '%s': %s", value, err_desc->message);
            g_error_free(err_desc);
            return EINVAL;
        }
        if (ac == 0)
        {
            g_strfreev(*target);
            *target = NULL;
        }
    }

    if (extra)
    {
        if (!extra_args_tab || !nb_extra_args)
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

    /* Post checks */

    /* empty string? */
    if ((flags & PFLG_NOT_EMPTY) && *target == NULL)
    {
        sprintf(err_msg, "Unexpected empty parameter '%s::%s', line %d",
                block_name, var_name, rh_config_GetItemLine(curr_item));
        return EINVAL;
    }

    return 0;

}

int  GetBoolParam(config_item_t block, const char *block_name,
                  const char *var_name, param_flags_t flags, bool *target,
                  char ***extra_args_tab, unsigned int *nb_extra_args,
                  char *err_msg )
{
    config_item_t  curr_item;
    int            rc, extra;
    char          *name;
    char          *value;
    int            tmp_bool;

    err_msg[0] = '\0';

    rc = get_cfg_param(block, block_name, var_name, flags, &name, &value,
                       &extra, &curr_item, err_msg);
    if (rc)
        return rc;

    tmp_bool = str2bool(value);
    if (tmp_bool == -1)
    {
        sprintf(err_msg,
                "Invalid value for '%s::%s', line %d: boolean expected (0, 1, true, false, yes, no, enabled, disabled)",
                block_name, var_name, rh_config_GetItemLine(curr_item));
        return EINVAL;
    }
    *target = (tmp_bool != 0);

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
int GetDurationParam(config_item_t block, const char *block_name,
                     const char *var_name, param_flags_t flags, time_t *target,
                     char ***extra_args_tab,
                     unsigned int *nb_extra_args, char *err_msg)
{
    config_item_t  curr_item;
    int            rc, extra;
    time_t         timeval;
    char          *name;
    char          *value;

    err_msg[0] = '\0';

    if ( nb_extra_args )
        *nb_extra_args = 0;
    if ( extra_args_tab )
        *extra_args_tab = NULL;

    rc = get_cfg_param(block, block_name, var_name, flags, &name, &value,
                       &extra, &curr_item, err_msg);
    if (rc)
        return rc;

    timeval = str2duration( value );
    if ( timeval == -1 )
    {
        sprintf( err_msg, "Invalid value for '%s::%s', line %d: duration expected. Eg: 10s",
                 block_name, var_name, rh_config_GetItemLine( curr_item ) );
        return EINVAL;
    }

    if ((flags & PFLG_POSITIVE) && (timeval < 0))
    {
        sprintf( err_msg, "Positive value expected for '%s::%s', line %d.", block_name, var_name,
                 rh_config_GetItemLine( curr_item ) );
        return EINVAL;
    }
    if ((flags & PFLG_NOT_NULL) && (timeval == 0))
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
int GetSizeParam(config_item_t block, const char *block_name,
                 const char *var_name, param_flags_t flags,
                 unsigned long long *target, char ***extra_args_tab,
                 unsigned int *nb_extra_args, char *err_msg)
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

    rc = get_cfg_param(block, block_name, var_name, flags, &name, &value,
                       &extra, &curr_item, err_msg);
    if (rc)
        return rc;

    sizeval = str2size( value );
    if ( sizeval == ( unsigned long long ) -1 )
    {
        sprintf( err_msg, "Invalid value for '%s::%s', line %d: size expected. Eg: 10MB",
                 block_name, var_name, rh_config_GetItemLine( curr_item ) );
        return EINVAL;
    }

    if ((flags & PFLG_NOT_NULL) && (sizeval == 0))
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
int            GetIntParam(config_item_t block, const char *block_name,
                           const char *var_name, param_flags_t flags, int *target,
                           char ***extra_args_tab, unsigned int *nb_extra_args,
                           char *err_msg)
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

    rc = get_cfg_param(block, block_name, var_name, flags, &name, &value,
                       &extra, &curr_item, err_msg);
    if (rc)
        return rc;

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

    if ((flags & PFLG_POSITIVE) && (intval < 0))
    {
        sprintf( err_msg, "Positive value expected for '%s::%s', line %d.", block_name, var_name,
                 rh_config_GetItemLine( curr_item ) );
        return EINVAL;
    }
    if ((flags & PFLG_NOT_NULL) && (intval == 0))
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
int GetInt64Param(config_item_t block, const char *block_name,
                  const char *var_name, param_flags_t flags, uint64_t *target,
                  char ***extra_args_tab, unsigned int *nb_extra_args,
                  char *err_msg)
{
    config_item_t  curr_item;
    int            rc, extra, nb_read;
    uint64_t       intval;
    char          *name;
    char          *value;
    char           tmpbuf[256];

    err_msg[0] = '\0';

    if ( nb_extra_args )
        *nb_extra_args = 0;
    if ( extra_args_tab )
        *extra_args_tab = NULL;

    rc = get_cfg_param(block, block_name, var_name, flags, &name, &value,
                       &extra, &curr_item, err_msg);
    if (rc)
        return rc;

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

    if ((flags & PFLG_NOT_NULL) && (intval == 0))
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
int           GetFloatParam(config_item_t block, const char *block_name,
                            const char *var_name, param_flags_t flags, double *target,
                            char ***extra_args_tab, unsigned int *nb_extra_args,
                            char *err_msg)
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

    rc = get_cfg_param(block, block_name, var_name, flags, &name, &value,
                       &extra, &curr_item, err_msg);
    if (rc)
        return rc;

    nb_read = sscanf( value, "%lf%256s", &val, tmpbuf );
    if ( nb_read < 1 )
    {
        sprintf( err_msg, "Invalid value for '%s::%s', line %d: float expected.", block_name,
                 var_name, rh_config_GetItemLine( curr_item ) );
        return EINVAL;
    }
    if ( nb_read > 1 )
    {
        if ((!(flags & PFLG_ALLOW_PCT_SIGN) && (tmpbuf[0] != '\0')) /* no sign allowed */
            || ((flags & PFLG_ALLOW_PCT_SIGN) && (strcmp(tmpbuf, "%") != 0)))   /* '%' allowed */
        {
            sprintf( err_msg,
                     "Invalid value for '%s::%s', line %d: extra characters '%s' found after float %.2f.",
                     block_name, var_name, rh_config_GetItemLine( curr_item ), tmpbuf, val );
            return EINVAL;
        }
    }

    if ((flags & PFLG_POSITIVE) && (val < 0.0))
    {
        sprintf( err_msg, "Positive value expected for '%s::%s', line %d.", block_name, var_name,
                 rh_config_GetItemLine( curr_item ) );
        return EINVAL;
    }
    if ((flags & PFLG_NOT_NULL) && (val == 0.0))
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
 * Check that no unknown parameter or block is found.
 * @param param_array NULL terminated array of allowed parameters.
 */
void CheckUnknownParameters(config_item_t block, const char *block_name,
                            const char * const *param_array )
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
            bool           found = false;

            if ( rh_config_GetKeyValue( curr_item, &name, &value, &args_flg ) == 0 )
            {
                for ( j = 0; param_array[j] != NULL; j++ )
                {
                    if ( !strcasecmp( param_array[j], name ) )
                    {
                        found = true;
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
            bool           found = false;

            name = rh_config_GetBlockName( curr_item );

            if ( name != NULL )
            {
                for ( j = 0; param_array[j] != NULL; j++ )
                {
                    if ( !strcasecmp( param_array[j], name ) )
                    {
                        found = true;
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

#define cfg_is_err(_rc, _flgs) (((_rc) != 0 && (_rc) != ENOENT) || \
                               ((_rc) == ENOENT && ((_flgs) & PFLG_MANDATORY)))

int read_scalar_params(config_item_t block, const char *block_name,
                       const cfg_param_t * params, char *msgout)
{
    int i;
    int rc = 0;

    /* read all expected parameters */
    for (i = 0; params[i].name != NULL; i++)
    {
        switch (params[i].type)
        {
            case PT_STRING:
                rc = GetStringParam(block, block_name, params[i].name,
                                    params[i].flags, (char*)params[i].ptr, params[i].ptrsize,
                                    NULL, NULL, msgout);
                if cfg_is_err(rc, params[i].flags)
                    return rc;
                break;

            case PT_CMD:
                rc = GetCommandParam(block, block_name, params[i].name,
                                     params[i].flags, (char***)params[i].ptr,
                                     NULL, NULL, msgout);
                if cfg_is_err(rc, params[i].flags)
                    return rc;
                break;

            case PT_BOOL:
                rc = GetBoolParam(block, block_name, params[i].name,
                                  params[i].flags, (bool*)params[i].ptr,
                                  NULL, NULL, msgout);
                if cfg_is_err(rc, params[i].flags)
                    return rc;
                break;

            case PT_DURATION:
                rc = GetDurationParam(block, block_name, params[i].name,
                                  params[i].flags, (time_t*)params[i].ptr,
                                  NULL, NULL, msgout);
                if cfg_is_err(rc, params[i].flags)
                    return rc;
                break;

            case PT_SIZE:
                rc = GetSizeParam(block, block_name, params[i].name,
                                  params[i].flags,
                                  (unsigned long long*)params[i].ptr,
                                  NULL, NULL, msgout);
                if cfg_is_err(rc, params[i].flags)
                    return rc;
                break;

            case PT_INT:
                rc = GetIntParam(block, block_name, params[i].name,
                                  params[i].flags, (int*)params[i].ptr,
                                  NULL, NULL, msgout);
                if cfg_is_err(rc, params[i].flags)
                    return rc;
                break;

            case PT_INT64:
                rc = GetInt64Param(block, block_name, params[i].name,
                                  params[i].flags, (uint64_t*)params[i].ptr,
                                  NULL, NULL, msgout);
                if cfg_is_err(rc, params[i].flags)
                    return rc;
                break;

            case PT_FLOAT:
                rc = GetFloatParam(block, block_name, params[i].name,
                                  params[i].flags, (double*)params[i].ptr,
                                  NULL, NULL, msgout);
                if cfg_is_err(rc, params[i].flags)
                    return rc;
                break;

            case PT_TYPE:
                sprintf(msgout, "Unexpected type for %s parameter (type)",
                        params[i].name);
                return EINVAL;
        }
    }
    return 0;
}


int get_cfg_block(config_file_t config, const char *name, config_item_t *item, char *msg_out)
{
    bool unique = true;
    config_item_t block = rh_config_FindItemByName(config, name, &unique);

    *item = NULL;

    if (block == NULL)
    {
        sprintf(msg_out, "Missing configuration block '%s'", name);
        return ENOENT;
    }
    else if (!unique)
    {
        sprintf(msg_out, "Found duplicate of block '%s' line %d.", name,
                rh_config_GetItemLine(block));
        return EEXIST;
    }

    if (rh_config_ItemType(block) != CONFIG_ITEM_BLOCK)
    {
        sprintf(msg_out, "A block is expected for '%s' item, line %d",
                name, rh_config_GetItemLine(block));
        return EINVAL;
    }
    *item = block;
    return 0;
}
