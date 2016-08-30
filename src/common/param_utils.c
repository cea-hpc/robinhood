/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2015 CEA/DAM
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

#include "rbh_misc.h"
#include "rbh_logs.h"
#include "status_manager.h"

#include <ctype.h>

#define PARAMS_TAG "params" /* tag for logs */

/**
 * Callback function for placeholder processing when
 * parsing a string with placeholders like {xxx}.
 * @param[in]     name  the placeholder name.
 * @param[in]     start_idx placeholder start index in original string.
 * @param[in]     end_idx   placeholder end index in original string.
 * @param[in,out] udata user data (udata parameter from placeholder_foreach()).
 * @return 0 on success, an error code on failure.
 */
typedef int (*placeholder_func_t) (const char *name, int start_idx,
                                   int end_idx, void *udata);

/** check if a string matches a variable name */
static bool match_varname(const char *str, int len)
{
    int i;

    if (len == 0)
        return true;    /* allowed empty var */

    /* letter expected as first char */
    if (!isalpha(str[0]))
        return false;

    /* letter, number, underscore or dot expected */
    for (i = 1; i < len; i++)
        if (!isalnum(str[i]) && str[i] != '_' && str[i] != '.')
            return false;

    return true;
}

/** placeholder iterator flags */
typedef enum {
    PH_ALLOW_EMPTY = (1 << 0),   /**< allow empty variable names */
    PH_STRICT_BRACES = (1 << 1), /**< strictly check open/close braces */
} ph_flags_t;

/**
 * Look for placeholders {xxx} in str and call ph_func for each
 * placeholder found. Processing stops at first error returned
 * by the callback function ph_func.
 * @param[in] str the string to be parsed.
 * @param[in] str_descr string context description to be displayed in
 *                      error messages (e.g. "cfg_block::foo_param line 42").
 * @param[in] ph_func callback function to be called for each placeholder.
 * @param[in,out] udata arbitrary user data passed to ph_func callback.
 * @return 0 on success, an error code on failure.
 */
static int placeholder_foreach(const char *str, const char *str_descr,
                               placeholder_func_t ph_func, void *udata,
                               ph_flags_t flags)
{
    const char *pass_begin = str;

    do {
        const char *begin_var;
        const char *end_var = NULL;
        char *var_name;
        int rc;

        /* Look for {var} patterns, allowing the usage of {}'s for other
         * purposes (like JSON format).
         * E.g. in '{"entry_id":"{fid}","fileclass":"{fileclass}"}'
         * only {fid} and {fileclass} must be interpreted as placeholders.
         */

        /* look for a variable */
        begin_var = strchr(pass_begin, '{');

        if (flags & PH_STRICT_BRACES)
            /* check for unexpected '}' */
            end_var = strchr(pass_begin, '}');

        /* no more variables */
        if (!begin_var) {
            if ((flags & PH_STRICT_BRACES) && (end_var != NULL)) {
                DisplayLog(LVL_CRIT, PARAMS_TAG,
                           "ERROR: unexpected '}' near '%s' in %s", pass_begin,
                           str_descr);
                return -EINVAL;
            }
            /* don't check '}' */
            break;
        }

        if (flags & PH_STRICT_BRACES) {
            if (end_var == NULL) {
                DisplayLog(LVL_CRIT, PARAMS_TAG, "ERROR: unmatched '{' in %s",
                           str_descr);
                return -EINVAL;
            } else if (end_var < begin_var) {
                DisplayLog(LVL_CRIT, PARAMS_TAG,
                           "ERROR: unexpected '}' near '%.*s' in %s",
                           (int)(begin_var - pass_begin + 1), pass_begin,
                           str_descr);
                return -EINVAL;
            }
            /* end_var is already set and is after begin_var */
        } else {
            /* get the first matching '}' after '{' */
            end_var = strchr(begin_var, '}');
            /* no strict braces control: allow no closing brace */
            if (!end_var)
                break;
        }

        if (!(flags & PH_ALLOW_EMPTY) && (end_var == begin_var + 1)) {
            DisplayLog(LVL_CRIT, PARAMS_TAG, "ERROR: empty var name in %s",
                       str_descr);
            return -EINVAL;
        }

        /* if the section between braces doesn't match a variable name,
         * skip the opening braces to look for a '{var}' section */
        if (!match_varname(begin_var + 1, end_var - begin_var - 1)) {
            /* unexpected format */
            if (flags & PH_STRICT_BRACES) {
                DisplayLog(LVL_CRIT, PARAMS_TAG,
                           "Unexpected variable syntax near '%.*s' in %s",
                           (int)(end_var - begin_var + 1), begin_var,
                           str_descr);
                return -EINVAL;
            }

            /* just skip it and continue parsing */
            pass_begin = begin_var + 1;
            continue;
        }

        var_name = strndup(begin_var + 1, end_var - begin_var - 1);
        if (!var_name)
            return -ENOMEM;

#ifdef _DEBUG_POLICIES
        fprintf(stderr, "processing variable '%s' in %s\n", var_name,
                str_descr);
#endif

        rc = ph_func(var_name, begin_var - str, end_var - str, udata);
        free(var_name);
        if (rc)
            return rc;

        pass_begin = end_var + 1;

    } while (1);

    return 0;
}

/**
 * Function to get the value of a placeholder.
 * @param[out] free_str whether the returned value must be freed.
 */
typedef char *(*param_value_get_func_t) (const entry_id_t *id,
                                         const attr_set_t *attrs,
                                         int attr_index, bool *free_str);

/** information about placeholder values */
struct param_descr {
    const char *name;
    int attr_index;                    /**< -1 for none */
    param_value_get_func_t get_func;
};

/* ========== placeholder value helpers ========== */

/** return a string attribute */
static char *get_str_attr(const entry_id_t *id, const attr_set_t *attrs,
                          int attr_index, bool *free_str)
{
    *free_str = false;

    if (attrs == NULL) {
        DisplayLog(LVL_MAJOR, PARAMS_TAG,
                   "ERROR: entry attributes are not available in this context");
        return NULL;
    }
    if (!attr_mask_test_index(&attrs->attr_mask, attr_index)) {
        /* for getting field_name in field_info array */
        assert(attr_index < 32);
        DisplayLog(LVL_MAJOR, PARAMS_TAG,
                   "ERROR: missing attribute '%s' to perform variable substitution",
                   field_infos[attr_index].field_name);
        return NULL;
    }

    if (attr_index == ATTR_INDEX_stripe_info)
#ifdef _LUSTRE
        return (char *)(ATTR(attrs, stripe_info).pool_name);
#else
        return NULL;
#endif
    else
        return (char *)&attrs->attr_values + field_infos[attr_index].offset;
}

/** return a fid string representation */
static char *get_fid_str(const entry_id_t *id, const attr_set_t *attrs,
                         int attr_index, bool *free_str)
{
    char *fid_str;

    if (id == NULL) {
        DisplayLog(LVL_MAJOR, PARAMS_TAG,
                   "ERROR: entry fid is not available in this context");
        return NULL;
    }

    if (asprintf(&fid_str, DFID_NOBRACE, PFID(id)) < 0)
        return NULL;

    *free_str = true;
    return fid_str;
}

/** return FS name */
static char *get_fsname_param(const entry_id_t *id, const attr_set_t *attrs,
                              int attr_index, bool *free_str)
{
    *free_str = false;
    return (char *)get_fsname();
}

/** return FS root directory */
static char *get_fsroot_param(const entry_id_t *id, const attr_set_t *attrs,
                              int attr_index, bool *free_str)
{
    *free_str = false;
    return (char *)global_config.fs_path;
}

/** return path to robinhood configuration file */
static char *get_cfg_param(const entry_id_t *id, const attr_set_t *attrs,
                           int attr_index, bool *free_str)
{
    *free_str = false;
    return (char *)config_file_path();
}

/** standard parameters allowed in placeholders */
static const struct param_descr std_params[] = {
    /* entry attributes std params */
    {"name", ATTR_INDEX_name, get_str_attr},
    {"path", ATTR_INDEX_fullpath, get_str_attr},
    {"fullpath", ATTR_INDEX_fullpath, get_str_attr},
    {"fid", -1, get_fid_str},
    {"ost_pool", ATTR_INDEX_stripe_info, get_str_attr},

    /* global params */
    {"fsname", -1, get_fsname_param},
    {"fsroot", -1, get_fsroot_param},
    {"fspath", -1, get_fsroot_param},
    {"cfg", -1, get_cfg_param},

    /* end of params */
    {NULL, -1, NULL}
};

/** get the std parameter descriptor for the given name */
static const struct param_descr *get_stdarg(const char *name)
{
    const struct param_descr *c;

    for (c = &std_params[0]; c->name != NULL; c++) {
        if (!strcasecmp(c->name, name))
            return c;
    }
    return NULL;
}

/** argument structure for set_param_mask() callback */
struct set_param_mask_args {
    /** description of the string being parsed */
    const char *str_descr;
    /** mask being built (to be returned by params_mask()) */
    attr_mask_t mask;
};

/** callback function to generate std params mask */
static int set_param_mask(const char *name, int begin_idx, int end_idx,
                          void *udata)
{
    struct set_param_mask_args *args = udata;
    const struct param_descr *a;

    if (unlikely(args == NULL))
        return -EINVAL;

    /* only std parameters have a mask */
    a = get_stdarg(name);
    if (a != NULL) {
        if (a->attr_index != -1)
            attr_mask_set_index(&args->mask, a->attr_index);
    }

    /* unknown param have no mask */
    return 0;
}

attr_mask_t params_mask(const char *str, const char *str_descr, bool *err)
{
    struct set_param_mask_args args = {
        .mask = {0},
        .str_descr = str_descr
    };

    *err = false;

    if (placeholder_foreach(str, str_descr, set_param_mask, (void *)&args, 0)) {
        *err = true;
        return null_mask;
    }

    return args.mask;
}

/** argument structure for build_cmd() callback */
struct build_cmd_args {
    bool quote;

    /** entry id, attrs, ... */
    const entry_id_t *id;
    const attr_set_t *attrs;
    /** arbitrary parameters */
    const struct rbh_params *user_params;
    /** additional parameters */
    const char **addl_params;
    /** status manager instance from context */
    const sm_instance_t *smi;
    /** description of the string being parsed */
    const char *str_descr;
    /** original string passed to subst_cmd_params() */
    const char *orig_str;
    /** index following the last processed placeholder in orig_str */
    int last_idx;
    /** String being built (to be returned by subst_cmd_params()).
     * Initially allocated and empty (""). */
    GString *out_str;
};

char *quote_shell_arg(const char *arg)
{
    const char *replace_with = "'\\''";
    char *arg_walk, *quoted, *quoted_walk;
    int count = 0;

    arg_walk = (char *)arg;
    while (*arg_walk) {
        if (*arg_walk == '\'') {
            ++count;
            if (count < 0) {
                /* It's unlikely given our input, but avoid integer overflow. */
                return NULL;
            }
        }
        ++arg_walk;
    }

    quoted = (char *)calloc(1, strlen(arg) +
                            (count * strlen(replace_with)) + 2 + 1);
    if (!quoted)
        return NULL;

    quoted_walk = quoted;
    *quoted_walk = '\'';
    ++quoted_walk;

    arg_walk = (char *)arg;
    while (*arg_walk) {
        if (*arg_walk == '\'') {
            strcat(quoted_walk, replace_with);
            quoted_walk += strlen(replace_with);
        } else {
            *quoted_walk = *arg_walk;
            ++quoted_walk;
        }
        ++arg_walk;
    }

    *quoted_walk = '\'';
    ++quoted_walk;
    *quoted_walk = '\0';

    return quoted;
}

/** callback function to build a command by replacing placeholders. */
static int build_cmd(const char *name, int begin_idx, int end_idx, void *udata)
{
    struct build_cmd_args *args = udata;
    const char *val = NULL;
    char *quoted_arg = NULL;
    bool free_val = false;
    int rc;

    if (unlikely(args == NULL))
        return -EINVAL;

    /* append from last position to current position as is */
    if (begin_idx > args->last_idx)
        g_string_append_len(args->out_str, args->orig_str + args->last_idx,
                            begin_idx - args->last_idx);

    /* get value for the current parameter */

    /* 1) search in user parameters */
    if (val == NULL && args->user_params != NULL)
        val = rbh_param_get(args->user_params, name);

    /* 2) search in std parameters */
    if (val == NULL) {
        const struct param_descr *a = get_stdarg(name);

        if (a != NULL) {
            val = a->get_func(args->id, args->attrs, a->attr_index, &free_val);
            if (val == NULL)
                return -ENOENT;
        }
    }

    /* 3) search in additional parameters */
    if (val == NULL && args->addl_params != NULL) {
        const char **cp;

        for (cp = &args->addl_params[0]; *cp != NULL; cp += 2) {
            if (!strcasecmp(cp[0], name))
                val = (char *)cp[1];
        }
    }

    /* 4) search in policy-specific parameters (status, specific info...) */
    if (val == NULL) {
        void *pval;
        const sm_info_def_t *def;
        unsigned int idx;

        rc = sm_attr_get(args->smi, args->attrs, name, &pval, &def, &idx);
        if (rc == 0) {
            GString *gs = g_string_new("");

            ListMgr_PrintAttrPtr(gs, def->db_type, pval, "");
            val = gs->str;
            free_val = true;
            g_string_free(gs, FALSE);
        } else if (rc == -ENODATA) {
            /* parameter exists but is not set.
             * No previous value was found, use empty string instead.
             */
            val = "";
        }
    }

    if (val == NULL) {
        /* not found */
        DisplayLog(LVL_CRIT, PARAMS_TAG,
                   "ERROR: unexpected variable '%s' in %s", name,
                   args->str_descr);
        return -EINVAL;
    }

    if (args->quote) {
        /* quote the value and append it to command line */
        quoted_arg = quote_shell_arg(val);
        if (!quoted_arg) {
            rc = -ENOMEM;
            goto out_free;
        }

        g_string_append(args->out_str, quoted_arg);
    } else
        g_string_append(args->out_str, val);

    args->last_idx = end_idx + 1;
    rc = 0;

 out_free:
    free(quoted_arg);
    if (free_val)
        free((char *)val);
    return rc;
}

char *subst_params(const char *str_in,
                   const char *str_descr,
                   const entry_id_t *p_id,
                   const attr_set_t *p_attrs,
                   const action_params_t *params,
                   const char **subst_array,
                   const struct sm_instance *smi,
                   bool quote, bool strict_braces)
{
    struct build_cmd_args args = {
        .quote = quote,
        .id = p_id,
        .attrs = p_attrs,
        .user_params = params,
        .addl_params = subst_array,
        .smi = smi,
        .str_descr = str_descr,
        .orig_str = str_in,
        .last_idx = 0,
        .out_str = NULL,
    };
    char *ret;

    args.out_str = g_string_new("");

    if (!args.out_str || !args.str_descr)
        goto err_free;

    if (placeholder_foreach(str_in, args.str_descr, build_cmd, (void *)&args,
                            PH_ALLOW_EMPTY | (strict_braces ? PH_STRICT_BRACES :
                                              0)))
        goto err_free;

    /* append the end of the string */
    if (args.last_idx < strlen(str_in))
        g_string_append(args.out_str, str_in + args.last_idx);

    /* don't release the string itself (freed by the caller) */
    ret = args.out_str->str;
    g_string_free(args.out_str, FALSE);

    DisplayLog(LVL_FULL, PARAMS_TAG, "'%s'->'%s' in %s", str_in, ret,
               str_descr);
    return ret;

 err_free:
    if (args.out_str)
        g_string_free(args.out_str, TRUE);
    return NULL;
}

/*
 * Ideally would want cmd_in to be char const * const *, but
 * implicit casts don't work well with these in standard C
 */
int subst_shell_params(char **cmd_in,
                       const char *str_descr,
                       const entry_id_t *p_id,
                       const attr_set_t *p_attrs,
                       const action_params_t *params,
                       const char **subst_array,
                       const struct sm_instance *smi,
                       bool strict_braces, char ***cmd_out)
{
    int i;
    char **out_av;
    int ac;

    if (!cmd_in || !cmd_in[0] || !str_descr)
        return -EINVAL;

    /* count ac once to allocate properly */
    for (ac = 0; cmd_in[ac]; ac++)
        ;

    /* allocate out_av, NULL terminated char array */
    out_av = calloc(sizeof(*out_av), ac + 1);
    for (i = 0; i < ac; i++) {
        out_av[i] = subst_params(cmd_in[i], str_descr, p_id, p_attrs,
                                 params, subst_array, smi, false,
                                 strict_braces);
        if (out_av[i] == NULL)
            goto err_free;

        DisplayLog(LVL_FULL, PARAMS_TAG, "[%d] '%s'->'%s' in %s", i,
                   cmd_in[i], out_av[i], str_descr);
    }

    *cmd_out = out_av;

    /* don't release out_av (freed by the caller) */
    return 0;

 err_free:
    g_strfreev(out_av);
    /* only EINVAL for now, might expand that if we explode subst_params */
    return -EINVAL;
}

char *concat_cmd(char **cmd)
{
    GString *built_command;
    char *out_str;
    int i;

    if (!cmd || !cmd[0])
        return NULL;

    built_command = g_string_new(cmd[0]);
    for (i = 1; cmd[i]; i++) {
        g_string_append_c(built_command, ' ');
        g_string_append(built_command, cmd[i]);
    }

    out_str = built_command->str;
    g_string_free(built_command, FALSE);

    return out_str;
}

/* use compare_generic ? */
int compare_cmd(char **c1, char **c2)
{
    int rc;

    while (c1 && c2) {
        rc = strcmp(*c1, *c2);
        if (rc)
            return rc;
    }

    /* only c1 left, it's bigger */
    if (c1)
        return 1;

    if (c2)
        return -1;

    return 0;
}
