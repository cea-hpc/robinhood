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
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "update_params.h"
#include "rbh_cfg_helpers.h"
#include "rbh_misc.h"
#include "rbh_logs.h"
#include <time.h>
#include <errno.h>

#define TAG "UpdtParams"

#define OLD_UPDT_PARAMS_BLOCK      "db_update_policy"
#define UPDT_PARAMS_BLOCK          "db_update_params"

/* exported variable available to all modules */
updt_params_t updt_params;

static void set_default_update_params(void *module_config)
{
    updt_params_t *params = (updt_params_t *)module_config;

    params->md.when = UPDT_ALWAYS;
#ifdef _HAVE_FID
    params->path.when = UPDT_ON_EVENT_PERIODIC;
    params->path.period_min = 0;
    params->path.period_max = 3600;
#endif
    params->fileclass.when = UPDT_ALWAYS;
}

static void write_default_update_params(FILE *output)
{
    print_begin_block(output, 0, UPDT_PARAMS_BLOCK, NULL);
    print_line(output, 1, "md_update        : always;");
#ifdef _HAVE_FID
    print_line(output, 1, "path_update      : on_event_periodic(0,1h);");
#endif
    print_line(output, 1, "fileclass_update : always;");
    print_end_block(output, 0);
}

static void write_update_params_template(FILE *output)
{
    print_begin_block(output, 0, UPDT_PARAMS_BLOCK, NULL);
    print_line(output, 1,
               "# possible policies for refreshing metadata and path in database:");
    print_line(output, 1,
               "#   never: get the information once, then never refresh it");
    print_line(output, 1,
               "#   always: always update entry info when processing it");
    print_line(output, 1, "#   on_event: only update on related event");
    print_line(output, 1, "#   periodic(interval): only update periodically");
    print_line(output, 1,
               "#   on_event_periodic(min_interval,max_interval)= on_event + periodic");
    fprintf(output, "\n");
    print_line(output, 1, "# Updating of file metadata");
    print_line(output, 1, "md_update = always ;");
#ifdef _HAVE_FID
    print_line(output, 1, "# Updating file path in database");
    print_line(output, 1, "path_update = on_event_periodic(0,1h) ;");
#endif
    print_line(output, 1, "# File classes matching");
    print_line(output, 1, "fileclass_update = always ;");

    print_end_block(output, 0);
}

static inline const char *update_param2str(updt_param_item_t *pol,
                                           char *buffer)
{
    char tmpbuf1[256];
    char tmpbuf2[256];

    switch (pol->when) {
    case UPDT_NEVER:
        return "never";
    case UPDT_ALWAYS:
        return "always";
    case UPDT_ON_EVENT:
        return "on_event";
    case UPDT_ON_EVENT_PERIODIC:
        FormatDurationFloat(tmpbuf1, sizeof(tmpbuf1), pol->period_min);
        FormatDurationFloat(tmpbuf2, sizeof(tmpbuf2), pol->period_max);
        sprintf(buffer, "on_event_periodic(%s,%s)", tmpbuf1, tmpbuf2);
        return buffer;
    case UPDT_PERIODIC:
        FormatDurationFloat(tmpbuf1, sizeof(tmpbuf1), pol->period_max);
        sprintf(buffer, "periodic(%s)", tmpbuf1);
        return buffer;
    default:
        return "???";
    }
}

static int read_update_item(updt_param_item_t *item, const char *str,
                            char **options, unsigned int nb_options,
                            char *msg_out)
{
    memset(item, 0, sizeof(updt_param_item_t));

    if (!strcasecmp(str, "never"))
        item->when = UPDT_NEVER;
    else if (!strcasecmp(str, "always"))
        item->when = UPDT_ALWAYS;
    else if (!strcasecmp(str, "on_event"))
        item->when = UPDT_ON_EVENT;
    else if (!strcasecmp(str, "periodic")) {
        time_t timeval;

        item->when = UPDT_PERIODIC;
        if ((nb_options != 1) || !options || !options[0]) {
            strcpy(msg_out,
                   "1 argument is expected for periodic update parameter. "
                   "E.g. periodic(30s);");
            return EINVAL;
        }
        /* read argument as a duration */
        timeval = str2duration(options[0]);
        if (timeval == -1) {
            sprintf(msg_out, "Invalid value for periodic update parameter: "
                    "duration expected. E.g. periodic(5min);");
            return EINVAL;
        }
        item->period_min = item->period_max = timeval;
    } else if (!strcasecmp(str, "on_event_periodic")) {
        time_t timeval1, timeval2;

        item->when = UPDT_ON_EVENT_PERIODIC;
        if ((nb_options != 2) || !options || !options[0] || !options[1]) {
            strcpy(msg_out,
                   "2 arguments are expected for on_event_periodic update parameter. "
                   "E.g. on_event_periodic(1s,30s);");
            return EINVAL;
        }
        /* read argument as a duration */
        timeval1 = str2duration(options[0]);
        timeval2 = str2duration(options[1]);
        if ((timeval1 == -1) || (timeval2 == -1)) {
            sprintf(msg_out,
                    "Invalid value for on_event_periodic update parameter: "
                    "durations expected. E.g. on_event_periodic(1s,5min);");
            return EINVAL;
        }
        item->period_min = timeval1;
        item->period_max = timeval2;
    } else {
        sprintf(msg_out,
                "Invalid update parameter '%s' (expected: never, always, "
                "on_event, periodic(<interval>), on_event_periodic(<intvl1>,<intvl2>)",
                str);
        return EINVAL;
    }

    return 0;
}

static int read_update_params(config_file_t config, void *module_config,
                              char *msg_out)
{
    updt_params_t   *params = (updt_params_t *) module_config;
    int              rc;
    char             tmpstr[1024];
    char           **options = NULL;
    unsigned int     nb_options = 0;
    config_item_t    updt_block;

    static const char *update_allow[] = {
        "md_update",
#ifdef _HAVE_FID
        "path_update",
#endif
        "fileclass_update",
        NULL
    };

    /* get db_update_params block */

    /* check the new name first */
    rc = get_cfg_block(config, UPDT_PARAMS_BLOCK, &updt_block, msg_out);
    if (rc == ENOENT) {
        /* try with the deprecated name */
        rc = get_cfg_block(config, OLD_UPDT_PARAMS_BLOCK, &updt_block, msg_out);
        if (rc == ENOENT)
            /* not mandatory: no error */
            return 0;
        else if (rc != 0)
            return rc;
        /* found the old name */
        DisplayLog(LVL_CRIT, TAG, "WARNING: block name '"
                   OLD_UPDT_PARAMS_BLOCK "' is deprecated. Use '"
                   UPDT_PARAMS_BLOCK "' instead");
    }

    /* get parameters from this block */
    rc = GetStringParam(updt_block, UPDT_PARAMS_BLOCK, "md_update",
                        PFLG_NO_WILDCARDS, tmpstr, sizeof(tmpstr),
                        &options, &nb_options, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    if (rc != ENOENT) {
        /* parse the parameter */
        rc = read_update_item(&params->md, tmpstr, options, nb_options,
                              msg_out);
        if (rc)
            return rc;
    }
#ifdef _HAVE_FID
    rc = GetStringParam(updt_block, UPDT_PARAMS_BLOCK, "path_update",
                        PFLG_NO_WILDCARDS, tmpstr, sizeof(tmpstr),
                        &options, &nb_options, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    if (rc != ENOENT) {
        /* parse the parameter */
        rc = read_update_item(&params->path, tmpstr, options, nb_options,
                              msg_out);
        if (rc)
            return rc;
    }
#endif

    /* get parameters from this block */
    rc = GetStringParam(updt_block, UPDT_PARAMS_BLOCK, "fileclass_update",
                        PFLG_NO_WILDCARDS, tmpstr, sizeof(tmpstr),
                        &options, &nb_options, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    if (rc != ENOENT) {
        /* parse the parameter */
        rc = read_update_item(&params->fileclass, tmpstr, options, nb_options,
                              msg_out);
        if (rc)
            return rc;

        if ((params->fileclass.when == UPDT_ON_EVENT) ||
            (params->fileclass.when == UPDT_ON_EVENT_PERIODIC)) {
            sprintf(msg_out, "Parameter not supported for fileclass update: "
                    "'never', 'always' or 'periodic' expected");
            return EINVAL;
        }
    }

    CheckUnknownParameters(updt_block, UPDT_PARAMS_BLOCK, update_allow);
    return 0;
}

static int reload_update_params(void *module_config)
{
    char buff1[256];
    char buff2[256];
    updt_params_t *params = (updt_params_t *) module_config;

    if ((updt_params.md.when != params->md.when)
        || (updt_params.md.period_min != params->md.period_min)
        || (updt_params.md.period_max != params->md.period_max)) {
        DisplayLog(LVL_EVENT, TAG,
                   UPDT_PARAMS_BLOCK "::md_update updated: %s->%s",
                   update_param2str(&updt_params.md, buff1),
                   update_param2str(&params->md, buff2));
        updt_params.md = params->md;
    }
#ifdef _HAVE_FID
    if ((updt_params.path.when != params->path.when)
        || (updt_params.path.period_min != params->path.period_min)
        || (updt_params.path.period_max != params->path.period_max)) {
        DisplayLog(LVL_EVENT, TAG,
                   UPDT_PARAMS_BLOCK "::path_update updated: %s->%s",
                   update_param2str(&updt_params.path, buff1),
                   update_param2str(&params->path, buff2));
        updt_params.path = params->path;
    }
#endif

    if ((updt_params.fileclass.when != params->fileclass.when)
        || (updt_params.fileclass.period_min != params->fileclass.period_min)
        || (updt_params.fileclass.period_max != params->fileclass.period_max)) {
        DisplayLog(LVL_EVENT, TAG,
                   UPDT_PARAMS_BLOCK "::fileclass_update updated: %s->%s",
                   update_param2str(&updt_params.fileclass, buff1),
                   update_param2str(&params->fileclass, buff2));
        updt_params.fileclass = params->fileclass;
    }

    return 0;
}

static int update_params_set(void *module_config, bool reload)
{
    updt_params_t *conf = (updt_params_t *) module_config;

    if (reload)
        return reload_update_params(module_config);

    updt_params = *conf;
    return 0;
}

static void *updt_param_new(void)
{
    return calloc(1, sizeof(updt_params_t));
}

static void updt_param_free(void *cfg)
{
    if (cfg != NULL)
        free(cfg);
}

/* export config functions */
mod_cfg_funcs_t updt_params_hdlr = {
    .module_name = "updt params",
    .new = updt_param_new,
    .free = updt_param_free,
    .set_default = set_default_update_params,
    .read = read_update_params,
    .set_config = update_params_set,
    .write_default = write_default_update_params,
    .write_template = write_update_params_template,
};

/**
 *  Check if the fileclass needs to be updated
 */
bool need_fileclass_update(const attr_set_t *p_attrs)
{
    bool        is_set = false;
    time_t      last = 0;
    const char *match = "";

    is_set = ATTR_MASK_TEST(p_attrs, class_update)
        && ATTR_MASK_TEST(p_attrs, fileclass);
    if (is_set) {
        last = ATTR(p_attrs, class_update);
        match = ATTR(p_attrs, fileclass);
    }

    /* check for periodic fileclass matching */
    if (!is_set) {
        DisplayLog(LVL_FULL, TAG, "Need to update fileclass (not set)");
        return true;
    } else if (updt_params.fileclass.when == UPDT_ALWAYS) {
        DisplayLog(LVL_FULL, TAG, "Need to update fileclass "
                   "(policy is 'always update')");
        return true;
    } else if (updt_params.fileclass.when == UPDT_NEVER) {
        DisplayLog(LVL_FULL, TAG, "No fileclass update "
                   "(policy is 'never update')");
        return false;
    } else if (updt_params.fileclass.when == UPDT_PERIODIC) {
        if (time(NULL) - last >= updt_params.fileclass.period_max) {
            DisplayLog(LVL_FULL, TAG, "Need to update fileclass "
                       "(out-of-date) (last match=%" PRI_TT ")", last);
            return true;
        } else {
            /* retrieve previous fileclass */
            DisplayLog(LVL_FULL, TAG, "Previously matched fileclass '%s'"
                       " is still valid (last match=%" PRI_TT ")", match, last);
            return false;
        }
    }
    RBH_BUG("Unexpected case: 'update_fileclass' cannot be determined");
    return -1;
}

/**
 *  Check if path or md needs to be updated
 *  \param p_allow_event [out] if set to TRUE, the path
 *         must be updated on related event.
 */
bool need_info_update(const attr_set_t *p_attrs, bool *update_if_event,
                      type_info_t type_info)
{
    bool        do_update = false;
    bool        is_set = false;
    time_t      last = 0;
    const char *why = "<unexpected>";
    const char *what = "";
    updt_param_item_t pol;

    if (update_if_event != NULL)
        *update_if_event = false;

    if (type_info == UPDT_MD) {
        pol = updt_params.md;
        what = "metadata";
        is_set = ATTR_MASK_TEST(p_attrs, md_update);
        if (is_set)
            last = ATTR(p_attrs, md_update);
    }
#ifdef _HAVE_FID
    else if (type_info == UPDT_PATH) {
        what = "POSIX path";
        pol = updt_params.path;
        is_set = ATTR_MASK_TEST(p_attrs, path_update);
        if (is_set)
            last = ATTR(p_attrs, path_update);
    }
#endif
    else {
        RBH_BUG("Unsupported info type");
        return -1;
    }

    if (!is_set) {
        do_update = true;
        why = "not in DB/never updated";
    }
    /* Need to update the path if it is partial */
    else if (ATTR_MASK_TEST(p_attrs, fullpath) &&
             ATTR(p_attrs, fullpath)[0] != '/') {
        do_update = true;
        why = "partial path in DB";
    } else if (pol.when == UPDT_ALWAYS) {
        do_update = true;
        why = "policy is 'always update'";
    } else if (pol.when == UPDT_NEVER) {
        do_update = false;
    } else if (pol.when == UPDT_ON_EVENT) {
        do_update = false;
        if (update_if_event != NULL)
            *update_if_event = true;
    } else if (pol.when == UPDT_PERIODIC) {
        if (time(NULL) - last >= pol.period_max) {
            do_update = true;
            why = "expired";
        } else {
            do_update = false;
        }
    } else if (pol.when == UPDT_ON_EVENT_PERIODIC) {
        /* if the update is too recent, do not update.
         * if the update is too old, force update.
         * else, update on path-related event. */
        if (time(NULL) - last < pol.period_min) {
            do_update = false;
        } else if (time(NULL) - last >= pol.period_max) {
            do_update = true;
            why = "expired";
        } else {    /* allow update on event */

            do_update = false;
            if (update_if_event != NULL)
                *update_if_event = true;
        }
    } else {
        DisplayLog(LVL_CRIT, TAG, "Unknown update policy %#x", pol.when);
        return -1;
    }

    if (do_update)
        DisplayLog(LVL_FULL, TAG, "Update of %s: reason=%s, "
                   "last_update=%" PRI_TT, what, why, last);

    return do_update;
}
