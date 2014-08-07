/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009, 2010 CEA/DAM
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

#include "status_manager.h"
#include "rbh_misc.h"
#include "Memory.h"

/** list of loaded status managers */
static status_manager_t **sm_cache = NULL;
static unsigned int sm_count = 0;

/** list of status manager instances */
static sm_instance_t **sm_inst = NULL;
unsigned int sm_inst_count = 0; /* must be available from other modules to handle attribute masks */

void sm_status_ensure_alloc(char const ***p_tab)
{
    if (*p_tab == NULL)
        *p_tab = MemCalloc(sm_inst_count, sizeof(char *));
}

void sm_status_free(char const ***p_tab)
{
    if (*p_tab != NULL)
    {
        MemFree(*p_tab);
        *p_tab = NULL;
    }
}

/* contents of status_manager:
name, flags, status_enum, status_count, status_needs_attrs_cached,
status_needs_attrs_fresh, get_status_func, changelog_cb
*/

#define LHSM_ST_COUNT   7
/* XXX /!\ Must match hsm_status_t order */
static const  char* lhsm_status_list[] = {"new","modified","retrieving","archiving",
                                          "synchro","released","release_pending"};

#define BASIC_ST_COUNT 2
static const  char* basic_status_list[] = {"ok", "failed"}; /* + not set */

static const char *hsm_status2str(hsm_status_t st)
{
    if ((st >= STATUS_COUNT) || (st == STATUS_UNKNOWN))
        return NULL;
    else
        return lhsm_status_list[st-1]; /* st=1 => new */
}

static int lhsm_status(struct sm_instance *smi,
                       const entry_id_t *id, const attr_set_t *attrs,
                       attr_set_t *refreshed_attrs)
{
    int rc;
    char fid_path[RBH_PATH_MAX];
    hsm_status_t st = STATUS_UNKNOWN;
    bool no_release = false, no_archive = false;

    if (ATTR_MASK_TEST(attrs, type) &&
        strcmp(ATTR(attrs, type), STR_TYPE_FILE) != 0)
    {
        /* not a file: no status */
        rc = 0;
        goto clean_status;
    }

    rc = BuildFidPath(id, fid_path);
    if (rc)
        goto clean_status;

    /** @TODO store no_release and no_archive as SM specific attributes */
    rc = LustreHSM_GetStatus(fid_path, &st, &no_release, &no_archive);
    if (rc)
        goto clean_status;

    /* set status in refreshed attrs */
    const char *str_st = hsm_status2str(st);
    if (str_st == NULL)
        goto clean_status;

    /* check allocation of sm_status array */
    sm_status_ensure_alloc(&refreshed_attrs->attr_values.sm_status);
    if (refreshed_attrs->attr_values.sm_status == NULL)
    {
        rc = -ENOMEM;
        goto clean_status;
    }

    STATUS_ATTR(refreshed_attrs, smi->smi_index) = str_st;
    ATTR_MASK_STATUS_SET(refreshed_attrs, smi->smi_index);

    return 0;

clean_status:
    if (refreshed_attrs->attr_values.sm_status != NULL)
        /* don't free it as it contains a const char* */
        STATUS_ATTR(refreshed_attrs, smi->smi_index) = NULL;

    /* Clean the status from the mask */
    ATTR_MASK_STATUS_UNSET(refreshed_attrs, smi->smi_index);

    return rc;
}


static status_manager_t status_mgrs[] = {
    {"lhsm", SM_SHARED, lhsm_status_list, LHSM_ST_COUNT, ATTR_MASK_type, 0,
      lhsm_status, NULL}, /* @TODO callback functions  */
    {"basic", 0, basic_status_list, BASIC_ST_COUNT, 0, 0, NULL, NULL}, /* @FIXME masks, functions  */

    {NULL, 0, NULL, 0, 0, 0, NULL, NULL}
};


/** TODO load status manager from dynamic module */
static status_manager_t *load_status_manager(const char *name)
{
    status_manager_t *curr;
    for (curr = status_mgrs; curr->name != NULL; curr++)
    {
        if (!strcasecmp(name, curr->name))
            return curr;
    }
    return NULL;
}


/** indicate if a status manager definition is already loaded */
static inline int sm_loaded(const char *name, status_manager_t **sm_ptr)
{
    int i;
    for (i = 0; i < sm_count; i++)
    {
        if (!strcasecmp(sm_cache[i]->name, name))
        {
            *sm_ptr = sm_cache[i];
            return TRUE;
        }
    }
    return FALSE;
}

/** get a loaded status manager or allocate a new one */
static const status_manager_t *sm_get(const char *name)
{
    status_manager_t *tmp;

    if (sm_loaded(name, &tmp))
        return tmp;

    tmp = load_status_manager(name);
    if (tmp == NULL)
        return NULL;

    /* add it to the cache */
    sm_count++;
    sm_cache = realloc(sm_cache, sm_count * sizeof(status_manager_t*));
    if (sm_cache == NULL)
        return NULL;
    sm_cache[sm_count-1] = tmp;

    return tmp;
}


/** check if an instance of shared status manager exists */
static inline int sm_instance_exists(const char *name, sm_instance_t **smi_ptr)
{
    int i;
    for (i = 0; i < sm_inst_count; i++)
    {
        if (!strcasecmp(sm_inst[i]->sm->name, name))
        {
            *smi_ptr = sm_inst[i];
            return TRUE;
        }
    }
    return FALSE;
}

/** create a status manager instance (if it does not already exist) */
sm_instance_t *create_sm_instance(const char *pol_name,const char *sm_name)
{
    sm_instance_t *smi = NULL;

    /* check that the status manager exists (load it if necessary) */
    const status_manager_t *sm = sm_get(sm_name);
    if (sm == NULL)
        return NULL;

    /* if it is shared, check if it is already instanciated */
    if ((sm->flags & SM_SHARED) && sm_instance_exists(sm_name, &smi))
        return smi;

    /* create an instance */
    smi = calloc(1, sizeof(sm_instance_t));
    if (smi == NULL)
        return NULL;

    smi->sm = sm;
    smi->smi_index = sm_inst_count;

    if (sm->flags & SM_SHARED)
    {
        /* If the status manager is shared between policies,
         * it just consists of the status manager name. */
        smi->instance_name = strdup(sm->name);
    }
    else /* private status manager (1 instance per policy) */
    {
        /* <status manager name>_<policy name>\0 */
        smi->instance_name = malloc(strlen(sm->name)+strlen(pol_name)+2);
        if (smi->instance_name == NULL)
            goto free_smi;
        sprintf(smi->instance_name, "%s_%s", sm->name, pol_name);
    }
    /* <instance_name>_status */
    smi->db_field = malloc(strlen(smi->instance_name)+8);
    if (smi->db_field == NULL)
        goto free_str1;
    sprintf(smi->db_field, "%s_status", smi->instance_name);

    /* add it the the list of SMIs */
    sm_inst_count++;
    sm_inst = realloc(sm_inst, sm_inst_count * sizeof(sm_instance_t*));
    if (sm_inst == NULL)
        goto free_str2;
    sm_inst[sm_inst_count-1] = smi;

    return smi;

free_str2:
    free(smi->db_field);
free_str1:
    free(smi->instance_name);
free_smi:
    free(smi);
    return NULL;
}

/** get the Nth status manager instance */
sm_instance_t * get_sm_instance(unsigned int n)
{
    if (n >= sm_inst_count)
        return NULL;
    else
        return sm_inst[n];
}

/** get the constant string that match the input string */
const char *get_status_str(const status_manager_t *sm, const char *in_str)
{
    int i;

    for (i = 0; i < sm->status_count; i++)
    {
        if (!strcmp(sm->status_enum[i], in_str))
            return sm->status_enum[i];
    }
    /* not found */
    return NULL;
}

/** return the list of allowed status for a status manager */
char *allowed_status_str(const status_manager_t *sm, char *buf, int sz)
{
    int i;
    char *cur = buf;
    buf[0] = '\0';

    rh_strncpy(cur, "\"\"(empty)", sz);
    cur += strlen(cur);

    for (i = 0; i < sm->status_count; i++)
    {
        if (!EMPTY_STRING(buf))
        {
            *cur = ',';
            cur++;
        }
        rh_strncpy(cur, sm->status_enum[i], sz - (ptrdiff_t)(cur - buf));
        cur += strlen(cur);
    }
    return buf;
}
