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
#include "rbh_logs.h"
#include "rbh_modules.h"
#include "Memory.h"

/** list of status manager instances */
static sm_instance_t **sm_inst = NULL;
unsigned int sm_inst_count = 0; /* must be available from other modules
                                 * to handle attribute masks */

/** list of status manager info */
struct _sm_attr_info *sm_attr_info = NULL;
unsigned int sm_attr_count;

void sm_status_ensure_alloc(char const ***p_tab)
{
    if (*p_tab != NULL || sm_inst_count == 0)
        return;

    *p_tab = MemCalloc(sm_inst_count, sizeof(char *));
}

void sm_status_free(char const ***p_tab)
{
    if (*p_tab == NULL)
        return;

    MemFree(*p_tab);
    *p_tab = NULL;
}

/** allocate sm_info array */
void sm_info_ensure_alloc(void ***p_tab)
{
    if (*p_tab != NULL || sm_attr_count == 0)
        return;

    *p_tab = MemCalloc(sm_attr_count, sizeof(void *));
}

/** free info array */
void sm_info_free(void ***p_tab)
{
    int i;

    if (*p_tab == NULL || sm_attr_count == 0)
        return;

    for (i = 0; i < sm_attr_count; i++)
        free((*p_tab)[i]);  /* strdup -> free */

    MemFree(*p_tab);
    *p_tab = NULL;
}

int set_sm_info(const sm_instance_t *smi, attr_set_t *pattrs,
                unsigned int attr_index, void *val)
{
    void **info;

    /* check allocation of sm_info array */
    sm_info_ensure_alloc(&pattrs->attr_values.sm_info);
    if (pattrs->attr_values.sm_info == NULL)
        return -ENOMEM;

    assert(attr2sminfo_index(smi_info_index(smi, attr_index))
           < sizeof(pattrs->attr_mask.sm_info) * CHAR_BIT);

    info = &SMI_INFO(pattrs, smi, attr_index);

    if (*info != NULL)
        /* free the previous value */
        free(*info);

    *info = val;
    ATTR_MASK_INFO_SET(pattrs, smi, attr_index);

    return 0;
}

/** build a string with the list of statuses in the given mask */
char *name_status_mask(uint32_t status_mask, char *buf, int sz)
{
    int i = 0;
    uint32_t m = 1;
    buf[0] = '\0';
    char *cur = buf;

    for (i = 0; i < sm_inst_count; i++, m <<= 1) {
        if (status_mask & m) {
            sm_instance_t *smi = get_sm_instance(i);
            /* append smi name */
            if (!EMPTY_STRING(buf)) {
                *cur = ',';
                cur++;
            }
            rh_strncpy(cur, smi->instance_name, sz - (ptrdiff_t) (cur - buf));
            cur += strlen(cur);
        }
    }
    return buf;
}

/** retrieve a status manager from its name */
sm_instance_t *smi_by_name(const char *smi_name)
{
    int i;

    for (i = 0; i < sm_inst_count; i++) {
        sm_instance_t *smi = sm_inst[i];

        if (!strcmp(smi->instance_name, smi_name))
            return smi;
    }
    /* not found */
    return NULL;
}

static const sm_info_def_t status_def = {
    .user_name = "status",
    .db_name = "status",
    .db_type = DB_TEXT, /* not used? */
    .db_type_size = 0,
    .crit_type = PT_STRING,
};

/** helper for sm_attr_get. Assume smi is set. */
static int get_smi_attr(const sm_instance_t *smi, const attr_set_t *p_attrs,
                        const char *attr_name, void **val,
                        const sm_info_def_t **ppdef, unsigned int *attr_index)
{
    int i;

    assert(smi != NULL);

    if (!strcasecmp(attr_name, "status")) {
        *ppdef = &status_def;

        *attr_index = smi_status_index(smi);
        if (val == NULL)
            /* caller doesn't care about the value, it just want to know if
             * the attribute exists (+ the attribute index)*/
            return 0;

        /* XXX NULL or empty string? */
        if (!ATTR_MASK_STATUS_TEST(p_attrs, smi->smi_index))
            return -ENODATA;
        if (p_attrs->attr_values.sm_status == NULL)
            return -ENODATA;

        *val = (char *)STATUS_ATTR(p_attrs, smi->smi_index);
        return *val != NULL ? 0 : -ENODATA;
    }

    /* other attrs */
    for (i = 0; i < smi->sm->nb_info; i++) {
        if (!strcasecmp(attr_name, smi->sm->info_types[i].user_name)) {
            *ppdef = &smi->sm->info_types[i];
            *attr_index = smi_info_index(smi, i);

            if (val == NULL)
                /* caller doesn't care about the value, it just want to know if
                 * the attribute exists (and the smi info index) */
                return 0;

            if (!ATTR_MASK_INFO_TEST(p_attrs, smi, i))
                return -ENODATA;
            if (p_attrs->attr_values.sm_info == NULL)
                return -ENODATA;

            *val = SMI_INFO(p_attrs, smi, i);
            return *val != NULL ? 0 : -ENODATA;
        }
    }
    return -EINVAL;
}

/* -EINVAL: invalid argument.
 * -ENODATA: attribute exists, but is missing.
 */
int sm_attr_get(const sm_instance_t *smi, const attr_set_t *p_attrs,
                const char *name, void **val, const sm_info_def_t **ppdef,
                unsigned int *attr_index)
{
    const char *dot = strchr(name, '.');

    /* if there is no smi in context, and no dot is found:
     * nothing can't match */
    if (!dot && !smi)
        return -EINVAL;

    if (dot) {
        char *smi_name = strndup(name, (ptrdiff_t) dot - (ptrdiff_t) name);
        sm_instance_t *smi2;

        /* get the status manager with the given name */
        smi2 = smi_by_name(smi_name);
        if (smi2 == NULL) {
            DisplayLog(LVL_CRIT, __func__,
                       "ERROR: unknown status manager '%s' in parameter '%s'",
                       smi_name, name);
            free(smi_name);
            return -EINVAL;
        }
        free(smi_name);

        return get_smi_attr(smi2, p_attrs, dot + 1, val, ppdef, attr_index);
    } else {
        return get_smi_attr(smi, p_attrs, name, val, ppdef, attr_index);
    }
}

/* contents of status_manager:
name, flags, status_enum, status_count, status_needs_attrs_cached,
status_needs_attrs_fresh, get_status_func, changelog_cb.

For status manager that handle removed entries the 2 masks are:
    - to determine if the entry is to be saved in softrm table
    - fields to save in softrm table (needed for undelete or recovery)
*/

/* -------------- Basic status manager implementation ------------------- */

#define BASIC_ST_COUNT 2
static const char *basic_status_list[] = { "ok", "failed" };    /* + not set */

static status_manager_t basic_sm = {
    .name = "basic",
    .status_enum = basic_status_list,
    .status_count = BASIC_ST_COUNT
};

/* -------------- managing status managers ---------- */

static status_manager_t *load_status_manager(const char *name)
{
    if (strcasecmp(name, "basic") == 0)
        return &basic_sm;

    return module_get_status_manager(name);
}

/** check if an instance of shared status manager exists */
static bool sm_instance_exists(const char *name, sm_instance_t **smi_ptr)
{
    int i;

    for (i = 0; i < sm_inst_count; i++) {
        if (!strcasecmp(sm_inst[i]->sm->name, name)) {
            *smi_ptr = sm_inst[i];
            return true;
        }
    }
    return false;
}

/**
 * As status managers don't know their index instance by advance,
 * they provide generic masks as if there were only their own status and
 * attributes.
 * This function translates generic masks to the actual ones.
 */
static attr_mask_t actual_mask(sm_instance_t *smi, attr_mask_t mask)
{
    uint64_t gen_info;
    uint32_t gen_status;

    /* generic attribute mask */
    gen_info = mask.sm_info & bit_range(GENERIC_INFO_OFFSET, smi->sm->nb_info);
    /* generic status mask */
    gen_status = mask.status & SMI_MASK(0);

    /* clean generic info */
    mask.sm_info &= ~gen_info;
    mask.status &= ~gen_status;

    /* replace with real info */
    if (gen_info)
        /* shift gen_info by real offset - GENERIC_INFO_OFFSET */
        mask.sm_info |= (gen_info << smi->sm_info_offset);

    if (gen_status)
        mask.status |= SMI_MASK(smi->smi_index);

    return mask;
}

/** translate a generic mask SMI_MASK(0) and GENERIC_INFO_OFFSET to all status
 *  and info masks */
attr_mask_t translate_all_status_mask(attr_mask_t mask)
{
    uint64_t gen_info;
    uint32_t gen_status;

    /* generic status mask */
    gen_status = mask.status & SMI_MASK(0);
    /* generic attribute mask */
    gen_info = mask.sm_info & bit_range(GENERIC_INFO_OFFSET, sm_attr_count);

    /* clean generic bits */
    mask.status &= ~gen_status;
    mask.sm_info &= ~gen_info;

    /* replace with real bits */
    if (gen_info)
        mask.sm_info |= all_sm_info_mask();

    if (gen_status)
        mask.status |= all_status_mask();

    return mask;
}

/** create a status manager instance (if it does not already exist) */
sm_instance_t *create_sm_instance(const char *pol_name, const char *sm_name)
{
    const status_manager_t *sm = load_status_manager(sm_name);
    sm_instance_t *smi = NULL;
    int i;

    /* load_status_manager() checks that the status manager exists and load it
     * if necessary. NULL means that it really isn't available. */
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

    /* compute the offset of specific policy info in attribute structure */
    if (sm_inst_count == 0)
        smi->sm_info_offset = 0;
    else
        /* offset of smi info: previous attr count */
        smi->sm_info_offset = sm_attr_count;

    if (sm->flags & SM_SHARED)
        /* If the status manager is shared between policies,
         * it just consists of the status manager name. */
        smi->instance_name = strdup(sm->name);
    else    /* private status manager (1 instance per policy) */
        /* same as <policy name>\0 */
        smi->instance_name = strdup(pol_name);

    if (smi->instance_name == NULL)
        goto out_free;

    /* <instance_name>_status */
    asprintf(&smi->db_field, "%s_status", smi->instance_name);
    if (smi->db_field == NULL)
        goto out_free;

    asprintf(&smi->user_name, "%s.status", smi->instance_name);
    if (smi->user_name == NULL)
        goto out_free;

    /* @TODO load its configuration */
    /* @TODO initialize it */

    /* check it fits into the status mask */
    if (sm_inst_count + 1 >= member_size(attr_mask_t, status) * CHAR_BIT) {
        DisplayLog(LVL_CRIT, "smi_create",
                   "Too many status managers: max %lu supported",
                   member_size(attr_mask_t, status) * CHAR_BIT);
        goto out_free;
    }

    /* check it fits into the sm_info mask */
    if (sm_attr_count + sm->nb_info >=
        member_size(attr_mask_t, sm_info) * CHAR_BIT) {
        DisplayLog(LVL_CRIT, "smi_create",
                   "Too many policy-specific attributes: max %lu supported",
                   member_size(attr_mask_t, sm_info) * CHAR_BIT);
        goto out_free;
    }

    /* add it the the list of SMIs */
    sm_inst_count++;
    sm_inst = realloc(sm_inst, sm_inst_count * sizeof(sm_instance_t *));
    if (sm_inst == NULL)
        goto out_free;
    sm_inst[sm_inst_count - 1] = smi;

    /* register sm specific info */
    sm_attr_count += sm->nb_info;
    sm_attr_info =
        realloc(sm_attr_info, sm_attr_count * sizeof(struct _sm_attr_info));
    if (sm_attr_info == NULL)
        goto out_free;

    /* <instance_name>_<attr_name> */
    for (i = 0; i < sm->nb_info; i++) {
        int tgt_idx = sm_attr_count - sm->nb_info + i;

        asprintf((char **)&sm_attr_info[tgt_idx].db_attr_name, "%s_%s",
                 smi->instance_name, smi->sm->info_types[i].db_name);
        asprintf((char **)&sm_attr_info[tgt_idx].user_attr_name, "%s.%s",
                 smi->instance_name, smi->sm->info_types[i].user_name);
        sm_attr_info[tgt_idx].def = &smi->sm->info_types[i];
        sm_attr_info[tgt_idx].smi = smi;
    }

    return smi;

 out_free:
    if (smi) {
        free(smi->user_name);
        free(smi->db_field);
        free(smi->instance_name);
        free(smi);
    }
    return NULL;
}

/** get the Nth status manager instance */
sm_instance_t *get_sm_instance(unsigned int n)
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

    if (in_str == NULL || EMPTY_STRING(in_str))
        return NULL;

    for (i = 0; i < sm->status_count; i++) {
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

    for (i = 0; i < sm->status_count; i++) {
        if (!EMPTY_STRING(buf)) {
            *cur = ',';
            cur++;
        }
        rh_strncpy(cur, sm->status_enum[i], sz - (ptrdiff_t) (cur - buf));
        cur += strlen(cur);
    }
    return buf;
}

#ifdef HAVE_CHANGELOGS
int run_all_cl_cb(const CL_REC_TYPE *logrec, const entry_id_t *id,
                  const attr_set_t *attrs, attr_set_t *refreshed_attrs,
                  attr_mask_t *status_need, uint32_t status_mask,
                  proc_action_e *rec_action)
{
    int rc, err_max = 0;
    int i = 0;
    sm_instance_t *smi;

    *rec_action = PROC_ACT_NONE;

    for (i = 0, smi = get_sm_instance(i); smi != NULL;
         i++, smi = get_sm_instance(i)) {
        bool getstatus = false;
        proc_action_e curr_action = PROC_ACT_NONE;

        if (smi->sm->changelog_cb == NULL)
            continue;

        /* entry not in policy scope */
        if ((SMI_MASK(i) & status_mask) == 0)
            continue;

        rc = smi->sm->changelog_cb(smi, logrec, id, attrs, refreshed_attrs,
                                   &getstatus, &curr_action);
        DisplayLog(LVL_DEBUG, __func__, "changelog callback for "
                   "status manager '%s' => rc=%d, action=%d",
                   smi->instance_name, rc, curr_action);

        if (err_max == 0 || rc > err_max)
            err_max = rc;

        if (rc == 0) {
            if (getstatus)
                status_need->status |= SMI_MASK(i);

            /* keep the action with the highest priority */
            if (curr_action > *rec_action)
                *rec_action = curr_action;
        }
    }
    return err_max;
}
#endif

/** When an entry is deleted, this function indicates what action is to be taken
 * by querying all status manager (remove from DB, move to softrm, ...)
 */
proc_action_e match_all_softrm_filters(const entry_id_t *id,
                                       const attr_set_t *attrs)
{
    int i = 0;
    proc_action_e pa = PROC_ACT_RM_ALL; /* default is rm */
    sm_instance_t *smi;

    while ((smi = get_sm_instance(i)) != NULL) {
        if (smi_manage_deleted(smi) && smi->sm->softrm_filter_func != NULL) {
            proc_action_e curr_pa;

            curr_pa = smi->sm->softrm_filter_func(smi, id, attrs);

            /* keep the action with the highest priority */
            if (curr_pa > pa)
                pa = curr_pa;
        }
        i++;
    }
    return pa;
}

/** set status and attribute masks of status manager instances,
 * once they are all loaded */
void smi_update_masks(void)
{
#define MASK_TAG "smi_masks"
    int i = 0;
    sm_instance_t *smi;

    for (i = 0, smi = get_sm_instance(i); smi != NULL;
         i++, smi = get_sm_instance(i)) {
        /* now that all smi are loaded sm_inst_count is known.
         * so we can compute the real attribute masks */
        smi->status_mask_fresh =
            actual_mask(smi, smi->sm->status_needs_attrs_fresh);
        smi->status_mask_cached =
            actual_mask(smi, smi->sm->status_needs_attrs_cached);

        if (smi->sm->flags & SM_DELETED) {
            smi->softrm_table_mask =
                actual_mask(smi, smi->sm->softrm_table_mask);
            smi->softrm_filter_mask =
                actual_mask(smi, smi->sm->softrm_filter_mask);
        }
    }
}

/** initialize all status managers having init function */
int smi_init_all(run_flags_t flags)
{
#define INIT_TAG "smi_init"
    int rc;
    int i = 0;
    sm_instance_t *smi;

    for (i = 0, smi = get_sm_instance(i); smi != NULL;
         i++, smi = get_sm_instance(i)) {
        if (smi->sm->init_func == NULL)
            continue;

        rc = smi->sm->init_func(smi, flags);
        if (rc != 0) {
            DisplayLog(LVL_CRIT, INIT_TAG,
                       "Failed to initialize status manager %s: error=%d",
                       smi->instance_name, rc);
            return rc;
        } else
            DisplayLog(LVL_VERB, INIT_TAG,
                       "Status manager %s successfully initialized",
                       smi->instance_name);
    }

    return 0;
}

static void *smi_cfg_new(void)
{
    void **smi_cfg_tab;
    sm_instance_t *smi;
    int i;

    smi_cfg_tab = calloc(sm_inst_count, sizeof(void *));
    if (smi_cfg_tab == NULL)
        return NULL;

    for (i = 0, smi = get_sm_instance(i); smi != NULL;
         i++, smi = get_sm_instance(i)) {
        if (smi->sm->cfg_funcs == NULL || smi->sm->cfg_funcs->new == NULL) {
            smi_cfg_tab[i] = NULL;
            continue;
        }

        smi_cfg_tab[i] = smi->sm->cfg_funcs->new();
        if (smi_cfg_tab[i] == NULL)
            goto reverse_free;
    }

    return smi_cfg_tab;

 reverse_free:
    /* allocation failed for last 'i' (do nothing if it was the first) */
    while (i != 0) {
        i--;
        smi = get_sm_instance(i);
        if (smi->sm->cfg_funcs == NULL || smi->sm->cfg_funcs->free == NULL) {
            smi_cfg_tab[i] = NULL;
            continue;
        }
        smi->sm->cfg_funcs->free(smi_cfg_tab[i]);
        smi_cfg_tab[i] = NULL;
    }
    free(smi_cfg_tab);

    return NULL;
}

static void smi_cfg_free(void *arg)
{
    void **smi_cfg_tab = arg;
    sm_instance_t *smi;
    int i;

    for (i = 0, smi = get_sm_instance(i); smi != NULL;
         i++, smi = get_sm_instance(i)) {
        if (smi->sm->cfg_funcs != NULL && smi->sm->cfg_funcs->free != NULL)
            smi->sm->cfg_funcs->free(smi_cfg_tab[i]);
        smi_cfg_tab[i] = NULL;
    }
    free(smi_cfg_tab);
}

static void smi_cfg_set_default(void *arg)
{
    void **smi_cfg_tab = arg;
    sm_instance_t *smi;
    int i;

    for (i = 0, smi = get_sm_instance(i); smi != NULL;
         i++, smi = get_sm_instance(i)) {
        if (smi->sm->cfg_funcs != NULL
            && smi->sm->cfg_funcs->set_default != NULL)
            smi->sm->cfg_funcs->set_default(smi_cfg_tab[i]);
    }
}

static int smi_cfg_read(config_file_t config, void *cfg, char *msg_out)
{
    void **smi_cfg_tab = cfg;
    sm_instance_t *smi;
    int i, rc;

    for (i = 0, smi = get_sm_instance(i); smi != NULL;
         i++, smi = get_sm_instance(i)) {
        if (smi->sm->cfg_funcs == NULL || smi->sm->cfg_funcs->read == NULL)
            continue;

        DisplayLog(LVL_DEBUG, "smi_cfg", "Loading status manager '%s' config",
                   smi->instance_name);

        rc = smi->sm->cfg_funcs->read(config, smi_cfg_tab[i], msg_out);
        if (rc != 0)
            return rc;
    }
    return 0;
}

static int smi_cfg_set(void *cfg, bool reload)
{
    void **smi_cfg_tab = cfg;
    sm_instance_t *smi;
    int i, rc;

    for (i = 0, smi = get_sm_instance(i); smi != NULL;
         i++, smi = get_sm_instance(i)) {
        if (smi->sm->cfg_funcs == NULL
            || smi->sm->cfg_funcs->set_config == NULL)
            continue;

        rc = smi->sm->cfg_funcs->set_config(smi_cfg_tab[i], reload);
        if (rc != 0)
            return rc;
    }
    return 0;
}

static void smi_cfg_write_default(FILE *f)
{
    sm_instance_t *smi;
    int i;

    for (i = 0, smi = get_sm_instance(i); smi != NULL;
         i++, smi = get_sm_instance(i)) {
        if (smi->sm->cfg_funcs != NULL
            && smi->sm->cfg_funcs->write_default != NULL)
            smi->sm->cfg_funcs->write_default(f);
    }
}

static void smi_cfg_write_template(FILE *f)
{
    sm_instance_t *smi;
    int i;

    for (i = 0, smi = get_sm_instance(i); smi != NULL;
         i++, smi = get_sm_instance(i)) {
        if (smi->sm->cfg_funcs != NULL
            && smi->sm->cfg_funcs->write_template != NULL)
            smi->sm->cfg_funcs->write_template(f);
    }
}

/** wraps config handlers for all status managers */
mod_cfg_funcs_t smi_cfg_hdlr = {
    .module_name = "status managers",
    .new = smi_cfg_new,
    .free = smi_cfg_free,
    .set_default = smi_cfg_set_default,
    .read = smi_cfg_read,
    .set_config = smi_cfg_set,
    .write_default = smi_cfg_write_default,
    .write_template = smi_cfg_write_template,
};
