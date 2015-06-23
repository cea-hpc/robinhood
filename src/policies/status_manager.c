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
unsigned int sm_inst_count = 0; /* must be available from other modules to handle attribute masks */

/** list of status manager info */
struct _sm_attr_info *sm_attr_info;
int sm_attr_count;


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
        free((*p_tab)[i]); /* strdup -> free */

    MemFree(*p_tab);
    *p_tab = NULL;
}

int set_sm_info(sm_instance_t *smi, attr_set_t *pattrs,
                unsigned int attr_index, void *val)
{
    void **info;

     /* check allocation of sm_info array */
    sm_info_ensure_alloc(&pattrs->attr_values.sm_info);
    if (pattrs->attr_values.sm_info == NULL)
        return -ENOMEM;

    if (unlikely(smi_info_index(smi, attr_index) >= ALL_ATTR_COUNT))
        RBH_BUG("attr index out of range");

    info = &SMI_INFO(pattrs, smi, attr_index);

    if (*info != NULL)
    /* free the previous value */
        free(*info);

    *info = val;
    ATTR_MASK_INFO_SET(pattrs, smi, attr_index);

    return 0;
}

/** build a string with the list of statuses in the given mask */
char *name_status_mask(uint64_t mask, char *buf, int sz)
{
    int i = 0;
    uint64_t m;
    buf[0] = '\0';
    char *cur = buf;

    for (i = 0, m = (1LL << ATTR_COUNT); i < sm_inst_count; i++, m <<= 1)
    {
        if (mask & m)
        {
            sm_instance_t *smi = get_sm_instance(i);
            /* append smi name */
            if (!EMPTY_STRING(buf))
            {
                *cur = ',';
                cur++;
            }
            rh_strncpy(cur, smi->instance_name, sz - (ptrdiff_t)(cur - buf));
            cur += strlen(cur);
        }
    }
    return buf;
}

/** retrieve a status manager from its name */
sm_instance_t *smi_by_name(const char *smi_name)
{
    int i;

    for (i = 0; i < sm_inst_count; i++)
    {
        sm_instance_t *smi = sm_inst[i];

        if (!strcmp(smi->instance_name, smi_name))
            return smi;
    }
    /* not found */
    return NULL;
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
static const  char* basic_status_list[] = {"ok", "failed"}; /* + not set */

/* -------------- shook status manager implementation ---------- */

#ifdef HAVE_SHOOK
static int shook_cl_cb(struct sm_instance *smi, const CL_REC_TYPE *logrec,
                       const entry_id_t *id, const attr_set_t *attrs,
                       attr_set_t *refreshed_attrs, bool *getit)
{
    /** TODO RBHv3 implement the following stuff */
    if (logrec->cr_type == CL_XATTR)
    {
        /* need to update status */
        *getit = true;
    }
    else if ( logrec->cr_type == CL_MTIME || logrec->cr_type == CL_TRUNC ||
              (logrec->cr_type == CL_CLOSE))
    {
        /** @TODO do like Lustre/HSM:
         * if file is modified or truncated, need to check its status
         * (probably modified) EXCEPT if its status is already 'modified'
         */
    }
    else if (logrec->cr_type == CL_CTIME || (logrec->cr_type == CL_SETATTR))
    {
        /* in Lustre v2.O, changing trusted xattr generates CTIME/SATTR event */
        *getit = true;
    }


    if ( p_op->db_exists )
    {
        /* if the old name is a restripe file, update the status */
        if (!strncmp(RESTRIPE_TGT_PREFIX, ATTR( &p_op->db_attrs, name ),
                     strlen(RESTRIPE_TGT_PREFIX)))
        {
            p_op->fs_attr_need |= ATTR_MASK_status;
            DisplayLog( LVL_DEBUG, ENTRYPROC_TAG,
                        "Getstatus needed because entry was a restripe target");
        }
    }

    return 0;
}
#endif /* HAVE_SHOOK */

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

    for (i = 0; i < sm_inst_count; i++)
    {
        if (!strcasecmp(sm_inst[i]->sm->name, name))
        {
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
static uint64_t actual_mask(sm_instance_t *smi, uint64_t mask)
{
    uint64_t       gen_bits;
    uint64_t       gen_status;

    /* generic attribute mask */
    gen_bits = mask & bit_range(GENERIC_INFO_OFFSET, smi->sm->nb_info);
    /* generic status mask */
    gen_status = mask & SMI_MASK(0);

    /* clean generic bits */
    mask &= ~gen_bits;
    mask &= ~gen_status;

    /* replace with real bits */
    if (gen_bits)
        /* shift gen_bits by real offset - GENERIC_INFO_OFFSET */
        mask |= (gen_bits << (smi_info_index(smi, 0) - GENERIC_INFO_OFFSET));

    if (gen_status)
        mask |= SMI_MASK(smi->smi_index);

    return mask;
}

/** translate a generic mask SMI_MASK(0) and GENERIC_INFO_OFFSET to all status and info masks */
uint64_t translate_all_status_mask(uint64_t mask)
{
    uint64_t       gen_bits;
    uint64_t       gen_status;

    /* generic attribute mask */
    gen_bits = mask & bit_range(GENERIC_INFO_OFFSET, sm_attr_count);
    /* generic status mask */
    gen_status = mask & SMI_MASK(0);

    /* clean generic bits */
    mask &= ~gen_bits;
    mask &= ~gen_status;

    /* replace with real bits */
    if (gen_bits)
        mask |= all_sm_info_mask();

    if (gen_status)
        mask |= all_status_mask();

    return mask;
}



/** create a status manager instance (if it does not already exist) */
sm_instance_t *create_sm_instance(const char *pol_name,const char *sm_name)
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
    else /* private status manager (1 instance per policy) */
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

    /* add it the the list of SMIs */
    sm_inst_count++;
    sm_inst = realloc(sm_inst, sm_inst_count * sizeof(sm_instance_t*));
    if (sm_inst == NULL)
        goto out_free;
    sm_inst[sm_inst_count-1] = smi;

    /* the attribute mask cannot handle more that 64 attributes */
    if (smi_info_index(smi, sm->nb_info - 1) >= 64)
    {
        DisplayLog(LVL_CRIT, "smi_create", "Too many policy-specific attributes (attribute mask is 64bits)");
        goto out_free;
    }

    sm_attr_count += sm->nb_info;
    sm_attr_info = realloc(sm_attr_info, sm_attr_count * sizeof(struct _sm_attr_info));
    if (sm_attr_info == NULL)
        goto out_free;

    /* <instance_name>_<attr_name> */
    for (i = 0; i < sm->nb_info; i++)
    {
        int tgt_idx = sm_attr_count - sm->nb_info + i;

        asprintf((char **)&sm_attr_info[tgt_idx].db_attr_name, "%s_%s", smi->instance_name,
                 smi->sm->info_types[i].db_name);
        asprintf((char **)&sm_attr_info[tgt_idx].user_attr_name, "%s.%s", smi->instance_name,
                 smi->sm->info_types[i].user_name);
        sm_attr_info[tgt_idx].def = &smi->sm->info_types[i];
        sm_attr_info[tgt_idx].smi = smi;
    }

    return smi;

out_free:
    if (smi)
    {
        free(smi->user_name);
        free(smi->db_field);
        free(smi->instance_name);
        free(smi);
    }
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

    if (in_str == NULL || EMPTY_STRING(in_str))
        return NULL;

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

#ifdef HAVE_CHANGELOGS
int run_all_cl_cb(const CL_REC_TYPE *logrec, const entry_id_t *id,
                  const attr_set_t *attrs, attr_set_t *refreshed_attrs,
                  uint64_t *status_need, uint64_t status_mask,
                  proc_action_e *rec_action)
{
    int rc, err_max = 0;
    int i = 0;
    sm_instance_t *smi;

    *rec_action = PROC_ACT_NONE;

    for (i = 0, smi = get_sm_instance(i); smi != NULL;
         i++, smi = get_sm_instance(i))
    {
        bool getstatus = false;
        proc_action_e curr_action = PROC_ACT_NONE;

        if (smi->sm->changelog_cb == NULL)
            continue;

        /* entry not in policy scope */
        if ((SMI_MASK(i) & status_mask) == 0)
            continue;

        rc = smi->sm->changelog_cb(smi, logrec, id, attrs, refreshed_attrs,
                                   &getstatus, &curr_action);
        if (err_max == 0 || rc > err_max)
            err_max = rc;

        if (rc == 0)
        {
            if (getstatus)
                *status_need |= SMI_MASK(i);

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
    int            i = 0;
    proc_action_e  pa = PROC_ACT_RM_ALL; /* default is rm */
    sm_instance_t *smi;

    while ((smi = get_sm_instance(i)) != NULL)
    {
        if (smi_manage_deleted(smi) && smi->sm->softrm_filter_func != NULL)
        {
            proc_action_e  curr_pa;

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
         i++, smi = get_sm_instance(i))
    {
        /* now that all smi are loaded sm_inst_count is known.
         * so we can compute the real attribute masks */
        smi->status_mask_fresh = actual_mask(smi, smi->sm->status_needs_attrs_fresh);
        smi->status_mask_cached = actual_mask(smi, smi->sm->status_needs_attrs_cached);

        if (smi->sm->flags & SM_DELETED)
        {
            smi->softrm_table_mask = actual_mask(smi, smi->sm->softrm_table_mask);
            smi->softrm_filter_mask = actual_mask(smi, smi->sm->softrm_filter_mask);
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
         i++, smi = get_sm_instance(i))
    {
        if (smi->sm->init_func == NULL)
            continue;

        rc = smi->sm->init_func(smi, flags);
        if (rc != 0)
        {
            DisplayLog(LVL_CRIT, INIT_TAG, "Failed to initialize status manager %s: error=%d",
                       smi->instance_name, rc);
            return rc;
        }
        else
            DisplayLog(LVL_VERB, INIT_TAG, "Status manager %s successfully initialized",
                       smi->instance_name);
    }

    return 0;
}

static void *smi_cfg_new(void)
{
    void **smi_cfg_tab;
    sm_instance_t *smi;
    int i;

    smi_cfg_tab = calloc(sm_inst_count, sizeof(void*));
    if (smi_cfg_tab == NULL)
        return NULL;

    for (i = 0, smi = get_sm_instance(i); smi != NULL;
         i++, smi = get_sm_instance(i))
    {
        if (smi->sm->cfg_funcs == NULL
            || smi->sm->cfg_funcs->new == NULL)
        {
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
    while (i != 0)
    {
        i--;
        smi = get_sm_instance(i);
        if (smi->sm->cfg_funcs == NULL
            || smi->sm->cfg_funcs->free == NULL)
        {
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
         i++, smi = get_sm_instance(i))
    {
        if (smi->sm->cfg_funcs != NULL
            && smi->sm->cfg_funcs->free != NULL)
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
         i++, smi = get_sm_instance(i))
    {
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
         i++, smi = get_sm_instance(i))
    {
        if (smi->sm->cfg_funcs == NULL
            || smi->sm->cfg_funcs->read == NULL)
            continue;

        DisplayLog(LVL_DEBUG, "smi_cfg", "Loading status manager '%s' config", smi->instance_name);

        rc = smi->sm->cfg_funcs->read(config, smi_cfg_tab[i], msg_out);
        if (rc != 0)
            return rc;
    }
    return 0;
}

static int smi_cfg_set(void *cfg,  bool reload)
{
    void **smi_cfg_tab = cfg;
    sm_instance_t *smi;
    int i, rc;

    for (i = 0, smi = get_sm_instance(i); smi != NULL;
         i++, smi = get_sm_instance(i))
    {
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
         i++, smi = get_sm_instance(i))
    {
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
         i++, smi = get_sm_instance(i))
    {
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
