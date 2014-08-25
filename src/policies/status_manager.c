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

/* -------------- Basic status manager implementation ------------------- */

#define BASIC_ST_COUNT 2
static const  char* basic_status_list[] = {"ok", "failed"}; /* + not set */

/* -------------- Lustre/HSM status manager implementation -------------- */

#ifdef _LUSTRE_HSM

/* tag for logs */
#define LHSM_TAG "lhsm"

#define LHSM_ST_COUNT   7
/* XXX /!\ Must match hsm_status_t order */
static const  char* lhsm_status_list[] = {"new","modified","retrieving","archiving",
                                          "synchro","released","release_pending"};

static const char *hsm_status2str(hsm_status_t st)
{
    if ((st >= STATUS_COUNT) || (st == STATUS_UNKNOWN))
        return NULL;
    else
        return lhsm_status_list[st-1]; /* st=1 => new */
}

/** get the HSM status of an entry */
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

/** helper to set the LHSM status in attribute structure */
static inline void set_lhsm_status(struct sm_instance *smi, attr_set_t *attrs, hsm_status_t status)
{
    /* new file, status is known */
    sm_status_ensure_alloc(&attrs->attr_values.sm_status);
    STATUS_ATTR(attrs, smi->smi_index) = hsm_status2str(status);
    ATTR_MASK_STATUS_SET(attrs, smi->smi_index);
}

/** helper to compare a LHSM status */
static bool status_equal(struct sm_instance *smi, const attr_set_t *attrs, hsm_status_t status)
{
    return !strcmp(STATUS_ATTR(attrs, smi->smi_index), hsm_status2str(status));
}

/** changelog callback */
static int lhsm_cl_cb(struct sm_instance *smi, const CL_REC_TYPE *logrec,
                      const entry_id_t *id, const attr_set_t *attrs,
                       attr_set_t *refreshed_attrs, bool *getit)
{
    /* If this is a CREATE record, we know its status is NEW
     * (except if it is already set to another value) */
    if (logrec->cr_type == CL_CREATE)
    {
        if (!ATTR_MASK_STATUS_TEST(attrs, smi->smi_index))
        {
            /* new file, status is new */
            set_lhsm_status(smi, refreshed_attrs, STATUS_NEW);
            /* no need to retrieve it from filesystem */
            *getit = false;
        }
        /* else: file is already known. Preserve the known status. */

        /* FIXME RBHv3 manage no_archive, no_release, last_archive, last_restore */
    }
    else if ((logrec->cr_type == CL_MKDIR) || (logrec->cr_type == CL_RMDIR))
    {
        /* no status for directories */
        *getit = false;
    }
    else if (logrec->cr_type == CL_HSM)
    {
        switch (hsm_get_cl_event(logrec->cr_flags))
        {
            case HE_ARCHIVE:
                /* is it a successfull copy? */
                if (hsm_get_cl_error(logrec->cr_flags) == CLF_HSM_SUCCESS)
                {
                    /** @TODO RBHv3 remember last archive time */

                    /* if dirty flag is set in the changelog record, the entry is dirty,
                     * else, it is up to date. */
                    set_lhsm_status(smi, refreshed_attrs,
                        (hsm_get_cl_flags(logrec->cr_flags) & CLF_HSM_DIRTY) ?
                        STATUS_MODIFIED : STATUS_SYNCHRO);
                    *getit = false;
                }
                else /* archive failed */
                {
                    /* Entry is probably still dirty. If dirty flag is not set,
                     * we need to ask the actual status */
                    if (hsm_get_cl_flags(logrec->cr_flags) & CLF_HSM_DIRTY)
                    {
                        set_lhsm_status(smi, refreshed_attrs, STATUS_MODIFIED);
                        *getit = false;
                    }
                    else /* archive failed but entry is not dirty?
                          * retrieve the status from filesystem */
                        *getit = true;
                }
                break;

            case HE_RESTORE:
                if (hsm_get_cl_error(logrec->cr_flags) == CLF_HSM_SUCCESS)
                {
                    /** @TODO RBHv3 remember last restore time */

                    /* status is 'up-to-date' after a successful restore */
                    set_lhsm_status(smi, refreshed_attrs, STATUS_SYNCHRO);
                    *getit = false;
                }
                else /* failed restore */
                {
                    /* Entry status remains 'released' */
                    set_lhsm_status(smi, refreshed_attrs, STATUS_RELEASED);
                    *getit = false;
                }
                break;

            case HE_RELEASE:
                if (hsm_get_cl_error(logrec->cr_flags) != CLF_HSM_SUCCESS)
                {
                    /* release records are not expected to be erroneous */
                    DisplayLog(LVL_CRIT, LHSM_TAG, "ERROR: "
                         "Unexpected HSM release event with error %d",
                         hsm_get_cl_error(logrec->cr_flags));
                    /* make sure of actual entry status */
                    *getit = true;
                }
                else /* successful release */
                {
                    set_lhsm_status(smi, refreshed_attrs, STATUS_RELEASED);
                    *getit = false;
                }
                break;

            case HE_STATE:
                /* state changed: did it become dirty? */
                if (hsm_get_cl_flags(logrec->cr_flags) & CLF_HSM_DIRTY)
                {
                    set_lhsm_status(smi, refreshed_attrs, STATUS_MODIFIED);
                    *getit = false;
                }
                else /* other status change: need to get it */
                    *getit = true;

                break;

            case HE_REMOVE:
            case HE_CANCEL:
                /* undetermined status after such an event */
                *getit = true;
                break;

            default:
                DisplayLog(LVL_CRIT, LHSM_TAG, "ERROR: unknown HSM event:"
                            "bitfield=%#x, event=%u", logrec->cr_flags,
                            hsm_get_cl_event(logrec->cr_flags));
                /* skip */
                return EINVAL;
        }
    }
    else if (logrec->cr_type == CL_MTIME || logrec->cr_type == CL_TRUNC ||
              (logrec->cr_type == CL_CLOSE))
    {
        /* If file is modified or truncated, need to check its status
         * (probably modified) EXCEPT if its status is already 'modified' */
        if (!ATTR_MASK_STATUS_TEST(attrs, smi->smi_index)
            || (!status_equal(smi, attrs, STATUS_NEW) &&
                !status_equal(smi, attrs, STATUS_MODIFIED)))
        {
            DisplayLog(LVL_DEBUG, LHSM_TAG,
                       "Getstatus needed because this is a %s event "
                       "and status is not already 'modified' or 'new': status=%s",
                       changelog_type2str(logrec->cr_type),
                       ATTR_MASK_STATUS_TEST(attrs, smi->smi_index)?
                         STATUS_ATTR(attrs, smi->smi_index) :"<not set>");
            *getit = true;
        }
    }
    /* other records: keep default value for status need */
    return 0;
}

/** @TODO to be managed in pipeline or in changelog callback: */
#if 0

/** Indicate if an entry is concerned by soft remove mecanism */
static inline bool soft_remove_filter(struct entry_proc_op_t *p_op)
{
    if (ATTR_FSorDB_TEST(p_op, type)
        && !strcmp(ATTR_FSorDB(p_op, type), STR_TYPE_DIR))
    {
        DisplayLog(LVL_FULL, ENTRYPROC_TAG, "Removing directory entry (no rm in backend)");
        return false;
    }
    else if (ATTR_FSorDB_TEST(p_op, status)
        && (ATTR_FSorDB(p_op, status) == STATUS_NEW))
    {
        DisplayLog(LVL_DEBUG, ENTRYPROC_TAG, "Removing 'new' entry ("DFID"): no remove in backend",
                    PFID(&p_op->entry_id));
        return false;
    }
    return true;
}

                if (logrec->cr_flags & CLF_UNLINK_HSM_EXISTS)
                    /* if CLF_UNLINK_HSM_EXISTS is set, we must clean something in the backend */
                    p_op->db_op_type = OP_TYPE_SOFT_REMOVE;
                else if (p_op->db_exists)
                    /* nothing in the backend, just clean the entry in DB */
                    p_op->db_op_type = OP_TYPE_REMOVE_LAST;
                else
                    /* ignore the record */
                    return STAGE_CHGLOG_CLR;
#endif
#endif /* _LUSTRE_HSM */

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



/* -------------- managing status managers ---------- */

static status_manager_t status_mgrs[] = {
    /* this policy needs the ols status to process changelog callbacks.
     * As we don't know the actual index of the status manager instance (smi)
     * we set it to SMI_MASK(0). It must be translated later by accessors.
     */
    {"lhsm", SM_SHARED, lhsm_status_list, LHSM_ST_COUNT,
     ATTR_MASK_type | SMI_MASK(0), 0, lhsm_status, lhsm_cl_cb},
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
static inline bool sm_loaded(const char *name, status_manager_t **sm_ptr)
{
    int i;
    for (i = 0; i < sm_count; i++)
    {
        if (!strcasecmp(sm_cache[i]->name, name))
        {
            *sm_ptr = sm_cache[i];
            return true;
        }
    }
    return false;
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
static inline bool sm_instance_exists(const char *name, sm_instance_t **smi_ptr)
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
        /* same as <policy name>\0 */
        smi->instance_name = malloc(strlen(pol_name)+1);
        if (smi->instance_name == NULL)
            goto free_smi;
        sprintf(smi->instance_name, "%s", pol_name);
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

int run_all_cl_cb(const CL_REC_TYPE *logrec, const entry_id_t *id,
                  const attr_set_t *attrs, attr_set_t *refreshed_attrs,
                  uint64_t *status_need)
{
    int rc, err_max = 0;
    int i = 0;
    sm_instance_t *smi;

    for (i = 0, smi = get_sm_instance(i); smi != NULL;
         i++, smi = get_sm_instance(i))
    {
        bool getstatus = false;

        if (smi->sm->changelog_cb == NULL)
            continue;

        rc = smi->sm->changelog_cb(smi, logrec, id, attrs, refreshed_attrs,
                                   &getstatus);
        if (err_max == 0 || rc > err_max)
            err_max = rc;

        if ((rc == 0) && getstatus)
        {
            *status_need |= SMI_MASK(i);
        }
    }
    return err_max;
}


