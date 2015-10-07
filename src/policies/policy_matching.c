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
/**
 * functions for applying policies to entries
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "policy_rules.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "rbh_cfg.h"
#include "uidgidcache.h"
#include "xplatform_print.h"
#include "rbh_boolexpr.h"
#include "status_manager.h"

#include <string.h>
#include <libgen.h>
#include <fnmatch.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include <attr/xattr.h>

#define POLICY_TAG "Policy"

/* macros for displaying entries */
#ifdef _HAVE_FID
#define F_ENT_ID DFID
#define P_ENT_ID(_id, _attr)    PFID(_id)
#else
#define F_ENT_ID    "'%s'"
#define P_ENT_ID(_id, _attr)    ATTR(_attr, fullpath)
#endif


//#define _DEBUG_POLICIES 1


static char   *ExtractParentDir(const char *file_path, char *out_buff)
{

    char           buff[RBH_PATH_MAX];
    char          *dir;

    rh_strncpy(buff, file_path, RBH_PATH_MAX);

    dir = dirname(buff);

    strcpy(out_buff, dir);

    return out_buff;
}


static int TestRegexp(const char *regexp, const char *to_be_tested)
{
    return !fnmatch(regexp, to_be_tested, 0);
}

/* flags for calling TestPathRegexp */
#define PATHREGEXP_IS_CHILD    0x00000001
#define PATHREGEXP_ANY_LEVEL   0x00000002

static bool TestPathRegexp(const char *regexp, const char *to_be_tested,
                           int flags)
{
    char full_path[RBH_PATH_MAX];
    const char * full_regexp = regexp;

    bool any_level = (flags & PATHREGEXP_ANY_LEVEL);
    bool is_child =  (flags & PATHREGEXP_IS_CHILD);

    /* is the regexp relative ?
     * (don't add the root path if expression starts with '**').
     */
    if (!IS_ABSOLUTE_PATH(regexp) && !(any_level && (regexp[0]=='*')))
    {
        /* add root path to the path to be tested */
        sprintf(full_path, "%s/%s", global_config.fs_path, regexp);
        full_regexp = full_path;
    }

    if (any_level)
    {
        return !fnmatch(full_regexp, to_be_tested,
                        (is_child?FNM_LEADING_DIR:0));
    }

    if (!fnmatch(full_regexp, to_be_tested,
                   FNM_PATHNAME | (is_child ? FNM_LEADING_DIR : 0)))
    {
#ifdef _DEBUG_POLICIES
        printf("MATCH regexp='%s', path='%s', tree = %d\n", full_regexp,
               to_be_tested, is_child);
#endif
        return true;
    }
#ifdef _DEBUG_POLICIES
    printf("NO MATCH regexp='%s', path='%s', tree = %d\n", full_regexp,
           to_be_tested, is_child);
#endif

    return false;
}                               /* TestRegexp */


static inline int size_compare(unsigned long long size1, compare_direction_t comp,
                                unsigned long long size2)
{
    switch (comp)
    {
    case COMP_GRTHAN:
        return (size1 > size2);
    case COMP_GRTHAN_EQ:
        return (size1 >= size2);
    case COMP_LSTHAN:
        return (size1 < size2);
    case COMP_LSTHAN_EQ:
        return (size1 <= size2);
    case COMP_EQUAL:
        return (size1 == size2);
    case COMP_DIFF:
        return (size1 != size2);
    default:
        DisplayLog(LVL_CRIT, POLICY_TAG, "Invalid comparator for size (%d)", comp);
        return 0;
    }
}

static inline int int_compare(int int1, compare_direction_t comp, int int2)
{
    switch (comp)
    {
    case COMP_GRTHAN:
        return (int1 > int2);
    case COMP_GRTHAN_EQ:
        return (int1 >= int2);
    case COMP_LSTHAN:
        return (int1 < int2);
    case COMP_LSTHAN_EQ:
        return (int1 <= int2);
    case COMP_EQUAL:
        return (int1 == int2);
    case COMP_DIFF:
        return (int1 != int2);
    default:
        DisplayLog(LVL_CRIT, POLICY_TAG, "Invalid comparator for int (%d)", comp);
        return 0;
    }
}

#define BOOL2POLICY(_rc_) ((_rc_)?POLICY_MATCH:POLICY_NO_MATCH)

static inline int negate_match(int rc)
{
        if (rc == POLICY_MATCH)
            return POLICY_NO_MATCH;
        else if (rc == POLICY_NO_MATCH)
            return POLICY_MATCH;
        else
            return rc;
}


#define CHECK_ATTR(_pset_, _attr_, _no_trace) do {                                    \
                                           if (!ATTR_MASK_TEST(_pset_, _attr_))       \
                                           {                                            \
                                               if (!(_no_trace))                        \
                                                   DisplayLog(LVL_MAJOR, POLICY_TAG,   \
                                                       "Missing attribute '%s' for evaluating boolean expression on " \
                                                        DFID, (#_attr_), PFID(p_entry_id)); \
                                               return POLICY_MISSING_ATTR;              \
                                           }                                            \
                                     } while (0)

static filter_comparator_t Policy2FilterComparator(compare_direction_t comp)
{
    switch(comp)
    {
        case COMP_GRTHAN: return MORETHAN_STRICT;
        case COMP_GRTHAN_EQ: return MORETHAN;
        case COMP_LSTHAN: return LESSTHAN_STRICT;
        case COMP_LSTHAN_EQ: return LESSTHAN;
        case COMP_EQUAL: return EQUAL;
        case COMP_DIFF: return NOTEQUAL;
        case COMP_LIKE: return LIKE;
        case COMP_UNLIKE: return UNLIKE;

        default:
            /* Error */
            DisplayLog(LVL_CRIT, POLICY_TAG, "ERROR: unknown comparator %d", comp);
            return (filter_comparator_t) -1;
    }
}

/* return the opposite direction */
static compare_direction_t oppose_compare(compare_direction_t comp)
{
    switch(comp)
    {
        case COMP_GRTHAN: return COMP_LSTHAN;
        case COMP_GRTHAN_EQ: return COMP_LSTHAN_EQ;
        case COMP_LSTHAN: return COMP_GRTHAN;
        case COMP_LSTHAN_EQ: return COMP_GRTHAN_EQ;

        /* reflexive operation */
        case COMP_EQUAL:
        case COMP_DIFF:
        case COMP_LIKE:
        case COMP_UNLIKE:
            return comp;

        default:
            /* Error */
            DisplayLog(LVL_CRIT, POLICY_TAG, "ERROR: unknown comparator %d", comp);
            return (compare_direction_t) -1;
    }
}

static inline time_t time_modify(time_t orig, const time_modifier_t *p_pol_mod)
{
    time_t newtime;
    if (!p_pol_mod) /* no modifier */
        return orig;

    /* if orig is already under time_min, keep it */
    if (orig <= p_pol_mod->time_min)
        return orig;

    newtime = orig * p_pol_mod->time_factor;
    if (newtime < p_pol_mod->time_min)
        newtime =  p_pol_mod->time_min;

    DisplayLog(LVL_FULL, POLICY_TAG, "Policy modifier enabled: time condition changed: %u -> %u",
                (unsigned int)orig, (unsigned int)newtime);
    return newtime;
}


/**
 * compare a value according to the attr type described in sm_info_def_t.
 * @return a POLICY_* value
 */
static int compare_generic(const sm_info_def_t *def,
                           const compare_triplet_t *p_triplet,
                           void *val,
                           const time_modifier_t *p_pol_mod)
{
    int rc;

    if (val == NULL)
        return POLICY_MISSING_ATTR;

    switch (def->db_type)
    {
        case DB_TEXT:
            if (def->crit_type != PT_STRING)
            {
                DisplayLog(LVL_MAJOR, POLICY_TAG, "Criteria type of '%s' is incompatible with DB type TEXT",
                           def->user_name);
                return POLICY_ERR;
            }
            assert(p_triplet->val.str != NULL);
            /* compare crit_value->str and (char *)val */
            rc = TestRegexp(p_triplet->val.str, (char *)val);

            if (p_triplet->op == COMP_EQUAL || p_triplet->op == COMP_LIKE)
                return BOOL2POLICY(rc);
            else
                return BOOL2POLICY(!rc);

        case DB_INT:
        case DB_UINT:
            if (def->crit_type == PT_DURATION)
            {
                /* XXX "dur_attr == 0" has a special meaning:
                   it matches if time has not been set */
                if (*((int *)val) == 0)
                {
                    if ((p_triplet->op == COMP_EQUAL)
                        && (p_triplet->val.duration == 0))
                        return POLICY_MATCH;
                    else /* last_archive > X do not match */
                        return POLICY_NO_MATCH;
                }

                /* compare with time enlapsed since date.
                 * take time modifiers into account */
                rc = int_compare(time(NULL) - *((int *)val), p_triplet->op,
                                 time_modify(p_triplet->val.duration, p_pol_mod));
            }
            else if (def->crit_type == PT_INT)
            {
                rc = int_compare(*((int *)val), p_triplet->op, p_triplet->val.integer);
            }
            else
            {
                DisplayLog(LVL_MAJOR, POLICY_TAG, "Criteria type of '%s' is incompatible with DB type INT/UINT",
                           def->user_name);
                return POLICY_ERR;
            }
            return BOOL2POLICY(rc);

        case DB_BIGINT:
        case DB_BIGUINT:

            if (def->crit_type != PT_INT64)
            {
                DisplayLog(LVL_MAJOR, POLICY_TAG, "Criteria type of '%s' is incompatible with DB type BIGINT/BIGUINT",
                           def->user_name);
                return POLICY_ERR;
            }

            rc = size_compare(*((ull_t *)val), p_triplet->op, p_triplet->val.size);
            return BOOL2POLICY(rc);

        case DB_ENUM_FTYPE:
        {
            const char *typedb;

            if (def->crit_type != PT_TYPE)
            {
                DisplayLog(LVL_MAJOR, POLICY_TAG, "Criteria type of '%s' is incompatible with DB type ENUM_FTYPE",
                           def->user_name);
                return POLICY_ERR;
            }
            typedb = type2db(p_triplet->val.type);
            if (typedb == NULL)
                return POLICY_ERR;
            else
                rc = !strcmp((char *)val, typedb);

            if (p_triplet->op == COMP_EQUAL)
                return BOOL2POLICY(rc);
            else
                return BOOL2POLICY(!rc);
        }

        case DB_BOOL:
            if (def->crit_type != PT_BOOL)
            {
                DisplayLog(LVL_MAJOR, POLICY_TAG, "Criteria type of '%s' is incompatible with DB type BOOL",
                           def->user_name);
                return POLICY_ERR;
            }

            /* Boolean are stored in 'integer' field by criteria2condition(). */
            /* Compare a bool with an integer (0 or <>0) */
            rc = ((*((bool *)val)) == !!p_triplet->val.integer);

            if (p_triplet->op == COMP_EQUAL)
                return BOOL2POLICY(rc);
            else
                return BOOL2POLICY(!rc);

        case DB_SHORT:
        case DB_USHORT:
            DisplayLog(LVL_MAJOR, POLICY_TAG, "Criteria type non supported: SHORT");
            return POLICY_ERR;


        case DB_ID:
            DisplayLog(LVL_MAJOR, POLICY_TAG, "Criteria type non supported: ID");
            return POLICY_ERR;

        case DB_STRIPE_INFO:
        case DB_STRIPE_ITEMS:
            DisplayLog(LVL_MAJOR, POLICY_TAG, "Criteria type non supported: STRIPE_INFO/STRIPE_ITEMS");
            return POLICY_ERR;

        default:
            DisplayLog(LVL_MAJOR, POLICY_TAG, "Unexpected criteria type in %s()",
                       __func__);
            return POLICY_ERR;
    }

    /** XXX how are stored: PT_FLOAT? */
}

/** Convert a comparator value to a DB filter value,
 * according to the given sm_info definition.
 * @param oppose_cmp
 */
static int set_filter_value_generic(const sm_info_def_t *def,
                                    compare_direction_t op,
                                    const compare_value_t *comp_val,
                                    db_type_u  *f_val, bool *oppose_cmp)
{

    switch (def->db_type)
    {
        case DB_TEXT:
            f_val->val_str = comp_val->str;
            break;
        case DB_INT: /* integer or date */
            if (def->crit_type == PT_DURATION)
            {
                /* XXX "dur_attr == 0" has a special meaning:
                       it matches if time has not been set */
                if ((op == COMP_EQUAL) && (comp_val->duration == 0))
                {
                    f_val->val_int = 0;
                }
                else
                {
                    f_val->val_int = time(NULL) - comp_val->duration;

                    /* enlapsed > X <=>  date < now - X */
                    *oppose_cmp = true;
                }
            }
            else
            {
                f_val->val_int = comp_val->integer;
            }
            break;
        case DB_UINT:
            if (def->crit_type == PT_DURATION)
            {
                /* XXX "dur_attr == 0" has a special meaning:
                       it matches if time has not been set */
                if ((op == COMP_EQUAL) && (comp_val->duration == 0))
                {
                    f_val->val_uint = 0;
                }
                else
                {
                    f_val->val_uint = time(NULL) - comp_val->duration;
                    /* enlapsed > X <=>  date < now - X */
                    *oppose_cmp = true;
                }
            }
            else
            {
                f_val->val_uint = comp_val->integer;
            }
            break;
        case DB_BIGINT:
            f_val->val_bigint = comp_val->size;
            break;
        case DB_BIGUINT:
            f_val->val_biguint = comp_val->size;
            break;
        case DB_ENUM_FTYPE:
            f_val->val_str = type2db(comp_val->type);
            break;
        case DB_BOOL:
            f_val->val_bool = comp_val->integer;
            break;
        case DB_SHORT:
            f_val->val_short = comp_val->integer;
            break;
        case DB_USHORT:
            f_val->val_ushort = comp_val->integer;
            break;
        case DB_ID:
            DisplayLog(LVL_MAJOR, POLICY_TAG, "Criteria type non supported: ID");
            return -1;

        case DB_STRIPE_INFO:
        case DB_STRIPE_ITEMS:
            DisplayLog(LVL_MAJOR, POLICY_TAG, "Criteria type non supported: STRIPE_INFO/STRIPE_ITEMS");
            return -1;

        default:
            DisplayLog(LVL_MAJOR, POLICY_TAG, "Unexpected criteria type in %s()",
                       __func__);
            return -1;
    }
    return 0;
}


/**
 * Convert criteria to ListMgr data
 * \param p_comp        IN: the condition to be converted
 * \param p_attr_index  OUT: related attribute index
 * \param p_compar      OUT: listmgr comparator
 * \param db_type_u     OUT: value
 * \param p_must_release OUT: set to TRUE if the db_type_u.val_str string must be released
 * \retval -1 if this is not a criteria stored in DB.
 */
/** @TODO factorize criteria2filter */
int criteria2filter(const compare_triplet_t *p_comp, unsigned int *p_attr_index,
                    filter_comparator_t *p_compar, filter_value_t *p_value,
                    bool *p_must_release, const sm_instance_t *smi)
{
    int len;
    char * new_str;
    bool add_root = false;

    *p_must_release = false;

    /*@FIXME this function, could make more generic processing using the
     * definitions of criteria_descr_t in rbh_boolexpr.c */
    switch(p_comp->crit)
    {
    case CRITERIA_TREE:

        /* is the path relative ? */
        if (!IS_ABSOLUTE_PATH(p_comp->val.str))
            add_root = true;

        /* fullpath like 'tree/% ' */

       *p_attr_index = ATTR_INDEX_fullpath;

        if ((p_comp->op == COMP_LIKE) || (p_comp->op == COMP_EQUAL))
            *p_compar = LIKE;
        else if ((p_comp->op == COMP_UNLIKE) || (p_comp->op == COMP_DIFF))
            *p_compar = UNLIKE;

        len = strlen(p_comp->val.str);

        if (add_root)
            len += strlen(global_config.fs_path) + 1; /* root path + '/' */

        new_str = MemAlloc(len+3); /* 3 => '/' '%' '\0' */
        *p_must_release = true;

        if (add_root)
            sprintf(new_str, "%s/%s", global_config.fs_path, p_comp->val.str);
        else
            strcpy(new_str, p_comp->val.str);

        /* XXX this won't match the root entry */

        /* is a / needed ? */
        if (!FINAL_SLASH(new_str))
        {
            new_str[len] = '/';
            len++;
        }
        /* add db 'wildcard' */
        new_str[len]='*';
        len++;
        new_str[len]='\0';

        p_value->value.val_str = new_str;
        break;

    case CRITERIA_PATH: /* fullpath 'path' */

        *p_attr_index = ATTR_INDEX_fullpath;
        *p_compar = Policy2FilterComparator(p_comp->op);

        if (!IS_ABSOLUTE_PATH(p_comp->val.str))
        {
            /* add root path */
            len = strlen(p_comp->val.str) + strlen(global_config.fs_path) + 1;
            new_str = MemAlloc(len + 1); /* +1 for \0 */
            *p_must_release = true;
            sprintf(new_str, "%s/%s", global_config.fs_path, p_comp->val.str);
            p_value->value.val_str = new_str;
        }
        else
            p_value->value.val_str = p_comp->val.str;

        break;

    case CRITERIA_FILENAME: /* name like 'filename' */

        *p_attr_index = ATTR_INDEX_name;
        *p_compar = Policy2FilterComparator(p_comp->op);
        p_value->value.val_str = p_comp->val.str;

        break;

    case CRITERIA_TYPE: /* type = 'type' */

        *p_attr_index = ATTR_INDEX_type;
        *p_compar = Policy2FilterComparator(p_comp->op);
        p_value->value.val_str = type2db(p_comp->val.type);
        break;

    case CRITERIA_OWNER: /* owner like 'owner' */
        *p_attr_index = ATTR_INDEX_owner;
        *p_compar = Policy2FilterComparator(p_comp->op);
        p_value->value.val_str = p_comp->val.str;
        break;

    case CRITERIA_GROUP:
        *p_attr_index = ATTR_INDEX_gr_name;
        *p_compar = Policy2FilterComparator(p_comp->op);
        p_value->value.val_str = p_comp->val.str;
        break;

    case CRITERIA_SIZE:
        *p_attr_index = ATTR_INDEX_size;
        *p_compar = Policy2FilterComparator(p_comp->op);
        p_value->value.val_biguint = p_comp->val.size;
        break;

    case CRITERIA_DEPTH:
        *p_attr_index = ATTR_INDEX_depth;
        *p_compar = Policy2FilterComparator(p_comp->op);
        p_value->value.val_uint = p_comp->val.integer;
        break;

    case CRITERIA_DIRCOUNT:
        *p_attr_index = ATTR_INDEX_dircount;
        *p_compar = Policy2FilterComparator(p_comp->op);
        p_value->value.val_uint = p_comp->val.integer;
        break;

    case CRITERIA_LAST_ACCESS:
        *p_attr_index = ATTR_INDEX_last_access;

    /*   last_access > 2h <=> access_time < time(NULL) - 2h */

        *p_compar = Policy2FilterComparator(oppose_compare(p_comp->op));
        p_value->value.val_uint = time(NULL) - p_comp->val.duration;
        break;

    case CRITERIA_LAST_MOD:
        *p_attr_index = ATTR_INDEX_last_mod;
        *p_compar = Policy2FilterComparator(oppose_compare(p_comp->op));
        p_value->value.val_uint = time(NULL) - p_comp->val.duration;
        break;

    case CRITERIA_CREATION:
        *p_attr_index = ATTR_INDEX_creation_time;
        *p_compar = Policy2FilterComparator(oppose_compare(p_comp->op));
        p_value->value.val_uint = time(NULL) - p_comp->val.duration;
        break;

    case CRITERIA_RMTIME:
        if (smi == NULL || !(smi->sm->flags & SM_DELETED))
        {
            DisplayLog(LVL_CRIT, POLICY_TAG, "rm_time condition out of a 'remove' policy");
            return -1;
        }

        /* XXX should only be used in a policy about 'removed' entries */
        *p_attr_index = ATTR_INDEX_rm_time;
        *p_compar = Policy2FilterComparator(oppose_compare(p_comp->op));
        p_value->value.val_uint = time(NULL) - p_comp->val.duration;
        break;

#ifdef _LUSTRE
    case CRITERIA_POOL:
        *p_attr_index = ATTR_INDEX_stripe_info;
        *p_compar = Policy2FilterComparator(p_comp->op);
        p_value->value.val_str = p_comp->val.str;
        break;

    case CRITERIA_OST:
        *p_attr_index = ATTR_INDEX_stripe_items;
        *p_compar = Policy2FilterComparator(p_comp->op);
        p_value->value.val_uint = p_comp->val.integer;
        break;
#endif

    case CRITERIA_STATUS:
        if (smi == NULL)
            RBH_BUG("status filter with no status manager in the context");

        *p_attr_index = smi_status_index(smi);
        *p_compar = Policy2FilterComparator(p_comp->op);
        p_value->value.val_str = p_comp->val.str;
        break;

    case CRITERIA_SM_INFO:
        {
            const sm_info_def_t *def = NULL;
            int rc;
            unsigned int idx;
            bool oppose = false;

            rc = sm_attr_get(smi, NULL, p_comp->attr_name, NULL, &def, &idx);
            if (rc < 0)
            {
                DisplayLog(LVL_CRIT, POLICY_TAG, "couldn't find criteria '%s' in context",
                           p_comp->attr_name);
                return -1;
            }
            DisplayLog(LVL_FULL, POLICY_TAG, "Attribute index of '%s' = %d", p_comp->attr_name, idx);
            *p_attr_index = idx;
            rc = set_filter_value_generic(def, p_comp->op, &p_comp->val, &p_value->value, &oppose);
            if (rc)
                return -1;

            if (oppose)
                *p_compar = Policy2FilterComparator(oppose_compare(p_comp->op));
            else
                *p_compar = Policy2FilterComparator(p_comp->op);

        }
        break;

    case CRITERIA_XATTR:
    default:
        *p_attr_index = ATTR_INDEX_FLG_UNSPEC;
        return -1;
    }

    return 0;
}


/** @TODO factorize eval_condition */
static policy_match_t eval_condition(const entry_id_t *p_entry_id,
                                     const attr_set_t *p_entry_attr,
                                     const compare_triplet_t *p_triplet,
                                     const time_modifier_t *p_pol_mod,
                                     const sm_instance_t *smi,
                                     int no_warning)
{
    char           tmpbuff[RBH_PATH_MAX];
    char          *rep;
    const char * typedb;
    int            rc;

    switch (p_triplet->crit)
    {
    case CRITERIA_TREE:
        /* fullpath is required */
        CHECK_ATTR(p_entry_attr, fullpath, no_warning);

        rep = ExtractParentDir(ATTR(p_entry_attr, fullpath), tmpbuff);
        rc = TestPathRegexp(p_triplet->val.str, rep, PATHREGEXP_IS_CHILD |
                              ((p_triplet->flags & CMP_FLG_ANY_LEVEL)?
                                    PATHREGEXP_ANY_LEVEL:0)) ;
        if (!rc) /* try matching root */
        {
            rc = TestPathRegexp(p_triplet->val.str, ATTR(p_entry_attr, fullpath),
                              ((p_triplet->flags & CMP_FLG_ANY_LEVEL)?
                                    PATHREGEXP_ANY_LEVEL:0)) ;
        }

#ifdef _DEBUG_POLICIES
        if (rc)
            printf("%s (dir %s) matches tree %s\n", ATTR(p_entry_attr, fullpath), rep,
                    p_triplet->val.str);
#endif

        if (p_triplet->op == COMP_EQUAL || p_triplet->op == COMP_LIKE)
            return BOOL2POLICY(rc);
        else
            return BOOL2POLICY(!rc);

    case CRITERIA_PATH:
        /* fullpath is required */
        CHECK_ATTR(p_entry_attr, fullpath, no_warning);

        rc = TestPathRegexp(p_triplet->val.str, ATTR(p_entry_attr, fullpath),
                             ((p_triplet->flags & CMP_FLG_ANY_LEVEL)?
                                    PATHREGEXP_ANY_LEVEL:0)) ;

        if (p_triplet->op == COMP_EQUAL || p_triplet->op == COMP_LIKE)
            return BOOL2POLICY(rc);
        else
            return BOOL2POLICY(!rc);

    case CRITERIA_FILENAME:

        /* filename is required */
        CHECK_ATTR(p_entry_attr, name, no_warning);

        rc = TestRegexp(p_triplet->val.str, ATTR(p_entry_attr, name));

#ifdef _DEBUG_POLICIES
        if (rc)
            printf("%s matches filename %s\n", ATTR(p_entry_attr, name), p_triplet->val.str);
#endif

        if (p_triplet->op == COMP_EQUAL || p_triplet->op == COMP_LIKE)
            return BOOL2POLICY(rc);
        else
            return BOOL2POLICY(!rc);

    case CRITERIA_TYPE:

        /* type is required */
        CHECK_ATTR(p_entry_attr, type, no_warning);

        typedb = type2db(p_triplet->val.type);
        if (typedb == NULL)
            return POLICY_ERR;
        else
            rc = !strcmp(ATTR(p_entry_attr, type), typedb);

        if (p_triplet->op == COMP_EQUAL)
            return BOOL2POLICY(rc);
        else
            return BOOL2POLICY(!rc);

    case CRITERIA_OWNER:
        /* owner is required */
        CHECK_ATTR(p_entry_attr, owner, no_warning);

        rc = TestRegexp(p_triplet->val.str, ATTR(p_entry_attr, owner));

        if (p_triplet->op == COMP_EQUAL || p_triplet->op == COMP_LIKE)
            return BOOL2POLICY(rc);
        else
            return BOOL2POLICY(!rc);

    case CRITERIA_GROUP:
        /* group is required */
        CHECK_ATTR(p_entry_attr, gr_name, no_warning);

        rc = TestRegexp(p_triplet->val.str, ATTR(p_entry_attr, gr_name));

        if (p_triplet->op == COMP_EQUAL || p_triplet->op == COMP_LIKE)
            return BOOL2POLICY(rc);
        else
            return BOOL2POLICY(!rc);

    case CRITERIA_SIZE:
        /* size is required */
        CHECK_ATTR(p_entry_attr, size, no_warning);

        rc = size_compare(ATTR(p_entry_attr, size), p_triplet->op, p_triplet->val.size);
        return BOOL2POLICY(rc);

    case CRITERIA_DEPTH:
        /* depth is required */
        CHECK_ATTR(p_entry_attr, depth, no_warning);

        rc = int_compare(ATTR(p_entry_attr, depth), p_triplet->op, p_triplet->val.integer);
        return BOOL2POLICY(rc);

    case CRITERIA_DIRCOUNT:
        /* dircount is required */
        CHECK_ATTR(p_entry_attr, dircount, no_warning);

        rc = int_compare(ATTR(p_entry_attr, dircount), p_triplet->op, p_triplet->val.integer);
        return BOOL2POLICY(rc);

    case CRITERIA_LAST_ACCESS:
        /* last_access is required */
        CHECK_ATTR(p_entry_attr, last_access, no_warning);

        rc = int_compare(time(NULL) - ATTR(p_entry_attr, last_access), p_triplet->op,
                          time_modify(p_triplet->val.duration, p_pol_mod));
        return BOOL2POLICY(rc);

    case CRITERIA_LAST_MOD:
        /* last_mod required */

        CHECK_ATTR(p_entry_attr, last_mod, no_warning);

        rc = int_compare(time(NULL) - ATTR(p_entry_attr, last_mod), p_triplet->op,
                          time_modify(p_triplet->val.duration, p_pol_mod));
        return BOOL2POLICY(rc);

    case CRITERIA_CREATION:
        /* creation_time is required */
        CHECK_ATTR(p_entry_attr, creation_time, no_warning);

        rc = int_compare(time(NULL) - ATTR(p_entry_attr, creation_time), p_triplet->op,
                          time_modify(p_triplet->val.duration, p_pol_mod));
        return BOOL2POLICY(rc);

        break;

    case CRITERIA_RMTIME:
        if (smi == NULL || !(smi->sm->flags & SM_DELETED))
        {
            DisplayLog(LVL_CRIT, POLICY_TAG, "rm_time condition out of a 'remove' policy");
            return POLICY_ERR;
        }

        /* rm_time is required */
        CHECK_ATTR(p_entry_attr, rm_time, no_warning);

        rc = int_compare(time(NULL) - ATTR(p_entry_attr, rm_time), p_triplet->op,
                          time_modify(p_triplet->val.duration, p_pol_mod));
        return BOOL2POLICY(rc);

        break;

#ifdef _LUSTRE
    case CRITERIA_POOL:
        /* /!\ objects != file or dir don't have stripe info (never match) */
        if (ATTR_MASK_TEST(p_entry_attr, type) &&
             strcmp(ATTR(p_entry_attr, type), STR_TYPE_DIR) &&
             strcmp(ATTR(p_entry_attr, type), STR_TYPE_FILE))
            return POLICY_NO_MATCH;

        /* pool name is required */
        CHECK_ATTR(p_entry_attr, stripe_info, no_warning);

        rc = TestRegexp(p_triplet->val.str, ATTR(p_entry_attr, stripe_info).pool_name);

#ifdef _DEBUG_POLICIES
        if (rc)
            printf("'%s' pool matches '%s'\n", ATTR(p_entry_attr, stripe_info).pool_name,
                    p_triplet->val.str);
#endif

        if (p_triplet->op == COMP_EQUAL || p_triplet->op == COMP_LIKE)
            return BOOL2POLICY(rc);
        else
            return BOOL2POLICY(!rc);

    case CRITERIA_OST:
    {
        int i;

        /* /!\ objects != file don't have stripe items (never match) */
        if (ATTR_MASK_TEST(p_entry_attr, type) &&
             strcmp(ATTR(p_entry_attr, type), STR_TYPE_FILE))
            return POLICY_NO_MATCH;

        /* stripe items are needed */
        CHECK_ATTR(p_entry_attr, stripe_items, no_warning);

        for (i = 0; i < ATTR(p_entry_attr, stripe_items).count; i++)
        {
            if (ATTR(p_entry_attr, stripe_items).stripe[i].ost_idx == p_triplet->val.integer)
            {
                /* if comparator is ==, at least 1 OST must match,
                 * if the cmp is !=, none must match */
                if (p_triplet->op == COMP_DIFF)
                    return POLICY_NO_MATCH;
                else if (p_triplet->op == COMP_EQUAL)
                    return POLICY_MATCH;
            }
        }
        /* no matching OST:
         * - if the operator is !=, the entry matches
         * - else, the entry doesn't match */
        if (p_triplet->op == COMP_DIFF)
            return POLICY_MATCH;
        else if (p_triplet->op == COMP_EQUAL)
            return POLICY_NO_MATCH;
        break;
    }
#endif

    case CRITERIA_STATUS:
        {
            if (smi == NULL)
                RBH_BUG("status criteria with no status manager in the context");

            if (!ATTR_MASK_STATUS_TEST(p_entry_attr, smi->smi_index))
                /* compare with empty string */
                rc = EMPTY_STRING(p_triplet->val.str);
            else
                rc = !strcmp(p_triplet->val.str, STATUS_ATTR(p_entry_attr, smi->smi_index));

            if (p_triplet->op == COMP_EQUAL)
                return BOOL2POLICY(rc);
            else
                return BOOL2POLICY(!rc);
        }
        break;

    case CRITERIA_SM_INFO:
        {
            void *val;
            const sm_info_def_t *def;
            unsigned int idx;

            rc = sm_attr_get(smi, p_entry_attr, p_triplet->attr_name, &val, &def, &idx);
            if (rc < 0)
            {
               if (!no_warning)
                   DisplayLog(LVL_MAJOR, POLICY_TAG,
                              "Missing attribute '%s' for evaluating boolean expression on "DFID,
                              p_triplet->attr_name, PFID(p_entry_id));
                return POLICY_MISSING_ATTR;
            }

            rc = compare_generic(def, p_triplet, val, p_pol_mod);
            DisplayLog(LVL_FULL, POLICY_TAG, "Matching '%s': rc=%d",
                       p_triplet->attr_name, rc);
            return rc;
        }

    case CRITERIA_XATTR:
    {
        const char * entry_path;
        char value[1024];
#if (!defined (_LUSTRE) || !defined(_HAVE_FID))
         /* fullpath needed to get xattr, except if fids are supported */
        CHECK_ATTR(p_entry_attr, fullpath, no_warning);
        entry_path = ATTR(p_entry_attr, fullpath);
#else
        if (p_entry_id)
        {
            /* use fid path */
            rc = BuildFidPath(p_entry_id, tmpbuff);
            if (rc)
               return POLICY_ERR;
            entry_path = tmpbuff;
        }
        else if (ATTR_MASK_TEST(p_entry_attr, fullpath))
        {
            /* use posix path */
            entry_path = ATTR(p_entry_attr, fullpath);
        }
        else
        {
            if (!no_warning)
                DisplayLog(LVL_DEBUG, POLICY_TAG,
                    "Missing fid or fullpath to evaluate boolean expression on xattr");
            return POLICY_MISSING_ATTR;
        }
#endif

        /* retrieve xattr value */
        rc = lgetxattr(entry_path, p_triplet->attr_name, value, 1024);
        if (rc < 0)
        {
            if (errno == ENOATTR || errno == ENODATA || errno == ENOENT)
                /* empty string == no attribute */
                strcpy(value, "");
            else if  (errno == ENOTSUP)
            {
                DisplayLog(LVL_CRIT, POLICY_TAG, "Error: condition on extended attribute "
                           "whereas this feature is not supported by the filesystem, or xattr name '%s' is invalid)",
                           p_triplet->attr_name);
                return POLICY_ERR;
            }
            else
            {
                DisplayLog(LVL_CRIT, POLICY_TAG, "Error getting xattr '%s' on '%s' : %s",
                           p_triplet->attr_name, entry_path, strerror(errno));
                return POLICY_ERR;
            }
        }
        else
        {
            /* security: set byte n+1 to '\0', to avoid overflows if attr is not a string */
            if (rc < 1024)
                value[rc] = '\0';
        }

        DisplayLog(LVL_FULL, POLICY_TAG, "<xattr>.%s = \"%s\" (%s)",
                   p_triplet->attr_name, value, entry_path);

        /* compare attribute value */

        rc = TestRegexp(p_triplet->val.str, value);

        if (p_triplet->op == COMP_EQUAL || p_triplet->op == COMP_LIKE)
            return BOOL2POLICY(rc);
        else
            return BOOL2POLICY(!rc);

        break;
    }

    default:
        DisplayLog(LVL_CRIT, POLICY_TAG, "This criteria (%#x) is not supported in this mode", p_triplet->crit);
        return POLICY_ERR;
    }

    return POLICY_ERR;

}


/* function for testing a boolean expression on a given entry */
static policy_match_t _entry_matches(const entry_id_t *p_entry_id, const attr_set_t *p_entry_attr,
                                     const bool_node_t *p_node, const time_modifier_t *p_pol_mod,
                                     const sm_instance_t *smi, /* to match status criteria */
                                     int no_warning)
{
    policy_match_t rc;

    if (!p_entry_id || !p_entry_attr || !p_node)
        return POLICY_ERR;

    switch (p_node->node_type)
    {
    case NODE_UNARY_EXPR:

        /* BOOL_NOT is the only supported unary operator */
        if (p_node->content_u.bool_expr.bool_op != BOOL_NOT)
            return POLICY_ERR;

        rc = _entry_matches(p_entry_id, p_entry_attr,
                            p_node->content_u.bool_expr.expr1, p_pol_mod,
                            smi, no_warning);

        return negate_match(rc);

    case NODE_BINARY_EXPR:
        /* always test the first expression */
        rc = _entry_matches(p_entry_id, p_entry_attr,
                            p_node->content_u.bool_expr.expr1, p_pol_mod,
                            smi, no_warning);

        /* in some cases, we can stop here */
        if ((p_node->content_u.bool_expr.bool_op == BOOL_OR) && (rc == POLICY_MATCH))
            return POLICY_MATCH;
        else if ((p_node->content_u.bool_expr.bool_op == BOOL_AND) && (rc == POLICY_NO_MATCH))
            return POLICY_NO_MATCH;
        else if (rc != POLICY_MATCH && rc != POLICY_NO_MATCH)
            return rc;

        /* compute the second expression */
        return _entry_matches(p_entry_id, p_entry_attr,
                              p_node->content_u.bool_expr.expr2,
                              p_pol_mod, smi, no_warning);

        break;

    case NODE_CONDITION:
        /* It's now time to test the value ! */
        return eval_condition(p_entry_id, p_entry_attr, p_node->content_u.condition,
                              p_pol_mod, smi, no_warning);
        break;

    case NODE_CONSTANT:
        return BOOL2POLICY(p_node->content_u.constant);
    }

    return POLICY_ERR;

}

policy_match_t entry_matches(const entry_id_t *p_entry_id, const attr_set_t *p_entry_attr,
                             bool_node_t *p_node, const time_modifier_t *p_pol_mod,
                             const sm_instance_t *smi)
{
    return _entry_matches(p_entry_id, p_entry_attr, p_node, p_pol_mod, smi, false);
}

static policy_match_t _is_whitelisted(const policy_descr_t *policy,
                              const entry_id_t *p_entry_id,
                              const attr_set_t *p_entry_attr,
                              fileset_item_t **fileset,
                              bool no_warning)
{
    unsigned int   i, count;
    policy_match_t rc = POLICY_NO_MATCH;
    whitelist_item_t *list;
    fileset_item_t **fs_list;

    if (fileset != NULL)
        *fileset = NULL;

    /* /!\ ignorelist is 'ignore_fileclass'
     *     whitelist is 'ignore'
     */
    list = policy->rules.whitelist_rules;
    count = policy->rules.whitelist_count;

    for (i = 0; i < count; i++)
    {
        switch (_entry_matches(p_entry_id, p_entry_attr, &list[i].bool_expr, NULL,
                               policy->status_mgr, no_warning))
        {
        case POLICY_MATCH:
            /* TODO remember the entry is ignored for this policy? */
            return POLICY_MATCH;
        case POLICY_MISSING_ATTR:
            if (!no_warning)
            {
                char buff[1024];
                BoolExpr2str(&list[i].bool_expr, buff, 1024);
                DisplayLog(LVL_MAJOR, POLICY_TAG, "Attribute is missing for checking whitelist rule '%s'", buff);
            }
            if (rc != POLICY_ERR)
                rc = POLICY_MISSING_ATTR;
            break;
        case POLICY_ERR:
        {
            char buff[1024];
            BoolExpr2str(&list[i].bool_expr, buff, 1024);
            DisplayLog(LVL_CRIT, POLICY_TAG, "An error occured while checking this whitelist rule: %s", buff);
            rc = POLICY_ERR;
            break;
        }
        case POLICY_NO_MATCH:
            /* continue testing other whitelist rules */
            break;
        }
    }

    count = policy->rules.ignore_count;
    fs_list = policy->rules.ignore_list;

    for (i = 0; i < count; i++)
    {
#ifdef _DEBUG_POLICIES
        printf("Checking if entry matches whitelisted fileset %s...\n", fs_list[i]->fileset_id);
#endif
        switch (_entry_matches(p_entry_id, p_entry_attr, &fs_list[i]->definition,
                               NULL, policy->status_mgr, no_warning))
        {
        case POLICY_MATCH:
        {
#ifdef _DEBUG_POLICIES
            printf("   -> match\n");
#endif
            if (fileset != NULL)
                *fileset = fs_list[i];

            /* TODO remember if the policy matches a ignore rule for this policy? */
            return POLICY_MATCH;
        }
        case POLICY_MISSING_ATTR:
#ifdef _DEBUG_POLICIES
            printf("   -> missing attr\n");
#endif
            if (!no_warning)
                DisplayLog(LVL_MAJOR, POLICY_TAG,
                        "Attribute is missing for checking ignore_fileclass rule");
            if (rc != POLICY_ERR)
                rc = POLICY_MISSING_ATTR;
            break;
        case POLICY_ERR:
#ifdef _DEBUG_POLICIES
            printf("   -> error\n");
#endif
            DisplayLog(LVL_CRIT, POLICY_TAG,
                        "An error occured when checking ignore_fileclass rule");
            rc = POLICY_ERR;
            break;
        case POLICY_NO_MATCH:
#ifdef _DEBUG_POLICIES
            printf("   -> no match\n");
#endif
            /* continue testing other whitelist rules */
            break;
        }
    }

    return rc;
}

policy_match_t is_whitelisted(const policy_descr_t *policy,
                              const entry_id_t *p_entry_id,
                              const attr_set_t *p_entry_attr,
                              fileset_item_t **fileset)
{
    return _is_whitelisted(policy, p_entry_id, p_entry_attr, fileset, false);
}


/** determine if a class is whitelisted for the given policy */
bool class_is_whitelisted(const policy_descr_t *policy, const char * class_id)
{
    unsigned int   i, count;
    fileset_item_t **fs_list;

    count = policy->rules.ignore_count;
    fs_list = policy->rules.ignore_list;

    for (i = 0; i < count; i++)
    {
        if (!strcasecmp(fs_list[i]->fileset_id, class_id))
            return true;
    }
    /* not found */
    return false;
}


/* Match classes according to p_attrs_cached+p_attrs_new,
 * set the result in p_attrs_new->fileclass.
 */
int match_classes(const entry_id_t *id, attr_set_t *p_attrs_new,
                  const attr_set_t *p_attrs_cached)
{
    unsigned int i;
    int          ok = 0;
    int left = sizeof(ATTR(p_attrs_new, fileclass));

    /* initialize output fileclass */
    char *pcur = ATTR(p_attrs_new, fileclass);
    *pcur = '\0';

    /* merge contents of the 2 input attr sets */
    attr_set_t attr_cp = *p_attrs_new;
    ListMgr_MergeAttrSets(&attr_cp, p_attrs_cached, false);

    for (i = 0; i < policies.fileset_count; i++)
    {
        fileset_item_t *fset = &policies.fileset_list[i];

        if (!fset->matchable)
        {
            ok++;
            continue;
        }

        switch (_entry_matches(id, &attr_cp, &fset->definition, NULL, NULL, true))
        {
            case POLICY_MATCH:
                ok ++;
                if (EMPTY_STRING(ATTR(p_attrs_new, fileclass)))
                {
                    strncpy(pcur, fset->fileset_id, left);
                    left -= strlen(pcur);
                    pcur += strlen(pcur);
                }
                else if (left > 1)
                {
                    *pcur = LIST_SEP_CHAR;
                    pcur++;
                    strncpy(pcur, fset->fileset_id, left - 1);
                    left -= strlen(pcur)+1;
                    pcur += strlen(pcur);
                }
                break;
            case POLICY_MISSING_ATTR:
                DisplayLog(LVL_MAJOR, POLICY_TAG, "Attribute is missing for checking fileset %s",
                           fset->fileset_id);
                break;
            case POLICY_ERR:
                DisplayLog(LVL_CRIT, POLICY_TAG, "An error occured when checking fileset %s",
                           fset->fileset_id);
                break;
            case POLICY_NO_MATCH:
                ok ++;
                /* continue testing other file classes */
                break;
        }
    }

    /* no fileclass could be matched without an error */
    if (policies.fileset_count != 0 && ok == 0)
    {
        ATTR_MASK_UNSET(p_attrs_new, fileclass);
    }
    else
    {
        ATTR(p_attrs_new, class_update) = time(NULL);
        ATTR_MASK_SET(p_attrs_new, fileclass);
        ATTR_MASK_SET(p_attrs_new, class_update);
    }

    return 0;
}


#if 0

fileset_item_t *GetFilesetById(const char *fileset_id)
{
    unsigned int   i;

    for (i = 0; i < policies.filesets.fileset_count; i++)
    {
        if (!strcmp(policies.filesets.fileset_list[i].fileset_id, fileset_id))
            return &policies.filesets.fileset_list[i];
    }

    return NULL;

}



/** @TODO XXX is this useful? */
char          *FilesetMatch(const entry_id_t * p_entry_id, const attr_set_t * p_entry_attr)
{
    unsigned int   i;
    int            rc = POLICY_NO_MATCH;

    for (i = 0; i < policies.filesets.fileset_count; i++)
    {
        switch (EntryMatches
                 (p_entry_id, p_entry_attr, &policies.filesets.fileset_list[i].definition))
        {
        case POLICY_MATCH:
            return policies.filesets.fileset_list[i].fileset_id;
        case POLICY_MISSING_ATTR:
            DisplayLog(LVL_MAJOR, POLICY_TAG, "Attribute is missing for checking fileset");
            if (rc != POLICY_ERR)
                rc = POLICY_MISSING_ATTR;
            break;
        case POLICY_ERR:
            DisplayLog(LVL_CRIT, POLICY_TAG, "An error occured when checking fileset");
            rc = POLICY_ERR;
            break;
        case POLICY_NO_MATCH:
            /* continue testing other file classes */
            break;
        }
    }

    return NULL;

}


fileset_item_t *GetFilesetById(const char *fileset_id)
{
    unsigned int   i;

    for (i = 0; i < policies.filesets.fileset_count; i++)
    {
        if (!strcmp(policies.filesets.fileset_list[i].fileset_id, fileset_id))
            return &policies.filesets.fileset_list[i];
    }

    return NULL;

}
#endif

/** get the first matching policy case for the given file */
rule_item_t *policy_case(const policy_descr_t *policy,
                         const entry_id_t * p_entry_id,
                         const attr_set_t * p_entry_attr,
                         fileset_item_t ** pp_fileset)
{
    int            count, i, j;
    unsigned int   default_index = ATTR_INDEX_FLG_UNSPEC;
    rule_item_t *pol_list;

    pol_list = policy->rules.rules;
    count = policy->rules.rule_count;

    /* for each policy (except default), check target filesets.
     *   - if a fileset matches, return the associated policy.
     *   - else, return defaut policy, if it is specified.
     *   - else, write a warning.
     */
    for (i = 0; i < count; i++)
    {

#ifdef _DEBUG_POLICIES
        printf("Checking policy %s...\n", pol_list[i].rule_id);
#endif

        if (!strcasecmp(pol_list[i].rule_id, "default"))
        {
            /* remember index of default policy */
            default_index = i;
            continue;
        }

        /* check filesets */

        for (j = 0; j < pol_list[i].target_count; j++)
        {

#ifdef _DEBUG_POLICIES
            printf("    Checking file class %s\n", pol_list[i].target_list[j]->fileset_id);
#endif

            switch (entry_matches(p_entry_id, p_entry_attr,
                                  &pol_list[i].target_list[j]->definition,
                                  NULL, policy->status_mgr))
            {
            case POLICY_MATCH:
                DisplayLog(LVL_FULL, POLICY_TAG,
                            "Entry " F_ENT_ID " matches target file class '%s' of policy '%s'",
                            P_ENT_ID(p_entry_id, p_entry_attr), pol_list[i].target_list[j]->fileset_id,
                            pol_list[i].rule_id);
                if (pp_fileset)
                    *pp_fileset = pol_list[i].target_list[j];
                return &pol_list[i];

            case POLICY_NO_MATCH:
                break;

            case POLICY_MISSING_ATTR:
                DisplayLog(LVL_MAJOR, POLICY_TAG, "Attributes are missing to check if entry " F_ENT_ID
                            " matches file class '%s' (in policy '%s')", P_ENT_ID(p_entry_id, p_entry_attr),
                            pol_list[i].target_list[j]->fileset_id, pol_list[i].rule_id);
                break;

            default:
                DisplayLog(LVL_CRIT, POLICY_TAG, "Error while checking if entry " F_ENT_ID
                            " matches file class '%s' (in policy '%s')", P_ENT_ID(p_entry_id, p_entry_attr),
                            pol_list[i].target_list[j]->fileset_id, pol_list[i].rule_id);
            }
        }
    }

    /* => entry matches no fileset in any policy */
    if (pp_fileset)
        *pp_fileset = NULL;


    /* if there a default ? */
    if (default_index != ATTR_INDEX_FLG_UNSPEC)
        return &pol_list[default_index];

    /* entry matches no policy => ignored */
    DisplayLog(LVL_DEBUG, POLICY_TAG, "Entry " F_ENT_ID " matches no policy case: not applying %s policy to it.",
               P_ENT_ID(p_entry_id, p_entry_attr), policy->name);

    return NULL;
}

/** get the policy case for the given fileclass.
 *  \param pp_fileset is set to the matching fileset
 *         or NULL for the default policy case
 */
rule_item_t * class_policy_case(const policy_descr_t *policy,
                                  const char *class_id,
                                  fileset_item_t **pp_fileset)
{
    int            count, i, j;
    rule_item_t *pol_list;

    count = policy->rules.rule_count;
    pol_list = policy->rules.rules;

    /* check name of target filesets for each policy.
     * if name is 'default', return the default policy case.
     * If policy case is not found, return NULL.
     */
    for (i = 0; i < count; i++)
    {
        if (!strcasecmp(pol_list[i].rule_id, "default"))
        {
            // XXX matches no fileclass
            continue;
#if 0
            /* do we look for default case? */
            if (!strcmp(class_id, CLASS_DEFAULT))
            {
                if (pp_fileset)
                    *pp_fileset = NULL;
                return &pol_list[i];
            }
            else
                continue;
#endif
        }

        /* check filesets */

        for (j = 0; j < pol_list[i].target_count; j++)
        {
            if (!strcasecmp(class_id, pol_list[i].target_list[j]->fileset_id))
            {
                DisplayLog(LVL_FULL, POLICY_TAG,
                    "FileClass '%s' is a target of policy '%s'",
                    class_id, pol_list[i].rule_id);
                if (pp_fileset)
                    *pp_fileset = pol_list[i].target_list[j];
                return &pol_list[i];
            }
        }
    }

    DisplayLog(LVL_MAJOR, POLICY_TAG, "Saved fileclass '%s' is no longer used in %s policy. Refresh needed.",
               class_id, policy->name);
    return NULL;
}


/**
 *  Check if an entry has a chance to be matched in any policy condition.
 */
policy_match_t policy_match_all(const policy_descr_t *policy,
                                const entry_id_t *p_entry_id,
                                const attr_set_t *p_entry_attr,
                                const time_modifier_t *time_mod,
                                fileset_item_t **pp_fileset)
{
    bool           could_not_match = false;
    int            count, i, j;
    int            default_index = -1;
    rule_item_t *pol_list;

    /* if it MATCHES any whitelist condition, return NO_MATCH
     * else, it could potentially match a policy, so we must test them.
     */
    switch (_is_whitelisted(policy, p_entry_id, p_entry_attr, pp_fileset, true))
    {
        case POLICY_MATCH:
            return POLICY_NO_MATCH;
        case POLICY_MISSING_ATTR:
            could_not_match = true;
            break;
        case POLICY_NO_MATCH:
            break;
        default:
            return POLICY_ERR;
    }

    pol_list = policy->rules.rules;
    count = policy->rules.rule_count;

    /* for each policy:
     * - if we get NO_MATCH for all filesets of the policy,
     *   no need to check the condition.
     * - if we get MATCH for any fileset of the policy,
     *   only test this condition.
     * - if we get MISSING_ATTR  for any fileset of the policy,
     *   test the condition.
     *
     * - if we get MATCH for the condition, return MATCH or MISSING_ATTR if we got some previously
     * - if we get NO_MATCH for the condition, this policy cannot be matched.
     * - if we get MISSING_ATTR for the condition, return MISSING_ATTR.
     */
    for (i = 0; i < count; i++)
    {
        bool match = false;
        bool all_no_match = (pol_list[i].target_count > 0 ? true : false);
        bool missing_attr = false;

#ifdef _DEBUG_POLICIES
        printf("Checking policy %s...\n", pol_list[i].rule_id);
#endif

        if (!strcasecmp(pol_list[i].rule_id, "default"))
        {
            /* remember index of default policy */
            default_index = i;
            continue;
        }

        /* check filesets */

        for (j = 0; j < pol_list[i].target_count; j++)
        {
#ifdef _DEBUG_POLICIES
            printf("    Checking file class %s\n", pol_list[i].target_list[j]->fileset_id);
#endif

            switch (_entry_matches(p_entry_id, p_entry_attr,
                                   &pol_list[i].target_list[j]->definition,
                                   time_mod, policy->status_mgr, true))
            {
            case POLICY_MATCH:
                DisplayLog(LVL_FULL, POLICY_TAG,
                            "Entry matches target file class '%s' of policy '%s'",
                            pol_list[i].target_list[j]->fileset_id, pol_list[i].rule_id);
                all_no_match = false;
                match = true;
                break;

            case POLICY_NO_MATCH:
                break;

            case POLICY_MISSING_ATTR:
                all_no_match = false;
                missing_attr = true;
                DisplayLog(LVL_FULL, POLICY_TAG, "Attributes are missing to check if entry"
                            " matches file class '%s' (in policy '%s')",
                            pol_list[i].target_list[j]->fileset_id, pol_list[i].rule_id);
                break;

            default:
                DisplayLog(LVL_CRIT, POLICY_TAG, "Error while checking if entry"
                            " matches file class '%s' (in policy '%s')",
                            pol_list[i].target_list[j]->fileset_id, pol_list[i].rule_id);
                return POLICY_ERR;
            }

            /* if entry matches or an attribute is missing, we can test the condition */
            if (match || missing_attr)
                break;
        }

#ifdef _DEBUG_POLICIES
        printf
            ("Summary for target filesets of policy %s: match=%d, missing_attr=%d, all_no_match=%d\n",
              pol_list[i].rule_id, match, missing_attr, all_no_match);
#endif
        /**
         * - if we get MATCH for any fileset of the policy,
         *   only test this condition.
         * - if we get MISSING_ATTR  for any fileset of the policy,
         *   test the condition.
         */

        /* if we got NO_MATCH for all filesets of the policy, no need to check the condition. */
        if (all_no_match)
            continue;

        /* test the condition of the policy */

        /* - if we get MATCH for the condition, return MATCH or MISSING_ATTR if we got some previously
         * - if we get NO_MATCH for the condition, this policy cannot be matched.
         * - if we get MISSING_ATTR for the condition, return MISSING_ATTR.
         */
        switch (_entry_matches(p_entry_id, p_entry_attr,
                               &pol_list[i].condition, time_mod,
                               policy->status_mgr, true))
        {
        case POLICY_NO_MATCH:
            /* the entry cannot match this item */
            break;
        case POLICY_MATCH:
            /* return MATCH if we add not missing attrs previously,
             * MISSING_ATTR else.
             */
            DisplayLog(LVL_DEBUG, POLICY_TAG,
                        "Entry matches the condition for policy '%s'.", pol_list[i].rule_id);
            if (missing_attr || could_not_match)
                return POLICY_MISSING_ATTR;
            else
                return POLICY_MATCH;
            break;
        case POLICY_MISSING_ATTR:
            return POLICY_MISSING_ATTR;
        default:
            DisplayLog(LVL_MAJOR, POLICY_TAG,
                        "Error checking if entry matches the condition for policy '%s'.",
                        pol_list[i].rule_id);
            return POLICY_ERR;
        }

        /*  if we get MATCH for any fileset of the policy,
         *  only test this condition. */
        if (match)
            return POLICY_NO_MATCH;

    }

    /* at this point, we have no chance to match policies,
     * now check default case.
     */

    if (default_index != -1)
    {
        /* XXX assumes default rule is not LUA */

        /* - if we get MATCH for the condition, return MATCH or MISSING_ATTR if we got some previously
         * - if we get NO_MATCH for the condition, no policy is matched.
         * - if we get MISSING_ATTR for the condition, return MISSING_ATTR.
         */
        switch (_entry_matches(p_entry_id, p_entry_attr,
                               &pol_list[default_index].condition,
                               time_mod, policy->status_mgr, true))
        {
        case POLICY_NO_MATCH:
            return POLICY_NO_MATCH;
            break;
        case POLICY_MATCH:
            /* return MATCH if we add not missing attrs previously,
             * MISSING_ATTR else.
             */
            DisplayLog(LVL_DEBUG, POLICY_TAG, "Entry matches the condition for default policy");
            if (could_not_match)
                return POLICY_MISSING_ATTR;
            else
                return POLICY_MATCH;
            break;
        case POLICY_MISSING_ATTR:
            return POLICY_MISSING_ATTR;
        default:
            DisplayLog(LVL_MAJOR, POLICY_TAG,
                        "Error checking if entry matches the condition for default");
            return POLICY_ERR;
        }

    }

    /* not matched */
    return POLICY_NO_MATCH;
}

policy_match_t match_scope(const policy_descr_t *pol, const entry_id_t *id,
                           const attr_set_t *attrs, bool warn)
{
    return _entry_matches(id, attrs, &pol->scope, NULL, pol->status_mgr, !warn);
}

#define LOG_MATCH(_m, _id, _a, _p) do { \
        if (log_config.debug_level >= LVL_FULL) { \
            if ((_m) == POLICY_MATCH) \
                DisplayLog(LVL_FULL, POLICY_TAG, "entry "DFID" matches scope for policy %s", \
                           PFID((_id)), (_p)->name); \
            else if ((_m) == POLICY_NO_MATCH) \
                DisplayLog(LVL_FULL, POLICY_TAG, "entry "DFID" doesn't match scope for policy %s", \
                           PFID((_id)), (_p)->name); \
            else if ((_m) == POLICY_MISSING_ATTR) {\
                attr_mask_t tmp = attr_mask_and_not(&(_p)->scope_mask, &(_a)->attr_mask); \
                DisplayLog(LVL_FULL, POLICY_TAG, "missing attrs to determine " \
                           "if entry "DFID" matches scope for policy %s: "     \
                           "scope_mask="DMASK", attr_mask="DMASK", missing="DMASK,    \
                           PFID((_id)), (_p)->name, PMASK(&(_p)->scope_mask), \
                           PMASK(&(_a)->attr_mask), PMASK(&tmp)); \
            } else \
                DisplayLog(LVL_FULL, POLICY_TAG, "entry "DFID": error matching scope for policy %s", \
                       PFID((_id)), (_p)->name); \
        } \
    } while(0)

void add_matching_scopes_mask(const entry_id_t *id, const attr_set_t *attrs,
                              bool tolerant, uint32_t *status_mask)
{
    unsigned int i;
    policy_match_t match;

    for (i = 0; i < policies.policy_count; i++)
    {
        uint32_t curr_mask;

        /* no status */
        if (policies.policy_list[i].status_mgr == NULL)
            continue;

        curr_mask = SMI_MASK(policies.policy_list[i].status_mgr->smi_index);

        /* Avoid rematching if the status is already set in the mask,
         * as it is already matched by another policy. */
        if ((*status_mask) & curr_mask)
            continue;

        if (tolerant)
        {
            match = match_scope(&policies.policy_list[i], id, attrs, false);
            LOG_MATCH(match, id, attrs, &policies.policy_list[i]);

            /* set the current attr bit if it is not sure it doesn't match */
            if (match != POLICY_NO_MATCH)
                *status_mask |= curr_mask;
        }
        else
        {
            match = match_scope(&policies.policy_list[i], id, attrs, true);
            LOG_MATCH(match, id, attrs, &policies.policy_list[i]);

            /* set the current attr bit if it is sure it matches */
            if (match == POLICY_MATCH)
                *status_mask |= curr_mask;
        }
    }
}
