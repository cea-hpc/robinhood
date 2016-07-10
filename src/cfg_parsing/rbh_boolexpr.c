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
#include "rbh_boolexpr.h"
#include "rbh_misc.h"
#include "analyze.h"
#include "status_manager.h"
#include "rbh_logs.h"

/**
 *  convert the syntaxic code for comparator to the configuration equivalent code
 */
static inline compare_direction_t syntax2conf_comparator(operator_t op)
{
    switch (op) {
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
static inline bool_op_t syntax2conf_boolop(bool_operator_t boolop)
{
    switch (boolop) {
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

static int process_any_level_condition(char *regexpr, char *err_msg)
{
    char   *curr = strstr(regexpr, "**");
    size_t  len  = strlen(regexpr);

    /* characters before and after '**' can only be '/' */
    for (curr = strstr(regexpr, "**"); curr != NULL;
         curr = strstr(curr + 2, "**")) {
        if (curr > regexpr) {
            char *prev = curr - 1;
            /* check character before '**' */
            if (*prev != '/') {
                sprintf(err_msg,
                        "Character before and after '**' must be a '/' in '%s'",
                        regexpr);
                return EINVAL;
            }
        }
        /* - last char is 'regexpr + len - 1'
         * - curr + 2 is the first char after '**'
         */
        if ((curr + 2) <= (regexpr + len - 1)) {
            /* check the character after '**' */
            if (curr[2] != '/') {
                sprintf(err_msg,
                        "Character before and after '**' must be a '/' in '%s'",
                        regexpr);
                return EINVAL;
            }
        }
    }

    for (curr = strchr(regexpr, '*'); curr != NULL;
         curr = strchr(curr + 2, '*')) {
        if (curr[1] != '*') {
            sprintf(err_msg,
                    "Single wildcard '*' cannot be used in the same expression as double wildcard '**' in '%s'",
                    regexpr);
            return EINVAL;
        }
    }

    /* non escaped '?' must be replaced by '[!/]'
     * '**' must be replaced by '*'
     */
    str_subst(regexpr, "?", "[!/]");
    str_subst(regexpr, "**", "*");

    return 0;
}

#if (!defined (_LUSTRE) || !defined(_HAVE_FID))
#define XATTR_NEED ATTR_MASK_fullpath
#else
#define XATTR_NEED 0
#endif

/** criteria parsing */
static struct criteria_descr_t {
    const char     *name;
    uint32_t        std_attr_mask;
    cfg_param_type  type;
    int             parsing_flags;
} const criteria_descr[] = {
    [CRITERIA_TREE] = {"tree", ATTR_MASK_fullpath, PT_STRING,
                       PFLG_ALLOW_ANY_DEPTH | PFLG_NOT_EMPTY
                       | PFLG_REMOVE_FINAL_SLASH},
    [CRITERIA_PATH] = {"path", ATTR_MASK_fullpath, PT_STRING,
                       PFLG_ALLOW_ANY_DEPTH | PFLG_NOT_EMPTY
                       | PFLG_REMOVE_FINAL_SLASH},
    [CRITERIA_FILENAME] = {"name", ATTR_MASK_name, PT_STRING,
                           PFLG_NOT_EMPTY | PFLG_NO_SLASH},
    [CRITERIA_TYPE] = {"type", ATTR_MASK_type, PT_TYPE, 0},
    [CRITERIA_OWNER] = {"owner", ATTR_MASK_uid, PT_STRING, PFLG_NOT_EMPTY},
    [CRITERIA_GROUP] = {"group", ATTR_MASK_gid, PT_STRING, PFLG_NOT_EMPTY},
    [CRITERIA_SIZE] = {"size", ATTR_MASK_size, PT_SIZE,
                       PFLG_POSITIVE | PFLG_COMPARABLE},
    [CRITERIA_DEPTH] = {"depth", ATTR_MASK_depth, PT_INT,
                        PFLG_POSITIVE | PFLG_COMPARABLE},
    [CRITERIA_DIRCOUNT] = {"dircount", ATTR_MASK_dircount, PT_INT,
                           PFLG_POSITIVE | PFLG_COMPARABLE},
    [CRITERIA_LAST_ACCESS] = {"last_access", ATTR_MASK_last_access, PT_DURATION,
                              PFLG_POSITIVE | PFLG_COMPARABLE},
    [CRITERIA_LAST_MOD] = {"last_mod", ATTR_MASK_last_mod, PT_DURATION,
                           PFLG_POSITIVE | PFLG_COMPARABLE},
    [CRITERIA_LAST_MDCHANGE] =
        {"last_mdchange", ATTR_MASK_last_mdchange, PT_DURATION,
         PFLG_POSITIVE | PFLG_COMPARABLE},
    [CRITERIA_CREATION] = {"creation", ATTR_MASK_creation_time, PT_DURATION,
                           PFLG_POSITIVE | PFLG_COMPARABLE},
    /* needs a 'remove' status manager */
    [CRITERIA_RMTIME] = {"rm_time", ATTR_MASK_rm_time, PT_DURATION,
                         PFLG_POSITIVE | PFLG_COMPARABLE | PFLG_STATUS},
#ifdef _LUSTRE
    [CRITERIA_POOL] = {"ost_pool", ATTR_MASK_stripe_info, PT_STRING, 0},
    [CRITERIA_OST] =
        {"ost_index", ATTR_MASK_stripe_items, PT_INT, PFLG_POSITIVE},
#endif
    [CRITERIA_FILECLASS] = {"fileclass", ATTR_MASK_fileclass, PT_STRING,
                            PFLG_NO_SLASH},
    /* status mask is context dependent */
    [CRITERIA_STATUS] =
        {"status", 0, PT_STRING, PFLG_STATUS | PFLG_NO_WILDCARDS},
    /* /!\ str2criteria relies on the fact that CRITERIA_XATTR is after
     * the last standard criteria */
    [CRITERIA_XATTR] = {XATTR_PREFIX, XATTR_NEED, PT_STRING, PFLG_XATTR},

    /* CRITERIA_SM_INFO: type and flags are provided by status managers
     * (sm_info_def_t) */
};

const char *criteria2str(compare_criteria_t crit)
{
    if (crit > CRITERIA_XATTR)
        return "?";

    return criteria_descr[crit].name;
}

compare_criteria_t str2criteria(const char *str, const struct sm_instance *smi,
                                const sm_info_def_t **def, unsigned int *idx)
{
    int i;

    /* special case of XATTR criteria: xattr.<attr_name> */
    if (!strncasecmp(str, XATTR_PREFIX ".", strlen(XATTR_PREFIX ".")))
        return CRITERIA_XATTR;

    for (i = 0; i < CRITERIA_XATTR; i++)
        if (!strcasecmp(str, criteria_descr[i].name))
            return i;

    i = sm_attr_get(smi, NULL, str, NULL, def, idx);
    if (i == 0)
        /* found this criteria in SM info */
        return CRITERIA_SM_INFO;

    return NO_CRITERIA;
}

unsigned int str2lru_attr(const char *str, const struct sm_instance *smi)
{
    const sm_info_def_t *def;
    unsigned int idx;
    int rc;

    if (!strcasecmp(str, criteria2str(CRITERIA_LAST_ACCESS)))
        return ATTR_INDEX_last_access;
    else if (!strcasecmp(str, criteria2str(CRITERIA_LAST_MOD)))
        return ATTR_INDEX_last_mod;
    else if (!strcasecmp(str, criteria2str(CRITERIA_LAST_MDCHANGE)))
        return ATTR_INDEX_last_mdchange;
    else if (!strcasecmp(str, criteria2str(CRITERIA_CREATION)))
        return ATTR_INDEX_creation_time;
    else if (!strcasecmp(str, criteria2str(CRITERIA_RMTIME)))
        return ATTR_INDEX_rm_time;
    else if (!strcasecmp(str, "none"))
        return LRU_ATTR_NONE;

    rc = sm_attr_get(smi, NULL, str, NULL, &def, &idx);
    if (rc < 0)
        return LRU_ATTR_INVAL;
    else if (def->crit_type != PT_DURATION)
        return LRU_ATTR_INVAL;

    return idx;
}

#define CHECK_INT_VALUE(_v, _flg) do {\
                if (((_flg) & PFLG_POSITIVE) && ((_v) < 0)) { \
                    sprintf(err_msg, "Positive value expected for %s criteria",\
                            key_value->varname); \
                    return EINVAL; \
                } \
                if (((_flg) & PFLG_NOT_NULL) && ((_v) == 0)) { \
                    sprintf(err_msg, "Null value not allowed for %s criteria", \
                            key_value->varname); \
                    return EINVAL; \
                } \
            } while (0)

static int criteria2condition(const type_key_value *key_value,
                              compare_triplet_t *p_triplet,
                              attr_mask_t *p_attr_mask, char *err_msg,
                              compare_criteria_t crit, cfg_param_type type,
                              attr_mask_t attr_mask, int flags,
                              const sm_instance_t *smi)
{
    /* unexpected status in this context */
    if (flags & PFLG_STATUS) {
        if (smi == NULL) {
            sprintf(err_msg, "'%s' criteria is not expected in this context",
                    key_value->varname);
            return EINVAL;
        } else if (!strcasecmp(key_value->varname, "status"))
            /* status attribute */
            attr_mask_set_index(p_attr_mask,
                                ATTR_INDEX_FLG_STATUS | smi->smi_index);
        else if (smi->sm->flags & SM_DELETED)
            /* attribute for deleted entries (e.g. rm_time) */
            *p_attr_mask = attr_mask_or(p_attr_mask, &attr_mask);
        else {
            /* this status manager does not support deleted entries */
            sprintf(err_msg,
                    "Attribute '%s' only applies to deleted entries but status manager '%s' does not manage deleted entries",
                    key_value->varname, smi->sm->name);
            return EINVAL;
        }
    } else
        *p_attr_mask = attr_mask_or(p_attr_mask, &attr_mask);

    if (crit == CRITERIA_SM_INFO) {
        rh_strncpy(p_triplet->attr_name, key_value->varname,
                   sizeof(p_triplet->attr_name));
    }

    p_triplet->crit = crit;
    p_triplet->op = syntax2conf_comparator(key_value->op_type);

    switch (type) {
    case PT_STRING:
        if ((flags & PFLG_NOT_EMPTY) && EMPTY_STRING(key_value->varvalue)) {
            sprintf(err_msg, "non-empty string expected for %s parameter",
                    key_value->varname);
            return EINVAL;
        }
        if ((flags & PFLG_NO_SLASH) && SLASH_IN(key_value->varvalue)) {
            sprintf(err_msg, "no slash (/) expected in %s parameter",
                    key_value->varname);
            return EINVAL;
        }

        if (global_config.uid_gid_as_numbers &&
            (crit == CRITERIA_OWNER || crit == CRITERIA_GROUP)) {
            db_type_u value;

            if (crit == CRITERIA_OWNER) {
                if (set_uid_val(key_value->varvalue, &value))
                    return EINVAL;
            } else {
                if (set_gid_val(key_value->varvalue, &value))
                    return EINVAL;
            }

            p_triplet->val.integer = value.val_int;

            if (p_triplet->op == COMP_LIKE)
                p_triplet->op = COMP_EQUAL;
            else if (p_triplet->op == COMP_UNLIKE)
                p_triplet->op = COMP_DIFF;

            return 0;
        }

        /* in case the string contains regexpr, those comparators
         * are changed to LIKE / UNLIKE */
        if (WILDCARDS_IN(key_value->varvalue)) {
            if (flags & PFLG_NO_WILDCARDS) {
                sprintf(err_msg, "No wildcard is allowed in %s criteria",
                        key_value->varname);
                return EINVAL;
            }

            if (p_triplet->op == COMP_EQUAL)
                p_triplet->op = COMP_LIKE;
            else if (p_triplet->op == COMP_DIFF)
                p_triplet->op = COMP_UNLIKE;
        }

        rh_strncpy(p_triplet->val.str, key_value->varvalue,
                   sizeof(p_triplet->val.str));

        if (flags & PFLG_XATTR) {
            char *p_xattr = strchr(key_value->varname, '.');
            p_xattr++;
            rh_strncpy(p_triplet->attr_name, p_xattr,
                       sizeof(p_triplet->attr_name));
        }
        /* PFLG_STATUS flag means the attibute is only allowed
         * in a policy scope. This is the case of 'status',
         * but also 'rm_time'... */
        else if ((flags & PFLG_STATUS)
                 && !strcasecmp(key_value->varname, "status")) {

            if ((get_status_str(smi->sm, p_triplet->val.str) == NULL)
                && (strlen(p_triplet->val.str) > 0)) {
                char tmp[RBH_NAME_MAX];

                /* non empty config parameter with NULL match
                 * => invalid status name */
                sprintf(err_msg, "Invalid status '%s' for '%s' status manager: "
                        "allowed values are %s",
                        key_value->varvalue, smi->sm->name,
                        allowed_status_str(smi->sm, tmp, sizeof(tmp)));
                return EINVAL;
            }
        } else if (ANY_LEVEL_MATCH(p_triplet->val.str)) {
            /* don't care for xattr and status value */
            if (flags & PFLG_ALLOW_ANY_DEPTH) {
                int rc;

                /* check the expression and adapt it to fnmatch */
                rc = process_any_level_condition(p_triplet->val.str, err_msg);
                if (rc)
                    return rc;
                p_triplet->flags |= CMP_FLG_ANY_LEVEL;
            } else {
                sprintf(err_msg,
                        "double star wildcard (**) not expected in %s parameter",
                        key_value->varname);
                return EINVAL;
            }
        }
        if ((flags & PFLG_REMOVE_FINAL_SLASH)
            && FINAL_SLASH(p_triplet->val.str))
            REMOVE_FINAL_SLASH(p_triplet->val.str);

        break;

    case PT_SIZE:
        /* a size is expected */
        p_triplet->val.size = str2size(key_value->varvalue);
        if (p_triplet->val.size == -1LL) {
            sprintf(err_msg, "%s criteria: invalid format for size: '%s'",
                    key_value->varname, key_value->varvalue);
            return EINVAL;
        }
        CHECK_INT_VALUE(p_triplet->val.size, flags);
        break;

    case PT_INT:
        p_triplet->val.integer = str2int(key_value->varvalue);
        if (p_triplet->val.integer == -1) {
            sprintf(err_msg, "%s criteria: integer expected: '%s'",
                    key_value->varname, key_value->varvalue);
            return EINVAL;
        }
        CHECK_INT_VALUE(p_triplet->val.integer, flags);
        break;

    case PT_BOOL:
        p_triplet->val.integer = str2bool(key_value->varvalue);
        if (p_triplet->val.integer == -1) {
            sprintf(err_msg,
                    "%s criteria: boolean expected (0, 1, true, false, yes, no, enabled, disabled): '%s'",
                    key_value->varname, key_value->varvalue);
            return EINVAL;
        }
        break;

    case PT_DURATION:
        p_triplet->val.duration = str2duration(key_value->varvalue);
        if (p_triplet->val.duration == -1) {
            sprintf(err_msg, "%s criteria: duration expected: '%s'",
                    key_value->varname, key_value->varvalue);
            return EINVAL;
        }
        CHECK_INT_VALUE(p_triplet->val.duration, flags);
        break;

    case PT_TYPE:
        p_triplet->val.type = str2type(key_value->varvalue);
        if (p_triplet->val.type == TYPE_NONE) {
            strcpy(err_msg, "Illegal condition on type: file, directory, "
                   "symlink, chr, blk, fifo or sock expected.");
            return EINVAL;
        }
        break;

    default:
        sprintf(err_msg, "Unsupported criteria type for '%s'",
                key_value->varname);
        return ENOTSUP;
    }

    /* > or < for a non comparable item */
    if (!(flags & PFLG_COMPARABLE)
        && (p_triplet->op != COMP_EQUAL)
        && (p_triplet->op != COMP_DIFF)
        && (p_triplet->op != COMP_LIKE)
        && (p_triplet->op != COMP_UNLIKE)) {
        sprintf(err_msg,
                "Illegal comparator for %s criteria: == or != expected",
                key_value->varname);
        return EINVAL;
    }

    return 0;
}

/**
 *  interpret and check a condition.
 */
static int interpret_condition(type_key_value *key_value,
                               compare_triplet_t *p_triplet,
                               attr_mask_t *p_attr_mask, char *err_msg,
                               const sm_instance_t *smi)
{
    const struct criteria_descr_t *pcrit;
    const sm_info_def_t *def;
    unsigned int idx;
    attr_mask_t tmp = null_mask;
    /* check the name for the condition */
    compare_criteria_t crit = str2criteria(key_value->varname, smi, &def, &idx);

    if (crit == NO_CRITERIA) {
        sprintf(err_msg, "Unknown or unsupported criteria '%s'",
                key_value->varname);
        return EINVAL;
    }

    p_triplet->flags = 0;

    /* lighten the following line of code */
    pcrit = &criteria_descr[crit];

    if (crit == CRITERIA_SM_INFO) {
        cfg_param_type t = def->crit_type;
        int pflags = (t == PT_DURATION || t == PT_SIZE || t == PT_INT
                      || t == PT_INT64 || t == PT_FLOAT) ? PFLG_COMPARABLE : 0;

        attr_mask_set_index(&tmp, idx);

        return criteria2condition(key_value, p_triplet, p_attr_mask, err_msg,
                                  crit, t, tmp, pflags, smi);
    } else {
        tmp.std = pcrit->std_attr_mask;

        return criteria2condition(key_value, p_triplet, p_attr_mask, err_msg,
                                  crit, pcrit->type, tmp, pcrit->parsing_flags,
                                  smi);
    }
}

/**
 *  Recursive function for building boolean expression.
 */
static int build_bool_expr(type_bool_expr *p_in_bool_expr,
                           bool_node_t *p_out_node,
                           attr_mask_t *p_attr_mask, char *err_msg,
                           const sm_instance_t *smi)
{
    int rc;

    switch (p_in_bool_expr->type) {
    case BOOL_CONDITION:
        p_out_node->node_type = NODE_CONDITION;
        p_out_node->content_u.condition =
            (compare_triplet_t *)malloc(sizeof(compare_triplet_t));
        if (!p_out_node->content_u.condition)
            goto errmem;

        rc = interpret_condition(&p_in_bool_expr->expr_u.key_value,
                                 p_out_node->content_u.condition, p_attr_mask,
                                 err_msg, smi);
        if (rc)
            goto freecondition;
        return 0;

        break;

    case BOOL_UNARY:

        /* in case of identity, directly return sub expression */
        if (p_in_bool_expr->oper == BOOL_OP_IDENTITY)
            return build_bool_expr(p_in_bool_expr->expr_u.members.expr1,
                                   p_out_node, p_attr_mask, err_msg, smi);

        p_out_node->node_type = NODE_UNARY_EXPR;
        p_out_node->content_u.bool_expr.bool_op =
            syntax2conf_boolop(p_in_bool_expr->oper);
        if (p_out_node->content_u.bool_expr.bool_op == BOOL_ERR) {
            strcpy(err_msg, "Unexpected boolean operator in expression");
            return EINVAL;
        }

        p_out_node->content_u.bool_expr.owner = 1;
        p_out_node->content_u.bool_expr.expr1 =
            (bool_node_t *)malloc(sizeof(bool_node_t));
        if (!p_out_node->content_u.bool_expr.expr1)
            goto errmem;
        p_out_node->content_u.bool_expr.expr2 = NULL;

        rc = build_bool_expr(p_in_bool_expr->expr_u.members.expr1,
                             p_out_node->content_u.bool_expr.expr1, p_attr_mask,
                             err_msg, smi);
        if (rc)
            goto free_expr1;
        return 0;

        break;

    case BOOL_BINARY:

        p_out_node->node_type = NODE_BINARY_EXPR;
        p_out_node->content_u.bool_expr.bool_op =
            syntax2conf_boolop(p_in_bool_expr->oper);

        if (p_out_node->content_u.bool_expr.bool_op == BOOL_ERR) {
            strcpy(err_msg, "Unexpected boolean operator in expression");
            return EINVAL;
        }

        p_out_node->content_u.bool_expr.owner = 1;
        p_out_node->content_u.bool_expr.expr1 =
            (bool_node_t *)malloc(sizeof(bool_node_t));
        if (!p_out_node->content_u.bool_expr.expr1)
            goto errmem;
        rc = build_bool_expr(p_in_bool_expr->expr_u.members.expr1,
                             p_out_node->content_u.bool_expr.expr1, p_attr_mask,
                             err_msg, smi);

        if (rc)
            goto free_expr1;

        p_out_node->content_u.bool_expr.expr2 =
            (bool_node_t *)malloc(sizeof(bool_node_t));
        if (!p_out_node->content_u.bool_expr.expr2)
            goto errmem;
        rc = build_bool_expr(p_in_bool_expr->expr_u.members.expr2,
                             p_out_node->content_u.bool_expr.expr2, p_attr_mask,
                             err_msg, smi);

        if (rc)
            goto free_expr2;

        return 0;

        break;

    default:
        sprintf(err_msg, "Invalid boolean node type %d while parsing",
                p_in_bool_expr->type);
        return EINVAL;
    }

 errmem:
    strcpy(err_msg, "Could not allocate memory");
    return ENOMEM;

 freecondition:
    free(p_out_node->content_u.condition);
    p_out_node->content_u.condition = NULL;
    return rc;

 free_expr2:
    free(p_out_node->content_u.bool_expr.expr2);
    p_out_node->content_u.bool_expr.expr2 = NULL;
 free_expr1:
    free(p_out_node->content_u.bool_expr.expr1);
    p_out_node->content_u.bool_expr.expr1 = NULL;
    return rc;
}

/** Create a boolean condition */
int CreateBoolCond(bool_node_t *p_out_node, compare_direction_t compar,
                   compare_criteria_t crit, compare_value_t val,
                   enum compare_flags flags)
{
    p_out_node->node_type = NODE_CONDITION;
    p_out_node->content_u.condition =
        (compare_triplet_t *)malloc(sizeof(compare_triplet_t));
    if (!p_out_node->content_u.condition)
        return -ENOMEM;
    memset(p_out_node->content_u.condition, 0, sizeof(compare_triplet_t));
    p_out_node->content_u.condition->flags = flags;
    p_out_node->content_u.condition->crit = crit;
    p_out_node->content_u.condition->op = compar;
    p_out_node->content_u.condition->val = val;
    return 0;
}

/** Append a boolean condition with bool op = AND */
int AppendBoolCond(bool_node_t *p_in_out_node, compare_direction_t compar,
                   compare_criteria_t crit, compare_value_t val,
                   enum compare_flags flags)
{
    bool_node_t copy_prev = *p_in_out_node;
    int rc = 0;

    p_in_out_node->node_type = NODE_BINARY_EXPR;
    p_in_out_node->content_u.bool_expr.bool_op = BOOL_AND;

    /* bool expr will be allocated */
    p_in_out_node->content_u.bool_expr.owner = 1;

    /* first expression = the previous expression */
    p_in_out_node->content_u.bool_expr.expr1 =
        (bool_node_t *)malloc(sizeof(bool_node_t));
    if (!p_in_out_node->content_u.bool_expr.expr1)
        return -ENOMEM;
    *p_in_out_node->content_u.bool_expr.expr1 = copy_prev;

    /* second expression = the appended value */
    p_in_out_node->content_u.bool_expr.expr2 =
        (bool_node_t *)malloc(sizeof(bool_node_t));
    if (!p_in_out_node->content_u.bool_expr.expr2) {
        rc = -ENOMEM;
        goto free_expr1;
    }

    /* expr2 is a triplet */
    rc = CreateBoolCond(p_in_out_node->content_u.bool_expr.expr2, compar,
                        crit, val, flags);
    if (rc)
        goto free_expr2;

    return 0;

 free_expr2:
    free(p_in_out_node->content_u.bool_expr.expr2);
 free_expr1:
    FreeBoolExpr(p_in_out_node->content_u.bool_expr.expr1, true);
    return rc;
}

int ConstantBoolExpr(bool constant, bool_node_t *p_bool_node)
{
    if (!p_bool_node)
        return EINVAL;

    p_bool_node->node_type = NODE_CONSTANT;
    p_bool_node->content_u.constant = constant;
    return 0;
}

/**
 * Build a policy boolean expression from the given block
 * \param smi(in) when specifying a policy scope, indicate the
 *          related status manager ('status' criteria is policy dependent).
 */
int GetBoolExpr(config_item_t block, const char *block_name,
                bool_node_t *p_bool_node, attr_mask_t *p_attr_mask,
                char *err_msg, const sm_instance_t *smi)
{
    generic_item *curr_block = (generic_item *)block;
    generic_item *subitem;
    int rc;

    /* initialize attr mask */
    *p_attr_mask = null_mask;

    /* check it is a block */
    if (!curr_block || (curr_block->type != TYPE_BLOCK)) {
        sprintf(err_msg, "'%s' is expected to be a block", block_name);
        return EINVAL;
    }

    /* Check the block contains something  */
    if (!curr_block->item.block.block_content) {
        sprintf(err_msg, "'%s' block is empty, line %d", block_name,
                rh_config_GetItemLine(block));
        return ENOENT;
    }

    /* check bloc content */
    subitem = curr_block->item.block.block_content;

    if (subitem->type != TYPE_BOOL_EXPR) {
        sprintf(err_msg, "Boolean expression expected in block '%s', line %d",
                block_name, rh_config_GetItemLine((config_item_t) subitem));
        return EINVAL;
    }

    if (subitem->next) {
        sprintf(err_msg,
                "A single boolean expression is expected in block '%s', line %d",
                block_name, rh_config_GetItemLine((config_item_t) subitem));
        return EINVAL;
    }

    /* now we can analyze the boolean expression */
    rc = build_bool_expr(&subitem->item.bool_expr, p_bool_node, p_attr_mask,
                         err_msg, smi);
    if (rc)
        sprintf(err_msg + strlen(err_msg), ", line %d",
                rh_config_GetItemLine((config_item_t) subitem));

    return rc;

}

/**
 *  Recursive function for freeing boolean expression.
 *  TODO: check these functions, in particular the 'owner'
 *        system, when an expression is a sub-part of another.
 */
int FreeBoolExpr(bool_node_t *p_expr, bool free_top_node)
{
    if (p_expr == NULL)
        return -EFAULT;

    switch (p_expr->node_type) {
    case NODE_CONSTANT:
        /* nothing to free */
        break;

    case NODE_CONDITION:
        free(p_expr->content_u.condition);
        break;

    case NODE_UNARY_EXPR:
        if (p_expr->content_u.bool_expr.owner)
            FreeBoolExpr(p_expr->content_u.bool_expr.expr1, true);
        break;

    case NODE_BINARY_EXPR:
        if (p_expr->content_u.bool_expr.owner) {
            FreeBoolExpr(p_expr->content_u.bool_expr.expr1, true);
            FreeBoolExpr(p_expr->content_u.bool_expr.expr2, true);
        }
        break;
    }

    if (free_top_node)
        free(p_expr);

    return 0;
}

/**
 *  Recursive function for building boolean expression, from a union/intersection
 *  of defined classes.
 */
static int build_set_expr(type_set *p_in_set,
                          bool_node_t *p_out_node, attr_mask_t *p_attr_mask,
                          const policies_t *policies, char *err_msg)
{
    int i, rc;

    if (p_in_set->set_type == SET_SINGLETON) {
        /* get class from its name */
        for (i = 0; i < policies->fileset_count; i++) {
            if (!strcasecmp(policies->fileset_list[i].fileset_id,
                            p_in_set->set_u.name)) {
                /* found */
                *p_out_node = policies->fileset_list[i].definition;
                *p_attr_mask =
                    attr_mask_or(p_attr_mask,
                                 &policies->fileset_list[i].attr_mask);
                /* top level expression is not owner of the content */
                p_out_node->content_u.bool_expr.owner = 0;
                return 0;
            }
        }
        sprintf(err_msg, "FileClass '%s' is undefined", p_in_set->set_u.name);
        return ENOENT;
    } else if (p_in_set->set_type == SET_NEGATION) {
        p_out_node->node_type = NODE_UNARY_EXPR;

        if (p_in_set->set_u.op.oper != SET_OP_NOT) {
            strcpy(err_msg, "Unexpected set operator in unary expression");
            return EINVAL;
        }
        p_out_node->content_u.bool_expr.bool_op = BOOL_NOT;

        p_out_node->content_u.bool_expr.owner = 1;
        p_out_node->content_u.bool_expr.expr1
            = (bool_node_t *)malloc(sizeof(bool_node_t));
        if (!p_out_node->content_u.bool_expr.expr1)
            goto errmem;

        p_out_node->content_u.bool_expr.expr2 = NULL;

        rc = build_set_expr(p_in_set->set_u.op.set1,
                            p_out_node->content_u.bool_expr.expr1,
                            p_attr_mask, policies, err_msg);
        if (rc)
            goto free_set1;
    } else {    /* not a singleton: Union or Inter or Negation */

        p_out_node->node_type = NODE_BINARY_EXPR;

        if (p_in_set->set_u.op.oper == SET_OP_UNION)
            /* entry matches one class OR the other */
            p_out_node->content_u.bool_expr.bool_op = BOOL_OR;
        else if (p_in_set->set_u.op.oper == SET_OP_INTER)
            /* entry matches one class AND the other */
            p_out_node->content_u.bool_expr.bool_op = BOOL_AND;
        else {
            strcpy(err_msg, "Unexpected set operator in expression");
            return EINVAL;
        }

        p_out_node->content_u.bool_expr.owner = 1;
        p_out_node->content_u.bool_expr.expr1
            = (bool_node_t *)malloc(sizeof(bool_node_t));
        if (!p_out_node->content_u.bool_expr.expr1)
            goto errmem;
        rc = build_set_expr(p_in_set->set_u.op.set1,
                            p_out_node->content_u.bool_expr.expr1,
                            p_attr_mask, policies, err_msg);

        if (rc)
            goto free_set1;

        p_out_node->content_u.bool_expr.expr2
            = (bool_node_t *)malloc(sizeof(bool_node_t));
        if (!p_out_node->content_u.bool_expr.expr2)
            goto errmem;
        rc = build_set_expr(p_in_set->set_u.op.set2,
                            p_out_node->content_u.bool_expr.expr2,
                            p_attr_mask, policies, err_msg);
        if (rc)
            goto free_set2;
    }

    return 0;

 errmem:
    sprintf(err_msg, "Could not allocate memory");
    return ENOMEM;

 free_set2:
    free(p_out_node->content_u.bool_expr.expr2);
 free_set1:
    free(p_out_node->content_u.bool_expr.expr1);
    return rc;

}

/**
 * Build a policy boolean expression from a union/intersection of fileclasses
 */
int GetSetExpr(config_item_t block, const char *block_name,
               bool_node_t *p_bool_node, attr_mask_t *p_attr_mask,
               const policies_t *policies, char *err_msg)
{
    generic_item *curr_block = (generic_item *)block;
    generic_item *subitem;
    int rc;

    /* initialize attr mask */
    *p_attr_mask = null_mask;

    /* check it is a block */
    if (!curr_block || (curr_block->type != TYPE_BLOCK)) {
        sprintf(err_msg, "'%s' is expected to be a block", block_name);
        return EINVAL;
    }

    /* Check the block contains something  */
    if (!curr_block->item.block.block_content) {
        sprintf(err_msg, "'%s' block is empty, line %d", block_name,
                rh_config_GetItemLine(block));
        return ENOENT;
    }

    /* check bloc content */
    subitem = curr_block->item.block.block_content;

    if (subitem->type != TYPE_SET) {
        sprintf(err_msg,
                "Union/intersection/negation of classes expected in block '%s', line %d",
                block_name, rh_config_GetItemLine((config_item_t) subitem));
        return EINVAL;
    }

    if (subitem->next) {
        sprintf(err_msg,
                "A single expression is expected in block '%s', line %d",
                block_name, rh_config_GetItemLine((config_item_t) subitem));
        return EINVAL;
    }

    /* now we can analyze the union/intersection */
    rc = build_set_expr(&subitem->item.set, p_bool_node, p_attr_mask,
                        policies, err_msg);
    if (rc)
        sprintf(err_msg + strlen(err_msg), ", line %d",
                rh_config_GetItemLine((config_item_t) subitem));

    return rc;

}

const char *op2str(compare_direction_t comp)
{
    switch (comp) {
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
    case COMP_NONE:
        return "?";
    }
    return "?";
}   /* op2str */

static int print_condition(const compare_triplet_t *p_triplet, char *out_str,
                           size_t str_size)
{
    char tmp_buff[256];

    switch (p_triplet->crit) {
        /* str values */
    case CRITERIA_TREE:
    case CRITERIA_PATH:
    case CRITERIA_FILENAME:
    case CRITERIA_FILECLASS:
#ifdef _LUSTRE
    case CRITERIA_POOL:
#endif
        return snprintf(out_str, str_size, "%s %s \"%s\"",
                        criteria2str(p_triplet->crit), op2str(p_triplet->op),
                        p_triplet->val.str);

    case CRITERIA_TYPE:
        return snprintf(out_str, str_size, "%s %s \"%s\"",
                        criteria2str(p_triplet->crit), op2str(p_triplet->op),
                        type2str(p_triplet->val.type));

        /* int values */
    case CRITERIA_DEPTH:
#ifdef _LUSTRE
    case CRITERIA_OST:
#endif
    case CRITERIA_DIRCOUNT:
        return snprintf(out_str, str_size, "%s %s %d",
                        criteria2str(p_triplet->crit), op2str(p_triplet->op),
                        p_triplet->val.integer);

    case CRITERIA_SIZE:
        FormatFileSize(tmp_buff, 256, p_triplet->val.size);
        return snprintf(out_str, str_size, "%s %s %s",
                        criteria2str(p_triplet->crit), op2str(p_triplet->op),
                        tmp_buff);

        /* UID/GID: str or int */
    case CRITERIA_OWNER:
    case CRITERIA_GROUP:
        if (global_config.uid_gid_as_numbers)
            return snprintf(out_str, str_size, "%s %s %d",
                            criteria2str(p_triplet->crit),
                            op2str(p_triplet->op), p_triplet->val.integer);
        else
            return snprintf(out_str, str_size, "%s %s \"%s\"",
                            criteria2str(p_triplet->crit),
                            op2str(p_triplet->op), p_triplet->val.str);

        /* duration values */

    case CRITERIA_LAST_ACCESS:
    case CRITERIA_LAST_MOD:
    case CRITERIA_LAST_MDCHANGE:
    case CRITERIA_CREATION:
    case CRITERIA_RMTIME:
        FormatDurationFloat(tmp_buff, 256, p_triplet->val.duration);
        return snprintf(out_str, str_size, "%s %s %s",
                        criteria2str(p_triplet->crit), op2str(p_triplet->op),
                        tmp_buff);

    case CRITERIA_XATTR:
        return snprintf(out_str, str_size, XATTR_PREFIX ".%s %s %s",
                        p_triplet->attr_name, op2str(p_triplet->op),
                        p_triplet->val.str);
    default:
        return -EINVAL;
    }
}

/**
 * Print a boolean expression to a string.
 * @return a negative value on error
 *         else, the number of chars written.
 */
int BoolExpr2str(bool_node_t *p_bool_node, char *out_str, size_t str_size)
{
    size_t written = 0;
    int rc;

    switch (p_bool_node->node_type) {
    case NODE_UNARY_EXPR:

        /* only BOOL_NOT is supported as unary operator */
        if (p_bool_node->content_u.bool_expr.bool_op != BOOL_NOT)
            return -EINVAL;
        written = snprintf(out_str, str_size, "NOT (");
        rc = BoolExpr2str(p_bool_node->content_u.bool_expr.expr1,
                          out_str + written, str_size - written);
        if (rc < 0)
            return rc;
        written += rc;
        written += snprintf(out_str + written, str_size - written, ")");
        return written;

    case NODE_BINARY_EXPR:
        written = snprintf(out_str, str_size, "(");
        rc = BoolExpr2str(p_bool_node->content_u.bool_expr.expr1,
                          out_str + written, str_size - written);
        if (rc < 0)
            return rc;
        written += rc;
        if (p_bool_node->content_u.bool_expr.bool_op == BOOL_OR)
            written +=
                snprintf(out_str + written, str_size - written, ") OR (");
        else
            written +=
                snprintf(out_str + written, str_size - written, ") AND (");

        rc = BoolExpr2str(p_bool_node->content_u.bool_expr.expr2,
                          out_str + written, str_size - written);
        if (rc < 0)
            return rc;
        written += rc;

        written += snprintf(out_str + written, str_size - written, ")");
        return written;

    case NODE_CONDITION:
        return print_condition(p_bool_node->content_u.condition, out_str,
                               str_size);

    case NODE_CONSTANT:
        return snprintf(out_str, str_size, "%s",
                        bool2str(p_bool_node->content_u.constant));
    }

    return -EINVAL;
}

/**
 * Compare 2 boolean expressions
 * @return TRUE if expression structure changed.
 * @return FALSE if they have the same structure,
 * @return  -1 on error.
 */
int compare_boolexpr(const bool_node_t *expr1, const bool_node_t *expr2)
{
    if (expr1->node_type != expr2->node_type)
        return 1;

    switch (expr1->node_type) {
    case NODE_UNARY_EXPR:
        if (expr1->content_u.bool_expr.bool_op !=
            expr2->content_u.bool_expr.bool_op)
            return true;

        return compare_boolexpr(expr1->content_u.bool_expr.expr1,
                                expr2->content_u.bool_expr.expr1);

    case NODE_BINARY_EXPR:
        if (expr1->content_u.bool_expr.bool_op !=
            expr2->content_u.bool_expr.bool_op)
            return true;

        return compare_boolexpr(expr1->content_u.bool_expr.expr1,
                                expr2->content_u.bool_expr.expr1)
               || compare_boolexpr(expr1->content_u.bool_expr.expr2,
                                   expr2->content_u.bool_expr.expr2);

    case NODE_CONDITION:
        /* compare criteria */
        if (expr1->content_u.condition->crit !=
            expr2->content_u.condition->crit)
            return true;

        /* compare operator, except for custom cmd and xattr */
        if ((expr1->content_u.condition->crit != CRITERIA_XATTR)
            && (expr1->content_u.condition->op !=
                expr2->content_u.condition->op))
            return true;

        /* same structure */
        return false;

    case NODE_CONSTANT:
        /* same structure, just a value change */
        return false;
    }

    /* should not happen */
    RBH_BUG("Unexpected node_type in boolean expression");
    return -1;
}   /* compare_boolexpr */

#define RELOAD_TAG "ReloadExpr"

/**
 * Update the numerical values of a boolean expression.
 * /!\ compare_boolexpr() must have returned 0 (else, unguarantied behavior).
 * @param tgt Boolean expression to be updated
 * @param src Boolean expression to take values from.
 * @return TRUE if expression values have been changed
 * @return FALSE if nothing has been changed
 */
bool update_boolexpr(bool_node_t *tgt, const bool_node_t *src)
{
    compare_triplet_t *p_triplet1;
    compare_triplet_t *p_triplet2;
    char tmp_buff1[256];
    char tmp_buff2[256];
    bool rc;

    switch (tgt->node_type) {
    case NODE_CONSTANT:
        if (tgt->content_u.constant != src->content_u.constant) {
            DisplayLog(LVL_EVENT, RELOAD_TAG,
                       "Value updated: %s -> %s",
                       bool2str(src->content_u.constant),
                       bool2str(tgt->content_u.constant));
            tgt->content_u.constant = src->content_u.constant;
            return true;
        } else
            return false;

    case NODE_UNARY_EXPR:
        return update_boolexpr(tgt->content_u.bool_expr.expr1,
                               src->content_u.bool_expr.expr1);

    case NODE_BINARY_EXPR:
        rc = update_boolexpr(tgt->content_u.bool_expr.expr1,
                             src->content_u.bool_expr.expr1);
        if (update_boolexpr
            (tgt->content_u.bool_expr.expr2, src->content_u.bool_expr.expr2))
            rc = true;
        return rc;

    case NODE_CONDITION:

        p_triplet1 = tgt->content_u.condition;
        p_triplet2 = src->content_u.condition;

        switch (p_triplet1->crit) {
        case CRITERIA_SIZE:
            if (p_triplet1->val.size != p_triplet2->val.size) {
                FormatFileSize(tmp_buff1, 256, p_triplet1->val.size);
                FormatFileSize(tmp_buff2, 256, p_triplet2->val.size);

                DisplayLog(LVL_EVENT, RELOAD_TAG,
                           "Criteria value updated: (%s %s %s) -> (%s %s %s)",
                           criteria2str(CRITERIA_SIZE), op2str(p_triplet1->op),
                           tmp_buff1, criteria2str(CRITERIA_SIZE),
                           op2str(p_triplet2->op), tmp_buff2);
                p_triplet1->val.size = p_triplet2->val.size;
                return true;
            } else
                return false;

            /* integer conditions */
        case CRITERIA_DEPTH:
#ifdef _LUSTRE
        case CRITERIA_OST:
#endif
        case CRITERIA_DIRCOUNT:
            if (p_triplet1->val.integer != p_triplet2->val.integer) {
                DisplayLog(LVL_EVENT, RELOAD_TAG,
                           "Criteria value updated: (%s %s %d) -> (%s %s %d)",
                           criteria2str(p_triplet1->crit),
                           op2str(p_triplet1->op), p_triplet1->val.integer,
                           criteria2str(p_triplet2->crit),
                           op2str(p_triplet2->op), p_triplet2->val.integer);
                p_triplet1->val.integer = p_triplet2->val.integer;
                return true;
            } else
                return false;

            /* duration conditions */
        case CRITERIA_LAST_ACCESS:
        case CRITERIA_LAST_MOD:
        case CRITERIA_LAST_MDCHANGE:
        case CRITERIA_CREATION:
            if (p_triplet1->val.duration != p_triplet2->val.duration) {
                FormatDurationFloat(tmp_buff1, 256, p_triplet1->val.duration);
                FormatDurationFloat(tmp_buff2, 256, p_triplet2->val.duration);
                DisplayLog(LVL_EVENT, RELOAD_TAG,
                           "Criteria value updated: (%s %s %s) -> (%s %s %s)",
                           criteria2str(p_triplet1->crit),
                           op2str(p_triplet1->op), tmp_buff1,
                           criteria2str(p_triplet2->crit),
                           op2str(p_triplet2->op), tmp_buff2);
                p_triplet1->val.duration = p_triplet2->val.duration;
                return true;
            } else
                return false;

        case CRITERIA_TYPE:
            if (p_triplet1->val.type != p_triplet2->val.type) {
                DisplayLog(LVL_EVENT, RELOAD_TAG,
                           "Criteria value updated: (%s %s %s) -> (%s %s %s)",
                           criteria2str(p_triplet1->crit),
                           op2str(p_triplet1->op),
                           type2str(p_triplet1->val.type),
                           criteria2str(p_triplet2->crit),
                           op2str(p_triplet2->op),
                           type2str(p_triplet2->val.type));
                p_triplet1->val.type = p_triplet2->val.type;
                return true;
            } else
                return false;

            /* unmodifiable conditions */
        case CRITERIA_TREE:
        case CRITERIA_PATH:
        case CRITERIA_FILENAME:
        case CRITERIA_FILECLASS:
#ifdef _LUSTRE
        case CRITERIA_POOL:
#endif
            if (strcmp(p_triplet1->val.str, p_triplet2->val.str)) {
                DisplayLog(LVL_MAJOR, RELOAD_TAG,
                           "Condition changed on attribute '%s' but this cannot be modified dynamically",
                           criteria2str(p_triplet1->crit));
            }
            return false;

        case CRITERIA_OWNER:
        case CRITERIA_GROUP:
            if ((global_config.uid_gid_as_numbers &&
                 p_triplet1->val.integer != p_triplet2->val.integer) ||
                (!global_config.uid_gid_as_numbers &&
                 strcmp(p_triplet1->val.str, p_triplet2->val.str))) {
                DisplayLog(LVL_MAJOR, RELOAD_TAG,
                           "Condition changed on attribute '%s' but this cannot be modified dynamically",
                           criteria2str(p_triplet1->crit));
            }
            return false;

        case CRITERIA_XATTR:
            if (strcmp(p_triplet1->val.str, p_triplet2->val.str)
                || strcmp(p_triplet1->attr_name, p_triplet2->attr_name)) {
                DisplayLog(LVL_MAJOR, RELOAD_TAG,
                           "xattr condition changed, but it cannot be modified dynamically");
            }
            return false;

        default:
            DisplayLog(LVL_CRIT, RELOAD_TAG,
                       "Unsupported attribute: %s",
                       criteria2str(p_triplet1->crit));
        }

        break;
    }

    /* should not happen */
    RBH_BUG("Unexpected node_type in boolean expression");
    return -1;
}   /* update_boolexpr */
