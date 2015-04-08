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
 * policy management
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "policy_rules.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "rbh_modules.h"
#include "rbh_cfg_helpers.h"
#include "status_manager.h"
#include <errno.h>
#include <fnmatch.h>

#define FILESETS_SECTION      "Filesets"
#define FILESET_BLOCK         "FileClass"

#define POLICY_DECLARATION    "define_policy"

#define OLD_PURGEPOLICY_BLOCK     "purge_policies"
#define OLD_MIGRPOLICY_BLOCK      "migration_policies"
#define OLD_UNLINKPOLICY_BLOCK    "hsm_remove_policy"

#define OLD_RULE_BLOCK        "policy"
#define RULE_BLOCK            "rule"

#define OLD_POLICIES_BLOCK    "policies"
#define POLICIES_BLOCK        "rules"

#define OLD_ACT_PARAMS        "hints"
#define ACT_PARAMS            "action_params"
#define OLD_ACT_PARAMS_SFX    "_"OLD_ACT_PARAMS
#define ACT_PARAMS_SFX        "_"ACT_PARAMS

#define IGNORE_BLOCK          "ignore"
#define IGNORE_FC             "ignore_fileclass"
#define CONDITION_BLOCK       "condition"
#define DEFINITION_BLOCK      "definition"
#define SCOPE_BLOCK           "scope"

//#define RMDIR_BLOCK      "rmdir_policy"
#define RM_RECURSE_BLOCK "recursive_rmdir"

#define RELOAD_TAG  "PolicyReload"
#define CHK_TAG     "PolicyCheck"
#define LOADER_TAG  "PolicyLoader"

static const policies_t  policy_initializer = {0};
policies_t policies = {0};


#define critical_err_check(_ptr_, _blkname_) do { if (!_ptr_) {\
                                        sprintf(msg_out, "Internal error reading %s block in config file", _blkname_); \
                                        return EFAULT; \
                                    }\
                                } while (0)

#define critical_err_check_goto(_ptr_, _blkname_, _rc, _label) \
            do { if (!_ptr_) {\
                               sprintf(msg_out, "Internal error reading %s block in config file", _blkname_); \
                               (_rc) = EFAULT; \
                               goto _label; \
                             }} while (0)

/**
 * Compare 2 boolean expressions
 * @return TRUE if expression structure changed.
 * @return FALSE if they have the same structure,
 * @return  -1 on error.
 */
int compare_boolexpr(const bool_node_t * expr1, const bool_node_t * expr2)
{
    if (expr1->node_type != expr2->node_type)
        return 1;

    switch (expr1->node_type)
    {
    case NODE_UNARY_EXPR:
        if (expr1->content_u.bool_expr.bool_op != expr2->content_u.bool_expr.bool_op)
            return true;

        return compare_boolexpr(expr1->content_u.bool_expr.expr1,
                                 expr2->content_u.bool_expr.expr1);

    case NODE_BINARY_EXPR:
        if (expr1->content_u.bool_expr.bool_op != expr2->content_u.bool_expr.bool_op)
            return true;

        return (compare_boolexpr
                 (expr1->content_u.bool_expr.expr1, expr2->content_u.bool_expr.expr1)
                 || compare_boolexpr(expr1->content_u.bool_expr.expr2,
                                      expr2->content_u.bool_expr.expr2));

    case NODE_CONDITION:
        /* compare criteria */
        if (expr1->content_u.condition->crit != expr2->content_u.condition->crit)
            return true;

        /* compare operator, except for custom cmd and xattr */
        if ((expr1->content_u.condition->crit != CRITERIA_XATTR)
             && (expr1->content_u.condition->op != expr2->content_u.condition->op))
            return true;

        /* same structure */
        return false;
    }

    /* should not happen */
    RBH_BUG("Unexpected node_type in boolean expression");
    return -1;
}                               /* compare_boolexpr */


/**
 * Update the numerical values of a boolean expression.
 * /!\ compare_boolexpr() must have returned 0 (else, unguarantied behavior).
 * @param tgt Boolean expression to be updated
 * @param src Boolean expression to take values from.
 * @return TRUE if expression values have been changed
 * @return FALSE if nothing has been changed
 */
bool update_boolexpr(const bool_node_t * tgt, const bool_node_t * src)
{
    compare_triplet_t *p_triplet1;
    compare_triplet_t *p_triplet2;
    char           tmp_buff1[256];
    char           tmp_buff2[256];
    bool           rc;

    switch (tgt->node_type)
    {
    case NODE_UNARY_EXPR:
        return update_boolexpr(tgt->content_u.bool_expr.expr1, src->content_u.bool_expr.expr1);

    case NODE_BINARY_EXPR:
        rc = update_boolexpr(tgt->content_u.bool_expr.expr1, src->content_u.bool_expr.expr1);
        if (update_boolexpr(tgt->content_u.bool_expr.expr2, src->content_u.bool_expr.expr2))
            rc = true;
        return rc;

    case NODE_CONDITION:

        p_triplet1 = tgt->content_u.condition;
        p_triplet2 = src->content_u.condition;

        switch (p_triplet1->crit)
        {
        case CRITERIA_SIZE:
            if (p_triplet1->val.size != p_triplet2->val.size)
            {
                FormatFileSize(tmp_buff1, 256, p_triplet1->val.size);
                FormatFileSize(tmp_buff2, 256, p_triplet2->val.size);

                DisplayLog(LVL_EVENT, RELOAD_TAG,
                            "Criteria value updated: (%s %s %s) -> (%s %s %s)",
                            criteria2str(CRITERIA_SIZE), op2str(p_triplet1->op), tmp_buff1,
                            criteria2str(CRITERIA_SIZE), op2str(p_triplet2->op), tmp_buff2);
                p_triplet1->val.size = p_triplet2->val.size;
                return true;
            }
            else
                return false;

            /* integer conditions */
        case CRITERIA_DEPTH:
#ifdef _LUSTRE
        case CRITERIA_OST:
#endif

#ifdef ATTR_INDEX_dircount
        case CRITERIA_DIRCOUNT:
#endif
            if (p_triplet1->val.integer != p_triplet2->val.integer)
            {
                DisplayLog(LVL_EVENT, RELOAD_TAG,
                            "Criteria value updated: (%s %s %d) -> (%s %s %d)",
                            criteria2str(p_triplet1->crit), op2str(p_triplet1->op),
                            p_triplet1->val.integer, criteria2str(p_triplet2->crit),
                            op2str(p_triplet2->op), p_triplet2->val.integer);
                p_triplet1->val.integer = p_triplet2->val.integer;
                return true;
            }
            else
                return false;

            /* duration conditions */
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
            if (p_triplet1->val.duration != p_triplet2->val.duration)
            {
                FormatDurationFloat(tmp_buff1, 256, p_triplet1->val.duration);
                FormatDurationFloat(tmp_buff2, 256, p_triplet2->val.duration);
                DisplayLog(LVL_EVENT, RELOAD_TAG,
                            "Criteria value updated: (%s %s %s) -> (%s %s %s)",
                            criteria2str(p_triplet1->crit), op2str(p_triplet1->op), tmp_buff1,
                            criteria2str(p_triplet2->crit), op2str(p_triplet2->op), tmp_buff2);
                p_triplet1->val.duration = p_triplet2->val.duration;
                return true;
            }
            else
                return false;

#ifdef ATTR_INDEX_type
        case CRITERIA_TYPE:
            if (p_triplet1->val.type != p_triplet2->val.type)
            {
                DisplayLog(LVL_EVENT, RELOAD_TAG,
                            "Criteria value updated: (%s %s %s) -> (%s %s %s)",
                            criteria2str(p_triplet1->crit), op2str(p_triplet1->op),
                            type2str(p_triplet1->val.type), criteria2str(p_triplet2->crit),
                            op2str(p_triplet2->op), type2str(p_triplet2->val.type));
                p_triplet1->val.type = p_triplet2->val.type;
                return true;
            }
            else
                return false;
#endif

            /* unmodifiable conditions */
        case CRITERIA_TREE:
        case CRITERIA_PATH:
        case CRITERIA_FILENAME:
        case CRITERIA_OWNER:
        case CRITERIA_GROUP:
#ifdef _LUSTRE
        case CRITERIA_POOL:
#endif
            if (strcmp(p_triplet1->val.str, p_triplet2->val.str))
            {
                DisplayLog(LVL_MAJOR, RELOAD_TAG,
                            "Condition changed on attribute '%s' but this cannot be modified dynamically",
                            criteria2str(p_triplet1->crit));
            }
            return false;

        case CRITERIA_XATTR:
            if (strcmp(p_triplet1->val.str, p_triplet2->val.str)
                 || strcmp(p_triplet1->xattr_name, p_triplet2->xattr_name))
            {
                DisplayLog(LVL_MAJOR, RELOAD_TAG,
                            "xattr condition changed, but it cannot be modified dynamically");
            }
            return false;

        default:
            DisplayLog(LVL_CRIT, RELOAD_TAG,
                        "Unsupported attribute: %s", criteria2str(p_triplet1->crit));
        }

        break;
    }

    /* should not happen */
    RBH_BUG("Unexpected node_type in boolean expression");
    return -1;
}                               /* update_boolexpr */


/** Update whitelist rules */
/* @TODO support whitelist rules update on SIGHUP */
static void __attribute__((__unused__))
            update_whitelist(whitelist_item_t * old_items, unsigned int old_count,
                              whitelist_item_t * new_items, unsigned int new_count,
                              const char *block_name)
{
    unsigned int   i;

    if (old_count != new_count)
    {
        DisplayLog(LVL_MAJOR, RELOAD_TAG,
                    "Whitelist rules count changed in block '%s' but cannot be modified dynamically: whitelist update cancelled",
                    block_name);
        return;
    }

    /* compare whitelist boolean expression structure */
    for (i = 0; i < new_count; i++)
    {
        if ((old_items[i].attr_mask != new_items[i].attr_mask)
             || compare_boolexpr(&old_items[i].bool_expr,
                                  &new_items[i].bool_expr))
        {
            DisplayLog(LVL_MAJOR, RELOAD_TAG,
                        "Whitelist expression #%u changed in block '%s'. "
                        "Only numerical values can be modified dynamically. "
                        "Whitelist update cancelled", i, block_name);
            return;
        }
    }

    /* if they are all the same, update/check their values */

    for (i = 0; i < new_count; i++)
    {
        if (update_boolexpr(&old_items[i].bool_expr, &new_items[i].bool_expr))
        {
            char           criteriastr[2048];
            BoolExpr2str(&old_items[i].bool_expr, criteriastr, 2048);
            DisplayLog(LVL_EVENT, RELOAD_TAG,
                        "Whitelist expression #%u in block '%s' has been updated and is now: %s", i,
                        block_name, criteriastr);
        }
    }

    /* XXX attr_mask is unchanged, since we keep the same expression structures */

}


static void free_whitelist(whitelist_item_t * p_items, unsigned int count)
{
    unsigned int   i;

    /* free boolean expressions */
    for (i = 0; i < count; i++)
    {
        FreeBoolExpr(&p_items[i].bool_expr, false);
    }

    if ((count > 0) && (p_items != NULL))
        free(p_items);
}

/**
 * Analyze a policy action parameter and set policy_action_t accordingly.
 * @param[in]     name         parameter name.
 * @param[in]     value        parameter value.
 * @param[in]     extra        array of extra arguments.
 * @param[in]     extra_cnt    item count in extra array.
 * @param[out]    action       policy_action_t to be filled.
 * @param[in,out] mask         Pointer to the attribute mask of placeholders
 *                             in action command line.
 * @return 0 on success, an error code on error.
 */
int parse_policy_action(const char *name, const char *value,
                        char **extra, unsigned int extra_cnt,
                        policy_action_t *action,
                        uint64_t *mask, char *msg_out)
{
    if (!strcasecmp(value, "cmd"))
    {
        uint64_t m;

        /* external command */
        /* 1 single argument expected */
        if (extra_cnt != 1)
        {
            sprintf(msg_out, "A single argument is expected for cmd. E.g.: %s = cmd(\"myscript.sh\");", name);
            return EINVAL;
        }
        rh_strncpy(action->action_u.command, extra[0],
                   sizeof(action->action_u.command));
        action->type = ACTION_COMMAND;

        /* Get attribute mask for this command, in case it contains attribute
         * placeholder */
        m = params_mask(action->action_u.command, name);
        if (m == (uint64_t)-1LL)
        {
            sprintf(msg_out, "Unexpected parameters in %s cmd", name);
            return EINVAL;
        }
        *mask |= m;
    }
    else /* <module>.<action_name> expected */
    {
        if (extra_cnt != 0)
        {
            sprintf(msg_out, "No extra argument is expected for '%s'", name);
            return EINVAL;
        }
        action->action_u.func.call = module_get_action_by_name(value);
        if (action->action_u.func.call == NULL)
        {
            sprintf(msg_out, "%s: unknown function '%s'", name, value);
            return EINVAL;
        }
        action->action_u.func.name = strdup(value);
        action->type = ACTION_FUNCTION;
    }

    return 0;
}

/** duplicate a string and convert it to lower case */
static char *strdup_lower(const char *str)
{
    char *out;

    out = strdup(str);
    if (!out)
        return NULL;

    /* convert to lower case */
    lowerstr(out);

    return out;
}

static int parse_policy_decl(config_item_t config_blk, const char *block_name,
                             policy_descr_t *policy, bool *manage_deleted,
                             char *msg_out)
{
    int          rc;
    const char  *name;
    char         tmpstr[1024];
    uint64_t     mask;
    char       **extra = NULL;
    unsigned int extra_cnt = 0;
    bool         unique;
    config_item_t sub_item;

    static const char *expect[] =
    {
        "status_manager", "scope", "default_action",
        "default_lru_sort_attr",
        NULL
    };

    name = rh_config_GetBlockId(config_blk);
    if (!name)
    {
        strcpy(msg_out, "Missing name for '"POLICY_DECLARATION"' block "
               "(ex: "POLICY_DECLARATION" my_policy { ...");
        return ENOENT;
    }

    /* @TODO check the policy name is not already used! */

    /* parse the parameter */
    if (strlen(name) > POLICY_NAME_LEN - 1)
    {
        sprintf(msg_out, "Policy name is too long (max: %u).", POLICY_NAME_LEN - 1);
        return EINVAL;
    }
    rh_strncpy(policy->name, name, sizeof(policy->name));

    /* read and parse default_action */
    rc = GetStringParam(config_blk, block_name, "default_action",
                        PFLG_MANDATORY, tmpstr, sizeof(tmpstr), &extra,
                        &extra_cnt, msg_out);
    if (rc)
        return rc;

    rc = parse_policy_action("default_action", tmpstr, extra, extra_cnt,
                             &policy->default_action,
                             &policy->rules.run_attr_mask, msg_out);
    if (rc)
        return rc;

    rc = GetStringParam(config_blk, block_name, "default_lru_sort_attr",
                        PFLG_NO_WILDCARDS | PFLG_MANDATORY, tmpstr, sizeof(tmpstr), NULL, NULL,
                        msg_out);
    if (rc)
        return rc;
    /* is it a time attribute? */
    rc = str2lru_attr(tmpstr);
    if (rc == -1)
    {
        strcpy(msg_out, "time attribute expected for 'default_lru_sort_attr': "
               ALLOWED_LRU_ATTRS_STR"...");
        return EINVAL;
    }
    else
        policy->default_lru_sort_attr = rc;

    extra = NULL;
    extra_cnt = 0;
    rc = GetStringParam(config_blk, block_name, "status_manager",
                        PFLG_MANDATORY | PFLG_NO_WILDCARDS, tmpstr, sizeof(tmpstr),
                        &extra, &extra_cnt, msg_out);
    if (rc == ENOENT)
        strcat(msg_out, "\nIf you don't need a status manager, you should explicitely specify: status_manager=none");
    if (rc != 0)
        return rc;

    if (!strcasecmp(tmpstr, "none"))
    {
        policy->status_mgr = NULL;
        if (extra_cnt > 0)
        {
            sprintf(msg_out, "No argument expected after 'status_manager = none': found '%s'",
                    extra[0]);
            return EINVAL;
        }
    }
    else
    {
        policy->status_mgr = create_sm_instance(policy->name, tmpstr);
        if (policy->status_mgr == NULL)
        {
            sprintf(msg_out, "Could not load status manager '%s'", tmpstr);
            return EINVAL;
        }

        if (extra_cnt > 1) /* max 1 argument expected */
        {
            sprintf(msg_out, "Too many arguments (%d) found for status_manager parameter '%s', in block '%s %s'.",
                    extra_cnt, tmpstr, block_name, name);
            return EINVAL;
        }
        else if (extra_cnt == 1)
        {
            /* special values 'removed' or 'deleted' means the policy applies to deleted files */
            if  (!strcasecmp(extra[0], "removed") || !strcasecmp(extra[0], "deleted"))
            {
                /* the status manager must handle them */
                if (!smi_manage_deleted(policy->status_mgr))
                {
                    sprintf(msg_out, "'%s' is specified for status manager '%s' whereas it cannot handle deleted entries.",
                            extra[0], tmpstr);
                    return EINVAL;
                }
                policy->manage_deleted = true;
                *manage_deleted = true;
                policy->implements = strdup_lower(extra[0]);
            }
            /* does the status manager support this action? */
            else if (smi_support_action(policy->status_mgr, extra[0]))
            {
                /* save the implemented action in policy */
                policy->implements = strdup_lower(extra[0]);
            }
            else
            {
                sprintf(msg_out, "status manager '%s' does not support action '%s' in block '%s %s'.",
                        tmpstr, extra[0], block_name, name);
                return EINVAL;
            }
        }
        /* extra_cnt == 0 */
        else if (smi_multi_action(policy->status_mgr))
        {
            sprintf(msg_out, "Missing mandatory argument for status_manager '%s' in block '%s %s': implemented action.",
                    tmpstr, block_name, name);
            return ENOENT;
        }
    }

    /* get scope parameter */
    unique = true;
    sub_item = rh_config_GetItemByName(config_blk, "scope", &unique);

    if (sub_item == NULL)
    {
        sprintf(msg_out, "Missing mandatory parameter 'scope' in block '%s %s'",
                block_name, name);
        return ENOENT;
    }
    if (!unique)
    {
        sprintf(msg_out, "Duplicate scope declaration in block '%s %s', line %d.",
                block_name, name, rh_config_GetItemLine(sub_item));
        return EEXIST;
    }
    if (rh_config_ItemType(sub_item) != CONFIG_ITEM_BLOCK)
    {
        sprintf(msg_out, "A sub-block is expected for '%s' item in block '%s %s', line %d",
                "scope", block_name, name, rh_config_GetItemLine(sub_item));
        return EINVAL;
    }

    /* analyze boolean expression */
    /* pass the status manager instance to interpret status condition
     * depending on the context */
    mask = 0;
    rc = GetBoolExpr(sub_item, SCOPE_BLOCK, &policy->scope, &mask,
                     msg_out, policy->status_mgr);
    if (rc)
        return rc;

    policy->scope_mask = mask;

    CheckUnknownParameters(config_blk, block_name, expect);
    return 0;
}


static int read_policy_definitions(config_file_t config, policies_t *pol,
                                   char *msg_out)
{
    unsigned int blc_index;
    int rc;

    for (blc_index = 0; blc_index < rh_config_GetNbBlocks(config); blc_index++)
    {
        char *block_name;
        config_item_t  curr_item = rh_config_GetBlockByIndex(config, blc_index);
        critical_err_check(curr_item, "root");

        if (rh_config_ItemType(curr_item) != CONFIG_ITEM_BLOCK)
            continue;

        block_name = rh_config_GetBlockName(curr_item);
        critical_err_check(block_name, "root");

        if (!strcasecmp(block_name, POLICY_DECLARATION))
        {
            bool manage_deleted = false;

            if (pol->policy_count == 0)
                pol->policy_list = (policy_descr_t *)malloc(sizeof(policy_descr_t));
            else
                pol->policy_list = (policy_descr_t *)realloc(pol->policy_list,
                        (pol->policy_count + 1) * sizeof(policy_descr_t));

            memset(&pol->policy_list[pol->policy_count], 0, sizeof(policy_descr_t));

            pol->policy_count ++;

            /* analyze policy declaration */
            rc = parse_policy_decl(curr_item, block_name,
                                   &pol->policy_list[pol->policy_count - 1],
                                   &manage_deleted, msg_out);
            if (rc)
                return rc;

            if (manage_deleted)
                pol->manage_deleted = 1;
        }
    }
    return 0;
}

#ifdef HAVE_RMDIR_POLICY

static void set_default_rmdir_policy(rmdir_policy_t *policy)
{
    policy->age_rm_empty_dirs = 0;      /* disable empty dir removal */

    policy->whitelist_rules = NULL;
    policy->whitelist_count = 0;

    /* directory to be removed recursively (even if not empty) */
    policy->recursive_rmdir_rules = NULL;
    policy->recursive_rmdir_count = 0;

    policy->global_attr_mask = 0;
}

static int write_default_rmdir_policy(FILE * output)
{
    print_begin_block(output, 0, RMDIR_BLOCK, NULL);
    print_line(output, 1, "age_rm_empty_dirs    : 0 (disabled)");
    print_line(output, 1, "whitelist rules      : NONE");
    print_line(output, 1, "recursive_rmdir rules: NONE");
    print_end_block(output, 0);

    return 0;
}

static int write_rmdir_policy_template(FILE * output)
{
    print_begin_block(output, 0, RMDIR_BLOCK, NULL);
    print_line(output, 1, "# Remove directories that have been empty for more than 8 days");
    print_line(output, 1, "age_rm_empty_dirs   = 8d ;");
    fprintf(output, "\n");
    print_line(output, 1, "# don't remove dirs that owns to 'root' or whose depth < 2");
    print_begin_block(output, 1, IGNORE_BLOCK, NULL);
    print_line(output, 2, "owner == root");
    print_line(output, 2, "or depth < 2");
    print_end_block(output, 1);
    fprintf(output, "\n");
    print_line(output, 1, "# Recursively remove directories matching this condition");
    print_line(output, 1, "# (/!\\ removes all the directory content)");
    print_begin_block(output, 1, RM_RECURSE_BLOCK, NULL);
    print_line(output, 2, "path == /mnt/lustre/old/*");
    print_line(output, 2, "and last_mod > 30d");
    print_end_block(output, 1);
    print_end_block(output, 0);

    return 0;
}


static int read_rmdir_policy(config_file_t config,  rmdir_policy_t *policy, char *msg_out)
{
    unsigned int   blc_index;
    int            rc, tmpval;

    static const char *rmdir_expect[] = { IGNORE_BLOCK, RM_RECURSE_BLOCK, "age_rm_empty_dirs", NULL };

    /* get RMDIR block */

    config_item_t  rmdir_block = rh_config_FindItemByName(config, RMDIR_BLOCK);

    /* not mandatory */
    if (rmdir_block == NULL)
    {
#ifdef _DEBUG_PARSING
        printf("%s block not found in config file\n", RMDIR_BLOCK);
#endif
        return 0;
    }

    /* initialize global attributes mask */
    policy->global_attr_mask = 0;

    /* read white list rules */

    for (blc_index = 0; blc_index < rh_config_GetNbItems(rmdir_block); blc_index++)
    {
        char          *block_name;
        config_item_t  curr_item = rh_config_GetItemByIndex(rmdir_block, blc_index);
        critical_err_check(curr_item, RMDIR_BLOCK);

        if (rh_config_ItemType(curr_item) != CONFIG_ITEM_BLOCK)
            continue;

        block_name = rh_config_GetBlockName(curr_item);
        critical_err_check(block_name, RMDIR_BLOCK);

#ifdef _DEBUG_PARSING
        printf("subblock found in %s: %s\n", RMDIR_BLOCK, block_name);
#endif

        if (!strcasecmp(block_name, IGNORE_BLOCK))
        {
            if (policy->whitelist_count == 0)
                policy->whitelist_rules =
                    (whitelist_item_t *) malloc(sizeof(whitelist_item_t));
            else
                policy->whitelist_rules =
                    (whitelist_item_t *) realloc(policy->whitelist_rules,
                                                    (policy->whitelist_count +
                                                      1) * sizeof(whitelist_item_t));

            policy->whitelist_count++;

            /* analyze boolean expression */
            /* XXX allow using 'status' in ignore statement? */
            rc = GetBoolExpr(curr_item, block_name,
                              &policy->whitelist_rules[policy->whitelist_count - 1].bool_expr,
                              &policy->whitelist_rules[policy->whitelist_count - 1].attr_mask,
                              msg_out, NULL);

            if (rc)
                return rc;

            policy->global_attr_mask |=
                policy->whitelist_rules[policy->whitelist_count - 1].attr_mask;
        }
        else if (!strcasecmp(block_name, RM_RECURSE_BLOCK))
        {
            if (policy->recursive_rmdir_count == 0)
                policy->recursive_rmdir_rules =
                    (whitelist_item_t *) malloc(sizeof(whitelist_item_t));
            else
                policy->recursive_rmdir_rules =
                    (whitelist_item_t *) realloc(policy->recursive_rmdir_rules,
                                                    (policy->recursive_rmdir_count + 1)
                                                    * sizeof(whitelist_item_t));

            policy->recursive_rmdir_count++;

            /* analyze boolean expression */
            rc = GetBoolExpr(curr_item, block_name,
                              &policy->recursive_rmdir_rules[policy->recursive_rmdir_count - 1].bool_expr,
                              &policy->recursive_rmdir_rules[policy->recursive_rmdir_count - 1].attr_mask,
                              msg_out);

            if (rc)
                return rc;

            policy->global_attr_mask |=
                policy->recursive_rmdir_rules[policy->recursive_rmdir_count - 1].attr_mask;
        }
        else
        {
            sprintf(msg_out, "Unknown sub-block '%s' in " RMDIR_BLOCK " block", block_name);
            return EINVAL;
        }

    }

    /* retrieve age_rm_empty_dirs */

    rc = GetDurationParam(rmdir_block, RMDIR_BLOCK, "age_rm_empty_dirs",
                           INT_PARAM_POSITIVE, &tmpval, NULL, NULL, msg_out);

#ifdef _DEBUG_PARSING
    if (rc)
        printf("no value found for age_rm_empty_dirs\n");
    else
        printf("value found for age_rm_empty_dirs: %d\n", tmpval);
#endif

    if (rc != 0 && rc != ENOENT)
        return rc;
    else if (rc != ENOENT)
        policy->age_rm_empty_dirs = tmpval;

    CheckUnknownParameters(rmdir_block, RMDIR_BLOCK, rmdir_expect);

    return 0;
}

#if 0 /** FIXME RBHv3 UNUSED FOR NOW */
__attribute__((unused))
static int reload_rmdir_policy(rmdir_policy_t *policy)
{
    /** @TODO prevent from concurrent access when rmdir policy is beeing checked */

    update_whitelist(policies.rmdir_policy.whitelist_rules, policies.rmdir_policy.whitelist_count,
                      policy->whitelist_rules, policy->whitelist_count, RMDIR_BLOCK);

    update_whitelist(policies.rmdir_policy.recursive_rmdir_rules, policies.rmdir_policy.recursive_rmdir_count,
                      policy->recursive_rmdir_rules, policy->recursive_rmdir_count, RMDIR_BLOCK);

    /* check age_rm_empty_dirs parameter */

    if (policies.rmdir_policy.age_rm_empty_dirs != policy->age_rm_empty_dirs)
    {
        char           tmp_buff1[256];
        char           tmp_buff2[256];

        FormatDurationFloat(tmp_buff1, 256, policies.rmdir_policy.age_rm_empty_dirs);
        FormatDurationFloat(tmp_buff2, 256, policy->age_rm_empty_dirs);
        DisplayLog(LVL_EVENT, RELOAD_TAG, RMDIR_BLOCK "::age_rm_empty_dirs updated: %s->%s",
                    tmp_buff1, tmp_buff2);
        policies.rmdir_policy.age_rm_empty_dirs = policy->age_rm_empty_dirs;
    }

    return 0;
}

#endif /* HAVE_RMDIR_POLICY */

#ifdef HAVE_RM_POLICY
static void set_default_unlink_policy(unlink_policy_t * upol)
{
    upol->hsm_remove = true; /* hsm_remove enabled */
    upol->deferred_remove_delay = 86400; /* 1 day */
}

static int write_default_unlink_policy(FILE * output)
{
    print_begin_block(output, 0, UNLINKPOLICY_BLOCK, NULL);
    print_line(output, 1, "hsm_remove       :  enabled");
    print_line(output, 1, "deferred_remove_delay:  1d");
    print_end_block(output, 0);
    return 0;
}

static int write_unlink_policy_template(FILE * output)
{
    print_begin_block(output, 0, UNLINKPOLICY_BLOCK, NULL);
    print_line(output, 1, "# set this parameter to 'off' for disabling HSM object removal");
    print_line(output, 1, "hsm_remove = enabled;");
    print_line(output, 1, "# delay before impacting object removal in HSM");
    print_line(output, 1, "deferred_remove_delay = 24h;");
    print_end_block(output, 0);
    return 0;
}
#endif

#endif /* 0 */

static int write_default_filesets(FILE *output)
{
    print_begin_block(output, 0, FILESETS_SECTION, NULL);
    print_line(output, 1, "# none");
    print_end_block(output, 0);

    return 0;
}

#if 0
static int write_default_policy(FILE * output, policy_type_t policy_type)
{
    if (policy_type == PURGE_POLICY)
        print_begin_block(output, 0, PURGEPOLICY_BLOCK, NULL);
    else if (policy_type == MIGR_POLICY)
        print_begin_block(output, 0, MIGRPOLICY_BLOCK, NULL);

    print_line(output, 1, "# none");
    print_end_block(output, 0);
    return 0;
}
#endif

static int write_template_filesets(FILE * output)
{
    fprintf(output, "#### File Class definitions ####\n\n");

    print_begin_block(output, 0, FILESETS_SECTION, NULL);

    print_begin_block(output, 1, FILESET_BLOCK, "Experiment_A");

    print_begin_block(output, 2, DEFINITION_BLOCK, NULL);
    print_line(output, 3, "tree == \"/mnt/lustre/dir_A\"");
    print_end_block(output, 2);

#ifdef HAVE_MIGR_POLICY
    print_line(output, 2, "# arbitrary parameters to pass to the migration command");
    print_line(output, 2, "migration_action_params {");
    print_line(output, 2, "    cos      = 3;");
    print_line(output, 2, "    priority = 2;");
    print_line(output, 2, "}");
#endif
#ifdef _LUSTRE_HSM
    print_line(output, 2, "# target archive");
    print_line(output, 2, "lhsm_archive_params { archive_id = 1; }");
#endif
    print_end_block(output, 1);

    fprintf(output, "\n");

    print_begin_block(output, 1, FILESET_BLOCK, "visualization");

    print_begin_block(output, 2, DEFINITION_BLOCK, NULL);
    print_line(output, 3, "tree == \"/mnt/lustre/dir_*\"");
    print_line(output, 3, "and");
    print_line(output, 3, "xattr.user.tag_visu == 1");
    print_end_block(output, 2);

#ifdef HAVE_MIGR_POLICY
    print_line(output, 2, "# arbitrary parameters to pass to the migration command");
    print_line(output, 2, "migration_action_params {");
    print_line(output, 2, "    cos      = 4;");
    print_line(output, 2, "    priority = 5;");
    print_line(output, 2, "}");
#endif
#ifdef _LUSTRE_HSM
    fprintf(output, "\n");
    print_line(output, 2, "# target archive");
    print_line(output, 2, "lhsm_archive_action_params { \"archive_id=2\" ; }");
#endif
    print_end_block(output, 1);
    fprintf(output, "\n");

#ifdef _LUSTRE
    print_begin_block(output, 1, FILESET_BLOCK, "pool_ssd");

    print_begin_block(output, 2, DEFINITION_BLOCK, NULL);
    print_line(output, 3, "ost_pool == \"ssd*\"");
    print_end_block(output, 2);
    print_end_block(output, 1);
    fprintf(output, "\n");
    print_begin_block(output, 1, FILESET_BLOCK, "ost_set");
    print_begin_block(output, 2, DEFINITION_BLOCK, NULL);
    print_line(output, 3, "# condition on ost_index is true");
    print_line(output, 3, "# if one of the storage objects of the file");
    print_line(output, 3, "# matches each condition:");
    print_line(output, 3, "# ost_index == 15 or ost_index == 20");
    print_line(output, 3, "# => a part of the file must be on OST 15 or 20");
    print_line(output, 3, "# ost_index == 15 and ost_index == 20");
    print_line(output, 3, "# => the file must have objects at least on OST 15 and 20");
    print_line(output, 3, "# ost_index != 12 and ost_index != 13");
    print_line(output, 3, "# => the file must not have objects on OSTs 12 and 13");
    print_line(output, 3, "ost_index == 1 or ost_index == 2 or");
    print_line(output, 3, "ost_index == 1 or ost_index == 2 or");
    print_line(output, 3, "ost_index == 3 or ost_index == 4");
    print_end_block(output, 2);
    print_end_block(output, 1);
    fprintf(output, "\n");
#endif

    print_line(output, 1, "# defining fileclass as a union or intersection:");
    print_begin_block(output, 1, FILESET_BLOCK, "visu_expA");
    print_begin_block(output, 2, DEFINITION_BLOCK, NULL);
    print_line(output, 2, "visualization inter Experiment_A");
    print_end_block(output, 2);
    print_end_block(output, 1);
    fprintf(output, "\n");

    print_end_block(output, 0);

    fprintf(output, "\n");

    return 0;
}

#if 0
#ifdef HAVE_MIGR_POLICY

static int write_migration_policy_template(FILE * output)
{

    fprintf(output, "#### Migration policies (archiving) ####\n\n");

    print_begin_block(output, 0, MIGRPOLICY_BLOCK, NULL);


    print_begin_block(output, 1, POLICY_BLOCK, "standard_copy");

    print_line(output, 2, "target_fileclass = experiment_A;");
    print_line(output, 2, "target_fileclass = pool_ssd;");
    fprintf(output, "\n");

    print_line(output, 2, "# Copy a file 6hours after its creation if it as never been archived.");
    print_line(output, 2, "# For next changes, archive it daily.");
    print_line(output, 2, "# In all cases, do nothing when it has been modified too recently (-30min).");
    print_begin_block(output, 2, CONDITION_BLOCK, NULL);
    print_line(output, 3, "((last_archive == 0 and creation > 6h) ");
    print_line(output, 3, "  or last_archive > 1d)");
    print_line(output, 3, "and last_mod > 30min");
    print_end_block(output, 2);

#ifdef _LUSTRE_HSM
    fprintf(output, "\n");
    print_line(output, 2, "# target archive (/!\\ policy archive_id overrides fileset archive_id)");
    print_line(output, 2, "lhsm_archive_hints = "archive_id=3" ;");
#endif

    print_end_block(output, 1);
    fprintf(output, "\n");

    print_begin_block(output, 1, POLICY_BLOCK, "visu_copy");

    print_line(output, 2, "target_fileclass = visualization ;");
    fprintf(output, "\n");

    print_line(output, 2, "# copy those files quickly after they have been modified");
    print_line(output, 2, "# or if they have been archived more that 6h ago");
    print_line(output, 2, "# (if they are still beeing modified)");

    print_begin_block(output, 2, CONDITION_BLOCK, NULL);
    print_line(output, 3, "last_mod > 1h");
    print_line(output, 3, "or");
    print_line(output, 3, "last_archive > 6h");
    print_end_block(output, 2);

    print_end_block(output, 1);
    fprintf(output, "\n");

    print_line(output, 1, "# Default migration policy.");
    print_line(output, 1, "# This applies to files that don't match previous fileclasses, i.e:");
    print_line(output, 1, "#   - don't match the 'ignore' block");
    print_line(output, 1, "#   - don't match a fileclass of 'ignore_fileclass' directives");
    print_line(output, 1, "#   - don't match any 'target_fileclass' of migration policies above");
    print_begin_block(output, 1, POLICY_BLOCK, "default");

    print_begin_block(output, 2, CONDITION_BLOCK, NULL);
    print_line(output, 3, "last_mod > 12h");
    print_end_block(output, 2);

#ifdef _LUSTRE_HSM
    fprintf(output, "\n");
    print_line(output, 2, "# target archive");
    print_line(output, 2, "lhsm_archive_hints = "archive_id=2" ;");
#endif

    print_end_block(output, 1);


    print_end_block(output, 0);
    fprintf(output, "\n");

    return 0;
}
#endif

#ifdef HAVE_PURGE_POLICY
#ifndef _LUSTRE_HSM
/* Template for NON-lustre-HSM purposes */
static int write_purge_policy_template(FILE * output)
{
    print_begin_block(output, 0, PURGEPOLICY_BLOCK, NULL);

    print_line(output, 1, "# Do not purge files accessed recently, or whose size is 0,");
    print_line(output, 1, "# or located in \"/mnt/lustre/system_files\" directory.");
    print_begin_block(output, 1, IGNORE_BLOCK, NULL);
    print_line(output, 2, "last_access < 1h");
    print_line(output, 2, "or size == 0");
    print_line(output, 2, "or tree == \"/mnt/lustre/system_files\"");
    print_end_block(output, 1);

    fprintf(output, "\n");

#ifdef _LUSTRE
    print_line(output, 1, "# do not purge files in FileClass \"pool_ssd\"");
    print_line(output, 1, IGNORE_FC " = pool_ssd ;");
    fprintf(output, "\n");
#endif

    print_line(output, 1, "# Purge files of class 'Experiment_A' after 24h");
    print_begin_block(output, 1, POLICY_BLOCK, "expA_1day");

    print_line(output, 2, "target_fileclass = Experiment_A ;");

    print_begin_block(output, 2, CONDITION_BLOCK, NULL);
    print_line(output, 3, "last_mod > 24h");
    print_line(output, 3, "and");
    print_line(output, 3, "last_access > 6h");
    print_end_block(output, 2);

    print_end_block(output, 1);
    fprintf(output, "\n");

    print_line(output, 1, "# Release files of class 'visualization' after 1 month,");
    print_line(output, 1, "# if they are not accessed for more that 3 days");
    print_begin_block(output, 1, POLICY_BLOCK, "visu_1month");

    print_line(output, 2, "target_fileclass = visualization ;");

    print_begin_block(output, 2, CONDITION_BLOCK, NULL);
    print_line(output, 3, "last_mod > 30d");
    print_line(output, 3, "and");
    print_line(output, 3, "last_access > 3d");
    print_end_block(output, 2);

    print_end_block(output, 1);
    fprintf(output, "\n");

    print_line(output, 1, "# Default purge policy.");
    print_line(output, 1, "# This applies to files that don't match previous fileclasses, i.e:");
    print_line(output, 1, "#   - don't match the 'ignore' block");
    print_line(output, 1, "#   - don't match a fileclass of 'ignore_fileclass' directives");
    print_line(output, 1, "#   - don't match any 'target_fileclass' of purge policies above");
    print_begin_block(output, 1, POLICY_BLOCK, "default");

    print_begin_block(output, 2, CONDITION_BLOCK, NULL);
    print_line(output, 3, "last_access > 12h");
    print_end_block(output, 2);

    print_end_block(output, 1);

    print_end_block(output, 0);
    fprintf(output, "\n");

    return 0;
}

#else

static int write_purge_policy_template(FILE * output)
{
    fprintf(output, "#### Purge policies (space release) ####\n\n");

    print_begin_block(output, 0, PURGEPOLICY_BLOCK, NULL);

    print_line(output, 1, "# do not purge files owned by \"foo\" or \"charlie\"");
    print_begin_block(output, 1, IGNORE_BLOCK, NULL);
    print_line(output, 2, "owner == \"foo\"");
    print_line(output, 2, "or");
    print_line(output, 2, "owner == \"charlie\"");
    print_end_block(output, 1);
    fprintf(output, "\n");

    print_line(output, 1, "# do not purge files in FileClass \"pool_ssd\"");
    print_line(output, 1, IGNORE_FC " = pool_ssd ;");
    fprintf(output, "\n");

    print_begin_block(output, 1, POLICY_BLOCK, "Experiment_A_purge");

    print_line(output, 2, "target_fileclass = experiment_A ;");
    fprintf(output, "\n");

    print_line(output, 2, "# purge files not accessed within the last 6 hours,");
    print_line(output, 2, "# and not copied-in within the last 12 hours");
    print_line(output, 2, "# and copied-out more that 2 hours ago");

    print_begin_block(output, 2, CONDITION_BLOCK, NULL);
    print_line(output, 3, "last_access > 6h");
    print_line(output, 3, "and");
    print_line(output, 3, "last_restore > 12h");
    print_line(output, 3, "and");
    print_line(output, 3, "last_archive > 2h");
    print_end_block(output, 2);

    print_end_block(output, 1);
    fprintf(output, "\n");

    print_begin_block(output, 1, POLICY_BLOCK, "visu_purge");

    print_line(output, 2, "target_fileclass = visualization ;");
    fprintf(output, "\n");

    print_line(output, 2, "# purge files not accessed within the last day,");
    print_line(output, 2, "# or modified during the week");

    print_begin_block(output, 2, CONDITION_BLOCK, NULL);
    print_line(output, 3, "last_access > 1d");
    print_line(output, 3, "and");
    print_line(output, 3, "last_mod > 7d");
    print_end_block(output, 2);

    print_end_block(output, 1);
    fprintf(output, "\n");

    print_line(output, 1, "# Default purge policy.");
    print_line(output, 1, "# This applies to files that don't match previous fileclasses, i.e:");
    print_line(output, 1, "#   - don't match the 'ignore' block");
    print_line(output, 1, "#   - don't match a fileclass of 'ignore_fileclass' directives");
    print_line(output, 1, "#   - don't match any 'target_fileclass' of purge policies above");
    print_begin_block(output, 1, POLICY_BLOCK, "default");

    print_begin_block(output, 2, CONDITION_BLOCK, NULL);
    print_line(output, 3, "last_access > 12h");
    print_end_block(output, 2);

    print_end_block(output, 1);

    print_end_block(output, 0);

    fprintf(output, "\n");

    return 0;
}
#endif /* HSM switch */
#endif /* purge policy */
#endif /* 0 */

action_params_t *get_fileset_policy_params(const fileset_item_t *fileset,
                                           const char *policy_name)
{
    action_params_t *params;
    char *key;

    if (fileset->policy_action_params == NULL)
        return NULL;

    /* convert policy name to lower case */
    key = strdup_lower(policy_name);
    params = g_hash_table_lookup(fileset->policy_action_params, key);
    free(key);

#ifdef _DEBUG_POLICIES
    fprintf(stderr, "Founds parameters for policy '%s' in fileset '%s'\n",
            policy_name, fileset->fileset_id);
#endif

    return params;
}

/**
 * Get an allocated action_params the given fileset and policy.
 * @retval NULL if memory allocation fails.
 */
static action_params_t *alloc_policy_params(fileset_item_t *fset,
                                            const char *policy_name,
                                            char *msg_out)
{
    action_params_t *params = NULL;

    if (fset->policy_action_params == NULL)
    {
        /* allocate an empty hash table */
        fset->policy_action_params = g_hash_table_new_full(
                                               g_str_hash, g_str_equal, free,
                                               (GDestroyNotify)rbh_params_free);

        if (fset->policy_action_params == NULL)
            return NULL;
    }
    else
        params = get_fileset_policy_params(fset, policy_name);

    if (params == NULL)
    {
        /* allocate and add parameters for this policy, if they don't exist */
        params = calloc(1, sizeof(action_params_t));
        if (!params)
            return NULL;

#ifdef _DEBUG_POLICIES
        fprintf(stderr, "Creating parameters for policy '%s' in fileset '%s'\n",
                policy_name, fset->fileset_id);
#endif
        g_hash_table_insert(fset->policy_action_params,
                            strdup_lower(policy_name), params);
    }

    return params;
}

/**
 * Check if a policy name exists in a given policy set.
 * @param[in]  p_pols the list of policies to search in
 * @param[in]  name   the policy name to search for
 * @param[out] index  index of the matching policy in the given list
 */
static bool _policy_exists(const policies_t *p_pols, const char *name, int *index)
{
    int i;

    for (i = 0; i < p_pols->policy_count; i++)
    {
        if (!strcasecmp(name, p_pols->policy_list[i].name))
        {
            if (index != NULL)
                *index = i;
            return true;
        }
    }
    return false;
}

/** Search for a policy name in the global (current) list */
bool policy_exists(const char *name, int *index)
{
    return _policy_exists(&policies, name, index);
}

/**
 * Fill a action_params_t structure from a config block.
 * @param[in]     param_block the action_params configuration block.
 * @param[in,out] params      pointer to action_params_t to be filled.
 * @param[in,out] mask        pointer to the attribute mask of placeholders
 *                            in action param values.
 */
int read_action_params(config_item_t param_block, action_params_t *params,
                       uint64_t *mask, char *msg_out)
{
    int i, rc;

    /* iterate on key/values of an action_params block */
    for (i = 0; i < rh_config_GetNbItems(param_block); i++)
    {
        config_item_t  sub_item = rh_config_GetItemByIndex(param_block, i);
        char          *subitem_name;
        char          *value;
        char          *descr;
        uint64_t       m;
        int            extra = 0;

        rc = rh_config_GetKeyValue(sub_item, &subitem_name, &value, &extra);
        if (rc)
            return rc;
        if (extra)
        {
                sprintf(msg_out, "Unexpected extra argument for parameter '%s' in %s, line %u.",
                        subitem_name, rh_config_GetBlockName(param_block),
                        rh_config_GetItemLine(sub_item));
                return EINVAL;
        }

#ifdef _DEBUG_POLICIES
        fprintf(stderr, "adding parameter[%d]: '%s'\n", i, subitem_name);
#endif

        /* add param to the list (don't allow duplicates) */
        rc = rbh_param_set(params, subitem_name, value, false);
        if (rc)
        {
            if (rc == -EEXIST)
                sprintf(msg_out, "Duplicate key '%s' in block %s, line %d.",
                        subitem_name, rh_config_GetBlockName(param_block),
                        rh_config_GetItemLine(sub_item));
            else
                sprintf(msg_out, "Failed to set key %s: %s", subitem_name,
                        strerror(-rc));
            return -rc;
        }

        /* build description (for logging purpose) */
        if (asprintf(&descr, "%s::%s parameter, line %d",
                     rh_config_GetBlockName(param_block), subitem_name,
                     rh_config_GetItemLine(sub_item)) < 0)
            return ENOMEM;

        /* Get attribute mask for this parameter, in case it contains attribute
         * placeholder */
        m = params_mask(value, descr);
        free(descr);
        if (m == (uint64_t)-1LL)
        {
            sprintf(msg_out, "Unexpected parameters in %s, line %u.",
                    rh_config_GetBlockName(param_block),
                    rh_config_GetItemLine(sub_item));
            return EINVAL;
        }
        *mask |= m;
    }

    return 0;
}


/** read a <policy>_action_params block in a fileset */
static int read_fset_action_params(config_item_t param_block, const char *blk_name,
                                   fileset_item_t *fset, policies_t *p_pols,
                                   char *msg_out)
{
    int    rc = 0;
    char  *pol_name;
    int    pol_idx;
    size_t sfx_len = strlen(ACT_PARAMS_SFX);
    size_t blk_len = strlen(blk_name);
    action_params_t *params;

    if (blk_len < sfx_len)
    {
        sprintf(msg_out, "unexpected block name '%s' in this context, line %d: "
                         "<policy_name>%s expected", blk_name,
                         rh_config_GetItemLine(param_block), ACT_PARAMS_SFX);
        return EINVAL;
    }

    /* parse the name to get the related param structure */
    pol_name = strdup(blk_name);
    if (pol_name == NULL)
    {
        strcpy(msg_out, "could not allocate memory");
        return ENOMEM;
    }

    /* truncate ACT_PARAMS_SFX:
     * 'xxxxx_yy': len=8, sfx_len=3
     * zero str[5]=str[8-3]
     */
    pol_name[blk_len - sfx_len] = '\0';

    pol_idx = -1;
    if (!_policy_exists(p_pols, pol_name, &pol_idx) || pol_idx == -1)
    {
        sprintf(msg_out, "No declaration found for policy '%s' "
                "while processing block '%s' line %d.", pol_name, blk_name,
                rh_config_GetItemLine(param_block));
        rc = ENOENT;
        goto out_free;
    }

    params = alloc_policy_params(fset, pol_name, msg_out);
    if (params == NULL) {
        rc = ENOMEM;
        goto out_free;
    }

#ifdef _DEBUG_POLICIES
    fprintf(stderr, "processing parameters '%s' for fileset '%s'\n", pol_name, fset->fileset_id);
#endif

    rc = read_action_params(param_block, params,
                            &p_pols->policy_list[pol_idx].rules.run_attr_mask,
                            msg_out);

out_free:
    free(pol_name);
    return rc;
}

/**
 * Read a [<policy>_]action_params block in a policy or a rule.
 * @param param_block   The configuration block to read from.
 * @param blk_name      Name of the configuration block.
 * @param policy        Name of the current policy.
 * @param params        The action_param struct to be filled.
 * @param[in,out] mask  Pointer to the attribute mask of placeholders
 *                      in action param values.
 * @param msg_out       Set to detailed error message in case of error.
 */
static int read_policy_action_params(config_item_t param_block,
                                     const char *blk_name,
                                     const char *policy_name,
                                     action_params_t *params,
                                     uint64_t *mask,
                                     char *msg_out)
{
    int      rc = 0;

    /* Check block name: allowed values are 'action_params'
     * and '<policy>_action_params. */
    if (strcasecmp(blk_name, ACT_PARAMS) != 0)
    {
        char *expected;

        if (asprintf(&expected, "%s"ACT_PARAMS_SFX, policy_name) < 0)
            return -ENOMEM;

        /* expected: <policy>_action_params */
        rc = strcasecmp(blk_name, expected);
        free(expected);

        if (rc != 0)
        {
            sprintf(msg_out, "Unexpected block name '%s' (line %u): "ACT_PARAMS
                    " or %s"ACT_PARAMS_SFX" expected.", blk_name,
                     rh_config_GetItemLine(param_block), policy_name);
            return EINVAL;
        }
    }

#ifdef _DEBUG_POLICIES
    fprintf(stderr, "processing parameters for policy '%s'\n", policy_name);
#endif

    return read_action_params(param_block, params, mask, msg_out);
}

/** test if the variable name is a policy hint (deprecated) */
static inline bool match_policy_action_hints(const char *s)
{
    return !fnmatch("*"OLD_ACT_PARAMS_SFX, s, FNM_CASEFOLD)
           || !strcasecmp(s, OLD_ACT_PARAMS);
}

/** test if the variable name is a policy action params */
static inline bool match_policy_action_params(const char *s)
{
    return !fnmatch("*"ACT_PARAMS_SFX, s, FNM_CASEFOLD)
           || !strcasecmp(s, ACT_PARAMS);
}

static void free_fileclass(fileset_item_t *fset)
{
    /* free fileset definition */
    FreeBoolExpr(&fset->definition, false);

    /* free action params */
    if (fset->policy_action_params != NULL)
    {
        g_hash_table_destroy(fset->policy_action_params);
        fset->policy_action_params = NULL;
    }
}

static void free_filesets(policies_t *p_policies)
{
    int i;

    for (i = 0; i < p_policies->fileset_count; i++)
        free_fileclass(&p_policies->fileset_list[i]);

    free(p_policies->fileset_list);
    p_policies->fileset_list = NULL;
    p_policies->fileset_count = 0;
}

/** get fileset from name (iterate up to count) */
static fileset_item_t *_get_fileset_by_name_max(const policies_t *p_policies,
                                                const char *name, int count)
{
    int i;

    for (i = 0; i < count; i++)
    {
        if (!strcasecmp(p_policies->fileset_list[i].fileset_id, name))
            return &p_policies->fileset_list[i];
    }
    return NULL;                /* not found */
}

fileset_item_t *get_fileset_by_name(const policies_t *p_policies,
                                    const char *name)
{
    return _get_fileset_by_name_max(p_policies, name,
                                    p_policies->fileset_count);
}

/** read a fileclass::definition block */
static int read_fileclass_definition(config_item_t cfg_item,
                                     fileset_item_t *fset,
                                     policies_t *p_policies,
                                     char *msg_out)
{
    int rc;

    /* 2 possible definition types expected: boolean expression
     * or fileset union and/or intersection */
    switch (rh_config_ContentType(cfg_item))
    {
        case CONFIG_ITEM_BOOL_EXPR:
            /* analyze boolean expression */
            rc = GetBoolExpr(cfg_item, DEFINITION_BLOCK,
                             &fset->definition, &fset->attr_mask,
                             msg_out, NULL);
            if (rc)
                return rc;
            break;

        case CONFIG_ITEM_SET:
            /* Build a policy boolean expression from a
             * union/intersection or fileclasses */
            rc = GetSetExpr(cfg_item, DEFINITION_BLOCK,
                             &fset->definition, &fset->attr_mask,
                             p_policies, msg_out);
            if (rc)
                return rc;
            break;

        default:
            sprintf(msg_out, "Boolean expression or set-based definition "
                    "expected in block '%s', line %d",
                    rh_config_GetBlockName(cfg_item),
                    rh_config_GetItemLine(cfg_item));
            return EINVAL;
    }

    p_policies->global_fileset_mask |= fset->attr_mask;

    if (fset->attr_mask & (
#ifdef ATTR_INDEX_last_archive
        ATTR_MASK_last_archive |
#endif
#ifdef ATTR_INDEX_last_restore
        ATTR_MASK_last_restore |
#endif
        ATTR_MASK_last_access | ATTR_MASK_last_mod))
    {
       DisplayLog(LVL_MAJOR, CHK_TAG, "WARNING: in FileClass '%s', line %d: "
                  "time-based conditions should be specified in policy "
                  "condition instead of file class definition",
                  fset->fileset_id, rh_config_GetItemLine(cfg_item));
    }

    return 0;
}

/** read a fileclass block */
static int read_fileclass_block(config_item_t class_cfg, policies_t *p_policies,
                                int curr_idx, char *msg_out)
{
    bool            definition_done = false;
    const char     *class_name;
    fileset_item_t *fset;
    int             i, rc;

    /* get fileclass name */
    class_name = rh_config_GetBlockId(class_cfg);

    if ((class_name == NULL) || (strlen(class_name) == 0))
    {
        sprintf(msg_out, "Fileclass name expected for block "
                FILESET_BLOCK", line %d. "
                "e.g. "FILESET_BLOCK" myclass { ...",
                rh_config_GetItemLine(class_cfg));
        return EINVAL;
    }

    /* check that class name is not already used (up to idx-1)*/
    if (_get_fileset_by_name_max(p_policies, class_name, curr_idx) != NULL)
    {
        sprintf(msg_out, "Duplicate fileclass declaration: '%s', line %d.",
                class_name, rh_config_GetItemLine(class_cfg));
        return EINVAL;
    }

    fset = &p_policies->fileset_list[curr_idx];

    rh_strncpy(fset->fileset_id, class_name, FILESET_ID_LEN);

    /* set default */
    fset->matchable = 1;

    for (i = 0; i < rh_config_GetNbItems(class_cfg); i++)
    {
        config_item_t  sub_item = rh_config_GetItemByIndex(class_cfg, i);
        char          *subitem_name;

        critical_err_check(sub_item, FILESET_BLOCK);

        switch (rh_config_ItemType(sub_item))
        {
            case CONFIG_ITEM_BLOCK:
            {
                subitem_name = rh_config_GetBlockName(sub_item);
                critical_err_check(subitem_name, FILESET_BLOCK);

                if (strcasecmp(subitem_name, DEFINITION_BLOCK) == 0)
                {
                    /* check double definition */
                    if (definition_done)
                    {
                        sprintf(msg_out, "Double fileclass definition in "
                                FILESET_BLOCK " block, line %d.",
                                rh_config_GetItemLine(sub_item));
                        return EINVAL;
                    }

                    /* read fileclass definition */
                    rc = read_fileclass_definition(sub_item, fset,
                                                   p_policies, msg_out);
                    if (rc == 0)
                        definition_done = true;
                }
                else if (match_policy_action_params(subitem_name))
                {
                    /* read policy action params */
                    rc = read_fset_action_params(sub_item, subitem_name, fset,
                                                 p_policies, msg_out);
                }
                else
                {
                    sprintf(msg_out, "'%s' sub-block unexpected in "
                            FILESET_BLOCK " block, line %d.",
                            subitem_name, rh_config_GetItemLine(sub_item));
                    rc = EINVAL;
                }
                if (rc)
                    return rc;

                break;
            }
            case CONFIG_ITEM_VAR:
            {
                char          *value = NULL;
                int            extra_args = 0;

                rc = rh_config_GetKeyValue(sub_item, &subitem_name,
                                           &value, &extra_args);
                if (rc)
                    return rc;

                if (!strcasecmp(subitem_name, "report"))
                {
                    if (extra_args)
                    {
                        sprintf(msg_out,
                                "Unexpected arguments for 'report' parameter, line %d.",
                                rh_config_GetItemLine(sub_item));
                        return EINVAL;
                    }
                    int tmp =str2bool(value);
                    if (tmp == -1)
                    {
                        sprintf(msg_out,
                                "Boolean expected for 'report' parameter, line %d.",
                                rh_config_GetItemLine(sub_item));
                        return EINVAL;
                    }
                    p_policies->fileset_list[i].matchable = tmp;
                }
                /* manage archive_id deprecation (now in action_params) */
                else if (!strcasecmp(subitem_name,"archive_id")
                      || !strcasecmp(subitem_name,"archive_num")) /* for backward compat. */
                {
                    sprintf(msg_out, "archive_id parameter (line %u) must be "
                            "specified in a <policy>_action_params block.",
                            rh_config_GetItemLine(sub_item));
                    return EINVAL;
                }
                /* is the variable of the form <policy_name>_hints ? */
                else if (match_policy_action_hints(subitem_name))
                {
                    sprintf(msg_out, "line %u: '<policy>_hints' parameters are no longer supported. "
                            "Define a '<policy>_action_params' block instead.",
                            rh_config_GetItemLine(sub_item));
                    return EINVAL;
                }
                else
                {
                    DisplayLog(LVL_CRIT, "Config Check",
                                "WARNING: unknown parameter '%s' in block '%s' line %d",
                                subitem_name, FILESET_BLOCK, rh_config_GetItemLine(sub_item));
                }
                break;
            }
            default :
                /* unexpected content */
                    sprintf(msg_out,
                             "Unexpected item in "FILESET_BLOCK" block, line %d.",
                             rh_config_GetItemLine(sub_item));
                    return EINVAL;
        } /* switch on item type */

    } /* loop on "fileclass" block contents */

    if (!definition_done)
    {
        sprintf(msg_out,
                "No definition in file class '%s', line %d", class_name,
                rh_config_GetItemLine(class_cfg));
        return ENOENT;
    }
    return 0;
}


/** Read filesets block */
static int read_filesets(config_file_t config, policies_t *p_policies,
                         char *msg_out)
{
    unsigned int   i;
    int            rc;
    config_item_t  fileset_block;

    /* get Filesets block */
    rc = get_cfg_block(config, FILESETS_SECTION, &fileset_block, msg_out);
    if (rc)
        return rc == ENOENT ? 0 : rc; /* not mandatory */

    /* initialize global attributes mask */
    p_policies->global_fileset_mask = 0;

    p_policies->fileset_count = rh_config_GetNbItems(fileset_block);

    /* allocate the fileset list (1/sub-block) */
    if (p_policies->fileset_count > 0)
    {
        p_policies->fileset_list =
            (fileset_item_t *) calloc(p_policies->fileset_count,
                                         sizeof(fileset_item_t));
        if (p_policies->fileset_list == NULL)
            return ENOMEM;
    }
    else
        p_policies->fileset_list = NULL;

    for (i = 0; i < p_policies->fileset_count; i++)
    {
        char          *block_name;
        config_item_t  curr_class = rh_config_GetItemByIndex(fileset_block, i);

        critical_err_check_goto(curr_class, FILESETS_SECTION, rc,
                                clean_filesets);

        if (rh_config_ItemType(curr_class) != CONFIG_ITEM_BLOCK)
        {
            strcpy(msg_out,
                   "Only "FILESET_BLOCK" sub-blocks are expected in "
                   FILESETS_SECTION" section");
            rc = EINVAL;
            goto clean_filesets;
        }
        block_name = rh_config_GetBlockName(curr_class);
        critical_err_check_goto(block_name, FILESETS_SECTION, rc,
                                clean_filesets);

        if (!strcasecmp(block_name, FILESET_BLOCK))
        {
            /* read fileclass block contents */
            rc = read_fileclass_block(curr_class, p_policies, i, msg_out);
            if (rc)
                goto clean_filesets;
        }
        else
        {
            sprintf(msg_out, "'%s' sub-block unexpected in %s section, line %d.",
                    block_name, FILESETS_SECTION, rh_config_GetItemLine(curr_class));
            rc = EINVAL;
            goto clean_filesets;
        }
    } /* end of "filesets" section */

    return 0;

clean_filesets:
    free_filesets(p_policies);
    return rc;
}

#if 0
#ifdef HAVE_RM_POLICY
/** Read filesets block */
static int read_unlink_policy(config_file_t config, unlink_policy_t * pol, char *msg_out)
{
    int            rc;
    int            intval;

    static const char *allowed[] = {
        "hsm_remove", "deferred_remove_delay", NULL };

    /* get unlink policy block */

    config_item_t  param_block = rh_config_FindItemByName(config, UNLINKPOLICY_BLOCK);
    if (param_block == NULL)
    {
        /* no error, because no parameter is mandatory */
        return 0;
    }

    /* check this is a block... */
    if (rh_config_ItemType(param_block) != CONFIG_ITEM_BLOCK)
    {
        strcpy(msg_out, "A block is expected for '" UNLINKPOLICY_BLOCK "' item");
        return EINVAL;
    }

    /* parse parameters */
    rc = GetBoolParam(param_block, UNLINKPOLICY_BLOCK, "hsm_remove",
                       0, &intval, NULL, NULL, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    else if (rc != ENOENT)
        pol->hsm_remove = intval;

    rc = GetDurationParam(param_block, UNLINKPOLICY_BLOCK, "deferred_remove_delay",
                           INT_PARAM_POSITIVE, &intval, NULL, NULL, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    else if (rc != ENOENT)
        pol->deferred_remove_delay = intval;

    CheckUnknownParameters(param_block, UNLINKPOLICY_BLOCK, allowed);

    return 0;

}
#endif
#endif


/** parse a rule config block and fill rule_item_t structure
 * @param[in]  all_policies  Needed to check fileset definition
 * @param[in]  policy        Needed to uild specific parameter name like '<policy>_hints',
  *                          check status manager properties...
 * @param[in]  policy_rules  Needed to check other rule names in the policy
 * @param[out] rule          The rule structure to fill-in
 */
static int parse_rule_block(config_item_t config_item,
                            const char *block_name,
                            const policies_t *all_policies,
                            const policy_descr_t *policy,
                            const policy_rules_t *policy_rules,
                            rule_item_t *rule, char *msg_out)
{
    char    *rule_name;
    bool     is_default = false;
    int      i, j, k, rc;
    uint64_t mask;
    bool     definition_done = false;

    /* initialize output */
    memset(rule, 0, sizeof(rule_item_t));

    /* get policy id */
    rule_name = rh_config_GetBlockId(config_item);
    critical_err_check(rule_name, block_name);

    /* check that this rule name is not already used in this policy */
    for (i = 0; i < policy_rules->rule_count; i++)
    {
        if (!strcasecmp(rule_name, policy_rules->rules[i].rule_id))
        {
            sprintf(msg_out, "Rule name '%s' (line %d) is already used by another rule in the policy.",
                    rule_name, rh_config_GetItemLine(config_item));
            return EINVAL;
        }
    }

    /* is it a default block? */
    is_default = !strcasecmp(rule_name, "default");

    /* save policy id */
    rh_strncpy(rule->rule_id, rule_name, sizeof(rule->rule_id));

    /* read block contents */
    for (i = 0; i < rh_config_GetNbItems(config_item); i++)
    {
        config_item_t  sub_item = rh_config_GetItemByIndex(config_item, i);
        critical_err_check(sub_item, block_name);
        char          *subitem_name;

        if (rh_config_ItemType(sub_item) == CONFIG_ITEM_BLOCK)
        {
            subitem_name = rh_config_GetBlockName(sub_item);
            critical_err_check(subitem_name, block_name);

            /* allowed blocks: action_params and condition */
            if (match_policy_action_params(subitem_name))
            {
                /* read policy action params */
                rc = read_policy_action_params(sub_item, subitem_name,
                                               policy->name,
                                               &rule->action_params,
                                               &rule->attr_mask,
                                               msg_out);
                if (rc)
                    return rc;
                continue;
            }
            else if (strcasecmp(subitem_name, CONDITION_BLOCK) != 0)
            {
                sprintf(msg_out, "'%s' sub-block unexpected in %s block, line %d.",
                         subitem_name, block_name, rh_config_GetItemLine(sub_item));
                return EINVAL;
            }

            /* check double condition */
            if (definition_done)
            {
                sprintf(msg_out, "Double condition in policy %s, line %d.",
                         rule_name, rh_config_GetItemLine(sub_item));
                return EINVAL;
            }

            /* analyze boolean expression */
            /* allow using 'status' related info in conditions */
            mask = 0;
            rc = GetBoolExpr(sub_item, CONDITION_BLOCK, &rule->condition,
                             &mask, msg_out, policy->status_mgr);
            if (rc)
                return rc;

            rule->attr_mask |= mask;
            definition_done = true;
        }
        else /* not a block */
        {
            char          *value = NULL;
            int            extra_args = 0;
            fileset_item_t *fs;

            rc = rh_config_GetKeyValue(sub_item, &subitem_name, &value, &extra_args);
            if (rc)
                return rc;

            /* expected : target filesets or action parameters */
            if (!strcasecmp(subitem_name, "target_fileclass"))
            {
                if (is_default)
                {
                    sprintf(msg_out, "No target_fileclass expected for default policy, line %d.",
                             rh_config_GetItemLine(sub_item));
                    /** @TODO free targets */
                    return EINVAL;
                }

                if (extra_args)
                {
                    sprintf(msg_out,
                             "Unexpected arguments for %s parameter, line %d.",
                             subitem_name, rh_config_GetItemLine(sub_item));
                    return EINVAL;
                }

                /* get the associated fileset */
                fs = get_fileset_by_name(all_policies, value);

                if (fs == NULL)
                {
                    sprintf(msg_out,
                            "Policy rule references unknown fileclass '%s', line %d.", value,
                            rh_config_GetItemLine(sub_item));
                    return EINVAL;
                }
                fs->used_in_policy = 1;

                /* note: matchable is only for the fileclass in DB.
                 * allow using non-matchable in policies */

                /* check that the fileset is not already referenced in the policy */
                for (j = 0; j < policy_rules->rule_count; j++)
                {
                    for (k = 0; k < policy_rules->rules[j].target_count; k++)
                        if (fs == policy_rules->rules[j].target_list[k])
                        {
                            sprintf(msg_out,
                                     "Fileclass '%s' is already a target of policy rule '%s'",
                                     value, policy_rules->rules[j].rule_id);
                            return EINVAL;
                        }
                }

                /* also check ignore_fileclass directives */
                for (j = 0; j < policy_rules->ignore_count; j++)
                {
                    if (fs == policy_rules->ignore_list[j])
                    {
                        sprintf(msg_out,
                                 "Fileclass '%s' is simultaneously ignored and referenced as a target for policy '%s'",
                                 value, rule_name);
                        return EINVAL;
                    }
                }

                /* append the fileset list */
                rule->target_count++;
                rule->target_list = (fileset_item_t **)realloc(rule->target_list,
                                        rule->target_count * sizeof(fileset_item_t *));
                rule->target_list[rule->target_count-1] = fs;

                /* add fileset mask to policy mask */
                rule->attr_mask |= fs->attr_mask;
            }
            else if (!strcasecmp(subitem_name, "action"))
            {
                char **extra_arg_tab = NULL;

                /* get extra args if there are */
                if (extra_args)
                    extra_args = rh_config_GetExtraArgs(sub_item, &extra_arg_tab);

                /* action defined at the policy level: overrides policy defaults */
                rc = parse_policy_action("action", value, extra_arg_tab,
                                         extra_args, &rule->action,
                                         &rule->attr_mask, msg_out);
                if (rc)
                    return rc;
            }
            /* manage action_hints deprecation (now in action_params) */
            else if (match_policy_action_hints(subitem_name))
            {
                sprintf(msg_out, "line %u: '%s' parameters are no longer supported. "
                        "Define an 'action_params' block instead.",
                        rh_config_GetItemLine(sub_item), subitem_name);
                return EINVAL;
            }
            /* manage archive_id deprecation (now in action_params) */
            else if (!strcasecmp(subitem_name, "archive_id")
                     || !strcasecmp(subitem_name, "archive_num")) /* for backward compat */
            {
                sprintf(msg_out, "archive_id parameter (line %u) must be "
                        "specified in a <policy>_action_params block.",
                        rh_config_GetItemLine(sub_item));
                return EINVAL;
            }
            else
            {
                DisplayLog(LVL_CRIT, "Config Check",
                            "WARNING: unknown parameter '%s' in block '%s' line %d",
                            subitem_name, block_name, rh_config_GetItemLine(sub_item));
            }
        } /* end of vars */
    }     /* loop on "rule" block content */

    if (!definition_done)
    {
        DisplayLog(LVL_MAJOR, CHK_TAG, "WARNING: in policy '%s', line %d: no condition specified!",
                   rule_name, rh_config_GetItemLine(config_item));
    }

    return 0;

}

static void free_policy_rules(policy_rules_t *rules)
{
if (rules->rules)
    /** FIXME rules contents must also be freed */
    free(rules->rules);
if (rules->ignore_list)
    free(rules->ignore_list);
if (rules->whitelist_count > 0)
    free_whitelist(rules->whitelist_rules, rules->whitelist_count);
else if (rules->whitelist_rules) /* preallocated? */
    free(rules->whitelist_rules);

    rules->rules = NULL;
    rules->ignore_list = NULL;
    rules->whitelist_rules = NULL;
    rules->whitelist_count = 0;
}

static void free_policy_descr(policy_descr_t *descr)
{
    /** FIXME free scope + sm_instance (+default action parameters) */
    free_policy_rules(&descr->rules);
}

/* macro for preallocating array depending on configuration blocks in Read_Policy_ */
#define PREALLOC_ARRAY_CONFIG(_block_name_, _type_, _array_var, _goto_label)      \
    do {                                                                          \
        count = rh_config_CountItemNames(section, _block_name_);                  \
        if (count > 0)                                                            \
        {                                                                         \
            rules->_array_var = (_type_ *)calloc(count, sizeof(_type_));          \
            if (rules->_array_var == NULL)                                        \
            {                                                                     \
                rc = ENOMEM;                                                      \
                goto _goto_label;                                                 \
            }                                                                     \
        }                                                                         \
        else if (count == 0)                                                      \
        {                                                                         \
            rules->_array_var = NULL;                                             \
        }                                                                         \
        else                                                                      \
        {                                                                         \
            rc = -1;                                                              \
            goto _goto_label;                                                     \
        }                                                                         \
    }                                                                             \
    while(0)


static int read_policy(config_file_t config, const policies_t *p_policies, char *msg_out,
                       policy_descr_t *policy_descr)
{
    unsigned int   i, j, k;
    int            rc, count;

    policy_rules_t *rules;
    config_item_t   section;
    /* 16: strlen("_policy") + aligned padding */
    char            section_name[POLICY_NAME_LEN+16] = "";

/* macros for cleaner code */
    rules = &policy_descr->rules;
#define curr_ign        rules->whitelist_count
#define curr_ign_fc     rules->ignore_count
#define curr_rule       rules->rule_count

    /* initialize output */
    memset(rules, 0, sizeof(*rules));

    /* check if the new name exists first */
    snprintf(section_name, sizeof(section_name)-1, "%s_%s",
             policy_descr->name, POLICIES_BLOCK);
    section_name[sizeof(section_name)-1] = '\0';

    /* get policy section */
    rc = get_cfg_block(config, section_name, &section, msg_out);
    if (rc == ENOENT)
    {
        /* try with old block name */
        snprintf(section_name, sizeof(section_name)-1, "%s_%s",
                 policy_descr->name, OLD_POLICIES_BLOCK);
        section_name[sizeof(section_name)-1] = '\0';

        /* get policy section */
        rc = get_cfg_block(config, section_name, &section, msg_out);
        if (rc == ENOENT)
            /* not mandatory: no error */
            return 0;
        else if (rc != 0)
            return rc;

        /* Deprecation warning */
        DisplayLog(LVL_MAJOR, LOADER_TAG, "WARNING: '*_"OLD_POLICIES_BLOCK
                   "' block names are deprecated. Rename '%s' block to "
                   "'%s_"POLICIES_BLOCK"'.", section_name,
                   policy_descr->name);
    }

    /* prealloc config arrays */
    PREALLOC_ARRAY_CONFIG(IGNORE_BLOCK, whitelist_item_t, whitelist_rules, err);
    PREALLOC_ARRAY_CONFIG(IGNORE_FC, fileset_item_t *, ignore_list, err);

    /* don't use PREALLOC_ARRAY_CONFIG for rules, as we also accept old rule name (policy)  */
    //PREALLOC_ARRAY_CONFIG(RULE_BLOCK, rule_item_t, rules, free_ignore_fc);
    count = rh_config_CountItemNames(section, RULE_BLOCK)+
                rh_config_CountItemNames(section, OLD_RULE_BLOCK);
    if (count > 0)
    {
        rules->rules = (rule_item_t *)calloc(count, sizeof(rule_item_t));
        if (rules->rules == NULL)
        {
            rc = ENOMEM;
            goto err;
        }
    }
    else if (count == 0)
    {
        rules->rules = NULL;
    }
    else
    {
        rc = -1;
        goto err;
    }

    count = rh_config_GetNbItems(section);

    /* read sub-blocks */
    for (i = 0; i < count; i++)
    {
        char          *item_name;
        config_item_t  curr_item = rh_config_GetItemByIndex(section, i);
        critical_err_check_goto(curr_item, section_name, rc, err);

        if (rh_config_ItemType(curr_item) == CONFIG_ITEM_BLOCK)
        {
            item_name = rh_config_GetBlockName(curr_item);
            critical_err_check_goto(item_name, section_name, rc, err);

            if (!strcasecmp(item_name, IGNORE_BLOCK))
            {
                /* analyze boolean expression */
                /* allow using status related info in ignore statement? */
                rc = GetBoolExpr(curr_item, item_name,
                                 &rules->whitelist_rules[curr_ign].bool_expr,
                                 &rules->whitelist_rules[curr_ign].attr_mask,
                                 msg_out, policy_descr->status_mgr);
                if (rc)
                    goto err;

                /* add expression attr mask to policy mask */
                rules->run_attr_mask |= rules->whitelist_rules[curr_ign].attr_mask;
                curr_ign++;
            }
            /* allow 'rule' or 'policy' */
            else if (!strcasecmp(item_name, RULE_BLOCK) || !strcasecmp(item_name, OLD_RULE_BLOCK))
            {
                /* parse 'rule' block */
                rc = parse_rule_block(curr_item, item_name, p_policies,
                                      policy_descr, rules,
                                      &rules->rules[curr_rule],
                                      msg_out);
                if (rc)
                    goto err;

                rules->run_attr_mask |= rules->rules[curr_rule].attr_mask;
                curr_rule++;
            }
            else
            {
                sprintf(msg_out, "'%s' sub-block unexpected in %s block, line %d.",
                         item_name, section_name, rh_config_GetItemLine(curr_item));
                rc = EINVAL;
                goto err;
            }
        }
        else                    /* not a block */
        {
            char          *value;
            int            extra_args = 0;

            rc = rh_config_GetKeyValue(curr_item, &item_name, &value, &extra_args);
            if (rc)
                goto err;

            /* only "ignore_fileclass" expected */
            if (strcasecmp(item_name, IGNORE_FC) != 0)
            {
                sprintf(msg_out, "'%s' parameter unexpected in %s block, line %d.",
                         item_name, section_name, rh_config_GetItemLine(curr_item));
                rc = EINVAL;
                goto err;
            }

            if (extra_args)
            {
                sprintf(msg_out,
                         "Unexpected arguments for %s parameter, line %d.",
                         item_name, rh_config_GetItemLine(curr_item));
                rc = EINVAL;
                goto err;
            }

            /* find fileset in policy */
            rules->ignore_list[curr_ign_fc] =
                get_fileset_by_name(p_policies, value);

            if (rules->ignore_list[curr_ign_fc] == NULL)
            {
                sprintf(msg_out, "Policy definition references unknown fileclass '%s', line %d.",
                         value, rh_config_GetItemLine(curr_item));
                rc = EINVAL;
                goto err;
            }

            rules->ignore_list[curr_ign_fc]->used_in_policy = 1;

            /* check that the fileset is not already referenced in a policy */
            for (j = 0; j < curr_rule; j++)
            {
                for (k = 0; k < rules->rules[j].target_count; k++)
                    if (rules->ignore_list[curr_ign_fc] ==
                         rules->rules[j].target_list[k])
                    {
                        sprintf(msg_out,
                                 "Fileclass '%s' is simultaneously ignored and referenced as a target for policy '%s'",
                                 value, rules->rules[j].rule_id);
                        rc = EINVAL;
                        goto err;
                    }
            }

            /* add fileset attr mask to policy mask */
            rules->run_attr_mask |= rules->ignore_list[curr_ign_fc]->attr_mask;
            curr_ign_fc++;

        }                       /* end of vars */

    }                           /* end of section content */

    return 0;

  err:
    free_policy_rules(rules);

    return rc;
}

/** @TODO manage SM config + SM init */

static int reload_policies(policies_t *p_policies)
{
    if (p_policies->policy_count != policies.policy_count)
    {
        /* policy count changed */
        DisplayLog(LVL_MAJOR, RELOAD_TAG, "Policy count changed %u->%u: "
                   "program restart required (skipping policy rules update).",
                   policies.policy_count, p_policies->policy_count);
    }

    /* TODO reload all policies */

#if 0
    purge_policy_t *policy = (purge_policy_t *) module_config;

    /* Reloading purge policies dynamically is quite complex:
     * 1) compare triggers: if they are the same (same count, same type)
     *    update their simple parameters: thresholds and check interval
     * 2) compare whitelist expressions count and structures.
     *    If the structure is the same (same parameters, comparators, etc.) only update
     *    the numerical values for conditions.
     */

    /** @TODO prevent from concurrent access when purge policy is beeing checked */

    update_whitelist(policies.purge_policy.whitelist_rules, policies.purge_policy.whitelist_count,
                      policy->whitelist_rules, policy->whitelist_count, PURGEPOLICY_BLOCK);

    /* XXX global_attr_mask is unchanged, since we keep the same expressions */

    /* free reloaded config structure (no used anymore) */
    free_whitelist(policy->whitelist_rules, policy->whitelist_count);

#endif
    return 0;
}

static int set_policies(void *cfg,  bool reload)
{
    policies_t *p_policies = (policies_t *)cfg;

    if (reload)
        return reload_policies(p_policies);
    else
        policies = *p_policies;
    return 0;
}

static void set_default_policies(void *module_config)
{
    policies_t    *pol = (policies_t *)module_config;

    *pol = policy_initializer;
}

static int read_policies(config_file_t config, void *cfg, char *msg_out)
{
    policies_t    *pol = (policies_t *)cfg;
    int            rc, i;

    memset(pol, 0, sizeof(*pol));

    /* read policy declarations, allocate policy descriptors */
    rc = read_policy_definitions(config, pol, msg_out);
    if (rc)
        return rc;

    /* load fileset definitions, and check fileset hints against defined policies */
    rc = read_filesets(config, pol, msg_out);
    if (rc)
        return rc;

    /* iterate on declared policies */
    for (i = 0; i < pol->policy_count; i++)
    {
        rc = read_policy(config, pol, msg_out, &pol->policy_list[i]);
        if (rc)
            return rc;
    }

    return 0;
}

static void write_policy_template(FILE *output)
{
    write_template_filesets(output);

// FIXME write policy templates
/*
    print_line(output, 1, "# default sort order for the policy (this is");
    print_line(output, 1, "# overridden by policy parameters::lru_sort_attr)");
    print_line(output, 1, "#default_lru_sort_attr = last_access ;");

    print_line(output, 1, "# Default action for this policy.");
    print_line(output, 1, "# The syntax to call built-in functions is <module_name>.<action_name>");
    print_line(output, 1, "# e.g. common.copy, common.unlink, lhsm.archive, lhsm.release...");
    print_line(output, 1, "default_action = common.unlink ;");
    print_line(output, 1, "# To call a custom script instead, use the following syntax:");
    print_line(output, 1, "# default_action = cmd(\"/usr/bin/move_to_trash.sh {path}\") ;");
    print_line(output, 1, "# Special parameters can passed to the command:");
    print_line(output, 1, "#    {path}: posix path to the entry");
#ifdef _LUSTRE
#   ifdef _HAVE_FID
    print_line(output, 1, "#    {fid}: fid of the entry");
#   endif
    print_line(output, 1, "#    {fsname}: Lustre fsname");
#endif
    print_line(output, 1, "#    {hints}: pass action_hints to the command");
    fprintf(output, "\n");
*/
}

static void write_policy_default(FILE * output)
{
    write_default_filesets(output);
// FIXME write policy defaults
}

static void *policies_cfg_new(void)
{
    return calloc(1, sizeof(policies_t));
}
static void policies_cfg_free(void *arg)
{
    policies_t *cfg = (policies_t*)arg;
    int i;

    if (cfg == NULL)
        return;

    for (i = 0; i < cfg->policy_count; i++)
        free_policy_descr(&cfg->policy_list[i]);

    free(cfg->policy_list);
    cfg->policy_list = NULL;
    cfg->policy_count = 0;

    free_filesets(cfg);
    free(cfg);
}


mod_cfg_funcs_t policies_cfg_hdlr = {
    .module_name = "policies",
    .new         = policies_cfg_new,
    .free        = policies_cfg_free,
    .set_default = set_default_policies,
    .read        = read_policies,
    .set_config  = set_policies,
    .write_default  = write_policy_default,
    .write_template = write_policy_template
};

