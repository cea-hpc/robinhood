/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright 2015 Cray Inc., All rights reserved.
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

#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "rbh_params.h"
#include "rbh_logs.h"
#include "global_config.h"
#include "rbh_misc.h"

/* avoid linking with all robinhood libs */
log_config_t log_config = { .debug_level = LVL_DEBUG };
global_config_t global_config = { .fs_path = "somefspath" };
unsigned int sm_inst_count;
unsigned int sm_attr_count;

void DisplayLogFn(log_level debug_level, const char *tag, const char *format, ...)
{
    if (LVL_DEBUG >= debug_level)
    {
        va_list args;

        va_start(args, format);
        vprintf(format, args);
        va_end(args);
        printf("\n");
    }
}

const char *config_file_path(void) { return "someconfigfile"; }
const char *get_fsname(void) { return "somefsname"; }

/* dummy ListMgr_PrintAttrPtr() function: avoid linking with all libs */
int ListMgr_PrintAttrPtr(GString *str, db_type_e type,
                         void *value_ptr, const char *quote)
{
    g_string_printf(str, "%p", value_ptr);
    return 0;
}

/* Will substitute values in first column with values in second
 * column. */
static const char *vars[] = {
    "foo", "barbar",
    "hello", "bye",
    "marco", "polo",
    "double", "{marco}",
    "triple", "{double}",
    "explorer", "marco",
    "quote1", "a'b",
    NULL, NULL
};

static const char *find_vars[] = {
    "", "somepath",
    NULL, NULL
};

static const char descr[] = "my test string";

static inline void assert_str_equal(const char *s1, const char *s2)
{
    if (!strcmp(s1, s2))
        return;

    fprintf(stderr, "'%s' differs from '%s'\n", s1, s2);
    abort();
}

/** test with braces in strict mode and non-strict mode
 * @param cmd the   original string
 * @param weak_res  result for weak mode (if different from cmd).
 */
static void test_braces(const char *cmd, const char* weak_res)
{
    char *newcmd;

    /* strict braces mode: should be an error */
    newcmd = subst_params(cmd, descr, NULL, NULL, NULL, vars, NULL, false, true);
    assert(newcmd == NULL);

    /* weak braces mode: no error */
    newcmd = subst_params(cmd, descr, NULL, NULL, NULL, vars, NULL, false, false);
    assert_str_equal(newcmd, weak_res ? weak_res : cmd);
    g_free(newcmd);
}

static void test_subst_params(void)
{
    char       *newcmd;
    const char *cmd;
    int         rc;
    const attr_set_t attrs = {
        .attr_mask = {.std = ATTR_MASK_name | ATTR_MASK_fullpath},
        .attr_values = {
            .name = "somename",
            .fullpath = "somepath",
        }
    };
#ifdef _HAVE_FID
    /* oid, seq, ver */
    entry_id_t id = { 0x1234, 0x5678, 0xabcd };
    const char fid_str[] = "0x1234:0x5678:0xabcd";
#else
    /* fskey, inode, validator */
    entry_id_t id = { 0x8BC1, 12345, 0x1 };
    const char fid_str[] = "8BC1/12345";
#endif
    struct rbh_params params = { 0 };

    /* convert the list to user params */
    rc = rbh_list2params(&params, vars, true);
    assert(rc == 0);

    /*
     * Basic checks. No variable.
     */

    /* Empty string */
    newcmd = subst_params("", descr, NULL, NULL, NULL, NULL, NULL, false, true);
    if (strcmp(newcmd, ""))
        abort();
    g_free(newcmd);

    /* Nothing interresting */
    cmd = "hello";
    newcmd = subst_params(cmd, descr, NULL, NULL, NULL, NULL, NULL, false, true);
    assert_str_equal(newcmd, cmd);
    g_free(newcmd);

    /* Empty variable */
    /* should fail if empty string is not a param */
    newcmd = subst_params("{}", descr, NULL, NULL, NULL, NULL, NULL, false, true);
    assert(newcmd == NULL);

    /* case of 'rbh-find': {} is expended to path */
    newcmd = subst_params("{}", descr, NULL, NULL, NULL, find_vars, NULL, false, true);
    assert_str_equal(newcmd, "somepath");
    g_free(newcmd);

    /* One unknown variable */
    newcmd = subst_params("{hello}", descr, NULL, NULL, NULL, NULL, NULL, false, true);
    assert(newcmd == NULL);

    /* One unknown variable with text before */
    newcmd = subst_params("qwerty{hello}", descr, NULL, NULL, NULL, NULL, NULL, false, true);
    assert(newcmd == NULL);

    /* One unknown variable with text after */
    newcmd = subst_params("{hello}cvbn", descr, NULL, NULL, NULL, NULL, NULL, false, true);
    assert(newcmd == NULL);

    /* One unknown variable with text around */
    newcmd = subst_params("qwerty{hello}cvbn", descr, NULL, NULL, NULL, NULL, NULL, false, true);
    assert(newcmd == NULL);

    /* two unknown variables */
    newcmd = subst_params("{azerty}{hello}", descr, NULL, NULL, NULL, NULL, NULL, false, true);
    assert(newcmd == NULL);

    /* two unknown variables with text around */
    newcmd = subst_params("jgds{azerty}lgkfhd{hello}iub", descr, NULL, NULL, NULL, NULL, NULL, false, true);
    assert(newcmd == NULL);

    /* string with lone { */
    test_braces("qwerty{hellocvbn", NULL);

    /* string with lone } */
    test_braces("qwerty}hellocvbn", NULL);

    /* string with 2 { and 1 } (std variable) */
    test_braces("qwerty{{fsroot}cvbn", "qwerty{somefspathcvbn");

    /* string with 2 { */
    test_braces("qwerty{{hellocvbn", NULL);

    /* string with 2 { */
    test_braces("qwerty{ghfd{hellocvbn", NULL);

    /* string with inverted {} */
    test_braces("qwerty}ghfd{hellocvbn", NULL);

    /* string with 2 { and 1 } (additional variable) */
    test_braces("qwerty{ghfd{hello}cvbn", "qwerty{ghfdbyecvbn");

    /* string with 1 { and 2 }} (additional variable) */
    test_braces("qwertyghfd{hello}}cvbn", "qwertyghfdbye}cvbn");

    /* string with } { and } (additional variable) */
    test_braces("qwerty}ghfd{hello}cvbn", "qwerty}ghfdbyecvbn");


    /*
     * With standard variables
     */

    /* One standard variable */
    newcmd = subst_params("{name}", descr, NULL, &attrs, NULL, NULL, NULL, false, true);
    assert_str_equal(newcmd, attrs.attr_values.name);
    g_free(newcmd);

    newcmd = subst_params("{fid}", "", &id, &attrs, NULL, NULL, NULL, false, true);
    assert_str_equal(newcmd, fid_str);
    g_free(newcmd);

    newcmd = subst_params("{fsname}", "", &id, &attrs, NULL, NULL, NULL, false, true);
    assert_str_equal(newcmd, "somefsname");
    g_free(newcmd);

    newcmd = subst_params("{fsroot}", "", &id, &attrs, NULL, NULL, NULL, false, true);
    assert_str_equal(newcmd, "somefspath");
    g_free(newcmd);

    newcmd = subst_params("{cfg}", "", &id, &attrs, NULL, NULL, NULL, false, true);
    assert_str_equal(newcmd, "someconfigfile");
    g_free(newcmd);

    /* try to resolve fid without passing id argument */
    newcmd = subst_params("{fid}", descr, NULL, &attrs, NULL, NULL, NULL, false, true);
    assert(newcmd == NULL);

    /* try to resolve missing attribute */
    newcmd = subst_params("{ost_pool}", descr, NULL, &attrs, NULL, NULL, NULL, false, true);
    assert(newcmd == NULL);

    /* Partial standard variable name */
    newcmd = subst_params("{nam}", descr, NULL, &attrs, NULL, NULL, NULL, false, true);
    assert(newcmd == NULL);

    /* Standard variable name with an extra letter */
    newcmd = subst_params("{namee}", descr, NULL, &attrs, NULL, NULL, NULL, false, true);
    assert(newcmd == NULL);

    /* Two standard variables */
    newcmd = subst_params("{name} {fullpath}", descr, NULL, &attrs, NULL, NULL, NULL, false, true);
    assert_str_equal(newcmd, "somename somepath");
    g_free(newcmd);

    /*
     * With some real variables (additional params).
     */

    /* Simple replacement */
    newcmd = subst_params("{foo}", descr, NULL, NULL, NULL, vars, NULL, false, true);
    assert_str_equal(newcmd, "barbar");
    g_free(newcmd);

    newcmd = subst_params("{hello}", descr, NULL, NULL, NULL, vars, NULL, false, true);
    assert_str_equal(newcmd, "bye");
    g_free(newcmd);

    newcmd = subst_params("{marco}", descr, NULL, NULL, NULL, vars, NULL, false, true);
    assert_str_equal(newcmd, "polo");
    g_free(newcmd);

    /*
     * With some real variables (user params).
     */
    /* Simple replacement */
    newcmd = subst_params("{foo}", descr, NULL, NULL, &params, NULL, NULL, false, true);
    assert_str_equal(newcmd, "barbar");
    g_free(newcmd);

    newcmd = subst_params("{hello}", descr, NULL, NULL, &params, NULL, NULL, false, true);
    assert_str_equal(newcmd, "bye");
    g_free(newcmd);

    newcmd = subst_params("{marco}", descr, NULL, NULL, &params, NULL, NULL, false, true);
    assert_str_equal(newcmd, "polo");
    g_free(newcmd);

    /* With quotes */
    newcmd = subst_params("{foo}", descr, NULL, NULL, NULL, vars, NULL, true, true);
    assert_str_equal(newcmd, "'barbar'");
    g_free(newcmd);

    newcmd = subst_params("{quote1}", descr, NULL, NULL, NULL, vars, NULL, false, true);
    assert_str_equal(newcmd, "a'b");
    g_free(newcmd);

    newcmd = subst_params("{quote1}", descr, NULL, NULL, NULL, vars, NULL, true, true);
    assert_str_equal(newcmd, "'a'\\''b'");
    g_free(newcmd);

    newcmd = subst_params("az {quote1}", descr, NULL, NULL, NULL, vars, NULL, true, true);
    assert_str_equal(newcmd, "az 'a'\\''b'");
    g_free(newcmd);

    newcmd = subst_params("{quote1} sx", descr, NULL, NULL, NULL, vars, NULL, true, true);
    assert_str_equal(newcmd, "'a'\\''b' sx");
    g_free(newcmd);

    newcmd = subst_params("az {quote1} sx", descr, NULL, NULL, NULL, vars, NULL, true, true);
    assert_str_equal(newcmd, "az 'a'\\''b' sx");
    g_free(newcmd);

    /* Non-existent variable in first column, but present in 2nd column */
    newcmd = subst_params("{barbar}", descr, NULL, NULL, NULL, vars, NULL, false, true);
    assert(newcmd == NULL);

    newcmd = subst_params("{bye}", descr, NULL, NULL, NULL, vars, NULL, false, true);
    assert(newcmd == NULL);

    newcmd = subst_params("{polo}", descr, NULL, NULL, NULL, vars, NULL, false, true);
    assert(newcmd == NULL);

    /* 2 variables */
    newcmd = subst_params("{foo} {hello}", descr, NULL, NULL, NULL, vars, NULL, false, true);
    assert_str_equal(newcmd, "barbar bye");
    g_free(newcmd);

    /* twice the same variable */
    newcmd = subst_params("{hello}{hello}", descr, NULL, NULL, NULL, vars, NULL, false, true);
    assert_str_equal(newcmd, "byebye");
    g_free(newcmd);

    /* 3 variables */
    newcmd = subst_params("{marco}{hello}{foo}", descr, NULL, NULL, NULL, vars, NULL, false, true);
    assert_str_equal(newcmd, "polobyebarbar");
    g_free(newcmd);

    /* 2 + 3 variables */
    newcmd = subst_params("A{marco} {hello} {marco}d{hello}w{hello}", descr, NULL, NULL, NULL, vars, NULL, false, true);
    assert_str_equal(newcmd, "Apolo bye polodbyewbye");
    g_free(newcmd);

    /* 5 times the same variable */
    newcmd = subst_params("{marco}{marco}{marco}{marco}{marco}", descr, NULL, NULL, NULL, vars, NULL, false, true);
    assert_str_equal(newcmd, "polopolopolopolopolo");
    g_free(newcmd);

    /*
     * Mix between standard and additional variables
     */

    /* One of each */
    newcmd = subst_params("{marco} {fullpath}", descr, NULL, &attrs, NULL, vars, NULL, false, true);
    assert_str_equal(newcmd, "polo somepath");
    g_free(newcmd);

    /* Several of each */
    newcmd = subst_params("{marco} {fullpath} {hello} {name} ", descr, NULL, &attrs, NULL, vars, NULL, false, true);
    assert_str_equal(newcmd, "polo somepath bye somename ");
    g_free(newcmd);

    /*
     * Mix between standard and user variables
     */

    /* One of each */
    newcmd = subst_params("{marco} {fullpath}", descr, NULL, &attrs, &params, NULL, NULL, false, true);
    assert_str_equal(newcmd, "polo somepath");
    g_free(newcmd);

    /* Several of each */
    newcmd = subst_params("{marco} {fullpath} {hello} {name} ", descr, NULL, &attrs, &params, NULL, NULL, false, true);
    assert_str_equal(newcmd, "polo somepath bye somename ");
    g_free(newcmd);

    /* test user params priority */
    struct rbh_params p = {0};

    rc = rbh_param_set(&p, "fid", "override", true);
    assert(rc == 0);
    rc = rbh_param_set(&p, "hello", "goodbye", true);
    assert(rc == 0);

    /* user params priority on std params */
    newcmd = subst_params("{fid}", descr, &id, &attrs, &p, NULL, NULL, false, true);
    assert_str_equal(newcmd, "override");
    g_free(newcmd);

    /* user params priority on additional params */
    newcmd = subst_params("{hello}", descr, &id, &attrs, &p, vars, NULL, false, true);
    assert_str_equal(newcmd, "goodbye");
    g_free(newcmd);

    rbh_params_free(&p);

#if 0
    /*
     * Multiple substitutions
     */

    /* "{triple}" -> "{double}" --> "{marco}" --> "polo" */
    /* TODO: is it an abuse -- should that work? Is it something we want to work? */
    newcmd = subst_params("{triple} ", descr, NULL, &attrs, NULL, vars, NULL, false);
    assert_str_equal(newcmd, "polo");
    g_free(newcmd);
#endif

#if 0
    /* "{{explorer}}" -> "{marco}" --> "polo" */
    /* TODO: is it an abuse -- should that work? Is it something we want to work? */
    newcmd = subst_params("{{explorer}} ", descr, NULL, &attrs, NULL, vars, NULL, false);
    assert_str_equal(newcmd, "polo");
    g_free(newcmd);
#endif

    rbh_params_free(&params);
}

static void test_param_mask(void)
{
    attr_mask_t newmask;
    bool        err = false;

    /* no variable */
    newmask = params_mask("", descr, &err);
    assert(!err && attr_mask_is_null(newmask));

    /* Empty variable */
    newmask = params_mask("{}", descr, &err);
    assert(err);

    /* Known std parameter */
    newmask = params_mask("{name}", descr, &err);
    assert(!err && newmask.std == ATTR_MASK_name);

    /* Known std parameter but not an attribute */
    newmask = params_mask("{fid}", descr, &err);
    assert(!err && attr_mask_is_null(newmask));

    /* Unknown parameter */
    newmask = params_mask("{marco}", descr, &err);
    assert(!err && attr_mask_is_null(newmask));

    /*
     * With extra parameters
     */

    /* no variable */
    newmask = params_mask("", descr, &err);
    assert(!err && attr_mask_is_null(newmask));

    /* Empty variable */
    newmask = params_mask("{}", descr, &err);
    assert(err);

    /* Known std parameter */
    newmask = params_mask("{name}", descr, &err);
    assert(!err && newmask.std == ATTR_MASK_name);

    /* Known std parameter but not an attribute */
    newmask = params_mask("{fid}", descr, &err);
    assert(!err && attr_mask_is_null(newmask));

    /* extra parameter */
    newmask = params_mask("{marco}", descr, &err);
    assert(!err && attr_mask_is_null(newmask));

    /* twice the same variable */
    newmask = params_mask("{fullpath}{fullpath}", descr, &err);
    assert(!err && newmask.std == ATTR_MASK_fullpath);

    /* 2 variables */
    newmask = params_mask("wertyu{fullpath}fghj {name}", descr, &err);
    assert(!err && (newmask.std == (ATTR_MASK_fullpath | ATTR_MASK_name)));

    /* 1 known variable and 1 unknown */
    newmask = params_mask("wertyu{fullpath}fghj {namee}", descr, &err);
    assert(!err && newmask.std == ATTR_MASK_fullpath);

    /* 5 mixed variables */
    newmask = params_mask("{hello}wertyu{fullpath}{marco}fghj {name}{fid}",
                          descr, &err);
    assert(!err && (newmask.std == (ATTR_MASK_fullpath | ATTR_MASK_name)));
}

int main(int argc, char **argv)
{
    test_subst_params();
    test_param_mask();

    printf("good\n");

    return 0;
}
