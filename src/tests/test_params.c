#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "global_config.h"
global_config_t global_config;

#include "rbh_params.h"
#include "rbh_logs.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>

/* avoid linking with all robinhood libs */
log_config_t log_config = { .debug_level = LVL_DEBUG };

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

int main(int argc, char **argv)
{
    /* exclude lists */
    const char *excl1[] = { "foo", NULL };
    const char *excl2[] = { "program", NULL };
    const char *excl3[] = { "foo", "program", NULL };

    struct rbh_params p = {0};
    struct rbh_params e = {0};
    GString *dump = g_string_new(NULL);
    int i;

    /* new param, don't override */
    if (rbh_param_set(&p, "foo", "bar", false))
        abort();
    /* new param, override */
    if (rbh_param_set(&p, "toto", "tutu", true))
        abort();
    /* override existing */
    if (rbh_param_set(&p, "toto", "tata", true))
        abort();
    /* new value must be 'tata' */
    if (strcmp(rbh_param_get(&p, "toto"), "tata"))
        abort();
    /* override should fail */
    if (!rbh_param_set(&p, "foo", "truc", false))
        abort();
    /* value must be 'bar' */
    if (strcmp(rbh_param_get(&p, "foo"), "bar"))
        abort();
    /* set other test values */
    if (rbh_param_set(&p, "program", argv[0], false))
        abort();
    if (rbh_param_set(&p, "XYZ", "ABC", false))
        abort();

    /* dump to CSV */
    if (rbh_params_serialize(&p, dump, NULL, RBH_PARAM_CSV | RBH_PARAM_COMPACT))
        abort();
    printf("%s\n", dump->str);
    /* compact: no space expected */
    if (strchr(dump->str, ' '))
        abort();
    g_string_assign(dump, "");

    /* dump to CSV (non compact) */
    if (rbh_params_serialize(&p, dump, NULL, RBH_PARAM_CSV))
        abort();
    printf("%s\n", dump->str);
    /* compact: space expected after comma */
    if (!strstr(dump->str, ", "))
        abort();
    g_string_assign(dump, "");

    /* use exclude lists */
    if (rbh_list2params(&e, excl1))
        abort();
    if (rbh_params_serialize(&p, dump, &e, RBH_PARAM_CSV | RBH_PARAM_COMPACT))
        abort();
    printf("%s\n", dump->str);
    /* foo unexpected */
    if (strstr(dump->str, "foo"))
        abort();
    g_string_assign(dump, "");
    rbh_params_free(&e);

    if (rbh_list2params(&e, excl2))
        abort();
    if (rbh_params_serialize(&p, dump, &e, RBH_PARAM_CSV | RBH_PARAM_COMPACT))
        abort();
    printf("%s\n", dump->str);
    /* program unexpected */
    if (strstr(dump->str, "program"))
        abort();
    g_string_assign(dump, "");
    rbh_params_free(&e);

    if (rbh_list2params(&e, excl3))
        abort();
    if (rbh_params_serialize(&p, dump, &e, RBH_PARAM_CSV | RBH_PARAM_COMPACT))
        abort();
    printf("%s\n", dump->str);
    /* foo and program unexpected */
    if (strstr(dump->str, "foo"))
        abort();
    if (strstr(dump->str, "program"))
        abort();
    g_string_assign(dump, "");
    rbh_params_free(&e);

    /* dump to CSV with a comma in values (should fail) */
    if (rbh_param_set(&p, "key", "val,ue", false))
        abort();
    if (rbh_params_serialize(&p, dump, NULL, RBH_PARAM_CSV | RBH_PARAM_COMPACT)
            != -EINVAL)
        abort();
    rbh_params_free(&p);

    /* stress tests */
    for (i = 0; i < 10000; i++) {
        if (rbh_param_set(&p, "foo", "bar", true))
            abort();
    }
    rbh_params_free(&p);

    for (i = 0; i < 10000; i++) {
        char key[100];
        char val[100];

        sprintf(key, "key%d", i);
        sprintf(val, "bar%d", i);
        if (rbh_param_set(&p, key, val, true))
            abort();
        if ((i - 1) % 1000 == 0)
            printf("set %u keys\n", i - 1);
    }

    /* test all values */
    for (i = 9999; i >= 0; i--) {
        char key[100];
        char val[100];

        sprintf(key, "key%d", i);
        sprintf(val, "bar%d", i);
        if (strcmp(rbh_param_get(&p, key), val))
            abort();
        if ((9999 - i) % 1000 == 0)
            printf("verified %u keys\n", 9999 - i);
    }

    /* dump to CSV */
    if (rbh_params_serialize(&p, dump, NULL, RBH_PARAM_CSV | RBH_PARAM_COMPACT))
        abort();
    /* truncate output at 1024 char */
    printf("%.*s...\n", 1024, dump->str);
    /* compact: no space expected */
    if (strchr(dump->str, ' '))
        abort();
    g_string_assign(dump, "");

    /* dump to CSV (non compact) */
    if (rbh_params_serialize(&p, dump, NULL, RBH_PARAM_CSV))
        abort();
    /* compact: space expected after comma */
    if (!strstr(dump->str, ", "))
        abort();
    g_string_assign(dump, "");
    rbh_params_free(&p);

    g_string_free(dump, TRUE);
    exit(0);
}
