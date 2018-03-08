/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 *
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2018 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
#include <stdio.h>
#include <assert.h>

unsigned int max_count_to_hash_size(unsigned int max_count);
void *log_config;

int main(int argc, char **argv)
{
    int i;

    for (i = 1; i < 1024 * 1024 * 1024; i <<= 1) {
        unsigned int s = max_count_to_hash_size(i);
        fprintf(stderr, "count2size(%d) = %u\n", i, s);
        assert(s >= i || s == 32000251);
    }
    return 0;
}
