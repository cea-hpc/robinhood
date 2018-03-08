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

/* A file ID (or Lustre FID) hash table. The hash table consists in a
 * fixed number of bucket, keyed on the ID, containing a linked list
 * of operation entries.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "entry_proc_tools.h"
#include "entry_proc_hash.h"
#include "Memory.h"
#include "rbh_logs.h"
#include "rbh_cfg.h"
#include "rbh_misc.h"
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>

/* List of prime numbers of different magnitudes, to size the hash table with a
 * suitable value according to max element count */
static const unsigned int primes[] = { 7919, 15187, 32119, 64157, 128021,
                                       256019, 512009, 999983, 2000003, 4006007,
                                       8004091, 16000463, 32000251, 0};

/** Return a suitable hash table size for the given entry count */
unsigned int max_count_to_hash_size(unsigned int max_count)
{
    const unsigned int *slot;

    for (slot = primes; slot[1] != 0; slot++) {
        if (max_count <= *slot)
            return *slot;
    }
    /* last value */
    return *slot;
}

/** Creates and return a new hash table */
struct id_hash *id_hash_init(const unsigned int hash_size, bool use_lock)
{
    unsigned int i;
    struct id_hash *hash;

    hash =
        MemAlloc(sizeof(struct id_hash) +
                 hash_size * sizeof(struct id_hash_slot));
    if (!hash) {
        DisplayLog(LVL_MAJOR, "Entry_Hash",
                   "Can't allocate new hash table with %d slots", hash_size);
    }

    for (i = 0; i < hash_size; i++) {
        struct id_hash_slot *slot = &hash->slot[i];

        if (use_lock)
            pthread_mutex_init(&slot->lock, NULL);
        rh_list_init(&slot->list);
        slot->count = 0;
    }

    hash->hash_size = hash_size;

    return hash;
}

void id_hash_stats(struct id_hash *id_hash, const char *log_str)
{
    unsigned int i, total, min, max;
    double avg;

    total = 0;
    min = max = id_hash->slot[0].count;

    for (i = 0; i < id_hash->hash_size; i++) {
        const struct id_hash_slot *slot = &id_hash->slot[i];

        total += slot->count;

        if (slot->count < min)
            min = slot->count;
        if (slot->count > max)
            max = slot->count;
    }

    avg = (double)total / (0.0 + id_hash->hash_size);
    DisplayLog(LVL_MAJOR, "STATS",
               "%s: %u (hash min=%u/max=%u/avg=%.1f)", log_str,
               total, min, max, avg);

#ifdef _DEBUG_HASH
    /* more than 50% of difference between hash lists ! Dump all values. */
    if ((max - min) > ((max + 1) / 2)) {
        unsigned int nb_min = 0;
        unsigned int nb_max = 0;

        for (i = 0; i < id_hash->hash_size; i++) {
            const struct id_hash_slot *slot = &id_hash->slot[i];

            if (slot->count == min)
                nb_min++;
            else if (slot->count == max)
                nb_max++;
        }
        DisplayLog(LVL_MAJOR, "DebugHash",
                   "nb slots with min/max count: %u/%u (total=%u)", nb_min,
                   nb_max, id_hash->hash_size);
    }
#endif

}

void id_hash_dump(struct id_hash *id_hash, bool parent)
{
    unsigned int i;
    entry_proc_op_t *op;

    /* dump all values */
    printf("==\n");
    for (i = 0; i < id_hash->hash_size; i++) {
        struct id_hash_slot *slot = &id_hash->slot[i];

        P(slot->lock);
        if (!parent) {
            rh_list_for_each_entry(op, &slot->list, id_hash_list)
                printf("[%u] " DFID "\n", i, PFID(&op->entry_id));
        } else {
            rh_list_for_each_entry(op, &slot->list, name_hash_list)
                printf("[%u] " DFID "/%s:" DFID "\n", i,
                       PFID(&ATTR(&op->fs_attrs, parent_id)),
                       ATTR(&op->fs_attrs, name), PFID(&op->entry_id));
        }
        V(slot->lock);
    }
}
