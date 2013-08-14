/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright 2013 Cray Inc. All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
/**
 *
 * \file    list.h
 * \brief   Double link list implementation.
 *
 * This uses a similar API as the linux kernel, with an extra prefix to
 * avoid a name space conflict with MySQL's my_list.h.
 */

#ifndef _RH_LIST_H
#define _RH_LIST_H

struct list_head {
    struct list_head *next;
    struct list_head *prev;
};

/* Initialize a list or an element. */
static inline void rh_list_init (struct list_head *head)
{
    head->next = head;
    head->prev = head;
}

/* Add to the head. */
static inline void rh_list_add (struct list_head *l, struct list_head *head)
{
    head->next->prev = l;
    l->next = head->next;
    l->prev = head;
    head->next = l;
}

/* Add to the tail. */
static inline void rh_list_add_tail (struct list_head *l, struct list_head *head)
{
    head->prev->next = l;
    l->next = head;
    l->prev = head->prev;
    head->prev = l;
}

/* Remove from a list. */
static inline void rh_list_del (struct list_head *l)
{
    l->next->prev = l->prev;
    l->prev->next = l->next;
}

/* Remove from a list and re-initialize. */
static inline void rh_list_del_init (struct list_head *l)
{
    rh_list_del(l);
    l->next = NULL;
    l->prev = NULL;
}

/* Add list2 after list1. list2 must not be empty. */
static inline void rh_list_splice_tail(struct list_head *list1,
                                       const struct list_head *list2)
{
    struct list_head *last = list1->prev;

    list1->prev = list2->prev;
    last->next = list2->next;

    list1->prev->next = list1;
    last->next->prev = last;
}

/* Cut list1 from the first entry up to, and including, the
 * position. Store the result in list2. */
static inline void rh_list_cut_head(struct list_head *list1,
                                    struct list_head *pos,
                                    struct list_head *list2)
{
    list2->next = list1->next;
    list2->prev = pos;

    list1->next = pos->next;
    list1->next->prev = list1;

    list2->next->prev = list2;
    list2->prev->next = list2;
}


/* return non-zero if the list is empty. */
static inline int rh_list_empty(const struct list_head *head)
{
    return head->next == head;
}

/* Return a pointer to the structure containing a list element. */
#define rh_list_entry(ptr, type, member) ((type *)((char *)(ptr)-(unsigned long)(&((type *)0)->member)))

/* Return a pointer to the first entry in the list. */
#define rh_list_first_entry(ptr, type, member) rh_list_entry((ptr)->next, type, member)

/* Return a pointer to the last entry in the list. */
#define rh_list_last_entry(ptr, type, member) rh_list_entry((ptr)->prev, type, member)

/* Iterate over a list. l is the cursor. */
#define rh_list_for_each_entry(l, head, member)              \
    for (l = rh_list_entry((head)->next, typeof(*l), member); \
         &l->member != (head);                               \
         l = rh_list_entry(l->member.next, typeof(*l), member))

/* Iterate over a list. l is the cursor. */
#define rh_list_for_each_entry_reverse(l, head, member)         \
    for (l = rh_list_entry((head)->prev, typeof(*l), member);   \
         &l->member != (head);                                  \
         l = rh_list_entry(l->member.prev, typeof(*l), member))

/* Iterate over a list in reverse. l is the cursor, tmp stores the
 * next entry. l can be removed during the iteration. */
#define rh_list_for_each_entry_safe_reverse(l, tmp, head, member)       \
    for (l = rh_list_entry((head)->prev, typeof(*l), member),           \
             tmp = rh_list_entry(l->member.prev, typeof(*l), member);   \
         &l->member != (head);                                          \
         l = tmp,                                                       \
             tmp = rh_list_entry(l->member.prev, typeof(*l), member))

/* Iterate in a list starting after any element in it. */
#define rh_list_for_each_entry_after(l, head, start, member)   \
    for(l =  rh_list_entry(start->member.next, typeof(*l), member); \
        &l->member != (head);                                  \
        l = rh_list_entry(l->member.next, typeof(*l), member))

#endif  /* _RH_LIST_H */
