#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/types.h>
#include <attr/xattr.h>
#include "lustre_extended_types.h"

static const char * pattern2str(uint32_t pattern)
{
    switch(pattern)
    {
        case LOV_PATTERN_RAID0: return "RAID0";
        case LOV_PATTERN_RAID1: return "RAID1";
    }
    return "?";
}

static int print_lov(struct lov_user_md * p_lum)
{
#ifdef LOV_USER_MAGIC_V3
    struct lov_user_md_v3 *p_lum3;
#endif
    unsigned int   i;

    /* Check protocol version number */
    if ( p_lum->lmm_magic == LOV_USER_MAGIC_V1 )
    {
        printf("magic=%#x (LOV_USER_MAGIC_V1)\n", p_lum->lmm_magic);
        printf("pattern=%#x (%s)\n", p_lum->lmm_pattern, pattern2str(p_lum->lmm_pattern));
#ifdef HAVE_OBJ_ID
        printf("object_id=%#Lx\n", p_lum->lmm_object_id);
#ifdef _HAVE_FID
        printf("object_seq=%#Lx\n", p_lum->lmm_object_seq);
#else /* lmm_object_gr for Lustre 1.x */
        printf("object_gr=%#Lx\n", p_lum->lmm_object_gr);
#endif
#else
        printf("object_id=%#Lx\n", p_lum->lmm_oi.oi.oi_id);
        printf("object_seq=%#Lx\n", p_lum->lmm_oi.oi.oi_seq);
#endif
        printf("stripe_size=%u\n", p_lum->lmm_stripe_size);
        printf("stripe_count=%hu\n", p_lum->lmm_stripe_count);
        printf("stripe_offset=%hd\n", p_lum->lmm_stripe_offset);
        printf("stripe objects:\n");
        for (i = 0; i < p_lum->lmm_stripe_count; i++)
        {
            printf("   [%u] ost_idx=%u\n", i, p_lum->lmm_objects[i].l_ost_idx);
            printf("   [%u] ost_gen=%u\n", i, p_lum->lmm_objects[i].l_ost_gen);
#ifdef HAVE_OBJ_ID
            printf("   [%u] object_id=%Lu\n", i, p_lum->lmm_objects[i].l_object_id);
#ifdef HAVE_OBJ_SEQ
            printf("   [%u] object_seq=%Lu\n", i, p_lum->lmm_objects[i].l_object_seq);
#else
            printf("   [%u] object_gr=%Lu\n", i, p_lum->lmm_objects[i].l_object_gr);
#endif
#else /* new structure (union of fid and id/seq) */
            printf("   [%u] object_id=%Lu\n", i, p_lum->lmm_objects[i].l_ost_oi.oi.oi_id);
            printf("   [%u] object_seq=%Lu\n", i, p_lum->lmm_objects[i].l_ost_oi.oi.oi_seq);
#endif
        }
        return 0;
    }
#ifdef LOV_USER_MAGIC_V3
    else if ( p_lum->lmm_magic == LOV_USER_MAGIC_V3 )
    {
        p_lum3 = ( struct lov_user_md_v3 * ) p_lum;
        char pool_name[LOV_MAXPOOLNAME+1];

        printf("magic=%#x (LOV_USER_MAGIC_V3)\n", p_lum3->lmm_magic);
        printf("pattern=%#x (%s)\n", p_lum3->lmm_pattern, pattern2str(p_lum3->lmm_pattern));
#ifdef HAVE_OBJ_ID
        printf("object_id=%#Lx\n", p_lum3->lmm_object_id);
#ifdef _HAVE_FID
        printf("object_seq=%#Lx\n", p_lum3->lmm_object_seq);
#else /* lmm_object_gr for Lustre 1.x */
        printf("object_gr=%#Lx\n", p_lum3->lmm_object_gr);
#endif
#else
        printf("object_id=%#Lx\n", p_lum3->lmm_oi.oi.oi_id);
        printf("object_seq=%#Lx\n", p_lum3->lmm_oi.oi.oi_seq);
#endif

        printf("stripe_size=%u\n", p_lum3->lmm_stripe_size);
        printf("stripe_count=%hu\n", p_lum3->lmm_stripe_count);
        printf("stripe_offset=%hd\n", p_lum3->lmm_stripe_offset);
        strncpy(pool_name, p_lum3->lmm_pool_name, LOV_MAXPOOLNAME);
        pool_name[LOV_MAXPOOLNAME] = '\0';
        printf("pool_name=%s\n", pool_name);
        printf("stripe objects:\n");
        for (i = 0; i < p_lum3->lmm_stripe_count; i++)
        {
            printf("   [%u] ost_idx=%u\n", i, p_lum3->lmm_objects[i].l_ost_idx);
            printf("   [%u] ost_gen=%u\n", i, p_lum3->lmm_objects[i].l_ost_gen);
#ifdef HAVE_OBJ_ID
            printf("   [%u] object_id=%Lu\n", i, p_lum3->lmm_objects[i].l_object_id);
#ifdef HAVE_OBJ_SEQ
            printf("   [%u] object_seq=%Lu\n", i, p_lum3->lmm_objects[i].l_object_seq);
#else
            printf("   [%u] object_gr=%Lu\n", i, p_lum3->lmm_objects[i].l_object_gr);
#endif
#else /* new structure (union of fid and id/seq) */
            printf("   [%u] object_id=%Lu\n", i, p_lum3->lmm_objects[i].l_ost_oi.oi.oi_id);
            printf("   [%u] object_seq=%Lu\n", i, p_lum3->lmm_objects[i].l_ost_oi.oi.oi_seq);
#endif
        }
        return 0;
    }
#endif
    else
    {
        fprintf(stderr, "Error: wrong magic %#x for lov_user_md\n",  p_lum->lmm_magic);
        return -1;
    }
}


static void usage(const char * bin)
{
    fprintf(stderr, "usage: %s <mdt_file>\n", bin);
}

int main(int argc, char ** argv)
{
    ssize_t len;
    char buff[4096];

    if (argc != 2)
    {
        usage(argv[0]);
        exit(1);
    }

    len = lgetxattr (argv[1], XATTR_NAME_LOV, buff, 4096);

    if (len < 1)
    {
        fprintf(stderr, "%s: "XATTR_NAME_LOV" is not readable\n", argv[1]);
        exit(1);
    }
    else if (len == 0)
    {
        fprintf(stderr, "%s: "XATTR_NAME_LOV" is empty\n", argv[1]);
        exit(1);
    }
    else if (len < sizeof(struct lov_user_md_v1)) /* v1 is the smaller (?) */
    {
        fprintf(stderr, "%s: "XATTR_NAME_LOV" size too small\n", argv[1]);
        exit(1);
    }

    /* attr is OK */
    print_lov((struct lov_user_md *) buff);

    exit(0);


}

