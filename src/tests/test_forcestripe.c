/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/types.h>
#include <asm/types.h>
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <linux/limits.h>
#include <attr/xattr.h>

#include "lustre_extended_types.h"

static inline int lum_size(struct lov_user_md *p_lum)
{
    switch(p_lum->lmm_magic) {
        case LOV_USER_MAGIC_V1:
            return (sizeof(struct lov_user_md_v1) + p_lum->lmm_stripe_count * sizeof(struct lov_user_ost_data_v1));
        case LOV_USER_MAGIC_V3:
            return (sizeof(struct lov_user_md_v3) + p_lum->lmm_stripe_count * sizeof(struct lov_user_ost_data_v1));
    }
    return 0;
}


int main(int argc, char **argv)
{
    int            rc, fd;
	const char *file1;
	char file_new[PATH_MAX];
    char           lum_buffer[4096];
    struct lov_user_md *p_lum = ( struct lov_user_md * ) lum_buffer;

	if (argc < 2)
	{
		fprintf(stderr, "usage: %s <file>\n", argv[0]);
		exit(1);
	}
    file1 = argv[1];

	/* get stripe from file 1 */

    memset( lum_buffer, 0, sizeof( lum_buffer ) );
    rc = llapi_file_get_stripe(file1, p_lum);
    if (rc)
    {
        fprintf(stderr, "llapi_file_get_stripe error: %s\n", strerror(-rc));
        exit(-rc);
    }

	/* set stripe to new file 2 (created with no stripe) */
    sprintf(file_new, "%s.create_nostripe", file1);
    fd = open(file_new, O_CREAT | O_LOV_DELAY_CREATE, 0644);
    if (fd < 0) {
        fprintf(stderr, "open error: %s", strerror(errno));
        exit(errno);
    }

    rc = fsetxattr(fd, "lustre.lov", (void*)p_lum, lum_size(p_lum), 0 /* create or replace */);
    if (rc) {
        fprintf(stderr, "fsetxattr error: %s", strerror(errno));
        exit(errno);
    }

	/* set stripe to file 3 (striped) */
}
