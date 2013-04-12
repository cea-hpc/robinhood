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

#include "lustre_extended_types.h"



int main(int argc, char ** argv)
{
    int fd = open(argv[1], O_CREAT | O_LOV_DELAY_CREATE, 0644);
    if (fd < 0)
        fprintf(stderr, "open error: %s", strerror(errno));
    else
    {
        printf ("open OK\n");
        close(fd);
    }
}
