# -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
# vim:expandtab:shiftwidth=4:tabstop=4:
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)

AC_INIT( [robinhood], [4.0], [robinhood-support@lists.sourceforge.net])
RELEASE="0.dev1"

AC_DEFINE_UNQUOTED(RELEASE, "$RELEASE", [release info])
AC_SUBST(RELEASE)

AC_CONFIG_AUX_DIR([autotools])
AC_CONFIG_MACRO_DIR([autotools/m4])
AC_CONFIG_HEADER([src/include/config.h])
#AC_CONFIG_SRCDIR([src/path/to/file.c])

AM_INIT_AUTOMAKE([-Wall -Werror tar-pax foreign subdir-objects])
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

AC_PREFIX_DEFAULT([/usr])

AC_GNU_SOURCE

AC_CONFIG_FILES([Makefile
                 src/Makefile
                 src/include/Makefile])

# required for automake 1.12 (since fedora 18)
m4_ifdef([AM_PROG_AR], [AM_PROG_AR])

AC_PROG_LIBTOOL
AC_PROG_CC

# required for automake 1.12 (since fedora 18)
m4_ifdef([AM_PROG_CC_C_O], [AM_PROG_CC_C_O])

AC_C_INLINE

# define everything necessary for accessing large files (64bits offset)
AC_SYS_LARGEFILE

# gcc options
CC_OPT="-Wall -Werror -Wstrict-prototypes -Wmissing-prototypes -Wmissing-declarations -std=gnu99 -Wno-variadic-macros"
AC_SUBST(CC_OPT)

AC_OUTPUT

exit 0
