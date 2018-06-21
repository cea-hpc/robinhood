AM_CFLAGS= $(CC_OPT) $(DB_CFLAGS) $(PURPOSE_CFLAGS)

include_HEADERS=mod_internal.h

pkglib_LTLIBRARIES=

pkglib_LTLIBRARIES+=librbhmodules.la
librbhmodules_la_SOURCES=mod_internal.c mod_internal.h
librbhmodules_la_LDFLAGS=-version-info 0:0:0

pkglib_LTLIBRARIES+=librbh_mod_common.la
librbh_mod_common_la_SOURCES=common_actions.c common_sched.c sched_ratelimit.c \
			     mod_internal.c
librbh_mod_common_la_LDFLAGS=-version-info 0:0:0
librbh_mod_common_la_LIBADD=-lz

pkglib_LTLIBRARIES+=librbh_mod_alerter.la
librbh_mod_alerter_la_SOURCES=alerter.c mod_internal.c mod_internal.h
librbh_mod_alerter_la_LDFLAGS=-version-info 0:0:0

pkglib_LTLIBRARIES+=librbh_mod_checker.la
librbh_mod_checker_la_SOURCES=checker.c mod_internal.c mod_internal.h
librbh_mod_checker_la_LDFLAGS=-version-info 0:0:0

pkglib_LTLIBRARIES+=librbh_mod_basic.la
librbh_mod_basic_la_SOURCES=basic.c mod_internal.c mod_internal.h
librbh_mod_basic_la_LDFLAGS=-version-info 0:0:0

pkglib_LTLIBRARIES+=librbh_mod_modeguard.la
librbh_mod_modeguard_la_SOURCES=modeguard.c mod_internal.c mod_internal.h
librbh_mod_modeguard_la_LDFLAGS=-version-info 0:0:0

pkglib_LTLIBRARIES+=librbh_mod_test.la
librbh_mod_test_la_SOURCES=test_sched.c mod_internal.c mod_internal.h
librbh_mod_test_la_LDFLAGS=-version-info 0:0:0

if LUSTRE_HSM
pkglib_LTLIBRARIES+=librbh_mod_lhsm.la
librbh_mod_lhsm_la_SOURCES=lhsm.c mod_internal.c mod_internal.h
librbh_mod_lhsm_la_LDFLAGS=-version-info 0:0:0
librbh_mod_lhsm_la_LIBADD=-llustreapi
endif
if HSM_LITE
pkglib_LTLIBRARIES+=librbh_mod_backup.la
librbh_mod_backup_la_SOURCES=backup.c backup.h mod_internal.c mod_internal.h
librbh_mod_backup_la_CFLAGS=$(AM_CFLAGS) -D_HSM_LITE
librbh_mod_backup_la_LDFLAGS=-version-info 0:0:0
endif
if SHOOK
pkglib_LTLIBRARIES+=librbh_mod_shook.la
librbh_mod_shook_la_SOURCES=shook.c backup.c backup.h mod_internal.c mod_internal.h
librbh_mod_shook_la_CFLAGS=$(AM_CFLAGS) -DHAVE_SHOOK
librbh_mod_shook_la_LDFLAGS=-version-info 0:0:0 -lshooksvr
endif
