SUBDIRS=test_suite

pkgdatadir=@datarootdir@/@PACKAGE_NAME@/tests

if COMMON_RPMS
nobase_dist_pkgdata_DATA =                      \
    $(srcdir)/test_suite/README.rst             \
    $(srcdir)/test_suite/cfg/*.conf             \
    $(srcdir)/test_suite/cfg/*.sql              \
    $(srcdir)/test_suite/valgrind.supp          \
    $(srcdir)/huge_posix/cfg/*.conf

nobase_dist_pkgdata_SCRIPTS =                   \
    $(srcdir)/test_suite/1-test_setup_lustre.sh \
    $(srcdir)/test_suite/1-test_setup_posix.sh  \
    $(srcdir)/test_suite/2-run-tests.sh         \
    $(srcdir)/test_suite/3-tests-lustre.sh      \
    $(srcdir)/test_suite/cleanup.sh             \
    $(srcdir)/test_suite/bench_rpc.sh           \
    $(srcdir)/test_suite/rm_script              \
    $(srcdir)/test_suite/lsetup.sh              \
    $(srcdir)/huge_posix/1-test_setup.sh        \
    $(srcdir)/huge_posix/2-run-tests.sh         \
    $(srcdir)/test_rpmbuild.sh                  \
    $(srcdir)/fill_fs.sh                        \
    $(srcdir)/completion.sh
endif
