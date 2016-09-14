SUBDIRS=test_suite

pkgdatadir=@datarootdir@/@PACKAGE_NAME@/tests

if COMMON_RPMS
nobase_dist_pkgdata_DATA =              \
    test_suite/README.rst               \
    test_suite/cfg/*.conf               \
    test_suite/cfg/*.sql                \
    test_suite/valgrind.supp            \
    huge_posix/cfg/*.conf

nobase_dist_pkgdata_SCRIPTS = \
    test_suite/1-test_setup_lustre.sh   \
    test_suite/1-test_setup_posix.sh    \
    test_suite/2-run-tests.sh           \
    test_suite/3-tests-lustre.sh        \
    test_suite/cleanup.sh               \
    test_suite/bench_rpc.sh             \
    test_suite/rm_script                \
    test_suite/hudson_lsetup.sh         \
    huge_posix/1-test_setup.sh          \
    huge_posix/2-run-tests.sh           \
    test_rpmbuild.sh			\
    fill_fs.sh                          \
    completion.sh
endif
