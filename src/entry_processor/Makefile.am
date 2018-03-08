AM_CFLAGS= $(CC_OPT) $(DB_CFLAGS) $(PURPOSE_CFLAGS)

noinst_LTLIBRARIES=libentryproc.la

libentryproc_la_SOURCES=entry_proc_impl.c entry_proc_tools.c entry_proc_tools.h \
			std_pipeline.c diff_pipeline.c entry_proc_hash.c

check_PROGRAMS=test_hash
TESTS=test_hash

# automake issue workaround
# see: https://www.gnu.org/software/automake/manual/html_node/Objects-created-both-with-libtool-and-without.html
noinst_LTLIBRARIES+=libtesthash.la
libtesthash_la_SOURCES=entry_proc_hash.c

test_hash_SOURCES=test_hash.c
test_hash_LDADD=libtesthash.la
test_hash_LDFLAGS=-Xlinker "--allow-shlib-undefined" -Xlinker "--unresolved-symbols=ignore-all"

indent:
	$(top_srcdir)/scripts/indent.sh
