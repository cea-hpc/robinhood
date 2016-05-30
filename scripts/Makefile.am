
EXTRA_DIST= robinhood.init			\
	robinhood.init.in			\
	robinhood.init.sles         		\
	robinhood.init.sles.in      		\
	robinhood.service			\
	robinhood@.service			\
	sysconfig_robinhood			\
	sysconfig_robinhood.in			\
	type_gen.pl				\
	indent.sh 				\
	demo/disk_usage.sh			\
	demo/migr_purge.sh			\
	demo/rh.migr_purge.conf			\
	demo/write_data.sh  			\
    cmd2man.sh make_mans.sh fix_man_options.sh  \
    ld.so.robinhood.conf.in rbh_cksum.sh.in

dist_sbin_SCRIPTS=rbh_cksum.sh

if COMMON_RPMS
dist_sbin_SCRIPTS+=rbh-config
endif

if HSM_LITE
dist_sbin_SCRIPTS+=rbhext_tool rbhext_tool_svr rbhext_tool_clnt
endif

all-local: rbh_cksum.sh
	chmod 755 rbh_cksum.sh
