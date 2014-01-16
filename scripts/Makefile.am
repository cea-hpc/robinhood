
EXTRA_DIST= robinhood.init			\
	robinhood.init.in			\
	robinhood.init.sles         		\
	robinhood.init.sles.in      		\
	sysconfig_robinhood			\
	sysconfig_robinhood.in			\
	type_gen.pl				\
	indent.sh 				\
	demo/disk_usage.sh			\
	demo/migr_purge.sh			\
	demo/rh.migr_purge.conf			\
	demo/write_data.sh  \
    cmd2man.sh make_mans.sh.in make_mans.sh fix_man_options.sh

dist_sbin_SCRIPTS=rbh-config
if HSM_LITE
dist_sbin_SCRIPTS+=rbhext_tool rbhext_tool_svr rbhext_tool_clnt
endif
