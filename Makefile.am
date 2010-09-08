ACLOCAL_AMFLAGS = -I autotools/m4

#SUBDIRS=src man scripts
SUBDIRS=src scripts tests

rpm_dir=`pwd`/rpms

if LUSTRE
  rpmbuild_opt=--with lustre
  lversion=`rpm -qa "lustre[-_]*modules*" --qf "%{Version}\n" 2>/dev/null | tail -1 | cut -d "." -f 1-2`
  lpackage=`rpm -qa "lustre[-_]*modules*" --qf "%{Name}\n"  2>/dev/null | tail -1`
  rpmbuild_opt+=--define="lversion $(lversion)" --define="lpackage $(lpackage)"
else
  rpmbuild_opt=--without lustre
endif

if USE_MYSQL_DB
  rpmbuild_opt += --with mysql
  dbname=mysql
  dbversion=`mysql_config --version | cut -d "." -f 1-2`
else
  rpmbuild_opt += --with sqlite
  dbname=sqlite
  dbversion=`rpm -qa "sqlite-devel*" --qf "%{Version}\n" | tail -1 | cut -d "." -f 1-2`
endif

if TMP_FS_MGR
  purpose=tmp_fs_mgr
endif
if LUSTRE_HSM
  purpose=lustre_hsm
  rpmbuild_opt += --with hsm
endif
if SHERPA
  purpose=sherpa
  rpmbuild_opt += --with sherpa
endif



new: clean all

rpms: robinhood.spec dist
	mkdir -p $(rpm_dir)/BUILD $(rpm_dir)/SPECS $(rpm_dir)/SRPMS $(rpm_dir)/RPMS \
	&& rpmbuild $(rpmbuild_opt) --define="_topdir $(rpm_dir)" \
		--define="dbversion $(dbversion)" --define="purpose $(purpose)" --define="_prefix $(prefix)" \
		-ta $(distdir).tar.gz \
	&& echo "RPMs successfully generated in $(rpm_dir)"

rpm: robinhood.spec dist
	mkdir -p $(rpm_dir)/BUILD $(rpm_dir)/SPECS $(rpm_dir)/SRPMS $(rpm_dir)/RPMS \
	&& rpmbuild $(rpmbuild_opt) --define="_topdir $(rpm_dir)" \
		--define="dbversion $(dbversion)" --define="purpose $(purpose)" \
		--define="_prefix $(prefix)" -tb $(distdir).tar.gz \
	&& echo "RPM successfully generated in $(rpm_dir)/RPMS"

srpm: robinhood.spec dist
	mkdir -p $(rpm_dir)/BUILD $(rpm_dir)/SPECS $(rpm_dir)/SRPMS $(rpm_dir)/RPMS \
	&& rpmbuild $(rpmbuild_opt) --define="_topdir $(rpm_dir)" --define="_prefix $(prefix)" \
		--define="dbversion $(dbversion)" --define="purpose $(purpose)" \
		-ts $(distdir).tar.gz \
	&& echo "SRPM successfully generated in $(rpm_dir)/SRPMS"

install-exec-local:
	install -m 750 scripts/rbh-config $(DESTDIR)/$(sbindir)/rbh-config



EXTRA_DIST= robinhood.spec robinhood.spec.in   		\
        doc/templates/hsm_policy_basic.conf     	\
        doc/templates/hsm_policy_detailed.conf  	\
        doc/templates/tmp_fs_mgr_basic.conf  		\
        doc/templates/tmp_fs_mgr_detailed.conf  	\
	LICENSE KnownBugs README.tmp_fs_mgr README.Lustre_HSM	\
	doc/admin_guides/robinhood_v220_admin_guide-tmp_fs_mgr.pdf \
	doc/admin_guides/robinhood_v220_tutorial-tmp_fs_mgr.pdf



#	goodies/vim/robinhood.vim cfg_templates/example.conf 
