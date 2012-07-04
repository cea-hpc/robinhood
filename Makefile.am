ACLOCAL_AMFLAGS = -I autotools/m4

#SUBDIRS=src man scripts
SUBDIRS=src scripts tests web_gui

rpm_dir=`pwd`/rpms

if LUSTRE
  rpmbuild_opt=--with lustre @LDEFINES@
#  lversion=`rpm -qa "lustre[-_]*modules*" --qf "%{Version}\n" 2>/dev/null | tail -1 | cut -d "." -f 1-2`
#  lpackage=`rpm -qa "lustre[-_]*modules*" --qf "%{Name}\n"  2>/dev/null | tail -1`
#ifeq ($(lversion), )
#  rpmbuild_opt+=--define="lversion $(lversion)" --define="lpackage $(lpackage)"
#endif
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
if SHOOK
  purpose=shook
  rpmbuild_opt += --with shook
endif
if BACKUP
  purpose=backup
  rpmbuild_opt += --with backup
endif



new: clean all

rpms: robinhood.spec dist
	mkdir -p $(rpm_dir)/BUILD $(rpm_dir)/SPECS $(rpm_dir)/SRPMS $(rpm_dir)/RPMS
	rpmbuild $(rpmbuild_opt) --define="_topdir $(rpm_dir)" \
		--define="dbversion $(dbversion)" --define="purpose $(purpose)" --define="_prefix $(prefix)" \
		-ta $(distdir).tar.gz

rpm: robinhood.spec dist
	mkdir -p $(rpm_dir)/BUILD $(rpm_dir)/SPECS $(rpm_dir)/SRPMS $(rpm_dir)/RPMS
	rpmbuild $(rpmbuild_opt) --define="_topdir $(rpm_dir)" --define="_prefix $(prefix)" \
		--define="dbversion $(dbversion)" --define="purpose $(purpose)" \
		--define="_prefix $(prefix)" -tb $(distdir).tar.gz

srpm: robinhood.spec dist
	mkdir -p $(rpm_dir)/BUILD $(rpm_dir)/SPECS $(rpm_dir)/SRPMS $(rpm_dir)/RPMS
	rpmbuild $(rpmbuild_opt) --define="_topdir $(rpm_dir)" --define="_prefix $(prefix)" \
		--define="dbversion $(dbversion)" --define="purpose $(purpose)" \
		-ts $(distdir).tar.gz

cppcheck:
	cppcheck -j12 -v --force --enable=all -I`pwd`/src/include -DHAVE_CONFIG_H @PURPOSE_CFLAGS@ @DB_CFLAGS@ src/

EXTRA_DIST= robinhood.spec robinhood.spec.in   		\
	robinhood.old_spec.in				\
        doc/templates					\
	LICENSE KnownBugs README.tmp_fs_mgr README.Lustre_HSM	\
	doc/admin_guides \
	ChangeLog



#	goodies/vim/robinhood.vim cfg_templates/example.conf 
