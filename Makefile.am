ACLOCAL_AMFLAGS = -I autotools/m4

SUBDIRS=src scripts tests web_gui man

rpm_dir=`pwd`/rpms

if LUSTRE
  rpmbuild_opt=--with lustre @LDEFINES@
if USER_LOVEA
  # Lustre 2.x only
  rpmbuild_opt += --with recovtools
endif

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
  rpmbuild_opt += --with lhsm
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

mydist:
	umask 022; \
	TAR_OPTIONS='--owner=0 --group=0 --numeric-owner --mode=u+rw,go+r-ws' $(MAKE) dist-gzip

rpms: robinhood.spec mydist
	mkdir -p $(rpm_dir)/BUILD $(rpm_dir)/SPECS $(rpm_dir)/SRPMS $(rpm_dir)/RPMS
	rpmbuild $(rpmbuild_opt) --define="_topdir $(rpm_dir)" \
		--define="dbversion $(dbversion)" --define="purpose $(purpose)" --define="_prefix $(prefix)" \
        --define="_sysconfdir @CONFDIR@" \
		-ta $(distdir).tar.gz

rpm: robinhood.spec mydist
	mkdir -p $(rpm_dir)/BUILD $(rpm_dir)/SPECS $(rpm_dir)/SRPMS $(rpm_dir)/RPMS
	rpmbuild $(rpmbuild_opt) --define="_topdir $(rpm_dir)" --define="_prefix $(prefix)" \
		--define="dbversion $(dbversion)" --define="purpose $(purpose)" \
        --define="_sysconfdir @CONFDIR@" \
		--define="_prefix $(prefix)" -tb $(distdir).tar.gz

srpm: robinhood.spec mydist
	mkdir -p $(rpm_dir)/BUILD $(rpm_dir)/SPECS $(rpm_dir)/SRPMS $(rpm_dir)/RPMS
	rpmbuild $(rpmbuild_opt) --define="_topdir $(rpm_dir)" --define="_prefix $(prefix)" \
		--define="dbversion $(dbversion)" --define="purpose $(purpose)" \
        --define="_sysconfdir @CONFDIR@" \
		-ts $(distdir).tar.gz

cppcheck:
	cppcheck -j12 -v --force --enable=all -I`pwd`/src/include -DHAVE_CONFIG_H @PURPOSE_CFLAGS@ @DB_CFLAGS@ src/

lcov:
	lcov --capture --directory . --output-file rbh_cov.info && genhtml rbh_cov.info --output-directory rbh_cov
mans:
	chmod +x ./scripts/make_mans.sh && ./scripts/make_mans.sh

EXTRA_DIST= robinhood.spec robinhood.spec.in   		\
	autogen.sh \
        doc/templates					\
	LICENSE.fr.txt LICENSE.en.txt README.tmp_fs_mgr README.Lustre_HSM	\
	doc/admin_guides \
    doc/install_webgui.txt \
	ChangeLog \
    man/*.1 \
    man/Makefile.am



#	goodies/vim/robinhood.vim cfg_templates/example.conf
