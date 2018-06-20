ACLOCAL_AMFLAGS = -I autotools/m4

check-valgrind:
	make -C src/tests check-valgrind

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

if !COMMON_RPMS
  rpmbuild_opt+=--without common_rpms
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

if LUSTRE_HSM
  rpmbuild_opt += --with lhsm
else
  rpmbuild_opt += --without lhsm
endif
if SHOOK
  rpmbuild_opt += --with shook
else
  rpmbuild_opt += --without shook
endif
if HSM_LITE
  rpmbuild_opt += --with backup
else
  rpmbuild_opt += --without backup
endif
if USE_JEMALLOC
  rpmbuild_opt += --with jemalloc
else
  rpmbuild_opt += --without jemalloc
endif


new: clean all

mydist:
	umask 022; \
	TAR_OPTIONS='--owner=0 --group=0 --numeric-owner --mode=u+rw,go+r-ws' $(MAKE) dist-gzip

rpms: robinhood.spec mydist
	mkdir -p $(rpm_dir)/BUILD $(rpm_dir)/SPECS $(rpm_dir)/SRPMS $(rpm_dir)/RPMS
	rpmbuild $(rpmbuild_opt) --define="_topdir $(rpm_dir)" \
		--define="dbversion $(dbversion)" --define="_prefix $(prefix)" \
		--define="configure_flags @ac_configure_args@" \
        --define="_sysconfdir @CONFDIR@" \
		-ta $(distdir).tar.gz

rpm: robinhood.spec mydist
	mkdir -p $(rpm_dir)/BUILD $(rpm_dir)/SPECS $(rpm_dir)/SRPMS $(rpm_dir)/RPMS
	rpmbuild $(rpmbuild_opt) --define="_topdir $(rpm_dir)" --define="_prefix $(prefix)" \
		--define="dbversion $(dbversion)" \
		--define="configure_flags @ac_configure_args@" \
        --define="_sysconfdir @CONFDIR@" \
		--define="_prefix $(prefix)" -tb $(distdir).tar.gz

srpm: robinhood.spec mydist
	mkdir -p $(rpm_dir)/SRPMS $(rpm_dir)/SOURCES
	cp -f $(distdir).tar.gz $(rpm_dir)/SOURCES/.
	rpmbuild --without lustre --define="_topdir $(rpm_dir)" -bs robinhood.spec

cppcheck:
	cppcheck -j12 -v --force --enable=all -I`pwd`/src/include -DHAVE_CONFIG_H @PURPOSE_CFLAGS@ @DB_CFLAGS@ src/

lcov:
	lcov --capture --directory . --output-file rbh_cov.info && genhtml rbh_cov.info --output-directory rbh_cov
mans:
	chmod +x ./scripts/make_mans.sh && ./scripts/make_mans.sh

EXTRA_DIST= robinhood.spec robinhood.spec.in \
	autogen.sh scripts/robinhood.target \
	LICENSE.fr.txt LICENSE.en.txt README.md \
	doc/admin_guides \
	doc/install_webgui.txt \
	doc/templates/includes/*.inc \
	doc/templates/*.conf \
	ChangeLog \
	man/*.1



#	goodies/vim/robinhood.vim
