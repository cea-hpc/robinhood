#!/bin/bash
set -e
set -x

topdir=$(readlink -m $(dirname $0)/..)
cd $topdir
rpmdir=$topdir/rpms

function error
{
	echo $* >&2
	exit 1
}

function clean_rpms
{
	local dir=$1
	# clean rpms
	find $dir/RPMS -type f -name "*.rpm" -delete
}

function check_posix_rpm
{
	local dir=$1

	# check that posix RPM was generated (default RPM build)
	local rpm=$(ls -t $dir/RPMS/*/robinhood-posix*)
	[ -z "$rpm" ] && error "robinhood-posix not found"

	rpm -qpR $rpm | grep lustre && error "nothing expected about lustre" || true
}

function check_lustre_rpm
{
	local dir=$1

	# check that Lustre RPM was generated
	local rpm=$(ls -t $dir/RPMS/*/robinhood-lustre*)
	[ -z "$rpm" ] && error "robinhood-lustre not found"

	rpm -qpR $rpm | grep "^lustre" || error "expected requirement about lustre"
	rpm -qpl $rpm | grep mod_lhsm || error "No lhsm module?"
	rpm -qpl $rpm | grep mod_backup || error "No backup module?"
}
	

############# MAIN #############

# clean all RPMS
find $rpmdir -type f -name "*.rpm" -delete

### check SRPM behaviors ###
./configure --enable-dist
make srpm

# get rpm name
srpm=$(ls -t $rpmdir/SRPMS/*.src.rpm | head -n 1)
[ -z "$srpm" ] && error "src rpm not found"

# check that default build is POSIX
rpmbuild --rebuild $srpm --define="_topdir $rpmdir"

check_posix_rpm $rpmdir
clean_rpms $rpmdir

# now tests for Lustre

# is there any package providing lustre-client?
p=$(rpm -q --whatprovides lustre-client | grep -v "no package") || true
if [ -z "$p" ]; then
	p=$(rpm -q lustre | grep -v "is not installed") || true
	if [ -z "$p" ]; then
		error "No lustre package found"
	fi

	# explicit lustre package name
	rpmbuild --rebuild $srpm --define="_topdir $rpmdir" --with lustre --define "lpackage lustre"
else
	# default is lustre-client
	rpmbuild --rebuild $srpm --define="_topdir $rpmdir" --with lustre
fi

check_lustre_rpm $rpmdir
clean_rpms $rpmdir

### check 'make rpm' behaviors ###
./configure --disable-lustre 
make rpm

check_posix_rpm $rpmdir
clean_rpms $rpmdir

./configure --enable-lustre
make rpm

check_lustre_rpm $rpmdir
clean_rpms $rpmdir

