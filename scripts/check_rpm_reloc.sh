#!/bin/bash

# without option:
# expected:
# rpm to install config to /etc/robinhood.d
# rpm to install binaries to /usr/[s]bin
# rpm relocated to /usr
# rbh to search config in /etc/robinhood.d
# init script to get config in /etc/robinhood.d

function error
{
    echo "ERROR: $@"
    exit 1
}

function get_cpu
{
    echo $(( $(cat /proc/cpuinfo  | grep -P "processor\s*:" | cut -d ':' -f 2 | tail -1) + 1 ))
}

function check_build_dir
{
    local dir=$1
    local rbhcfg=$2

    # use 'strings' to read the path from the binary
    for cfg in $(strings $dir/src/common/RobinhoodMisc.o  | grep robinhood.d); do
        echo $cfg | egrep -E "^$rbhcfg"  > /dev/null || error "wrong config location: $cfg not in $rbhcfg"
        echo "$cfg OK"
    done

    confdir=$(egrep -E "^RH_CONF_DIR" $dir/scripts/robinhood.init | awk '{print $1}' | cut -d '=' -f 2)
    [ "$confdir" == "$rbhcfg/tmpfs" ] || error "unexpected config dir in robinhood.init: $confdir"
    echo "$confdir OK"
}


function check_locations
{
    config_options="$1"
    expected_confdir="$2"
    expected_rbhconfdir="$2/robinhood.d"
    expected_binprefix="$3"

    echo "./configure $config_options"
    ./configure $config_options | grep "Using config dir" || error "configure error"

    echo "building RPM..."
    make rpm 2>&1 | grep -E "Using config dir|$expected_rbhconfdir" || error "make rpm error"
    
    # check RPM content:
    rpm=$(ls -tr ./rpms/RPMS/x86_64/robinhood-tmpfs* | tail -1)
    [ -z $rpm ] && error "no matching RPM found"

    echo "checking RPM: $rpm"

    reloc=$(rpm -qpi $rpm | grep Relocation | awk '{print $(NF)}')
    [ "$reloc" == "$expected_binprefix" ] || error "wrong RPM relocation: $reloc"
    echo "Relocation $reloc OK"

    sbin=$(dirname $(rpm -qpl $rpm | grep "rbh-report"))
    [ "$sbin" == "$expected_binprefix/sbin" ] || error "wrong sbin location: $sbin"
    echo "sbindir $sbin OK"

    for cfg in $(rpm -qpl $rpm | grep "robinhood.d/"); do
        # does it start with expected_rbhconfdir?
        echo $cfg | egrep -E "^$expected_rbhconfdir/"  > /dev/null || error "wrong config location: $cfg not in $expected_rbhconfdir"
        echo "$cfg OK"
    done

    last_build=$(ls -tr rpms/BUILD | tail -1)
    [ -z $last_build ] && error "no BUILD found"

    echo "checking executables in rpms/BUILD/$last_build..."
    check_build_dir "rpms/BUILD/$last_build" "$expected_rbhconfdir"

    echo "building source tree..."
    make -j $(get_cpu) >/dev/null 2>&1 || error "build error"

    echo "checking executables in source tree..."
    check_build_dir "." "$expected_rbhconfdir"

    echo "make install..."
    export DESTDIR=/tmp/install.$$
    for f in $(make install | grep '/bin/install' | awk '{print $(NF)}' | tr -d "'"); do
        echo $f | egrep -E "^$DESTDIR$expected_binprefix" || error "$f not in \$DESTDIR$expected_binprefix"
    done
    rm -rf $DESTDIR
    unset DESTDIR

    echo
    return 0
}

check_locations "" "/etc" "/usr"
check_locations "--prefix=/opt/rbh" "/opt/rbh/etc" "/opt/rbh"
check_locations "--prefix=/opt/rbh --sysconfdir=/opt/rbh/cfg" "/opt/rbh/cfg" "/opt/rbh"










#./configure  && make rpm | grep config && rpm -qpl /cea/home/gpocre/leibovi/robinhood.git/rpms/RPMS/x86_64/robinhood-tmpfs-2.4.3-2.lustre2.1.el6.x86_64.rpm && rpm -qpi /cea/home/gpocre/leibovi/robinhood.git/rpms/RPMS/x86_64/robinhood-tmpfs-2.4.3-2.lustre2.1.el6.x86_64.rpm
