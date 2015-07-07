#!/bin/bash

dir=$(readlink -m $(dirname $0))
root=$(readlink -m "$dir/..")

main=robinhood
prefix=rbh

which txt2man > /dev/null
if (($? != 0)); then
    echo "'txt2man' is not installed on this machine: man pages won't be updated" >&2
    exit 0
fi

if [[ -x $root/src/robinhood/$main ]]; then
    $dir/cmd2man.sh $root/src/robinhood/$main "policy engine and statistics tool for large file systems" "$prefix-report(1), $prefix-find(1), $prefix-du(1), $prefix-diff(1)" | txt2man -v "Robinhood $VERSION" -t $main -s 1 -I dir -I mdt_idx -I attrset -I ost_index -I cfg_file -I output_file -I logfile -I loglevel -I pidfile -I user_name -I grp_name -I filepath -I policy1 -I policy2 -I percent | $dir/fix_man_options.sh > $root/man/$main.1
else
    echo "src/robinhood/$main is not built: can't generate man/$main.1" >&2
fi

if [[ -x $root/src/robinhood/$prefix-report ]]; then
    $dir/cmd2man.sh $root/src/robinhood/$prefix-report "querying command for robinhood policy engine" "$main(1), $prefix-find(1), $prefix-du(1), $prefix-diff(1)" | txt2man -v "Robinhood $VERSION" -t $prefix-report -s 1 -I path -I id -I username -I groupname -I ost_index -I ost_set -I class_expr -I cnt -I range -I cfg_file -I loglevel -I status_name -I status_value | $dir/fix_man_options.sh > $root/man/$prefix-report.1
else
    echo "src/robinhood/$prefix-report is not built: can't generate man/$prefix-report.1" >&2
fi

if [[ -x $root/src/robinhood/$prefix-find ]]; then
    $dir/cmd2man.sh $root/src/robinhood/$prefix-find "find clone that query robinhood DB" "$main(1), $prefix-report(1), $prefix-du(1), $prefix-diff(1)" | txt2man -v "Robinhood $VERSION" -t $prefix-find -s 1 -I user -I group -I type -I filename -I size_crit -I time_crit -I minute_crit -I second_crit -I status -I ost_index -I log_level -I config_file | $dir/fix_man_options.sh > $root/man/$prefix-find.1
else
    echo "src/robinhood/$prefix-find is not built: can't generate man/$prefix-find.1" >&2
fi

if [[ -x $root/src/robinhood/$prefix-du ]]; then
    $dir/cmd2man.sh $root/src/robinhood/$prefix-du "du clone that query robinhood DB" "$main(1), $prefix-report(1), $prefix-find(1), $prefix-diff(1)" | txt2man -v "Robinhood $VERSION" -t $prefix-du -s 1 -I user -I group -I type -I status -I log_level -I config_file | $dir/fix_man_options.sh > $root/man/$prefix-du.1
else
    echo "src/robinhood/$prefix-du is not built: can't generate man/$prefix-du.1" >&2
fi

if [[ -x $root/src/robinhood/$prefix-diff ]]; then
    $dir/cmd2man.sh $root/src/robinhood/$prefix-diff "list differences between robinhood database and the filesystem" "$main(1), $prefix-report(1), $prefix-find(1), $prefix-du(1)" | txt2man -v "Robinhood $VERSION" -t $prefix-diff -s 1 -I loglevel -I configfile -I fs -I db | $dir/fix_man_options.sh > $root/man/$prefix-diff.1
else
    echo "src/robinhood/$prefix-diff is not built: can't generate man/$prefix-diff.1" >&2
fi
