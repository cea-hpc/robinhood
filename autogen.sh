#!/bin/bash
wdir=$(dirname $(readlink -m "$0"))

mkdir -p "$wdir/autotools/m4/"
autoreconf --install

function install_hook
{
    local src_file="$1"
    local tgt_file="$2"

    if [ ! -e "$wdir/.git/hooks/$tgt_file" ]; then
        echo "installing git hook: $tgt_file"
        rm -f "$wdir/.git/hooks/$tgt_file"
        ln -s "../../scripts/$src_file" "$wdir/.git/hooks/$tgt_file"
    fi
    if [ ! -x "$wdir/.git/hooks/$tgt_file" ]; then
        chmod +x "$wdir/.git/hooks/$tgt_file"
    fi
}

# hook to run basic code check before commit
install_hook git-pre-commit pre-commit
# hook to prepare commit message
install_hook git-prepare-commit prepare-commit-msg
# hook to add ChangeId to commit message
install_hook git-commit-msg commit-msg
