#!/bin/bash
autoreconf --install

wdir=$(dirname $(readlink -m "$0"))

function install_hook
{
    local src_file="$1"
    local tgt_file="$2"

    if [ ! -e "$wdir/.git/hooks/$tgt_file" ]; then
        echo "installing git hook: $tgt_file"
        ln -s "../../scripts/$src_file" "$wdir/.git/hooks/$tgt_file"
    fi
    if [ ! -x "$wdir/.git/hooks/$tgt_file" ]; then
        chmod +x "$wdir/.git/hooks/$tgt_file"
    fi
}

install_hook git_prepare_hook prepare-commit-msg
install_hook pre-commit pre-commit
install_hook commit-msg commit-msg
