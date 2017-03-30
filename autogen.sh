#!/bin/bash
autoreconf --install

set -e

wdir=$(dirname $(readlink -m "$0"))
# if git prepare commit hook is a file, rename it to .orig
# and replace it with a link to the project hook
if [ ! -e "$wdir/.git/hooks/prepare-commit-msg" ] ||
   [ ! -x "$wdir/.git/hooks/prepare-commit-msg" ]; then
    echo "installing git prepare-commit hook"
    ln -s '../../scripts/git_prepare_hook' "$wdir/.git/hooks/prepare-commit-msg"
fi

if [ ! -e "$wdir/.git/hooks/pre-commit" ] ||
   [ ! -x "$wdir/.git/hooks/pre-commit" ]; then
    echo "installing git pre-commit hook"
    ln -s '../../scripts/pre-commit' "$wdir/.git/hooks/pre-commit"
fi
