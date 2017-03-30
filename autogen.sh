#!/bin/bash
autoreconf --install

wdir=$(dirname $(readlink -m "$0"))
# if git prepare commit hook is a file, rename it to .orig
# and replace it with a link to the project hook
if [ -d "$wdir/.git/hooks" ]; then
    echo "installing git prepare-commit hook"
    mv -f "$wdir/.git/hooks/prepare-commit-msg" "$wdir/.git/hooks/prepare-commit-msg.save" || exit 1
    ln -s '../../scripts/git_prepare_hook' "$wdir/.git/hooks/prepare-commit-msg" || exit 1
fi

if [ -d "$wdir/.git/hooks" ]; then
    echo "installing git pre-commit hook"
    mv -f "$wdir/.git/hooks/pre-commit" "$wdir/.git/hooks/pre-commit.save" || exit 1
    ln -s '../../scripts/pre-commit' "$wdir/.git/hooks/pre-commit" || exit 1
fi
