# used to be rbh 2.5 "purge" policy in TMPFS mode
define_policy cleaning {
    scope { type != directory }
    status_manager = none;
    default_action = common.unlink;
    default_lru_sort_attr = last_access;
}

# used to be rbh 2.5 "rmdir" policies in TMPFS mode
define_policy rmdir_empty {
    scope {type == directory and dircount == 0}
    status_manager = none;
    default_action = common.rmdir;
    default_lru_sort_attr = last_mod;
}

define_policy rmdir_recurse {
    scope {type == directory}
    status_manager = none;
    default_action = cmd("rm -rf {fullpath}");
    default_lru_sort_attr = last_mod;
}
