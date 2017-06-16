# definition of 2 rmdir policies:
# 1 for empty directories
# 1 for non-empty directories (recursive removal)

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
