define_policy migration {
    scope { type == file and (status == new or status == modified) }
    status_manager = $STATUS_MGR($MIGR_ACTION);
    default_action = $DEFAULT_ARCHIVE;
    default_lru_sort_attr = last_mod;
}

define_policy purge {
    scope { type == file and status == synchro }
    status_manager = $STATUS_MGR($PURGE_ACTION);
    default_action = $DEFAULT_PURGE;
    default_lru_sort_attr = last_access;
}

define_policy hsm_remove {
    scope { type == file }
    status_manager = $STATUS_MGR(removed);
    default_action = $DEFAULT_HSMRM;
    default_lru_sort_attr = rm_time;
}

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
