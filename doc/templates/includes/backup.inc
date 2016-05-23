# used to be rbh 2.5 "migration" policy for backup mode
define_policy backup_archive {
    scope { (type == file or type == symlink) and (status == new or status == modified) }
    status_manager = backup(archive);
    status_current = archiving;
    default_action = common.copy;
    default_lru_sort_attr = last_mod;
}

# used to be rbh 2.5 "hsm_rm" policy for backup mode
define_policy backup_remove {
    scope { type == file or type == symlink }
    status_manager = backup(removed);
    # this action is executed with a fake 'path' attribute which
    # is actually the path in backend
    default_action = common.unlink;
    default_lru_sort_attr = rm_time;
}
