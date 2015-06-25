# used to be rbh 2.5 "migration" policy for backup mode
define_policy backup_archive {
    scope { type == file and (status == new or status == modified) }
    status_manager = backup(archive);
    default_action = common.copy;
    default_lru_sort_attr = last_mod;
}

# used to be rbh 2.5 "hsm_rm" policy for backup mode
define_policy backup_remove {
    scope { type == file }
    status_manager = backup(removed);
    default_action = common.unlink; # in backend???
    default_lru_sort_attr = rm_time;
}
