define_policy shook_archive {
    scope { (type == file or type == symlink)
            and (status == new or status == modified) }
    status_manager = shook(archive);
    status_current = archiving;
    default_action = common.copy;
    default_lru_sort_attr = last_mod;
}

define_policy shook_release {
    scope { type == file and status == synchro }
    status_manager = shook(release);
    status_current = release_pending;
    default_action = shook.release;
    default_lru_sort_attr = last_access;
}

define_policy shook_remove {
    scope { type == file }
    status_manager = shook(removed);
    # this action is executed with a fake 'path' attribute which
    # is actually the path in backend
    default_action = common.unlink;
    default_lru_sort_attr = rm_time;
}

# vim:expandtab:shiftwidth=4:tabstop=4:
