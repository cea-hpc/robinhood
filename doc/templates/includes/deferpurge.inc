define_policy deferpurge_check {
    status_manager = deferpurge(check);
    scope { type != directory }
    default_lru_sort_attr = last_access;
    default_action = cmd("/usr/sbin/rbh_mark_purgeable.sh '{path}'");
}

define_policy deferpurge_purge {
    status_manager = deferpurge(purge);
    scope { type != directory
            and status == purgeable }
    default_lru_sort_attr = last_access;
    default_action = common.unlink
}
