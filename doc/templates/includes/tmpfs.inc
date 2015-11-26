# used to be rbh 2.5 "purge" policy in TMPFS mode
define_policy cleanup {
    scope { type != directory }
    status_manager = none;
    default_action = common.unlink;
    default_lru_sort_attr = last_access;
}
