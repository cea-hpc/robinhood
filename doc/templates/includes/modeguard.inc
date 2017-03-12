# By default modeguard will enforce both setgid and sticky
# bits on directories. Directory setgid bit is inherited but a user
# could remove it by mistake. The sticky bit is not inherited.
#
define_policy modeguard {
    status_manager = modeguard;
    scope { type == directory }
    default_action = modeguard.action;
    default_lru_sort_attr = last_mod;
}
