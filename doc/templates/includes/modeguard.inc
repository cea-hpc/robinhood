# Robinhood Policy definition for modeguard

define_policy modeguard {
    status_manager = modeguard;
    scope { type == directory }
    default_action = modeguard.enforce_mode;
    default_lru_sort_attr = last_mod;
}

# By default, modeguard will enforce setgid bit on directories.
# Directory setgid is inherited from parent but a user or some copy
# tool might remove it. But this is just an example, you can easily
# modify the set/clear masks below using the octal notation.
#
modeguard_config {
        set_mask = "2000";
#        clear_mask = "0002";
}
