# Robinhood Policy definition for modeguard

define_policy modeguard {
    status_manager = modeguard;
    scope { type == directory }
    default_action = modeguard.enforce_mode;
    default_lru_sort_attr = last_mod;
}
