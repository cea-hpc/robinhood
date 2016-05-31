#####################################################
#   Policy definitions for Lustre/HSM
#   Do not modify without the agreement
#   of your robinhood support!
#####################################################

# used to be rbh 2.5 "migration" policy for Lustre/HSM
define_policy lhsm_archive {
    scope { type == file
            and no_archive != 1
            and (status == new or status == modified) }
    status_manager = lhsm(archive);
    status_current = archiving;
    default_action = lhsm.archive;
    default_lru_sort_attr = last_mod;
}

# used to be rbh 2.5 "purge" policy for Lustre/HSM
define_policy lhsm_release {
    scope { type == file
            and no_release != 1
            and status == synchro }
    status_manager = lhsm(release);
    default_action = lhsm.release;
    default_lru_sort_attr = last_access;
}

# used to be rbh 2.5 "hsm_rm" policy for Lustre/HSM
define_policy lhsm_remove {
    scope { type == file }
    status_manager = lhsm(removed);
    default_action = lhsm.hsm_remove;
    default_lru_sort_attr = rm_time;
}

# vim:expandtab:shiftwidth=4:tabstop=4:
