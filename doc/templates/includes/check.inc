define_policy checksum {
    status_manager = checker;
    scope { type == file }
    default_lru_sort_attr = last_check; # oldest check first, 0==unchecked
    # 'output' stands for previous value
    default_action = cmd("/usr/sbin/rbh_cksum.sh '{output}' '{path}'");
}
