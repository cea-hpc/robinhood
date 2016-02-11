define_policy md5_check {
    status_manager = checker;
    scope { type == file }
    default_lru_sort_attr = last_check; # oldest check first, 0==unchecked
    # 'output' stands for previous value
    default_action = cmd("/usr/sbin/rbh_md5_wrap.sh '{output}' '{path}'");
}
