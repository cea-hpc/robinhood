# This rmdir policy applies to all directories (empty or non-empty)
# This allow applying policies to both empty and non empty directories
# in a single policy run.
# - Explicitely add conditions "dircount == 0" in targeted fileclass
# to apply action to empty directories only.
# - Explicitely set action = cmd("rm -fr {fullpath}") in the policy rules
# to recursively remove non-empty directories.

define_policy rmdir {
    scope {type == directory}
    status_manager = none;
    default_action = common.rmdir;
    default_lru_sort_attr = last_mod;
}
