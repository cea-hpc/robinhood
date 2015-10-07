# new implementation of alerts as policies in robinhood v3

define_policy alert {
    status_manager = alerter;
    scope = all;
    default_lru_sort_attr = last_check; # oldest check, 0 (unchecked) first
    default_action = alerter.alert;
}

alert_parameters {
    action_params { alert = raise; }
}

alert_trigger {
    trigger_on = scheduled;
    check_interval = 6h;
}

alert_rules {
    rule alert_check {
        ## List all fileclasses that would raise alerts HERE:
        target_fileclass = f1;
        target_fileclass = f2;
        target_fileclass = largedir;

        # customize alert title:
        action_params { title = "entry matches '{fileclass}' ({rule})"; }

        # the first time, check object 1h after their last modification
        # then, check them daily
        condition { (last_check == 0 and last_mod > 1h) or (last_check > 1d) }
    }

    # clear alert status after 1d for files that are not in target fileclasses
    rule default {
        action = none;
        action_params { alert = clear; }
        condition { last_check > 1d }
    }
}
