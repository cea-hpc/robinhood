# new implementation of alerts as policies in robinhood v3

define_policy alert {
    status_manager = alerter;
    scope { last_check == 0 or last_check > 1d }
    default_lru_sort_attr = last_check; # oldest check first, 0==unchecked
    default_action = alerter.alert;
}

alert_parameters {
    action_params { alert = raise; }
    report_actions = no;
}

alert_trigger {
    trigger_on = scheduled;
    check_interval = 6h;
}
