<?php
/*
 * Copyright (C) 2016-2017 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */


/*
 * ldapauth V0.1
 * Use ldap informations for access control
 * requirements: php-ldap
 * WARNING: This version has a simplied access control for files
 * which allows users to see all files with r/x on others.
 */

class ldapauth extends Plugin {
    public $Name = "LDAPAuth";
    public $Description = "Use ldap informations for access control";
    public $Version = "V0.1";

    public $Req_lib = array('ldap');


    /* The ui page which allows the user to check his access */
    public $ui_page = true;

    /* Allow to access to ldap informations with the API */
    public $api_export = true;

    /* You need to configure the host and dn before using the plugin */
    private $ldap_host = "ldapserver";
    private $ldap_dn = "dc=foo,dc=bar,dc=foo,dc=com";

    private $ldap = null;


    /*
     * Plugin options
     */

    public function init() {
        $this->ldap_host = $this->ldap_host;
        $this->ldap = ldap_connect($this->ldap_host) or die("LDAPAuth: Could not connect to LDAP");
        ldap_set_option($this->ldap,LDAP_OPT_PROTOCOL_VERSION,3);
        ldap_bind($this->ldap) or die("LDAPAuth: Could not bind to LDAP");
    }


    /* Called from UI menu */
    function ui_header($param) {
        $newparam = '<script src="plugins/ldapauth/script.js"></script>'."\n";
        $param = $param.$newparam;
        return $param;
    }
    private function get_user_info_int($uid) {
        $data =array();

        $results = ldap_search($this->ldap,$this->ldap_dn,"(uid=$uid)");
        $entries = ldap_get_entries($this->ldap, $results);
        array_shift($entries);
        $data['uid'] = $uid;
        $data['uidnumber']= $entries[0]['uidnumber'][0];
        $data['gidnumber']= $entries[0]['gidnumber'][0];

        $data['groups'] = array();
        $results = ldap_search($this->ldap,$this->ldap_dn,"(memberuid=$uid)");
        $entries = ldap_get_entries($this->ldap, $results);
        array_shift($entries);
        foreach ($entries as $g) {
            $data['groups'][]=$g['cn'][0];
        }

        return $data;
    }

    public function get_user_info($uid) {
        $data = $this->get_user_info_int($uid);
        $newdata = array();
        $newdata['uids'] = array();
        $newdata['uids'][] = $data['uid'];
        $newdata['uids'][] = $data['uidnumber'];
        $newdata['groups'] = $data['groups'];
        $newdata['groups'][] = $data['gidnumber'];
        return $newdata;
    }


    public function access_sql_filter($param)
    {
        $sql_where = $param[0];

        /* just ignore the standard filter */
        $sql_where = "";
        unset($param[2]["k_uid"]);

        /* build a new filter from ldap informations */
        $part = $this->get_user_info(get_user());
        $sql_where.= "(";
        $sql_where.= "uid IN ('".implode($part['uids'],"','")."') OR gid IN ('";
        $sql_where.= implode($part['groups'],"','")."')";

        /* Simple access control for files */
        if ($param[1] == "NAMES") {
            $sql_where.= ' OR (mode & 3)>0)';
        } else {
            $sql_where.= ')';
        }

        $param[0] = $sql_where;
        return $param;
    }

    /* Custom api call */
    public function api_native($param) {
        if (! $this->api_export)
            return;
        /* Custom api call */
        if ($param[0] == "ldapauth") {
            $data = $this->get_user_info_int(get_user());
            return $data;
        }

        if ($param[0] == "ldapauth_sqlfilter") {
            $data = $this->access_sql_filter(get_user());
            return $data;
        }
    }

    /* Called from UI menu */
    public function ui_menu_top($param) {
        if (! $this->ui_page || ! $this->api_export)
            return;
        $newparam = "<li><a href='#' onclick='ldapauth_GetInfo()'>WhoAmI</a></li>\n";
        $param = $param.$newparam;
        return $param;
    }

}

