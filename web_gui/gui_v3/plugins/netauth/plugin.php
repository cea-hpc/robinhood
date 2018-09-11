<?php
/*
 * Copyright (C) 2018 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */


/*
 * netauth V0.1
 * Use remote_addr informations for access control
 */

class netauth extends Plugin {
    public $Name = "IPAuth";
    public $Description = "Use IP for access control";
    public $Version = "V0.1";

    /* Use DNS name instead */
    public $dns_resolv = true;

    /* Force IP Auth even if we already have an uid*/
    public $force_netauth = false;


    /*
     * Plugin options
     */

    public function init() {
    }


    public function get_user($uid) {
	if ($uid == '$NOAUTH' || $this->force_netauth) {
		if ($this->dns_resolv  && array_key_exists('REMOTE_HOST', $_SERVER))
			return $_SERVER['REMOTE_HOST'];
		else
			return $_SERVER['REMOTE_ADDR'];
	}
        return $uid;
    }



}

