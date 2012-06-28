<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
require "app/model/SizeManager.class.php";

class SizeController extends Controller
{
    public function executeMain()
    {
//        $others_count = 0;
//        $others_size = 0;
//        $top_count = array();
//        $top_size = array();

        $menuManager = new MenuManager();
        $sections = $menuManager->getSections();

        $sizeManager = new SizeManager();
        $result = $sizeManager->getSizeProfile();

        $sz_ranges = array();
        global $sz_range_name;

        $i=0;
        foreach ($result as $k => $val)
        {
            $sz_ranges[$sz_range_name[$i]] = $val;
            $i++;
        }

        $users = $sizeManager->getSizeProfileUsers();

        $this->page->addVar( 'menu' , $sections );
        $this->page->addVar( 'sz_ranges', $sz_ranges );
        $this->page->addVar( 'users', $users );
        $this->page->addVar( 'fsname' , $sizeManager->getfsname() );
    }

    public function executePopup()
    {
        $sizeManager = new SizeManager();
        $acct_schema = $sizeManager->getAcctSchema();

        $user = $_GET['user'];

        //if the user name contains spaces, replace %20 by a space
        $user = str_replace("%20", " ", $user);

        $one_user = $sizeManager->getSizeProfileUsers($user);

        $this->page->addVar( 'user_info', $one_user );
    }
}

?>
