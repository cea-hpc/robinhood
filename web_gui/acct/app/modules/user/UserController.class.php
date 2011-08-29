<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
require "app/model/UserManager.class.php";

class UserController extends Controller
{
    public function executeMain()
    {
        $others_count = 0;
        $others_size = 0;
        $top_count = array();
        $top_size = array();

        $menuManager = new MenuManager();
        $sections = $menuManager->getSections();

        $userManager = new UserManager();
        $result = $userManager->getStat();

        $count = $result->getCount();
        $blks = $result->getBlocks();
        array_multisort($count, SORT_NUMERIC, SORT_DESC);
        array_multisort($blks, SORT_NUMERIC, SORT_DESC);

        
        //Create array for the count section (top10)
        $i = 0;
        foreach( $count as $key => $value )
        {
            if( $i < LIMIT )
            {
                $top_count[$key] = $value;
            }
            else
            {
                $others_count += $value;
            }
            $i++;
        }
        $top_count['Others'] = $others_count;
        
        //Create array for the volume section (top10)
        $i = 0;
        foreach( $blks as $key => $value )
        {
            if( $i < LIMIT )
            {
                $top_size[$key] = $value * DEV_BSIZE;
            }
            else
            {
                $others_size += $value * DEV_BSIZE;
            }
            $i++;
        }
        $top_size['Others'] = $others_size;

        $this->page->addVar( 'menu' , $sections );
        $this->page->addVar( 'top_count', $top_count );
        $this->page->addVar( 'top_size', $top_size );
        $this->page->addVar( 'statistics', $result );

        $this->page->addVar( 'fsname' , $userManager->getfsname() );
    }

    public function executePopup()
    {
        $userManager = new UserManager();
        $acct_schema = $userManager->getAcctSchema();

        //if the user name contains spaces, replace %20 by a space
        $_GET['user'] = str_replace("%20", " ", $_GET['user']);
        
         //if the group field exists in ACCT table, sort the db result by group
        if( array_key_exists( GROUP, $acct_schema ) )
            $result = $userManager->getDetailedStat( $_GET['user'], GROUP );
        else
            $result = $userManager->getDetailedStat( $_GET['user'], null );

        $user_status = array();
        $has_status = 0;
        foreach( $result as $line )
        {
            if( array_key_exists( STATUS, $acct_schema ) ) {
                $has_status = 1;
                if ( !array_key_exists( TYPE, $acct_schema ) || ( $line[TYPE] != 'dir' ) ) {
                        if (isset($user_status[$line[STATUS]]))
                            $user_status[$line[STATUS]] += $line[COUNT];
                        else
                            $user_status[$line[STATUS]] = $line[COUNT];
                }
            }
        }
        
        $this->page->addVar( 'acct_schema', $acct_schema );
        $this->page->addVar( 'result', $result );
        if ($has_status)
            $this->page->addVar( 'user_status',  $user_status );
    }
}

?>
