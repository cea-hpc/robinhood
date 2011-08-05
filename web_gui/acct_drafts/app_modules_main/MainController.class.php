<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
require "app/model/UserManager.class.php";

class MainController extends Controller
{
    public function executeMain()
    {
        $others_count = 0;
        $others_size = 0;
        $top_count = array();
        $top_size = array();
        $menu = array();

        $menuManager = new MenuManager();
        $sections = $menuManager->getSections();

        $manager_name = ucfirst( $this->module ).'Manager';
        $manager = new $manager_name();
        $result = $manager->getStat();

        $count = $result->getCount();
        $size = $result->getSize();
        array_multisort($count, SORT_NUMERIC, SORT_DESC);
        array_multisort($size, SORT_NUMERIC, SORT_DESC);

        //Create final arrays
        $i = 0;
        foreach( $count as $key => $value )
        {
            if( $i < LIMIT )
            {
                $result_count[$key] = $value;
            }
            else
            {
                $others_count += $value;
            }
            $i++;
        }
        $result_count['Others'] = $others_count;
        
        $i = 0;
        foreach( $size as $key => $value )
        {
            if( $i < LIMIT )
            {
                $result_size[$key] = $value;
            }
            else
            {
                $others_size += $value;
            }
            $i++;
        }
        $result_size['Others'] = $others_size;
        $this->page->addVar( 'menu' , $menu );
        $this->page->addVar( 'count', $result_count );
        $this->page->addVar( 'size', $result_size );
        $this->page->addVar( 'statistics', $result );
        $this->page->addVar( 'index', $this->module );
    }

    public function executePopup()
    {
        $manager_name = ucfirst( $this->module ).'Manager';
        $manager = new $manager_name();
        $acct_schema = $manager->getAcctSchema();

        if( $this->module == "user" )
        {
            //if the user name contains spaces, replace %20 by a space
            $_GET['user'] = str_replace("%20", " ", $_GET['user']);
            
             //if the group field exists in ACCT table, sort the db result by group
            if( array_key_exists( GROUP, $acct_schema ) )
                $result = $manager->getDetailedStat( $_GET['user'], GROUP );
            else
                $result = $manager->getDetailedStat( $_GET['user'], null );
        }
        else if( $this->module == "group" )
        {
             //if the group name contains spaces, replace %20 by a space
            $_GET['group'] = str_replace("%20", " ", $_GET['group']);

            //if the owner field exists in ACCT table, sort the db result by owner
            if( array_key_exists( OWNER, $acct_schema ) )
                $result = $manager->getDetailedStat( $_GET['group'], OWNER );
            else
                $result = $manager->getDetailedStat( $_GET['group'], null );
                
        }
        $this->page->addVar( 'acct_schema', $acct_schema );
        $this->page->addVar( 'result', $result );
    }
}

?>
