<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
require "app/model/GroupManager.class.php";

class GroupController extends Controller
{
    public function executeMain()
    {
        $others_count = 0;
        $others_size = 0;
        $top_count = array();
        $top_size = array();

        $menuManager = new MenuManager();
        $sections = $menuManager->getSections();

        $groupManager = new GroupManager();
        $result = $groupManager->getStat();

        $count = $result->getCount();
        $size = $result->getSize();
        array_multisort($count, SORT_NUMERIC, SORT_DESC);
        array_multisort($size, SORT_NUMERIC, SORT_DESC);

        
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
        foreach( $size as $key => $value )
        {
            if( $i < LIMIT )
            {
                $top_size[$key] = $value;
            }
            else
            {
                $others_size += $value;
            }
            $i++;
        }
        $top_size['Others'] = $others_size;

        $this->page->addVar( 'menu' , $sections );
        $this->page->addVar( 'top_count', $top_count );
        $this->page->addVar( 'top_size', $top_size );
        $this->page->addVar( 'statistics', $result );
    }

    public function executePopup()
    {
        $group_manager = new GroupManager();
        $acct_schema = $group_manager->getAcctSchema();
        //if the group name contains spaces, replace %20 by a space
        $_GET['group'] = str_replace("%20", " ", $_GET['group']);

        //if the owner field exists in ACCT table, sort the db result by owner
        if( array_key_exists( OWNER, $acct_schema ) )
            $result = $group_manager->getDetailedStat( $_GET['group'], OWNER );
        else
            $result = $group_manager->getDetailedStat( $_GET['group'], null );
            
        $this->page->addVar( 'acct_schema', $acct_schema );
        $this->page->addVar( 'result', $result );
    }
}

?>
