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

        $this->page->addVar( 'fsname' , $groupManager->getfsname() );
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
            
        $group_status = array();
        $has_status = 0;
        foreach( $result as $line )
        {
            if( array_key_exists( STATUS, $acct_schema ) ) {
                $has_status = 1;
                if ( !array_key_exists( TYPE, $acct_schema ) || ( $line[TYPE] != 'dir' ) ) {
                        if (isset($group_status[$line[STATUS]]))
                            $group_status[$line[STATUS]] += $line[COUNT];
                        else
                            $group_status[$line[STATUS]] = $line[COUNT];
                }
            }
        }
 
        $this->page->addVar( 'acct_schema', $acct_schema );
        $this->page->addVar( 'result', $result );
        if ($has_status)
            $this->page->addVar( 'group_status',  $group_status );
    }
}

?>
