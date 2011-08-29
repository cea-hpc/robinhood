<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
require "app/model/StatusManager.class.php";

class StatusController extends Controller
{
    public function executeMain()
    {
        $others_count = 0;
        $others_size = 0;
        $top_count = array();
        $top_size = array();

        $menuManager = new MenuManager();
        $sections = $menuManager->getSections();

        $statusManager = new StatusManager();
        $result = $statusManager->getStat();

        $count = $result->getCount();
        $blks = $result->getBlocks();
        arsort( $count, SORT_NUMERIC );
        arsort( $blks, SORT_NUMERIC );

        
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

        $this->page->addVar( 'fsname' , $statusManager->getfsname() );
    }

    public function executePopup()
    {
        $statusManager = new StatusManager();
        $acct_schema = $statusManager->getAcctSchema();

        //if the user name contains spaces, replace %20 by a space
        $_GET['status'] = str_replace("%20", " ", $_GET['status']);
        
         //if the owner field exists in ACCT table, sort the db result by group
        if( array_key_exists( OWNER, $acct_schema ) )
            $result = $statusManager->getDetailedStat( $_GET['status'], OWNER );
        else
            $result = $statusManager->getDetailedStat( $_GET['status'], null );
        
        $this->page->addVar( 'acct_schema', $acct_schema );
        $this->page->addVar( 'result', $result );

    }
}

?>
