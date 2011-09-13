<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
require "app/model/SearchManager.class.php";

class SearchController extends Controller
{
    public function executeMain()
    {
        $user = "";
        $group = "";
        $path = "";

        $menuManager = new MenuManager();
        $sections = $menuManager->getSections();
        $this->page->addVar( 'menu' , $sections );

        if( $this->getApplication()->getRequest()->hasPostValues() )
        {
            if( $this->getApplication()->getRequest()->getPostData("user") )
            {
                $user = $this->getApplication()->getRequest()->getPostData("user") ;
            }
            if( $this->getApplication()->getRequest()->getPostData("group") )
            {
                $group = $this->getApplication()->getRequest()->getPostData("group");
            }
            if( $this->getApplication()->getRequest()->getPostData("path") )
            {
                $path = $this->getApplication()->getRequest()->getPostData("path");
            }
            $searchManager = new SearchManager();
            $result = $searchManager->getStatistics( $user, $group, $path );
            $rowNumber = $searchManager->getRowNumber();
            $this->page->addVar( 'result' , $result );
            $this->page->addVar( 'rowNumber', $rowNumber );
            $this->page->addVar( 'page', 'result' );
        }
        else
        {
             $this->page->addVar( 'page', 'form' );
        }

        $this->page->addVar( 'fsname' , $menuManager->getfsname() );
    }
}

