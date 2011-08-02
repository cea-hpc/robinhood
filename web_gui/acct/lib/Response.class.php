<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

class Response
{
    private $page;

   /**
    * This method displays the page generated
    */ 
    public function send()
    {
        exit( $this->page->generatePage() );
    }

   /**
    * This method set the variable 'page'
    */
    public function setPage( Page $page )
    {
        $this->page = $page;
    }

    //TODO -> redirect404();
}

?>
