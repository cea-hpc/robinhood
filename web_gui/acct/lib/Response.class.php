<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

class Response extends ApplicationComponent
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

    public function redirect404()
    {
        $this->page = new Page( $this->getApplication(), "blank" );
        $this->page->setContent( 'error404.html' );
        //$this->addHeader( 'HTTP/1.0 404 Not Found' );
        $this->send();
    }
}

?>
