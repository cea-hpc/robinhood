<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

/**
 * This class is the parent of all the controllers.
 */
abstract class Controller extends ApplicationComponent
{
    protected $module;
    private $type;
    protected $page;

   /**
    * The constructor instantiate the Page object
    * @param application
    * @param module
    * @param type
    */
    public function __construct( Application $app, $module, $type )
    {
        parent::__construct( $app );
        $this->page = new Page( $app, $type );
        $this->module = $module;
        $this->type = $type;
        $this->setView();
    }

    public function execute()
    {
        $method = 'execute'.ucfirst($this->type);
        if( !is_callable( array($this, $method) ) )
        {
            throw new RuntimeException( "The method ".$method." is undefined for the module ".$this->module );
        }
        $this->$method();
    }

   /**
    * This method selects the view matching the page and module
    */
    private function setView()
    {
        $view = dirname(__FILE__).'/../app/modules/'.$this->module.'/views/'.$this->type.'.php';
        $this->page->setContent( $view );
    }

   /**
    * This method returns the Page
    * @return page
    */
    public function getPage()
    {
        return $this->page;
    }
}
?>
