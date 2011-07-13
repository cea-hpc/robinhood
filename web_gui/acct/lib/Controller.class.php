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
    private $pageName;
    protected $page;

   /**
    * The constructor instantiate the Page object
    * @param application
    * @param module
    * @param type
    */
    public function __construct( Application $app, $module,  $pageName, $type )
    {
        parent::__construct( $app );
        $this->page = new Page( $app, $type );
        $this->module = $module;
        $this->type = $type;
        $this->pageName = $pageName;
        $this->setViews();
    }

    public function execute()
    {
        $method = 'execute'.ucfirst($this->pageName);
        if( !is_callable( array($this, $method) ) )
        {
            throw new RuntimeException( "The page ".$this->pageName." is undefined for the module ".$this->module );
        }
        $this->$method();
    }

   /**
    * This method selects the view matching the page and module
    */
    private function setViews()
    {
        $view = dirname(__FILE__).'/../app/modules/'.$this->module.'/views/'.$this->pageName.'.php';
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
