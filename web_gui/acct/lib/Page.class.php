<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

/**
 * This class creates the page to display
 */
class Page extends ApplicationComponent
{
    private $content;
    private $vars = array();
    private $template;

   /**
    * The constructor sets the variable template
    * @param application
    * @param template 
    */
    public function __construct( Application $app, $template )
    {
        parent::__construct( $app );
        $this->template = $template;
    }

   /**
    * This method creates variables which will be used in views
    * @param var name
    * @param value
    */
    public function addVar($var, $value)
    {
        $this->vars[$var] = $value;
    }

   /**
    * This method generates the content which will be displayed in the template
    * @return content
    */
    public function generatePage()
    {
        if ( !file_exists( $this->content ) )
        {
            throw new Exception( "The view $this->content is unreachable" );
        }
        extract($this->vars);

        ob_start();
        require $this->content;
        $content =  ob_get_clean();

        ob_start();
        require dirname(__FILE__).'/../app/templates/layout'.ucfirst($this->template).'.php'; 
        return ob_get_clean();
    }

    public function setContent( $content )
    {
        $this->content = $content;
    }

}

?>
