<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

require 'autoload.php';

/**
 * This class is the main one.
 */
class Application
{
    private $request;
    private $response;

    public function __construct()
    {
        $this->request = new Request( $this );
        $this->response = new Response( $this );
    }

   /**
    * This method launches the application.
    */
    public function run()
    {
        $router = new Router($this);

        $controller = $router->getController();
        $controller->execute();

        $this->response->setPage($controller->getPage());
        $this->response->send();
    }

   /**
    * This method returns the application request object.
    * @return Request
    */ 
    public function getRequest()
    {
        return $this->request;
    }

   /**
    * This method returns the application response object.
    * @return Response
    */
    public function getResponse()
    {
        return $this->response;
    }

}

?>
