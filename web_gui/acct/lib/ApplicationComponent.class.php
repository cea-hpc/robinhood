<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

/**
 * This class is the parent of components such as Router, Page, Controller etc...
 */
abstract class ApplicationComponent
{
    private $application;

    public function __construct( Application $application )
    {
        $this->application = $application;
    }

   /**
    * This method return the current application object
    * @return application
    */
    public function getApplication()
    {
        return $this->application;
    }

}
?> 
