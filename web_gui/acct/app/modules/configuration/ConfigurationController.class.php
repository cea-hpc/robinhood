<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
require "app/model/UserManager.class.php";

class ConfigurationController extends Controller
{
    public function executeConfigForm()
    {
        //recupere info
        if( $this->getApplication()->getRequest()->getPostData("dbms") )
        {
        }
    }
}
