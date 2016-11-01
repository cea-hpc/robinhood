<!DOCTYPE html>
<?php
/*
 * Copyright (C) 2016 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

include "config.php";
include "common.php";

?>

<?php
/*******************************************************
 *                   HEADER                             *
 *******************************************************/
?>
<html lang="en">
<head>
<title>Robinhood Report</title>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="css/bootstrap.css">
<link rel="stylesheet" href="css/dataTables.bootstrap.css">
<link rel="stylesheet" href="css/dashboard.css">
<script src="js/jquery-2.2.4.min.js"></script>
<script src="js/bootstrap.min.js"></script>
<script src="js/Chart.bundle.js"></script>
<script src="js/jquery.dataTables.js"></script>
<script src="js/filesize.dataTables.js"></script>
<script src="customjs/newgui.js"></script>
</head>

<?php
/*******************************************************
 *                   BODY                               *
 *******************************************************/
?>
<body>

    <nav class="navbar navbar-inverse navbar-fixed-top">
      <div class="container-fluid">
        <div class="navbar-header">
          <button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#navbar" aria-expanded="false" aria-controls="navbar">
            <span class="sr-only">Toggle navigation</span>
            <span class="icon-bar"></span>
            <span class="icon-bar"></span>
            <span class="icon-bar"></span>
          </button>
          <a class="navbar-brand" href="#" id="main_title">Robinhood Policy Engine</a>

        </div>
        <div id="navbar" class="navbar-collapse collapse">
          <ul class="nav navbar-nav navbar-right">
            <li><a href="https://github.com/cea-hpc/robinhood/wiki/Documentation">Help</a></li>
          </ul>
    <i><p id="quote" class="navbar-text"></p></i>

        </div>
      </div>
    </nav>

    <div class="container-fluid">
      <div  class="row">
        <div class="col-sm-3 col-md-2 sidebar">
     <div class="nav nav-sidebar">
      <a class="navbar-brand" href="http://robinhood.sf.net"><img src="images/logo_rh_sf.gif" ></a>
      <a class="navbar-brand" href="http://www-hpc.cea.fr/index-en.htm"><img src="images/logoCEA20.jpg" height="64"></a>
    </div><br><br>
          <ul class="nav nav-sidebar">
<?php

/****************************************
 *       LEFT MENU BUTTONS               *
 ****************************************/

$fields = get_acct_columns();
foreach ($fields as $field) {
        echo '<li><a href="#" onclick="GetGraph(\''.$field.'\')">'.l($field).'</a></li>';
}

echo '<li><a href="#"  onclick="GetGraph(\'Files\')">Files</a></li>';
?>

          </ul>
<?php
/****************************************
 *                 FILTER               *
 ****************************************/
?>

    <form id="filterform" name="filterform">
    <fieldset class="form-group">
    <label for="formUID">Filter</label>
    <input type="text" class="form-control" id="formUID" name="uid" placeholder="UID">
    </fieldset>
    <fieldset class="form-group">
    <input type="text" class="form-control" id="formGID" name="gid" placeholder="GID">
    </fieldset>
    <fieldset class="form-group">
    <input type="text" class="form-control" id="formFilename" name="filename" placeholder="Filename">
    </fieldset>
    <button type="button" class="btn btn-primary" id="myStateButton" data-loading-text="Loading..." autocomplete="off">Filter</button>
    <button type="button" class="btn btn-primary" onclick="CleanForm();GetGraph(lastGet)">Clean</button>
    </form>

        </div>
        <div class="col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2 main">
       <div id="messagebox"></div>
        <canvas id="ctx"></canvas>
        <table id="datalist" class="table table-striped table-bordered" width="100%"></table>

      </div>

        </div>
      </div>
    </div>



<?php
$permission = getFilePermission("config.php");
if ($permission != "640") {
        echo "<script>$(msg_danger(\"Bad permission on config.php ($permission) shoud be 640 .\"))</script>";
}

if ($DB_TYPE == "" || $DB_HOST == "" || $DB_NAME == "" || $DB_USER == "") {
        echo "<script>$(msg_danger(\"You need to configure the database in config.php ! Type/home/db_name or user is missing !\"))</script>";
}

if ($DB_LASTERROR!="") {
        echo "<script>$(msg_danger(\"PDO Database error: $DB_LASTERROR\"))</script>";
}



?>

</body>
</html>
