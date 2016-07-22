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
<!--  <link rel="stylesheet" href="css/jquery.dataTables.min.css">
<link rel="stylesheet" href="css/dataTables.material.css"> -->
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
<div class="container-fluid">
<div class="page-header">
<h1>Robinhood Policy Engine 
<img src="images/logo_rh_sf.gif" class=".img-thumbnail" align="right"> 
<img src="images/logoCEA20.jpg" height="64" align="right" class=".img-thumbnail">
</h1>
<p class="check-api-id">Robinhood API state is : </p>
</div>
<div class="row">
<div class="col-sm-2">
<div class="list-group">

<?php

/****************************************
 *       LEFT MENU BUTTONS               *
 ****************************************/

$fields = get_acct_columns();
foreach ($fields as $field) {
        echo '<button type="button" class="list-group-item" onclick="GetGraph(\''.$field.'\')">'.l($field).'</button>';
}

echo '<button type="button" class="list-group-item" onclick="GetGraph(\'Files\')">Files</button>';
?>

</div>
</div>
<?php
/****************************************
 *                 GRAPH                *
 ****************************************/
?>
<div class="col-sm-8" >
<canvas id="ctx"></canvas>
</div>
</div>

<div class="row">
<?php
/****************************************
 *                 FILTER               *
 ****************************************/
?>
<div class="col-sm-2">
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
<button type="button" class="btn btn-primary" onclick="GetGraph(lastGet)">Refresh</button>
<button type="button" class="btn btn-primary" onclick="CleanForm()">Clean</button>
</form>

</div>
<?php
/****************************************
 *               DATATABLES             *
 ****************************************/
?>

<div class="col-sm-8">
<table id="datalist" class="table table-striped table-bordered" width="100%"></table>
</div>
</div>
</div>

</body>
</html>

