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


require_once "config.php";
require_once "common.php";
require_once "plugin.php";

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
    <link rel="stylesheet" href="css/bootstrap-datetimepicker.css">
    <link rel="stylesheet" href="css/bootstrap-slider.css">
    <script src="js/jquery-2.2.4.min.js"></script>
    <script src="js/bootstrap.min.js"></script>
    <script src="js/Chart.bundle.js"></script>
    <script src="js/jquery.dataTables.js"></script>
    <script src="js/filesize.dataTables.js"></script>
    <script src="js/moment.js"></script>
    <script src="js/bootstrap-datetimepicker.min.js"></script>
    <script src="js/bootstrap-slider.js"></script>
    <script src="customjs/param.php"></script>
    <script src="customjs/newgui.js"></script>
    <?php echo plugins_call("ui_header", "<!--header from plugins-->"); ?>
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
        <li><a id="loggedas" onclick="logout()"  href=""></a></li>
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
      <a class="navbar-brand" href="<?php echo $CUSTOM['vendor_url']; ?>"><img src="<?php echo $CUSTOM['vendor_logo']; ?>" height="64"></a>
    </div>
<br><br>
          <ul class="nav nav-sidebar">
<?php

/****************************************
 *       LEFT MENU BUTTONS               *
 ****************************************/

$fields = get_acct_columns();
foreach ($fields as $field) {
        echo '<li><a href="#" onclick="GetGraph(\''.$field.'\')">'.l($field).'</a></li>';
}

if (!$DISABLE_FILES_PAGE)
{
	echo '<li><a href="#" onclick="GetGraph(\'Files\')">Files</a></li>';
}

echo plugins_call("ui_menu_top", "<!--Data from plugins-->\n");
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

            <fieldset class="form-group">
            <label>Size range</label>
            <input id="ex1" data-slider-id='ex1Slider' type="text" name=minsize />
            <input id="ex2" data-slider-id='ex2Slider' type="text" name=maxsize />
            </fieldset>

<script>
$('#ex1').slider({
        min: 0,
        max: 1125899906842624,
        step: 8,
        scale: 'logarithmic',
        formatter: function(value) {
                return formatBytes(value,1);
        }
});


$('#ex2').slider({
        min: 0,
         max: 1125899906842624,
        value: 1125899906842624,
        step: 8,
        scale: 'logarithmic',
        formatter: function(value) {
                return formatBytes(value,1);
        }
});

</script>

<?php
    echo plugins_call("ui_form_filter", "<!--Data from plugins-->");
?>

            <button type="button" id="filter" class="btn btn-primary" data-loading-text="Loading..." autocomplete="off" onclick="GetGraph(lastGet)">Filter</button>
            <button type="button" class="btn btn-primary" onclick="CleanForm();GetGraph(lastGet)">Clean</button>
    </form>


<?php
    echo plugins_call("ui_menu_bottom", "<!--data from plugins-->\n");
?>

</div>

<div class="col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2 main"> <!-- Graph/Data Div-->
    <div id="messagebox"></div> <!-- MessageBox Div-->
    <div id="main_content">
    </div>
</div> <!-- Graph Div end-->
</div>
</div>


<?php
$permission = getFilePermission("config.php");
if ($permission != "640") {
        echo "<script>$(msg_danger(\"Bad permission on config.php ($permission) shoud be 640 .\"))</script>";
}

if ($DB_LASTERROR != "") {
        echo "<script>$(msg_danger(\"PDO Database error: $DB_LASTERROR\"))</script>";
}

/* Load values in the filder is parameters are set */
echo setFormValues();
/* Load a specific graph is the parameter is set */
echo callGraph();
?>

</body>
</html>

