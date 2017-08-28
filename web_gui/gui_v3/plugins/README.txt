Robinhood new web interface (gui_v3) plugins

I - INTRODUCTION
===========

This is a very simple plugin system which allows some customization in the robinhood gui.

II - USE
=================

In config.php or config_local.php just add the name of the plugins to $PLUGINS_REG.

Plugins are run in the $PLUGINS_REG list order. Some plugins might doesn't work with other plugins

III - WRITE
===================

The requirement for a plugin are:
 -Folder with the name of the plugin
 -php file called plugin.php which contains a class which match your plugin name

The plugin class overload a class called plugin which contains all the existings callback.
Methods are described in plugin.php at the root of the website.

