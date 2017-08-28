#!/bin/bash

# Static lint
for f in $(find ../ -name "*.php"); do
       php -l $f
       if [ $? -ne 0 ]; then
               exit $?
       fi;
done;

# Advanced tests
php-cgi ./nonreg.php
exit $?

