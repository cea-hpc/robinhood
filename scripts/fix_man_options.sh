#!/bin/bash
sed -e "s#\\\fP-\\\fI\([A-Za-z]*\)\\\fP#-\1\\\fP#g" | sed -e "s#-\\\fI\([A-Za-z]*\)\\\fP#-\1#g"
