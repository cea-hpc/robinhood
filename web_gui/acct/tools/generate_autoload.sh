#/bin/sh
autoload_path="../lib/autoload.php"

touch $autoload_path

is_first=1

content="<?php
    require_once 'ezcomponents/Base/src/base.php';
    spl_autoload_register( array( 'ezcBase', 'autoload' ) );

    function autoload(\$class)
    {
        \$classes = array (\n";

files=`find ../lib ../app -name "*.class.php"`

for file in ${files}; do
    if (( $is_first != 1 )); then
        content=$content",\n"
    fi
    class_name=`expr $file : ".*/\(.*\)\.class\.php"`
    content=$content"\t\t'"$class_name"' => '"$file"'"
    is_first=0;
done

content=$content"\n\t);
	require dirname(__FILE__).'/'.\$classes[\$class];
    }
    spl_autoload_register('autoload');
?>"
echo -e "$content" > $autoload_path
echo "Autoload file generated in $autoload_path"
