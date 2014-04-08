f=$1

if [ ! -f $f ]; then
    echo "$f: file not found"
    exit 1
fi

# remove extra space after parenthesis
sed -e 's#( #(#g' $f | sed -e 's# )#)#g'
