
/***********************************
 * Build table and get data from API
 **********************************/
function browser_GetInfo(){

        /* Create the table template */
        document.getElementById("main_content").innerHTML = `
                <h1>Browser</h1>
                <div id="tree"></div>
                `;


        /* Retrieve the data */
        $.ajax({
                url: "api/index.php?request=native/vars"
        }).then(function(data) {
                var RootId=data['RootId'];
                var tree = [{
                        text: "/",
                nodes: [],
                id: data['RootId'],
                lazyLoad: true,

                }];
                console.log(tree);
                $('#tree').treeview({
                        data: tree,
                        showTags: true,
                        lazyLoad: function(node, loader) {

                                $.ajax({
                                        url: "api/index.php?request=native/files/parent_id.filter/" + node.id
                                }).then(function(data) {
                                        console.log(data);
                                        nodes=[];

                                        for (var key in data) {
                                                tnode={}
                                                tnode['text']=data[key].name;
                                                tnode['id']=data[key].id;
                                                tnode['tags']=[]
                                                tnode['tags'].push(data[key].uid+" : " + data[key].gid + " : " + data[key].mode);
                                                if (data[key]['type']=="dir") {
                                                        tnode['icon']="glyphicon glyphicon-folder-close";
                                                        tnode['lazyLoad']=true;
                                                        tnode['showTags']=true;
                                                } else {
                                                        tnode['icon']="glyphicon glyphicon-open-file";
                                                        tnode['tags'].push(formatCount(data[key].blocks + "Blks"));
                                                        tnode['tags'].push(formatBytes(data[key].size));

                                                }

                                                nodes.push(tnode);
                                        }
                                        loader(nodes); 
                                });

                        }, /* end of lazyload */

                    });
        });

}

// Retrieve the data
/*            $.ajax({
                url: "api/index.php?request=native/names"
            }).then(function(data) {
            var tree = {};
            for (var key in data) {
                console.log(data[key);
            }
        }*/
