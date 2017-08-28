/*
 * Copyright (C) 2016-2017 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */


/***********************************
 * Build table and get data from API
 * Use lazyload
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
                                                tnode['tags'].push(data[key].uid+" : " + data[key].gid + " : " + parseInt(data[key].mode,10).toString(8));
                                                if (data[key]['type']=="dir") {
                                                        tnode['icon']="glyphicon glyphicon-folder-close";
                                                        tnode['lazyLoad']=true;
                                                        tnode['showTags']=true;
                                                        /* You can add more tags for folder here */
                                                } else {
                                                        tnode['icon']="glyphicon glyphicon-open-file";
                                                        tnode['tags'].push(formatCount(data[key].blocks + "Blks"));
                                                        tnode['tags'].push(formatBytes(data[key].size));
                                                        /* You can add more tags for files here */

                                                }

                                                nodes.push(tnode);
                                        }
                                        loader(nodes);
                                });

                        }, /* end of lazyload */

                    });
        });

}
