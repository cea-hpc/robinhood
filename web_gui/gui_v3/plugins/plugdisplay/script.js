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

function plugdisplay_GetInfo(plugin){
        /* Retrieve the data */
        $.ajax({
                url: "plugins/"+plugin+"/help.html",
                statusCode: {
                        404: function () {
                            document.getElementById("main_content").innerHTML = "<h1>Help not available</h1><h2>Read the sources</h2><h3>The hospitality in this country is as warm as the weather.</h3>"
                        }
                }
        }).then(function(data) {
                document.getElementById("main_content").innerHTML = data;
        });
}
