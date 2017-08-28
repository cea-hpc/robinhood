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
 **********************************/
function ldapauth_GetInfo(){

        /* Create the table template */
        document.getElementById("main_content").innerHTML = `

                <h1>WhoAmI</h1>
                <div id="ldapauth" name="ldapauth"></div>
                <table class="table">
                <thead>
                <tr>
                <th>Group</th>
                </tr>
                </thead>
                <tbody id=fsinfobody name=fsinfobody>
                </tbody>
                </table>
                `;

        /* Retrieve the data */
        $.ajax({
                url: "api/index.php?request=native/ldapauth"
        }).then(function(data) {
                document.getElementById("ldapauth").innerHTML = "<h3> User: " + data.uid + " - UidNumber: " + data.uidnumber + " - GidNumber: " + data.gidnumber +"</h3>";
                console.log(data);
                for (var key in data.groups) {
                        $("#fsinfobody").append("<tr><td>"+data.groups[key]+"</td></tr>")
                }
        });
}
