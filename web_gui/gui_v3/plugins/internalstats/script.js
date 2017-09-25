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
function internalstats_GetInfo(){

/* Create the table template */
    document.getElementById("main_content").innerHTML = `
<h1>Internal Stats</h1>
 <table class="table">
    <thead>
      <tr>
        <th>Key</th>
        <th>Val</th>
      </tr>
    </thead>
    <tbody id=fsinfobody name=fsinfobody>
    </tbody>
  </table>
 `;

/* Retrieve the data */
    $.ajax({
        url: "api/index.php?request=native/vars"
    }).then(function(data) {
    console.log(data);
    for (var key in data) {
        $("#fsinfobody").append("<tr><td>"+key+"</td><td>"+data[key]+"</td></tr>")
    }
    });
}
