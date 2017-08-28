
/***********************************
 * Build table and get data from API
 **********************************/
function fsinfo_GetInfo(){

/* Create the table template */
    document.getElementById("main_content").innerHTML = `
<h1>FS Info</h1>
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
