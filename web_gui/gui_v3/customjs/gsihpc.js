
$(document).ready(function() {


  $('#filterform input').on('keypress', function(event){
    if(event.which==13 && !event.shiftKey){
      var $btn = $('#myStateButton').button('loading');
      GetGraph(lastGet);
      window.setTimeout( function(){
        $btn.button('reset')
      }, 500 );
    }
  });
  $('#myStateButton').on('click', function () {
    var $btn = $(this).button('loading');
    GetGraph(lastGet);
    window.setTimeout( function(){
      $btn.button('reset')
    }, 500 );

  })
});
