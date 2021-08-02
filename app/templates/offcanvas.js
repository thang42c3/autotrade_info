(function () {
  'use strict'

  document.querySelector('[data-bs-toggle="offcanvas"]').addEventListener('click', function () {
    document.querySelector('.offcanvas-collapse').classList.toggle('open')
  })
})()

http.listen((process.env.PORT || 5000), function(){
  console.log('listening on *:5000');
});