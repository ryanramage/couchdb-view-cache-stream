var view_cache_stream = require('../lib/index')
var test = require('tape')
var devnull = require('dev-null');
var async = require('async')

test('fill in this', function (t) {
  var cache = view_cache_stream('http://localhost:5984/idx-edm-v5/_design/idx/_view/by_id')

  async.timesSeries(20, function(n, next) {
    run(cache, n+'', next)

  }, t.end)

})


function run (cache, name, cb) {
  console.time(name)
  cache(function(err, stream) {
    stream.on('end', function() {
      console.timeEnd(name)
      cb()
    })
    .pipe(devnull())
  })
}
