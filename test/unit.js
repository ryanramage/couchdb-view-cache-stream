var view_cache_stream = require('../lib/index')
var test = require('tape')
var async = require('async')
var couchr = require('couchr')
var concat = require('concat-stream')
var db = 'http://localhost:5984/idx-edm-v5'
var view = '/_design/idx/_view/by_id'
var docid = 'E3331885'

test('fill in this', function (t) {
  var cache = view_cache_stream(db + view)

  async.timesSeries(10, function(n, next) {
    run(cache, n+'', next)
  }, function (err) {
    t.error(err)
    alter(db, docid, function(err) {
      t.error(err)
      setTimeout(function(){
        async.timesSeries(10, function(n, next) {
          run(cache, n+'', next)
        }, t.end)
      }, 5000);
    })
  })
})


function run (cache, name, cb) {
  console.time(name)
  cache(function(err, stream) {
    stream.on('end', function() {
      console.timeEnd(name)
      cb()
    })
    .pipe(concat(function(data){
      console.log('data')
    }))

  })
}

function alter (db, docid, cb) {
  couchr.get(db + '/' + docid, function (err, doc) {
    doc['Legal Plan'] += 'A'
    couchr.put(db + '/' + docid, doc, cb)
  })
}
