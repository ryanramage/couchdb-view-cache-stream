var Readable = require('stream').Readable
var request = require('request')
var jsonfilter = require('jsonfilter')
var ndjson = require('ndjson')
var through = require('through2')
var levelup = require('levelup')
var devnull = require('dev-null')

module.exports = function (view_url, opts) {
  if (!opts) opts = {}
  var last_etag
  var current_view

  return function (ready) {
    request.head(view_url, function (err, resp, body) {
      if (err) return ready(err)
      if (last_etag === resp.headers.etag) {
        console.log('using thing')
        return ready(null, current_view.createValueStream().pipe(ndjson.stringify()))
      }

      console.log('fetching')
      var db_path = opts.db_path || '/' + 'vcs/' + Date.now() + '/' + resp.headers.etag;
      var tempdb = levelup(db_path, {
        db: opts.db_type || require('memdown'),
        keyEncoding: 'json',
        valueEncoding: 'json'
      })
      var dapipe = request(view_url).pipe(jsonfilter('rows.*'))
          .pipe(ndjson.parse())
          .pipe(through.obj(function(data, enc, cb) {
            tempdb.put(data.key, data.value, function (err) {
              if (err) return cb(err)
              cb(null, JSON.stringify(data.value) + '\n')
            })

          }, function (cb) {
            last_etag = resp.headers.etag;
            current_view = tempdb;
            cb();
          }))

      ready(null, dapipe)
    })
  }
}
