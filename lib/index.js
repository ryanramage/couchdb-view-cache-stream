var url = require('url')
var Readable = require('stream').Readable
var request = require('request')
var jsonfilter = require('jsonfilter')
var ndjson = require('ndjson')
var through = require('through2')
var levelup = require('levelup')
var devnull = require('dev-null')
var from = require('from2')

module.exports = function (view_url, opts) {
  if (!opts) opts = {}
  var last_etag
  var current_view

  return function (ready) {
    var _all_docs = toInfoUrl(view_url)
    request.head(_all_docs, function (err, resp, body) {
      if (err) return ready(err)
      if (last_etag === resp.headers.etag) return ready(null, from.obj(current_view))

      var tempdb = []
      var dapipe = request(view_url).pipe(jsonfilter('rows.*'))
          .pipe(ndjson.parse())
          .pipe(through.obj(function(data, enc, cb) {
            tempdb.push(data.value)
            cb(null, data.value)
          }, function (cb) {
            last_etag = resp.headers.etag;
            current_view = tempdb;
            cb();
          }))

      ready(null, dapipe)
    })
  }
}

function toInfoUrl(view_url) {
  var view_url_parsed = url.parse(view_url);
  var temp = view_url_parsed.pathname.split('/');
  temp.splice(-2, 2)
  temp.push('_all_docs');
  temp = temp[1] + '/_all_docs'
  view_url_parsed.pathname = temp;
  return url.format(view_url_parsed)
}
