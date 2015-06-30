var url = require('url');
var request = require('request');
var jsonfilter = require('jsonfilter');
var ndjson = require('ndjson');
var through = require('through2');
var streamify = require('stream-array');
var object_stream = require('object-stream')
var clone = require('clone-deep')

module.exports = function(view_url, opts){

  if (!opts) opts = {};
  if (!opts.preload) opts.preload = false;

  var info_url = toInfoUrl(view_url);
  var update_seq;
  var last_array;


  return function(){

    if (!last_array) {
      last_array = [];
      var stream = request(view_url).pipe(jsonfilter('rows.*.value'))
        .pipe(ndjson.parse())
        .pipe(through.obj(function(obj, enc, cb){
          last_array.push(clone(obj));
          this.push(obj);
          cb();
        }))
      return stream;

    }
    return object_stream.fromArray(clone(last_array));
  }

};



function fetch_real_view(view_url, cb) {
    var stream = cache().pipe(jsonfilter('rows.*.value'))
      .pipe(ndjson.parse())
  request({url: view_url, encoding: null}, function(err, resp, body){
    cb(err, body);
  })
}

function fetch_update_seq(info_url, cb) {
  request(info_url, function(err, resp, body){
    if (err) return cb(err);
    try { return cb(null, JSON.parse(body).view_index.update_seq)  }
    catch(e) { cb(e) }
  })
}


function toInfoUrl(view_url) {
  var view_url_parsed = url.parse(view_url);

  var temp = view_url_parsed.pathname.split('/');
  temp.splice(-2, 2)
  temp.push('_info');
  temp = temp.join('/');
  view_url_parsed.pathname = temp;
  return url.format(view_url_parsed)
}
