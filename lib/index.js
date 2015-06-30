var url = require('url');
var stream = require('stream');
var request = require('request');
var jsonfilter = require('jsonfilter');
var ndjson = require('ndjson');
var through = require('through2');
var clone = require('clone-deep');

module.exports = function(view_url, opts){

  if (!opts) opts = {};
  if (!opts.preload) opts.preload = false;

  var info_url = toInfoUrl(view_url);
  var update_seq;
  var last_array;


  return function(){
    var readable = new stream.Readable({ objectMode: true });
    var done = false;
    var index = -1;
    var seq;
    var _cloned_last_array;
    readable._read = function () {
      if (done) return;

      var after_seq = function(){
        if (update_seq === seq) {

          if (!_cloned_last_array) _cloned_last_array = clone(last_array);

          if (index < _cloned_last_array.length) {
            readable.push(_cloned_last_array[++index]);
          } else {
            done = true;
            readable.push(null);
          }
        } else {
          last_array = [];
          request(view_url).pipe(jsonfilter('rows.*.value'))
          .pipe(ndjson.parse())
          .pipe(through.obj(function(obj, enc, cb){
            last_array.push(clone(obj));
            cb();
          }, function(cb){
            update_seq = seq;
            _cloned_last_array = clone(last_array);
            readable.push(_cloned_last_array[++index]);
            cb()
          }))
        }

      }

      if (seq && update_seq) return after_seq();
      fetch_update_seq(info_url, function(err, _seq){
        seq = _seq;
        after_seq();
      })




    };
    return readable;
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
