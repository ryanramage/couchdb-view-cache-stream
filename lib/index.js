var url = require('url');

var request = require('request');
var jsonfilter = require('jsonfilter');
var ndjson = require('ndjson');
var through = require('through2');
var levelup = require('levelup');
var devnull = require('dev-null');

module.exports = function(view_url, opts){

  if (!opts) opts = {};
  if (!opts.preload) opts.preload = false;

  var info_url = toInfoUrl(view_url);

  var update_seq;
  var current_view;
  var loading = false;

  var load = function(done){
    if (!done) done = function(){}; // a dumb callback
    if (loading) return done();

    loading = true;
    fetch_update_seq(info_url, function(err, seq){

      var clean_up = false;

      if (err) { loading = false; return done(err) }
      if (update_seq && update_seq === seq) { loading = false; return done() }
      if (update_seq) clean_up = true;

      console.log('updating view', view_url,' from', update_seq, 'to', seq)
      // TODO we should move this to a sublevel, but it was not working for memdown
      var db_path = opts.db_path || '/' + 'vcs/' + Date.now() + '/' + update_seq;
      var tempdb = levelup(db_path, {
        db: opts.db_type || require('memdown'),
        keyEncoding: 'json',
        valueEncoding: 'json'
      })

      request(view_url).pipe(jsonfilter('rows.*'))
        .pipe(ndjson.parse())
        .pipe(through.obj(function(data, enc, cb){
          tempdb.put(data.key, data.value, cb)
        }, function(cb){
          process.nextTick(function(){
            update_seq = seq;
            current_view = tempdb;
            done(null, seq);
          });
          cb()
        }))
        .pipe(devnull())
    })
  }


  load(function(err, seq){
    if(seq) console.log('view loaded. currrently at update_seq', seq)
  })

  return function(){
    process.nextTick(load);

    if (!current_view) {
      var stream = request(view_url)
        .pipe(jsonfilter('rows.*.value'))
        .pipe(ndjson.parse())
      return stream;
    }

    // TODO - danger - on first load current_view will be undefined until the load
    // is complete. Need to think how we want to handle this
    return current_view.createValueStream()
  }
};



function fetch_update_seq(info_url, cb) {
  request(info_url, function(err, resp, body){
    if (err) return cb(err);
    try { return cb(null, JSON.parse(body).view_index.update_seq)  }
    catch(e) { console.log('got an error'); cb(e) }
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
