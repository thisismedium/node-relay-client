var url = require('url');
var api  = require("./relayapi");
var conf = require("./config");


/// --- RelayIO

exports.RelayIO = RelayIO;
function RelayIO (feedId, insertKey) {

  var dispatcher    = transport(conf.DISPATCHER_HTTP_URI);
  var worker        = undefined;
  var gettingWorker = false;
  var queue         = [];

  this.insert = function (insertKey, data, next) {
    if (arguments.length < 2)
      throw (new TypeError ("You must provide an insert key and an object."));
    else
      getWorker(false, function (err, uri) {
        err ? next(err) : _insert(insertKey, data, next);
      });
  };

  function getWorker (reportFail, next) {
    if (!reportFail && worker)
      next(null);
    else if (!reportFail && gettingWorker)
      queue.push(next);
    else {
      var workerRequest;
      queue.push(next);

      gettingWorker = true;
      worker = undefined;

      if (reportFail)
        workerRequest = new api.ReportWorkerFailure(feedId);
      else
        workerRequest = new api.GetWorker(feedId);

      dispatcher.send(workerRequest, function (error, mesg) {
        if (error)
          next(error);
        else {
          gettingWorker = false;
          worker = transport('http://' + mesg.getWorkerUri() + '/api');
          while (queue.length > 0)
            queue.pop()();
        }
      });
    }
  };

  function _insert(insertKey, data, next) {
    var insertReq = new api.Insert(feedId, insertKey, data);
    worker.send(insertReq, function (error, mesg) {
      if (error)
        next(error);
      else if (mesg.isFailure()) {
        getWorker(true, function() {
          _insert(insertKey, data, next);
        });
      }
    });
  }
}


/// --- HTTP Transport

function RelayHttpTransport(uri) {
  var qs = require('querystring'),
      http = require('http');

  uri = parseUri(uri);
  var port = uri.port || (uri.protocol == 'https:' ? 443 : 80),
      path = uri.pathname + (uri.search || '');

  this.send = function send(request, next) {
    try {
      var data = dumpRequest(request),
          client = http.createClient(port, uri.hostname),
          req = client.request('POST', path, {
            'content-type': 'application/x-www-form-urlencoded',
            'content-length': data.length,
            'host' : uri.hostname
          });

      console.dir(request.serialize());
      req.end(data);

      req.on('response', function(resp) {
        capture(resp, 'utf-8', function(err, data) {
          if (err)
            next(err);
          else if (resp.statusCode > 299)
            next(new Error("Bad reply: " + resp.statusCode), data, resp);
          else
            unboxResponse(data, next);
        });
      });
    } catch (err) {
      next(err);
    }
  };

  function dumpRequest(obj) {
    var req = api.makeRequest(obj);
    return qs.stringify({ request: JSON.stringify(req.serialize()) });
  }

  function unboxResponse(body, next) {
    loadResponse(body, function(err, reply) {
      console.dir(reply.serialize());
      if (err)
        next(err);
      else if (reply.isSuccess())
        next(null, reply.getBody());
      else {
        var fail = reply.getError();
        next(new Error(fail.getName() + ': ' + fail.getMessage()), reply);
      }
    });
  }

  function loadResponse(data, next) {
    var err = null, val;

    try {
      val = api.readResponse(JSON.parse(data));
    } catch (x) {
      err = x;
    }

    next(err, val);
  }
}

function capture(stream, encoding, next) {
  var buffer = '';

  stream.addListener('data', function(chunk) {
    buffer += chunk.toString(encoding);
  });

  stream.addListener('end', function() {
    next(null, buffer);
  });

  stream.addListener('error', next);

  return stream;
}


/// --- Transports

var TRANSPORTS = { "http:" : RelayHttpTransport };

var transport = (function() {
  var memo = {};

  function make(uri) {
    if (!TRANSPORTS.hasOwnProperty(uri.protocol))
      throw new Error('Unrecognized transport protocol: ' + uri.protocol);
    return new TRANSPORTS[uri.protocol](uri);
  }

  return function transport(uri) {
    var trans = memo[uri];
    if (trans === undefined)
      trans = memo[uri] = make(parseUri(uri));
    return trans;
  };
})();


/// --- Aux

function parseUri(obj) {
  return (typeof obj == 'string') ? url.parse(obj) : obj;
}