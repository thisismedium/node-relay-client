/// # Node Relay Client #
//
// This client implements Relay transport layers.  It presents the
// feed API through `RelayIO` methods.

var url = require('url');
var api = require('./relayapi');


/// ## RelayIO ##

exports.RelayIO = RelayIO;

// Wrap up the Relay API for a particular feed.  Public methods
// provide a way to interact with the feed.
//
// + feedId: String identifying the feed.
// + dispatch: String URI.  Defaults to 'http://dispatcher.relay.io/api'.
//
// Returns RelayIO instance.
function RelayIO (feedId, dispatch) {

  var dispatcher    = transport(dispatch || 'http://dispatcher.relay.io/api');
  var worker        = undefined;
  var gettingWorker = false;
  var queue         = [];

  // Send an object to the feed.
  //
  // + insertKey: String private key for the feed.
  // + data: Object that will be passed through JSON.stringify() and sent.
  // + next: Function error/success callback.
  //
  // Returns RelayIO instance.
  this.insert = function (insertKey, data, next) {
    if (arguments.length < 2)
      next(new TypeError ("You must provide an insert key and an object."));
    else
      getWorker(false, function (err, uri) {
        err ? next(err) : _insert(insertKey, data, next);
      });
    return this;
  };

  // Find the worker URI for this feed.  If `reportFail` is true and a
  // worker hasn't been found yet, notify the dispatcher that the
  // worker has failed.  Upon success, update the `worker` state with
  // a new worker transport.
  //
  // + reportFail: Boolean if true notify the dispatcher of failure.
  // + next: Function error/success callback.
  //
  // Returns nothing.
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

  // Send an insert request to this feed.  Handle the case of a
  // failure message by notifying the dispatcher that the worker
  // failed.
  //
  // + insertKey: String private key for this feed.
  // + data: Object to insert
  // + next: Function error/success callback
  //
  // Returns nothing.
  function _insert(insertKey, data, next) {
    var insertReq = new api.Insert(feedId, insertKey, data);
    worker.send(insertReq, function (error, mesg) {
      if (error) {
        if (mesg && mesg.isFailure()) {
          getWorker(true, function() {
            _insert(insertKey, data, next);
          });
        }
        else if (!mesg)
          next(error);
      }
    });
  }
}


/// ## Transports ##

// A transport wraps up a URI, hiding the low-level aspects of a
// particular communication protocol.  A Transport object should
// provide these methods:
//
// + Transport(uri): Create a new instance.
// + .send(obj, next): Send obj, call `next` with error/success.

var transport = (function() {
  var registry = {},
      memo = {};

  // Create a Transport instance for a Relay URI.  Instances are
  // memoized.
  //
  // + uri: String URI of the Relay API.
  //
  // Returns Transport instance.
  function transport(uri) {
    var trans = memo[uri];
    if (trans === undefined)
      trans = memo[uri] = make(parseUri(uri));
    return trans;
  };

  // Register a Transport implementation for a protocol.
  //
  // + protocol: String name of the protocol (e.g. 'http')
  // + method: Function that creates a new Transport object.
  //
  // Returns transport function (for chaining)
  transport.def = function defineTransport(protocol, method) {
    registry[protocol + ':'] = method;
    return this;
  };

  function make(uri) {
    if (!registry.hasOwnProperty(uri.protocol))
      throw new Error('Unrecognized transport protocol: ' + uri.protocol);
    return new registry[uri.protocol](uri);
  }

  return transport;
})();


/// ## HTTP Transport ##

transport.def('http', function RelayHttpTransport(uri) {
  var qs = require('querystring'),
      http = require('http');

  uri = parseUri(uri);
  var port = uri.port || (uri.protocol == 'https:' ? 443 : 80),
      path = uri.pathname + (uri.search || '');

  // Package up the object and make a new request.  When a response is
  // received, check for errors and try to pass the unboxed response
  // to `next`.
  //
  // + obj: RelayAPI instance.
  // + next: Function error/success callback.
  //
  // Returns Transport instance.
  this.send = function send(obj, next) {
    try {
      var data = dumpRequest(obj),
          client = http.createClient(port, uri.hostname),
          req = client.request('POST', path, {
            'content-type': 'application/x-www-form-urlencoded',
            'content-length': data.length,
            'host' : uri.hostname
          });

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
    return this;
  };

  // Wrap the API object in a request, then prepare it for transport
  // by serializing it.
  //
  // + obj: RelayAPI instance
  //
  // Returns String data.
  function dumpRequest(obj) {
    var req = api.makeRequest(obj);
    return qs.stringify({ request: JSON.stringify(req.serialize()) });
  }

  // Take a serialized response and unbox it.  On error, `next` is
  // called with an Error instance and possibly the Response.
  //
  // + body: String response data
  // + next: Function error/success callback
  //
  // Returns nothing.
  function unboxResponse(body, next) {
    loadResponse(body, function(err, reply) {
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

  // Load serialized response into a Response object, passing it to
  // `next`.
  //
  // + data: String serialized response
  // + next: Function error/success callback
  //
  // Returns nothing.
  function loadResponse(data, next) {
    var err = null, val;

    try {
      val = api.readResponse(JSON.parse(data));
    } catch (x) {
      err = x;
    }

    next(err, val);
  }
});


/// --- Aux

// Possibly parse a String URI into a URL object.  If an object is
// given, just return it.
//
// + obj: Object to parse; if it's a String pass it to url.parse()
//
// Returns a URL object.
function parseUri(obj) {
  return (typeof obj == 'string') ? url.parse(obj) : obj;
}

// Read all data from a stream into a buffer.  When the stream ends,
// pass the buffer to `next`.
//
// + stream: Stream object that emits `data` and `end` events.
// + encoding: String encoding type to convert chunks to strings.
// + next: Function error/success callback
//
// Returns the stream.
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
