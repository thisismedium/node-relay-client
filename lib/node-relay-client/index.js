var api  = require("./relayapi");
var conf = require("./config");

var httpRequest = require('request');

var transports = {"http" : RelayHttpTransport};

function RelayHttpTransport (domain) {
    this.send = function (request, next) {
        var toSend = escape(JSON.stringify(request.serialize()));
        httpRequest({ uri: "http://" + domain + "/api?request=" + toSend },
                function(error, resp, body) {
                    next(api.readResponse(JSON.parse(body)));
                });
    }
};

exports.RelayIO = RelayIO;
function RelayIO (feedId, insertKey) {

    var workerDomain  = undefined;
    var gettingWorker = false;    
    var queue         = [];       

    function getWorker (reportFail, next) {
        if (!reportFail && workerDomain) {
            next(workerDomain);
        } else if (!reportFail && gettingWorker) {
          queue.push(next);
        } else {
            var workerRequest;
            queue.push(next);
            
            gettingWorker = true;
            workerDomain  = undefined;

            if (reportFail) 
                workerRequest = api.makeRequest(new api.ReportWorkerFailure(feedId));
            else
                workerRequest = api.makeRequest(new api.GetWorker(feedId));
            
            var trans = new transports[conf.TRANSPORT_TYPE](conf.DISPATCHER_HTTP_URI);
            trans.send(workerRequest, function (mesg) {
                if(mesg.isSuccess()){
                  getworker    = false;
                  workerDomain = mesg.getBody().getWorkerUri();

                  while (queue.length > 0) {
                    queue.pop()(mesg.getBody().getWorkerUri());
                  }
                  
                } else {
                    // handle the error
                }
            });
        }
    };

    function doInsert(insertKey, data, next) {
        var insertReq = api.makeRequest(new api.Insert(feedId, insertKey, data));
        var trans     = new transports[conf.TRANSPORT_TYPE](workerDomain);
        trans.send(insertReq, function (mesg) {
            if (mesg.isFailure()) {
                getWorker(true, function(domain) {
                    doInsert(insertKey, data, next);
                });
            } else {
                next (mesg);
            } 
        });
    }

    this.insert = function (insertKey, data, next) {
        if (arguments.length < 2) {
            throw (new TypeError ("You must provide an insert key and an object."));
        } else {
            getWorker(false, function (domain) {
                doInsert(insertKey, data, next);
            })
        }
    };
}
