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

    var workerDomain = undefined;

    function getWorker (reportFail, next) {
        if (!reportFail && workerDomain) {
            next(workerDomain);
        } else {
            var workerRequest;
            
            if (reportFail) 
                workerRequest = api.makeRequest(new api.ReportWorkerFailure(feedId));
            else
                workerRequest = api.makeRequest(new api.GetWorker(feedId));
            
            var trans = new transports[conf.TRANSPORT_TYPE](conf.DISPATCHER_HTTP_URI);
            trans.send(workerRequest, function (mesg) {
                if(mesg.isSuccess()){
                    next(mesg.getBody().getWorkerUri());
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
                queue.push([insertKey, data]);
                getWorker(true, function(domain) {
                    workerDomain = domain
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
                workerDomain = domain;
                doInsert(insertKey, data, next);
            })
        }
    };
}