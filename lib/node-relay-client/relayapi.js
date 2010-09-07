function prop (x) { return function () { return x; }; }
function require(x) {
    if (x == undefined) {
        throw "required field is missing";
    } else {
        return x;
    }
};
var makeRelayApi = function (api) {

    // relay.io API

    // This api defines the common language spoken accross all
    // relay.io components.  It is a work in progress.

    // The general idea is that you can use the containers below
    // to confirm data is in its proper state before using it.

    // all communication is made up of messages and containers that
    // hold messages.

    // There are three kinds of messages, Request, Responses and Errors
    // There are two kinds of contains, Request and Responses

    api.SUCCESS = 0;
    api.FAILURE = 1;

    // Messages
    //
    // Message must have the following methods:
    //   getType ()
    //   serialize ()
    //   read (json_data)

    // Response Messages

    function FeedInfo (feedId,insertKey) {

        this.getType = prop("FeedInfo");

        this.serialize = prop({ "feedId" : feedId,
                                "insertKey" : insertKey });

        this.read = function (data) {
            return new FeedInfo (data.feedId,data.insertKey);
        };

        this.getInsertKey = prop(insertKey);
        this.getFeedId    = prop(feedId);

    };

    function CommandExitStatus (succeed) {

        this.getType   = prop("CommandExitStatus");
        this.serialize = prop({ "exitStatus" : (succeed) ? api.SUCCESS : api.FAILURE } );

        this.read = function (data) {
            return new CommandExitStatus (data.exitStatus == api.SUCCESS);
        };

        this.isSuccess = prop(succeed);

    };

    function WorkerUri (uri) {
        this.getType   = prop("WorkerUri");
        this.serialize = prop({ "workerUri" : uri });
        this.read = function (data) {
            return new WorkerUri (data.workerUri);
        };
        this.getWorkerUri = prop(uri);
    };

    function DataPacket (data){
        this.getType   = prop("DataPacket");
        this.serialize = prop ({ "data" : data });

        this.read = function (data) {
            return new DataPacket(data.data);
        };

        this.getData = prop(data);
    };

    function NullMessage () {
        this.getType   = prop("NullMessage");
        this.serialize = prop({});
        this.read      = function () { return (new NullMessage()); };
    };

    // Request Messages

    function GetWorker (feedId) {
        this.getType = prop("GetWorker");
        this.serialize = prop({ "feedId" : feedId });
        this.read = function (data) { return (new GetWorker(require(data.feedId))); };
        this.getFeedId = prop(feedId);
    };

    function ReportWorkerFailure (feedId) {
        this.getType = prop("ReportWorkerFailure");
        this.serialize = prop({"feedId" : feedId });
        this.read = function (data) { return (new ReportWorkerFailure(data.feedId)); };
        this.getFeedId = prop(feedId);
    };

    function GetUpdate (feedId, offset, fastForward) {
        this.getType   = prop("GetUpdate");
        this.serialize = function () {
            var ob = { "feedId" : feedId, "offset" : offset };
            if (fastForward) {
                ob['fastForward'] = fastForward;
            }
            return ob;
        };

        this.read = function (data) {
            return (new GetUpdate (require(data.feedId),
                                   require(data.offset),
                                   (data.fastForward ? data.fastForward : false)));
        };

        this.getFeedId = prop(feedId);
        this.getOffset = prop(offset);
        this.getFastForward = prop(fastForward);
    };

    function Insert (feedId, insertKey, insertData) {
        this.getType   = prop("Insert");
        this.serialize = prop({ "feedId" : feedId, "insertKey" : insertKey, "insertData" : insertData });

        this.read = function (data) {
            return (new Insert (data.feedId, data.insertKey, data.insertData));
        };

        this.getFeedId     = prop(feedId);
        this.getInsertKey  = prop(insertKey);
        this.getInsertData = prop(insertData);
    };

    // Error Messages

    function Error (name,message) {
        this.getType = function () { return name; };
        this.serialize = function () {
            return { "errorName" : name, "errorMessage": message };
        };
        this.read = function (data) {
            return new Error(data.errorName,data.errorMessage);
        };
        this.getName = function () { return name; };
        this.getMessage = function () { return message; };
    };

    function makeFeedNotFoundError () {
        return new Error("feedNotFoundError","");
    };

    function makeNullError () {
        return new Error ("nullError","");
    };

    function makeBadRequestError () {
        return new Error ("badRequest","Malformed request.");
    };

    // Message Wrappers

    function Response (status,err,body){

        this.serialize = function (){
            var sObj = { "status": status };

            if (err){
                sObj.error = err.serialize();
            }

            if (status == api.SUCCESS){
                sObj.type = body.getType();
                sObj.body = body.serialize();
            }

            return sObj;

        };

        this.read = function (data) {
            // magically convert a json object into
            // a message type...

            return new Response(data.status,
                                (data.error) ? (new Error).read(data.error) : makeNullError(),
                                (data.type) ? (new api[data.type]).read(data.body) : undefined);

        };

        this.isSuccess = function () { return (status == api.SUCCESS); };
        this.isFailure = function () { return (status == api.FAILURE); };
        this.getBody   = function () { return body; };
        this.getError  = function () { return err; };

    };

    success = function (packet,err) {
        return new Response (api.SUCCESS, err, packet);
    };

    failure = function (err) {
        return new Response (api.FAILURE, err, new NullMessage());
    };

    readResponse = function (data) {
        return (new Response).read(data);
    };

    function Request (request) {
        this.getType = function () { return request.getType(); };
        this.serialize = function () {
            return { type : this.getType (), body : request.serialize() };
        };
        this.toJson = function () {
            return JSON.stringify(this.serialize());
        };
        this.read = function (data) {
            if (api[data.type] != undefined){
                try {
                    return (new Request((new api[data.type]).read(data.body)));
                } catch (err) {
                    return undefined;
                }
            }else {
                return undefined;
            }
        };
        this.getBody = function () { return request; };
    };

    function makeRequest (request) { return (new Request(request)); }
    function readRequest (data) {
        return (new Request).read(data);
    };

    api.FeedInfo = FeedInfo;
    api.CommandExitStatus = CommandExitStatus;
    api.WorkerUri = WorkerUri;
    api.DataPacket = DataPacket;
    api.NullMessage = NullMessage;
    api.Error = Error;

    api.ReportWorkerFailure = ReportWorkerFailure;
    api.GetWorker = GetWorker;
    api.GetUpdate = GetUpdate;
    api.Insert = Insert;

    api.Request = Request;
    api.makeRequest = makeRequest;

    api.makeFeedNotFoundError = makeFeedNotFoundError;
    api.makeBadRequestError = makeBadRequestError;

    api.success = success;
    api.failure = failure;

    api.readResponse = readResponse;
    api.readRequest = readRequest;

    return api;

};


makeRelayApi(exports);

