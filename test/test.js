var rio = require("node-relay-client");
var it  = require("../deps/iterators.js/src/iterators");

var relay = new rio.RelayIO("73B44EF3-CED8-400D-AB73-ED9397E4B718");

function insert(a,b) { relay.insert ("1C471305-B172-4DDC-993D-893E3F4D5010", a, b) };

insert('{"test":"foo1"}', function() {});
insert('{"test":"foo2"}', function() {});
insert('{"test":"foo3"}', function() {});


it.eachAsync(it.range(0,50), function (i, next){
   insert('{"test":"'+i+'"}', next);
})

function tickTock () {
    process.nextTick(tickTock);
}
tickTock();
