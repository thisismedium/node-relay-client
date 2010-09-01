var rio = require("../src/relay.io");
var it  = require("../src/lib/iterators");

var relay = new rio.RelayIO("73B44EF3-CED8-400D-AB73-ED9397E4B718");

it.eachAsync(it.range(0,50), function (i, next){
    relay.insert("1C471305-B172-4DDC-993D-893E3F4D5010", '{"test":"'+i+'"}', next);
})


function tickTock () {
    process.nextTick(tickTock);
}
tickTock();