var sparqljs = require("sparqljs");
var ldfclient = require("ldf-client");
var asynciterator = require("asynciterator");
var http = require("http");
var readline = require('readline');
var ldf = require('ldf-client');
var request = require("request");
var fs = require('fs');
var inArray = require('in-array');

//var StarIterator = require("./stariterator.js")

const ReorderingGraphPatternIterator = require('ldf-client/lib/triple-pattern-fragments/ReorderingGraphPatternIterator.js');
const TriplePatternIterator = require('ldf-client/lib/triple-pattern-fragments/TriplePatternIterator.js');
const EventEmitter = require('events');
const myEE = new EventEmitter();
ldf.Logger.setLevel('WARNING')

var zzTimeout = 1000;
var cardinalityLimit = 20;
var srvResTime = [];
var nbZZCalls = 0;
var naughtyList = ["10227"]

function execQuery(queryFile) {

  var fs = require('fs');
  var triples = JSON.parse(fs.readFileSync(queryFile, 'utf8'));

  // Initialisation of result set for each
   var verifSetZZ = new Set();
   var verifSetLDF = new Set();

  var queryNumber = queryFile.slice(22,-3);

  // Reading LDF results
  var startZZ = Date.now();
  var waitingAnswer = 0;

  var zzRes = starExtractor(triples);
  zzRes.on('data', function(r){
    verifSetZZ.add(r);
    //console.log(r);
  });

        // When finished reading ZZ results, stop timer and test soundness
  zzRes.on('end', function(){
    var endZZ = Date.now();
    var timerZZ = (endZZ-startZZ);

    var queryNumber = queryFile.slice(22,-5);

    // var stream = fs.createWriteStream("throughput.csv", {flags:'a'});
    // srvResTime.forEach( function (item,index) {
    //   stream.write(item + "\n");
    // });
    // stream.end();
    console.log(queryNumber + "," + timerZZ);
  });

}

function starExtractor(triples) {

  var options = { fragmentsClient : ldf_serv }

  if (triples.star) {
    var s1 = triples.star[0].subject;
    var p1 = triples.star[0].predicate;
    var o1 = triples.star[0].object;
    var s2 = triples.star[1].subject;
    var p2 = triples.star[1].predicate;
    var o2 = triples.star[1].object;

    //effectively calling the StarIterator constructor
    var res = evalStar(s1,p1,o1,s2,p2,o2);

  } else {
    res = new asynciterator.SingletonIterator({});
  }

  return new ReorderingGraphPatternIterator(res, triples.remain, options);
}

function evalStar(s1,p1,o1,s2,p2,o2) {

    URIs1 = encodeURIComponent(s1);
    URIp1 = encodeURIComponent(p1);
    URIo1 = encodeURIComponent(o1);
    URIs2 = encodeURIComponent(s2);
    URIp2 = encodeURIComponent(p2);
    URIo2 = encodeURIComponent(o2);

    var url = zz_serv +"/star" + "?s1=" + URIs1 + "&p1=" + URIp1 + "&o1=" + URIo1 + "&s2=" + URIs2 + "&p2=" + URIp2 + "&o2=" + URIo2 + "&page=";
    var star = new StarIterator(url);
    return star;
}

var args = process.argv.slice(2);
if (args.length == 1) {
  var zz_serv = 'http://127.0.0.1:3000';
  var ldf_serv = new ldf.FragmentsClient('http://localhost:2000/watdiv');
}
else {
  var zz_serv = 'http://127.0.0.1:' + args[1] + '';
  var ldf_serv = new ldf.FragmentsClient('http://localhost:' + args[2] + '/watdiv');
}

class StarIterator extends asynciterator.BufferedIterator {

  constructor(prefixURL){
    super();
    this.prefixURL = prefixURL;
    this.page = 1;
    this.current = prefixURL+this.page;
  }

  _read(count, done) {
    var myself = this;
    var srvTimerSt = Date.now();
    var req = request(this.current, function (error, response, body) {
        if (!error && response.statusCode == 200) {
          var srvTimerEnd = Date.now();
          srvResTime.push(srvTimerEnd - srvTimerSt);
          nbZZCalls++;
          var data = JSON.parse(body);
          for (var b of data.values) {
            myself._push(b);
          }
          if(!data.next) {
            myself.close();
          }
          else {
            myself.page = data.next;
            myself.current = myself.prefixURL+myself.page;
          }
         }
         else {
           myEE.emit(error);
           myself.close();
         }
      done();
    })
  }
}

execQuery(args[0]);
