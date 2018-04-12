var sparqljs = require("sparqljs");
var ldfclient = require("ldf-client");
var asynciterator = require("asynciterator");
var http = require("http");
var readline = require('readline');
var ldf = require('ldf-client');
var request = require("request");
var fs = require('fs');

const ReorderingGraphPatternIterator = require('ldf-client/lib/triple-pattern-fragments/ReorderingGraphPatternIterator.js');
const TriplePatternIterator = require('ldf-client/lib/triple-pattern-fragments/TriplePatternIterator.js');
const EventEmitter = require('events');
const myEE = new EventEmitter();
ldf.Logger.setLevel('WARNING')


class StarIterator extends asynciterator.BufferedIterator {

  constructor(prefixURL){
    super();
    this.prefixURL = prefixURL;
    this.page = 1;
    this.current = prefixURL+this.page;
  }

  _read(count, done) {
    var myself = this;
    request(this.current, function (error, response, body) {
        if (!error && response.statusCode == 200) {
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

function evalStar(s1,p1,o1,s2,p2,o2) {
    URIs1 = encodeURIComponent(s1);
    URIp1 = encodeURIComponent(p1);
    URIo1 = encodeURIComponent(o1);
    URIs2 = encodeURIComponent(s2);
    URIp2 = encodeURIComponent(p2);
    URIo2 = encodeURIComponent(o2);

    var url = zz_serv + "?s1=" + URIs1 + "&p1=" + URIp1 + "&o1=" + URIo1 + "&s2=" + URIs2 + "&p2=" + URIp2 + "&o2=" + URIo2 + "&page=";
    var star = new StarIterator(url);
    return star;
}

function starExtractor(query) {
  var options = {
    fragmentsClient : ldf_serv
  }

  var nbCountForSubject = {};
  for (i = 0; i< query.where[0].triples.length; i++) {
    if(!nbCountForSubject[query.where[0].triples[i].subject]) {
      nbCountForSubject[query.where[0].triples[i].subject] = 0;
    }
    nbCountForSubject[query.where[0].triples[i].subject]++;
  }

  var subjects = Object.keys(nbCountForSubject);

  var mostCountForASubject = Math.max.apply(Math, subjects.map(function(k) {
    return nbCountForSubject[k];
  }));

  if(mostCountForASubject == 1) {
    console.log("false");
    return new ReorderingGraphPatternIterator(new asynciterator.SingletonIterator({}), query.where[0].triples, options);
  }

  var mainSubject = (function(){
    for (i = 0; i< subjects.length; i++) {
      if(nbCountForSubject[subjects[i]] == mostCountForASubject){
        return subjects[i];
      }
    }
  })();

  var star = [];
  var delIndex = [];
  for (i = 0; i< query.where[0].triples.length; i++) {
    if(query.where[0].triples[i].subject == mainSubject) {
      star.push(query.where[0].triples[i]);
      delIndex.push(i);
    }
  }

  query.where[0].triples.splice(delIndex[1],1);
  query.where[0].triples.splice(delIndex[0],1);

  var s1 = star[0].subject;
  var p1 = star[0].predicate;
  var o1 = star[0].object;
  var s2 = star[1].subject;
  var p2 = star[1].predicate;
  var o2 = star[1].object;

  var res = evalStar(s1,p1,o1,s2,p2,o2);

  var queryLeft = query.where[0].triples;

  var iterator = new ReorderingGraphPatternIterator(res, queryLeft, options);

  return iterator;
}

function execQuery(queryFile) {

  // Parser declaration
  var SparqlParser = sparqljs.Parser;
  var parser = new SparqlParser();

  // Preparation of query for ZZ
  var query = fs.readFileSync(queryFile, {encoding : 'utf8'});
  var parsedQuery = parser.parse(query);
  var triples = parsedQuery.where[0].triples;

  // Preparation of query for LDF
  var queryFull = fs.readFileSync(queryFile, {encoding : 'utf8'});
  var parsedQueryFull = parser.parse(queryFull);
  var triplesFull = parsedQueryFull.where[0].triples;

  // Initialisation of result set for each
  var verifSetZZ = new Set();
  var verifSetLDF = new Set();

  // Reading ZZ results
  var startLDF = Date.now();
  var ldfRes = new ReorderingGraphPatternIterator(new asynciterator.SingletonIterator({}), triplesFull, {fragmentsClient : ldf_serv});
  ldfRes.on('data', function(r){
    verifSetLDF.add(r);
    //console.log(r);
  })

  // When finished reading ZZ results, stop timer and go to LDF
  ldfRes.on('end', function(){
    var endLDF = Date.now();
    var timerLDF = (endLDF-startLDF);
    // console.log("LDF : " + timerLDF + "ms");

    // Reading LDF results
    console.time("ZZ Query");
    var startZZ = Date.now();
    var zzRes = starExtractor(parsedQuery);
    zzRes.on('data', function(r){
      verifSetZZ.add(r);
      //console.log(r);
    });

    // When finished reading ZZ results, stop timer and test soundness
    zzRes.on('end', function(){
      var endZZ = Date.now();
      var timerZZ = (endZZ-startZZ);
      var soundness = soundnessCheck(verifSetZZ,verifSetLDF);
      var queryNumber = queryFile.slice(14,-3);
      console.log(queryNumber + "," + timerLDF + "," + timerZZ + "," + soundness)
    });

  ;})


}

function soundnessCheck(setZZ,setLDF) {
  var sound = !(setZZ.size != setLDF.size && JSON.stringify(Array.from(setZZ)) != JSON.stringify(Array.from(setLDF)));
  /*
  console.log("Sound : " + sound);
  console.log("LDF size : " + setLDF.size);
  console.log("ZZ size : " + setZZ.size);
  */
  return sound;
}

var args = process.argv.slice(2);
if (args.length == 1) {
  var zz_serv = 'http://127.0.0.1:3000/star';
  var ldf_serv = new ldf.FragmentsClient('http://localhost:2000/watdiv');
}
else {
  var zz_serv = 'http://127.0.0.1:' + args[1] + '/star';
  var ldf_serv = new ldf.FragmentsClient('http://localhost:' + args[2] + '/watdiv');
}

execQuery(args[0]);
