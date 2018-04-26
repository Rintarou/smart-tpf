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

  // Parser declaration
  var SparqlParser = sparqljs.Parser;
  var parser = new SparqlParser();

  // Preparation of query for ZZ
  var query = fs.readFileSync(queryFile, {encoding : 'utf8'});
  var parsedQuery = parser.parse(query);
  var triples = parsedQuery.where[0].triples;

  // Preparation of query for LDF
  //var queryFull = fs.readFileSync(queryFile, {encoding : 'utf8'});
  var parsedQueryFull = parser.parse(query/*queryFull*/);
  var triplesFull = parsedQueryFull.where[0].triples;

  //REMOVE THIS IS FOR TESTING
  //triplesFull = [].push(triplesFull[0]);

  // Initialisation of result set for each
  var verifSetZZ = new Set();
  var verifSetLDF = new Set();

  // Reading ZZ results
  var startLDF = Date.now();
  var ldfRes = new ReorderingGraphPatternIterator(new asynciterator.SingletonIterator({}), triplesFull, {fragmentsClient : ldf_serv});
  ldfRes.on('data', function(r){
    verifSetLDF.add(r);
    //console.log(r);
  });

  // When finished reading LDF results, stop timer and go to ZZ
  ldfRes.on('end', function(){
    var endLDF = Date.now();
    var timerLDF = (endLDF-startLDF);
    // console.log("LDF : " + timerLDF + "ms");

    var queryNumber = queryFile.slice(14,-3);

    // Reading LDF results
    console.time("ZZ Query");
    var startZZ = Date.now();
    if (inArray(naughtyList,queryNumber)) {
      var zzRes = new ReorderingGraphPatternIterator(new asynciterator.SingletonIterator({}), triplesFull, {fragmentsClient : ldf_serv});
      zzRes.on('data', function(r){
        verifSetZZ.add(r);
        //console.log(r);
      });

      zzRes.on('end', function(){
        var endZZ = Date.now();
        var timerZZ = (endZZ-startZZ);
        var soundness = soundnessCheck(verifSetZZ,verifSetLDF);
        var queryNumber = queryFile.slice(14,-3);

        var stream = fs.createWriteStream("throughput.csv", {flags:'a'});
        srvResTime.forEach( function (item,index) {
            stream.write(item + "\n");
        });
        stream.end();
        console.log(queryNumber + "," + timerLDF + "," + timerZZ + "," + soundness + "," + nbZZCalls);
      });
    }
    else {
      var waitingAnswer = 0;
      for (i = 0; i < triples.length; i++) {
        let s0 = encodeURIComponent(triples[i].subject);
        let p0 = encodeURIComponent(triples[i].predicate);
        let o0 = encodeURIComponent(triples[i].object);
        var cardGetUrl = zz_serv + "/cardi" + "?s=" + s0 + "&p=" + p0 + "&o=" + o0;

        waitingAnswer++;
        triples[i].reqCard = request(cardGetUrl, function(error, response, body){
          //var data = JSON.parse(body);
          waitingAnswer--;
          if(!waitingAnswer) {
            var zzRes = starExtractor(triples);
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

              var stream = fs.createWriteStream("throughput.csv", {flags:'a'});
              srvResTime.forEach( function (item,index) {
                stream.write(item + "\n");
              });
              stream.end();
              console.log(queryNumber + "," + timerLDF + "," + timerZZ + "," + soundness + "," + nbZZCalls);
            });
          } else {
            //console.log(waitingAnswer);
          }

        //console.log("card: ", data.value);
      });
    }
    }
  ;})

}

function starExtractor(triples) {

  // generating options to use for the request
  var options = { fragmentsClient : ldf_serv }

  // counting the amount of every single subject,
  // to determine stars
  var nbCountForSubject = {};
  for (i = 0; i< triples.length; i++) {
    if(!nbCountForSubject[triples[i].subject]) {
      nbCountForSubject[triples[i].subject] = 0;
    }
    nbCountForSubject[triples[i].subject]++;
  }

  var subjects = Object.keys(nbCountForSubject);

  //determining the biggest subject count, for biggest star
  var mostCountForASubject = Math.max.apply(Math, subjects.map(function(k) {
    return nbCountForSubject[k];
  }));

  //if there are no stars
  //we return a regular ReorderingGraphPatternIterator
  if(mostCountForASubject == 1) {
    return new ReorderingGraphPatternIterator(new asynciterator.SingletonIterator({}), triples, options);
  }

  if(triples.length == 2) {
    var s1 = triples[0].subject;
    var p1 = triples[0].predicate;
    var o1 = triples[0].object;
    var s2 = triples[1].subject;
    var p2 = triples[1].predicate;
    var o2 = triples[1].object;

    //effectively calling the StarIterator constructor
    var res = evalStar(s1,p1,o1,s2,p2,o2);

    //making new ReorderingGraphPatternIterator passing StarIterator as parent
    return new ReorderingGraphPatternIterator(res, {}, options);
  }

  for (i = 0; i < triples.length; i++) {
    triples[i].cardinality = JSON.parse(triples[i].reqCard.response.body).value;
    triples[i].reqCard = {};
  }
  triples.sort((a,b)=>a.cardinality - b.cardinality)

  let it = 0;
  let jt = 1;
  let kt = 0;
  //do {
  breakcon = true;
    while(  jt < triples.length && breakcon) {
      if(triples[it].subject == triples[jt].subject) {
        breakcon = false;
      } else {
        jt++;
      }
    }
    if(breakcon) {
      jt = 0;
    }
    // kt = 0;
    // while(kt == it || kt == jt) {
    //   kt++;
    // }
    //it++;
    //jt=it+1;
  //} while( it == 0 && it < triples.length) //triple1.cardinality*triple2.cardinality > triples[kt].cardinality * triples[kt].cardinality

  var triple1 = triples[it];
  var triple2 = triples[jt];

  // console.log("triple1: ", triple1, "\ntriple2: ",triple2);

  var queryLeft = triples.filter(triple => triple !== triple1 && triple !== triple2)
  //console.log("Square : " + queryLeft[0].triples]);
  //console.log("Product : " + (triple1.cardinality*triple2.cardinality));
  if(it != 0 || jt == 0 || triple1.subject != triple2.subject || queryLeft[0].cardinality*queryLeft[0].cardinality < triple1.cardinality*triple2.cardinality) {
    //console.log("@here: no star");
    return new ReorderingGraphPatternIterator(new asynciterator.SingletonIterator({}), triples, options);
  }

  var s1 = triple1.subject;
  var p1 = triple1.predicate;
  var o1 = triple1.object;
  var s2 = triple2.subject;
  var p2 = triple2.predicate;
  var o2 = triple2.object;

  //effectively calling the StarIterator constructor
  var res = evalStar(s1,p1,o1,s2,p2,o2);



  //making new ReorderingGraphPatternIterator passing StarIterator as parent
  var iterator = new ReorderingGraphPatternIterator(res, queryLeft, options);

  //console.log(triples);

  return iterator;
    //console.log(triples);

/*
  //retrieving the mainSubject out from the count
  var mainSubject = (function(){
    for (i = 0; i< subjects.length; i++) {
      if(nbCountForSubject[subjects[i]] == mostCountForASubject){
        return subjects[i];
      }
    }
  })();

  //initialize star as an empty array
  //initialize the array for indexes of elements to remove from the triples
  var star = [];
  var delIndex = [];
  for (i = 0; i< triples.length; i++) {
    if(triples[i].subject == mainSubject) {
      star.push(triples[i]);
      delIndex.push(i);
    }
  }

  //removing triples that are not to be in here anymore
  //TODO having bigger star management
  //and fixing this splic with actual delIndex
  triples.splice(delIndex[1],1);
  triples.splice(delIndex[0],1);

  //assigning elements to be passed to be passed to the evalStar function
  //TODO see above, support for > 2 stars
  var s1 = star[0].subject;
  var p1 = star[0].predicate;
  var o1 = star[0].object;
  var s2 = star[1].subject;
  var p2 = star[1].predicate;
  var o2 = star[1].object;

  //effectively calling the StarIterator constructor
  var res = evalStar(s1,p1,o1,s2,p2,o2);

  var queryLeft = triples;

  //making new ReorderingGraphPatternIterator passing StarIterator as parent
  var iterator = new ReorderingGraphPatternIterator(res, queryLeft, options);

  return iterator;*/
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
  var zz_serv = 'http://127.0.0.1:3000';
  var ldf_serv = new ldf.FragmentsClient('http://localhost:2000/watdiv');
}
else {
  var zz_serv = 'http://127.0.0.1:' + args[1] + '';
  var ldf_serv = new ldf.FragmentsClient('http://localhost:' + args[2] + '/watdiv');
}

execQuery(args[0]);

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
