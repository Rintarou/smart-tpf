var sparqljs = require("sparqljs");
var ldfclient = require("ldf-client");
var asynciterator = require("asynciterator");
var http = require("http");
var request = require("request");
var ldf = require('ldf-client');
const EventEmitter = require('events');
const myEE = new EventEmitter();

const ReorderingGraphPatternIterator = require('ldf-client/lib/triple-pattern-fragments/ReorderingGraphPatternIterator.js')
ldf.Logger.setLevel('WARNING')

var zz_serv = 'http://127.0.0.1:5000/star'
var ldf_serv = new ldf.FragmentsClient('http://127.0.0.1:4000/watdiv');

 var SparqlParser = sparqljs.Parser;
 var parser = new SparqlParser();

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

    //console.log(url);

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
  } else {
    console.log("true");
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
  for (var ind in delIndex.reverse()) {
    query.where[0].triples.splice(ind,1);
  }

  var s1 = star[0].subject;
  var p1 = star[0].predicate;
  var o1 = star[0].object;
  var s2 = star[1].subject;
  var p2 = star[1].predicate;
  var o2 = star[1].object;

  var res = evalStar(s1,p1,o1,s2,p2,o2);

  var queryLeft = query.where[0].triples;

  var bs = "SELECT * WHERE {  ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre> <http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre19> .  ?v0 <http://ogp.me/ns#title> ?v1  } LIMIT 100";
  var testingRuben = parser.parse("SELECT * WHERE {  ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre> <http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre19> .  ?v0 <http://ogp.me/ns#title> ?v1 } LIMIT 100");
  testingRuben = testingRuben.where[0].triples;
  let iterator = new ReorderingGraphPatternIterator(res, queryLeft, options);

  return iterator;
}

var test_query = "SELECT * WHERE {  <http://db.uwaterloo.ca/~galuc/wsdbm/Retailer699> <http://purl.org/goodrelations/offers> ?v0 .  ?v0 <http://purl.org/goodrelations/includes> ?v1 .  ?v0 <http://purl.org/goodrelations/validThrough> ?v3 .  ?v1 <http://schema.org/printPage> ?v4 .  }";
var test_query2 = "SELECT * WHERE {  ?v0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://db.uwaterloo.ca/~galuc/wsdbm/Role1> .  ?v2 <http://schema.org/contactPoint> ?v0 .  ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/gender> ?v3 .  }";

var parsedQuery = parser.parse(test_query2);

var res = starExtractor(parsedQuery);


//res.on('data', function (result) { console.log("results: ",result); });
