var sparqljs = require("sparqljs");
var ldfclient = require("ldf-client");
var asynciterator = require("asynciterator");
var http = require("http");
var request = require("request");
var ldf = require('ldf-client');

const ReorderingGraphPatternIterator = require('ldf-client/lib/triple-pattern-fragments/ReorderingGraphPatternIterator.js')
ldf.Logger.setLevel('WARNING')

var zz_serv = 'http://127.0.0.1:5000/star'
var ldf_serv = new ldf.FragmentsClient('http://127.0.0.1:4000/');

 var SparqlParser = sparqljs.Parser;
 var parser = new SparqlParser();



// console.log(parsedQuery);
// console.log('triples: ');
// console.log(parsedQuery.where[0].triples);


class StarIterator extends asynciterator.BufferedIterator {

  constructor(prefixURL){
    super();
    this.prefixURL = prefixURL;
    this.page = 1;
    this.current = prefixURL+this.page;
  }

  _read(count, done) {
    request(this.current, function (error, response, body) {
      var myself = this;
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
           emit(error);
           myself.close();
         }
    })
    done();
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

    console.log(url);

    var star = new StarIterator(url);
    return star;
}

//evalStar("","http://www.w3.org/1999/02/22-rdf-syntax-ns#type","http://db.uwaterloo.ca/~galuc/wsdbm/Role1","","http://schema.org/email","");

// res.on('readable', function(){
//   // Ce que tu veux genre :
//   console.log(res.read());
// })

// var starit = new StarIterator(testUrl);
// for (var i = 0; i < 3; i++) {
//   console.log(starit.read());
// }

function starExtractor(query) {
  var nbCountForSubject = {};
  // console.log("query.where[0].triples: ", query.where[0].triples);
  for (i = 0; i< query.where[0].triples.length; i++) {
    if(!nbCountForSubject[query.where[0].triples[i].subject]) {
      nbCountForSubject[query.where[0].triples[i].subject] = 0;
    }
    nbCountForSubject[query.where[0].triples[i].subject]++;
  }

  var subjects = Object.keys(nbCountForSubject);
  // console.log("subjects: ", subjects);
  // console.log("nbCountForSubject: ", nbCountForSubject);

  var mostCountForASubject = Math.max.apply(Math, subjects.map(function(k) {
    //console.log(nbCountForSubject[k]);
    return nbCountForSubject[k];
  }));

  // console.log("subjects.map(function(k) {return nbCountForSubject[k];}) : ",subjects.map(function(k) {return nbCountForSubject[k];}))
  // console.log(Math.max(subjects.map(function(k) {return nbCountForSubject[k];})));
  // var a = [1,2,1];
  // console.log(Math.max(a));
  // console.log("mostCountForASubject: ", mostCountForASubject);

  // console.log("Object.keys(nbCountForSubject): ",Object.keys(nbCountForSubject));

  var mainSubject = (function(){
    for (i = 0; i< subjects.length; i++) {
      if(nbCountForSubject[subjects[i]] == mostCountForASubject){
        return subjects[i];
      }
    }
  })();

  //console.log("mainSubject: ", mainSubject);

  var star = [];
  var delIndex = [];
  for (i = 0; i< query.where[0].triples.length; i++) {
    // console.log("query.where[0].triples[i].subject: ",query.where[0].triples[i].subject);
    if(query.where[0].triples[i].subject == mainSubject) {
      star.push(query.where[0].triples[i]);
      delIndex.push(i);
    }
  }
  for (var ind in delIndex) {
    query.where[0].triples.splice(ind,1);
  }

  console.log("star: ", star);

  // //console.log("star[0]: ", star[0]);
  // for(var i = 0; i<star.length; i++) {
  //   for(property in star[i]) {
  //     // console.log("property: ", property);
  //     // console.log("star[i][property]: ", star[i][property]);
  //     var regex = /^\?/;
  //     if ((new RegExp(regex)).test(star[i][property])) {
  //       star[i][property] = "";
  //     }
  //   }
  // }

  var s1 = star[0].subject;
  var p1 = star[0].predicate;
  var o1 = star[0].object;
  var s2 = star[1].subject;
  var p2 = star[1].predicate;
  var o2 = star[1].object;

  var res = evalStar(s1,p1,o1,s2,p2,o2);
  var options = {
    fragmentsClient : ldf_serv
  }
  //console.log("query");
  //console.log(query)

  var queryLeft = query.where[0].triples;

  let iterator = new ReorderingGraphPatternIterator(res, queryLeft, options)
  // res.on('readable', function(){
  //   // Ce que tu veux genre :
  //   console.log(res.read());
  // })

  return iterator;
}

//reoarderingraphpatterniterator(iteraotr, query, options(json server etc))

//console.log("testq: ", parser.parse(test_query));
// console.log("parsedQuery: ", parsedQuery.where[0].triples);
var test_query = "SELECT * WHERE {  <http://db.uwaterloo.ca/~galuc/wsdbm/Retailer699> <http://purl.org/goodrelations/offers> ?v0 .  ?v0 <http://purl.org/goodrelations/includes> ?v1 .  ?v0 <http://purl.org/goodrelations/validThrough> ?v3 .  ?v1 <http://schema.org/printPage> ?v4 .  }";
var test_query2 = "SELECT * WHERE {  ?v0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://db.uwaterloo.ca/~galuc/wsdbm/Role1> .  ?v2 <http://schema.org/contactPoint> ?v0 .  ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/gender> ?v3 .  }";

var parsedQuery = parser.parse(test_query2);

var res = starExtractor(parsedQuery);


res.on('data', function (result) { console.log(result); });
