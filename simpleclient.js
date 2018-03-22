var sparqljs = require("sparqljs");
var ldfclient = require("ldf-client");
var asynciterator = require("asynciterator");
var http = require("http");

var test_query = 'PREFIX foaf: <http://xmlns.com/foaf/0.1/> ' + 'SELECT * { ?mickey foaf:name "Mickey Mouse"@en; foaf:knows ?other. }'
var other_query = 'SELECT * WHERE {  <http://db.uwaterloo.ca/~galuc/wsdbm/Retailer699> <http://purl.org/goodrelations/offers> ?v0 .  ?v0 <http://purl.org/goodrelations/includes> ?v1 .  ?v0 <http://purl.org/goodrelations/validThrough> ?v3 .  ?v1 <http://schema.org/printPage> ?v4 .  }'

var testUrl =  'http://127.0.0.1:5000/star?s1=&p1=http%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23type&o1=http%3A%2F%2Fdb.uwaterloo.ca%2F~galuc%2Fwsdbm%2FRole1&s2=&p2=http%3A%2F%2Fschema.org%2Femail&o2=&page='

var servUrl = 'http://localhost:5000/star'

 var SparqlParser = sparqljs.Parser;
 var parser = new SparqlParser();
 var parsedQuery = parser.parse(other_query);

// console.log(parsedQuery);
// console.log('triples: ');
// console.log(parsedQuery.where[0].triples);


class StarIterator extends asynciterator.AsyncIterator.BufferedIterator {

  constructor(prefixURL){
    super();
    this.prefixURL = prefixURL;
    this.page = 1;
    this.current = prefixURL+this.page;
  }

  _read(count, done) {
    var myself = this;
    http.request(this.current, function (error, response, body) {
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
           console.error(error);
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

    var url = servUrl + "?s1=" + URIs1 + "&p1=" + URIp1 + "&o1=" + URIo1 + "&s2=" + URIs2 + "&p2=" + URIp2 + "&o2=" + URIo2 + "&page=";

    var star = new StarIterator(url);
    return star;
}

var res = evalStar("","http://www.w3.org/1999/02/22-rdf-syntax-ns#type","http://db.uwaterloo.ca/~galuc/wsdbm/Role1","","http://schema.org/email","");

res.on('readable', function(){
  // Ce que tu veux genre :
  console.log(res.read());
})

// var starit = new StarIterator(testUrl);
// for (var i = 0; i < 3; i++) {
//   console.log(starit.read());
// }

const getKey = function(obj,val) {

}

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
  console.log("subjects: ", subjects);
  console.log("nbCountForSubject: ", nbCountForSubject);

  var mostCountForASubject = Math.max.apply(Math, subjects.map(function(k) {
    //console.log(nbCountForSubject[k]);
    return nbCountForSubject[k];
  }));

  // console.log("subjects.map(function(k) {return nbCountForSubject[k];}) : ",subjects.map(function(k) {return nbCountForSubject[k];}))
  // console.log(Math.max(subjects.map(function(k) {return nbCountForSubject[k];})));
  // var a = [1,2,1];
  // console.log(Math.max(a));
  // console.log("mostCountForASubject: ", mostCountForASubject);

  console.log("Object.keys(nbCountForSubject): ",Object.keys(nbCountForSubject));

  var mainSubject = (function(){
    for (i = 0; i< subjects.length; i++) {
      if(nbCountForSubject[subjects[i]] == mostCountForASubject){
        return subjects[i];
      }
    }
  })();

  //console.log("mainSubject: ", mainSubject);

  //TODO:rendre ça propre

  var star = [];
  for (i = 0; i< query.where[0].triples.length; i++) {
    // console.log("query.where[0].triples[i].subject: ",query.where[0].triples[i].subject);
    if(query.where[0].triples[i].subject == mainSubject) {
      star.push(query.where[0].triples[i]);
      query.where[0].triples.splice(i,1);
      i--;
    }
  }

  //console.log("star[0]: ", star[0]);
  for(var i = 0; i<star.length; i++) {
    for(property in star[i]) {
      // console.log("property: ", property);
      // console.log("star[i][property]: ", star[i][property]);
      var regex = /^\?/;
      if ((new RegExp(regex)).test(star[i][property])) {
        star[i][property] = "";
      }
    }
  }

  return star;
}

//console.log("testq: ", parser.parse(test_query));
console.log("parsedQuery: ", parsedQuery.where[0].triples);
console.log("starExtractor parsed: ");
console.log(starExtractor(parsedQuery));

http.get({
  hostname: '127.0.0.1',
  port: 5000,
  path: '/star?s1=&p1=http%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23type&o1=http%3A%2F%2Fdb.uwaterloo.ca%2F~galuc%2Fwsdbm%2FRole1&s2=&p2=http%3A%2F%2Fschema.org%2Femail&o2=&page=1',
}, function(res) {
  res.on('data', function(data) {
    //console.log(JSON.parse(data));
  });
});
