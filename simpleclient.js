var sparqljs = require("sparqljs");
var ldfclient = require("ldf-client");
var asynciterator = require("asynciterator");
var http = require("http");

var test_query = 'PREFIX foaf: <http://xmlns.com/foaf/0.1/> ' + 'SELECT * { ?mickey foaf:name "Mickey Mouse"@en; foaf:knows ?other. }'

var testUrl =  'http://127.0.0.1:5000/star?s1=&p1=http%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23type&o1=http%3A%2F%2Fdb.uwaterloo.ca%2F~galuc%2Fwsdbm%2FRole1&s2=&p2=http%3A%2F%2Fschema.org%2Femail&o2=&page='

 var SparqlParser = sparqljs.Parser;
 var parser = new SparqlParser();
 var parsedQuery = parser.parse(test_query);

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
    console.log('in');
    http.get(this.current, function(err, body){
      if(err) {
        this.emit("error",err)
        this.close();
      }
      else {
        var data = JSON.parse(body);
        console.log("\n\nbody: ");
        console.log(body);
        for (b in data.values) {
          this._push(b);
        }
        if(data.next == 'null') {
          this.close();
        }
        else {
          this.page = data.next;
          this.current = prefixURL+this.page;
        }
      }
    });
    done();
  }
}

var starit = new StarIterator(testUrl);
for (var i = 0; i < 3; i++) {
  console.log(starit.read());
}

const getKey = function(obj,val) {
  Object.keys(obj).find(function(key) { obj[key] === val; });
}

function starExtractor(query) {
  var tmp = {};
  for (triple in query.where[0].triples) {
    if(!tmp[triple.subject]) {
      tmp[triple.subject] = 0;
    }
    tmp[triple.subject]++;
  }

  var mainSubject = getKey(tmp, Math.max(Object.keys(tmp).map(function(k,v) {
    return tmp[v];
  })));



  tmp = [];
  for (triple in query.where[0].triples) {
    if(triple.subject == mainSubject) {
      tmp.push(triple);
    }
  }
  return tmp;
}

//console.log(starExtractor(parsedQuery));

http.get({
  hostname: '127.0.0.1',
  port: 5000,
  path: '/star?s1=&p1=http%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23type&o1=http%3A%2F%2Fdb.uwaterloo.ca%2F~galuc%2Fwsdbm%2FRole1&s2=&p2=http%3A%2F%2Fschema.org%2Femail&o2=&page=1',
}, function(res) {
  res.on('data', function(data) {
    //console.log(JSON.parse(data));
  });
});
