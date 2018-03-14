var sparqljs = require("sparqljs");
var ldfclient = require("ldf-client");
var asynciterator = require("asynciterator");
var http = require("http");

var test_query = 'PREFIX foaf: <http://xmlns.com/foaf/0.1/> ' + 'SELECT * { ?mickey foaf:name "Mickey Mouse"@en; foaf:knows ?other. }'

 // http://127.0.0.1:5000/star?s1=&p1=http%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23type&o1=http%3A%2F%2Fdb.uwaterloo.ca%2F~galuc%2Fwsdbm%2FRole1&s2=&p2=http%3A%2F%2Fschema.org%2Femail&o2=&page=1

http.get({
  hostname: '127.0.0.1',
  port: 5000,
  path: '/star?s1=&p1=http%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23type&o1=http%3A%2F%2Fdb.uwaterloo.ca%2F~galuc%2Fwsdbm%2FRole1&s2=&p2=http%3A%2F%2Fschema.org%2Femail&o2=&page=1',
}, function(res) {
  res.on('data', function(data) {
    console.log(JSON.parse(data));
  });
});
