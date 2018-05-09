var sparqljs = require("sparqljs");
var ldfclient = require("ldf-client");
var asynciterator = require("asynciterator");
var http = require("http");
var readline = require('readline');
var ldf = require('ldf-client');
var request = require("request");
var fs = require('fs');
var inArray = require('in-array');


function execQuery(queryFile) {

  var SparqlParser = sparqljs.Parser;
  var parser = new SparqlParser();

  var query = fs.readFileSync(queryFile, {encoding : 'utf8'});
  var parsedQuery = parser.parse(query);

  console.log(JSON.stringify(parsedQuery.where[0].triples));
}

var args = process.argv.slice(2);

execQuery(args[0]);
