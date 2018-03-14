
var SparqlParser = require('sparqljs').Parser;

var Query = function(raw) {

  var parser = new SparqlParser();

  this.raw = parser.parse(raw);
  this.subQueries = {};
  this.results = {};

  this.optimize = function () {
    //TODO: actually slice the query in subqueries and optimize
  };

  this.send = function () {
    if(this.subQueries == {}) {
      throw new Error("query needs to be subqueried and optimized");
    }
    //using angular?
    for (var subquery in this.subQueries) {
      var stop = false;
      this.results[subquery] =  {};
      var slice = 0;
      while(!stop) {
        $http.post("client.serverURL //TODO:bidonvalue", subquery, slice++).then(function successCallback(response) {
          if(response.data.length){
            this.results[subquery].addAll(response.data);
          } else {
            stop = true;
          }
        }, function errorCallback() {
          console.log("no data was retrieved");
        });
      }
    }
  }
};
