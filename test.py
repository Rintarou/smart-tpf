import simplejoin
import itertools
from tabulate import tabulate

# tp1 = ("http://www.w3.org/1999/02/22-rdf-syntax-ns#type","http://xmlns.com/foaf/0.1/Person")
# tp2 = ("http://swrc.ontoware.org/ontology#affiliation", "")
# tp2 = ("http://swrc.ontoware.org/ontology#affiliation", "http://data.semanticweb.org/organization/university-of-tokyo")

tp1 = ("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://db.uwaterloo.ca/~galuc/wsdbm/Role1")
tp2 = ("http://schema.org/email","")

page = simplejoin.paginatedZZ(tp1,tp2,0,100)

for i in page :
    print(i)
