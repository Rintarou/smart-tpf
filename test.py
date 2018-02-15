import simplejoin
from tabulate import tabulate

tp1 = ("http://www.w3.org/1999/02/22-rdf-syntax-ns#type","http://xmlns.com/foaf/0.1/Person")
tp2 = ("http://swrc.ontoware.org/ontology#affiliation", "http://data.semanticweb.org/organization/university-of-tokyo")

result = simplejoin.zigzag(tp1,tp2)
print(tabulate(result))
