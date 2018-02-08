import itertools
from hdt import *

path = "datasets/dogfood.hdt"
document = HDTDocument(path)

(triplesTP1, cardinalityTP1) = document.search_triples_ids("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://xmlns.com/foaf/0.1/Person")
# print("cardinality of { ?s rdf:type foaf:Person }: %i" % cardinalityTP1)


(triplesTP2, cardinalityTP2) = document.search_triples_ids("", "http://swrc.ontoware.org/ontology#affiliation", "http://data.semanticweb.org/organization/shizuoka-university")
# http://data.semanticweb.org/organization/shizuoka-university
# print("cardinality of { ?s swrc:affiliation dswo:shizuoka-university }: %i" % cardinalityTP2)

vTP1 = next(triplesTP1)
vTP2 = next(triplesTP2)

k1 = vTP1[0]
k2 = vTP2[0]

vals1 = []
vals2 = []

res = []

while triplesTP1.nb_reads < cardinalityTP1 and triplesTP2.nb_reads < cardinalityTP2 :

    if k1 == k2:
        while vTP1[0] == k1:
            vals1.append(vTP1)
            vTP1 = next(triplesTP1)
        k1 = vTP1[0]

        while vTP2[0] == k2:
            vals2.append(vTP2)
            vTP2 = next(triplesTP2)
        k2 = vTP2[0]

        for elem in [x for x in itertools.product(vals1,vals2)] :
            res.append(elem)
        vals1 = []
        vals2 = []

    else :
        if k2 > k1:
            while k2 > k1:
                vTP1 = next(triplesTP1)
                k1 = vTP1[0]
        else :
            while k1 > k2:
                vTP2 = next(triplesTP2)
                k2 = vTP2[0]



resVals = []
for elem in res :
    elemVal1 = document.tripleid_to_string(elem[0][0],elem[0][1],elem[0][2])
    elemVal2 = document.tripleid_to_string(elem[1][0],elem[1][1],elem[1][2])
    resVals.append((elemVal1,elemVal2))

for r in resVals :
    subject = r[0][0]
    print(subject)
