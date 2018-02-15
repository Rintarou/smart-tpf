import itertools
from hdt import *

path = "datasets/dogfood.hdt"
document = HDTDocument(path)

(triplesTP1, cardinalityTP1) = document.search_triples_ids("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://xmlns.com/foaf/0.1/Person")
print("TP1 = { "+ triplesTP1.subject +" "+ triplesTP1.predicate +" "+ triplesTP1.object +" }")
print("Cardinality of TP1 : %i\n" % cardinalityTP1)


(triplesTP2, cardinalityTP2) = document.search_triples_ids("", "http://swrc.ontoware.org/ontology#affiliation", "http://data.semanticweb.org/organization/shizuoka-university")
# http://data.semanticweb.org/organization/shizuoka-university
print("TP2 = { "+ triplesTP2.subject +" "+ triplesTP2.predicate +" "+ triplesTP2.object +" }")
print("Cardinality of TP2 : %i\n" % cardinalityTP2)

vTP1 = next(triplesTP1)
vTP2 = next(triplesTP2)

vals1 = []
vals2 = []

res = []

while triplesTP1.has_next() and triplesTP2.has_next() :

    if vTP1[0] == vTP2[0]:
        k = vTP1[0]
        end = False
        while vTP1[0] == k and not end:
            vals1.append(vTP1)
            if triplesTP1.has_next():
                vTP1 = next(triplesTP1)
            else :
                end = True

        end = False
        while vTP2[0] == k and not end:
            vals2.append(vTP2)
            if triplesTP2.has_next():
                vTP2 = next(triplesTP2)
            else :
                end = True

        for elem in [x for x in itertools.product(vals1,vals2)] :
            res.append(elem)
        vals1 = []
        vals2 = []

    else :
        while vTP2[0] > vTP1[0] and triplesTP1.has_next():
                vTP1 = next(triplesTP1)

        while vTP1[0] > vTP2[0] and triplesTP2.has_next():
                vTP2 = next(triplesTP2)

if not triplesTP1.has_next():
    while vTP1[0] > vTP2[0] and triplesTP2.has_next():
            vTP2 = next(triplesTP2)
    if vTP1[0] == vTP2[0] :
        k = vTP1[0]
        vals1.append(vTP1)
        end = False
        while vTP2[0] == k and not end:
            vals2.append(vTP2)
            if triplesTP2.has_next():
                vTP2 = next(triplesTP2)
            else :
                end = True
        for elem in [x for x in itertools.product(vals1,vals2)] :
                res.append(elem)


if not triplesTP2.has_next():
    while vTP2[0] > vTP1[0] and triplesTP1.has_next():
            vTP1 = next(triplesTP1)
    if vTP1[0] == vTP2[0] :
        k = vTP1[0]
        vals2.append(vTP2)
        end = False
        while vTP1[0] == k and not end:
            vals1.append(vTP1)
            if triplesTP1.has_next():
                vTP1 = next(triplesTP1)
            else :
                end = True
        for elem in [x for x in itertools.product(vals1,vals2)] :
                res.append(elem)


resVals = []
for elem in res :
    elemVal1 = document.tripleid_to_string(elem[0][0],elem[0][1],elem[0][2])
    elemVal2 = document.tripleid_to_string(elem[1][0],elem[1][1],elem[1][2])
    resVals.append((elemVal1,elemVal2))

print("Results : ")
for r in resVals :
    subject = r[0][0]
    print(subject)
