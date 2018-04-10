import itertools
import re
from hdt import *
from flask import request, Flask, url_for, jsonify
from flask_api import FlaskAPI, status, exceptions

app = FlaskAPI(__name__)

PATH = "datasets/benchmark.hdt"
DOC = HDTDocument(PATH)
P_SIZE = 100

# Test URL : http://127.0.0.1:5000/star?s1=&p1=http%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23type&o1=http%3A%2F%2Fdb.uwaterloo.ca%2F~galuc%2Fwsdbm%2FRole1&s2=&p2=http%3A%2F%2Fschema.org%2Femail&o2=&page=1

@app.route("/star", methods=['GET'])
def star():
    s1 = request.args.get('s1')
    p1 = request.args.get('p1') # "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    o1 = request.args.get('o1') # "http://db.uwaterloo.ca/~galuc/wsdbm/Role1"
    s2 = request.args.get('s2')
    p2 = request.args.get('p2') # "http://schema.org/email"
    o2 = request.args.get('o2')
    pNb = int(request.args.get('page'))

    tp1 = [s1,p1,o1]
    tp2 = [s2,p2,o2]

    tp1Vars = [None,None,None]
    tp2Vars = [None,None,None]

    for i in range(3):
        if bool(re.search('^\?', tp1[i])):
            tp1Vars[i] = tp1[i]
            tp1[i] = ""
        if bool(re.search('^\?', tp2[i])):
            tp2Vars[i] = tp2[i]
            tp2[i] = ""
    page = paginatedZZ(tp1,tp2,tp1Vars,tp2Vars,(pNb-1)*P_SIZE,P_SIZE)
    nextPage = [x for x in paginatedZZ(tp1,tp2,tp1Vars,tp2Vars,(pNb)*P_SIZE,1)]

    last = not nextPage
    resp = {}
    if last :
        resp ['next'] = None
    else:
        resp ['next'] = pNb + 1
    resp['values'] = [x for x in page]
    return resp

def zigzag(tp1,tp2,tp1Vars,tp2Vars):

    (triplesTP1, cardinalityTP1) = DOC.search_triples_ids("", tp1[1], tp1[2])
    # print("TP1 = { "+ triplesTP1.subject +" "+ triplesTP1.predicate +" "+ triplesTP1.object +" }")
    # print("Cardinality of TP1 : %i\n" % cardinalityTP1)


    (triplesTP2, cardinalityTP2) = DOC.search_triples_ids("", tp2[1], tp2[2])
    # print("TP2 = { "+ triplesTP2.subject +" "+ triplesTP2.predicate +" "+ triplesTP2.object +" }")
    # print("Cardinality of TP2 : %i\n" % cardinalityTP2)

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
        elemVal1 = DOC.tripleid_to_string(elem[0][0],elem[0][1],elem[0][2])
        elemVal2 = DOC.tripleid_to_string(elem[1][0],elem[1][1],elem[1][2])
        resVals.append((elemVal1,elemVal2))

    for val in resVals:

        select = {}
        for i in range(3):
            if tp1Vars[i] :
                select.update({tp1Vars[i] : val[0][i]})
            if tp2Vars[i] :
                select.update({tp2Vars[i] : val[1][i]})
        yield select


def paginatedZZ(tp1,tp2,tp1Vars,tp2Vars,offset,pgSize) :
    result = zigzag(tp1,tp2,tp1Vars,tp2Vars)
    page = itertools.islice(result,offset,(offset+pgSize))
    return page

if __name__ == "__main__":
    app.run(debug=True)
