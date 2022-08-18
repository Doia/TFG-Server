from email import header
from json import dumps
from select import select
from flask import Flask, jsonify, request, Response
from flask_mysqldb import MySQL
from flask_cors import CORS, cross_origin
from requests import head
from algoritmo import algoritmo
from movies import Movie 
import sys
import constants

import csv
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, String

hostName = "localhost"
serverPort = 8081
#totalcont = -1

api_cors_config = {
    "origins" : ["*"],
    "methods" : ["GET", "POST", "PUT", "OPTIONS"],
    "allow_headers": ["Authorization", "Content-Type"]
}

app = Flask(__name__)
cors = CORS(app, resources={r"*": {"origins": "*"}})

# Required
app.config["MYSQL_HOST"] = constants.MYSQL_HOST
app.config["MYSQL_USER"] = constants.MYSQL_USER
app.config["MYSQL_PASSWORD"] = constants.MYSQL_PASSWORD
app.config["MYSQL_DB"] = constants.MYSQL_DB

mysql = MySQL(app)

@app.before_first_request
def getTotalCont():
    global totalcont
    totalcont = queryNumber("select count(*) from movies", mysql.connection)

@cross_origin()
@app.route('/prueba/<int:id>/')
def prueba(id):
    res =  "prueba: " + str(id);
    data = { "content": res, "state" : "OK", "cont" : len(res), "totalcont" : totalcont}
    return (data, 200)

#Rutas
@cross_origin
@app.route('/api/movie/', methods=['GET'])
def getAllMoviesView():
  
    res = queryMovie("Select * from movies LIMIT 5", mysql.connection)
    if res == None:
        data = {'Error': "Error: No se ha podido realizar la consulta a la DB."}
        state = 400
    else:
        data = { "content": res, "state" : "OK", "cont" : len(res), "totalcont" : totalcont}
        state = 200
    return (data, state)

@cross_origin()
@app.route('/api/movie/<int:id>/', methods=['GET'])
def getMoviesById(id):
    res = queryMovie("Select * from movies where film_id = {idt}".format(idt = id), mysql.connection)
    if res == None:
        data = {'Error': "Error: No se ha podido realizar la consulta a la DB."}
        state = 400
    elif res == []:
        data = { "content": res, "state" : "OK", "cont" : len(res), "totalcont" : totalcont}
        state = 200
    else:
        array = list(map(int, res[0]['ids_similar_films'][1 : len(res[0]['ids_similar_films'])- 1].split(',')))
        res_extend = queryMovie("Select * from movies where film_id in" + res[0]['ids_similar_films'].replace('[','(').replace(']', ')'), mysql.connection)

        indexes = []
        for peli in res_extend:
            indexes.append(array.index(peli['film_id']))

        aux = [None] * len(indexes)
        for i, indice in enumerate(indexes):
            aux[indice] = res_extend[i]

        res.extend(aux)

        data = { "content": res, "state" : "OK", "cont" : len(res), "totalcont" : totalcont}
        state = 200
    return (data, state)

@cross_origin()
@app.route('/api/movie/page/<int:page>/', methods=['GET'])
def getMovieByPage(page):
    res = queryMovie("select * from movies order by film_id limit {paget}, 50".format(paget = page * 50), mysql.connection)
    if res == None:
        data = {'Error': "Error: No se ha podido realizar la consulta a la DB."}
        state = 400
    else:
        data = { "content": res, "state" : "OK", "cont" : len(res), "totalcont" : totalcont}
        state = 200
    return (data, state)

@cross_origin()
@app.route('/api/movie/<string:name>/', methods=['GET'])
def getMovieByName(name):
    res = [name]
    #res = queryMovie("select * from movies where original_title like \"%{namet}%\" limit 0, 50".format(namet = name), mysql.connection)
    if res == None:
        data = {'Error': "Error: No se ha podido realizar la consulta a la DB."}
        state = 400
    else:

        resAlg = algoritmo.execute(name, 0, True)
        
        resquery = queryMovie("select * from movies where film_id in {list} ".format(list = str(resAlg).replace('[','(').replace(']', ')')), mysql.connection)
        res = sortIndex(resAlg, resquery)

        data = { "content": res, "state" : "OK", "cont" : len(res), "totalcont" : totalcont}
        state = 200
    return (data, state) 

@cross_origin()
@app.route('/api/movie/similar/', methods=['POST'])
def getMovieByText():
    if request.method == 'POST':
        text = request.json['text'];
        model = request.json['model']

        array = algoritmo.execute(text, int(model), False)
        arraystr = str(array).replace('[', '(').replace(']', ')')
        resquery = queryMovie("select * from movies where film_id in " + arraystr, mysql.connection)
        if resquery == None:
            data = {'Error': "Error: No se ha podido realizar la consulta a la DB."}
            state = 400
        else:
            res = sortIndex(array, resquery)

            data = { "content": res, "state" : "OK", "cont" : len(res), "totalcont" : totalcont}
            state = 200
        return (data, state)
    return ([], 404)

@cross_origin()
@app.route('/api/upload/probe/')
def probe():

    path = 'personajes10.csv'

    df = pd.read_csv(path)
    print('here')
    headers = df.columns.values.tolist()

    data = headers
    state = 200
    return (data, state)

# @cross_origin()
# @app.route('/api/upload/library/')
# def addLibrary():

#     # title = request.json['title'];
#     # path = request.json['path'];

#     title = 'Personajes21'
#     path = 'personajes2.csv'

#     try:
#         mysql.connection.cursor().execute("INSERT INTO  libraries (title) VALUES (\'{title}\')".format(title = title))
#         mysql.connection.commit()
#     except:
#         state = 400
#         data = {'Error': "Error: No se ha podido insertar la tabla en la DB."}
#         return (data, state)

#     #Actualiza library_headers
#     query = "SELECT id FROM libraries WHERE title = \'{title}\'".format(title = title)

#     id = queryNumber(query, mysql.connection)


#     df = pd.read_csv(path)
#     print('here')
#     headers = df.columns.values.tolist()

#     engine = create_engine("mysql+pymysql://" + constants.MYSQL_USER + ":" + constants.MYSQL_PASSWORD + "@" + constants.MYSQL_HOST + "/" + constants.MYSQL_DB)

#     headersPriority = []
#     idLibrary = []
#     for header in headers:
#         idLibrary.append(id)
#         headersPriority.append(0)

#     dataHeaders = pd.DataFrame()
#     dataHeaders['id_library'] = idLibrary
#     dataHeaders['header_name'] = headers
#     dataHeaders['header_priority'] = headersPriority

#     dataHeaders.to_sql('library_headers', con=engine, if_exists='append', index = True)

#     #df = calculeDataFrame(df)

#     df.to_sql(title, con=engine, if_exists='fail', index = True)

#     headers = []
#     res = { 'headers' : headers}
#     data = { "content": res, "state" : "OK", "cont" : len(res), "totalcont" : totalcont}

#     state = 200
#     return (data, state)

def calculeDataFrame(df):
    print('hola')
    

    


### Util

def sortIndex(resAlg, resQuery):
    indexes = []
    for peli in resQuery:
        indexes.append(resAlg.index(peli['film_id']))

    aux = [None] * len(indexes)
    for i, indice in enumerate(indexes):
        aux[indice] = resQuery[i]
    return aux;


### funciones para consultas ###

def queryNumber(query, connection):
    number = execute_query(query, connection)
    if number != None:
        return number[0][0]
    else:
        return None

def queryMovie(query, connection):
    listMovies = execute_query(query, connection)

    if listMovies != None:
        content = []
        for movie in listMovies:
            content.append(Movie(movie).__dict__)
        return content
    else:
        return None

def execute_query(query, connection):
    cursor = connection.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    return data


if __name__ == '__main__':
    algoritmo = algoritmo()
    app.run(port= serverPort, debug=True)








