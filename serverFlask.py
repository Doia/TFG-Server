from flask import Flask, request, Response
from flask_mysqldb import MySQL
from flask_cors import CORS, cross_origin
from requests import head
from algoritmo import algoritmo
from movies import Movie 
from genres import Genre 
import constants

hostName = constants.HOSTNAME
serverPort = constants.SERVERPORT

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

#Rutas

@cross_origin
@app.route('/api/movie/', methods=['GET'])
def getAllMovies():

    res = queryMovie("Select * from movies LIMIT 50", mysql.connection)
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

    res = queryMovie("Select * from movies where id = {idt}".format(idt = id), mysql.connection)

    if res == None:
        data = {'Error': "Error: No se ha podido realizar la consulta a la DB."}
        state = 400
    elif res == []:
        data = { "content": res, "state" : "OK", "cont" : len(res), "totalcont" : totalcont}
        state = 200
    else:      
        array = list(map(int,  res[0]['ids_similar_films'][1 : len(res[0]['ids_similar_films'])- 1].split(', ') ))
        res_extend = queryMovie("Select * from movies where id in {list} ".format(list= str(array).replace('[','(').replace(']', ')')), mysql.connection)
        aux = sortIndex(array, res_extend)
        res.extend(aux)

        data = { "content": res, "state" : "OK", "cont" : len(res), "totalcont" : totalcont}
        state = 200
    return (data, state)

@cross_origin()
@app.route('/api/movie/page/<int:page>/', methods=['GET'])
def getMovieByPage(page):
    res = queryMovie("select * from movies order by id limit {paget}, 50".format(paget = page * 50), mysql.connection)
    if res == None:
        data = {'Error': "Error: No se ha podido realizar la consulta a la DB."}
        state = 400
    else:
        data = { "content": res, "state" : "OK", "cont" : len(res), "totalcont" : totalcont}
        state = 200
    return (data, state)

@cross_origin()
@app.route('/api/movie/title/', methods=['POST'])
def getMovieByName():
    
    genres = request.json['genres']
    mode = request.json['mode']
    name = request.json['title']

    res = []
    if mode == 'Algorithm':

        IdsToDiscard = None
        if len(genres) != 0:
            #Filtramos por generos to discard
            queryGenresToDiscard = "SELECT id_movie FROM movies_genres where {queryGenres}".format(queryGenres= generateQueryGenresToDiscard(genres));
            IdsToDiscard = execute_query(queryGenresToDiscard, mysql.connection) #Tupla de Tupla de INT

        resAlg = algoritmo.execute(name, constants.EMBEDDING.TITLE, IdsToDiscard)     
        resquery = queryMovie("select * from movies where id in {list} ".format(list = str(resAlg).replace('[','(').replace(']', ')')), mysql.connection)
        res = sortIndex(resAlg, resquery)

        data = { "content": res, "state" : "OK", "cont" : len(res), "totalcont" : totalcont}
        state = 200

    elif mode == 'Query':

        if name != "":
            auxQuery = "MATCH(movies.original_title) AGAINST(\'\"{namet}\"\')".format(namet = name)
        else:
            auxQuery = "original_title like \"%{namet}%\"".format(namet = name)

        if len(genres) == 0: 
            res = queryMovie("select * from movies where {auxQuery} limit 0, 50".format(auxQuery = auxQuery), mysql.connection)
        else:
            #Filtramos por generos
            querySelectGenres = """SELECT movies.* FROM movies left Join movies_genres on movies.id = movies_genres.id_movie
                                where {queryGenres} and {auxQuery}
                                limit 0, 50""".format(auxQuery = auxQuery, queryGenres= generateQueryGenres(genres));
            res = queryMovie(querySelectGenres, mysql.connection)
        if res == None:
            data = {'Error': "Error: No se ha podido realizar la consulta a la DB."}
            state = 400
        else:
            data = { "content": res, "state" : "OK", "cont" : len(res), "totalcont" : totalcont}
            state = 200
    else:
        data = {'Error': "Error: Modo de realizar la consulta erroneo."}
        state = 500
    
    return (data, state) 

@cross_origin()
@app.route('/api/movie/actors/', methods=['POST'])
def getMovieByActor():
    
    genres = request.json['genres']
    mode = request.json['mode']
    name = request.json['actors']

    res = []
    if mode == 'Algorithm':
        IdsToDiscard = None
        if len(genres) != 0:
            #Filtramos por generos to discard
            queryGenresToDiscard = "SELECT id_movie FROM movies_genres where {queryGenres}".format(queryGenres= generateQueryGenresToDiscard(genres));
            IdsToDiscard = execute_query(queryGenresToDiscard, mysql.connection) #Tupla de Tupla de INT

        resAlg = algoritmo.execute(name, constants.EMBEDDING.ACTORS, IdsToDiscard)
        resquery = queryMovie("select * from movies where id in {list} ".format(list = str(resAlg).replace('[','(').replace(']', ')')), mysql.connection)
        res = sortIndex(resAlg, resquery)

        data = { "content": res, "state" : "OK", "cont" : len(res), "totalcont" : totalcont}
        state = 200
    
    elif mode == 'Query':

        if name != "":
            auxQuery = "MATCH(movies.actors_string) AGAINST(\'\"{namet}\"\')".format(namet = name)
        else:
            auxQuery = "actors_string like \"%{namet}%\"".format(namet = name)

        if len(genres) == 0:
            res = queryMovie("select * from movies where {auxQuery} limit 0, 50".format(auxQuery = auxQuery), mysql.connection)
        else:
            #Filtramos por generos
            querySelectGenres = """SELECT movies.* FROM movies left Join movies_genres on movies.id = movies_genres.id_movie
                                where {queryGenres} and {auxQuery}
                                limit 0, 50""".format(auxQuery = auxQuery, queryGenres= generateQueryGenres(genres));

            res = queryMovie(querySelectGenres, mysql.connection)

        if res == None:
            data = {'Error': "Error: No se ha podido realizar la consulta a la DB."}
            state = 400
        else:
            data = { "content": res, "state" : "OK", "cont" : len(res), "totalcont" : totalcont}
            state = 200
    else:
        data = {'Error': "Error: Modo de realizar la consulta erroneo."}
        state = 500

    return (data, state) 

@cross_origin()
@app.route('/api/movie/similar/', methods=['POST'])
def getMovieByText():
    text = request.json['text'];
    genres = request.json['genres']


    IdsToDiscard = None
    if len(genres) != 0:
        #Filtramos por generos to discard
        queryGenresToDiscard = "SELECT id_movie FROM movies_genres where {queryGenres}".format(queryGenres= generateQueryGenresToDiscard(genres));
        IdsToDiscard = execute_query(queryGenresToDiscard, mysql.connection) #Tupla de Tupla de INT

    array = algoritmo.execute(text, constants.EMBEDDING.OVERVIEW, IdsToDiscard)

    arraystr = str(array).replace('[', '(').replace(']', ')')

    resquery = queryMovie("select * from movies where id in " + arraystr, mysql.connection)
    if resquery == None:
        data = {'Error': "Error: No se ha podido realizar la consulta a la DB."}
        state = 400
    else:
        res = sortIndex(array, resquery)

        data = { "content": res, "state" : "OK", "cont" : len(res), "totalcont" : totalcont}
        state = 200
    return (data, state)


@cross_origin()
@app.route('/api/genres/', methods=['GET'])
def getGenresList():
    res = queryGenre("Select * from genres", mysql.connection)
    if res == None:
        data = {'Error': "Error: No se ha podido realizar la consulta a la DB."}
        state = 400
    else:
        data = { "content": res, "state" : "OK", "cont" : len(res), "totalcont" : totalcont}
        state = 200
    return (data, state)

    
### Util

def sortIndex(resAlg, resQuery):
    indexes = []
    for peli in resQuery:
        indexes.append(resAlg.index(peli['id']))

    aux = [None] * len(indexes)
    for i, indice in enumerate(indexes):
        aux[indice] = resQuery[i]
    return aux

def genreToStr(n):
    return 'genre_' + str(n)

#Dada una lista de generos [2,3,4] devuelve algo tal que asi:
# "genre_2 = True OR genre_3 = True OR genre_4 = True" 
def generateQueryGenres(genres):
    res = ""
    isNotFirst = False
    for n in genres:
        if isNotFirst:
            res += "AND "
        isNotFirst = True
        res += genreToStr(n) + ' = True '
    return res


#Dada una lista de generos [2,3,4] devuelve algo tal que asi:
# "genre_2 = True OR genre_3 = True OR genre_4 = True" 
def generateQueryGenresToDiscard(genres):
    res = ""
    isNotFirst = False
    for n in genres:
        if isNotFirst:
            res += "OR "
        isNotFirst = True
        res += genreToStr(n) + ' != True '
    return res

### funciones para consultas ### 

def queryNumber(query, connection):
    number = execute_query(query, connection)
    if number != None:
        return number[0][0]
    else:
        return None

def queryGenre(query, connection):
    listGenres = execute_query(query, connection)
    if listGenres != None:
        content = []
        for genre in listGenres:
            content.append(Genre(genre).__dict__)
        return content
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








