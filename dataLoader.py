import pandas as pd
import pickle
import constants
from sqlalchemy import create_engine
from sqlalchemy import text

#Utils

def listToString(list):
    str1 = "["
    isNotFirst = False

    for elem in list:
        if isNotFirst:
            str1 += ', '
        isNotFirst = True

        str1 += str(elem)
    return str1 + "]"

def createPrimaryKeyQuery(table, column_name):
      return """ALTER TABLE {table} 
CHANGE COLUMN {column_name} {column_name} BIGINT NOT NULL ,
ADD PRIMARY KEY ({column_name});""".format(table = table, column_name = column_name);

def createFulltextIndexQuery(table, column_name, index_name):
      return """CREATE FULLTEXT INDEX {index_name}
ON {table}({column_name});""".format(table = table, column_name = column_name, index_name=index_name);


FILE_NAME = constants.DATA_FOLDER + constants.MOVIES_DATASET
MATRIX_FILE_NAME = constants.EMBEDDING_FOLDER + constants.SIMILARITY_MATRIX
GENRES_FILE_NAME = constants.DATA_FOLDER + constants.GENRES_DATASET

LOAD_DB = True
LOAD_genres_table = False
conf_db = False

print("Cargando dataset...")

df = pd.read_csv(FILE_NAME)

print("creando engine...")
engine = create_engine("mysql+pymysql://" + constants.MYSQL_USER + ":" + constants.MYSQL_PASSWORD + "@" + constants.MYSQL_HOST + "/" + constants.MYSQL_DB)

if LOAD_genres_table:
  #Tabla de generos

  print("cargando tabla de generos...")
  df_genres = pd.read_csv(GENRES_FILE_NAME)

  print("creando tabla de generos...")
  df_genres.to_sql('genres', con=engine, if_exists='replace', index = False)

  print("creando tabla de movies_genres...")

  #creamos las cabeceras
  genresHeader = ['id_movie']
  for id_genre in df_genres['genre_id']:
        aux = 'genre_' + str(id_genre)
        genresHeader.append(aux)

  #Creamos la tabla
  moviesGenres = []
  for index, row in df.iterrows():
      booleanArr = [False] * len(df_genres['genre_id'])

      if pd.isna(row['genres']) == False:
        aux = list(map(int, row['genres'][1 : len(row['genres'])- 1].split(', ')))
        for genre in aux:
          booleanArr[genre] = True    

      resArr = [row['id']] + booleanArr
      moviesGenres.append(resArr)  

  df_moviesGenres = pd.DataFrame(moviesGenres, columns= genresHeader)

  df_moviesGenres.to_sql('movies_genres', con=engine, if_exists='replace', index = False)



if LOAD_DB:     
  print("Cargando Matriz de similitudes...")

  try:
    with open( MATRIX_FILE_NAME, "rb") as fIn:
      resp = pickle.load(fIn)

      cosenos_biencoder = []
      index_biencoder = []
      for elem in resp['similarity']:
          cosenos_biencoder.append(listToString(elem.tolist()))
      
      for elem in resp['indexes']:
          index_biencoder.append(listToString(elem.tolist()))

      df['index_biencoder'] = index_biencoder
      df['cosenos_biencoder'] = cosenos_biencoder

      del df['index_biencoder']
      del df['cosenos_biencoder']

      df.to_csv(FILE_NAME, index=False)


      

  except Exception as e:
    raise e
  else:
    print("Matriz de similitudes cargada correctamente.")

    print("creando tabla de movies...")
    df.to_sql('movies', con=engine, if_exists='replace', index = False)
    
print("Todas las tablas cargadas con exito...")

if (conf_db):

  print("Configurando Base de datos...")

  with engine.connect() as connection:
      print("Creando claves primarias...")
      connection.execute(text(createPrimaryKeyQuery('movies', 'id')))
      connection.execute(text(createPrimaryKeyQuery('genres', 'genre_id')))
      connection.execute(text(createPrimaryKeyQuery('movies_genres', 'id_movie')))

      print("Creando indices...")
      connection.execute(text(createFulltextIndexQuery('movies', 'original_title', 'index_original_title')))
      connection.execute(text(createFulltextIndexQuery('movies', 'actors_string', 'index_actors_string')))
      connection.execute(text(createFulltextIndexQuery('movies', 'overview', 'index_overview')))

      print("Indices creados con exito")

print("Script Finalizado!!")





	

