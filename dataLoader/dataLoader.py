from ast import Load
import pandas as pd
import numpy as np
import pickle
from sqlalchemy import create_engine


def listToString(list):
    str1 = "["
    isNotFirst = False

    for elem in list:
        if isNotFirst:
            str1 += ', '
        isNotFirst = True

        str1 += str(elem)
    return str1 + "]"


LOCALPATH = "C:\\Users\\Diego\\Desktop\\TFG\\TFG-Server\\dataLoader\\"

FILE_NAME = "Final dataset_08_21.csv"
MATRIX_FILE_NAME = "similarity_matrix_08_21"

LOAD_DB = False
LOAD_genres_table = True

print("Cargando dataset...")

df = pd.read_csv(LOCALPATH + FILE_NAME)

#auxIds = list(range(0, len(df['id'])))


df['id'] = list(range(0, len(df['id'])))

df.to_csv(LOCALPATH + FILE_NAME)
      
print("Cargando Matriz de similitudes...")

try:
  with open( LOCALPATH + MATRIX_FILE_NAME +'.pkl', "rb") as fIn:
    resp = pickle.load(fIn)

    cosenos_biencoder = []
    index_biencoder = []
    for elem in resp['similarity']:
        cosenos_biencoder.append(listToString(elem.tolist()))
    
    for elem in resp['indexes']:
        index_biencoder.append(listToString(elem.tolist()))

    df['index_biencoder'] = index_biencoder
    df['cosenos_biencoder'] = cosenos_biencoder


except Exception as e:
  raise e
else:
  print("Matriz de similitudes cargada correctamente.")

# print dataframe.
print("creando engine...")
engine = create_engine("mysql+pymysql://" + 'root' + ":" + 'sasa' + "@" + 'localhost' + "/" + 'bd_tfg')




if (LOAD_genres_table):
  #Tabla de generos

  # initialize list of lists

  genres = [[ 0, 'Family' ], [ 1, 'Drama' ], [ 2, 'Comedy' ], [ 3, 'Crime' ], [ 4, 'Documentary' ], [ 5, 'Animation' ], [ 6, 'Romance' ], [ 7, 'Adventure' ], [ 8, 'Action' ], [ 9, 'Science Fiction' ], [ 10, 'Thriller' ], [ 11, 'Horror' ], [ 12, 'Mystery' ], [ 13, 'Music' ], [ 14, 'Fantasy' ], [ 15, 'War' ], [ 16, 'Western' ], [ 17, 'History' ], [ 18, 'TV Movie' ]]
  df_genres = pd.DataFrame(genres, columns=['id', 'genre'])

  print("creando tabla de generos...")
  df_genres.to_sql('genres', con=engine, if_exists='replace', index = False)

  print("creando tabla de generos y Movies...")

  moviesGenres = []

  for index, row in df.iterrows():
      booleanArr = [False, False, False, False, False, False, False, False, False, False, False, False,False, False, False, False,False, False, False]
      if pd.isna(row['genres']) == False:
        aux = list(map(int, row['genres'][1 : len(row['genres'])- 1].split(', ')))
        for genre in aux:
          booleanArr[genre] = True    

      resArr = [row['id']] + booleanArr
      moviesGenres.append(resArr)  


  df_moviesGenres = pd.DataFrame(moviesGenres, columns=['id_movie', 'genre_0', 'genre_1', 'genre_2', 'genre_3', 'genre_4','genre_5',
                                                        'genre_6', 'genre_7', 'genre_8', 'genre_9','genre_10', 'genre_11', 'genre_12',
                                                        'genre_13', 'genre_14','genre_15', 'genre_16', 'genre_17', 'genre_18'])
  df_moviesGenres.to_sql('movies_genres', con=engine, if_exists='replace', index = False)


  # genresMovies = []

  # for index, row in df.iterrows():
  #     if pd.isna(row['genres']) == False:
  #         aux = list(map(int, row['genres'][1 : len(row['genres'])- 1].split(', ')))
  #         for genre in aux:
  #           genresMovies.append([row['id'], genre])  

  # df_genresMovies = pd.DataFrame(genresMovies, columns=['id_movie', 'id_genre'])
  # df_genresMovies.to_sql('genres_movies', con=engine, if_exists='replace', index = False)

if LOAD_DB:
    print("creando tabla de movies...")
    df.to_sql('movies', con=engine, if_exists='replace', index = False)
    
print("Todas las tablas cargadas con exito...")

	

