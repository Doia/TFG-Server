
#Server constants

# HOSTNAME = "localhost"
# SERVERPORT = 8080
# MYSQL_HOST = '2.137.49.135'
# MYSQL_USER = "root"
# MYSQL_PASSWORD = "ZnvzV4whRfi4xpQw"
# MYSQL_DB = "bbdd_tfg"

#Local constanst

HOSTNAME = "localhost"
SERVERPORT = 8080
MYSQL_HOST = 'localhost'
MYSQL_USER = "root"
MYSQL_PASSWORD = "sasa"
MYSQL_DB = "bd_tfg"

#Data Base Constants


#Data Files Constants

DATA_FOLDER = "dataset/"
MODEL_FOLDER = "modelos/"
EMBEDDING_FOLDER = "embeddings/"

#Data Files names

ACTORS_EMBEDDING = "embeddings-actors-08-31.pkl"
TITLE_EMBEDDING = "embeddings-title-08-31.pkl"
OVERVIEW_EMBEDDING = "embeddings-overviews-08-31.pkl"

SIMILARITY_MATRIX = "similarity_matrix-08-31.pkl"

MOVIES_DATASET = "Final dataset_08_31.csv"
GENRES_DATASET = "genres_ids.csv"
MOVIES_GENRES_DATASET = "movies_genres.csv"

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

#ENUM
class EMBEDDING:
    TITLE = 1
    OVERVIEW = 2
    ACTORS = 3