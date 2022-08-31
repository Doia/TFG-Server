from dataclasses import dataclass
import pandas as pd
import pickle
import numpy as np
from sentence_transformers import SentenceTransformer, util, models
import torch
import constants
import os


class algoritmo:

    def __init__(self):

        self.actorsEmbeddings_file_path = constants.EMBEDDING_FOLDER + constants.ACTORS_EMBEDDING
        self.titleEmbeddings_file_path = constants.EMBEDDING_FOLDER + constants.TITLE_EMBEDDING
        self.overviewEmbeddings_file_path = constants.EMBEDDING_FOLDER + constants.OVERVIEW_EMBEDDING

        #INICIAR DEVICE
        self.device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

        #DESCARGAR E INICIAR MODELOS
        if os.path.exists(constants.MODEL_FOLDER + constants.MODEL_NAME):
            print('LOADING MODELS...')
            self.modelo = SentenceTransformer(model_name_or_path = constants.MODEL_FOLDER + constants.MODEL_NAME)
        else:
            print('DOWNLOADING MODELS...')
            self.modelo = SentenceTransformer(model_name_or_path = constants.MODEL_NAME)
            self.modelo.save(constants.MODEL_FOLDER + constants.MODEL_NAME)

        self.modelo = self.modelo.to(self.device)

        print('LOADING EMBEDDINGS OVERVIEW...')
        #LOAD EMBEDDINGS OVERVIEW
        with open(self.overviewEmbeddings_file_path, "rb") as fIn:
            self.overviewEmbeddings = torch.from_numpy(pickle.load(fIn)).to(self.device)
        

        print('LOADING EMBEDDINGS TITLE...')
        #LOAD EMBEDDINGS TITLE
        with open(self.titleEmbeddings_file_path, "rb") as fIn:
            self.titleEmbeddings = torch.from_numpy(pickle.load(fIn)).to(self.device)

        # print('LOADING EMBEDDINGS ACTORS...')
        # #LOAD EMBEDDINGS TITLE
        # with open(self.actorsEmbeddings_file_path, "rb") as fIn:
        #     self.actorsEmbeddings = torch.from_numpy(pickle.load(fIn)).to(self.device)
        

        print('ALL DATA LOADED SUCCESFULLY!\n')

    def execute(self, text, embbeding, genresToDiscard = None):

        #ALGORITMO
        if embbeding == constants.EMBEDDING.TITLE:
            embeddings = self.titleEmbeddings
        elif embbeding == constants.EMBEDDING.OVERVIEW:
            embeddings = self.overviewEmbeddings
        elif embbeding == constants.EMBEDDING.ACTORS:
            embeddings = self.actorsEmbeddings

        num_save = 50
        normalize_embeddings = False
        
        query_embedding = self.modelo.encode(text,
                                        convert_to_tensor=True,
                                        device = self.device,
                                        normalize_embeddings=normalize_embeddings)
        cosenos_aux = util.pytorch_cos_sim(query_embedding, embeddings).cpu().numpy() #Matriz (cantidad_por_iteracion X len(embeddings))
        cosenos_aux = cosenos_aux[0]


        #Lista Ids Que no quieres
        #Filter with genres
        if genresToDiscard != None:
            for id in genresToDiscard:
                cosenos_aux[id[0]] = -1
   
        indexes_best_numGuardar = np.argpartition(-cosenos_aux, num_save-1)[0:num_save]
        cosenos_best_numGuardar = np.take(cosenos_aux, indexes_best_numGuardar)
        indexes_biencoder = np.take(indexes_best_numGuardar, np.argsort(-cosenos_best_numGuardar)).tolist()
        #cosenos_biencoder = np.take(cosenos_aux, indexes_biencoder).tolist()

        return indexes_biencoder
        


