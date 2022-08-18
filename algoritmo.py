from dataclasses import dataclass
import pandas as pd
import pickle
import numpy as np
from sentence_transformers import SentenceTransformer, util, models
import torch
from asyncio.windows_events import NULL
import time


class algoritmo:

    def __init__(self):

        self.titleEmbeddings_file_path = "embeddings all-MiniLM-L6-v2 - titles"
        self.overviewEmbeddings_file_path = "embeddings- 22-03-2022"

        self.df_file_path = "Final-clean-dataset-0-duplicated.csv"
        self.nombres_modelos = [
            "sentence-transformers/paraphrase-MiniLM-L6-v2",
            "sentence-transformers/all-MiniLM-L6-v2",
        ]
        
        #DESCARGAR E INICIAR MODELOS
        self.download_models()
        self.modelos = list(map(lambda modelo: SentenceTransformer(model_name_or_path = "modelos/" + modelo), self.nombres_modelos))

        #INICIAR DEVICE
        self.device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

        #LOAD EMBEDDINGS OVERVIEW
        with open(self.overviewEmbeddings_file_path +'.pkl', "rb") as fIn:
            self.overviewEmbeddings = pickle.load(fIn).to(self.device)
        #self.overviewEmbeddings.cpu()

        #LOAD EMBEDDINGS TITLE
        with open(self.titleEmbeddings_file_path +'.pkl', "rb") as fIn:
            self.titleEmbeddings = pickle.load(fIn).to(self.device)
        #self.titleEmbeddings.cpu()

        #LOAD CSV
        self.df_original = pd.read_csv(self.df_file_path)


    def download_models(self):
        self.modelos = list(map(lambda modelo: SentenceTransformer(model_name_or_path = modelo), self.nombres_modelos))

        for i, modelo in enumerate(self.modelos):
            modelo.save('modelos/' + self.nombres_modelos[i])

    def execute(self, text, modelo, isTitle):

        #ALGORITMO
        if isTitle:
            embeddings = self.titleEmbeddings
        else:
            embeddings = self.overviewEmbeddings

        modelo = self.modelos[modelo]
        modelo = modelo.to(self.device)

        num_save = 50
        normalize_embeddings = False
        

        query_embedding = modelo.encode(text,
                                        convert_to_tensor=True,
                                        device = self.device,
                                        normalize_embeddings=normalize_embeddings)
        cosenos_aux = util.pytorch_cos_sim(query_embedding, embeddings).cpu().numpy() #Matriz (cantidad_por_iteracion X len(embeddings))
        cosenos_aux = cosenos_aux[0]
        indexes_best_numGuardar = np.argpartition(-cosenos_aux, num_save-1)[0:num_save]
        cosenos_best_numGuardar = np.take(cosenos_aux, indexes_best_numGuardar)
        indexes_biencoder = np.take(indexes_best_numGuardar, np.argsort(-cosenos_best_numGuardar)).tolist()
        cosenos_biencoder = np.take(cosenos_aux, indexes_biencoder).tolist()

        ids = []
        for i in indexes_biencoder:
            ids.append(self.df_original['id'][i])
        return ids

