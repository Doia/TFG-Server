
class Movie:

    def __init__(self, data):

        self.genres = data[0]
        self.id = data[1]
        self.original_title = data[2]
        self.overview = data[3]
        self.poster_path = data[4]
        self.release_date = data[5]
        self.runtime = data[6]
        self.actors = data[7]
        self.title_es = data[8]
        self.overview_es = data[9]
        self.len_overview = data[10]
        self.actors_string = data[11]

        self.ids_similar_films = data[12]

        self.cosenos_similar_films = data[13]

