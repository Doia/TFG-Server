
class Movie:

    def __init__(self, data):

        self.film_id = data[0]
        self.ids_similar_films = data[1]
        self.genres = data[2]
        self.original_title = data[3]
        self.overview = data[4]
        self.poster_path = data[5]
        self.release_date = data[6]
        self.runtime = data[7]

    def toJson(self):
        return self.__dict__

