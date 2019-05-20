import os
import logging
import pandas as pd
from pyspark.sql import Row
from pyspark.sql import types
from pyspark.sql.functions import explode
import pyspark.sql.functions as func
from sklearn.preprocessing import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import DoubleType
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import plotly.plotly as py
import plotly.graph_objs as go
import plotly

# setting user, api key and access token
plotly.tools.set_credentials_file(username='illhamhanafi', api_key='ogNKJZXFl7n9EJHDAzvz')
mapbox_access_token = 'pk.eyJ1IjoiYnVqYW5ncGVzaW1pcyIsImEiOiJjanRlNmY5N3cxZXM4NDlucm54dm55ejc1In0.nrK84u8ztnUJ8CYkiojZUA'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transformDF(dataframe):
    dataframe = dataframe.withColumn("Latitude", dataframe["Latitude"].cast("double"))
    dataframe = dataframe.withColumn("Longitude", dataframe["Longitude"].cast("double"))
    # self.df = self.df.withColumn("Region", self.df["Region"].cast("double"))
    assembler = VectorAssembler(
        inputCols=["Latitude", "Longitude"],
        outputCol='features')
    dataframe = assembler.transform(dataframe)
    return dataframe

class ClusteringEngine:
    """A city clustering engine
    """

    def __train_model(self):
        """Train the model with the current dataset
        """
        logger.info("Training the cluster model...")
        kmeans = KMeans().setK(10).setSeed(1)
        model = kmeans.fit(self.df)
        self.predictions = model.transform(self.df)
        logger.info("Model built!")
        logger.info("Evaluating the cluster model...")
        evaluator = ClusteringEvaluator()
        silhouette = evaluator.evaluate(self.predictions)
        logger.info("Silhouette with squared euclidean distance = " + str(silhouette))
        self.predictions.show(20)

    def add_city(self, country_fetched, city_fetched, accentCity_fetched, region_fetched, population_fetched, latitude_fetched, longitude_fetched):
        """Add additional city in DB and retrain the model
        """
        new_city = self.spark_session.createDataFrame([(country_fetched, city_fetched, accentCity_fetched, region_fetched, population_fetched, latitude_fetched, longitude_fetched)],
                                                         ["Country", "City", "AccentCity", "Region", "Population", "Latitude", "Longitude"])
        # Add new city to the existi1ng ones
        new_city = transformDF(new_city)
        self.df = self.df.union(new_city)
        # Re-train the model with the new ratings
        self.__train_model()
        self.predictions.createOrReplaceTempView("citydata")
        requested_cluster = self.spark_session.sql('SELECT Country, City, AccentCity, Region, Population, Latitude, Longitude, prediction from citydata where Latitude = "%s" and Longitude = "%s"' % (latitude_fetched, longitude_fetched))
        requested_cluster= requested_cluster.toPandas()
        requested_cluster = requested_cluster.to_json()
        return requested_cluster

    # def get_map():
    #     self.df

    # def get_top_ratings(self, user_id, books_count):
    #     """Recommends up to book_count top unrated books to user_id
    #     """
    #     users = self.ratingsdf.select(self.als.getUserCol())
    #     users = users.filter(users.userID == user_id)
    #     userSubsetRecs = self.model.recommendForUserSubset(users, books_count)
    #     userSubsetRecs = userSubsetRecs.withColumn("recommendations", explode("recommendations"))
    #     userSubsetRecs = userSubsetRecs.select(func.col('User-ID'),
    #                                            func.col('recommendations')['ISBN'].alias('ISBN'),
    #                                            func.col('recommendations')['Rating'].alias('Rating')).\
    #                                                                                 drop('recommendations')
    #     userSubsetRecs = userSubsetRecs.drop('Rating')
    #     # userSubsetRecs = userSubsetRecs.join(self.moviesdf, ("movieId"), 'inner')
    #     # userSubsetRecs.show()
    #     # userSubsetRecs.printSchema()
    #     userSubsetRecs = userSubsetRecs.toPandas()
    #     userSubsetRecs = userSubsetRecs.to_json()
    #     return userSubsetRecs

    # def get_top_movie_recommend(self, movie_id, user_count):
    #     """Recommends up to movies_count top unrated movies to user_id
    #     """
    #     movies = self.ratingsdf.select(self.als.getItemCol())
    #     movies = movies.filter(movies.movieId == movie_id)
    #     movieSubsetRecs = self.model.recommendForItemSubset(movies, user_count)
    #     movieSubsetRecs = movieSubsetRecs.withColumn("recommendations", explode("recommendations"))
    #     movieSubsetRecs = movieSubsetRecs.select(func.col('movieId'),
    #                                              func.col('recommendations')['userId'].alias('userId'),
    #                                              func.col('recommendations')['Rating'].alias('Rating')).\
    #                                                                                     drop('recommendations')
    #     movieSubsetRecs = movieSubsetRecs.drop('Rating')
    #     movieSubsetRecs = movieSubsetRecs.join(self.moviesdf, ("movieId"), 'inner')
    #     # userSubsetRecs.show()
    #     # userSubsetRecs.printSchema()
    #     movieSubsetRecs = movieSubsetRecs.toPandas()
    #     movieSubsetRecs = movieSubsetRecs.to_json()
    #     return movieSubsetRecs

    # def get_ratings_for_movie_ids(self, user_id, movie_id):
    #     """Given a user_id and a list of movie_ids, predict ratings for them
    #     """
    #     request = self.spark_session.createDataFrame([(user_id, movie_id)], ["userId", "movieId"])
    #     ratings = self.model.transform(request).collect()
    #     return ratings

    # def add_ratings(self, user_id, movie_id, ratings_given):
    #     """Add additional movie ratings in the format (user_id, movie_id, rating)
    #     """
    #     # Convert ratings to an RDD
    #     new_ratings = self.spark_session.createDataFrame([(user_id, movie_id, ratings_given)],
    #                                                      ["userId", "movieId", "rating"])
    #     # Add new ratings to the existing ones
    #     self.ratingsdf = self.ratingsdf.union(new_ratings)
    #     # Re-train the ALS model with the new ratings
    #     self.__train_model()
    #     new_ratings = new_ratings.toPandas()
    #     new_ratings = new_ratings.to_json()
    #     return new_ratings

    # def get_history(self, user_id):
    #     """Get rating history for a user
    #     """
    #     self.ratingsdf.createOrReplaceTempView("ratingsdata")
    #     user_history = self.spark_session.sql('SELECT userId, movieId, rating from ratingsdata where userId = "%s"' %user_id)
    #     user_history = user_history.join(self.moviesdf, ("movieId"), 'inner')
    #     user_history = user_history.toPandas()
    #     user_history = user_history.to_json()
    #     return user_history

    def __init__(self, spark_session, dataset_folder_path):
        """Init the clustering engine given a Spark context and a dataset path
        """
        logger.info("Starting up the Clustering Engine: ")
        self.spark_session = spark_session
        # Load ratings data for later use
        logger.info("Loading City Data...")
        dataset_file_path = os.path.join(dataset_folder_path, 'worldcitiespop.csv')
        self.df = spark_session.read.csv(dataset_file_path, header=True, inferSchema=True).na.drop()
        # self.ratingsdf = self.ratingsdf.drop(self.ratingsdf.columns[1])
        # self.ratingsdf = self.ratingsdf.dropna()
        # self.ratingsdf = self.ratingsdf.toPandas()
        # Load movies data for later use
        # logger.info("Loading Books data...")
        # movies_file_path = os.path.join(dataset_path, 'BX-Books.csv')
        # self.ratingsdf.column = ['ISBN', 'bookTitle', 'bookAuthor', 'yearOfPublication', 'publisher', 'imgurlS', 'imgurlM','imgurlL']
        # self.bookdf = spark_session.read.csv(movies_file_path, header=True, inferSchema=True).na.drop()
        # Train the model
        self.df = transformDF(self.df)
        self.__train_model()
