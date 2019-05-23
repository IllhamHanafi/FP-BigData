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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transformDF(dataframe):
    dataframe = dataframe.withColumn("Latitude", dataframe["Latitude"].cast("double"))
    dataframe = dataframe.withColumn("Longitude", dataframe["Longitude"].cast("double"))
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
        logger.info("Splitting dataset into 3...")
        # Model 0: 1/3 data pertama.
        # Model 1: 1/3 data pertama + 1/3 data kedua.
        # Model 2: semua data
        self.df0 = self.dforiginal.limit(int(self.dataset_count / 3))
        self.df1 = self.dforiginal.limit(int(self.dataset_count * 2 / 3))
        self.df2 = self.dforiginal
        print('df 0 count = ' + str(self.df0.count()))
        print('df 1 count = ' + str(self.df1.count()))
        print('df 2 count = ' + str(self.df2.count()))
        logger.info("Dataset Splitted !")

        logger.info("Training model 0...")
        kmeans_0 = KMeans().setK(10).setSeed(1)
        model_0 = kmeans_0.fit(self.df0)
        self.predictions_0 = model_0.transform(self.df0)
        logger.info("Model 0 built!")
        logger.info("Evaluating the model 0...")
        evaluator_0 = ClusteringEvaluator()
        silhouette_0 = evaluator_0.evaluate(self.predictions_0)
        logger.info("Silhouette with squared euclidean distance = " + str(silhouette_0))
        self.centers_0 = model_0.clusterCenters()
        logger.info("Model 0 Done !")

        logger.info("Training model 1...")
        kmeans_1 = KMeans().setK(10).setSeed(1)
        model_1 = kmeans_1.fit(self.df1)
        self.predictions_1 = model_1.transform(self.df1)
        logger.info("Model 1 built!")
        logger.info("Evaluating the model 1...")
        evaluator_1 = ClusteringEvaluator()
        silhouette_1 = evaluator_1.evaluate(self.predictions_1)
        logger.info("Silhouette with squared euclidean distance = " + str(silhouette_1))
        self.centers_1 = model_1.clusterCenters()
        logger.info("Model 1 Done !")

        logger.info("Training model 2...")
        kmeans_2 = KMeans().setK(10).setSeed(1)
        model_2 = kmeans_2.fit(self.df2)
        self.predictions_2 = model_2.transform(self.df2)
        logger.info("Model 2 built!")
        logger.info("Evaluating the model 2...")
        evaluator_2 = ClusteringEvaluator()
        silhouette_2 = evaluator_2.evaluate(self.predictions_2)
        logger.info("Silhouette with squared euclidean distance = " + str(silhouette_2))
        self.centers_2 = model_2.clusterCenters()
        logger.info("Model 2 Done !")

    def cluster_city(self, latitude_fetched, longitude_fetched, model_numb):
        """Add additional city in DB and retrain the model
        """
        distance = []
        if model_numb == 0:
            center_varname = self.centers_0
        elif model_numb == 1:
            center_varname = self.centers_1
        elif model_numb == 2:
            center_varname = self.centers_2
        for center in center_varname:
            distance.append((pow((float(center[0]) - float(latitude_fetched)), 2)) + (pow((float(center[1]) - float(longitude_fetched)), 2)))
        cluster = distance.index(min(distance))
        return cluster

    def __init__(self, spark_session, dataset_folder_path):
        """Init the clustering engine given a Spark context and a dataset path
        """
        logger.info("Starting up the Clustering Engine: ")
        self.spark_session = spark_session
        logger.info("Loading City Data...")
        file_counter = 0
        while True:
            file_name = 'result' + str(file_counter) + '.txt'
            dataset_file_path = os.path.join(dataset_folder_path, file_name)
            exist = os.path.isfile(dataset_file_path)
            if exist:
                if file_counter == 0:
                    self.dforiginal = spark_session.read.csv(dataset_file_path, header=None, inferSchema=True)
                else:
                    new_df = spark_session.read.csv(dataset_file_path, header=None, inferSchema=True)
                    self.dforiginal = self.dforiginal.union(new_df)
                self.dataset_count = self.dforiginal.count()
                print('dataset loaded = ' + str(self.dataset_count))
                print(file_name + ' Loaded !')
                file_counter += 1
            else:
                break
        self.dforiginal = self.dforiginal.selectExpr("_c0 as Country", "_c1 as City", "_c2 as AccentCity", "_c3 as Region", "_c4 as Population", "_c5 as Latitude", "_c6 as Longitude")
        # self.df.show()
        # print(self.dforiginal.count())
        self.dforiginal = transformDF(self.dforiginal)
        self.__train_model()
