from flask import Blueprint, Flask, render_template, request

main = Blueprint('main', __name__)

import json
from engine import ClusteringEngine

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@main.route("/clusterCity", methods=["POST"])
def cluster_city():
    """"Untuk menambahkan sebuah kota ke dataset"""
    # get the ratings from the Flask POST request object
    country_fetched = request.form.get('Country')
    city_fetched = request.form.get('City')
    accentCity_fetched = request.form.get('AccentCity')
    region_fetched = request.form.get('Region')
    population_fetched = int(request.form.get('Population'))
    latitude_fetched = float(request.form.get('Latitude'))
    longitude_fetched = float(request.form.get('Longitude'))
    # add them to the model using then engine API
    cluster_location = clustering_engine.cluster_city(country_fetched, city_fetched, accentCity_fetched,
                                                 region_fetched, population_fetched, latitude_fetched, longitude_fetched)
    # ratings = ratings.toJSON()
    return json.dumps(cluster_location)

# @main.route("/getmap", methods=["GET"])
# def get_map():
#     """"Untuk menampilkan peta dunia berdasarkan data yang sudah ada"""
#     logger.debug("World Map Requested")
#     top_rated = recommendation_engine.get_recommendation(user_id, count)
#     return render_template('getmap.html')


# @main.route("/movies/<int:movie_id>/recommend/<int:count>", methods=["GET"])
# def movie_recommending(movie_id, count):
#     """"Untuk menampilkan film <movie_id> terbaik direkomendasikan ke sejumlah <count> user"""
#     logger.debug("MovieId %s TOP user recommending", movie_id)
#     top_rated = recommendation_engine.get_top_movie_recommend(movie_id, count)
#     return json.dumps(top_rated)
#
#
# @main.route("/<int:user_id>/ratings/<int:movie_id>", methods=["GET"])
# def movie_ratings(user_id, movie_id):
#     """"Untuk melakukan prediksi user <user_id> memberi rating X terhadap film <movie_id>"""
#     logger.debug("User %s rating requested for movie %s", user_id, movie_id)
#     ratings = recommendation_engine.get_ratings_for_movie_ids(user_id, movie_id)
#     return json.dumps(ratings)
#
# @main.route("/<int:user_id>/history", methods=["GET"])
# def ratings_history(user_id):
#     """"Untuk melihat riwayat pemberian rating oleh user <user_id>"""
#     logger.debug("History for user %s is requested", user_id)
#     user_history = recommendation_engine.get_history(user_id)
#     return json.dumps(user_history)
#
# @main.route("/<int:user_id>/giverating", methods=["POST"])
# def add_ratings(user_id):
#     """"Untuk submit <user_id> memberikan rating untuk film X"""
#     # get the ratings from the Flask POST request object
#     movieId_fetched = int(request.form.get('movieId'))
#     ratings_fetched = float(request.form.get('ratingGiven'))
#     # print(movieId_fetched)
#     # print(ratings_fetched)
#     # add them to the model using then engine API
#     new_rating = recommendation_engine.add_ratings(user_id, movieId_fetched, ratings_fetched)
#     # ratings = ratings.toJSON()
#     return json.dumps(new_rating)


def create_app(spark_session, dataset_path):
    global clustering_engine

    clustering_engine = ClusteringEngine(spark_session, dataset_path)

    app = Flask(__name__)
    app.register_blueprint(main)
    return app
