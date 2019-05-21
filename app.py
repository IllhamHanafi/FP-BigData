from flask import Blueprint, Flask, render_template, request

main = Blueprint('main', __name__)

import json
from engine import ClusteringEngine

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@main.route("/clusterCity/model<int:model_numb>", methods=["POST"])
def cluster_city(model_numb):
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
    cluster_location = clustering_engine.cluster_city(latitude_fetched, longitude_fetched, model_numb)
    return json.dumps(cluster_location)

def create_app(spark_session, dataset_path):
    global clustering_engine

    clustering_engine = ClusteringEngine(spark_session, dataset_path)

    app = Flask(__name__)
    app.register_blueprint(main)
    return app
