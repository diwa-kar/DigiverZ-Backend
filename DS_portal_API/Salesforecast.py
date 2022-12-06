from pyspark.sql.functions import when, lit, regexp_replace, substring, to_timestamp, to_date, col
from pyspark.sql.types import DoubleType, StringType
from DS_portal_API.flask_rest_API import test_db
from pyspark.sql.types import *
from sklearn.preprocessing import LabelEncoder
from pyspark.sql import SparkSession
import pyspark
import sys

import seaborn as sns
from pymongo import MongoClient
import pprint
import os
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
import sklearn
import numpy as np
import pickle
import pandas as pd
import math
from bson.objectid import ObjectId
from flask import jsonify, request
from flask_cors import CORS, cross_origin
from bson import json_util
import json
import bson

import datetime
from datetime import datetime
from dateutil import relativedelta

from flask import Flask
from http import client
from unittest import result
from dotenv import load_dotenv, find_dotenv


from pycaret.datasets import get_data
from pycaret.regression import *


import matplotlib
matplotlib.use('Agg')

load_dotenv(find_dotenv())


def Salesforecast_API(endpoints):

    @endpoints.route("/sales_date", methods=['POST'])
    def Salesforecast_date():
        collection = test_db.sales_results
        _req = request.get_json()
        # end_date = _req['value']
        end_date = _req['date']
        start_date = '2018-01-01'
        s_datetime_object = datetime.strptime(start_date, '%Y-%m-%d').date()
        datetime_object = datetime.strptime(end_date, '%Y-%m-%d').date()

        # s_datetime_object = datetime.strptime(start_date, '%Y-%m-%d')
        # datetime_object = datetime.strptime(end_date, '%Y-%m-%d')

        # pycaret_opt = _req['pycaretopt']
        # print("im printing options")
        # print(pycaret_opt)

        print("im printing the date")
        print('\n')
        print(datetime_object)
        print(s_datetime_object)
        print("im printing types of dates the date")
        print('\n')
        print("sdate", type(s_datetime_object))
        print("e_date", type(datetime_object))

        delta = relativedelta.relativedelta(datetime_object, s_datetime_object)
        res_months = delta.months + (delta.years * 12) + 1
        # delta =  datetime_object - s_datetime_object
        # months = delta.months
        print("im printing difference btw the date")
        print('\n')

        print(res_months)

        with open('H:\React\Data science project\ds-portal-flask\saved_steps_salesforcast.pkl', 'rb') as file:
            data = pickle.load(file)

        result = data["result"]
        y = data["y"]
        forcast_loaded = result.get_forecast(steps=res_months)
        pred_ci = forcast_loaded.conf_int()

        ax = y.plot(label='observed', figsize=(14, 7))
        forcast_loaded.predicted_mean.plot(ax=ax, label='forecast')
        ax.fill_between(
            pred_ci.index, pred_ci.iloc[:, 0], pred_ci.iloc[:, 1], color='k', alpha=0.25)
        ax.set_xlabel('Date')
        ax.set_ylabel('Furniture Sales')

        plt.legend()
        plt.show()
        plt.savefig(
            r"H:\React\Data science project\ds-portal\src\assets\salesforecastresult.png")
        plt.savefig(r"salesforecast.png")
        plt.clf()

        resp = jsonify("OK")
        resp.status_code = 200

        return resp

    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx salesforecast result get request xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

    @endpoints.route("/salesforecastresult", methods=['GET'])
    # @cross_origin()
    def find_all_Salesforecast_result():
        collection = test_db.Salesforecast_details

        user = collection.find()

        return json.loads(json_util.dumps(user))
  # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx salesforecast result get request xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

    return endpoints
