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

from flask import Flask
from http import client
from unittest import result
from dotenv import load_dotenv, find_dotenv


from pycaret.datasets import get_data
from pycaret.classification import *


import matplotlib
matplotlib.use('Agg')

load_dotenv(find_dotenv())


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.appName('First_App').getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)


Global_csv_file = None


def dq_report_endpoint(endpoints):
 # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx csv adder trail xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    @endpoints.route("/dqreportcsvreader",)
    # @cross_origin()
    def csv_reader():
        collections = test_db.Dq_reprot
        result = collections.find()
        return json.loads(json_util.dumps(result))
  # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx csv adder trail xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

    @endpoints.route("/dqreport", methods=['POST'])
    # @cross_origin()
    def add_csv():
        print("im inside dqreport")
        f = request.files['file']
        """  Global_csv_file = f """
        pandas_df = pd.read_csv(f)
        print(pandas_df)

        pandas_df.to_pickle("./dq_report_csv.pkl")

        # csv_file = pandas_df
        # print("loading the csv file")
        # print(csv_file)

        collections2 = test_db.Dq_report_details
        # collections2 = test_db.Salesforecast_details

        now = datetime.now()
        current_time = now.strftime("%m/%d/%Y,%H:%M:%S")

        # sdf = spark.createDataFrame(pandas_df.astype(str))

        # print('im from spark')
        # sdf.show()

        # sdf.printSchema()

        # print(type(sdf))

        # print(sdf.head(10))

        # print("\n")
        # print(sdf.tail(10))

        # print("\n")
        # print(sdf.describe())

        # print("\n")
        # print(type(sdf.describe()))

        pdf = pandas_df.fillna(0)

        size = sys.getsizeof(pandas_df)
        res_size = size/1000000
        dataset_shape = pandas_df.shape

        # *df_datatypes
        dtype = pandas_df.dtypes.values.tolist()
        for i in range(len(dtype)):
            dtype[i] = str(dtype[i])

        df_duplicate = pandas_df.duplicated().sum()

        res_des = pandas_df.describe().fillna(0).values.tolist()

        # plt.hist(pdf['MOVIES'])
        # plt.savefig('histtry.png')

        print(current_time)

        # inserted_id = collections2.insert_one({'csv_file':csv_file.values.tolist(),'ColunmList': list(pandas_df.columns), 'dataset_shape':dataset_shape, 'df_head':pdf.head().values.tolist(),'df_tail':pdf.tail().values.tolist(),
        # 'size': res_size, 'null_values':pandas_df.isnull().sum().values.tolist(),'unique_values':pandas_df.nunique().values.tolist(),
        # 'df_datatypes':dtype,'df_duplicate_value':str(df_duplicate), 'df_des':res_des,'file_name':f.filename    }).inserted_id

        inserted_id = collections2.insert_one({'ColunmList': list(pandas_df.columns), 'dataset_shape': dataset_shape, 'df_head': pdf.head().values.tolist(), 'df_head_15': pdf.head(15).values.tolist(), 'df_tail': pdf.tail().values.tolist(),
                                               'size': res_size, 'null_values': pandas_df.isnull().sum().values.tolist(), 'unique_values': pandas_df.nunique().values.tolist(),
                                               'df_datatypes': dtype, 'df_duplicate_value': str(df_duplicate), 'df_des': res_des, 'file_name': f.filename, 'current_time': current_time}).inserted_id

        print(inserted_id)
        resp = jsonify("user added succesfully")
        resp.status_code = 200

        return resp

    @endpoints.route("/dqgraph", methods=['GET'])
    # @cross_origin()
    def DQ_graph():
        plt.hist(Global_csv_file['MOVIES'])
        plt.savefig('histtry.png')
        resp = jsonify("histogram graph is added")
        resp.status_code = 200

        return resp
 # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx fetching dqcsv trail xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

    @endpoints.route("/dqcsv", methods=['GET'])
    # @cross_origin()
    def find_all_dq_csv():
        collection = test_db.Dq_report_details
        user = collection.find()

        return json.loads(json_util.dumps(user))
 # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx fetching dqcsv trail xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
  # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx dq result get request xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

    @endpoints.route("/dqresult", methods=['GET'])
    # @cross_origin()
    def find_all_dq_result():
        collection = test_db.Dq_report_details
        user = collection.find()

        return json.loads(json_util.dumps(user))
  # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx dq result get request xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

    @endpoints.route("/dqhistory", methods=['GET'])
    # @cross_origin()
    def dq_result_history():
        collection = test_db.Dq_report_details
        user = collection.find()

        return json.loads(json_util.dumps(user))

    @endpoints.route("/dqgraph_1", methods=['POST'])
    # @cross_origin()
    def DQ_graph_1():
        _req = request.get_json()

        df = pd.read_pickle("./dq_report_csv.pkl")

        graph_1 = _req['graph_1']

        print(graph_1)
        plt.hist(df[graph_1])
        plt.savefig(
            r"H:\React\Data science project\ds-portal\src\assets\graph1.png")
        plt.clf()
        plt.hist(df[graph_1])  
        plt.savefig(
            r"H:\React\Data science project\ds-portal\src\assets\trail.png")
        
        
        
        plt.clf()

        resp = jsonify("histogram graph is added")
        resp.status_code = 200

        return resp

    @endpoints.route("/dqgraph_2", methods=['POST'])
    # @cross_origin()
    def DQ_graph_2():
        _req = request.get_json()
        df = pd.read_pickle("./dq_report_csv.pkl")

        graph_2 = _req['graph_2']
        plt.scatter(df.index, df[graph_2])
        plt.savefig(
            r"H:\React\Data science project\ds-portal\src\assets\graph2.png")
        plt.clf()

        df = pd.read_pickle("./dq_report_csv.pkl")

        resp = jsonify("scaterplt graph is added")
        resp.status_code = 200

        return resp

    @endpoints.route("/dqgraph_3", methods=['POST'])
    # @cross_origin()
    def DQ_graph_3():
        _req = request.get_json()
        df = pd.read_pickle("./dq_report_csv.pkl")

        graph_3 = _req['graph_3']
        sns.set(rc={'figure.figsize': (5, 5)})
        sns.kdeplot(df[graph_3], shade=True)
        plt.savefig(
            r"H:\React\Data science project\ds-portal\src\assets\graph3.png")
        plt.clf()

        df = pd.read_pickle("./dq_report_csv.pkl")

        resp = jsonify("scaterplt graph is added")
        resp.status_code = 200

        return resp

    @endpoints.route("/dqgraph_4", methods=['POST'])
    # @cross_origin()
    def DQ_graph_4():
        _req = request.get_json()
        df = pd.read_pickle("./dq_report_csv.pkl")

        graph_4 = _req['graph_4']
        graph_4_1 = _req['graph_4_1']
        sns.boxplot(x=graph_4, y=graph_4_1, data=df, palette='rainbow')
        plt.savefig(
            r"H:\React\Data science project\ds-portal\src\assets\graph4.png")
        plt.clf()

        df = pd.read_pickle("./dq_report_csv.pkl")

        resp = jsonify("scaterplt graph is added")
        resp.status_code = 200

        return resp

    @endpoints.route("/dqgraph_5", methods=['POST'])
    # @cross_origin()
    def DQ_graph_5():
        _req = request.get_json()
        df = pd.read_pickle("./dq_report_csv.pkl")

        graph_5 = _req['graph_5']
        graph_5_1 = _req['graph_5_1']
        sns.stripplot(x=graph_5, y=graph_5_1, data=df, palette='rainbow')
        plt.savefig(
            r"H:\React\Data science project\ds-portal\src\assets\graph5.png")
        plt.clf()

        df = pd.read_pickle("./dq_report_csv.pkl")

        resp = jsonify("scaterplt graph is added")
        resp.status_code = 200

        return resp

    @endpoints.route("/dqgraph_6", methods=['POST'])
    # @cross_origin()
    def DQ_graph_6():
        _req = request.get_json()
        df = pd.read_pickle("./dq_report_csv.pkl")

        graph_6 = _req['graph_6']
        graph_6_1 = _req['graph_6_1']
        sns.violinplot(x=graph_6, y=graph_6_1, data=df, palette='rainbow')

        plt.savefig(
            r"H:\React\Data science project\ds-portal\src\assets\graph6.png")
        plt.clf()

        df = pd.read_pickle("./dq_report_csv.pkl")

        resp = jsonify("scaterplt graph is added")
        resp.status_code = 200

        return resp

    @endpoints.route("/dqgraph_7", methods=['POST'])
    # @cross_origin()
    def DQ_graph_7():
        _req = request.get_json()
        df = pd.read_pickle("./dq_report_csv.pkl")

        graph_7 = _req['graph_7']
        graph_7_1 = _req['graph_7_1']
        sns.swarmplot(x=graph_7, y=graph_7_1, data=df, palette='rainbow')

        plt.savefig(
            r"H:\React\Data science project\ds-portal\src\assets\graph7.png")
        plt.clf()

        df = pd.read_pickle("./dq_report_csv.pkl")

        resp = jsonify("scaterplt graph is added")
        resp.status_code = 200

        return resp

    @endpoints.route("/dqgraph_8", methods=['POST'])
    # @cross_origin()
    def DQ_graph_8():
        _req = request.get_json()
        df = pd.read_pickle("./dq_report_csv.pkl")

        graph_8 = _req['graph_8']

        sns.countplot(x=graph_8, data=df)

        plt.savefig(
            r"H:\React\Data science project\ds-portal\src\assets\graph8.png")
        plt.clf()

        df = pd.read_pickle("./dq_report_csv.pkl")

        resp = jsonify("scaterplt graph is added")
        resp.status_code = 200

        return resp
    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx Alogrithm analyzer xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

    @endpoints.route("/algofile_upload", methods=['POST'])
    # @cross_origin()
    def add_file_algo_analyze():

        f = request.files['file']
        global glob_file
        glob_file = f
        collection = test_db.algo_details

        pandas_df = pd.read_csv(f, low_memory=False, encoding='unicode_escape')
        pandas_df.to_pickle("./algo.pkl")
        # *size and shape of df
        size = sys.getsizeof(pandas_df)
        res_size = size/1000000
        dataset_shape = pandas_df.shape

        inserted_id = collection.insert_one({
            'collist': list(pandas_df.columns),
            'size': res_size,
            'dataset_shape': dataset_shape,
            'file_name': f.filename
        }).inserted_id
        print(inserted_id)
        resp = jsonify("user added succesfully")
        resp.status_code = 200

        return resp

    @endpoints.route("/algocolunmnames", methods=['GET'])
    # @cross_origin()
    def algo_colunm_name():

        collection = test_db.algo_details
        find_all_col_from_collection = collection.find()
        resp = jsonify("Colunm names")
        resp.status_code = 200
        return json.loads(json_util.dumps(find_all_col_from_collection))

    @endpoints.route("/post_col_name", methods=['POST'])
    @cross_origin()
    def algo_analyze_result():
        collection = test_db.algo_results
        _req = request.get_json()
        col_name = _req['colunm']
        pycaret_opt = _req['pycaretopt']
        print("im printing options")
        print(pycaret_opt)

        

        f = pd.read_pickle("./algo.pkl")

        f.to_csv(r'file.csv')

        dataset = get_data('file')

       
        

        # if pycaret_opt == "Classification":
            

        data = dataset.sample(frac=0.95, random_state=786)
        data_unseen = dataset.drop(data.index)
        data.reset_index(inplace=True, drop=True)
        data_unseen.reset_index(inplace=True, drop=True)
        print('Data for Modeling: ' + str(data.shape))
        print('Unseen Data For Predictions: ' + str(data_unseen.shape))

        exp_clf101 = setup(data=data, target= col_name)
        best_model = compare_models()
        best_model = pull()
        algo_result = best_model.values.tolist()
        algo_result_modified = best_model.to_json(orient='records')
        inserted_id = collection.insert_one({
            'analyzed_data':
            algo_result,
            'analyzed_data_modified':algo_result_modified
        }).inserted_id
        print(inserted_id)
        resp = jsonify("OK")
        resp.status_code = 200
        
        return resp

  



    @endpoints.route("/algoresults", methods=['GET'])
    # @cross_origin()
    def algo_results():

        collection = test_db.algo_results
        algo_results = collection.find()
        resp = jsonify("Colunm names")
        resp.status_code = 200
        return json.loads(json_util.dumps(algo_results))

    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx Alogrithm analyzer xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

    @endpoints.errorhandler(404)
    # @cross_origin()
    def not_found(error=None):
        message = {
            'status': 404,
            'message': 'not found' + request.url
        }
        resp = jsonify(message)
        resp.status_code = 400

    return endpoints
