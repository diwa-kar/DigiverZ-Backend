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
from flask import Flask
from http import client
from unittest import result
from dotenv import load_dotenv, find_dotenv


import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

import os
import pprint
from pymongo import MongoClient
load_dotenv(find_dotenv())
import seaborn as sns

import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit,regexp_replace,substring,to_timestamp,to_date,col
from pyspark.sql.types import DoubleType,StringType
from sklearn.preprocessing import LabelEncoder
from pyspark.sql.types import *

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.appName('First_App').getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled',True)



app = Flask(__name__)
cors = CORS(app)

password = os.environ.get("MONGODB_PWD")


connection_string = f"mongodb+srv://diwa:diwa123@cluster0.bpzxiyo.mongodb.net/?retryWrites=true&w=majority"

client = MongoClient(connection_string)

dbs = client.list_database_names()
test_db = client.testdB
collections = test_db.list_collection_names()
print(collections)


def DS_portal_API(endpoints):


    @endpoints.route("/modelbuilderresult", methods=['POST'])
    # @cross_origin()
    def MB_post():
        collection = test_db.model_builder
        _req = request.get_json()
        buy_price = _req['buyprice']
        year = _req['year']
        kms = _req['kms']
        owner = _req['owner']
        Fuel_Type_Petrol = _req['fuel_type']
        Seller_Type_Individual = _req['seller']
        Transmission_Mannual = _req['gear']
        # Fuel_Type_Diesel

        kms_f = float(kms)
        kms_driven = math.log(kms_f)

        # data for posting
        Fuel_Type_Petrol_P = Fuel_Type_Petrol
        Seller_Type_Individual_P = Seller_Type_Individual
        Transmission_Mannual_P = Transmission_Mannual

        if (Fuel_Type_Petrol == 'Petrol'):
            Fuel_Type_Petrol = 1
            Fuel_Type_Diesel = 0
        else:
            Fuel_Type_Petrol = 0
            Fuel_Type_Diesel = 1

        if (Seller_Type_Individual == 'Individual'):
            Seller_Type_Individual = 1
        else:
            Seller_Type_Individual = 0

        if (Transmission_Mannual == 'Manual'):
            Transmission_Mannual = 1
        else:
            Transmission_Mannual = 0

        with open("H:\React\Data science project\ds-portal-flask\DS_portal_API\Random_forest_regression_model.pkl", 'rb') as file:
            data = pickle.load(file)
            df = data["df"]


            rf_random = data["rf_random"]
            fig, ax = plt.subplots(1,1, figsize=(12, 7))
            plt.bar('Year', 'Buy_price', color='red', width=0.5)

            print("im head ")

            print(df.head(10))

            plt.suptitle('Salary (US$) v Country')
            plt.title('')
            plt.ylabel('Salary')
            plt.xticks(rotation=90)
            plt.savefig(r"squares.png", transparent=True)



        prediction = rf_random.predict([[buy_price, kms_driven, owner, year, Fuel_Type_Diesel,
                                  Fuel_Type_Petrol, Seller_Type_Individual, Transmission_Mannual]])
        output = round(prediction[0], 2)

        if request.method == 'POST':
            inserted_id = collection.insert_one({'Buy_price': buy_price, 'Year':  year, 'Kilometres': kms, 'Owner': owner,  'Fuel_Type': Fuel_Type_Petrol_P,
                                                'Seller_Type': Seller_Type_Individual_P, 'Transmission_Type':  Transmission_Mannual_P, 'Output': output}).inserted_id
            print(inserted_id)
            resp = jsonify("user inputs added succesfully")
            resp.status_code = 200

            return resp
        return json.loads(json_util.dumps(output))

    @endpoints.route("/modelbuilderresulthistory",)
    # @cross_origin()
    def result_history():
        collection = test_db.model_builder
        result = collection.find()
        return json.loads(json_util.dumps(result))

# xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx array csv reader for dqreport  xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx #
 
    # @endpoints.route("/dqreportcsvreader",)
    # @cross_origin()
    # def csv_reader():
    #     collections = test_db.Dq_reprot
    #     result = collections.find()
    #     return json.loads(json_util.dumps(result))

    # @endpoints.route("/dqreport", methods=['POST'])
    # @cross_origin()
    # def add_csv():
    #     print("im inside dqreport")
    #     f = request.files['file']
    #     pandas_df = pd.read_csv(f)
    #     print(pandas_df)

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

        # svm = sns.heatmap(pandas_df.isnull(),cbar=False,cmap='viridis')

        # figure = svm.get_figure()    
        # figure.savefig('svm_conf.png', dpi=400)
         
        # vars_with_na = [var for var in pandas_df.columns if pandas_df[var].isnull().sum() > 0]

        # # figure = svm.get_figure()    
        # # figure.savefig('svm_conf.png',dpi=400)
        # dataset_shape = pandas_df.shape
        # def printinfo():
        #     temp = pd.DataFrame(index=pandas_df.columns)
            
            
        #     temp['null_count'] = pandas_df.isnull().sum()
        #     temp['unique_count'] = pandas_df.nunique()
        #     temp['duplicate_count'] =pandas_df.duplicated().sum() 
    
    
        #     return temp.values.tolist()

        # collections2 = test_db.Dq_report_details
        # inserted_id = collections2.insert_one({'ColunmList': vars_with_na, 'dec':printinfo(), 'dataset_shape':dataset_shape, 'df_head':pandas_df.head().values.tolist(),'df_tail':pandas_df.tail().values.tolist(),
        # 'df_des':pandas_df.describe().values.tolist() }).inserted_id
        # print(inserted_id)
       
        
        # collections = test_db.Dq_reprot

        # collections.insert_one({
        #     "binary_field": bson.Binary(pickle.dumps(f)), })

        
        # resp = jsonify("user added succesfully")
        # resp.status_code = 200

      

        # return resp
    

    

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


if __name__ == "__main__":
    app.run(debug=True)
