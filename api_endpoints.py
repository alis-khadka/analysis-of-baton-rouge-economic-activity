# api_endpoints.py

import json
from datetime import datetime, date

from flask import Flask, request, jsonify
from flask_cors import CORS

from pyspark.sql import SparkSession
from pyspark.sql.functions import first, col, count, expr, to_date
from pyspark.sql.types import DateType

from cassandra.cluster import Cluster

app = Flask(__name__)

CORS(app)

# Initializing the Spark session
spark = SparkSession.builder \
    .appName("APIEndpoints") \
    .getOrCreate()

# Cassandra DB details
contact_points = ['127.0.0.1']
keyspace = 'bigdata_keyspace'
table_name = f'{keyspace}.businesses'

# Connecting Cassandra DB
cluster = Cluster(contact_points=contact_points)
session = cluster.connect()
session.set_keyspace(keyspace)

# Execute a SELECT query to fetch all records from the table
select_query = f"SELECT * FROM {table_name};"
rows = session.execute(select_query)

# Convert the result into a list of tuples
data = [(row.business_close_date, row.business_name, row.business_open_date, row.business_status, 
         row.city, row.full_street_name, row.latitude, row.longitude, row.naics_category, row.naics_code, row.naics_group, row.state) 
        for row in rows]

# Create a DataFrame using PySpark
df = spark.createDataFrame(data, 
                           ["business_close_date", "business_name", "business_open_date", "business_status", 
                            "city", "full_street_name", "latitude", "longitude", "naics_category", "naics_code", "naics_group", "state"])



@app.route("/", methods=['GET'])
def check_server_health():
    response = {
        'total_records': df.count(),
        'status': 'Data from DB has been loaded successfully!' if df.count() > 1 else 'Something went wrong while loading data from Cassandra!!!'
    }

    return jsonify(response)

@app.route('/business_categories')
def business_categories():
    distinct_groups = (df
                            .select("naics_group")
                            .distinct()
                            .orderBy("naics_group")
                        )

    response = {
        'success': True,
        'data': {
            'naics_categories': distinct_groups.rdd.map(lambda row: row[0]).collect()
        }
    }

    return jsonify(response)

@app.route('/street_names')
def streets():
    distinct_streets = (df
                            .select("full_street_name")
                            .distinct()
                            .orderBy("full_street_name")
                        )

    response = {
        'success': True,
        'data': {
            'street_names': distinct_streets.rdd.map(lambda row: {'value': row[0], 'label': row[0]}).collect()
        }
    }

    return jsonify(response)

# Types of businesses suitable for specific regions.
# group by street, sample location point of one business
# for each naics_group, calculate efective opened (active - closed)
# the naics_group which has highest value -> is the suitable shit
@app.route('/suitable_business_for_a_street', methods=['GET'])
def suitable_business_for_a_street():
    # Get query parameters from the request
    street_name = request.args.get('street_name')

    if not street_name:
        return jsonify({
            'success': False,
            'message': 'Missing "street_name" param!!!'
        })

    street_filtered_df = df.where(df["full_street_name"] == street_name)

    street_details = street_filtered_df.groupBy(
            "naics_group",
        ).agg(
            count("*").alias("total_count"),
            expr("sum(case when `business_status` = 'O' then 1 else 0 end)").alias("active_count"),
            expr("sum(case when `business_status` = 'C' then 1 else 0 end)").alias("closed_count"),
            (expr("sum(case when `business_status` = 'O' then 1 else 0 end)") - expr("sum(case when `business_status` = 'C' then 1 else 0 end)")).alias("effective_count")
        ).orderBy(col("effective_count").desc())

    # Converting filtered DataFrame to JSON response
    response = {
        'success': True,
        'data': {
            'business_data_by_groups': street_details.toJSON().map(lambda x: json.loads(x)).collect(),
            'suitable_business_data': street_details.first().asDict()
        }
    }

    return jsonify(response)

# [ Geospatial distribution of businesses over time ]
# params -> start_date and end_date
# response: business name, business status, location point, naics_group
# description: map with red dots, on hover: bigger and show details as tooltip, color code for naics_groups
@app.route('/business_distribution', methods=['GET'])
def distribution_of_businesses():
    # Get query parameters from the request
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if start_date is None:
        start_date = '2000-01-01'
    else:
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()

    if end_date is None:
        end_date = date.today()
    else:
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

    # Cast date columns to DateType
    dfNew = df.withColumn("business_open_date", col("business_open_date").cast(DateType()))

    # Filter records based on the date range
    business_distribution = (dfNew
                   .filter(
                       (col('business_open_date') >= start_date) & (col('business_open_date') <= end_date)
                    ).orderBy(
                        col('full_street_name')
                    ).select(
                        col('business_name'),
                        col('business_status'),
                        col('naics_group'),
                        col('latitude'),
                        col('longitude')
                    )
                )

    response = {
        'success': True,
        'data': {
            'business_distribution': business_distribution.toJSON().map(lambda x: json.loads(x)).collect(),
            'start_date': str(start_date),
            'end_date': str(end_date)
        }
    }

    return jsonify(response)

# Regions that have experienced economic growth or decline
# resposne: street_name, sample of location point, hover: details of total open and closed, color for growth or decline
@app.route('/economic_activity', methods=['GET'])
def economic_activity():
    economic_activities = df.groupBy(
            "full_street_name",
        ).agg(
            count("*").alias("total_count"),
            expr("sum(case when `business_status` = 'O' then 1 else 0 end)").alias("active_count"),
            expr("sum(case when `business_status` = 'C' then 1 else 0 end)").alias("closed_count"),
            (expr("sum(case when `business_status` = 'O' then 1 else 0 end)") - expr("sum(case when `business_status` = 'C' then 1 else 0 end)")).alias("effective_count"),
            first('latitude'),
            first('longitude')
        ).orderBy(col("effective_count").desc())

    # Converting filtered DataFrame to JSON response
    response = {
        'success': True,
        'data': {
            'economic_activities': economic_activities.toJSON().map(lambda x: json.loads(x)).collect()
        }
    }

    return jsonify(response)

