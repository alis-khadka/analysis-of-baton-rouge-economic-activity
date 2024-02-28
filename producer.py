from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, count, expr, trim, when


spark = SparkSession.builder.appName('BigDataProject').getOrCreate()

csvBusinessInfo = './Businesses_Registered_with_EBR_Parish_20240221.csv'
dfBusiness = spark.read.csv(csvBusinessInfo, header=True, inferSchema=True)

csvAddressInfo = './Street_Address_Listing_20240226.csv'
dfAddress = spark.read.csv(csvAddressInfo, header=True, inferSchema=True)

# Filter out rows with missing or invalid data
dfBusiness = dfBusiness.filter(
    (col("PHYSICAL ADDRESS - CITY") == "BATON ROUGE") &
    (col("PHYSICAL ADDRESS - STATE") == "LA")
)

joined_df = dfBusiness.join(dfAddress, dfBusiness["ADDRESS ID"] == dfAddress["ADDRESS NO"], "inner")

# Concatenate the values of the existing columns separated by space
joined_df = joined_df.withColumn("FULL STREET NAME", 
                                concat_ws(" ", 
                                          *[trim(col(c)) for c in ["STREET PREFIX DIRECTION", 
                                                                   "STREET PREFIX TYPE", 
                                                                   "STREET NAME", 
                                                                   "STREET SUFFIX TYPE", 
                                                                   "STREET SUFFIX DIRECTION"]]))
joined_df = joined_df.where(joined_df["FULL STREET NAME"] == "BELLACOSA AVE")


# print(filteredDfBusiness.count())
# print(joined_df.count())

# Group by NAICS CATEGORY, PHYSICAL ADDRESS - CITY, and PHYSICAL ADDRESS - STATE and count occurrences
# distribution = dfNew.groupBy("NAICS CATEGORY", "PHYSICAL ADDRESS - CITY", "PHYSICAL ADDRESS - STATE").count()
distributionByState = joined_df.groupBy(
    "FULL STREET NAME",
    "NAICS CATEGORY",
).agg(count("*").alias("COUNT"))

# print(distribution.show())
print(distributionByState.show(100))

def calc_street_details_store_types(df, street_name):
  street_filtered_df = df.where(joined_df["FULL STREET NAME"] == street_name)

  stree_details = street_filtered_df.groupBy(
        "FULL STREET NAME",
        "NAICS CATEGORY",
    ).agg(
        count("*").alias("TOTAL COUNT"),
        expr("sum(case when `BUSINESS STATUS` = 'O' then 1 else 0 end)").alias("ACTIVE COUNT"),
        expr("sum(case when `BUSINESS STATUS` = 'C' then 1 else 0 end)").alias("CLOSED COUNT")
    )
