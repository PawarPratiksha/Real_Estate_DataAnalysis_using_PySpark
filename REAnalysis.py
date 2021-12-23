import sys
from pyspark import SparkContext
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext

# Assignment1 : Display Property ID, Size, Price for given location
# Sample Output:
# (u'1500741', 1800, 324900), 
# (u'1633709', 750, 164900), 

def getProperiesForLocation(inputRDD, location):
    
    header = inputRDD.first()

    log_txt = inputRDD.filter(lambda line: line != header)


    temp_var = log_txt.map(lambda k: k.split("|"))


    log_df=temp_var.toDF(header.split("|"))




    log_df1 = log_df.withColumn("Size", log_df["Size"].cast(IntegerType()))
    log_df1 = log_df.withColumn("Price", log_df["Price"].cast(IntegerType()))

    outputRDD = log_df1.select(["Property ID","Size","Price"]).where(log_df1.Location==location)
    return outputRDD

# Assignment2 : Display Unique Locations

def getUniqueLocations(inputRDD):
    # <enter your code here> #
    temp_var = inputRDD.map(lambda k: k.split("|"))
    
    header = inputRDD.first()

    log_df=temp_var.toDF(header.split("|"))

    
    outputRDD = log_df.select("Location").distinct()
    return outputRDD

# Assignment3 : Compute Price of Property
# Sample Output Record: (u'1629848', u'Thomas County', 29000, 563565.0)

def getPropertyWithPrice(inputRDD):
    header = inputRDD.first()

    #filter out the header, make sure the rest looks correct
    #log_txt = df.filter(lambda line: line != header)
    #log_txt.take(10)

    temp_var = inputRDD.map(lambda k: k.split("|"))
    log_df=temp_var.toDF(header.split("|"))

    
    log_df = log_df.withColumnRenamed("Price SQ Ft","PriceSQFt")
    log_df = log_df.select("Property ID","Location","Price", (log_df.Size * log_df.PriceSQFt).alias("ActualPrice"))
    log_df = log_df.withColumn("Price",log_df.Price.cast("int"))
    log_df = log_df.withColumn("ActualPrice",f.round(log_df.ActualPrice.cast("Float"),0))
    log_df = log_df.where(f.col("Price").isNotNull())
    #outputRDD = log_df.sort(log_df.Price.desc())
    outputRDD=log_df.rdd
    return outputRDD


# Assignment4: Get Property ID, count, sorted in descending order of count

def getPropertyCountByLocation(inputRDD):
    header = inputRDD.first()

    #filter out the header, make sure the rest looks correct
    log_txt = inputRDD.filter(lambda line: line != header)
    #log_txt.take(10)

    temp_var = log_txt.map(lambda k: k.split("|"))
    log_df=temp_var.toDF(header.split("|"))

    outputRDD = log_df.groupBy("Location").count()
    return outputRDD

# Assignment5: Set Operations on RDD
# Get (Property ID, Location) for properties having 3 bedrooms and more than 2 bathrooms and costing less than USD 150000

def getRequiredProperties(inputRDD):
    header = inputRDD.first()

    #filter out the header, make sure the rest looks correct
    log_txt = inputRDD.filter(lambda line: line != header)
    

    temp_var = log_txt.map(lambda k: k.split("|"))
    log_df=temp_var.toDF(header.split("|"))
    log_df= log_df.withColumn("Bedrooms",f.col("Bedrooms").cast("int"))
    log_df= log_df.withColumn("Bathrooms",f.col("Bathrooms").cast("int"))

    log_df1 = log_df.select(["Property ID","Location"]).where((log_df.Price<150000)&(log_df.Bedrooms=="3"))
    log_df2 = log_df.select(["Property ID","Location"]).where((log_df.Price<150000)&(log_df.Bathrooms>2))

    outputRDD = log_df2.join(log_df1, log_df2["Property ID"] == log_df1["Property ID"], how="inner")
    return outputRDD
    

if __name__ == "__main__":
    
    # create Spark Context
    spark = SparkSession.builder.appName("RealEstateDataAnalysis").getOrCreate()
    inputFile = "dbfs:/FileStore/shared_uploads/Pratiksha.Pawar@wipro.com/realestate.txt"
    inputRDD = spark.sparkContext.textFile(inputFile)
     

    # Assignment 1: Properties for given location
    location = "La Oceana"
    propertiesByLocRDD = getProperiesForLocation(inputRDD, location)

    print("Properties for Location - %s" %location)
    print("-----------------------------------------------------------------------------------")
    print(propertiesByLocRDD.collect())

    # Assignment 2: Display Unique Locations
    uniqueLocationsRDD = getUniqueLocations(inputRDD)
    print("Unique Locations")
    print("-----------------------------------------------------------------------------------")
    print(uniqueLocationsRDD.collect())
    
    # Assignment 3: Compute Price of Property
    propertyWithPriceRDD = getPropertyWithPrice(inputRDD)
    print("Properties with Price")
    print("-----------------------------------------------------------------------------------")
    print(propertyWithPriceRDD.takeOrdered(10,key = (lambda x : -x[2])))
    
    
    # Assignment 4: Get Property ID, count, sorted in descending order of count
    propertyCountRDD = getPropertyCountByLocation(inputRDD)
    print("Properties count by Location (sorted by count)")
    print("-----------------------------------------------------------------------------------")
    print(propertyCountRDD.collect())
    
    # Assignment 5: Properties with 3 bedrooms and alleast 2 bathrooms
    reqdPropertyListRDD = getRequiredProperties(inputRDD)
    print("Properties with 3 bedrooms and atleast 2 bathrooms")
    print("-----------------------------------------------------------------------------------")
    print(reqdPropertyListRDD.collect())