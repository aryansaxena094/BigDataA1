import csv
import os
import sys
# Spark imports
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
# Dask imports
import dask.bag as db
import dask.dataframe as df  # you can use Dask bags or dataframes
from csv import reader

'''
INTRODUCTION

The goal of this assignment is to implement a basic analysis of textual 
data using Apache Spark (http://spark.apache.org) and 
Dask (https://dask.org). 
'''

'''
DATASET

We will study a dataset provided by the city of Montreal that contains 
the list of trees treated against the emerald ash borer 
(https://en.wikipedia.org/wiki/Emerald_ash_borer). The dataset is 
described at 
http://donnees.ville.montreal.qc.ca/dataset/frenes-publics-proteges-injection-agrile-du-frene 
(use Google translate to translate from French to English). 

We will use the 2015 and 2016 data sets available in directory `data`.
'''

'''
HELPER FUNCTIONS

These functions are here to help you. Instructions will tell you when
you should use them. Don't modify them!
'''

#Initialize a spark session.
def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark

#Useful functions to print RDDs and Dataframes.
def toCSVLineRDD(rdd):
    '''
    This function convert an RDD or a DataFrame into a CSV string
    '''
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row]))\
           .reduce(lambda x,y: os.linesep.join([x,y]))
    return a + os.linesep

def toCSVLine(data):
    '''
    Convert an RDD or a DataFrame into a CSV string
    '''
    if isinstance(data, RDD):
        return toCSVLineRDD(data)
    elif isinstance(data, DataFrame):
        return toCSVLineRDD(data.rdd)
    return None

'''
Plain PYTHON implementation

To get started smoothly and become familiar with the assignment's 
technical context (Git, GitHub, pytest, GitHub actions), we will implement a 
few steps in plain Python.
'''

def rddsep(s):
    sep = ','
    q = '"'
    k = ''
    csvl = []
    inq = False
    for c in s:
        if c == sep  and not inq:
            csvl.append(k)
            k = ''
        else:
            k += c
            if c == q:
                inq = False if inq else True
    csvl.append(k)
    return csvl

#Python answer functions
def count(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the number of trees (non-header lines) in
    the data file passed as first argument.
    Test file: tests/test_count.py
    Note: The return value should be an integer
    '''
    with open(filename,'r') as csvfile:
        header = csvfile.readline().strip()
        count = sum(1 for row in csvfile)
        return count
    # ADD YOUR CODE HERE
    raise NotImplementedError

def parks(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks.py
    Note: The return value should be an integer
    '''
    with open(filename, 'r') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)
        Nom_park_index = header.index('Nom_parc')
        parkcount = 0
        for row in reader:
            if row[Nom_park_index]:
                parkcount += 1
    return parkcount
    # ADD YOUR CODE HERE
    raise NotImplementedError

def uniq_parks(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the list of unique parks where trees
    were treated. The list must be ordered alphabetically. Every element in the list must be printed on
    a new line.
    Test file: tests/test_uniq_parks.py
    Note: The return value should be a string with one park name per line
    '''
    parklist = []
    parkstr = ""

    with open(filename, encoding="utf-8") as csvfile:
        csvreader = reader(csvfile)
        header = next(csvreader)
        Nom_park_index = header.index('Nom_parc')

        for row in csvreader:
            if (row[Nom_park_index]!=""):
                parklist.append(row[Nom_park_index])

        for parks in list(dict.fromkeys(sorted(parklist))):
            parkstr = parkstr + parks + "\n"
    return parkstr

def uniq_parks_counts(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that counts the number of trees treated in each park
    and prints a list of "park,count" pairs in a CSV manner ordered
    alphabetically by the park name. Every element in the list must be printed
    on a new line.
    Test file: tests/test_uniq_parks_counts.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''
    parklist = {}
    parkstr = ""

    with open(filename, encoding="utf-8") as csvfile:
        csvreader = reader(csvfile)
        header = next(csvreader)
        Nom_park_index = header.index('Nom_parc')
        
        for row in csvreader:
            park_name = row[Nom_park_index]
            if park_name != "" and not park_name in parklist:
                parklist[park_name] = 1
            elif park_name != "" and park_name in parklist:
                parklist[park_name] += 1
        
    parklist = dict(sorted(parklist.items()))

    for i, j in parklist.items():
        parkstr = parkstr + str(i) + "," + str(j) + "\n"
    return parkstr

def frequent_parks_count(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the list of the 10 parks with the
    highest number of treated trees. Parks must be ordered by decreasing
    number of treated trees and by alphabetical order when they have similar number.
    Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''
    parkdict = {}
    parkstr = ""

    with open(filename, encoding="utf-8") as csvfile:
        csvreader = reader(csvfile)
        header = next(csvreader)
        Nom_park_index = header.index('Nom_parc')
        
        for row in csvreader:
            park_name = row[Nom_park_index]
            if park_name != "" and not park_name in parkdict:
                parkdict[park_name] = 1
            elif park_name != "" and park_name in parkdict:
                parkdict[park_name] += 1

        freqpark = sorted(parkdict.items(), key = lambda x: (-x[1],x[0]))[0:10]

        for i, j in freqpark:
            parkstr = parkstr + str(i) + "," + str(j) + "\n" 

        return parkstr

def intersection(filename1, filename2):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the alphabetically sorted list of
    parks that had trees treated both in 2016 and 2015. Every list element
    must be printed on a new line.
    Test file: tests/test_intersection.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''
    parkdict1 = {}
    parkdict2 = {}
    parkstr = ""

    with open(filename1, encoding="utf-8") as csvfile:
        csvreader = reader(csvfile)
        header = next(csvreader)
        Nom_park_index = header.index('Nom_parc')
        
        for row in csvreader:
            park_name = row[Nom_park_index]
            if park_name != "" and not park_name in parkdict1:
                parkdict1[park_name] = 1
            elif park_name != "" and park_name in parkdict1:
                parkdict1[park_name] += 1

    with open(filename2, encoding="utf-8") as csvfile:
        csvreader = reader(csvfile)
        header = next(csvreader)
        Nom_park_index = header.index('Nom_parc')
        
        for row in csvreader:
            park_name = row[Nom_park_index]
            if park_name != "" and not park_name in parkdict2:
                parkdict2[park_name] = 1
            elif park_name != "" and park_name in parkdict2:
                parkdict2[park_name] += 1
    
    for common in sorted(parkdict1.keys() & parkdict2.keys()):
        parkstr = parkstr + common + "\n"
    
    return parkstr

'''
SPARK RDD IMPLEMENTATION

You will now have to re-implement all the functions above using Apache 
Spark's Resilient Distributed Datasets API (RDD, see documentation at 
https://spark.apache.org/docs/latest/rdd-programming-guide.html). 
Outputs must be identical to the ones obtained above in plain Python. 
However, all operations must be re-implemented using the RDD API, you 
are not allowed to simply convert results obtained with plain Python to 
RDDs (this will be checked). Note that the function *toCSVLine* in the 
HELPER section at the top of this file converts RDDs into CSV strings.
'''

# RDD functions

def count_rdd(filename):
    '''
    Write a Python script using RDDs that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_rdd.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    rddpyspark = spark.sparkContext.textFile(filename)
    header = rddpyspark.first()
    Nom_parc_index = header.index("Nom_parc")
    count = rddpyspark.filter(lambda x : x[Nom_parc_index] != '').filter(lambda x: x != header).count()
    return count
    # ADD YOUR CODE HERE
    raise NotImplementedError

def parks_rdd(filename):
    '''
    Write a Python script using RDDs that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_rdd.py
    Note: The return value should be an integer
    '''

    spark = init_spark()

    rdd = spark.sparkContext.textFile(filename).mapPartitions(lambda x: reader(x, delimiter=','))

    header = rdd.first()

    rddfilter = rdd.filter(lambda row: row[6] != "")

    rddfilter = rddfilter.filter(lambda row: row != header)

    rddparks = rddfilter.map(lambda row: (row[6], 1))

    rddparkc = rddparks.reduceByKey(lambda x, y: x + y)

    totaltrees = rddparkc.map(lambda row: row[1]).reduce(lambda x, y: x + y)

    return totaltrees

def uniq_parks_rdd(filename):
    '''
    Write a Python script using RDDs that prints the list of unique parks where
    trees were treated. The list must be ordered alphabetically. Every element
    in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_rdd.py
    Note: The return value should be a CSV string
    '''

    parkstr = ""
    spark = init_spark()

    data = spark.sparkContext.textFile(filename)
    header = data.first()

    data = data.filter(lambda x: x!=header)

    csvdata = data.map(lambda x: rddsep(x))
    
    data2 = csvdata.map(lambda x: (x[6]))

    parkdata = data2.filter(bool)

    parkdata = parkdata.map(lambda x: x[1:-1] if x.startswith('"') and x.endswith('"') else x)

    uniqparks = parkdata.distinct()
    uniqparks = uniqparks.sortBy(lambda x: x)

    uniqparkslist = uniqparks.collect()

    for p in uniqparkslist: 
        parkstr = parkstr + p + "\n"
    
    return parkstr

def uniq_parks_counts_rdd(filename):
    '''
    Write a Python script using RDDs that counts the number of trees treated in
    each park and prints a list of "park,count" pairs in a CSV manner ordered
    alphabetically by the park name. Every element in the list must be printed
    on a new line.
    Test file: tests/test_uniq_parks_counts_rdd.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''
    parkstr = ""
    
    spark = init_spark()
    
    data = spark.sparkContext.textFile(filename)

    header = data.first()

    data = data.filter(lambda x: x!=header)

    csvdata = data.map(lambda x: rddsep(x))

    parksdata = csvdata.map(lambda x: (x[6]))

    parksdata = parksdata.filter(bool)

    parksdata = parksdata.map(lambda x: x[1: -1] if x.startswith('"') and x.endswith('"') else x)

    parksdata = parksdata.map(lambda x: (x, 1))

    parksdata = parksdata.reduceByKey(lambda x, y: x + y)

    parksdatasorted = parksdata.sortBy(lambda x: x[0])

    uniqparks = parksdatasorted.collect()

    for p, c in uniqparks:
        parkstr = parkstr + p + ',' + str(c) + "\n"
    
    return parkstr


def frequent_parks_count_rdd(filename):
    '''
    Write a Python script using RDDs that prints the list of the 10 parks with
    the highest number of treated trees. Parks must be ordered by decreasing
    number of treated trees and by alphabetical order when they have similar
    number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_rdd.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    parkstr = ""

    spark = init_spark()

    data = spark.sparkContext.textFile(filename)

    header = data.first()

    data = data.filter(lambda x: x != header)

    csvdata = data.map(lambda x: rddsep(x))

    data2 = csvdata.map(lambda x: (x[6]))

    parksdata = data2.filter(bool)

    parksdata = parksdata.map(lambda x: x[1: -1] if x.startswith('"') and x.endswith('"') else x)

    parkscountdata = parksdata.map(lambda x: (x, 1))

    parkscountdata = parkscountdata.reduceByKey(lambda x, y: x + y)

    parkscountdatasorted = parkscountdata.sortBy(lambda x: (-x[1], x[0]))

    parkscountdatasorted = parkscountdatasorted.collect()[:10]

    for p, c in parkscountdatasorted:
        parkstr = parkstr + p + ',' + str(c) + '\n'

    return parkstr

def intersection_rdd(filename1, filename2):
    '''
    Write a Python script using RDDs that prints the alphabetically sorted list
    of parks that had trees treated both in 2016 and 2015. Every list element
    must be printed on a new line.
    Test file: tests/test_intersection_rdd.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''
    parkstr = ""

    spark = init_spark()
    data1 = spark.sparkContext.textFile(filename1)
    header1 = data1.first()
    data1 = data1.filter(lambda x: x != header1)

    csvdata1 = data1.map(lambda x: rddsep(x))
    treedata1= csvdata1.map(lambda x: (x[6]))
    
    parksdata1 = treedata1.filter(bool)
    parksdata1 = parksdata1.map(lambda x: x[1:-1] if x.startswith('"') and x.endswith('"') else x)

    data2 = spark.sparkContext.textFile(filename2)
    header2 = data2.first()
    data2 = data2.filter(lambda x: x != header2)
    
    csvdata2 = data2.map(lambda x: rddsep(x))
    treedata2= csvdata2.map(lambda x: (x[6]))
    
    parksdata2 = treedata2.filter(bool)
    parksdata2 = parksdata2.map(lambda x: x[1:-1] if x.startswith('"') and x.endswith('"') else x)

    commonpark = parksdata1.intersection(parksdata2)
    commonpark = commonpark.sortBy(lambda x: x)
    commonpark = commonpark.collect()

    for p in commonpark:
        parkstr = parkstr + p + '\n'
    return parkstr


'''
SPARK DATAFRAME IMPLEMENTATION

You will now re-implement all the tasks above using Apache Spark's 
DataFrame API (see documentation at 
https://spark.apache.org/docs/latest/sql-programming-guide.html). 
Outputs must be identical to the ones obtained above in plain Python. 
Note: all operations must be re-implemented using the DataFrame API, 
you are not allowed to simply convert results obtained with the RDD API 
to Data Frames. Note that the function *toCSVLine* in the HELPER 
section at the top of this file also converts DataFrames into CSV 
strings.
'''

# DataFrame functions

def count_df(filename):
    '''
    Write a Python script using DataFrames that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_df.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    dfpyspark = spark.read.csv(filename,header = True, inferSchema=True)
    count = dfpyspark.select('Nom_parc').count()
    return count
    # ADD YOUR CODE HERE
    raise NotImplementedError

def parks_df(filename):
    '''
    Write a Python script using DataFrames that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_df.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    dfpyspark = spark.read.csv(filename, header = True, inferSchema=True)
    notnull = dfpyspark[dfpyspark["Nom_parc"].isNotNull()].select("Nom_parc").count()
    return notnull
    # ADD YOUR CODE HERE
    raise NotImplementedError

def uniq_parks_df(filename):
    '''
    Write a Python script using DataFrames that prints the list of unique parks
    where trees were treated. The list must be ordered alphabetically. Every
    element in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_df.py
    Note: The return value should be a CSV string
    '''

    spark = init_spark()
    dfpyspark = spark.read.csv(filename, header = True, inferSchema=True)
    notnull = dfpyspark[dfpyspark["Nom_parc"].isNotNull()].select("Nom_parc").drop_duplicates()
    notnull_sorted = notnull.orderBy("Nom_parc")
    return toCSVLine(notnull_sorted)
    # ADD YOUR CODE HERE
    raise NotImplementedError

def uniq_parks_counts_df(filename):
    '''
    Write a Python script using DataFrames that counts the number of trees
    treated in each park and prints a list of "park,count" pairs in a CSV
    manner ordered alphabetically by the park name. Every element in the list
    must be printed on a new line.
    Test file: tests/test_uniq_parks_counts_df.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    spark = init_spark()
    dfpyspark = spark.read.csv(filename, header = True, inferSchema=True)
    notnull = dfpyspark[dfpyspark["Nom_parc"].isNotNull()].select("Nom_parc").drop_duplicates()
    notnull = dfpyspark.groupBy("Nom_parc").count()
    notnull = notnull.na.drop()
    notnull_sorted = notnull.orderBy("Nom_parc")
    return toCSVLine(notnull_sorted)
    # ADD YOUR CODE HERE
    raise NotImplementedError

def frequent_parks_count_df(filename):
    '''
    Write a Python script using DataFrames that prints the list of the 10 parks
    with the highest number of treated trees. Parks must be ordered by
    decreasing number of treated trees and by alphabetical order when they have
    similar number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_df.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    spark = init_spark()
    dfpyspark = spark.read.csv(filename, header = True, inferSchema=True)
    notnull = dfpyspark[dfpyspark["Nom_parc"].isNotNull()].select("Nom_parc").drop_duplicates()
    notnull = dfpyspark.groupBy("Nom_parc").count().na.drop()
    notnull_sorted = notnull.orderBy(desc("Count")).limit(10)
    return toCSVLine(notnull_sorted)
    # ADD YOUR CODE HERE
    raise NotImplementedError

def intersection_df(filename1, filename2):
    '''
    Write a Python script using DataFrames that prints the alphabetically
    sorted list of parks that had trees treated both in 2016 and 2015. Every
    list element must be printed on a new line.
    Test file: tests/test_intersection_df.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    spark = init_spark()
    pyspfile1 = spark.read.csv(filename1, header=True, inferSchema=True)
    pyspfile2 = spark.read.csv(filename2, header=True, inferSchema=True)
    treesfile1 = pyspfile1[pyspfile1["Nom_parc"].isNotNull()].select("Nom_parc").drop_duplicates()
    treesfile2 = pyspfile2[pyspfile2["Nom_parc"].isNotNull()].select("Nom_parc").drop_duplicates()
    ans = treesfile1.intersect(treesfile2)
    ans = ans.orderBy("Nom_parc")
    return toCSVLine(ans)
    # ADD YOUR CODE HERE
    raise NotImplementedError

'''
DASK IMPLEMENTATION

You will now re-implement all the tasks above using Dask (see 
documentation at http://docs.dask.org/en/latest). Outputs must be 
identical to the ones obtained previously. Note: all operations must be 
re-implemented using Dask, you are not allowed to simply convert 
results obtained with the other APIs.
'''

# Dask functions

def count_dask(filename):
    '''
    Write a Python script using Dask that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_dask.py
    Note: The return value should be an integer
    '''
    dd1 = df.read_csv(filename, dtype={'Nom_parc': 'str'})
    return len(dd1)
    # ADD YOUR CODE HERE
    raise NotImplementedError

def parks_dask(filename):
    '''
    Write a Python script using Dask that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_dask.py
    Note: The return value should be an integer
    '''
    dd1 = df.read_csv(filename, dtype={'Nom_parc': 'str'})
    dd2 = dd1.compute()
    return int(dd2.Nom_parc.count())
    # ADD YOUR CODE HERE
    raise NotImplementedError

def uniq_parks_dask(filename):
    '''
    Write a Python script using Dask that prints the list of unique parks
    where trees were treated. The list must be ordered alphabetically. Every
    element in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_dask.py
    Note: The return value should be a CSV string
    '''
    parkstr = ""
    dfdask = df.read_csv(filename, usecols = ["Nom_parc"], dtype = { "Nom_parc": str })

    dfdask = dfdask.dropna(subset=['Nom_parc'], how = 'all')

    parkswithoutdup = dfdask.drop_duplicates(subset = ['Nom_parc'])
    
    parkssorted = parkswithoutdup.sort_values("Nom_parc", ascending = True)

    parkssorted = parkssorted.compute()
    
    for _, row in parkssorted.iterrows():
        parkstr += row["Nom_parc"] + "\n"

    return parkstr

def uniq_parks_counts_dask(filename):
    '''
    Write a Python script using Dask that counts the number of trees
    treated in each park and prints a list of "park,count" pairs in a CSV
    manner ordered alphabetically by the park name. Every element in the list
    must be printed on a new line.
    Test file: tests/test_uniq_parks_counts_dask.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    parkstr = ""

    parks = df.read_csv(filename, usecols = ["Nom_parc"], dtype = { "Nom_parc": str, })

    parks = parks.dropna(subset=['Nom_parc'], how = 'all')

    parks = parks.sort_values("Nom_parc", ascending = True)

    parkcounts = parks["Nom_parc"].value_counts(sort = False)
    
    parkcounts = parkcounts.reset_index()

    parkcounts = parkcounts.compute()

    parkcounts.columns = ["Nom_parc", "counts"]

    for _, row in parkcounts.iterrows():
        parkstr += row["Nom_parc"] + "," + str(row["counts"]) + "\n"
    
    return parkstr

    

def frequent_parks_count_dask(filename):
    '''
    Write a Python script using Dask that prints the list of the 10 parks
    with the highest number of treated trees. Parks must be ordered by
    decreasing number of treated trees and by alphabetical order when they have
    similar number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_dask.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    parkstr = ""

    parks = df.read_csv(filename, usecols = ["Nom_parc"], dtype = { "Nom_parc": str, })
    
    parks = parks.dropna(subset=['Nom_parc'], how = 'all')

    parks = parks.sort_values("Nom_parc", ascending = True)
    
    parkswithcount = parks['Nom_parc']

    parkswithcount = parkswithcount.value_counts(sort = True, ascending = False)
    
    parkswithcount = parkswithcount.reset_index()
    
    parkswithcount.columns = ['Nom_parc','counts']

    parkswithcount.sort_values(by = "counts", key = lambda x: (-x[1], x[0]))

    parkswithcount = parkswithcount.compute()
    freqparks = parkswithcount.head(10)

    for _, row in freqparks.iterrows():
        parkstr += row["Nom_parc"] + "," + str(row["counts"]) + "\n"
    
    return parkstr




def intersection_dask(filename1, filename2):
    '''
    Write a Python script using Dask that prints the alphabetically
    sorted list of parks that had trees treated both in 2016 and 2015. Every
    list element must be printed on a new line.
    Test file: tests/test_intersection_dask.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    parkstr = ""
    parks = df.read_csv(filename1, usecols = ["Nom_parc"], dtype = { "Nom_parc": str, }).dropna(subset=['Nom_parc'], how = 'all')

    parknotdup = parks.drop_duplicates(subset = ['Nom_parc'])

    parks2 = df.read_csv(filename2, usecols = ["Nom_parc"], dtype = { "Nom_parc": str, }).dropna(subset=['Nom_parc'], how = 'all')

    park2notdup = parks2.drop_duplicates(subset = ['Nom_parc'])

    commonparks = df.merge(parknotdup, park2notdup, how = 'inner', on = ['Nom_parc'])

    for _, row in commonparks.iterrows():
        parkstr += row["Nom_parc"] + "\n"

    return parkstr