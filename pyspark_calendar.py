#!/usr/bin/env python
# coding: utf-8

# ### Como utilizar PySpark no Jupyter Notebook
# ---
# 
# [Instalação PySpark](https://medium.com/designed-by-data/instalando-apache-pyspark-para-funcionar-com-jupyter-notebook-no-macos-42f992c45842)

# #### Criando sessão com Spark e bibliotecas

# In[2]:


from pyspark.sql.session import SparkSession
from pyspark import SparkContext 
sc = SparkContext.getOrCreate() 
spark = SparkSession(sc)


from pyspark.sql.functions import *

#from pyspark.sql.functions import explode, sequence, to_date

# Inputs
beginDate = '2000-01-01'
endDate = '2050-12-31'

# Range dates
calendar = spark.sql(
    f"""
    select 
        explode(sequence(to_date('{beginDate}'),
                         to_date('{endDate}'),
                         interval 1 day)
                         ) as calendar_date
    """
)

calendar = calendar\
    .withColumn('datediff_now', datediff(current_date(), col('calendar_date')))\
    .withColumn('weekday_name', date_format(col('calendar_date'), 'EEEE'))\
    .withColumn('year', year(col('calendar_date')))\
    .withColumn('month', month(col('calendar_date')))\
    .withColumn('dayofweek', dayofweek(col('calendar_date')))\
    .withColumn('dayofmonth', dayofmonth(col('calendar_date')))\
    .withColumn('dayofyear', dayofyear(col('calendar_date')))\
    .withColumn('weekofyear', weekofyear(col('calendar_date')))\
    .withColumn('quarter', quarter(col('calendar_date')))

calendar = calendar\
    .withColumn('quarter_year', concat(col('quarter'),lit('/'),col('year')))\
    .withColumn('month_year', concat(col('month'),lit('/'),col('year')))\
    .withColumn('week_year', concat(col('weekofyear'),lit('/'),col('year')))\
    .withColumn('firstdayof_month', when(col('dayofmonth') == 1, True)\
                                    .otherwise(False))\
    .withColumn('lastdayof_month', when(col('calendar_date') == last_day(col('calendar_date')), True)\
                                    .otherwise(False))\
    .withColumn('firstdayof_week', when(col('dayofweek') == 1, True)\
                                    .otherwise(False))\
    .withColumn('lastdayof_week', when(col('dayofweek') == 7, True)\
                                    .otherwise(False))\
    .withColumn('is_weekend', when(col('weekday_name').isin('Saturday','Sunday'), True)\
                                    .otherwise(False))

calendar = calendar.drop('firstdayof_week','firstdayof_month')

calendar.createOrReplaceTempView("calendar")



def read_sparkcsv(path, delimiter = ";",file_type = "csv",infer_schema = "true",first_row_is_header = "true"):
    
    """
    Função para ler arquivo csv
    """
    
    # The applied options are for CSV files. For other file types, these will be ignored.
    df = spark.read.format(file_type) \
      .option("inferSchema", infer_schema) \
      .option("header", first_row_is_header) \
      .option("sep", delimiter) \
      .load(path)\
      .dropDuplicates()
    
    return df