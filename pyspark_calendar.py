#### Criação de uma tabela calendário ####
# --.--.--.--.--.--.--.--.--.--.--.--.-- #


# Criando sessão com Spark e bibliotecas
from pyspark.sql.session import SparkSession
from pyspark import SparkContext 
sc = SparkContext.getOrCreate() 
spark = SparkSession(sc)

# Biblioteca
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

# Acrescentando novsas variáveis 
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

# Criando uma View temporária
calendar.createOrReplaceTempView("calendar")