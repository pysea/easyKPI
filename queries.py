import pandas as pd
import numpy as np
# spark
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark


def TxFemAdmin(df, spark):
    # ---
    df = df.select(df.id_page, df.NumPage, explode(df.VectorBM25))
    df = df.select(df.id_page, df.NumPage, explode(df.col))
    df.createOrReplaceTempView('df_temp')
    query = f"select *, (value + value2) as Score from \
            (select * from df_temp \
            where key in ('femmes')  \
            order by value desc) a \
            inner join  \
                (select id_page, key as key2, value as value2 from df_temp \
                where key like 'administration' \
                        order by value desc) b\
            on a.id_page = b.id_page \
            order by Score desc"


    df1 = spark.sql(query)
    lst = [row['NumPage'] for row in df1.collect()]
    lst = lst[:6]
    print(df1.show())
    print(df1.count())
    return lst

def TxFemAdminSent(df, spark):
    # ---
    df = df.select(df.id_page, df.NumPage, df.id_sent, df.sent_main, explode(df.VectorBM25))
    df = df.select(df.id_page, df.NumPage, df.id_sent, df.sent_main, explode(df.col))
    df.createOrReplaceTempView('df_temp')
    query = f"select distinct *, (value + value2) as Score from \
            (select * from df_temp \
            where key like 'femme%'  \
            order by value desc) a \
            inner join  \
                (select id_page as id_page2 , id_sent as id_sent2, key as key2, value as value2 from df_temp \
                where key like 'admin%' \
                        order by value desc) b\
            on a.id_sent = b.id_sent2 \
            order by Score desc"


    df1 = spark.sql(query)
    print(df1.show())
    # print(df1.count())
    return df1