# system
import os, glob, sys, subprocess
# spacy
import spacy
# spark
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark
# perso
from getpdf2 import Extraction
import getBM25 as get
from BM25page import BM25page
import Queries as q
from datetime import datetime

spark = SparkSession \
    .builder \
    .master('local[*]') \
    .appName('Spark') \
    .enableHiveSupport() \
    .getOrCreate()
#
# fullBM25 = input("Do you want to get full BM25 ?  (y/n) > ")
# if fullBM25 == 'y':
#     start=datetime.now()
#     get.getFullBM25()
#     print(f'time : {start - datetime.now()}')
#
# pageBM25 = input("Do you want to get page BM25 ?  (y/n) > ")
# if pageBM25 == 'y':
#     c = input(f"Which company to do you to query ? {dic_pdf}")
#     cni = dic_pdf[int(c)]
#
#     lstparquet = [file for file in glob.glob1(dic_path['PATHBM25'],
#                                               '*.parquet') \
#                   if cni in file.split('_')[0]]
#     print(lstparquet)
#     df_temp = spark.read.parquet(os.path.join(dic_path['PATHBM25'],
#                                 lstparquet[0]))
#     lstpage = q.TxFemAdmin(df_temp, spark)
#     get.getPageBM25(spark, cni ,lstpage,'TxFemAdmin')
#
# df_temp = spark.read.parquet(os.path.join(dic_path['PATHBM25P'],
#                                           'TxFemAdmin_LOREAL_DC_2014_FR-297.parquet'))
# df_temp.show(5)
# q.TxFemAdminSent(df_temp, spark)
# start=datetime.now()
# path = '/home/boris/Desktop/mim/pdf/ENGIE'
# print('---')
# ex = Extraction(path)
# bmp = BM25page(spark)
# # ---
# print('Loading spacy : en_core_web_sm')
# nlpEN = spacy.load('en_core_web_sm')
# print('Loading spacy : en_core_web_sm - OK.')
# print('Loading spacy : fr_core_news_sm')
# nlpFR = spacy.load('fr_core_news_sm')
# print('Loading spacy : fr_core_news_sm - OK.')
# print('Loading : stop words.')
# StopW = ex.StopW()
# print('Loading stop words : OK.')
file = 'ENGIE_DC_2014_FR.txt'
pdfpath = r'/home/boris/Desktop/mim/pdf/ENGIE'
DocTerm = bmp.DocTerm(file, pdfpath, StopW)
DocTerm = DocTerm.repartition(4).persist()
DocTerm.createOrReplaceTempView('DocTerm')
print(f'DocTerm : OK')
print(DocTerm.show(5))
# # # ---
# tf = bmp.tf()
# tf = tf.repartition(4).persist()
# tf.createOrReplaceTempView('tf')
# print(f'tf : OK')
# # #sys.exit()
# # # ---
# idf = bmp.idf(tf)
# idf = idf.repartition(4).persist()
# idf.createOrReplaceTempView('idf')
# print(f'idf : OK')
# # # ---
# tfidf = bmp.tfidf()
# tfidf = tfidf.repartition(4).persist()
# tfidf.createOrReplaceTempView('tfidf')
# print(f'tfidf : OK')
# # # ---
# tfidf_full = bmp.tfidf_full(tfidf)
# #print(tfidf_full.show())
# print(f'tfidf_full : OK')
# #break
# tfidf_full = tfidf_full.repartition(4).persist()
# tfidf_full.write.parquet('/home/boris/Desktop/mim/pdf/ENGIE/test.parquet')
# print(tfidf_full.show())
# df_temp = spark.read.parquet('/home/boris/Desktop/mim/pdf/ENGIE/test.parquet')
# res = q.TxFemAdminSent(df_temp, spark)
# t = res.select('sent_main').collect()
# print(t)
# ph = DocTerm.where(DocTerm.id_sent == 5502).collect()
# ph1 = DocTerm.where(DocTerm.id_sent == 7377).collect()
# ph2= DocTerm.where(DocTerm.id_sent == 647).collect()
# ph3 = DocTerm.where(DocTerm.id_sent == 7376).collect()
# ph4 = DocTerm.where(DocTerm.id_sent == 649).collect()
# ph5 = DocTerm.where(DocTerm.id_sent == 11372).collect()
# ph6 = DocTerm.where(DocTerm.id_sent == 11362).collect()
# ph7 = DocTerm.where(DocTerm.id_sent == 7879).collect()
# print(ph)
# print(ph1)
# print(ph2)
# print(ph3)
# print(ph4)
# print(ph5)
# print(ph6)
# print(ph7)
# print(f'time : {datetime.now() - start}')