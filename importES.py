# system
import os, glob, sys, subprocess
import pandas as pd
import numpy as np
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
# elastic search
from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')


def importModel1(lstfiles, N):
    nb = len(lstfiles)
    inbound = nb/2
    ct = 200
    for file in lstfiles:
        print('---')
        print(f'file : {file} - {ct}/{nb}')
        print('Please wait...')
        try:
            df = pd.read_csv(os.path.join(PDFFR, file),
                         sep='\n')
            file1 = open("/home/boris/Desktop/mim/logs/imported.txt", "a")  # append mode
            file1.write(f"{file} \n")
        except:
            print('could not load df')
            file1 = open("/home/boris/Desktop/mim/logs/notimported.txt", "a")  # append mode
            file1.write(f"{file} \n")
            continue

        # 1. Create list of sent
        lst_sent = []
        for tup in df.itertuples():
            if not isinstance(tup[1],float):
                words = tup[1].split(" ")
                if len(words) > 5:
                    lst_sent.append(tup[1])

        # 2. Create sent grams
        N = 5
        sent_grams = [lst_sent[i:i + N] \
                      for i in range(len(lst_sent) - N + 1)]
        print(len(sent_grams))
        # 3. Load to ES.
        sys.exit()
        for idx, grams in enumerate(sent_grams):
            dic = {
                'idx_sent': idx,
                'company':  file.split('_')[0],
                'type':     file.split('_')[1],
                'year':     file.split('_')[2],
                'lang':     file.split('_')[3][:-4],
                'sent':     ' '.join(grams)
            }
            es.index(index="raw_pdf_fr_gram5",
                     body=dic)

        ct += 1
        print('Done')

    print('Prg successful.')

def importModel2(lstfiles, N):
    nb = len(lstfiles)
    inbound = nb / 2
    ct = 0
    for file in lstfiles:
        print('---')
        print(f'file : {file} - {ct}/{nb}')
        print('Please wait...')
        try:
            df = pd.read_csv(os.path.join(PDFFR, file),
                             sep='\n')
            file1 = open("/home/boris/Desktop/mim/logs/imported.txt", "a")  # append mode
            file1.write(f"{file} \n")
        except:
            print('could not load df')
            file1 = open("/home/boris/Desktop/mim/logs/notimported.txt", "a")  # append mode
            file1.write(f"{file} \n")
            continue

        # 1. Create list of sent
        lst_sent = []
        counter = 0
        lst_inter = []
        for tup in df.itertuples():
            if not isinstance(tup[1], float):
                words = tup[1].split(" ")
                if len(words) > 5:
                    if counter < N:
                        lst_inter.append(tup[1])
                        counter += 1
                    else:
                        lst_sent.append(lst_inter)
                        lst_inter = []
                        counter = 0
        # print(f'len(lst_sent) : {len(lst_sent)}')

        # 2. Finding joining sent
        lst_join = []
        for idx, ngram in enumerate(lst_sent):
            if idx < len(lst_sent)-1:
                last_two = ngram[3:]
                first_two = lst_sent[idx+1][:3]
                # print(ngram)
                # print(last_two)
                # print(lst_sent[idx+1])
                # print(first_two)
                # sys.exit()
                last_two.extend(first_two)
                lst_join.append(last_two)
            else:
                break

        # print(lst_join)
        # 3. Adding join sent to list
        # print(f'len(lst_join) : {len(lst_join)}')
        lst_sent.extend(lst_join)
        print(f'\t> nb sent : {len(lst_sent)}')
        for idx, grams in enumerate(lst_sent):
            dic = {
                'idx_sent': idx,
                'company':  file.split('_')[0],
                'type':     file.split('_')[1],
                'year':     file.split('_')[2],
                'lang':     file.split('_')[3][:-4],
                'sent':     ' '.join(grams)
            }
            es.index(index="m2_pdf_fr_gram6",
                     body=dic)

        ct += 1
        print('Done')
        # sys.exit()



if __name__ == '__main__':
    ######################################################
    PDFFR = '/home/boris/Desktop/mim/pdf_fr/pdf_txt'
    # ex = Extraction(PDFFR)
    # ex.convertPages()
    # sys.exit()
    #####################################################
    # file1 = open("/home/boris/Desktop/mim/logs/imported.txt", "r")
    # file1 = file1.readlines()
    # file1 = [file.replace(' \n', '') for file in file1]
    # file2 = open("/home/boris/Desktop/mim/logs/notimported.txt", "r")
    # file2 = file2.readlines()
    # file2 = [file.replace(' \n', '') for file in file2]
    # file1.extend(file2)
    # print(file1)
    # # sys.exit()
    # print('Starting ES import...')
    lstfiles = [file for file in glob.glob1(PDFFR, '*.txt')]
                # if file not in file1]
    # importPdfSent(lstfiles, 5)
    importModel2(lstfiles, 6)
    print('import successful')