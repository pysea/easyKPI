from elasticsearch import Elasticsearch
import pandas as pd
import os, glob,sys
import spacy
from getpdf2 import Extraction
import preprocessing as pp
es = Elasticsearch('http://localhost:9200')

ex = Extraction('/home/boris')

TxFemCon = {
  "size": 1000,
  "query": {
    "bool": {
        "must" : [
          {
           "match": {
             "sent": {
              "query": "femmes proportion",
              "operator": "and"
           }
           }
         }
       ],
       "should": [
         {"match": {
           "sent": "conseil"
         }
         },
         {"match": {
          "sent": "administration"
          }
         },
         {"match": {
          "sent": "membres"
          }
         }
       ]
     }
   }
}


def query_results(res):
    test = res['hits']['hits']
    lst_all = []
    idx = 0
    for results in test:
        # ---
        _id = results['_id']
        _score = results['_score']
        company = results['_source']['company']
        year = results['_source']['year']
        sent = results['_source']['sent']
        sent = pp.ppSent(sent)
        doc = nlpFR(sent)
        tokens = [token.text.lower() for token in doc \
                  if token.text.lower() not in stopW \
                  and token.pos_ != 'PUNCT']
        vectors = [token.vector_norm for token in doc \
                  if token.text.lower() not in stopW\
                   and token.pos_ != 'PUNCT']

        df = pd.DataFrame({'_id': _id,
                            'company': company,
                            'year':year,
                            'sent':sent,
                            '_score': _score,
                             'vectors': [vectors]})
        # print(df1.head())
        # sys.exit()
        # # print(df1.head())
        # df1.reset_index(inplace=True)
        # df = pd.DataFrame({'_id': _id,
        #                     'company': company,
        #                     'year':year,
        #                     'sent':sent,
        #                    '_score': _score},
        #                    index=[0])
        # df.reset_index(inplace=True)
        # print(df.head())
        # sys.exit()
        # df_merge = pd.merge(df,
        #                     df1,
        #                     how='outer',
        #                     left_on='index',
        #                     right_on='index')
        #print(df_merge.head())
        lst_all.append(df)

    df_all = pd.concat(lst_all,axis=0,ignore_index=False)

    print(df_all.columns)
    return df_all

def _export(df, EXPORT):
    # ---
    df.to_csv(os.path.join(EXPORT,'test.csv'),
                  sep='|', encoding='latin1')
    print('export successful.')


if __name__=='__main__':
    EXPORT = r'/home/boris/Desktop/mim/ES_query_results'
    print('Loading spacy : en_core_web_sm')
    nlpEN = spacy.load('en_core_web_sm')
    print('Loading spacy : en_core_web_sm - OK.')
    print('Loading spacy : fr_core_news_sm')
    nlpFR = spacy.load('fr_core_news_sm')
    print('Loading spacy : fr_core_news_sm - OK.')
    print('Loading : stop words.')
    lst = ['*', "d'", "l'", '/', "n'", ',', 'de', "l'"]
    stopFR = list(spacy.lang.fr.stop_words.STOP_WORDS)
    stopEN = list(spacy.lang.en.stop_words.STOP_WORDS)
    stopW = stopFR + stopEN
    stopW.extend(lst)
    stopW.append(["'",'"','#'])
    # stopW = set(stopW)
    print(stopW)
    res = es.search(index="m2_pdf_fr_gram6",
                    body=TxFemCon)
    df_fem = query_results(res)
    print(df_fem.head())
    _export(df_fem, EXPORT)