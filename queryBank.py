from elasticsearch import Elasticsearch
import pandas as pd
import os, glob,sys
import spacy
import preprocessing as pp
es = Elasticsearch('http://localhost:9200')

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