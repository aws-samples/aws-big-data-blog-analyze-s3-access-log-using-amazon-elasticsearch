from __future__ import print_function
from pprint import pprint
import boto3
import json
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch import helpers
import re
import os
import urllib
import json
from datetime import datetime

s3 = boto3.client('s3')
es_client=boto3.client('es')
endpoint_name=os.environ['DOMAINNAME']

#Define Amazon ES Index and Template name
index_name='access-logs-index-1'
template_name='access_logs_template'

#Define the Index Template Body
template_body= {
                  "index_patterns": ["access-log-index*", "access-logs-index*"],
                  "settings": {
                    "number_of_shards": 1
                  },
                  "mappings": {
                    "properties": {
                      "remoteip": {
                        "type": "ip"
                      },
                      "requestdatetime": {
                        "type": "date",
                        "format": "dd/MMM/yyyy:HH:mm:ss Z"
                      }
                    }
                  }
                }


def connectES(esEndPoint):
    print ('Connecting to the ES Endpoint {0}'.format(esEndPoint))
    try:
        esClient = Elasticsearch(
            hosts=[{'host': esEndPoint, 'port': 443}],
            use_ssl=True,
            http_auth=('adminuser01', 'StrongP@ssw0rd'),
            verify_certs=True,
            connection_class=RequestsHttpConnection)
        return esClient
    except Exception as E:
        print("Unable to connect to {0}".format(esEndPoint))
        print(E)
        exit(1)
        

def createTemplate(esClient):
    try:
        res = esClient.indices.exists_template(template_name)
        if res is False:
            esClient.indices.put_template(template_name, body=template_body)
        return 1
    except Exception as E:
            print("Unable to Create Template {0}".format(template_name))
            print(E)
            exit(2)

def createIndex(esClient):
    try:
        res = esClient.indices.exists(index_name)
        if res is False:
            esClient.indices.create(index_name)
        return 1
    except Exception as E:
            print("Unable to Create Index {0}".format(index_name))
            print(E)
            exit(3)

def indexDocElement(esClient,response):
    try:
        helpers.bulk(esClient, response)
        print("Document indexed")
    except Exception as E:
        print("Document not indexed")
        print("Error: ",E)
        exit(4)

def lambda_handler(event, context):
    #Connect to ES 
    esClient = connectES(endpoint_name)
    #Create Index Template 
    createTemplate(esClient)
    #Create Index
    createIndex(esClient)
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    
    try:
        ### S3 access log format keys
        S3_KEYS = ["bucketowner","bucket","requestdatetime","remoteip","requester","requestid","operation","key","requesturi_operation","requesturi_key","requesturi_httpprotoversion","httpstatus","errorcode","bytessent","objectsize","totaltime","turnaroundtime","referrer","useragent","versionid","hostid","sigv","ciphersuite","authtype","endPoint","tlsversion"]
        ### S3 access log format regex
        S3_REGEX = '([^ ]*) ([^ ]*) \\[(.*?)\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) \\\"([^ ]*) ([^ ]*) (- |[^ ]*)\\\" (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\") ([^ ]*)(?: ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*))?.*$'
        R = re.compile(S3_REGEX)
        response = s3.get_object(Bucket=bucket, Key=key)
        accesslogdata = response['Body'].read().decode('utf-8')
        lines = accesslogdata.splitlines()
        
        bulk_ind_doc=[]
        #Scan and process each line
        for line in lines:
            match = R.match(line)
            values = match.groups(0)
            doc = dict(zip(S3_KEYS, values))
            #Prepare documents for bulk indexing
            bulk_ind_doc.append({"_index": index_name,  "_source" : doc})    
            
        #Bulk index the documents
        indexDocElement(esClient,bulk_ind_doc)
        
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e


