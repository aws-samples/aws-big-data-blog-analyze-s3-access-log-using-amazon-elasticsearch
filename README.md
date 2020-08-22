# Analyze S3 access log using Amazon Elasticsearch Service

The AWS Big Data blog post Analyzing Amazon S3 server access logs using the Amazon Elasticsearch Service demonstrates how to analyze Amazon S3 server access log using Amazon Elasticsearch Service. The blog post creates Amazon ES cluster, Lambda function and configure event on S3 bucket to trigger Lambda function. The Lambda function reads the file, processes the access log, and sends it to Amazon ES cluster. You can use Kibana to create interactive visuals and analyze logs over a time period.

This Github project describes in detail the Lambda code components used in the blog post. 

This solution uses a Python deployment code and executes as needed. Let’s walk over the code which indexes the data to the ES cluster. The following are the high-level tasks the Python code does for you.

•	Connects to the Amazon ES domain and defines an index template that will automatically be applied to access-logs indices. 
•	Creates access-logs-index-1 index. 
•	Read and build an access log file with the format expected by _bulk index operation.
•	Writes the parsed access log file into Amazon ES.

To connect to Amazon ES, the Python code uses few libraries such as Elasticsearch, RequestsHttpConnection, and urllib. The package has these libraries installed already. The Python code has a section to connect to Amazon ES domain, validate, and create the index. The details of the code segment have been discussed in detail in Building and Maintaining an Amazon S3 Metadata blog post.  

The first step in the processing is to define the variables and index template body. Index templates body define settings and mappings that you can automatically apply when creating new indices.


```#Define the Index Template Body
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
```
       
The next step is to define methods to connect to Amazon ES cluster. Elasticsearch is configured with Fine-grained access control with internal database master user. 

``` def connectES(esEndPoint):
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
 ```
        
The following step defines the methods to create template and index. The index mapping and shard settings will by default derived from template. 


 ```def createTemplate(esClient):
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
 ```

The next step defines method to dump the index to the Elasticsearch. The code use Python helpers to bulk load data into an Elasticsearch index

 ```def indexDocElement(esClient,response):
    try:
        helpers.bulk(esClient, response)
        print("Document indexed")
    except Exception as E:
        print("Document not indexed")
        print("Error: ",E)
        exit(4)
```

The following code segment does the actual parsing of raw data and dumps it to Amazon ES. In the first step, I have defined the regular expression corresponding to the access log values in variable S3_REGEX. Then each line of the file is processed and matched with S3_REGEX to make sure the access log patterns are uniform. The processed data is aggregated in tuples using zip function and indexed into Amazon ES using the bulk method.

 ```#Connect to ES 
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
```


