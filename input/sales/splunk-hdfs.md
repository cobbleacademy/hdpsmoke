# Splunk over HDFS Data Visualization

## Introduction:
The purpose is to evaluate Splunk integration with external component HDFS but Splink integration is beyond HDFS like RDBMS, Kafka and Nifi. Splunk has the ability to index, search, visualize and analyze the pre-populated data with additional lookup features. The evaluation is completed using widely available docker containers with different set of images. The containers are mainly hadoop services (namenode, datanode, resourcemanager, nodemanager and historyserver) and splunk with support of additional image containers.

Splunk datasplit logic depends on the Hadoop's InputFormat implementation, which means all the InputFormat based data is supported. However Splunk provides new datasplit generator to work wtih Hive and Parquet files. Splunk Analytics for Hadoop currently supports 4 Hive (v0.12) file format types: Textfile, RCfile, ORC files and Sequencefile.

![](splunk-hdfds-logo.png)

## HDFS Data:

### AVRO Format:

{"platform": "android", "device": "Samsung Galaxy S6", "build": 2470, "created": 1501512849, "error": "Error 7", "ip": "107.115.121.147", "type": "error", "appVersionName": "3.9"}

![](avro_hdfs_01.png)


### JSON Compressed Format:

{"customer": {"city": "AUSTIN", "zip": "78720", "firstName": "ADRIAN", "accountNumber": "901109767", "lastName": "GRIMES", "address": "6871 Walnut Grove Ave.", "phone": "2469004836", "state": "TX", "sex": "M", "age": "48"}, "timestamp": "2013-09-04T06:01:01", "servername": "cupcake.1.woc.com", "charactertype": "Milk Maid", "items": [{"category": "cheese", "itemid": "SC-MG-G10", "price": 20.0, "description": "Roquefort"}, {"category": "armor", "itemid": "DB-SG-G01", "price": 25.0, "description": "'Vegan Friendly Gloves'"}], "total": 45.0, "type": "purchase", "region": "Limburgerland"}

![](json_gz_hdfs_01.png)


## Splunk Data Visualizations using search:

### Top 20 IPs:

![](index-top-ip-avro_00.png)

![](index-ip-avro_01.png)


### Top 10 Customer Cities:

![](index-top-city-orders_03.png)

![](index-top-city-orders_00.png)

![](index-top-city-orders_01.png)

![](index-top-city-orders_02.png)


## Splunk Hadoop Jobs:

### Hadoop Jobs:

Splunk runs YARN jobs and places the index and search results into HDFS Work directory

![](yarn-job-01.png)


![](yarn-job-02.png)


## Hadoop Connection:

### Provider:

![](hadoop_provider.png)

## Index Definitions:

### AvroData index:

![](index-ip.png)

### Orders index:

![](index_orders.png)
