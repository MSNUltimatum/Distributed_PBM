curl -X "POST" "http://localhost:8083/connectors" \
-H "Content-Type: application/json" \
-d $'{
  "name": "hdfs-sink",
  "config": {
    "partition.duration.ms": "600000",
    "flush.size": "1000",
    "path.format": "\'year\'=YYYY/\'month\'=MM/\'day\'=dd/",
    "rotate.schedule.interval.ms": "600000",
    "connector.class": "HdfsSinkConnector",
    "locale": "en",
    "format.class": "io.confluent.connect.hdfs.string.StringFormat",
    "partitioner.class": "io.confluent.connect.hdfs.partitioner.TimeBasedPartitioner",
    "topics": "pbm_results",
    "tasks.max": "1",
    "topics.dir": "/output",
    "timezone": "UTC",
    "hdfs.url": "hdfs://namenode:8020",
    "name": "hdfs-sink"
  }
}'