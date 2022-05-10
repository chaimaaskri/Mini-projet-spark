import findspark
findspark.init


from pyspark import sparkContext
from pyspark.streaming importStreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__=="__main__":
    
    sc=SparkContext(appName="Kafka Spark Demo")

    ssc=StreamingContext(sc,60)
    
    message=KafkaUtils.createDirectStream(ssc,topics=['testtopic'],kafkaParams= {"metadata.broker.list":"localhost:9092"})

        words=message.map(lambda x: x[1]).flatMap(lambda x: x.split(" "))
  
        wordcount=words.map(lamba x: (x,1)).reduceByKey(lambda a,b: a+b)
        
        wordcount.pprint()



scc.start()
scc.awaitTermination()