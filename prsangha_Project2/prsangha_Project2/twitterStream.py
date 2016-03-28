from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")
    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)

def make_plot(counts):
    plt.ylabel('Word Count')
    plt.xlabel('Time Step')
    plt.axis([0,11,0,300])

    y=[]
    for a in counts:
        y.append(a[0])
    x = [0,1,2,3,4,5,6,7,8,9,10,11]
    plt.plot(x,y,color='b')
    plt.plot(x,y,'b-',marker = 'o')

    y=[]
    for a in counts:
        y.append(a[1])
    plt.plot(x,y,color='r')
    plt.plot(x,y,'ro')

    plt.show()

def load_wordlist(filename):
    with open(filename, 'r') as aFile:
        aList = aFile.read().splitlines()
    return aList

def updateFunction(newvalues, runningCount):
    if runningCount is None:
        runningCount=0
    return sum(newvalues,runningCount)

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))
    tweets = tweets.flatMap(lambda line: line.split(" "))
    words = tweets.flatMap(lambda line: line.split(" "))
    tweets = tweets.filter(lambda x: x in pwords or x in nwords)
    tweets = tweets.map(lambda x: ("positive",1) if x in pwords else ("negative",1))
    tweets = tweets.reduceByKey(lambda x,y: x+y)
    tweets = tweets.updateStateByKey(updateFunction)
    tweets.pprint()

    pds = words.filter(lambda x: x in pwords)
    nds = words.filter(lambda x: x in nwords)

    plist=[]
    nlist=[]

    pds.foreachRDD(lambda t,rdd: plist.append(rdd.count()))    
    nds.foreachRDD(lambda t,rdd: nlist.append(rdd.count()))

    counts = []
  
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    for i in range(0,len(plist)):
        counts.append((plist[i],nlist[i]))

    return counts

if __name__=="__main__":
    main()
