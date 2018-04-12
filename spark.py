import json
from kafka import KafkaConsumer
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import nltk

lemmatizer = nltk.WordNetLemmatizer()
stemmer = nltk.stem.porter.PorterStemmer()
grammar = r"""
            NP:
                {<DT|PP\$>?<JJ>+<NN>}
                {<NNP>+}
        """
chunker = nltk.RegexpParser(grammar)

from nltk.corpus import stopwords
stopwords = stopwords.words('english')


#spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.3.0,org.elasticsearch:elasticsearch-hadoop:6.2.2 spark.py
def elasticWrite(entities):

    ent_rdd = entities.map(json.dumps)

    ent_rdd.pprint()

    #Broken here
    ent_rdd.foreachRDD(lambda rdd: rdd.saveAsNewAPIHadoopDataset(
            path="-",
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable",
            valueClass="org.elasticsearch.hadoop.mr.MapWritable",
            conf={
                "es.resource" : "meetup-spark-mar-13/spark",
                # "es.input.json" : "true",
                "es.net.http.auth.user": "elastic",
                "es.net.http.auth.pass": "9crfG&f_$$gDepA4Ys~#"
            }))

def syns(word):
    from nltk.corpus import wordnet as wn

    try:
        return wn.synsets(word.replace(" ", "_"))[0].lemma_names()
    except:
        return list()

def extractEntities(json):
    text = json['description']
    toks = nltk.word_tokenize(text)
    postoks = nltk.tag.pos_tag(toks)
    tree = chunker.parse(postoks)


    def leaves(tree):
        """Finds NP (nounphrase) leaf nodes of a chunk tree."""
        for subtree in tree.subtrees(filter = lambda t: t.label()=='NP'):
            yield subtree.leaves()

    def normalise(word):
        """Normalises words to lowercase and stems and lemmatizes it."""
        word = word.lower()
        word = stemmer.stem(word)
        word = lemmatizer.lemmatize(word)
        return word

    def acceptable_word(word):
        """Checks conditions for acceptable word: length, stopword."""
        accepted = bool(3 <= len(word) <= 40
                        and word.lower() not in stopwords)
        return accepted


    def get_terms(tree):
        for leaf in leaves(tree):
            term = [ w for w,t in leaf if acceptable_word(w) ]
            yield term

    terms = get_terms(tree)

    json['terms'] = []
    for term in terms:
        phrase = ' '.join(term)
        if phrase.__len__() == 0:
            continue
        phrase = phrase.replace(".", "")
        json['terms'].append(phrase.lower())
        syn = syns(phrase)

        for s in syn:
            json['terms'].append(s.replace("_", " ").lower())

    #print json

    return json


def main():
    con = SparkConf()
    sc = SparkContext(conf=con)
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint("checkpoint")
    kstream = KafkaUtils.createDirectStream(ssc, topics = ['events'], kafkaParams = {'metadata.broker.list': 'localhost:9092'})
    meetups = kstream.map(lambda line: json.loads(line[1]))
    entities = meetups.filter(lambda line: line is not None and line.has_key('description')).map(lambda line: extractEntities(line)) #array of entities
    #elasticWrite(entities)
    print entities


    ssc.start()
    ssc.awaitTermination()
    
if __name__=="__main__":

    main()