import nltk
import sys
from nltk import WordNetLemmatizer
from pyspark.ml.feature import IDF, CountVectorizer
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.clustering import LDA, BisectingKMeans
from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext

import re


def map_termID_to_Word(termIndices):
    words = []
    for termID in termIndices:
        words.append(vocab_broadcast.value[termID])

    return words


def cleanup_text(record):
    text = str(record[1])
    words = text.split(" ")

    # Default list of Stopwords
    stopwords_core = ['a', u'about', u'above', u'after', u'again', u'against', u'all', u'am', u'an', u'and', u'any',
                      u'are', u'arent', u'as', u'at',
                      u'be', u'because', u'been', u'before', u'being', u'below', u'between', u'both', u'but', u'by',
                      u'can', 'cant', 'come', u'could', 'couldnt',
                      u'd', u'did', u'didn', u'do', u'does', u'doesnt', u'doing', u'dont', u'down', u'during',
                      u'each',
                      u'few', 'finally', u'for', u'from', u'further',
                      u'had', u'hadnt', u'has', u'hasnt', u'have', u'havent', u'having', u'he', u'her', u'here',
                      u'hers', u'herself', u'him', u'himself', u'his', u'how',
                      u'i', u'if', u'in', u'into', u'is', u'isnt', u'it', u'its', u'itself',
                      u'just',
                      u'll',
                      u'm', u'me', u'might', u'more', u'most', u'must', u'my', u'myself',
                      u'no', u'nor', u'not', u'now',
                      u'o', u'of', u'off', u'on', u'once', u'only', u'or', u'other', u'our', u'ours', u'ourselves',
                      u'out', u'over', u'own',
                      u'r', u're',
                      u's', 'said', u'same', u'she', u'should', u'shouldnt', u'so', u'some', u'such',
                      u't', u'than', u'that', 'thats', u'the', u'their', u'theirs', u'them', u'themselves', u'then',
                      u'there', u'these', u'they', u'this', u'those', u'through', u'to', u'too',
                      u'under', u'until', u'up',
                      u'very',
                      u'was', u'wasnt', u'we', u'were', u'werent', u'what', u'when', u'where', u'which', u'while',
                      u'who', u'whom', u'why', u'will', u'with', u'wont', u'would',
                      u'y', u'you', u'your', u'yours', u'yourself', u'yourselves']

    # Custom List of Stopwords - Add your own here
    stopwords_custom = ['blockquote', 'body', 'center', "del", 'div', 'font', 'head', ' hr ', 'block', 'align', '0px',
                        '3d',
                        'arial', 'background', 'bgcolor', ' br ', 'cellpadding', 'cellspacing', 'div', 'font', 'height',
                        'helvetica', 'href', 'img', 'valign', 'width', 'strong', 'serif', 'sans', ' alt ', 'display',
                        'src',
                        'style', ' tr ', 'tdtable', ' td ', 'tdtr', ' ef ', 'png', 'text', ' id ', 'gov',
                        'net', 'http', '.com', 'www', '.edu', 'jsp', 'html', 'span', 'nbsp', 'color']
    stopwords = stopwords_core + stopwords_custom
    stopwords = [word.lower() for word in stopwords]

    # text_out = [re.sub('[^a-zA-Z0-9]', '', word) for word in words]  # Remove special characters
    text_out = [re.sub('[^a-zA-Z]', '', word) for word in words]  # Remove special characters and numbers

    # Manually set path to NLTK location or try https://stackoverflow.com/a/26585654
    nltk.data.path.append("/home/bharat/nltk_data/")
    lmtzr = WordNetLemmatizer()
    text_out = [lmtzr.lemmatize(word.lower()) for word in text_out if
                len(word) > 2 and word.lower() not in stopwords]  # Remove stopwords and words under X length
    return text_out


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print >> sys.stderr, "Usage: spark-submit NLPLDAllocation <Location of Email dump on Hadoop> <number of topics>"
        exit(-1)

    conf = SparkConf().setAppName("NLPLDA")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    rawdata = sqlContext.read.load(sys.argv[1], format="csv", header=True, delimiter="|")

    # https://community.hortonworks.com/articles/84781/spark-text-analytics-uncovering-data-driven-topics.html

    udf_cleantext = udf(cleanup_text, ArrayType(StringType()))
    clean_text = rawdata.withColumn("words", udf_cleantext(struct([rawdata[x] for x in rawdata.columns])))

    cv = CountVectorizer(inputCol="words", outputCol="rawFeatures", vocabSize=1000)
    cvmodel = cv.fit(clean_text)
    featurizedData = cvmodel.transform(clean_text)

    vocab = cvmodel.vocabulary
    vocab_broadcast = sc.broadcast(vocab)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)

    lda = LDA(k=sys.argv[2], seed=123, optimizer="em", featuresCol="features")

    ldamodel = lda.fit(rescaledData)

    ldatopics = ldamodel.describeTopics()
    ldatopics.show(sys.argv[2])

    udf_map_termID_to_Word = udf(map_termID_to_Word, ArrayType(StringType()))
    ldatopics_mapped = ldatopics.withColumn("topic_desc", udf_map_termID_to_Word(ldatopics.termIndices))
    ldatopics_mapped.select(ldatopics_mapped.topic, ldatopics_mapped.topic_desc).show(sys.argv[2], False)
