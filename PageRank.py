# https://github.com/amplab/graphx/blob/master/python/examples/pagerank.py
import csv
import sys
from operator import add

from pyspark import SparkConf, SparkContext

def computeContribs(nodes, rank):
    num_nodes = len(nodes)
    for node in nodes: yield (node, rank / num_nodes)


def parseNeighbors(nodes):
    parts = nodes.split(',')
    return parts[1], parts[2]


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print >> sys.stderr, "Usage: PageRank <file> <number_of_iterations>"
        exit(-1)

    # Initialize the spark context.
    conf = SparkConf().setAppName("pagerank")
    sc = SparkContext(conf=conf)
    opfile = 'pagerank.csv'
    lines = sc.textFile(sys.argv[1])

    # Loads all nodes from input file and initialize their neighbors.
    links = lines.map(lambda nodes: parseNeighbors(nodes)).distinct().groupByKey().cache()

    # Loads all nodes with other node(s) link to from input file and initialize ranks of them to one.
    ranks = links.map(lambda (node, neighbors): (node, 1.0))

    # Calculates and updates node ranks continuously using PageRank algorithm.
    for iteration in xrange(int(sys.argv[2])):
        # Calculates node contributions to the rank of other nodes.
        contribs = links.join(ranks).flatMap(lambda (node, (nodes, rank)):
                                             computeContribs(nodes, rank))

        # Re-calculates node ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    # Sorted Ranks in descending order 
    sortedranks = ranks.map(lambda (a, b): (b, a)).sortByKey(0, 1)

    # Display Top Ten
    print(sortedranks.take(10))

    # Write top 100 to a file
    with open(opfile, 'w') as fp:
        writer = csv.writer(fp)
        writer.writerow(("email","pagerank"))

    for (rank, link) in sortedranks.take(100):
        print "%s has rank: %s." % (link, rank)
        with open(opfile, 'a') as fp:
            writer = csv.writer(fp)
            writer.writerow((link, rank))
