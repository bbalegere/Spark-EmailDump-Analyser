# Spark-EmailDump-Analyser
Description - Use Apache Spark to parse and analyse large email dumps. 

Wikileaks releases many email dumps such as the Podesta email files. This project uses Apache Spark infrastructure to generate a csv file of the data and a NodeList/EdgeList which can then be used to perform Network Analysis.
The project also does Natural Language Processing (NLP) using Latent Dirichlet allocation (LDA) on the contents of the email dump.
PageRank calculation for the Nodes can be performed by specifying the number of iterations.

## Instructions (using Google Cloud and Podesta Email Dump as Example)
1. Create a Google Cloud Account - as of now you get $300 free credit with 365 days validity
1. Follow the tutorial here to setup the Spark Cluster - https://cloud.google.com/solutions/monte-carlo-methods-with-hadoop-spark
1. Run the following commands on the master and on all the nodes to install the necessary packages

    `sudo apt-get install python-pip`
    
    `sudo pip install python-dateutil`
    
    `sudo apt-get install python-dev`
    
    `sudo pip install pandas`
    
    `sudo pip install bs4`
    
    `sudo apt-get install libxml2-dev libxslt1-dev`
    
    `sudo pip install lxml`
    
    `sudo pip install nltk`
    
    `python -m    nltk.downloader all`
1. Get the Email dump - Example Podesta Dump 

    `wget https://file.wikileaks.org/file/podesta-emails/podesta-emails.mbox-2016-11-06.gz`
1. Copy the all files from this project and run `chmod u+x *`
1. Modify email_unpack.sh to reflect the correct filename
1. Explode the compressed mbox file into individual EML files, this is required to make use of parallel processing `./email_unpack.sh`
1. Incase the email dump is a collection of eml files, then the above step is not required
1. Upload the folder containing the eml files into Hadoop 

    `hdfs dfs -copyFromLocal /home/bharat/messages/ /user/bharat/podesta/msgs`
1. Run the code to parse the emails by passing the location of the messages folder on Hadoop and the Output File Name

    `spark-submit ParseEmailDump.py /user/bharat/podesta/msgs/messages/ parseddump.csv`
1. Run the code to generate the EmailContent, Nodes and Edges

    `python CreateNodesEdges.py`

1. Upload the generated file to Hadoop

    `hdfs dfs -copyFromLocal /home/bharat/sparkemaildmp/EmailContent.csv /user/bharat/podesta/`
    
    `hdfs dfs -copyFromLocal /home/bharat/sparkemaildmp/Edges.csv /user/bharat/podesta/`
1. Run the NLP LDA analysis code by passing in the location of the EmailContent.csv on Hadoop and the number of topics to generate. You will have to edit the code change the location of the nltk data. In this its `nltk.data.path.append("/home/bharat/nltk_data/")`
    
    `spark-submit NLPLDAllocation.py /user/bharat/podesta/EmailContent.csv 20`
    
1. Run the PageRank code to generate top 100 PageRanks in descending order by passing the location of the edges.csv and the number of iterations to run the PageRank Alogorithm for. The pageranks will be written to pagerank.csv

    `spark-submit PageRank.py /user/bharat/podesta/Edges.csv 20`
   



