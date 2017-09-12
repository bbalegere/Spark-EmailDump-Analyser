import csv
import sys
import pandas as pd

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print >> sys.stderr, "Usage: CreateNodesEdges <inputfile>"
        exit(-1)

    fulldf = pd.read_csv(sys.argv[1], quoting=csv.QUOTE_MINIMAL, encoding='utf-8', delimiter='|')

    # Dump Email Content - to be used later for LDA
    emailOutput = "EmailContent.csv"
    contentdf = fulldf[["Message"]]
    print("Writing Contents of the Email to " + emailOutput)
    contentdf = contentdf.dropna()
    print(contentdf.info())
    contentdf.to_csv(emailOutput, sep='|')

    print("Creating NodeList and EdgeList")
    df = fulldf[["From", "To"]]
    df = df.dropna()
    print(df.info())

    x = list(df.index)

    # create pairs from sender to receivers. For multiple receivers, create multiple rows
    pairs = []
    for i in x:
        toids = []
        fromid = ''
        toids = list(set(df["To"][i].split(',')))
        # Some From ids have repetitions, fix it
        fromid = df["From"][i].split(',')[0]

        for email in toids:
            if fromid.strip() != "" and email.strip() != "":
                pairs.append([fromid.lower(), email.lower()])

    fixed_df = pd.DataFrame(pairs, columns=["From", "To"])
    print(fixed_df.head(5))
    fixed_df.to_csv("Edges.csv")

    all_emails = set(list(fixed_df["From"]) + list(fixed_df["To"]))
    df_emails = pd.DataFrame(sorted(all_emails), columns=["email"])
    df_emails.to_csv("Nodes.csv")
    print(df_emails.head(5))
