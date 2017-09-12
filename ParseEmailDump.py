import csv
import email
import re
import sys
from string import punctuation

import pyzmail
from bs4 import BeautifulSoup
from pyspark import SparkConf, SparkContext

reload(sys)
sys.setdefaultencoding('utf8')

symbols = punctuation
symbols = symbols.replace("@", '')
symbols = symbols.replace(".", '')


def html_to_text(html):
    # https://stackoverflow.com/questions/328356/extracting-text-from-html-file-using-python/24618186#24618186
    soup = BeautifulSoup(html, "lxml")

    # kill all script and style elements
    for script in soup(["script", "style"]):
        script.extract()  # rip it out

    # get text
    text = soup.get_text()
    # break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in text.splitlines())
    # break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # drop blank lines
    text = ' '.join(chunk for chunk in chunks if chunk)
    return text


def removeNonAscii(s):
    if s is None:
        return ''
    else:
        return "".join(i for i in s if ord(i) < 128)


def parsemail(emaildump):
    toids = ''
    fromid = ''
    email_content = ''
    maildate = ''
    row = (maildate, fromid, toids, email_content)

    emaildmp = removeNonAscii(emaildump)
    emsg = email.message_from_string(emaildmp)
    maildate = str(emsg['Date'])

    msg = pyzmail.PyzMessage.factory(emaildmp)

    if msg.text_part != None:
        email_content = msg.text_part.get_payload()
        email_content = html_to_text(email_content)
        email_content = email_content.replace("\n", ' ')
        email_content = email_content.replace("\r", ' ')
        email_content = email_content.replace("\t", ' ')

        for i in symbols:
            email_content = email_content.replace(i, ' ')

        email_content = email_content.replace('  ', ' ')
        email_content = email_content.replace('  ', ' ')
        email_content.rstrip()
        email_content.lstrip()
        email_content = removeNonAscii(email_content)

    subject = removeNonAscii(msg.get_subject())
    email_content = subject + ' ' + email_content

    # Extract only email ids, and exclude the names
    # https://stackoverflow.com/questions/17681670/extract-email-sub-strings-from-large-document/17681902#17681902

    fromid = ','.join(re.findall(r'[\w\.-]+@[\w\.-]+', str(removeNonAscii(emsg['From']))))

    # There are some cases where I am not able to parse To addresses correctly. Need to investigate
    toids = ','.join(re.findall(r'[\w\.-]+@[\w\.-]+', str(removeNonAscii(emsg['To']))))

    row = (maildate, fromid, toids, email_content)

    return row


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print >> sys.stderr, "Usage: spark-submit ParseEmailDump.py <Directory on Hadoop containing the eml files> <outputfile name>"
        exit(-1)

    # Initialize the spark context.
    conf = SparkConf().setAppName("podesta")
    sc = SparkContext(conf=conf)
    
    files = sc.wholeTextFiles(sys.argv[1])

    mails = files.mapValues(lambda mail: parsemail(mail))
    mailcontents = mails.map(lambda (a, b): b)
    print(mailcontents.take(10))

    with open(sys.argv[2], 'w') as fp:
        writer = csv.writer(fp, delimiter='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(("Date", "From", "To", "Message"))
        writer.writerows(mailcontents.collect())
