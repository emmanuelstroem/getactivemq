import psycopg2 # to connect to postgres db #pip install psycopg2
# import stomp # to connect to activemq
import csv # to write and read csv
import generateCSV # to be able to use functions and variables in genereateCSV
import argparse # to take command line arguments
import os
import sys
import time
from psycopg2 import sql
# import json

from twisted.internet import defer, reactor

from stompest.config import StompConfig # to connect to activemq
from stompest.async import Stomp # to insert into activemq
from stompest.async.listener import SubscriptionListener # to read from activemq

# connect to database
try:
    db_conn = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='' ")
except:
    print "unable to connect to the database"

cursor = db_conn.cursor()

#VARIABLES

# CLI arguments
parser = argparse.ArgumentParser()
parser.add_argument("-i", "--csv", type=str, help='csv file name to store the generated random content')

args = parser.parse_args()
desiredCSV = args.csv

# stomp activemq variables
user = os.getenv("ACTIVEMQ_USER") or "admin"
password = os.getenv("ACTIVEMQ_PASSWORD") or "admin"
host = os.getenv("ACTIVEMQ_HOST") or "localhost"
port = os.getenv("ACTIVEMQ_PORT") or 61613

def create_table():
    try:
        cursor.execute("""CREATE TABLE IF NOT EXISTS getactivemq (column0 text, column1 text, column2 text, column3 text, column4 text, column5 text, column6 text, column7 text, column8 text, column9 text, column10 text, column11 text)""")
        # db_conn.commit()
    except:
        print "cannot create table in database"


# check values in database and write to dataFromDb.csv
def readFromDb():
    with open('dataFromDB.csv', 'wb') as csvfile:
        csv_writer = csv.writer(csvfile, dialect='excel', delimiter=',',\
            quotechar='|', quoting=csv.QUOTE_MINIMAL)

        try:
            cursor.execute("""SELECT * from getactivemq""")
        except:
            print "cannot query table"

        rowsFromDb = cursor.fetchall()

        # print "\nRows: \n"
        for row in rowsFromDb:
            # print "   ", row
            csv_writer.writerow(row)


# check values in database and write to dataFromDb.csv
def readFromActiveMQ():
    with open('dataFromActiveMQ.csv', 'wb') as csvfile:
        csv_writer = csv.writer(csvfile, dialect='excel', delimiter=',',\
        quotechar='|', quoting=csv.QUOTE_MINIMAL)

messages = 0

class Listener(object):
    @defer.inlineCallbacks
    def run(self):
        config = StompConfig('tcp://%s:%d' % (host, port), login=user, passcode=password, version='1.1')
        client = Stomp(config)
        yield client.connect(host='mybroker')

        self.count = 0
        self.start = time.time()
        client.subscribe(destination='atcg', listener=SubscriptionListener(self.handleFrame), headers={'ack': 'auto', 'id': 'required-for-STOMP-1.1'})

    @defer.inlineCallbacks
    def handleFrame(self, client, frame):
        if frame is not '':
            for data in frame:
                if data[0] == 'body':
                    self.count += 1
                    print data[1]
                    print '\n'
                    next()
            # next()
            print self.count
            self.stop(client)
        else:
            self.stop(client)

    @defer.inlineCallbacks
    def stop(self, client):
        print 'Disconnecting. Waiting for RECEIPT frame ...',
        yield client.disconnect(receipt='bye')
        print 'ok'

        diff = time.time() - self.start
        print 'Received %s frames in %f seconds' % (self.count, diff)
        reactor.stop()




@defer.inlineCallbacks
def readCSV():
    config = StompConfig('tcp://%s:%d' % (host, port), login=user, passcode=password, version='1.1')
    client = Stomp(config)
    yield client.connect(host='mybroker')

    count = 0
    start = time.time()

    with open(desiredCSV, 'r') as readFile:
        csv_reader = csv.reader(readFile)
        for row in csv_reader:
            if row[4] != 'C' and row[4] != 'G':

                try:
                    print row
                    cursor.execute(sql.SQL("insert into {} values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)").format(sql.Identifier('getactivemq')), row)
                    db_conn.commit()
                except:
                    print "cannot insert into table"

            elif row[4] == 'C' or row[4] == 'G':
                data = unicode(row)
                client.send(destination='atcg', body=data, headers={'persistent': 'false'})
                count += 1

            else:
                print 'Error reading 5th column'
    diff = time.time() - start
    print 'Sent %s frames in %f seconds' % (count, diff)

    yield client.disconnect(receipt='bye')


# readCSV()

if __name__ == '__main__':
    create_table()
    readCSV()
    generateCSV.writeToCSV(desiredCSV)
    # readFromDb()
    # readFromActiveMQ()
    Listener().run()
    reactor.run()
