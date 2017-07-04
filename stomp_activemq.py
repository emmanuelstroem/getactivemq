import psycopg2 # to connect to postgres db #pip install psycopg2
import stomp # to connect to activemq
import csv # to write and read csv
import generateCSV # to be able to use functions and variables in genereateCSV
import argparse # to take command line arguments
import os

# connect to database
try:
    db_conn = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='' ")
except:
    print "unable to connect to the database"

cursor = db_conn.cursor()



# class to listen and print stomp to activemq connections
class StompConnectionListener(stomp.ConnectionListener):
    def on_error(self, headers, message):
        print('received an ERROR "%s"' % message)
    def on_message(self, headers, message):
        print('received a message "%s"' % message)


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

# create stomp connection
try:
    stomp_conn = stomp.Connection(host_and_ports = [(host, port)])
except:
    print 'Stomp connection FAILED!'

stomp_conn.start()
stomp_conn.connect(login=user, passcode=password, wait=True)
stomp_conn.set_listener('', StompConnectionListener())
stomp_conn.subscribe(destination='/queue/test', id=1, ack='auto')


generateCSV.writeToCSV(desiredCSV)

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

# call the function to read data from db
readFromDb()



# check values in database and write to dataFromDb.csv
def readFromActiveMQ():
    with open('dataFromActiveMQ.csv', 'wb') as csvfile:
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


# call the function to read data from db



def readCSV():
    with open(desiredCSV, 'r') as readFile:
        csv_reader = csv.reader(readFile)
        for row in csv_reader:
            if row[4] != 'C' and row[4] != 'G':

                try:
                    # cursor.execute(sql.SQL("insert into {} values (%s, %s, %s, %s, %s)").format(sql.Identifier('getactivemq')), [row[0], row[1], row[2], row[3], row[4]])
                    cursor.execute(sql.SQL("insert into {} values (%s, %s, %s, %s, %s)").format(sql.Identifier('getactivemq')), row)
                    conn.commit()
                except:
                    print "cannot insert into table"

            elif row[4] == 'C' or row[4] == 'G':
                # print row
                stomp_conn.send(body=' '.join(row), destination='/queue/test')

readCSV()