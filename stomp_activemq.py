import psycopg2 # to connect to postgres db #pip install psycopg2
import stomp # to connect to activemq
import generateCSV # to be able to call generate

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
stomp_conn.set_listener('', MyListener())
stomp_conn.subscribe(destination='/queue/test', id=1, ack='auto')
