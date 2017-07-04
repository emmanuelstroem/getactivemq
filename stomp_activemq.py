import psycopg2 # to connect to postgres db #pip install psycopg2

# connect to database
try:
    db_conn = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='' ")
except:
    print "unable to connect to the database"

cursor = db_conn.cursor()
