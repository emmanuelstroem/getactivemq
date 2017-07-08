# Pre-Requisites:

+ git

+ pip
    - install using ```easy_install pip```

+ psycopg2
    - install using ```pip install psycopg2```

+ stompest
    - install stompest.async using ```pip install stompest.async```
    - install stompest.config using ```pip install stompest.config```

___

### Database Configurations
___
+ Postgres database engine
    - install from [https://www.postgresql.org/download/](https://www.postgresql.org/download/)
    - database name: ```postgres```
    - database username: ```postgress```
    - database password: ```''```
    - table name: ```getactivemq```
    - table columns: ```column0..column11```

___

### Output files
___
 - dataFromDB.csv: stores rows retreived from database
 - dataFromActiveMQ: stores data received from ActiveMQ
 - {{user_input}}.csv: stores random generated data from array

___

### Program Parameters
___
- ```-i``` : specifies the name of the csv file ending with .csv
- ```-h``` : print program CLI help


___
### How to run the program
____
- clone the project using:
    - ```git clone https://github.com/emmanuelstroem/getactivemq.git```
- cd into the cloned folder:
    - ```cd getactivemq```
- run the program using:
    - ``` python python stomp_activemq.py -i  test.csv```
        - where ```test.csv``` is the file name specified by the user.