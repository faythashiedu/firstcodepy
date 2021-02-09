import pymysql.cursors
# PYMYSQL MODULE TO INTERACT WITH THE DATABASE

# CLASS DBMAN TO HOLD THE WHOLE DATASTRUCTURES AND APPLICATIONS FOR DATABASE INTERACTION.
class DBMan:

    # INITIALIZE ATTRIBUTES RRQUIRED FOR DATABASE CONNECTION LIKE DBNAME, HOST, PASSWORD ETC
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    # METHOD TO CONNECT TO DATABASE USING ATTRIBUTES INITIALIZED, AND RETURN A CONNECTION OBJECT THAT CAN BE USED FOR DATABASE INTERACTIONS
    def connect_to_db(self):
        # Connect to the database
        connection = pymysql.connect(host= self.host,
                                    user= self.user,
                                    password= self.password,
                                    database=self.database,
                                    cursorclass=pymysql.cursors.DictCursor)
        return connection

    # METHOD TO SEND WRITE RELATED QUERIES TO THE DATABASE USING THE CONNECTION OBJECT ESTABLISHED
    def write_to_db(self, sql):
        connection = self.connect_to_db()
        with connection.cursor() as cursor:
        
            cursor.execute(sql)

            connection.commit()

    # METHOD TO SEND READ RELATED QUERIES TO THE DATABASE USING THE CONNECTION OBJECT ESTABLISHED
    def read_from_db(self, sql, fetchall = True): # METHOD TAKES THE PARAMETER FETCH ALL(TRU/ FALSE), WHICH IS TO DETERMINE HOW THE METHOD SHOULD FETCH FROM DB - TRUE TO GET ALL ENTRIES FOUND AND FALSE TO RETURN JUST ONE. 
        with self.connect_to_db().cursor() as cursor:
        
            cursor.execute(sql)

            if fetchall == True:

                result = cursor.fetchall()
            
            else:

                result = cursor.fetchone()

            return result