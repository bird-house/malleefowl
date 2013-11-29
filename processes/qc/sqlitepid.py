"""
Created on 14.11.2013

:author: tobiaskipp
"""
import sqlite3

class SqlitePid():
    """ The class is specifically designed for the use with the QC tool.
    
    Digital Object is abbreviated as DO
    The database is expected to contain a table with (location,identifier) entries.
    location is the file on the server 
    identifier is the PID of the digital object (e.g. 10876/CORDEX-adse-adsa-vaef-adsd)
    """

    def __init__(self,database_location):
        self.drop_whitelist=[]
        self.conn = sqlite3.connect(database_location)
        self.cursor=self.conn.cursor()
        self.tablename = "pidinfo"
        self.keywords = ["location","identifier"]
        s = "("
        for key in self.keywords:
            s+=key+" text,"
        self.keyword_tuple_string = s[:-1]+");"
    
    def first_run(self):
        """Makes sure that the database has the required table."""
        stmt="CREATE TABLE IF NOT EXISTS "+self.tablename+self.keyword_tuple_string
        self.cursor.executescript(stmt)
        self.conn.commit()

    def add_do(self,location,identifier):
        """Adds a DO and its reference to the database.

        :param location: The referenced file or dataset.
        :param identifer: The string identifier for the handle system.
        """
        self.cursor.execute("INSERT INTO "+self.tablename+" VALUES(?,?)",(location,identifier))
        self.conn.commit()

    def get_by_key_value(self,key,value):
        """ Search for database entries with the given key and value.

        :param key: A valid variable name. ["identifier","location"]
        :param value: The value the variable must have to be selected.
        :returns: A list of found tuples (location,identifier)
        """
        if(key in self.keywords):
            stmt="SELECT "+", ".join(self.keywords)+" FROM "+self.tablename+" WHERE "+key+"=?"
            self.cursor.execute(stmt,(value,))
            return self.cursor.fetchall()
        else:
            return []

    def get_like_location(self,location):
        """ Search for database entries with location ending with the given location.

        :param location: At least end of a full path location of the file. If too short many
        results might be found.
        :returns: A list of found matches to the search.
        """
        stmt=("SELECT "+", ".join(self.keywords)+" FROM "+self.tablename+" WHERE location LIKE \"%"+
            location+"\"")
        self.cursor.execute(stmt)
        results = self.cursor.fetchall()
        return results
        


    def get_identifiers(self):
        """ Search for all identifiers in the database 
        
        :returns: A list of DO identifiers.
        """
        stmt = "SELECT identifier FROM "+self.tablename+""
        self.cursor.execute(stmt)
        return self.cursor.fetchall()

    def remove_by_key_and_value(self,key,value):
        """Remove digital objects form the database with the given variable and value.

        To prevent SQLInjections a whitelist is used. If the key is not in the whitelist
        nothing is executed.

        :param key: The name of the variable. Must be one of [identifier, url, location]
        :param value: The value to match the variable with.
        """
        if(key in self.keywords):
            stmt = "DELETE FROM "+self.tablename+" WHERE "+key+"=?"
            self.cursor.execute(stmt,(value,))
            self.conn.commit()

