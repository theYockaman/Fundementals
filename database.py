import sqlite3 
from pandas import DataFrame, read_sql_query





class Column:
    def __init__(self, columnName:str, dtype:type, **kwargs) -> None:
        """Column for SQLite Table Database

        :param columnName: Column Name
        :type columnName: str
        :param dtype: DataType of Column
        :type dtype: type
        """
        
        # Column Name 
        self.name = columnName
        
        # Column Data Type
        self.dtype = dtype
        
        # Add Keyword Arguments
        self.__dict__.update(kwargs)
        
    @staticmethod
    def _convertType(t:type)-> str:
        """Convert Type into SQL Datatype

        :param t: Type in Python you want to Convert
        :type t: type
        :return: SQL Datatype
        :rtype: str
        """
        
        # Convert Type to SQLite Datatype
        dictionary = {int:"INTEGER",float:"REAL",str:"TEXT",bool:"BOOLEAN"}
        
        return dictionary.get(t)

    @property
    def sql(self) -> str:
        """SQL Code

        :return: SQL Code to create Column
        :rtype: str
        """
        
        # SQL code for Column Creation
        return f"{self.name} {self._convertType(self.dtype)}"

class Table:
    def __init__(self, tableName:str,databaseConnection:sqlite3.Connection) -> None:
        """Table from SQLite Database

        :param tableName: Table Name
        :type tableName: str
        :param databaseConnection: Connection to the Database
        :type databaseConnection: sqlite3.Connection
        """
        # Table Name
        self.name = tableName
        
        # Database Connection
        self.connection = databaseConnection
        
    @staticmethod
    def exist(tableName:str, databaseConnection:sqlite3.Connection) -> bool:
        """Checks the Tables Existance in the Database

        :param tableName: Table Name
        :type tableName: str
        :param databaseConnection: Database Connection
        :type databaseConnection: sqlite3.Connection
        :return: If the Table Exists
        :rtype: bool
        """
        return databaseConnection.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{tableName}';").fetchone() is not None
    
    @staticmethod
    def create(tableName:str, columns:list[Column], databaseConnection:sqlite3.Connection) -> None:
        """Create a Table in the Database

        :param tableName: Table Name
        :type tableName: str
        :param columns: Columns
        :type columns: list[Column]
        :param databaseConnection: Connect to Database
        :type databaseConnection: sqlite3.Connection
        :raises TypeError: Table Already Exists
        """
        
        if Table.exist(tableName,databaseConnection):
            raise TypeError("Table Already Exists")
    
        # Creates the SQL Code
        sql = f"CREATE TABLE {tableName} ("
        
        for column in columns:
            if column == columns[-1]:
                sql += column.sql
            else:
                sql += f"{column.sql}, "
            
        sql += ") ;"
        # Creating Tables
        databaseConnection.execute(sql)
        databaseConnection.commit()

    @staticmethod
    def delete(tableName:str, databaseConnection:sqlite3.Connection) -> None:
        """Delete the Table in the Database

        :param tableName: Table Name
        :type tableName: str
        :param databaseConnection: Database Connection
        :type databaseConnection: sqlite3.Connection
        :raises TypeError: Table Does Not Exist
        """
        
        # Checking the existance of the Table in the Database
        if not Table.exist(tableName, databaseConnection): raise TypeError(f"{tableName} Table Does Not Exist")
        
        # Deleting the Table
        databaseConnection.execute(f"DROP TABLE {tableName};")
        databaseConnection.commit()

    def update(self, df:DataFrame) -> None:
        """Update the Table to the Dataframe Given

        :param df: The Updated DataFrame
        :type df: DataFrame
        """
        df.to_sql(self.name,self.connection, if_exists='replace', index = False)
   
    @property
    def data(self) -> DataFrame:
        return read_sql_query(f"SELECT * FROM {self.name}", self.connection)
   
class Database:
    def __init__(self, databaseDirectory:str) -> None:
        """Creates and Opens Database

        :param databaseDirectory: Directory of the Database
        :type databaseDirectory: str
        """
     
        if not self.exist(databaseDirectory):
            self.create(databaseDirectory)
        
        # Database Directory
        self.databaseDirectory = databaseDirectory
        
        # Database Connection
        self.connection = sqlite3.connect(self.databaseDirectory,timeout=8)
        
    @staticmethod
    def exist(databaseDirectory:str) -> bool:
        """Checks the Existance of the Database

        :param databaseDirectory: Database Directory
        :type databaseDirectory: str
        :return: If the Database Exists
        :rtype: bool
        """
        from os.path import exists
        if databaseDirectory[-3:] != ".db": return False
        return exists(databaseDirectory)
    
    @staticmethod
    def create(databaseDirectory:str) -> None:
        """Create a Database

        :param databaseDirectory: Directory of the Database
        :type databaseDirectory: str
        :raises TypeError: Database Already Exists
        :raises TypeError: That is Not a Database File must end in '.db'
        """
        
        # Checks to see if the Database already exists
        if Database.exist(databaseDirectory):
            raise TypeError("Database Already Exists")
        
        # Checks to make sure it is a Database
        if databaseDirectory[-3:] != ".db": raise TypeError("That is Not a Database File must end in '.db' ")
        
        # Creation of the Database File
        with open(databaseDirectory,"x") as file:
            pass
    
    @staticmethod
    def delete(databaseDirectory:str) -> None:
        """Delete the Database

        :param databaseDirectory: Database Directory
        :type databaseDirectory: str
        :raises TypeError: Database Does Not Exist or Wrong Directory
        """
        from os import remove
        
        if Database.exist(databaseDirectory):
            remove(databaseDirectory)
        else:
            raise TypeError("Database Does Not Exist or Wrong Directory")
    
    def addTable(self, tableName:str, columns:list[Column] ) -> None:
        """Add a Table to Database

        :param tableName: Table Name
        :type tableName: str
        :param columns: Columns to add to the Table
        :type columns: list[Column]
        :raises TypeError: Table Already Exists
        """
        
        # Check to see if the Table Exists
        if Table.exist(tableName, self.connection): raise TypeError("Table Already Exists")
        
        # Create Table
        Table.create(tableName, columns, self.connection)
        
    def deleteTable(self, tableName:str) -> None:
        """Delete the Table

        :param tableName: Table Name
        :type tableName: str
        :raises TypeError: Table Does Not Exist
        """
        # Check if the Table Exists
        if not Table.exist(tableName, self.connection): raise TypeError("Table Does Not Exist")
        
        # Delete Table
        Table.delete(tableName,self.connection)
     
    def getTable(self, tableName:str) -> Table:
        
        if not Table.exist(tableName,self.connection): raise TypeError("Table Does Not Exist")
        
        # Create a Table Objects
        return Table(tableName,self.connection)
     
     
         
    @property
    def tables(self) -> list[Table]:
        """Tables that are held in the Database

        :return: List of Tables in the Database
        :rtype: list[Table]
        """
        # Table Names
        tableNames = [name[0] for name in self.connection.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]
        
        # Create a Table Objects
        return [Table(name, self.connection) for name in tableNames]
    