from pandas import DataFrame, concat, merge
from database import Column, Database, Table
from datetime import datetime
from indicator import Indicator
from abc import abstractmethod

# Fundemental Indicator Class
class Fundemental(Indicator):
    def __init__(self, fundementalName:str, description:str, database:Database = None, **kwargs) -> None:
        
        # Name of Fundemental Indicator & Description of Fundemental Indicator
        super().__init__(fundementalName, description,kwargs=kwargs)
        
        # Setting up Database
        self._db = database
        
    @property
    def db(self) -> Database:
        return self._db
    
    @abstractmethod
    def _updateDatabase(self) -> None:
        pass
    
# Fundemental Indicators      
class PriceToEarnings(Fundemental):
    def __init__(self, forwardPE:float = None, trailingPE:float = None,database:Database = None, **kwargs) -> None:
        
        # Name and Description
        super().__init__("Price to Earnings Ratio","Description", kwargs = kwargs)
        
        # Database Connection
        self._db = database
        
        # Forward PE Value
        self._forwardPE = forwardPE
        
        # Trailing PE Value
        self._trailingPE = trailingPE
        
        self._update()
        
    @property
    def forwardPE(self) -> float:
        return self._forwardPE
    
    @property
    def trailingPE(self) -> float:
        return self._trailingPE
    
    @staticmethod
    def calculatePercent(forwardPE:float = None, trailingPE:float = None) -> float:
        if forwardPE is None or trailingPE is None: return None
        
        return 1- forwardPE/trailingPE
    
    def _updateDatabase(self) -> None:
        
        # Create the Table if the Table Does Not Exist
        if not Table.exist("Fundementals",self.db.connection):
            self.db.addTable("Fundementals",[
                Column("Date",str),
                Column("TrailingPE",float),
                Column("ForwardPE",float)
            ])
        
        # Get the Table
        table = self.db.getTable("Fundementals")
        
        # Get the Data from the Table
        data = table.data
        
        # New Data to update the Database
        newData = DataFrame({"Date":[str(datetime.now().date())],"TrailingPE":[self.trailingPE],"ForwardPE":[self.forwardPE]})
        
        if list(data.loc[data['Date'] == str(datetime.now().date())].values) != []:
            if not set(newData.keys()).issubset(data.loc[data['Date'] == str(datetime.now().date())].columns):
                data = merge(data, newData, on='Date')
            else:
                data.loc[data['Date'] == str(datetime.now().date()),newData.columns] = newData.values
            
        else:
            # Add New Data to the Database
            data = concat([data, newData], axis=0, ignore_index= True)
        
        # Update the Database
        table.update(data)
        
    def _update(self) -> None:
        
        # Calculate Percent
        self._percent = self.calculatePercent(self._forwardPE,self._trailingPE)
        
        if self.db is not None:
            # Updating Database
            self._updateDatabase()
 
class PriceToEarningsGrowth(Fundemental):
    def __init__(self, peg:float = None, trailingPEG:float = None, database:Database = None, **kwargs) -> None:
        
       # Database Connection
        self._db = database
        
        # Name and Description
        super().__init__("Price to Earnings Growth", "description", kwargs = kwargs)
        
        # PEG
        self._peg = peg
        
        # Trailing PEG
        self._trailingPEG = trailingPEG
        
        self._update()
    
    @property
    def peg(self) -> float:
        return self._peg
 
    @property
    def trailingPEG(self) -> float:
        return self._trailingPEG
 
    @staticmethod
    def calculatePercent(peg:float, trailingPEG:float):
        return 1 - peg/trailingPEG
 
    def _updateDatabase(self) -> None:
        
        # Create the Table if the Table Does Not Exist
        if not Table.exist("Fundementals",self.db.connection):
            self._db.addTable("Fundementals",[
                Column("Date",str),
                Column("TrailingPEG",float),
                Column("PEG",float)
            ])
        
        # Get the Table
        table = self.db.getTable("Fundementals")
        
        # Get the Data from the Table
        data = table.data
        
        # New Data to update the Database
        newData = DataFrame({"Date":[str(datetime.now().date())],"PEG":[self.peg],"TrailingPEG":[self.trailingPEG]})
        
        # Update Data
        if list(data.loc[data['Date'] == str(datetime.now().date())].values) != []:
            if not set(newData.keys()).issubset(data.loc[data['Date'] == str(datetime.now().date())].columns):
                data = merge(data, newData, on='Date')
            else:
                data.loc[data['Date'] == str(datetime.now().date()),newData.columns] = newData.values
            
        else:
            # Add New Data to the Database
            data = concat([data, newData], axis=0, ignore_index= True)
            
        # Update the Database
        table.update(data)
        
    def _update(self) -> None:
        
        # Calculate Percent
        self._percent = self.calculatePercent(self.peg,self.trailingPEG)
        
        if self.db is not None:
            # Updating Database
            self._updateDatabase()
 
class EarningsPerShare(Fundemental):
    def __init__(self, forwardEPS:float = None, trailingEPS:float = None, database:Database = None, **kwargs) -> None:
        
        # Database Connection
        self._db = database
        
        # Name and Description
        super().__init__("Earnings Per Share", "description", kwargs = kwargs)
        
        # Forward EPS
        self._forwardEPS = forwardEPS
        
        # Trailing EPS
        self._trailingEPS = trailingEPS
        
        self._update()
    
    @property
    def forwardEPS(self) -> float:
        return self._forwardEPS
 
    @property
    def trailingEPS(self) -> float:
        return self._trailingEPS
 
    @staticmethod
    def calculatePercent(forwardEPS:float = None, trailingEPS:float = None):
        if forwardEPS is None or trailingEPS is None: return None
        return 1 - forwardEPS/trailingEPS
 
    def _updateDatabase(self) -> None:
        
        # Create the Table if the Table Does Not Exist
        if not Table.exist("Fundementals",self.db.connection):
            self._db.addTable("Fundementals",[
                Column("Date",str),
                Column("TrailingEPS",float),
                Column("ForwardEPS",float)
            ])
        
        # Get the Table
        table = self.db.getTable("Fundementals")
        
        # Get the Data from the Table
        data = table.data
        
        # New Data to update the Database
        newData = DataFrame({"Date":[str(datetime.now().date())],"ForwardEPS":[self.forwardEPS],"TrailingEPS":[self.trailingEPS]})
        
        # Update Data
        if list(data.loc[data['Date'] == str(datetime.now().date())].values) != []:
            if not set(newData.keys()).issubset(data.loc[data['Date'] == str(datetime.now().date())].columns):
                data = merge(data, newData, on='Date')
            else:
                data.loc[data['Date'] == str(datetime.now().date()),newData.columns] = newData.values
            
        else:
            # Add New Data to the Database
            data = concat([data, newData], axis=0, ignore_index= True)
            
        # Update the Database
        table.update(data)
        
    def _update(self) -> None:
        
        # Calculate Percent
        self._percent = self.calculatePercent(self.forwardEPS,self.trailingEPS)
        
        if self.db is not None:
            # Updating Database
            self._updateDatabase()
 
class FreeCashflow(Fundemental):
    def __init__(self, freeCashflow:float = None, marketCap:float = None, database:Database = None, **kwargs) -> None:
        
        # Database Connection
        self._db = database
        
        # Name and Description
        super().__init__("Free Cashflow", "description", kwargs = kwargs)
        
        # Market Cap
        self._marketCap = marketCap
        
        # Free Cashflow
        self._freeCashflow = freeCashflow
        
        self._update()
    
    @property
    def freeCashflow(self) -> float:
        return self._freeCashflow
 
    @property
    def marketCap(self) -> float:
        return self._marketCap
 
    @staticmethod
    def calculatePercent(freeCashflow:float = None, marketCap:float = None) -> float:
        if freeCashflow is None or marketCap is None: return None
        return 1 - freeCashflow/marketCap
    
    def _updateDatabase(self) -> None:
        
        # Create the Table if the Table Does Not Exist
        if not Table.exist("Fundementals",self.db.connection):
            self._db.addTable("Fundementals",[
                Column("Date",str),
                Column("FreeCashflow",float),
                Column("MarketCap",float)
            ])
        
        # Get the Table
        table = self.db.getTable("Fundementals")
        
        # Get the Data from the Table
        data = table.data
        
        # New Data to update the Database
        newData = DataFrame({"Date":[str(datetime.now().date())],"FreeCashflow":[self.freeCashflow],"MarketCap":[self.marketCap]})
        
        # Update Data
        if list(data.loc[data['Date'] == str(datetime.now().date())].values) != []:
            if not set(newData.keys()).issubset(data.loc[data['Date'] == str(datetime.now().date())].columns):
                data = merge(data, newData, on='Date')
            else:
                data.loc[data['Date'] == str(datetime.now().date()),newData.columns] = newData.values
            
        else:
            # Add New Data to the Database
            data = concat([data, newData], axis=0, ignore_index= True)
            
        # Update the Database
        table.update(data)
        
    def _update(self) -> None:
        
        # Calculate Percent
        self._percent = self.calculatePercent(self.freeCashflow,self.marketCap)
        
        if self.db is not None:
            # Updating Database
            self._updateDatabase()
  
class PriceToBook(Fundemental):
    def __init__(self, pb:float = None, database:Database = None, **kwargs) -> None:
        
        # Database Connection
        self._db = database
        
        # Name and Description
        super().__init__("Price to Book", "description", kwargs = kwargs)
        
        # Price to Book
        self._pb = pb
        
        self._update()
    
    @property
    def pb(self) -> float:
        return self._pb
 
    @staticmethod
    def calculatePercent(pb:float = None) -> float:
        if pb is None: return None
        return (1-pb)/pb
  
    def _updateDatabase(self) -> None:
        
        # Create the Table if the Table Does Not Exist
        if not Table.exist("Fundementals",self.db.connection):
            self._db.addTable("Fundementals",[
                Column("Date",str),
                Column("PriceToBook",float)
            ])
        
        # Get the Table
        table = self.db.getTable("Fundementals")
        
        # Get the Data from the Table
        data = table.data
        
        # New Data to update the Database
        newData = DataFrame({"Date":[str(datetime.now().date())],"PriceToBook":[self.pb]})
        
        # Update Data
        if list(data.loc[data['Date'] == str(datetime.now().date())].values) != []:
            if not set(newData.keys()).issubset(data.loc[data['Date'] == str(datetime.now().date())].columns):
                data = merge(data, newData, on='Date')
            else:
                data.loc[data['Date'] == str(datetime.now().date()),newData.columns] = newData.values
            
        else:
            # Add New Data to the Database
            data = concat([data, newData], axis=0, ignore_index= True)
            
        # Update the Database
        table.update(data)
        
    def _update(self) -> None:
        
        # Calculate Percent
        self._percent = self.calculatePercent(self.pb)
        
        if self.db is not None:
            # Updating Database
            self._updateDatabase()

class ReturnOnEquity(Fundemental):
    def __init__(self, database:Database = None, roe:float = None, **kwargs) -> None:
        
        # Database Connection
        self._db = database
        
        # Name and Description
        super().__init__("Return on Equity", "description", kwargs=kwargs)
        
        # Return On Equity
        self._roe = roe
        
        self._update()
    
    @property
    def roe(self) -> float:
        return self._roe
 
    @staticmethod
    def calculatePercent(roe:float = None):
        if roe is None: return None
        return (10-roe)/10
 
    def _updateDatabase(self) -> None:
        
        # Create the Table if the Table Does Not Exist
        if not Table.exist("Fundementals",self.db.connection):
            self._db.addTable("Fundementals",[
                Column("Date",str),
                Column("ReturnOnEquity",float)
            ])
        
        # Get the Table
        table = self.db.getTable("Fundementals")
        
        # Get the Data from the Table
        data = table.data
        
        # New Data to update the Database
        newData = DataFrame({"Date":[str(datetime.now().date())],"ReturnOnEquity":[self.roe]})
        
        # Update Data
        if list(data.loc[data['Date'] == str(datetime.now().date())].values) != []:
            if not set(newData.keys()).issubset(data.loc[data['Date'] == str(datetime.now().date())].columns):
                data = merge(data, newData, on='Date')
            else:
                data.loc[data['Date'] == str(datetime.now().date()),newData.columns] = newData.values
            
        else:
            # Add New Data to the Database
            data = concat([data, newData], axis=0, ignore_index= True)
            
        # Update the Database
        table.update(data)
        
    def _update(self) -> None:
        
        # Calculate Percent
        self._percent = self.calculatePercent(self.roe)
        
        if self.db is not None:
            
            # Updating Database
            self._updateDatabase()
 
class DividendPayout(Fundemental):
    def __init__(self, dp:float = None, database:Database = None, **kwargs) -> None:
        
        # Database Connection
        self._db = database
        
        # Name and Description
        super().__init__("Dividend Payout", "description", kwargs = kwargs)
        
        # Dividend Payout
        self._dp = dp
        
        self._update()
    
    @property
    def dp(self) -> float:
        return self._dp
    
    @staticmethod
    def calculatePercent(dp:float = None):
        if dp is None: return None
        return 1/dp
 
    def _updateDatabase(self) -> None:
        
        # Create the Table if the Table Does Not Exist
        if not Table.exist("Fundementals",self.db.connection):
            self._db.addTable("Fundementals",[
                Column("Date",str),
                Column("DividendPayout",float)
            ])
        
        # Get the Table
        table = self.db.getTable("Fundementals")
        
        # Get the Data from the Table
        data = table.data
        
        # New Data to update the Database
        newData = DataFrame({"Date":[str(datetime.now().date())],"DividendPayout":[self.dp]})
        
        # Update Data
        if list(data.loc[data['Date'] == str(datetime.now().date())].values) != []:
            if not set(newData.keys()).issubset(data.loc[data['Date'] == str(datetime.now().date())].columns):
                data = merge(data, newData, on='Date')
            else:
                data.loc[data['Date'] == str(datetime.now().date()),newData.columns] = newData.values
            
        else:
            # Add New Data to the Database
            data = concat([data, newData], axis=0, ignore_index= True)
            
        # Update the Database
        table.update(data)
        
    def _update(self) -> None:
        
        # Calculate Percent
        self._percent = self.calculatePercent(self.dp)
        
        if self.db is not None:
            # Updating Database
            self._updateDatabase()
 
class PriceToSales(Fundemental):
    def __init__(self, ps:float = None, database:Database = None, **kwargs) -> None:
        
        # Database Connection
        self._db = database
        
        # Name and Description
        super().__init__("Price to Sales", "description", kwargs = kwargs)
        
        # Price to Sales
        self._ps = ps
        
        self._update()
    
    @property
    def ps(self) -> float:
        return self._ps
    
    @staticmethod
    def calculatePercent(ps:float = None):
        if ps is None: return None
        return ps
 
    def _updateDatabase(self) -> None:
        
        # Create the Table if the Table Does Not Exist
        if not Table.exist("Fundementals",self.db.connection):
            self._db.addTable("Fundementals",[
                Column("Date",str),
                Column("PriceToSales",float)
            ])
        
        # Get the Table
        table = self.db.getTable("Fundementals")
        
        # Get the Data from the Table
        data = table.data
        
        # New Data to update the Database
        newData = DataFrame({"Date":[str(datetime.now().date())],"PriceToSales":[self.ps]})
        
        # Update Data
        if list(data.loc[data['Date'] == str(datetime.now().date())].values) != []:
            if not set(newData.keys()).issubset(data.loc[data['Date'] == str(datetime.now().date())].columns):
                data = merge(data, newData, on='Date')
            else:
                data.loc[data['Date'] == str(datetime.now().date()),newData.columns] = newData.values
            
        else:
            # Add New Data to the Database
            data = concat([data, newData], axis=0, ignore_index= True)
            
        # Update the Database
        table.update(data)
        
    def _update(self) -> None:
        
        # Calculate Percent
        self._percent = self.calculatePercent(self.ps)
        
        if self.db is not None:
            # Updating Database
            self._updateDatabase()
 
class DividendYield(Fundemental):
    def __init__(self, dy:float = None, database:Database = None, **kwargs) -> None:
        
        # Database Connection
        self._db = database
        
        # Name and Description
        super().__init__("Dividend Yield", "description", kwargs = kwargs)
        
        # Dividend Yield
        self._dy = dy
        
        self._update()
    
    @property
    def dy(self) -> float:
        return self._dy
    
    @staticmethod
    def calculatePercent(dy:float = None):
        if dy is None: return None
        return 1/(1+dy)
 
    def _updateDatabase(self) -> None:
        
        # Create the Table if the Table Does Not Exist
        if not Table.exist("Fundementals",self.db.connection):
            self._db.addTable("Fundementals",[
                Column("Date",str),
                Column("DividendYield",float)
            ])
        
        # Get the Table
        table = self.db.getTable("Fundementals")
        
        # Get the Data from the Table
        data = table.data
        
        # New Data to update the Database
        newData = DataFrame({"Date":[str(datetime.now().date())],"DividendYield":[self.dy]})
        
        # Update Data
        if list(data.loc[data['Date'] == str(datetime.now().date())].values) != []:
            if not set(newData.keys()).issubset(data.loc[data['Date'] == str(datetime.now().date())].columns):
                data = merge(data, newData, on='Date')
            else:
                data.loc[data['Date'] == str(datetime.now().date()),newData.columns] = newData.values
            
        else:
            # Add New Data to the Database
            data = concat([data, newData], axis=0, ignore_index= True)
            
        # Update the Database
        table.update(data)
        
    def _update(self) -> None:
        
        # Calculate Percent
        self._percent = self.calculatePercent(self.dy)
        
        if self.db is not None:
            # Updating Database
            self._updateDatabase()
 
class DebtToEquity(Fundemental):
    def __init__(self, de:float = None, database:Database = None, **kwargs) -> None:
        
        # Database Connection
        self._db = database
        
        # Name and Description
        super().__init__("Dividend Yield", "description", kwargs = kwargs)
        
        # Debt To Equity
        self._de = de
        
        self._update()
    
    @property
    def de(self) -> float:
        return self._de
    
    @staticmethod
    def calculatePercent(de:float = None):
        if de is None: return None
        return 1/(1+de)
 
    def _updateDatabase(self) -> None:
        
        # Create the Table if the Table Does Not Exist
        if not Table.exist("Fundementals",self.db.connection):
            self._db.addTable("Fundementals",[
                Column("Date",str),
                Column("DebtToEquity",float)
            ])
        
        # Get the Table
        table = self.db.getTable("Fundementals")
        
        # Get the Data from the Table
        data = table.data
        
        # New Data to update the Database
        newData = DataFrame({"Date":[str(datetime.now().date())],"DebtToEquity":[self.de]})
        
        # Update Data
        if list(data.loc[data['Date'] == str(datetime.now().date())].values) != []:
            if not set(newData.keys()).issubset(data.loc[data['Date'] == str(datetime.now().date())].columns):
                data = merge(data, newData, on='Date')
            else:
                data.loc[data['Date'] == str(datetime.now().date()),newData.columns] = newData.values
            
        else:
            # Add New Data to the Database
            data = concat([data, newData], axis=0, ignore_index= True)
            
        # Update the Database
        table.update(data)
        
    def _update(self) -> None:
        
        # Calculate Percent
        self._percent = self.calculatePercent(self.de)
        
        if self.db is not None:
            # Updating Database
            self._updateDatabase()
 
 
 
 
 