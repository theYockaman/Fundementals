from abc import ABC, abstractmethod, abstractstaticmethod
from typing import Union
from pandas import DataFrame

class Indicator(ABC):
    def __init__(self, indicatorName:str, description:str, **kwargs) -> None:
        
        # Indicator Name
        self._name = indicatorName
        
        # Indicator Description
        self._description = description
        
        # Add the other Keyword Arguments
        self.__dict__.update(kwargs)
        
    @property
    def name(self) -> str:
        return self._name
    
    @property
    def description(self) -> str:
        return self._description
    
    @property
    def percent(self) -> Union[float,DataFrame]:
        return self._percent
    
    @abstractstaticmethod
    def calculatePercent() -> Union[float,DataFrame]:
        return None
    
    @abstractmethod
    def _update(self) -> None:
        
        # Calculate the Percent
        self._percent = self.calculatePercent()

    def __str__(self) -> str:
        if self.percent is None:
            return f"{self.name}: 0%"
        else:
            return f"{self.name}: {round(self.percent*100,2)}%"
