from typing import Dict
from typing import List

from pydantic import BaseModel


class trade_data(BaseModel):
    totalTradesUSD: float
    totalFeeUSD: float
    token0TradeVolume: float
    token1TradeVolume: float
    token0TradeVolumeUSD: float
    token1TradeVolumeUSD: float

    def __add__(self, other: 'trade_data') -> 'trade_data':
        """
    Adds the values of another trade_data object to the current trade_data object.

    Args:
        other (trade_data): The trade_data object to be added.

    Returns:
        trade_data: The updated trade_data object with the values of the other trade_data object added to it.

    Example:
        trade1 = trade_data()
        trade2 = trade_data()
        trade1.__add__(trade2)  # Returns the updated trade1 object with the values of trade2 added to it.

    """
        self.totalTradesUSD += other.totalTradesUSD
        self.totalFeeUSD += other.totalFeeUSD
        self.token0TradeVolume += other.token0TradeVolume
        self.token1TradeVolume += other.token1TradeVolume
        self.token0TradeVolumeUSD += other.token0TradeVolumeUSD
        self.token1TradeVolumeUSD += other.token1TradeVolumeUSD
        return self

    def __sub__(self, other: 'trade_data') -> 'trade_data':
        """

    Subtracts the values of the given 'trade_data' object from the current 'trade_data' object and returns the updated 'trade_data' object.

    Parameters:
        - other: A 'trade_data' object representing the data to be subtracted from the current object.

    Returns:
        - A 'trade_data' object with the values subtracted from the current object.

    Example:
        trade_data1 = trade_data(...)
        trade_data2 = trade_data(...)
        result = trade_data1 - trade_data2

    """
        self.totalTradesUSD -= other.totalTradesUSD
        self.totalFeeUSD -= other.totalFeeUSD
        self.token0TradeVolume -= other.token0TradeVolume
        self.token1TradeVolume -= other.token1TradeVolume
        self.token0TradeVolumeUSD -= other.token0TradeVolumeUSD
        self.token1TradeVolumeUSD -= other.token1TradeVolumeUSD
        return self

    def __abs__(self) -> 'trade_data':
        """
    Returns a new instance of `trade_data` with all the attributes converted to their absolute values.

    This method calculates the absolute values of the following attributes:
    - `totalTradesUSD`
    - `totalFeeUSD`
    - `token0TradeVolume`
    - `token1TradeVolume`
    - `token0TradeVolumeUSD`
    - `token1TradeVolumeUSD`

    The absolute value of an attribute is the positive value of that attribute, regardless of its original sign.

    Returns:
        A new instance of `trade_data` with all the attributes converted to their absolute values.

    """
        self.totalTradesUSD = abs(self.totalTradesUSD)
        self.totalFeeUSD = abs(self.totalFeeUSD)
        self.token0TradeVolume = abs(self.token0TradeVolume)
        self.token1TradeVolume = abs(self.token1TradeVolume)
        self.token0TradeVolumeUSD = abs(self.token0TradeVolumeUSD)
        self.token1TradeVolumeUSD = abs(self.token1TradeVolumeUSD)
        return self


class event_trade_data(BaseModel):
    logs: List[Dict]
    trades: trade_data


class epoch_event_trade_data(BaseModel):
    Swap: event_trade_data
    Mint: event_trade_data
    Burn: event_trade_data
    Trades: trade_data
