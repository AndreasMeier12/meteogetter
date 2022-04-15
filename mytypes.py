from dataclasses import dataclass

import pandas


@dataclass(frozen=True)
class MonthResult:
    data: pandas.DataFrame
    month: int
    year: int
