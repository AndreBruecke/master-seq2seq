import pandas as pd
import re
from unidecode import unidecode

def remove_diacritics(series: pd.Series) -> pd.Series:
    # Removes diacritics from string column
    return series.apply(unidecode)

def to_lower(series: pd.Series) -> pd.Series:
    # Converts string column to lowercase
    return series.apply(lambda s: s.lower().strip())

def remove_brackets(series: pd.Series) -> pd.Series:
    # Remove brackets, e.g. "Max Mustermann (job description)" -> "Max Mustermann"
    return series.apply(lambda l: re.sub(r'\(.*?\)', '', l).strip())

def remove_abbreviations(series: pd.Series) -> pd.Series:
    # Remove abbreviations, e.g. "George W. Bush" -> "George Bush"
    return series.apply(lambda l: re.sub(r' ([A-Za-z]\.)+ ', ' ', l).strip())
