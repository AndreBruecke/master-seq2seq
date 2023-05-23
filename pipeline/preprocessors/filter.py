import pandas as pd
import re
from unidecode import unidecode
from fuzzyset import FuzzySet


def __normalize_string(sequence: str):
    return unidecode(sequence.strip().lower())

def filter_titles(df: pd.DataFrame) -> pd.DataFrame:
    # Filter out historic figures with titles, e.g. "Charles de Secondat, Baron de Montesquieu"
    df = df[~df['input'].str.contains(r',.*?')]
    df = df[~df['target'].str.contains(r',.*?')]
    return df

def filter_special_characters(df: pd.DataFrame, to_drop=r'#|=|<|>|~|\?|\*|@|\d|\.\.|\{|\}|\+|&|\\|!|%|:|\[|\]|"|_|\(|\)') -> pd.DataFrame:
    # Filters out entries that contain special characters
    return df[~df['input'].str.contains(to_drop) & ~df['target'].str.contains(to_drop)]

def filter_distant_pairs(df: pd.DataFrame, distance_threshold: float = 0.2) -> pd.DataFrame:
    # Filter pairs 
    cp_df = df.copy(deep=True)
    cp_df['input'] = cp_df['input'].apply(__normalize_string).apply(lambda l: re.split(r' +', l.lower()))
    cp_df['target'] = cp_df['target'].apply(__normalize_string).apply(lambda l: re.split(r' +', l.lower()))

    drop_msk = []
    for _, row in cp_df.iterrows():
        l = FuzzySet(use_levenshtein=True)
        r = FuzzySet(use_levenshtein=True)
        for tkn in row['input']: l.add(tkn)
        for tkn in row['target']: r.add(tkn)
        l_scores = [l.get(tkn) for tkn in row['target']]
        r_scores = [r.get(tkn) for tkn in row['input']]
        if any([e is None for e in l_scores]) or any([e is None for e in r_scores]) or any([e[0][0] < distance_threshold for e in l_scores]) or any([e[0][0] < distance_threshold for e in r_scores]):
            l_chars_missing = 0
            r_chars_missing = 0
            for i in range(len(l_scores)):
                if l_scores[i] is None or l_scores[i][0][0] < distance_threshold:
                    l_chars_missing += len(row['target'][i])
            for i in range(len(r_scores)):
                if r_scores[i] is None or r_scores[i][0][0] < distance_threshold:
                    r_chars_missing += len(row['input'][i])
            if l_chars_missing > 2 or r_chars_missing > 2:
                drop_msk.append(False)
                continue
        drop_msk.append(True)
    df = df[drop_msk]
    return df

def filter_different_token_length_pairs(df: pd.DataFrame) -> pd.DataFrame:
    df['input_split'] = df['input'].apply(lambda l: re.split(r'( +|-)', l))
    df['target_split'] = df['target'].apply(lambda l: re.split(r'( +|-)', l))

    df = df[df['input_split'].str.len() == df['target_split'].str.len()]    
    return df.drop(['input_split', 'target_split'], axis=1)

def filter_equal(df: pd.DataFrame) -> pd.DataFrame:
    return df[df['input'] != df['target']]

def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates()