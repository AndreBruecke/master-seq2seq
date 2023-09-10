import pandas as pd
import random
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

def filter_distant_pairs(df: pd.DataFrame, distance_threshold: float = 0.2, total_l_chars_allowed_missing: int = 2, total_r_chars_allowed_missing: int = 2) -> pd.DataFrame:
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
            if l_chars_missing > total_l_chars_allowed_missing or r_chars_missing > total_r_chars_allowed_missing:
                drop_msk.append(False)
                continue
        drop_msk.append(True)

    df = df[drop_msk]
    return df

def filter_different_token_length_pairs(df: pd.DataFrame) -> pd.DataFrame:
    df['input_split'] = df['input'].apply(lambda l: re.split(r' +|-', l))
    df['target_split'] = df['target'].apply(lambda l: re.split(r' +|-', l))

    df = df[df['input_split'].str.len() == df['target_split'].str.len()]    
    return df.drop(['input_split', 'target_split'], axis=1)

def filter_different_token_length_variants(df: pd.DataFrame, keep_empty=True) -> pd.DataFrame:
    def filter_variants(row):
        filtered = []
        for v in row['target_split']:
            if len(v) == len(row['input_split']): filtered.append(' '.join(v))
        row['target'] = '#'.join(filtered)
        if len(row['target']) < 1 and (not keep_empty or any([len(v) > 1 for v in row['target_split']])):
            row['target'] = '<DROP>'
        return row

    df['input_split'] = df['input'].apply(lambda l: re.split(r' +|-', l))
    df['target_split'] = df['target'].apply(lambda l: [re.split(r' +|-', v) for v in l.split('#')])

    df = df.apply(filter_variants, axis=1)
    df = df[df['target'] != '<DROP>']
    return df.drop(['input_split', 'target_split'], axis=1)

def filter_equal(df: pd.DataFrame) -> pd.DataFrame:
    return df[df['input'] != df['target']]

def filter_large_token_diff(df: pd.DataFrame, input_threshold=3, target_threshold=3) -> pd.DataFrame:
    x1 = df['input'].apply(lambda s: len(re.split(r' +|-', s)))
    x2 = df['target'].apply(lambda s: len(re.split(r' +|-', s)))
    mask = x2 - x1 <= target_threshold
    mask_2 = x1 - x2 <= input_threshold
    print(df[~mask])
    print(df[~mask_2])
    return df[mask & mask_2]

def filter_large_character_diff(df: pd.DataFrame, input_threshold=9999, target_threshold=25) -> pd.DataFrame:
    print(df[(df['input'].str.len() - df['target'].str.len()) > input_threshold])
    df = df[(df['input'].str.len() - df['target'].str.len()) <= input_threshold]

    print(df[(df['target'].str.len() - df['input'].str.len()) > target_threshold])
    return df[(df['target'].str.len() - df['input'].str.len()) <= target_threshold]

def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates()

def filter_historic_per(df: pd.DataFrame, check_target=True) -> pd.DataFrame:
    df = df[~df['input'].str.contains(r' ?.*? (of |de |den |av |di |van |car |da |du |dan |dari |af |von |vo |od |del |der |die |el |vu |han |na |od |lo |cel |the |le |nga |z |o |i |d\').*?')]
    df = df[~df['input'].str.contains(r'(princ|san |pope|papa|pape|paus|emperor|impera|king )')]
    df = df[~df['input'].str.contains(r'[^\w][ivx][ivx]+[^\w]') & ~df['input'].str.contains(r'\d')]
    df = df[~df['input'].str.contains(r'^[ivx]+[^\w]') & ~df['input'].str.contains(r'[^\w][ivx]+$')]
    if check_target:
        df = df[~df['target'].str.contains(r' ?.*? (of |de |den |av |di |van |car |da |du |dan |dari |af |von |vo |od |del |der |die |el |vu |han |na |od |lo |cel |the |le |nga |z |o |i |d\').*?')]
        df = df[~df['target'].str.contains(r'(princ|san |pope|papa|pape|paus|emperor|impera|king )')]
        df = df[~df['target'].str.contains(r'[^\w][ivx][ivx]+[^\w]') & ~df['target'].str.contains(r'\d')]
        df = df[~df['target'].str.contains(r'^[ivx][ivx]+[^\w]') & ~df['target'].str.contains(r'[^\w][ivx][ivx]+$')]
    return df

def filter_common_loc(df: pd.DataFrame) -> pd.DataFrame:
    regex = r'( ?river ?| ?creek ?| ?rio ?| ?island ?| ?school ?| ?lake ?| ?church ?| ?afon ?| ?abhainn ?| ?condado ?| ?cemetery ?| ?wadi ?)'
    i_without_common = df['input'].apply(lambda s: re.sub(regex, '', s).strip())
    t_without_common = df['target'].apply(lambda s: re.sub(regex, '', s).strip())
    mask = i_without_common != t_without_common
    print(df[~mask])
    return df[mask]

def filter_common_org(df: pd.DataFrame) -> pd.DataFrame:
    regex = r'( ?international ?| ?internacional ?| ?union ?| ?flughafen ?| ?internationale ?| ?aeroport ?| ?futbol ?| ?bank ?| ?airport ?| ?liga ?)'
    i_without_common = df['input'].apply(lambda s: re.sub(regex, '', s).strip())
    t_without_common = df['target'].apply(lambda s: re.sub(regex, '', s).strip())
    mask = i_without_common != t_without_common
    print(df[~mask])
    return df[mask]

def filter_substr(df: pd.DataFrame) -> pd.DataFrame:
    mask = df.apply(lambda row: row.target.startswith(row.input + ' ') or row.target.endswith(' ' + row.input) or (' '+row.input+' ') in row.target, axis=1)
    mask_2 = df.apply(lambda row: (row.input.startswith(row.target + ' ') or row.input.endswith(' ' + row.target) or (' '+row.input+' ') in row.target) and random.random() <= 0.5, axis=1)
    print(df[mask])
    print(df[mask_2])
    return df[~mask & ~mask_2]

def filter_numbers_only(df: pd.DataFrame) -> pd.DataFrame:
    print(df[df['input'].str.isnumeric() | df['target'].str.isnumeric()])
    return df[~df['input'].str.isnumeric() & ~df['target'].str.isnumeric()]

def filter_abbreviations(df: pd.DataFrame, check_target=True) -> pd.DataFrame:
    df = df[~df['input'].str.contains(r'[^\w]\w\.[^\w]')]
    df = df[~df['input'].str.contains(r'^\w\.') & ~df['input'].str.contains(r'[^\w]\w\.$')]
    if check_target:
        df = df[~df['target'].str.contains(r'[^\w]\w\.[^\w]')]
        df = df[~df['target'].str.contains(r'^\w\.') & ~df['target'].str.contains(r'[^\w]\w\.$')]
    return df
