import pandas as pd
import unicodedata as ud

latin_letters = {}

def is_latin(uchr):
    try: return latin_letters[uchr]
    except KeyError:
         return latin_letters.setdefault(uchr, 'LATIN' in ud.name(uchr))

def only_roman_chars(unistr):
    return all(is_latin(uchr) for uchr in unistr if uchr.isalpha())


def geonames_to_entities(input_path: str) -> pd.DataFrame:
    df = pd.read_csv(input_path, sep='\t', encoding='utf-8', header=None, names=['c0', 'geonameid', 'lang', 'name', 'isPreferredName', 'isShortName', 'isColloquial', 'isHistoric', 'c2', 'c3'])
    df = df[['geonameid', 'lang', 'name', 'isPreferredName', 'isShortName', 'isColloquial', 'isHistoric']]
    df = df[~df['geonameid'].isna() & ~df['name'].isna()]
    df = df[~df['lang'].isin(['unlc', 'wkdt', 'link', 'post'])]
    df = df[~df['name'].str.contains('^[A-Z]{1,4}$') & ~df['name'].str.contains('\(.*?\)')]
    
    alphabet_mask = df['name'].apply(only_roman_chars)
    df = df[alphabet_mask]

    df = df[df['isColloquial'].isna()]
    df = df[df['isHistoric'].isna()]
    df = df[df['isShortName'].isna()]

    # Filter out entities that occur only once
    agg = df[['geonameid', 'name']].groupby(by='geonameid', as_index=False).name.nunique()
    agg = agg[agg['name'] > 1]
    ids = set(agg['geonameid'].to_list())
    df = df[df['geonameid'].isin(ids)]

    df = df[['geonameid', 'name']]
    df = df.rename(columns={'geonameid': 'group', 'name': 'target'})
    return df