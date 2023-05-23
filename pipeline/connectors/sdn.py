import pandas as pd
import unicodedata as ud

latin_letters = {}

def is_latin(uchr):
    try: return latin_letters[uchr]
    except KeyError:
         return latin_letters.setdefault(uchr, 'LATIN' in ud.name(uchr))

def only_roman_chars(unistr):
    return all(is_latin(uchr) for uchr in unistr if uchr.isalpha())


def sdn_to_variants(name_input_path: str, alt_input_path: str, entity_type: str='individual'):
    name_df = pd.read_csv(name_input_path, sep='|', encoding='utf-8', header=None, names=['id', 'name', 'type', '_3', '_4', '_5', '_6', '_7', '_8', '_9', '_10', '_11'], skipfooter=1, dtype=str)[['id', 'name', 'type']]
    name_df = name_df.replace({'type': {'-0-': 'entity', 'aircraft': 'entity', 'vessel': 'entity'}})
    alphabet_mask = name_df['name'].apply(only_roman_chars)
    name_df = name_df[alphabet_mask]
    alt_df = pd.read_csv(alt_input_path, sep='|', encoding='utf-8', header=None, names=['id', '_1', '_2', 'alt_name', '_4'], skipfooter=1, dtype=str)[['id', 'alt_name']]
    alphabet_mask = alt_df['alt_name'].apply(only_roman_chars)
    alt_df = alt_df[alphabet_mask]

    name_df = name_df[name_df['type'] == entity_type]
    joined = name_df.set_index('id').join(alt_df.set_index('id'), how='left', rsuffix='_2')[['name', 'alt_name']]
    joined = joined[~joined['alt_name'].isna()]
    return joined
