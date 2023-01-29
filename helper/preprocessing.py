import pandas as pd

import unicodedata as ud
from unidecode import unidecode
# from rapidfuzz.distance import Levenshtein


latin_letters = {}

def is_latin(uchr):
    try: return latin_letters[uchr]
    except KeyError:
         return latin_letters.setdefault(uchr, 'LATIN' in ud.name(uchr))

def only_roman_chars(unistr):
    return all(is_latin(uchr) for uchr in unistr if uchr.isalpha())


def normalize(sequence: str):
    return unidecode(sequence.strip().lower())


def process_jrc(input_path: str, e_type: str='P', length_restriction: int=70):
    jrc_df = pd.read_csv(input_path, sep='|', encoding='utf-8')
    jrc_df['name'] = jrc_df['name'].apply(lambda n: ' '.join([t.lower() for t in n.split('+')]))
    alphabet_mask = jrc_df['name'].apply(only_roman_chars)
    jrc_groups = jrc_df[(jrc_df['type'] == e_type) & alphabet_mask][['id', 'name']].groupby(by='id')['name'].apply(list)

    # Constructing variant pairs
    pairs_dict = { 'input': [], 'target': [] }
    for i, val in jrc_groups.iteritems():
        input = unidecode(val[0])
        for variant in val[1:]:
            target = unidecode(variant)
            if input == target: continue  # Variant only differs in diacritics
            pairs_dict['input'].append(input)
            pairs_dict['target'].append(target)
    pairs_df = pd.DataFrame(pairs_dict).drop_duplicates()
    pairs_df = pairs_df[(pairs_df['input'].str.len() <= length_restriction) & (pairs_df['target'].str.len() <= length_restriction)]
    pairs_df['input'] = pairs_df['input'].apply(lambda s: s.lower().strip())
    pairs_df['target'] = pairs_df['target'].apply(lambda s: s.lower().strip())

    # Dropping or replacing unwanted special characters
    to_drop = r'#|=|<|>|~|\?|\*|@|\d|\.\.|\{|\}|\+|&|\\|!|%|:|\[|\]|"|_|\(|\)'
    to_replace = [
        ['`', '\''], [' & ', ' '], [' $ ', ' '], [' == ', ' '], [' << ', ' '], 
    ]
    def repl(s):
        for r in to_replace:
            s = s.replace(r[0], r[1])
        return s
    pairs_df['target'] = pairs_df['target'].apply(repl)
    pairs_df['input'] = pairs_df['input'].apply(repl)
    pairs_df = pairs_df[~pairs_df['target'].str.contains(to_drop) & ~pairs_df['input'].str.contains(to_drop)]

    return pairs_df


def process_common_name_collection(surname_path: str, givenname_path: str, dist_restriction: float=0.5):
    surname_variant_df = pd.read_csv(surname_path, encoding='utf-8', header=None, names=['name', 'variants'])
    givenname_variant_df = pd.read_csv(givenname_path, encoding='utf-8', header=None, names=['name', 'variants'])

    surname_variant_df = surname_variant_df[~surname_variant_df['variants'].isna()]
    surname_variant_df['variants'] = surname_variant_df['variants'].apply(lambda vs: vs.split(' '))
    
    surname_pairs_dict = { 'input': [], 'target': [] }
    for _, row in surname_variant_df.iterrows():
        for v in row['variants']:
            dist_norm = Levenshtein.normalized_distance(row['name'], v)
            if dist_norm <= dist_restriction:
                surname_pairs_dict['input'].append(row['name'])
                surname_pairs_dict['target'].append(v)

    givenname_pairs_dict = { 'input': [], 'target': [] }
    for _, row in givenname_variant_df.iterrows():
        for v in row['variants']:
            dist_norm = Levenshtein.normalized_distance(row['name'], v)
            if dist_norm <= dist_restriction:
                givenname_pairs_dict['input'].append(row['name'])
                givenname_pairs_dict['target'].append(v)

    return pd.DataFrame(surname_pairs_dict), pd.DataFrame(givenname_pairs_dict)


def process_db_of_notable_people(input_path: str):
    df = pd.read_csv(input_path, sep='|', encoding='utf-8', encoding_errors='replace')
    df['name'] = df['name'].apply(lambda s: ' '.join(s.lower().split('_')))
