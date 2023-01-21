import pandas as pd

import unicodedata as ud
from unidecode import unidecode

latin_letters = {}

def is_latin(uchr):
    try: return latin_letters[uchr]
    except KeyError:
         return latin_letters.setdefault(uchr, 'LATIN' in ud.name(uchr))

def only_roman_chars(unistr):
    return all(is_latin(uchr) for uchr in unistr if uchr.isalpha())


def process_jrc(input_path: str, e_type: str='P'):
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