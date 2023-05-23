import pandas as pd
import unicodedata as ud

latin_letters = {}

def is_latin(uchr):
    try: return latin_letters[uchr]
    except KeyError:
         return latin_letters.setdefault(uchr, 'LATIN' in ud.name(uchr))

def only_roman_chars(unistr):
    return all(is_latin(uchr) for uchr in unistr if uchr.isalpha())


def jrc_to_pairs(input_path: str, entity_type: str='P'):
    df = pd.read_csv(input_path, sep='\t', encoding='utf-8', skiprows=1, header=None, names=['id', 'type', 'lang', 'name'])
    df = df[df['type'] == entity_type]
    df['name'] = df['name'].apply(lambda n: ' '.join([t for t in n.split('+')]))
    alphabet_mask = df['name'].apply(only_roman_chars)
    df = df[alphabet_mask]

    # Filter out entities that occur only once
    agg = df[['id', 'lang']].groupby(by='id', as_index=False).count()
    agg = agg[agg['lang'] > 1]
    ids = set(agg['id'].to_list())
    df = df[df['id'].isin(ids)]
    
    # Create label pairs
    pairs_dict = { 'id': [], 'input': [], 'target': [], 'input_lang': [], 'target_lang': [] }
    ids = df['id'].drop_duplicates().tolist()
    df = df.set_index(df['id'], drop=True)
    steps_n = int(len(ids) / 100.0)
    for ix, id in enumerate(ids):
        if ix % steps_n == 0: print('.', end='')
        
        slice = df.loc[[id]]
        if len(slice) < 2:
            continue

        for input_i, (input_lang, input_label) in enumerate(zip(slice['lang'], slice['name'])):
            for target_i, (target_lang, target_label) in enumerate(zip(slice['lang'], slice['name'])):
                if input_i == target_i: continue
                if input_label == target_label and input_lang == target_lang: continue
                pairs_dict['id'].append(id)
                pairs_dict['input'].append(input_label)
                pairs_dict['target'].append(target_label)
                pairs_dict['input_lang'].append(input_lang)
                pairs_dict['target_lang'].append(target_lang)
    pairs_df = pd.DataFrame(pairs_dict)
    return pairs_df