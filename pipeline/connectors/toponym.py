import pandas as pd
import unicodedata as ud

latin_letters = {}

def is_latin(uchr):
    try: return latin_letters[uchr]
    except KeyError:
         return latin_letters.setdefault(uchr, 'LATIN' in ud.name(uchr))

def only_roman_chars(unistr):
    return all(is_latin(uchr) for uchr in unistr if uchr.isalpha())


def toponym_to_labeled_pairs(input_path: str, jrc: bool = False) -> pd.DataFrame:
    df = pd.read_csv(input_path, sep='|' if jrc else '\t', encoding='utf-8', header=None, dtype=str)
    df = df.rename(columns={0: 'input', 1: 'target', 2: 'label'})[['input', 'target', 'label']]
    df = df.dropna()
    if jrc:
        df['input'] = df['input'].apply(lambda n: ' '.join([t for t in n.lower().strip().split('+')]))
        df['target'] = df['target'].apply(lambda n: ' '.join([t for t in n.lower().strip().split('+')]))
        df['label'] = df['label'].replace(to_replace={'1': 'TRUE', '0': 'FALSE'})
    else:
        df['input'] = df['input'].apply(lambda n: n.lower().strip())
        df['target'] = df['target'].apply(lambda n: n.lower().strip())
    alphabet_mask_i = df['input'].apply(only_roman_chars)
    alphabet_mask_t = df['target'].apply(only_roman_chars)
    df = df[alphabet_mask_i & alphabet_mask_t]
    df = df[~((df['input'] == df['target']) & (df['label'] != 'TRUE'))]
    return df.drop_duplicates()

def toponym_labeled_pairs_to_sample(input_path: str, sample_size: int = 200000, random_state: int = 1000) -> pd.DataFrame:
    df = pd.read_csv(input_path, sep='|', encoding='utf-8', dtype=str)
    true_sample = df[df['label'] == 'TRUE'].sample(n=sample_size, random_state=random_state)
    false_sample = df[df['label'] == 'FALSE'].sample(n=sample_size, random_state=random_state)
    return pd.concat([true_sample, false_sample])