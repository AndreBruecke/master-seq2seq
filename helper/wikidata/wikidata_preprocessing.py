import pandas as pd
import os
import re
import sys

from abydos.phonetic import FuzzySoundex
from unidecode import unidecode
from fuzzyset import FuzzySet


label_file = 'T:/MasterData/wikidata_dump/human_labels.tsv'
focussed_languages = dict([(char, i) for i, char in enumerate(['en', 'fr', 'de', 'nl', 'it', 'es', 'pt', 'pl', 'sq', 'sv', 'cs', 'hu', 'da', 'sl', 'fi', 'nn', 'tr', 'lv', 'vi', 'id'])])

def normalize(sequence: str):
    return unidecode(sequence.strip().lower())

def apply_basic_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    # Remove brackets, e.g. "Max Mustermann (job description)" -> "Max Mustermann"
    df['label'] = df['label'].apply(lambda l: re.sub(r'\(.*?\)', '', l).strip())
    # Remove abbreviations, e.g. "George W. Bush" -> "George Bush"
    df['label'] = df['label'].apply(lambda l: re.sub(r' ([A-Za-z]\.)+ ', ' ', l).strip())
    # Filter out historic figures with titles, e.g. "Charles de Secondat, Baron de Montesquieu"
    df = df[~df['label'].str.contains(r',.*?')]
    return df

def filter_single_occurrences(df: pd.DataFrame) -> pd.DataFrame:
    agg = df[['id', 'lang']].groupby(by='id', as_index=False).count()
    print('Size before:', len(agg))
    agg = agg[agg['lang'] > 1]
    print('Size after:', len(agg))
    ids = set(agg['id'].to_list())
    df = df[df['id'].isin(ids)]
    return df

def to_pairs(df: pd.DataFrame) -> pd.DataFrame:
    pairs_dict = { 'id': [], 'label_en': [], 'label': [], 'lang': [] }
    df = df[df['lang'].isin(focussed_languages.keys())]
    print(len(df))
    ids = df['id'].drop_duplicates().tolist()
    print(len(ids))
    df = df.set_index(df['id'], drop=True)
    for n, id in enumerate(ids):
        if n % 4393 == 0:
            print(f'{n} / {len(ids)}')
        slice = df.loc[[id]]
        if len(slice) < 2 or 'en' not in slice['lang'].tolist():
            continue
        label_en = slice[slice['lang'] == 'en'].iloc[0]['label']
        for lang, label in zip(slice['lang'], slice['label']):
            pairs_dict['id'].append(id)
            pairs_dict['label_en'].append(label_en)
            pairs_dict['label'].append(label)
            pairs_dict['lang'].append(lang)
    pairs_df = pd.DataFrame(pairs_dict)            
    return pairs_df[pairs_df['lang'] != 'en']

def filter_pairs(df: pd.DataFrame) -> pd.DataFrame:
    def match_terms(_in):
        if len(_in['label']) == len(_in['label_en']):
            return _in
        elif len(_in['label']) > len(_in['label_en']):
            if len(set(_in['label_en']).intersection(_in['label'])) == len(_in['label_en']):
                _in['label'] = []
            else:
                pass
        return _in
    df['label'] = df['label'].apply(normalize)
    df['label_en'] = df['label_en'].apply(normalize)

    df['label_en'] = df['label_en'].apply(lambda l: re.split(r' +', l.lower()))
    df['label'] = df['label'].apply(lambda l: re.split(r' +', l.lower()))

    df = df[df['label'].str.len() >= df['label_en'].str.len()]
    df = df[df['label'] != df['label_en']]

    df = df.apply(match_terms, axis=1)
    df = df[df['label'].str.len() > 1]
    
    return df

def remove_distant_pairs(df: pd.DataFrame) -> pd.DataFrame:
    cp_df = df.copy(deep=True)
    # pe = FuzzySoundex(max_length=8)
    cp_df['label'] = cp_df['label'].apply(normalize).apply(lambda l: re.split(r' +', l.lower()))
    cp_df['label_en'] = cp_df['label_en'].apply(normalize).apply(lambda l: re.split(r' +', l.lower()))
    # df['label_phntc'] = df['label'].apply(lambda l: [pe.encode_alpha(p) for p in l])
    # df['label_en_phntc'] = df['label_en'].apply(lambda l: [pe.encode_alpha(p) for p in l])
    
    drop_msk = []
    for ix, row in cp_df.iterrows():
        if ix % 10000 == 0:
            print(f'{ix} done, only {len(df)-ix} left to process.')
        l = FuzzySet(use_levenshtein=True)
        r = FuzzySet(use_levenshtein=True)
        for tkn in row['label']: l.add(tkn)
        for tkn in row['label_en']: r.add(tkn)
        l_scores = [l.get(tkn) for tkn in row['label_en']]
        r_scores = [r.get(tkn) for tkn in row['label']]
        if any([e is None for e in l_scores]) or any([e is None for e in r_scores]) or any([e[0][0] < 0.2 for e in l_scores]) or any([e[0][0] < 0.2 for e in r_scores]):
            l_chars_missing = 0
            r_chars_missing = 0
            for i in range(len(l_scores)):
                if l_scores[i] is None or l_scores[i][0][0] < 0.2:
                    l_chars_missing += len(row['label_en'][i])
            for i in range(len(r_scores)):
                if r_scores[i] is None or r_scores[i][0][0] < 0.2:
                    r_chars_missing += len(row['label'][i])
            if l_chars_missing > 2 or r_chars_missing > 2:
                drop_msk.append(False)
                continue
        drop_msk.append(True)
    df = df[drop_msk]
    df = df[df['label'] != df['label_en']]
    return df


# def transliteration_stats(in_file: str):
#     df = pd.read_csv(in_file, sep='\t', encoding='utf-8')
#     most_common_deviations = df[['lang', 'label']].groupby(by='lang', as_index=False)['label'].count().sort_values(by='label', ascending=False)
#     print(most_common_deviations.head(50))



# df = pd.read_csv(label_file, sep='\t', encoding='utf-8')
# df = apply_basic_cleaning(df)
# df = filter_single_occurrences(df)
# df.to_csv(label_file.replace('.tsv', '_filtered.tsv'), index=False, sep='\t', encoding='utf-8')
# pairs_df = to_pairs(df)

pairs_df = pd.read_csv(label_file.replace('.tsv', '_pairs.tsv'), sep='\t', encoding='utf-8')
pairs_df = remove_distant_pairs(pairs_df)
pairs_df.to_csv(label_file.replace('.tsv', '_pairs_distfilter.tsv'), index=False, sep='\t', encoding='utf-8')
