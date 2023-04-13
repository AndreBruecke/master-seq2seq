import pandas as pd
import re

from unidecode import unidecode

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



# def transliteration_stats(in_file: str):
#     df = pd.read_csv(in_file, sep='\t', encoding='utf-8')
#     most_common_deviations = df[['lang', 'label']].groupby(by='lang', as_index=False)['label'].count().sort_values(by='label', ascending=False)
#     print(most_common_deviations.head(50))



# df = pd.read_csv(label_file, sep='\t', encoding='utf-8')
# df = apply_basic_cleaning(df)
# df = filter_single_occurrences(df)
# df.to_csv(label_file.replace('.tsv', '_filtered.tsv'), index=False, sep='\t', encoding='utf-8')
# pairs_df = to_pairs(df)

# pairs_df = pd.read_csv(label_file.replace('.tsv', '_pairs.tsv'), sep='\t', encoding='utf-8')
# filter_pairs(pairs_df.copy(deep=True))
# pairs_df.to_csv(label_file.replace('.tsv', '_pairs.tsv'), index=False, sep='\t', encoding='utf-8')
