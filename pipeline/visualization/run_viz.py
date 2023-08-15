import pandas as pd
import unicodedata as ud
from unidecode import unidecode

# SOURCES
WIKIDATA_HUMAN_QUERY_RESULT = './data/pipeline_inputs/wikidata_human_query_result.tsv'
WIKIDATA_ORG_QUERY_RESULT = './data/pipeline_inputs/wikidata_organization_query_result.tsv'
WIKIDATA_LOC_QUERY_RESULT = './data/pipeline_inputs/wikidata_location_query_result.tsv'

TOPONYM_P = './data/pipeline_inputs/toponym_matching/dataset_final_jrc_person.csv'
TOPONYM_ORG = './data/pipeline_inputs/toponym_matching/dataset_final_jrc_organization.csv'
TOPONYM_LOC = './data/pipeline_inputs/toponym_matching/dataset-string-similarity.txt'

GEONAMES = './data/pipeline_inputs/geonames.org/alternateNamesV2.txt'

OUTPUT_FOLDER = './data/visualization/'

def remove_diacritics(series: pd.Series) -> pd.Series:
    # Removes diacritics from string column
    return series.apply(unidecode)

latin_letters = {}

def is_latin(uchr):
    try: return latin_letters[uchr]
    except KeyError:
         return latin_letters.setdefault(uchr, 'LATIN' in ud.name(uchr))

def only_roman_chars(unistr):
    return all(is_latin(uchr) for uchr in unistr if uchr.isalpha())


def wikidata_to_viz_input(input_path: str) -> pd.DataFrame:
    df = pd.read_csv(input_path, sep='\t', encoding='utf-8')
    df = df.dropna()
    df['label'] = remove_diacritics(df['label'])
    df['label'] = df['label'].apply(lambda l: l.lower().strip())

    # Filter out entities that occur only once
    agg = df[['id', 'lang']].groupby(by='id', as_index=False).count()
    agg = agg[agg['lang'] > 1]
    ids = set(agg['id'].to_list())
    df = df[df['id'].isin(ids)]
    return df

def toponym_to_viz_input(input_path: str, jrc: bool = False) -> pd.DataFrame:
    if jrc:
        df = pd.read_csv(input_path, sep='|', encoding='utf-8', header=None, dtype=str)
        df = df.rename(columns={0: 'input', 1: 'target', 2: 'label'})
    else:
        df = pd.read_csv(input_path, '\t', encoding='utf-8', header=None, dtype=str)
        df = df.rename(columns={0: 'input', 1: 'target', 2: 'label', 7: 'lang1', 8: 'lang2'})[['input', 'target', 'label', 'lang1', 'lang2']]
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
    df['input'] = remove_diacritics(df['input'])
    df['target'] = remove_diacritics(df['target'])
    df = df[~((df['input'] == df['target']) & (df['label'] != 'TRUE'))]
    return df.drop_duplicates()

def geonames_to_viz_input(input_path: str) -> pd.DataFrame:
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

    df['name'] = remove_diacritics(df['name'])
    df = df.drop_duplicates()

    # Filter out entities that occur only once
    agg = df[['geonameid', 'name']].groupby(by='geonameid', as_index=False).name.nunique()
    agg = agg[agg['name'] > 1]
    ids = set(agg['geonameid'].to_list())
    df = df[df['geonameid'].isin(ids)]
    df = df[['geonameid', 'lang', 'name']]
    return df


for input in [["WIKIDATA_ORG", WIKIDATA_ORG_QUERY_RESULT], ["WIKIDATA_LOC", WIKIDATA_LOC_QUERY_RESULT], ["WIKIDATA_HUMAN", WIKIDATA_HUMAN_QUERY_RESULT]]:
    print('\n---\n', input[0])
    df = wikidata_to_viz_input(input[1])
    print(df.head(3))
    print()
    print(len(df), 'unique names')
    print(len(df['id'].drop_duplicates()), 'entities')
    df['count'] = 1
    variants_per_entity = df[['id', 'count']].groupby(by='id', as_index=False)['count'].count()
    variants_per_entity.to_csv(OUTPUT_FOLDER+input[0]+'_variants_per_entity.csv', sep='|', index=False)
    top_ids = variants_per_entity.sort_values(by='count', ascending=False).head(10)['id'].tolist()
    df[df['id'].isin(top_ids)].to_csv(OUTPUT_FOLDER+input[0]+'_top_10_variants.csv', sep='|', index=False, encoding='utf-8')
    variants_per_lang = df[['lang', 'count']].groupby(by='lang', as_index=False)['count'].count()
    variants_per_lang.to_csv(OUTPUT_FOLDER+input[0]+'_variants_per_lang.csv', sep='|', index=False)
    df['length'] = df['label'].str.len()
    length_counts = df[['length', 'count']].groupby(by='length', as_index=False)['count'].count()
    length_counts.to_csv(OUTPUT_FOLDER+input[0]+'_length_counts.csv', sep='|', index=False)

print('\n---\nGEONAMES')
df = geonames_to_viz_input(GEONAMES)
print(df.head(3))
print()
print(len(df), 'unique names')
print(len(df['geonameid'].drop_duplicates()), 'entities')
df['count'] = 1
variants_per_entity = df[['geonameid', 'count']].groupby(by='geonameid', as_index=False)['count'].count()
variants_per_entity.to_csv(OUTPUT_FOLDER+'GEONAMES_variants_per_entity.csv', sep='|', index=False)
top_ids = variants_per_entity.sort_values(by='count', ascending=False).head(10)['geonameid'].tolist()
df[df['geonameid'].isin(top_ids)].to_csv(OUTPUT_FOLDER+'GEONAMES_top_10_variants.csv', sep='|', index=False, encoding='utf-8')
variants_per_lang = df[['lang', 'count']].groupby(by='lang', as_index=False)['count'].count()
variants_per_lang.to_csv(OUTPUT_FOLDER+'GEONAMES_variants_per_lang.csv', sep='|', index=False)
df['length'] = df['name'].str.len()
length_counts = df[['length', 'count']].groupby(by='length', as_index=False)['count'].count()
length_counts.to_csv(OUTPUT_FOLDER+'GEONAMES_length_counts.csv', sep='|', index=False)

print('\n---\nTOPONYM_ORG')
df = toponym_to_viz_input(TOPONYM_ORG, jrc=True)
print(df.head(3))
print()
print(len(df), 'pairs')
print('\t', len(df[df['label'] == 'TRUE']), 'true pairs')
print('\t', len(df[df['label'] == 'FALSE']), 'false pairs')
lbl_df = pd.concat([df[['input']].rename(columns={'input': 'name'}), df[['target']].rename(columns={'target': 'name'})])
lbl_df['count'] = 1
lbl_df['length'] = lbl_df['name'].str.len()
length_counts = lbl_df[['length', 'count']].groupby(by='length', as_index=False)['count'].count()
length_counts.to_csv(OUTPUT_FOLDER+'TOPONYM_ORG_length_counts.csv', sep='|', index=False)
df.sample(100, random_state=1000).to_csv(OUTPUT_FOLDER+'TOPONYM_ORG_sample.csv', sep='|', index=False, encoding='utf-8')

print('\n---\nTOPONYM_P')
df = toponym_to_viz_input(TOPONYM_P, jrc=True)
print(df.head(3))
print()
print(len(df), 'pairs')
print('\t', len(df[df['label'] == 'TRUE']), 'true pairs')
print('\t', len(df[df['label'] == 'FALSE']), 'false pairs')
lbl_df = pd.concat([df[['input']].rename(columns={'input': 'name'}), df[['target']].rename(columns={'target': 'name'})])
lbl_df['count'] = 1
lbl_df['length'] = lbl_df['name'].str.len()
length_counts = lbl_df[['length', 'count']].groupby(by='length', as_index=False)['count'].count()
length_counts.to_csv(OUTPUT_FOLDER+'TOPONYM_P_length_counts.csv', sep='|', index=False)
df.sample(100, random_state=1000).to_csv(OUTPUT_FOLDER+'TOPONYM_P_sample.csv', sep='|', index=False, encoding='utf-8')

print('\n---\nTOPONYM_LOC')
df = toponym_to_viz_input(TOPONYM_LOC)
print(df.head(3))
print()
print(len(df), 'pairs')
print('\t', len(df[df['label'] == 'TRUE']), 'true pairs')
print('\t', len(df[df['label'] == 'FALSE']), 'false pairs')

lbl_df = pd.concat([df[['input']].rename(columns={'input': 'name'}), df[['target']].rename(columns={'target': 'name'})])
lbl_df['count'] = 1
lbl_df['length'] = lbl_df['name'].str.len()
length_counts = lbl_df[['length', 'count']].groupby(by='length', as_index=False)['count'].count()
length_counts.to_csv(OUTPUT_FOLDER+'TOPONYM_LOC_length_counts.csv', sep='|', index=False)
df['count'] = 1
variants_per_lang = df[['lang1', 'lang2', 'count']].groupby(by=['lang1', 'lang2'], as_index=False)['count'].count()
variants_per_lang.to_csv(OUTPUT_FOLDER+'TOPONYM_LOC_pairs_per_lang.csv', sep='|', index=False)
df.sample(100, random_state=1000).to_csv(OUTPUT_FOLDER+'TOPONYM_LOC_sample.csv', sep='|', index=False, encoding='utf-8')