import ast
import pandas as pd
import re

import unicodedata as ud
from unidecode import unidecode
from rapidfuzz.distance import Levenshtein
import textdistance

pd.options.display.width = 0

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


def denorm_movielens(cast_input_path: str, crew_input_path: str, cast_output_path: str, crew_output_path: str):
    df = pd.read_csv(cast_input_path, sep='\t', encoding='utf-8')
    df['cast'] = df['cast'].apply(ast.literal_eval)

    def remove_unwanted_cast_attributes(col):
        for e in col:
            del e['cast_id']
            del e['credit_id']
            del e['order']
            del e['profile_path']
        return col

    df['cast'] = df['cast'].apply(remove_unwanted_cast_attributes)
    df = df.explode('cast').reset_index(drop=True)
    cast_df = pd.json_normalize(df['cast'])
    df = pd.concat([df[['id']], cast_df], axis=1)
    df = df.rename(columns={'character': 'disambiguation'})
    df.to_csv(cast_output_path, sep='\t', encoding='utf-8', index=False)
    print(df.head())

    df = pd.read_csv(crew_input_path, sep='\t', encoding='utf-8')
    df['crew'] = df['crew'].apply(ast.literal_eval)

    def remove_unwanted_crew_attributes(col):
        for e in col:
            del e['credit_id']
            del e['department']
            del e['profile_path']
        return col

    df['crew'] = df['crew'].apply(remove_unwanted_crew_attributes)
    df = df.explode('crew').reset_index(drop=True)
    crew_df = pd.json_normalize(df['crew'])
    df = pd.concat([df[['id']], crew_df], axis=1)
    df = df.rename(columns={'job': 'disambiguation'})
    df.to_csv(crew_output_path, sep='\t', encoding='utf-8', index=False)
    print(df.head())


def denorm_imdb_selection(imdb_name_path: str, imdb_principals_path: str, out_path: str):
    imdb_names = pd.read_csv(imdb_name_path, encoding='utf-8', sep='\t', dtype=str)
    imdb_names = imdb_names.drop(['knownForTitles', 'primaryProfession', 'deathYear'], axis=1)

    imdb_principals = pd.read_csv(imdb_principals_path, encoding='utf-8', sep='\t',
                                  dtype=str)
    imdb_principals = imdb_principals.drop(['ordering', 'job', 'characters'], axis=1)

    denorm = imdb_names.merge(imdb_principals, on='nconst')
    denorm.to_csv(out_path, index=False, sep='\t', encoding='utf-8')


def combine_imdb_movielens(ldir: str):
    imdb_df = pd.read_csv(ldir + 'imdb_in_movielens_denorm.tsv', sep='\t', encoding='utf-8', dtype=str)

    movielens_links = pd.read_csv(ldir + 'kaggle_movielens_links.csv', sep=',', dtype=str)
    movielens_links['tconst'] = movielens_links['imdbId'].apply(lambda i_id: 'tt' + i_id)
    movielens_links = movielens_links.drop(['movieId', 'imdbId'], axis=1)

    linked_df = imdb_df.merge(movielens_links, on='tconst')

    movielens_cast = pd.read_csv(ldir + 'kaggle_movielens_credits_cast.denorm.csv', sep='\t', encoding='utf-8', dtype=str)
    movielens_crew = pd.read_csv(ldir + 'kaggle_movielens_credits_crew.denorm.csv', sep='\t', encoding='utf-8', dtype=str)
    movielens_names = pd.concat([movielens_cast, movielens_crew]).drop(['id.1'], axis=1)
    movielens_names = movielens_names.rename(columns={'id': 'tmdbId'})

    combined_df = linked_df.merge(movielens_names, on='tmdbId')
    combined_df.to_csv(ldir + 'movielens_imdb_combined.tsv', sep='\t', index=False, encoding='utf-8')


# ldir = 'D:/HDa/Thesis/Repos/master-seq2seq/large_data/'
"""
combined_df = pd.read_csv(ldir + 'movielens_imdb_combined_dedup.tsv', sep='\t', encoding='utf-8')
combined_df = combined_df[combined_df['imdb_name'] != combined_df['movielens_name']]
combined_df = combined_df[combined_df['imdb_name'].str[0] == combined_df['movielens_name'].str[0]]
combined_df.to_csv(ldir + 'movielens_imdb_combined_dedup_blocking.tsv', sep='\t', index=False, encoding='utf-8')


potential_matches = pd.read_csv(ldir + 'movielens_imdb_combined_dedup_blocking.tsv', sep='\t', encoding='utf-8')
potential_matches['ro_sim'] = 0

for i, row in potential_matches.iterrows():
    potential_matches.at[i, 'ro_sim'] = textdistance.ratcliff_obershelp.normalized_similarity(row['imdb_name'], row['movielens_name'])

potential_matches.to_csv(ldir + 'movielens_imdb_combined_dedup_blocking_sim.tsv', sep='\t', encoding='utf-8')


potential_matches = pd.read_csv(ldir + 'movielens_imdb_combined_dedup_blocking_sim.tsv', sep='\t', encoding='utf-8').drop(['ix'], axis=1)
potential_matches['imdb_name'] = potential_matches['imdb_name'].apply(lambda s: s.strip())
potential_matches['movielens_name'] = potential_matches['movielens_name'].apply(lambda s: s.strip())
potential_matches = potential_matches[potential_matches['imdb_name'] != potential_matches['movielens_name']]
likely_matches = potential_matches[potential_matches['ro_sim'] > 0.85]
print(len(likely_matches))
print(likely_matches.sort_values(by='ro_sim', ascending=True).head(60))



temp = 'placeholder'
with open(ldir + 'imdb_name.basics/data.tsv', encoding='utf-8') as f:
    with open(ldir + 'imdb_name.basics/data-in-movielens.tsv', 'w', encoding='utf-8') as out_f:
        for i, line in enumerate(f):
            if i == 0:
                out_f.write(line)
                continue
            elif i % 90000 == 0:
                print('.', end='')
            n_id = line.split('\t')[0]
            if n_id in name_ids:
                out_f.write(line)
"""
