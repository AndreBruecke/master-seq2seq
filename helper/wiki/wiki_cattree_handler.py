import pandas as pd

from matchers import *


def extract_list_categories(cat_file_path: str, out_path: str):
    with open(cat_file_path, 'r', encoding='utf-8') as cat_f:
        with open(out_path, 'w', encoding='utf-8') as out_f:
            for _, line in enumerate(cat_f):
                if 'Lists_of' not in line and 'List_of' not in line:
                    continue
                out_f.write(line)


def extract_list_pages(cat_file_path: str, out_path: str):
    with open(cat_file_path, 'r', encoding='utf-8') as cat_f:
        with open(out_path, 'w', encoding='utf-8') as out_f:
            for _, line in enumerate(cat_f):
                if 'Lists_of' not in line and 'List_of' not in line:
                    continue
                cols = line.split(',', 1) if not line.startswith('"') else [c.replace('"', '') for c in line.split('",', 1)]
                filtered = [f.strip() for f in cols[1].split(' ') if 'List_of' in f]
                out_f.write(f'{cols[0]}\t{" ".join(filtered)}\n')


def process_person_list_pages(cat_file: str, page_file: str):
    cat_df = pd.read_csv(cat_file, encoding='utf-8', header=None, names=['category', 'sub_categories'], dtype=str)
    pg_df = pd.read_csv(page_file, sep='\t', encoding='utf-8', encoding_errors='ignore', header=None, names=['category', 'sub_pages'], dtype=str, skiprows=[224368])

    cat_df['category'] = cat_df['category'].apply(lambda c: c.lower().replace('_', ' '))
    cat_df['sub_categories'] = cat_df['sub_categories'].apply(lambda c: [sc.replace('_', ' ') for sc in c.lower().split(' ')])

    mask = cat_df['category'].apply(match_list_of_people_categories)
    cat_df = cat_df[mask]

    unique_list_categories = set()
    for i, row in cat_df.iterrows():
        unique_list_categories.add(row['category'])
        for sub_c in row['sub_categories']:
            unique_list_categories.add(sub_c)
    
    pg_df['category'] = pg_df['category'].apply(lambda c: c.lower().replace('_', ' '))
    pg_df = pg_df[pg_df['category'].isin(unique_list_categories)]
    pg_df['sub_pages'] = pg_df['sub_pages'].apply(lambda c: [sc.replace('_', ' ') for sc in c.lower().split(' ')])
    print(pg_df.head())



# extract_list_pages('T:/MasterData/wikipedia_dump/kaggle__wikipedia_category_tree/page_children_cats.csv', 'T:/MasterData/wikipedia_dump/kaggle__wikipedia_category_tree/list_pages.csv')
process_person_list_pages('T:/MasterData/wikipedia_dump/kaggle__wikipedia_category_tree/list_cats.csv', 'T:/MasterData/wikipedia_dump/kaggle__wikipedia_category_tree/list_pages.csv')