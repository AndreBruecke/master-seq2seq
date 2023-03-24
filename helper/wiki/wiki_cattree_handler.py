import pandas as pd

from matchers import *


def extract_list_categories(cat_file_path: str, out_path: str):
    with open(cat_file_path, 'r', encoding='utf-8') as cat_f:
        with open(out_path, 'w', encoding='utf-8') as out_f:
            for _, line in enumerate(cat_f):
                if 'Lists_of' not in line and 'List_of' not in line:
                    continue
                out_f.write(line)


 
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
    pg_df['sub_pages'] = pg_df['sub_pages'].apply(lambda c: [sc.replace('_', ' ') for sc in str(c).lower().split(' ')])
    
    # Interesting for test data: lists of people sharing a surname; lists of families
    drop_terms = [
        'military personnel', 'monarchs', 'century', 'fictional', 'characters'
    ]

    drop_list = [
        3763, 5491, 9609, 12563, 27922, 27935, 32141, 33224, 33634, 35756, 40327, 45899, 48108, 48766, 50977, 52340, 52624, 53552, 54154, 54155, 55635, 55777, 66302, 68068, 69648, 76939, 79660,
        84163, 86000, 88112, 88504, 93886, 93958, 95261, 96533, 97032, 97844, 99433, 100909, 105834, 107217, 108351, 112327, 116031, 117265, 117786, 118154, 120749, 121750, 122699, 128740, 129344,
        130456, 134522, 139558, 139978, 141950, 142718, 142722, 142844, 144652, 147469, 148866, 149279, 155376, 159070, 159156, 163261, 166463, 168498, 169849, 174654, 176888, 178271, 178511, 179582,
        180260, 182067, 192845, 194217, 194903, 196552, 196553, 196554, 196555, 196556, 196557, 196558, 196559, 196560, 201851, 206029, 211842, 212561
    ]
    drop_list = set(drop_list)
    pg_df = pg_df[~pg_df.index.isin(drop_list)]

    unique_list_pages = set()
    for i, row in pg_df.iterrows():
        if row['sub_pages'][0] == 'nan': continue
        for sc in row['sub_pages']:
            unique_list_pages.add(sc)
    
    unique_list_pages = list(unique_list_pages)
    with open('T:/MasterData/wikipedia_dump/kaggle__wikipedia_category_tree/selection_of_lists.txt', 'w', encoding='utf-8') as out_f:
        out_f.write('\n'.join(unique_list_pages))



# extract_list_pages('T:/MasterData/wikipedia_dump/kaggle__wikipedia_category_tree/page_children_cats.csv', 'T:/MasterData/wikipedia_dump/kaggle__wikipedia_category_tree/list_pages.csv')
process_person_list_pages('T:/MasterData/wikipedia_dump/kaggle__wikipedia_category_tree/list_cats.csv', 'T:/MasterData/wikipedia_dump/kaggle__wikipedia_category_tree/list_pages.csv')