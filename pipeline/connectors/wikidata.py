import pandas as pd

def wikidata_to_pairs(input_path: str) -> pd.DataFrame:
    df = pd.read_csv(input_path, sep='\t', encoding='utf-8')
    df = df.dropna().drop_duplicates()

    df = df.merge(right=df, how='inner', on='id')
    df = df.rename(columns={'label_x':'input', 'label_y':'target'}).reset_index(drop=True)[['id', 'input', 'target']]
    return df[df['input'] != df['target']].drop_duplicates()

def wikidata_to_variant_list(input_path: str) -> pd.DataFrame:
    df = pd.read_csv(input_path, sep='\t', encoding='utf-8')

    groups = df.groupby(by='id').agg({'label': lambda x: list(x), 'lang': lambda x: list(x)})
    
    # Create label pairs
    v_dict = { 'id': [], 'input': [], 'target': [], 'input_langs': [] }
    for ix, row in enumerate(groups.itertuples()):
        for i, input in enumerate(row[1]):
            target = set()
            for t, target_candidate in enumerate(row[1]):
                if i != t and target_candidate != input and target_candidate not in target:
                    target.add(target_candidate)
            v_dict['id'].append(row[0])
            v_dict['input'].append(input)
            v_dict['target'].append('#'.join(target))
            v_dict['input_langs'].append(row[2][i])

    v_df = pd.DataFrame(v_dict)
    return v_df.groupby(by=['id', 'input', 'target'], as_index=False).agg({'input_langs': lambda x: list(x)})
    