import pandas as pd

def wikidata_to_pairs(input_path: str) -> pd.DataFrame:
    df = pd.read_csv(input_path, sep='\t', encoding='utf-8')
    # Filter out entities that occur only once
    agg = df[['id', 'lang']].groupby(by='id', as_index=False).count()
    agg = agg[agg['lang'] > 1]
    ids = set(agg['id'].to_list())
    df = df[df['id'].isin(ids)]

    # Create label pairs
    pairs_dict = { 'id': [], 'input': [], 'target': [], 'lang': [] }
    ids = df['id'].drop_duplicates().tolist()
    df = df.set_index(df['id'], drop=True)
    for id in ids:
        slice = df.loc[[id]]
        if len(slice) < 2 or 'en' not in slice['lang'].tolist():
            continue
        label_en = slice[slice['lang'] == 'en'].iloc[0]['label']
        for lang, label in zip(slice['lang'], slice['label']):
            pairs_dict['id'].append(id)
            pairs_dict['input'].append(label_en)
            pairs_dict['target'].append(label)
            pairs_dict['lang'].append(lang)
    pairs_df = pd.DataFrame(pairs_dict)
    return pairs_df[pairs_df['lang'] != 'en']

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
    