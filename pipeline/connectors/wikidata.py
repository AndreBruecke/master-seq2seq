import pandas as pd

def wikidata_to_pairs(df: pd.DataFrame) -> pd.DataFrame:
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