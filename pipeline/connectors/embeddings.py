import pandas as pd

def to_merged_groups(wikidata_input_paths, undersampling_min: int = 500000) -> pd.DataFrame:
    all_dfs = []
    for path in wikidata_input_paths:
        df = pd.read_csv(path, sep='\t', encoding='utf-8')[['id', 'label']]
        # Filter out entities that occur only once
        agg = df.groupby(by='id', as_index=False).count()
        agg = agg[agg['label'] > 1]
        ids = set(agg['id'].to_list())
        df = df[df['id'].isin(ids)]
        df = df.drop_duplicates()
        all_dfs.append(df)

    min_len = min([len(d) for d in all_dfs])
    min_len = min_len if min_len > undersampling_min else undersampling_min
    for i, d in enumerate(all_dfs):
        if len(d) <= min_len: continue
        all_dfs[i] = d.sample(min_len)

    agg = df.groupby(by='id', as_index=False).count()
    agg = agg[agg['label'] > 1]
    ids = set(agg['id'].to_list())
    df = df[df['id'].isin(ids)]

    df =  pd.concat(all_dfs)
    df = df.rename(columns={'id': 'group', 'label': 'target'})
    return df