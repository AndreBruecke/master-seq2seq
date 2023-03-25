import pandas as pd

label_file = 'T:/MasterData/wikidata_dump/human_labels.tsv'


def filter_single_occurrences(in_file: str, out_file: str):
    df = pd.read_csv(in_file, sep='\t', encoding='utf-8')
    agg = df[['id', 'lang']].groupby(by='id', as_index=False).count()
    print('Size before:', len(agg))
    agg = agg[agg['lang'] > 1]
    print('Size after:', len(agg))
    ids = set(agg['id'].to_list())
    df = df[df['id'].isin(ids)]
    df.to_csv(out_file, index=False, sep='\t', encoding='utf-8')



filter_single_occurrences(label_file, label_file.replace('.tsv', '_filtered.tsv'))
