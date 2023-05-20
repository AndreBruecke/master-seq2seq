from preprocessors.normalize import *
from preprocessors.filter import *

WIKIDATA_HUMAN_QUERY_RESULT = './data/pipeline_inputs/wikidata_human_query_result.tsv'
WIKIDATA_HUMAN_PAIRS = './data/pipeline_inputs/wikidata_human_pairs.tsv'
WIKIDATA_SIMILAR_PAIRS_NORMALIZED = './data/wikidata_similar_pairs_norm.csv'

steps = {
    'filter_titles': filter_titles,
    'filter_distant_pairs': filter_distant_pairs,
    'filter_equal_pairs': filter_equal,
    'to_lower': to_lower,
    'remove_diacritics': remove_diacritics,
    'remove_brackets': remove_brackets,
    'remove_abbreviations': remove_abbreviations,
}

pipelines = {
    'wikidata_similar_pairs_normalized': [
        { 'name': 'filter_titles', 'columns': None, 'params': None },
        { 'name': 'remove_brackets', 'columns': ['input', 'target'], 'params': None },
        { 'name': 'remove_abbreviations', 'columns': ['input', 'target'], 'params': None },
        { 'name': 'remove_diacritics', 'columns': ['input', 'target'], 'params': None },
        { 'name': 'to_lower', 'columns': ['input', 'target'], 'params': None },
        { 'name': 'filter_distant_pairs', 'columns': None, 'params': None },
        { 'name': 'filter_equal_pairs', 'columns': None, 'params': None },
    ]
}


def run_step(data, step_def):
    print(f"\t> Running step: {step_def['name']}")
    func = steps[step_def['name']]
    if step_def['columns'] is None:
        if step_def['params'] is None:
            return func(data)
        else:
            return func(data, **step_def['params'])
    else:
        for c in step_def['columns']:
            if step_def['params'] is None:
                data[c] = func(data[c])
            else:
                data[c] = func(data[c], **step_def['params'])
        return data


def run_pipeline(pipeline_name: str, input_path: str, input_sep: str, output_path: str):
    print("=" * 25)
    print(f"RUNNING PIPELINE: {pipeline_name}")
    print("=" * 25)
    df = pd.read_csv(input_path, sep=input_sep, encoding='utf-8')
    for step in pipelines[pipeline_name]:
        df = run_step(df, step)
        print(df.head())
    df.to_csv(output_path, sep='|', index=False, encoding='utf-8')



if __name__ == '__main__':
    run_pipeline('wikidata_similar_pairs_normalized', WIKIDATA_HUMAN_PAIRS, '\t', WIKIDATA_SIMILAR_PAIRS_NORMALIZED)
