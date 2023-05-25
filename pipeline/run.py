from preprocessors.normalize import *
from preprocessors.filter import *
from connectors import *

# SOURCES
WIKIDATA_HUMAN_QUERY_RESULT = './data/pipeline_inputs/wikidata_human_query_result.tsv'
JRC_ENTITIES = './data/pipeline_inputs/jrc_entities'
# Evaluation sources
SDN_NAMES = './data/pipeline_inputs/us_sdn_names.pip'
SDN_ALT = './data/pipeline_inputs/us_sdn_alt.pip'

# UNPROCESSED input/target pairs (after running connectors)
WIKIDATA_HUMAN_PAIRS = './data/pipeline_inputs/wikidata_human_pairs.csv'
WIKIDATA_HUMAN_VARIANT_LIST = './data/pipeline_inputs/wikidata_human_variant_list.csv'
JRC_HUMAN_PAIRS = './data/pipeline_inputs/jrc_human_pairs.csv'

# PROCESSED pairs
WIKIDATA_P_SIMILAR_PAIRS_NORMALIZED = './data/wikidata_person_similar_pairs_norm.csv'
JRC_P_SIMILAR_PAIRS_NORMALIZED = './data/jrc_person_similar_pairs_norm.csv'

pipelines = {
    'wikidata_similar_pairs_normalized': [
        # Pipeline to create WIKIDATA_P_SIMILAR_PAIRS_NORMALIZED
        { 'func': filter_titles, 'columns': None, 'params': None },
        { 'func': remove_brackets, 'columns': ['input', 'target'], 'params': None },
        { 'func': remove_abbreviations, 'columns': ['input', 'target'], 'params': None },
        { 'func': remove_diacritics, 'columns': ['input', 'target'], 'params': None },
        { 'func': to_lower, 'columns': ['input', 'target'], 'params': None },
        { 'func': filter_distant_pairs, 'columns': None, 'params': {'distance_threshold': 0.35} },
        { 'func': filter_different_token_length_pairs, 'columns': None, 'params': None },
        { 'func': filter_equal, 'columns': None, 'params': None },
    ],
    'jrc_similar_pairs_normalized': [
        # Pipeline to create JRC_P_SIMILAR_PAIRS_NORMALIZED
        { 'func': remove_diacritics, 'columns': ['input', 'target'], 'params': None },
        { 'func': replace_characters, 'columns': ['input', 'target'], 'params': {'to_replace': [['`','\''], [' & ',' '], [' $ ',' '], [' == ',' '], [' << ',' '],]} },
        { 'func': filter_special_characters, 'columns': None, 'params': {'to_drop': r'#|=|<|>|~|\?|\*|@|\d|\.\.|\{|\}|\+|&|\\|!|%|:|\[|\]|"|_|\(|\)'} },
        { 'func': to_lower, 'columns': ['input', 'target'], 'params': None },
        { 'func': filter_equal, 'columns': None, 'params': None },
        { 'func': drop_duplicates, 'columns': None, 'params': None },
        { 'func': filter_distant_pairs, 'columns': None, 'params': {'distance_threshold': 0.35} },
        { 'func': filter_different_token_length_pairs, 'columns': None, 'params': None },
    ],
    'wikidata_to_en_normalized': [
        # Pipeline to create JRC_P_TO_EN_NORMALIZED based on WIKIDATA_P_SIMILAR_PAIRS_NORMALIZED
        
    ]
}


def run_step(data, step_def):
    print(f"\t> Running step: {step_def['func'].__name__}")
    func = step_def['func']
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
    df.to_csv(output_path, sep='|', index=False, encoding='utf-8')



if __name__ == '__main__':
    # run_pipeline('jrc_similar_pairs_normalized', JRC_HUMAN_PAIRS, '|', JRC_P_SIMILAR_PAIRS_NORMALIZED)
    pass
