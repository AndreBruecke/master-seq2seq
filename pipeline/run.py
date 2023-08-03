from preprocessors.normalize import *
from preprocessors.filter import *
from connectors import *

# SOURCES
WIKIDATA_HUMAN_QUERY_RESULT = './data/pipeline_inputs/wikidata_human_query_result.tsv'
WIKIDATA_ORG_QUERY_RESULT = './data/pipeline_inputs/wikidata_organization_query_result.tsv'
WIKIDATA_LOC_QUERY_RESULT = './data/pipeline_inputs/wikidata_location_query_result.tsv'
JRC_ENTITIES = './data/pipeline_inputs/jrc_entities'
TOPONYM_P = './data/pipeline_inputs/toponym_matching/dataset_final_jrc_person.csv'
TOPONYM_ORG = './data/pipeline_inputs/toponym_matching/dataset_final_jrc_organization.csv'
TOPONYM_LOC = './data/pipeline_inputs/toponym_matching/dataset-string-similarity.txt'
# Evaluation sources
SDN_NAMES = './data/pipeline_inputs/us_sdn_names.pip'
SDN_ALT = './data/pipeline_inputs/us_sdn_alt.pip'

# UNPROCESSED input/target pairs (after running connectors)
WIKIDATA_HUMAN_PAIRS = './data/pipeline_inputs/wikidata_human_pairs.csv'
WIKIDATA_ORG_PAIRS = './data/pipeline_inputs/wikidata_organization_pairs.csv'
WIKIDATA_LOC_PAIRS = './data/pipeline_inputs/wikidata_location_pairs.csv'
WIKIDATA_HUMAN_VARIANT_LIST = './data/pipeline_inputs/wikidata_human_variant_list.csv'
JRC_HUMAN_PAIRS = './data/pipeline_inputs/jrc_human_pairs.csv'
SIMILARITY_HUMAN_GROUPS = './data/pipeline_inputs/similarity_human_groups.csv'
SIMILARITY_LOC_GROUPS = './data/pipeline_inputs/similarity_loc_groups.csv'
SIMILARITY_ORG_GROUPS = './data/pipeline_inputs/similarity_org_groups.csv'
TOPONYM_P_LABELED_PAIRS = './data/pipeline_inputs/toponym_p_labeled_pairs.csv'
TOPONYM_ORG_LABELED_PAIRS = './data/pipeline_inputs/toponym_org_labeled_pairs.csv'
TOPONYM_LOC_LABELED_PAIRS = './data/pipeline_inputs/toponym_loc_labeled_pairs.csv'

# PROCESSED pairs
WIKIDATA_P_SIMILAR_PAIRS_NORMALIZED = './data/wikidata_person_similar_pairs_norm.csv'
WIKIDATA_ORG_SIMILAR_PAIRS_NORMALIZED = './data/wikidata_org_similar_pairs_norm.csv'
WIKIDATA_LOC_SIMILAR_PAIRS_NORMALIZED = './data/wikidata_loc_similar_pairs_norm.csv'
WIKIDATA_P_TO_EN_NORMALIZED = './data/wikidata_person_to_en_norm.csv'
WIKIDATA_LOC_TO_EN_NORMALIZED = './data/wikidata_loc_to_en_norm.csv'
WIKIDATA_P_VARIANT_LIST_NORMALIZED = './data/wikidata_person_variant_list_norm.csv'
JRC_P_SIMILAR_PAIRS_NORMALIZED = './data/jrc_person_similar_pairs_norm.csv'

# PROCESSED for evaluation
JRC_P_TO_EN_EVALUATION = './data/evaluation/jrc_person_to_en_norm.csv'
SIMILARITY_P_TRAIN = './data/evaluation/similarity_p_train.csv'
SIMILARITY_LOC_TRAIN = './data/evaluation/similarity_loc_train.csv'
SIMILARITY_ORG_TRAIN = './data/evaluation/similarity_org_train.csv'
SIMILARITY_TOPONYM_P_SAMPLE = './data/evaluation/similarity_toponym_p_sample.csv'
SIMILARITY_TOPONYM_LOC_SAMPLE = './data/evaluation/similarity_toponym_loc_sample.csv'
SIMILARITY_TOPONYM_ORG_SAMPLE = './data/evaluation/similarity_toponym_org_sample.csv'

pipelines = {
    'wikidata_similar_pairs_normalized': [
        # Pipeline to create WIKIDATA_P_SIMILAR_PAIRS_NORMALIZED
        { 'func': lambda df: df.fillna(''), 'columns': None, 'params': None },
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
        # Pipeline to create WIKIDATA_P_TO_EN_NORMALIZED based on WIKIDATA_P_SIMILAR_PAIRS_NORMALIZED
        { 'func': lambda df: df.rename(columns={'input': 'target', 'target': 'input'}), 'columns': None, 'params': None }
    ],
    'wikidata_variant_list_normalized': [
        # Pipeline to create WIKIDATA_P_VARIANT_LIST_NORMALIZED
        { 'func': lambda df: df[['id', 'input', 'target']], 'columns': None, 'params': None },
        { 'func': lambda t: t.fillna(''), 'columns': ['target'], 'params': None },
        { 'func': remove_brackets, 'columns': ['input', 'target'], 'params': None },
        { 'func': remove_abbreviations, 'columns': ['input', 'target'], 'params': None },
        { 'func': remove_diacritics, 'columns': ['input', 'target'], 'params': None },
        { 'func': to_lower, 'columns': ['input', 'target'], 'params': None },
        { 'func': filter_different_token_length_variants, 'columns': None, 'params': {'keep_empty': True} },
        { 'func': drop_duplicates, 'columns': None, 'params': None },
        { 'func': filter_equal, 'columns': None, 'params': None },
    ],
    'jrc_to_en_evaluation': [
        # Pipeline to create JRC_P_TO_EN_EVALUATION from JRC_P_SIMILAR_PAIRS_NORMALIZED
        { 'func': lambda df: df[df['target_lang'] == 'en'], 'columns': None, 'params': None }
    ],
    'similarity_train': [
        # Pipeline to create training data for similarity learning:
        { 'func': lambda df: df.fillna(''), 'columns': None, 'params': None },
        { 'func': remove_brackets, 'columns': ['target'], 'params': None },
        { 'func': remove_diacritics, 'columns': ['target'], 'params': None },
        { 'func': replace_characters, 'columns': ['target'], 'params': {'to_replace': [['"',''],]} },
        { 'func': to_lower, 'columns': ['group', 'target'], 'params': None },
        { 'func': drop_duplicates, 'columns': None, 'params': None },
    ],
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
    # Connector usage
    # df = wikidata_to_pairs(WIKIDATA_LOC_QUERY_RESULT)
    df_p = toponym_labeled_pairs_to_sample(TOPONYM_P_LABELED_PAIRS)
    df_loc = toponym_labeled_pairs_to_sample(TOPONYM_LOC_LABELED_PAIRS)
    df_org = toponym_labeled_pairs_to_sample(TOPONYM_ORG_LABELED_PAIRS)
    df_p.to_csv(SIMILARITY_TOPONYM_P_SAMPLE, sep='\t', index=False, header=False, encoding='utf-8')
    df_loc.to_csv(SIMILARITY_TOPONYM_LOC_SAMPLE, sep='\t', index=False, header=False, encoding='utf-8')
    df_org.to_csv(SIMILARITY_TOPONYM_ORG_SAMPLE, sep='\t', index=False, header=False, encoding='utf-8')
    
    # Pipeline
    # run_pipeline('similarity_train', SIMILARITY_HUMAN_GROUPS, '|', SIMILARITY_P_TRAIN)
    # run_pipeline('similarity_train', SIMILARITY_LOC_GROUPS, '|', SIMILARITY_LOC_TRAIN)
    # run_pipeline('similarity_train', SIMILARITY_ORG_GROUPS, '|', SIMILARITY_ORG_TRAIN)
    pass
