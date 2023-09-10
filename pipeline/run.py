from typing import Iterable

from preprocessors.normalize import *
from preprocessors.filter import *
from connectors import *

# SOURCES
WIKIDATA_HUMAN_QUERY_RESULT = './data/pipeline_inputs/wikidata_human_query_result.tsv'
WIKIDATA_ORG_QUERY_RESULT = './data/pipeline_inputs/wikidata_organization_query_result.tsv'
WIKIDATA_LOC_QUERY_RESULT = './data/pipeline_inputs/wikidata_location_query_result.tsv'
TOPONYM_P = './data/pipeline_inputs/toponym_matching/dataset_final_jrc_person.csv'
TOPONYM_ORG = './data/pipeline_inputs/toponym_matching/dataset_final_jrc_organization.csv'
TOPONYM_LOC = './data/pipeline_inputs/toponym_matching/dataset-string-similarity.txt'
GEONAMES = './data/pipeline_inputs/geonames.org/alternateNamesV2.txt'
JRC_ENTITIES = './data/pipeline_inputs/jrc_entities'
# Evaluation sources
SDN_NAMES = './data/pipeline_inputs/us_sdn_names.pip'
SDN_ALT = './data/pipeline_inputs/us_sdn_alt.pip'

# UNPROCESSED input/target pairs (after running connectors)
WIKIDATA_HUMAN_PAIRS = './data/pipeline_inputs/wikidata_human_pairs_full.csv'
WIKIDATA_ORG_PAIRS = './data/pipeline_inputs/wikidata_organization_pairs_full.csv'
WIKIDATA_LOC_PAIRS = './data/pipeline_inputs/wikidata_location_pairs_full.csv'
WIKIDATA_HUMAN_VARIANT_LIST = './data/pipeline_inputs/wikidata_human_variant_list.csv'
JRC_HUMAN_PAIRS = './data/pipeline_inputs/jrc_human_pairs.csv'
SIMILARITY_HUMAN_GROUPS = './data/pipeline_inputs/similarity_human_groups.csv'
SIMILARITY_LOC_GROUPS = './data/pipeline_inputs/similarity_loc_groups.csv'
SIMILARITY_ORG_GROUPS = './data/pipeline_inputs/similarity_org_groups.csv'
GEONAMES_GROUPS = './data/pipeline_inputs/geonames_groups.csv'
GEONAMES_PAIRS = './data/pipeline_inputs/geonames_pairs.csv'
TOPONYM_P_LABELED_PAIRS = './data/pipeline_inputs/toponym_p_labeled_pairs.csv'
TOPONYM_ORG_LABELED_PAIRS = './data/pipeline_inputs/toponym_org_labeled_pairs.csv'
TOPONYM_LOC_LABELED_PAIRS = './data/pipeline_inputs/toponym_loc_labeled_pairs.csv'

# PROCESSED pairs
WIKIDATA_P_BASIC_NORMALIZED = './data/wikidata_per_basic_norm.csv'
WIKIDATA_ORG_BASIC_NORMALIZED = './data/wikidata_org_basic_norm.csv'
WIKIDATA_LOC_BASIC_NORMALIZED = './data/wikidata_loc_basic_norm.csv'
GEONAMES_BASIC_NORMALIZED = './data/geonames_basic_norm.csv'
TOPONYM_P_BASIC_NORMALIZED = './data/toponym_per_basic_norm.csv'
TOPONYM_LOC_BASIC_NORMALIZED = './data/toponym_loc_basic_norm.csv'
TOPONYM_ORG_BASIC_NORMALIZED = './data/toponym_org_basic_norm.csv'

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

# EXPERIMENT data
E1_PER_TRAIN_VAL = './data/experiments/per_train_val_e1.csv'
E1_LOC_TRAIN_VAL = './data/experiments/loc_train_val_e1.csv'
E1_ORG_TRAIN_VAL = './data/experiments/org_train_val_e1.csv'
E1_PER_TEST = './data/experiments/per_test.csv'
E1_LOC_TEST = './data/experiments/loc_test.csv'
E1_ORG_TEST = './data/experiments/org_test.csv'
E2_PER_TRAIN_VAL = './data/experiments/per_train_val_e2.csv'
E2_LOC_TRAIN_VAL = './data/experiments/loc_train_val_e2.csv'
E2_ORG_TRAIN_VAL = './data/experiments/org_train_val_e2.csv'

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
    'wikidata_basic_normalized': [
        { 'func': lambda df: df.dropna(), 'columns': None, 'params': None },
        { 'func': remove_diacritics, 'columns': ['input', 'target'], 'params': None },
        { 'func': to_lower, 'columns': ['input', 'target'], 'params': None },
        { 'func': remove_brackets, 'columns': ['input', 'target'], 'params': None },
        { 'func': filter_special_characters, 'columns': None, 'params': {'to_drop': r'#|\$|§|@|"|,,'} },
        { 'func': drop_duplicates, 'columns': None, 'params': None },
        { 'func': filter_equal, 'columns': None, 'params': None },
        { 'func': filter_large_token_diff, 'columns': None, 'params': None },
        { 'func': filter_large_character_diff, 'columns': None, 'params': None },
    ],
    'toponym_basic_normalized': [
        { 'func': lambda df: df.dropna(), 'columns': None, 'params': None },
        { 'func': lambda df: df[df['label'] == 'TRUE'], 'columns': None, 'params': None },
        { 'func': remove_diacritics, 'columns': ['input', 'target'], 'params': None },
        { 'func': to_lower, 'columns': ['input', 'target'], 'params': None },
        { 'func': remove_brackets, 'columns': ['input', 'target'], 'params': None },
        { 'func': filter_special_characters, 'columns': None, 'params': {'to_drop': r'#|\$|§|@|"|,,'} },
        { 'func': drop_duplicates, 'columns': None, 'params': None },
        { 'func': filter_equal, 'columns': None, 'params': None },
        { 'func': lambda df: df[['input', 'target']], 'columns': None, 'params': None },
        { 'func': filter_large_token_diff, 'columns': None, 'params': None },
        { 'func': filter_large_character_diff, 'columns': None, 'params': None },
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
    'e2_per': [
        { 'func': lambda df: df.dropna(), 'columns': None, 'params': None },
        # Removing pairs, that have a token difference of more than N
        { 'func': filter_large_token_diff, 'columns': None, 'params': {'target_threshold': 1, 'input_threshold': 2} },
        # Removing pairs, where the target is more than N characters longer compared to the input
        { 'func': filter_large_character_diff, 'columns': None, 'params': {'target_threshold': 12, 'input_threshold': 20} },
        { 'func': filter_historic_per, 'columns': None, 'params': None },
        { 'func': filter_abbreviations, 'columns': None, 'params': None },
        { 'func': filter_substr, 'columns': None, 'params': None },
    ],
    'e2_loc': [
        { 'func': lambda df: df.dropna(), 'columns': None, 'params': None },
        # Removing pairs, that have a token difference of more than N
        { 'func': filter_large_token_diff, 'columns': None, 'params': {'target_threshold': 1, 'input_threshold': 2} },
        # Removing pairs, where the target is more than N characters longer compared to the input
        { 'func': filter_large_character_diff, 'columns': None, 'params': {'target_threshold': 12, 'input_threshold': 20} },
        { 'func': filter_numbers_only, 'columns': None, 'params': None },
        { 'func': filter_common_loc, 'columns': None, 'params': None },
        { 'func': filter_substr, 'columns': None, 'params': None },
    ],
    'e2_org': [
        { 'func': lambda df: df.dropna(), 'columns': None, 'params': None },
        # Removing pairs, that have a token difference of more than N
        { 'func': filter_large_token_diff, 'columns': None, 'params': {'target_threshold': 2, 'input_threshold': 3} },
        # Removing pairs, where the target is more than N characters longer compared to the input
        { 'func': filter_large_character_diff, 'columns': None, 'params': {'target_threshold': 15, 'input_threshold': 25} },
        { 'func': filter_numbers_only, 'columns': None, 'params': None },
        { 'func': filter_common_org, 'columns': None, 'params': None },
        { 'func': filter_substr, 'columns': None, 'params': None },
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
    df = pd.read_csv(input_path, sep=input_sep, encoding='utf-8', dtype=str)
    for step in pipelines[pipeline_name]:
        df = run_step(df, step)
    df.to_csv(output_path, sep='|', index=False, encoding='utf-8')


if __name__ == '__main__':
    # Connector usage
    # df = wikidata_to_pairs(WIKIDATA_LOC_QUERY_RESULT)
    # df_p = toponym_labeled_pairs_to_sample(TOPONYM_P_LABELED_PAIRS)
    # df_loc = toponym_labeled_pairs_to_sample(TOPONYM_LOC_LABELED_PAIRS)
    # df_org = toponym_labeled_pairs_to_sample(TOPONYM_ORG_LABELED_PAIRS)
    # df_p.to_csv(SIMILARITY_TOPONYM_P_SAMPLE, sep='\t', index=False, header=False, encoding='utf-8')
    # df_loc.to_csv(SIMILARITY_TOPONYM_LOC_SAMPLE, sep='\t', index=False, header=False, encoding='utf-8')
    # df_org.to_csv(SIMILARITY_TOPONYM_ORG_SAMPLE, sep='\t', index=False, header=False, encoding='utf-8')

    # Pipeline   
    # run_pipeline('wikidata_basic_normalized', WIKIDATA_HUMAN_PAIRS, '|', WIKIDATA_P_BASIC_NORMALIZED)
    # run_pipeline('wikidata_basic_normalized', WIKIDATA_LOC_PAIRS, '|', WIKIDATA_LOC_BASIC_NORMALIZED)
    # run_pipeline('wikidata_basic_normalized', WIKIDATA_ORG_PAIRS, '|', WIKIDATA_ORG_BASIC_NORMALIZED)
    # run_pipeline('wikidata_basic_normalized', GEONAMES_PAIRS, '|', GEONAMES_BASIC_NORMALIZED)

    # run_pipeline('toponym_basic_normalized', TOPONYM_P_LABELED_PAIRS, '|', TOPONYM_P_BASIC_NORMALIZED)
    # run_pipeline('toponym_basic_normalized', TOPONYM_LOC_LABELED_PAIRS, '|', TOPONYM_LOC_BASIC_NORMALIZED)
    # run_pipeline('toponym_basic_normalized', TOPONYM_ORG_LABELED_PAIRS, '|', TOPONYM_ORG_BASIC_NORMALIZED)

    run_pipeline('e2_per', E1_PER_TRAIN_VAL, '|', E2_PER_TRAIN_VAL)
    run_pipeline('e2_loc', E1_LOC_TRAIN_VAL, '|', E2_LOC_TRAIN_VAL)
    run_pipeline('e2_org', E1_ORG_TRAIN_VAL, '|', E2_ORG_TRAIN_VAL)
    pass
