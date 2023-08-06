from connectors.jrc import jrc_to_pairs
from connectors.wikidata import wikidata_to_pairs
from connectors.wikidata import wikidata_to_variant_list

from connectors.sdn import sdn_to_variants

from connectors.embeddings import to_merged_groups

from connectors.toponym import toponym_to_labeled_pairs
from connectors.toponym import toponym_labeled_pairs_to_sample

from connectors.geonames import geonames_to_entities