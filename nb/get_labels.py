import polars as pl
from glob import glob
import sys
import json

# get label information

def available_labels(label_dir):
    active_labels = glob(label_dir+'*/*.json')
    all_labels = []
    count = 0
    for label in active_labels:
        # Opening JSON file
        f = open(label)
        data = json.load(f)
        count += 1
        all_labels.append([label, data['set_id'], data['spl_version'], data['title']])

    label_df = pl.DataFrame(all_labels, schema=[("file", pl.Utf8),
                                                ("set_id", pl.Utf8),
                                                ("spl_version", pl.Int64),
                                                ("title", pl.Utf8)])
    return label_df


def main():
    # get available labels 
    # 2024 file, set_id, spl_version, title
    label_df = available_labels('./data/2024_latest_labels/')

    # get rxnorm to label file name
    ## rxnorm_mappings: set_id, spl_version, rxcui, rxstring
    ## rxnorm_ingredient: product_rx_cui, product_name, product_omop_concept_id, ingredient_rx_cui, ingredient_name, ingredient_omop_concept_id
    rxnorm_2024_mappings = pl.read_csv('./data/20240312/rxnorm_mappings.csv'
                                       ).select(
                                            pl.col('SETID').alias('set_id'),
                                            pl.col('SPL_VERSION').alias('spl_version'),
                                            pl.col('RXCUI'), #.alias('ingredients_rxcuis'),
                                            pl.col('RXSTRING')) 
    rxnorm_ingredient = pl.read_csv('./data/20240312/rxnorm_product_to_ingredient.csv')
    ## combine into product table
    ## SETID, SPL_VERSION, ingredient_rx_cui, ingredient_name
    product_2024 = (rxnorm_2024_mappings.join(rxnorm_ingredient,
                           right_on=['product_rx_cui'],
                           left_on = 'RXCUI', how='inner')
                     .select(
                         pl.col('set_id'),
                         pl.col('spl_version'),
                         pl.col('ingredient_rx_cui'),
                         pl.col('ingredient_name')
                     ))
    
    # get files mapped from rxnorm
    ## file, set_id, spl_version, title, ingredient_rx_cui, ingredient_name
    mapped_files = label_df.join(product_2024, on=['set_id', 'spl_version'], how='inner')
    missed_files = label_df['file'].unique().shape[0] - mapped_files['file'].unique().shape[0]
    print(f"Files mapped {mapped_files['file'].unique().shape[0]}")
    print(f"Files not mapped {missed_files}")

    # map to onsides, to get extracted ADRS
    ## pt_meddra_id, pt_meddra_term, ingredient_rx_cui, ingredient_name
    adr_section = (
    pl.read_csv('/Users/undinagisladottir/Documents/Columbia/Tatonetti_Lab/20231113_onsides/adverse_reactions.csv',
                          dtypes={
                            'pt_meddra_id': pl.Int64,
                            'pt_meddra_term': pl.Utf8,
                            'num_ingredients':  pl.Int64,
                            'ingredients_rxcuis': pl.Utf8,
                            'ingredients_names': pl.Utf8
                          })
                          .filter(pl.col('num_ingredients') == 1)
                          .select(
                              pl.col('pt_meddra_id'),
                              pl.col('pt_meddra_term').str.to_lowercase(),
                              pl.col('ingredients_rxcuis').cast(pl.Int64).alias('ingredient_rx_cui'),
                              pl.col('ingredients_names').str.to_lowercase().alias('ingredient_name')
                              )
    )
    
    onsides_mapped_files = mapped_files.join(adr_section, on=['ingredient_rx_cui', 'ingredient_name'], how='inner')
    print(f"Files in Onsides: {onsides_mapped_files['file'].unique().shape[0]}")

    # get mapping between condition_name to meddra_pt terms
    # cohort_id, condition_name, meddra_pt_id, meddra_pt_term, positive_controls (N)
    meddra_mappings = (pl.read_csv('./data/onsides_mapping.csv')
                       
    # join with the meddra_pt mappings to get condition_name
                   .select(
                       pl.col('cohort_id'),
                       pl.col('condition_name').str.to_lowercase(),
                       pl.col('meddra_pt_id').alias('pt_meddra_id'),
                       pl.col('meddra_pt_term').str.to_lowercase().alias('pt_meddra_term'),
                       pl.col('Positive Controls (N)').alias('positive_controls')               
                       )
                   )
    onsides_w_ADRs = onsides_mapped_files.join(meddra_mappings, on=['pt_meddra_id', 'pt_meddra_term'], how='inner')
    print(f"Total number of labels w/ ADR in reference set: {onsides_w_ADRs['file'].unique().shape[0]}")    
    ## get reference set
    pryan_set = (pl.read_csv('./data/pryan_reference_set_ades.csv')
                 .select(
                    pl.col('cohort_id'), 
                    pl.col('condition_name').str.to_lowercase(),
                    'drug_concept_id',
                    pl.col('drug_name').str.to_lowercase().alias('ingredient_name'),
                    'affect'
                    )
                .with_columns(
                    ingredient_name=pl.when(pl.col('ingredient_name')=='olmesartan medoxomil')
                            .then(pl.lit('olmesartan'))
                            .otherwise(pl.col('ingredient_name')))
                )
    
    ## get reference set in onsides
    pryan_onsides = onsides_w_ADRs.with_columns(
        label = 1
        ).join(
            pryan_set.with_columns(reference = 1), on=['ingredient_name', 'condition_name'], how='left'
            )

    # create table summarizing the number of positive controls captured -- write to csv
    (pryan_set.with_columns(reference = 1).join(
            onsides_w_ADRs.with_columns(label = 1),  on=['ingredient_name', 'condition_name'], how='left'
        ).filter(pl.col('affect') == 1)
        .select('file',
                'condition_name', 
                'ingredient_name',
                'label',
                'reference').unique().sort(['condition_name', 'ingredient_name']).fill_null(0)
        .group_by(['condition_name']).agg(pl.sum('label').alias('labels'), pl.sum('reference').alias('reference'))
        .with_columns(missed = pl.col('reference') - pl.col('labels'))
    ).write_csv('./data/label_reference_comparison.csv')

    # create table with all ADR and drug pairs and whether they were captured or not
    (pryan_set.with_columns(reference = 1).join(
            onsides_w_ADRs.with_columns(label = 1),  on=['ingredient_name', 'condition_name'], how='left'
        ).filter(pl.col('affect') == 1)
        .select('file',
                'condition_name', 
                'ingredient_name',
                'label',
                'reference').unique().sort(['condition_name', 'ingredient_name']).fill_null(0)
    ).write_csv('./data/label_reference_drugs_captured.csv')

    # write table breakdown of positive, negative, and non-positive drugs
    (pryan_onsides.group_by('condition_name', 'affect')
          .agg(pl.col('ingredient_name').n_unique().alias('unique_drugs'))
          .sort('condition_name', 'affect')).write_csv('./data/ADR_drug_breakdown.csv')
    
    # write table with all label names and ADRs and pos/non-pos
    pryan_onsides.unique().write_csv('./data/labels_w_ADRs.csv')
    
main()