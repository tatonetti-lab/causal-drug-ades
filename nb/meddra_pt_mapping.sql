select condition_name, count(distinct drug_name)
FROM user_uog2000.reference_ades
INNER JOIN effect_onsides_v02.adverse_reactions

group by condition_name;

SELECT *
FROM effect_onsides_v02.ingredients
WHERE ingredient_name like '%cesium%'

####
# note olmesartan medoxomil ---
# note one drop -- capreomycin
SELECT concept_code as ingredient_rxcui, concept_name as drug_name, group_concat(distinct pt_meddra_term)
FROM user_uog2000.reference_ades
INNER JOIN concept on drug_concept_id = concept_id
INNER JOIN effect_onsides_v02.adverse_reactions on concept_code = ingredients_rxcuis
WHERE condition_name = 'Acute Kidney Injury'
AND affect = 1
AND (pt_meddra_term like '%renal%' OR pt_meddra_term like '%kidney%')
GROUP BY concept_code, concept_name;

SELECT cohort_id, condition_name, pt_meddra_id, pt_meddra_term, count(distinct ingredients_names)
FROM user_uog2000.reference_ades
INNER JOIN concept on drug_concept_id = concept_id
INNER JOIN effect_onsides_v02.adverse_reactions on concept_code = ingredients_rxcuis
WHERE condition_name = 'Acute Kidney Injury'
AND affect = 1
AND pt_meddra_id in (10038435, 10062237, 10069339)
GROUP BY cohort_id, condition_name, pt_meddra_id, pt_meddra_term;


SELECT *
FROM effect_onsides_v02.adverse_reactions
WHERE pt_meddra_term like '%qt%'

######## ALI

SELECT *
FROM user_uog2000.reference_ades
WHERE condition_name = 'Acute Liver Injury'
AND affect = 1;

SELECT *
FROM user_uog2000.ade_cohort_conditions
INNER JOIN concept on concept_id = condition_concept_id

SELECT concept_code as ingredient_rxcui, concept_name as drug_name, group_concat(distinct pt_meddra_term)
FROM user_uog2000.reference_ades
INNER JOIN concept on drug_concept_id = concept_id
INNER JOIN effect_onsides_v02.adverse_reactions on concept_code = ingredients_rxcuis
WHERE condition_name = 'Acute Liver Injury'
AND affect = 1
AND pt_meddra_id in (10019851, 10019692, 10024690, 10060795, 10000804, 10062685, 10019837, 10019663, 10067125)
GROUP BY concept_code, concept_name;

SELECT cohort_id, condition_name, pt_meddra_id, pt_meddra_term, count(distinct ingredients_names)
FROM user_uog2000.reference_ades
INNER JOIN concept on drug_concept_id = concept_id
INNER JOIN effect_onsides_v02.adverse_reactions on concept_code = ingredients_rxcuis
WHERE condition_name = 'Acute Liver Injury'
AND affect = 1
AND (pt_meddra_term like '%hepat%' OR pt_meddra_term like '%liver%')
GROUP BY cohort_id, condition_name, pt_meddra_id, pt_meddra_term;

SELECT cohort_id, condition_name, pt_meddra_id, pt_meddra_term, count(distinct ingredients_names)
FROM user_uog2000.reference_ades
INNER JOIN concept on drug_concept_id = concept_id
INNER JOIN effect_onsides_v02.adverse_reactions on concept_code = ingredients_rxcuis
WHERE condition_name = 'Acute Liver Injury'
AND affect = 1
AND pt_meddra_id in (10019851, 10019692, 10024690, 10060795, 10000804, 10062685, 10019837, 10019663, 10067125)
GROUP BY cohort_id, condition_name, pt_meddra_id, pt_meddra_term;


## ALI = (10019851, 10019692, 10024690, 10060795, 10000804, 10062685, 10019837) ## 10067125 ## 10019663

######## AMI
## kimitations --- meddra pts acute myo vs. myo

SELECT *
FROM user_uog2000.reference_ades
WHERE condition_name = 'Acute Myocardial Infarction'
AND affect = 1;

SELECT concept_code as ingredient_rxcui, concept_name as drug_name, group_concat(distinct pt_meddra_term)
FROM user_uog2000.reference_ades
INNER JOIN concept on drug_concept_id = concept_id
INNER JOIN effect_onsides_v02.adverse_reactions on concept_code = ingredients_rxcuis
WHERE condition_name = 'Acute Myocardial Infarction'
AND affect = 1
AND (pt_meddra_term like '%myocardial infar%')
GROUP BY concept_code, concept_name;

SELECT cohort_id, condition_name, pt_meddra_id, pt_meddra_term, count(distinct ingredients_names)
FROM user_uog2000.reference_ades
INNER JOIN concept on drug_concept_id = concept_id
INNER JOIN effect_onsides_v02.adverse_reactions on concept_code = ingredients_rxcuis
WHERE condition_name = 'Acute Myocardial Infarction'
AND affect = 1
AND (pt_meddra_term like '%myocardial infar%')
GROUP BY cohort_id, condition_name, pt_meddra_id, pt_meddra_term;


######## GIB
## 24
SELECT *
FROM user_uog2000.reference_ades
WHERE condition_name = 'GI bleed'
AND affect = 1;

SELECT concept_code as ingredient_rxcui, concept_name as drug_name, group_concat(distinct pt_meddra_term)
FROM user_uog2000.reference_ades
INNER JOIN concept on drug_concept_id = concept_id
INNER JOIN effect_onsides_v02.adverse_reactions on concept_code = ingredients_rxcuis
WHERE condition_name = 'GI bleed'
AND affect = 1
AND pt_meddra_id in (10017955, 10018830)
GROUP BY concept_code, concept_name;

SELECT cohort_id, condition_name, pt_meddra_id, pt_meddra_term, count(distinct ingredients_names)
FROM user_uog2000.reference_ades
INNER JOIN concept on drug_concept_id = concept_id
INNER JOIN effect_onsides_v02.adverse_reactions on concept_code = ingredients_rxcuis
WHERE condition_name = 'GI bleed'
AND affect = 1
AND ((pt_meddra_term like '%gastro%') OR (pt_meddra_term like '%Haematemesis%'))
GROUP BY cohort_id, condition_name, pt_meddra_id, pt_meddra_term;



SELECT condition_name as 'Condition'
    , cohort_id as 'Cohort ID'
    , condition_concept_id as 'OMOP Concept ID'
    , concept_name as 'Condition Name'
FROM user_uog2000.ade_cohort_conditions
INNER JOIN concept on concept_id=condition_concept_id