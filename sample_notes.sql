create temporary table user_uog2000.x_condition_era_count_by_person as
    SELECT person_id,
           count(condition_era_id) as ndx
    FROM
        condition_era
    GROUP BY person_id;

select drug_concept_id, count(distinct person_id) c, sum(ndx > 10), sum(ndx > 100), sum(ndx > 1000)
from user_uog2000.reference_ades
join user_uog2000.x_condition_era_count_by_person using (person_id)
where datediff(first_drug_era_start_date, firstdx) > 6*30 # at least 6 mo. of data before first Rx date
and ndx > 10
group by drug_concept_id
having c >= 100
order by 2 desc


