
/* These two temporary tables are essential to ensure that your model can run smoothely in Python */

/* Below creates your debtor metrics */ 

-- create or replace temporary table edwprodhh.pub_mbutler.temporary_full_table as (
    with zip as (
        
        select 
            
                debtor_idx,
                case 
                    when length(zip_code) = 5 
                    then zip_code 
                    when length(zip_code) > 5 
                    then left(zip_code, 5) 
                    else null 
                end as zip_code
        
        from edwprodhh.pub_jchang.master_debtor 

    )
    , debtor_data as 
    (
        
        select
            md.debtor_idx,  
            md.zip_code,  
            md.assigned_amount,
            md.batch_date as assigned_date, 
            md.payment
        from edwprodhh.pub_jchang.master_debtor as md
        left join zip as zp
        on zp.debtor_idx = md.debtor_idx
        where batch_date between '2022-01-01' and current_date()
        and md.debtor_idx is not null and md.assigned_amount is not null 
    ), 
    median_household_income as (
        select 
            dd.assigned_date,  
            debtor_idx, 
            median_household_income
        from debtor_data dd
        left join edwprodhh.pub_mbutler.master_zip_code_stats as cs
        on dd.zip_code = cs.zip_code
    ),  
    experian_score as (
        select 
            debtor_idx, 
            sc.experian_score 
        from edwprodhh.pub_jchang.master_debtor as dt 
        left join edwprodhh.dw.dimscore as sc 
        on sc.dimscore_idx = dt.debtor_idx
        where batch_date between '2022-01-01' and current_date()
        and dt.debtor_idx is not null and assigned is not null 
    ), 
    equabli_score_join as (
        select 
            md.debtor_idx,
            equabli_score as eqb_score, 
            sum(assigned) as assigned_amt, 
            sum(payment) as payment
        from edwprodhh.pub_jchang.master_debtor as md
       	where 
        batch_date between '2022-01-01' and current_date() 
    	and assigned > 0 
        and assigned is not null and md.debtor_idx is not null 
        group by 1, 2
        order by 1
    ),
    complete_debtor_data as (
        select 
            mh.assigned_date, 
            mh.debtor_idx,
            eqb_score, 
            assigned_amt, 
            mh.median_household_income, 
            experian_score
        from median_household_income as mh 
        left join equabli_score_join as eq
        on eq.debtor_idx = mh.debtor_idx 
        left join experian_score as ex 
        on ex.debtor_idx = eq.debtor_idx 
    )
    select * 
    from complete_debtor_data 
); 

/* Below creates your t_app metrics */ 


create or replace temporary table edwprodhh.pub_mbutler.temporary_raw_dataset as (
    with commission_attributions as (
        select      
            contact_id,
            sum(attribution_weight * sig_comm_amt) as dol_commission_attr
        from edwprodhh.pub_jchang.transform_payment_attribution_contact_weights
        where contact_type = 'Text Message'
        group by contact_id
    ),
    previous_contacts as (
        select 
            debtor_idx,
            contact_time, 
            contact_id, 
            row_number() over (partition by debtor_idx order by contact_time) as previous_contact_num
        from edwprodhh.pub_jchang.master_contacts 
    ),
    previous_contact_nums as (
        select 
            debtor_idx,     
            contact_id, 
            previous_contact_num - 1 as previous_contacts
        from previous_contacts
    ),
    texts as (
        select 
            emid_idx as text_idx, 
            status_date,
            row_number() over (partition by t.debtor_idx order by status_date) as frequency, 
            t.debtor_idx, 
            previous_contacts, 
            dol_commission_attr
        from edwprodhh.pub_jchang.master_texts as t
        inner join previous_contact_nums as co 
            on t.debtor_idx = co.debtor_idx 
            and t.emid_idx = co.contact_id
        left join commission_attributions as attr 
            on t.emid_idx = attr.contact_id
    ),
    all_contacts as (
        select 
            debtor_idx,
            contact_id,
            contact_type, 
            row_number() over (partition by debtor_idx, contact_type order by contact_time) as contact_num
        from edwprodhh.pub_jchang.master_contacts 
        where contact_time between '2022-01-01' and current_date()
    ),
    previous_contact_nums_by_type as (
        select 
            debtor_idx, 
            contact_id, 
            case 
                when contact_type = 'dialer-agent call' 
                    then contact_num - 1 
                else 0 
            end as dialer_agent_call, 
            case 
                when contact_type = 'outbound-manual call' 
                    then contact_num - 1 
                else 0 
            end as outbound_manual_call, 
            case 
                when contact_type = 'text message' 
                    then contact_num - 1 
                else 0 
            end as text_message,
            case 
                when contact_type = 'text' 
                    then contact_num - 1 
                else 0 
            end as text,
            case 
                when contact_type = 'dialer-agentless call' 
                    then contact_num - 1 
                else 0 
            end as dialer_agentless_call,
            case 
                when contact_type = 'letter' 
                    then contact_num - 1 
                else 0 
            end as letter,
            case 
                when contact_type = 'inbound-agent call' 
                    then contact_num - 1 
                else 0 
            end as inbound_agent_call,
            case 
                when contact_type = 'email' 
                    then contact_num - 1 
                else 0 
            end as email
        from all_contacts
    ),
    full_dataset as (
        select     
            debtor.debtor_idx, 
            text_idx, 
            frequency,
            assigned_date,  
            status_date - assigned_date as debt_age, 
            previous_contacts,
            eqb_score, 
            assigned_amt, 
            median_household_income, 
            experian_score,
            coalesce(dol_commission_Attr, 0) as dol_commission_Attr
        from 
        texts as t 
        inner join edwprodhh.pub_mbutler.temporary_full_table as debtor
        on t.debtor_idx = debtor.debtor_idx 
    )
    select * 
    from full_dataset 
) 

/* below creates the final table that you are going to pull into your model */ 
;
create or replace table edwprodhh.pub_mbutler.contact_strategy_texts_5 as (
    with raw_data as (
        select *
        from edwprodhh.pub_mbutler.temporary_raw_dataset
    ),
    trimmed_data as (
        select 
            debtor_idx, 
            text_idx, 
            assigned_amt, 
            frequency, 
            previous_contacts, 
            debt_age, 
            eqb_score, 
            median_household_income, 
            case when experian_score = 0 then null else experian_score end as experian_score,
            dol_commission_attr, 
            percentile_cont(0.10) within group (order by previous_contacts) over () as previous_contacts_lower,
            percentile_cont(0.70) within group (order by previous_contacts) over () as previous_contacts_upper,
            percentile_cont(0.10) within group (order by frequency) over () as frequency_lower,
            percentile_cont(0.70) within group (order by frequency) over () as frequency_upper,
            percentile_cont(0.05) within group (order by assigned_amt) over () as assigned_amt_lower,
            percentile_cont(0.70) within group (order by assigned_amt) over () as assigned_amt_upper,
            percentile_cont(0.10) within group (order by debt_age) over () as debt_age_lower,
            percentile_cont(0.70) within group (order by debt_age) over () as debt_age_upper, 
            percentile_cont(0.10) within group (order by median_household_income) over () as median_household_income_lower
        from raw_data 
        where 
            dol_commission_attr >= 0 
            and dol_commission_attr <= 50 
            and ((experian_score between 400 and 800) or experian_score is null) 
    ),
    applied_trim as (
        select 
            debtor_idx, 
            text_idx, 
            assigned_amt, 
            frequency, 
            debt_age, 
            previous_contacts, 
            eqb_score, 
            median_household_income, 
            experian_score,
            dol_commission_attr, 
            case when dol_commission_attr > 0 then 1 else 0 end as is_success, 
            row_number() over (partition by is_success order by random()) as row_num 
        from trimmed_data
        where 
            (frequency is null or (frequency between frequency_lower and frequency_upper)) 
            and (previous_contacts is null or (previous_contacts between previous_contacts_lower and previous_contacts_upper)) 
            and (assigned_amt is null or (assigned_amt between assigned_amt_lower and assigned_amt_upper)) 
            and (debt_age is null or (debt_age between debt_age_lower and debt_age_upper)) 
            and (median_household_income is null or (median_household_income >= median_household_income_lower)) 
    ),
    normalized_data as (
        select 
            max(dol_commission_attr) as max_dol, 
            min(dol_commission_attr) as min_dol 
        from applied_trim
    ),
    success_metric as (
        select 
            *
        from applied_trim
    )
    , oversampled_success as (
        select *
        from success_metric
        where is_success = 1 
        union all 
        select *
        from success_metric
        where is_success = 1 
        union all
        select *
        from success_metric
        where is_success = 1
        union all
        select *
        from success_metric
        where is_success = 1
        union all
        select *
        from success_metric
        where is_success = 1
        ) 
        
, join_up as 
(select * 
from oversampled_success
union all
select * 
from success_metric 
where is_success = 0
) 
, success_partition as
(

   select 
            
            *, 
            percent_rank() over (partition by is_success order by random(4)) as pn, 
            row_number() over (partition by is_success order by random(4)) as rn
        
    from join_up

) 
, final as
(


    
    select 
            assigned_amt, 
            debt_age, 
            previous_contacts, 
            frequency, 
            eqb_score, 
            median_household_income, 
            experian_score,
            dol_commission_attr
            
    
    from success_partition 
    where rn <= (select max(rn) from success_partition where is_success = 0 and pn <= 0.70) 
	
) 
, stored_observations as
  (select *, 
  percent_rank() over (order by random(4)) as pn 
  from final 
) 
, test as 
(
select  assigned_amt, 
            debt_age, 
            previous_contacts, 
            frequency, 
            eqb_score, 
            median_household_income, 
            experian_score,
            dol_commission_attr
from stored_observations where pn < 0.99 
)
select * 
from test 
) 