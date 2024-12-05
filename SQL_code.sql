WITH A AS (
select
contact_email
, contact_first_name
, contact_last_name
, account_name
, case when master_annual_revenue is null then 'No Annual Revenue'
    when master_annual_revenue = 'Pre-Revenue' then 'No Annual Revenue'
    when master_annual_revenue = '$0M-$1M' then '0 - 1M'
    when master_annual_revenue = 'Under $500,000' then '0 - 1M'
    when master_annual_revenue = '$500,000 - $1 mil.' then '0 - 1M'
    when master_annual_revenue = '$1 mil. - $5 mil.' then '1M - 10M'
    when master_annual_revenue = '$1M-$5M' then '1M - 10M'
    when master_annual_revenue = '$5 mil. - $10 mil.' then '1M - 10M'
    when master_annual_revenue = '$5M-$10M' then '1M - 10M'
    when master_annual_revenue = '$10 mil. - $25 mil.' then '10M - 50M'
    when master_annual_revenue = '$10M-$25M' then '10M - 50M'
    when master_annual_revenue = '$25 mil. - $50 mil.' then '10M - 50M'
    when master_annual_revenue = '$25M-$50M' then '10M - 50M'
    when master_annual_revenue = '$50 mil. - $100 mil.' then '50M+'
    when master_annual_revenue = '$50M-$100M' then '50M+'
    when master_annual_revenue = '$100 mil. - $250 mil.' then '50M+'
    when master_annual_revenue = '$100M-$250M' then '50M+'
    when master_annual_revenue = '$250 mil. - $500 mil.' then '50M+'
    when master_annual_revenue = '$250M-$500M' then '50M+'
    when master_annual_revenue = '$500 mil. - $1 bil.' then '50M+'
    when master_annual_revenue = '$500M-$1B' then '50M+'
    when master_annual_revenue = '$1B-$5B' then '50M+'
    when master_annual_revenue = '$1 bil. - $5 bil.' then '50M+'
    when master_annual_revenue = '$5B+' then '50M+'
    when master_annual_revenue = 'Over $5 bil.' then '50M+'
    when master_annual_revenue = '0 - 1M' then '0 - 1M'
    when master_annual_revenue = '1M - 10M' then '1M - 10M'
    when master_annual_revenue = '10M - 50M' then '10M - 50M'
    when master_annual_revenue = '50M+' then '50M+'
    when master_annual_revenue = 'No Annual Revenue' then 'No Annual Revenue'
    else 'Unknown' end as rev_range
, email_list
from
(with a1 as (
select
account_id
, account_name
, contact_first_name
, contact_last_name
, contact_email
, master_annual_revenue
, email_list
from
(select
zz.*
, row_number()over(partition by contact_email order by last_invoice desc) as email_rank
from (
SELECT DISTINCT 
a.account_id
, b.account_name
, b.contact_first_name
, b.contact_last_name
, b.contact_email 
, d.master_annual_revenue
, last_invoice
, case when last_invoice > dateadd(days,-45,convert_timezone('America/Chicago', current_date)) then 'Active Client'
    else 'Past Client' end as EMAIL_LIST
from
(select 
    salesforce_account_id as account_id
    , max(convert_timezone('America/Chicago', record_date)) as last_invoice
    , min(convert_timezone('America/Chicago', record_date)) as first_invoice
    from dwh.fct_client_service_invoice_summary
    where amount_in > 0
group by 1) a
left join (SELECT * FROM dwh.dim_sf_account) b
    on a.account_id = b.account_id
left join PROD_DWH.DWH.DIM_CLIENT AS D 
    on a.account_id = D.SALESFORCE_ACCOUNT_ID
where 
convert_timezone('America/Chicago', last_invoice) > dateadd(months,-24,convert_timezone('America/Chicago', current_date))
and lower(b.contact_first_name) not like '%test%' --Exclude Test Records
and lower(b.contact_last_name) not like '%test%' --Exclude Test Records
and b.contact_email not like '%@paro%' --Exclude Test Records
and b.contact_email is not null --Exclude NULL Records
and trim(a.account_id) not in (select distinct trim(account_id) from prod_dwh.dwh.dim_sf_opportunity where business_unit in ('Staff Aug/White Label','Seasonal Tax Deadlines') and account_id is not null) )zz --Exclude White Label
) zzz where email_rank = 1),

b1 as (
select
account_id
, account_name
, contact_first_name
, contact_last_name
, contact_email
, master_annual_revenue
, email_list
from (
select 
bz.*
, row_number()over(partition by contact_email order by account_id desc) as email_rank  
from (select 
c.account_id
, d.account_name
, d.contact_first_name
, d.contact_last_name
, d.contact_email 
, e.master_annual_revenue
,'Lost Opp' as email_list
from
(select distinct a.account_id 
 --, b.account_id
from (select opportunity_id, account_id, stage_name , LEAD_SOURCE 
      from dwh.dim_sf_opportunity 
        where 
        convert_timezone('America/Chicago', created_date) > dateadd(months,-24,convert_timezone('America/Chicago', current_date))
        and stage_name in ('Closed Lost', 'closedClientRejected', 'closedArchived', 'Close Archive')
        and LEAD_SOURCE in ('Advertising', 'LinkedIn', 'Organic', 'PPC', 'SEO / Direct Traffic', 'SEO/Direct Traffic', 'Social (Free)', 'Social (Paid)', 'Webimax', 'Web Chat', 'Retargeting', 'Affiliate', 'Paid Partnership', 'Inbound Phone Call', 'Paid Ad')
        and account_id not in (select distinct trim(account_id) from prod_dwh.dwh.dim_sf_opportunity where business_unit in ('Staff Aug/White Label','Seasonal Tax Deadlines') and account_id is not null)
        ) a
left join (select distinct account_id 
                 from dwh.dim_sf_opportunity
                    where stage_name in
                        ('Proposal','freelancerEngaged','proposalCall','closedWon','Discovery','sowSubmitted','Reengage','Closed Won','Open')) b
  on a.account_id=b.account_id
where b.account_ID is null
order by a.account_id desc) c
left join dwh.dim_sf_account d
on c.account_id = d.account_id
left join prod_dwh.dwh.dim_client as e
on c.account_id = e.salesforce_account_id
where lower(d.contact_first_name) not like '%test%' --Exclude Test Records
and lower(d.contact_last_name) not like '%test%' --Exclude Test Records
and d.contact_email not like '%@paro%' --Exclude Test Records
and d.contact_email is not null --Exclude NULL Records
and c.account_id not in (select distinct account_id from dwh.dim_sf_opportunity where stage_name in ('Proposal','freelancerEngaged','proposalCall','Discovery','sowSubmitted','Reengage','Open')
and ACCOUNT_ID is not null)) as bz) where email_rank = 1),-- Exclude Open Opps

c1 as (
select
account_id
, account_name
, contact_first_name
, contact_last_name
, contact_email
, master_annual_revenue
, email_list
from
(select
*
, row_number()over(partition by contact_email order by created_date desc) as email_rank
from (select
converted_account_id as account_id
, company as account_name
, first_name as contact_first_name
, last_name as contact_last_name
, email as contact_email
, revenue_range as master_annual_revenue
, 'Unconverted Lead' as email_list
, created_date
from dwh.dim_sf_lead 
where convert_timezone('America/Chicago', created_date) > dateadd(months,-24,convert_timezone('America/Chicago', current_date))
and lead_source in ('Advertising', 'LinkedIn', 'Organic', 'PPC', 'SEO / Direct Traffic', 'SEO/Direct Traffic', 'Social (Free)', 'Social (Paid)'
                      , 'Webimax', 'Web Chat', 'Retargeting', 'Affiliate', 'Paid Partnership', 'Inbound Phone Call', 'Paid Ad', 'Referral')
and converted_account_id is NULL
AND lower(first_name) NOT LIKE '%test%' --Exclude Test Records
AND lower(last_name) NOT LIKE '%test%' --Exclude Test Records
AND email NOT LIKE '%@paro%' --Exclude Test Records
AND email IS NOT NULL --Exclude NULL Records
AND SALES_REPRESENTATIVE_NAME NOT IN ('Jason Nahani') --EXCLUDE Ecommerce
AND COMPANY NOT LIKE '%Accounting%' AND COMPANY NOT LIKE '%CPA%' AND COMPANY NOT LIKE '%Tax%'
AND SUBMITTED_INDUSTRY <> 'Accounting Firm'
AND status in ('Not Interested','Archived - DQ','Nurture','Unqualified','Archived - Nurture','No Response','Cold')
)cz) where email_rank = 1)

select a1.* from a1
union
(select b1.* from b1 where b1.contact_email not in (select distinct contact_email from a1))
union
(select c1.* from c1 where c1.contact_email not in (select distinct contact_email from a1) 
    and c1.contact_email not in (select distinct contact_email from b1)))    
),

B AS (
  SELECT *, 
    CASE 
      WHEN EMAIL_LIST = 'Active Client' AND REV_RANGE IN ('0 - 1M','1M - 10M','No Annual Revenue','Unknown') THEN 'ACTIVE_0-10M'
      WHEN EMAIL_LIST = 'Past Client' AND REV_RANGE IN ('0 - 1M','1M - 10M','No Annual Revenue','Unknown') THEN 'PAST_0-10M'
      WHEN EMAIL_LIST = 'Lost Opp' AND REV_RANGE IN ('0 - 1M','1M - 10M','No Annual Revenue','Unknown') THEN 'LOST_OPPS_0-10M'
      WHEN EMAIL_LIST = 'Unconverted Lead' AND REV_RANGE IN ('0 - 1M','1M - 10M','No Annual Revenue','Unknown') THEN 'UNCONVERTED_LEADS_0-10M'
      WHEN EMAIL_LIST = 'Active Client' AND REV_RANGE IN ('10M - 50M') THEN 'ACTIVE_10-50M'
      WHEN EMAIL_LIST = 'Past Client' AND REV_RANGE IN ('10M - 50M') THEN 'PAST_10-50M'
      WHEN EMAIL_LIST = 'Lost Opp' AND REV_RANGE IN ('10M - 50M') THEN 'LOST_OPPS_10-50M'
      WHEN EMAIL_LIST = 'Unconverted Lead' AND REV_RANGE IN ('10M - 50M') THEN 'UNCONVERTED_LEADS_10-50M'
      WHEN EMAIL_LIST = 'Active Client' AND REV_RANGE IN ('50M+') THEN 'ACTIVE_50M+'
      WHEN EMAIL_LIST = 'Past Client' AND REV_RANGE IN ('50M+') THEN 'PAST_50M+'
      WHEN EMAIL_LIST = 'Lost Opp' AND REV_RANGE IN ('50M+') THEN 'LOST_OPPS_50M+'
      WHEN EMAIL_LIST = 'Unconverted Lead' AND REV_RANGE IN ('50M+') THEN 'UNCONVERTED_LEADS_50M+'
    END AS SEGMENT
  FROM A
) 

SELECT * FROM B;