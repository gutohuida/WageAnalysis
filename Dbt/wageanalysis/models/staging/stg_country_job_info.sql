--{{ config(materialized='view') }}

with treated as (
select 
    country, 
    substring(substring(wage_text, 'in the .* area'), '(?<=the\s)(.*)(?=\sarea)') as country_found,
    job, 
    trim(replace(substring(estimated_pay, '^(\D+)'), chr(160), '')) as currency,
	cast(substring(estimated_pay, '\d+') as numeric) as pay,
	substring(estimated_pay, '(\D+)$') as number_format, 
    right("period",2) as "period", 
    case 
		when trim(substring(last_update ,'(?!Updated) .*')) = '' then null
		else to_date(trim(substring(last_update ,'(?!Updated) .*')), 'MON DD, YYYY') 
	end as last_update,
    case 
		when substring(wage_text, 'average salary of .{1,4}\d+.{0,1}\d*') is NULL then NULL
		else cast(substring(substring(replace(wage_text, ',', ''), 'average salary of .{1,4}\d+.{0,1}\d*'),'\d+.{0,1}\d*') as numeric)
	end as average_salary, 
    wage_text, 
    cast(substring(substring(replace(pay_range, chr(160), ' '), '^(.{1,4}\d+.)'),'\d+') as numeric) as pay_range_min,
	substring(substring(replace(pay_range, chr(160), ' '), '^(.{1,4}\d+.)'),'(\D+)$') as pay_range_min_format,
	cast(substring(substring(replace(pay_range, chr(160), ' '), '(\d+.)$'),'\d+') as numeric) as pay_range_max,
	substring(substring(replace(pay_range, chr(160), ' '), '(\d+.)$'),'(\D+)$') as pay_range_max_format, 
    cast(substring(substring(replace(base_pay, chr(160), ' '), '^(.{1,4}\d+.)'),'\d+') as numeric) as base_pay_min,
	substring(substring(replace(base_pay, chr(160), ' '), '^(.{1,4}\d+.)'),'(\D+)$') as base_pay_min_format,
	cast(substring(substring(replace(base_pay, chr(160), ' '), '(\d+.)$'),'\d+') as numeric) as base_pay_max,
	substring(substring(replace(base_pay, chr(160), ' '), '(\d+.)$'),'(\D+)$') as base_pay_max_format,
    cast(substring(substring(replace(additional_pay, chr(160), ' '), '^(.{1,4}\d+.)'),'\d+') as numeric) as additional_pay_min,
	substring(substring(replace(additional_pay, chr(160), ' '), '^(.{1,4}\d+.)'),'(\D+)$') as additional_pay_min_format,
	cast(substring(substring(replace(additional_pay, chr(160), ' '), '(\d+.)$'),'\d+') as numeric) as additional_pay_max,
	substring(substring(replace(additional_pay, chr(160), ' '), '(\d+.)$'),'(\D+)$') as additional_pay_max_format,  
    insert_date, 
    update_date
from {{ source('raw','country_job_info')}})

select distinct
    country,
    job,
    case
        when currency = '$'   then 'USD'
        when currency = '£'   then 'GBP'
        when currency = '€'   then 'EUR'
        when currency = '₩'   then 'KRW'
        when currency = '₪'   then 'ILS'
        when currency = '¥'   then 'JPY'
        when currency = 'A$'  then 'AUD'
        when currency = 'R$'  then 'BRL'
        when currency = 'CA$' then 'CAD'
        when currency = 'CN¥' then 'CNY'
        when currency = 'MX$' then 'MXN'
        when currency = 'NZ$' then 'NZD'
        else currency
    end as currency,
    case
        when number_format = 'K' then pay * 1000
        when number_format = 'M' then pay * 1000000
        else pay
    end as pay,
    "period",
    last_update,
    average_salary,
    wage_text as wage_description,
    case
        when pay_range_min_format = 'K' then pay_range_min * 1000
        when pay_range_min_format = 'M' then pay_range_min * 1000000
        else pay_range_min
    end as pay_range_min,
    case
        when pay_range_max_format = 'K' then pay_range_max * 1000
        when pay_range_max_format = 'M' then pay_range_max * 1000000
        else pay_range_max
    end as pay_range_max,
    case
        when base_pay_min_format = 'K' then base_pay_min * 1000
        when base_pay_min_format = 'M' then base_pay_min * 1000000
        else base_pay_min
    end as base_pay_min,
    case
        when base_pay_max_format = 'K' then base_pay_max * 1000
        when base_pay_max_format = 'M' then base_pay_max * 1000000
        else base_pay_max
    end as base_pay_max,
    case
        when additional_pay_min_format = 'K' then additional_pay_min * 1000
        when additional_pay_min_format = 'M' then additional_pay_min * 1000000
        else additional_pay_min
    end as additional_pay_min,
    case
        when additional_pay_max_format = 'K' then additional_pay_max * 1000
        when additional_pay_max_format = 'M' then additional_pay_max * 1000000
        else additional_pay_max
    end as additional_pay_max
from treated
where country = country_found