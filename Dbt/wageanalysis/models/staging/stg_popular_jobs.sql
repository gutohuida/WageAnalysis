with treated as (
select 
	country,
    substring(substring(wage_text, 'in the .* area'), '(?<=the\s)(.*)(?=\sarea)') as country_found, 
	job, 
	company, 
	score, 
	substring(open_jobs,'\d+') as open_jobs, 
	substring(data_collected,'\d+') as data_collected,
	trim(replace(substring(replace(min, ',', ''),'^(\D+)'), chr(160), '')) as currency,
	cast(substring(replace(min, ',', ''),'\d+') as numeric) as min,
	substring(replace(min, ',', ''),'(\D+)$') as min_format, 
	cast(substring(replace(max, ',', ''),'\d+') as numeric) as max,
	substring(replace(max, ',', ''),'(\D+)$') as max_format, 
	cast(substring(replace(likely, ',', ''),'\d+') as numeric) as likely,
	substring(replace(likely, ',', ''),'(\D+)$') as likely_format, 
	right("period",2) as period
from {{ source('raw','popular_companies')}})


select distinct
    country,
    job,
    company,
    score,
    open_jobs,
    data_collected,
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
        when min_format = 'K' then min * 1000
        when min_format = 'M' then min * 1000000
        else min
    end as min,
    case
        when max_format = 'K' then max * 1000
        when max_format = 'M' then max * 1000000
        else max
    end as max,
    case
        when likely_format = 'K' then likely * 1000
        when likely_format = 'M' then likely * 1000000
        else likely
    end as likely,
    "period"
from treated
where country = country_found