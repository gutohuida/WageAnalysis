with lattest as (
select 
	country,
	job, 
	currency, 
	pay, 
	"period", 
	last_update, 
	average_salary,
	wage_description, 
	pay_range_min, 
	pay_range_max, 
	base_pay_min, 
	base_pay_max, 
	additional_pay_min, 
	additional_pay_max,
	row_number() over (partition by country, job order by last_update desc) as "rank"
from {{ref('stg_country_job_info')}} scji),

joined as (
select 
	l.country,
	l.job, 
	l.currency, 
	l.pay, 
	l."period", 
	l.last_update, 
	l.average_salary,
	l.wage_description, 
	l.pay_range_min, 
	l.pay_range_max, 
	l.base_pay_min, 
	l.base_pay_max, 
	l.additional_pay_min, 
	l.additional_pay_max,
	c."name",
	c.currency_name,
	c.currency_code 
from lattest l
join {{source('raw', 'countrys')}} c 
on lower(l.country) = lower(c."name")
where "rank" = 1)

select 
	country, 
	currency,
	currency_code 
from joined
where trim(currency) != trim(currency_code)
group by country, currency, currency_code