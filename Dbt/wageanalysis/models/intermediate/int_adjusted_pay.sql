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
	c.currency_code,
	ce.exchange_rate 
from lattest l
join {{source('raw','countrys')}} c 
on lower(l.country) = lower(c."name")
join {{source('raw','currency_exchange')}} ce 
on c.currency_code = ce.currency 
where "rank" = 1
and concat_ws('-', l.country, l.currency, c.currency_code) 
	not in (select 
				concat_ws('-', country, currency, currency_code) 
			from {{ref('adt_currency_country')}} acc))
			
select
	country,
	job, 
	currency,
	case
		when "period" = 'mo' then ((pay / exchange_rate) * 12)::numeric::money
		else (pay / exchange_rate)::numeric::money
	end	as pay,
	case
		when "period" = 'mo' then ((average_salary / exchange_rate) * 12)::numeric::money
		else (average_salary / exchange_rate)::numeric::money
	end	as average_salary,
	case
		when "period" = 'mo' then ((pay_range_min / exchange_rate) * 12)::numeric::money
		else (pay_range_min / exchange_rate)::numeric::money
	end	as pay_range_min, 
	case
		when "period" = 'mo' then ((pay_range_max / exchange_rate) * 12)::numeric::money
		else (pay_range_max / exchange_rate)::numeric::money
	end	as pay_range_max, 
	case
		when "period" = 'mo' then ((base_pay_min / exchange_rate) * 12)::numeric::money
		else (base_pay_min / exchange_rate)::numeric::money
	end	as base_pay_min,
	case
		when "period" = 'mo' then ((base_pay_max / exchange_rate) * 12)::numeric::money
		else (base_pay_max / exchange_rate)::numeric::money
	end	as base_pay_max,
	case
		when "period" = 'mo' then ((additional_pay_min / exchange_rate) * 12)::numeric::money
		else (additional_pay_min / exchange_rate)::numeric::money
	end	as additional_pay_min,
	case
		when "period" = 'mo' then ((additional_pay_max / exchange_rate) * 12)::numeric::money
		else (additional_pay_max / exchange_rate)::numeric::money
	end	as additional_pay_max,
	"period" as base_period, 
	last_update
from joined