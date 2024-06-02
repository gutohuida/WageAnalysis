drop table if exists raw.country_job_info;
create table raw.country_job_info (
	country varchar(50),
	job varchar(100),
	estimated_pay varchar(50),
	period varchar(10),
	last_update varchar(50),
	wage_text text,
	pay_range varchar(100),
	base_pay varchar(50),
	additional_pay varchar(50),
	insert_date timestamp default (now() at time zone('utc')),
	update_date timestamp default (now() at time zone('utc'))
	
);

drop table if exists raw.popular_companies;
create table raw.popular_companies (
	country varchar(50),
	job varchar(100),
	wage_text text,
	company varchar(100),
	score varchar(3),
	open_jobs varchar(50),
	data_collected varchar(50),
	min varchar(15),
	max varchar(15),
	likely varchar(15),
	period varchar(10),
	insert_date timestamp default (now() at time zone('utc')),
	update_date timestamp default (now() at time zone('utc'))
);

DROP TABLE IF EXISTS raw.country_expenses;
CREATE TABLE raw.country_expenses (
	country text NULL,
	city text NULL,
	family_of_4 text NULL,
	single text NULL,
	cost_comparison text NULL,
	rent_comparison text NULL,
	insert_date timestamp default (now() at time zone('utc')),
	update_date timestamp default (now() at time zone('utc'))
);

DROP TABLE IF EXISTS raw.country_expenses_detail;
CREATE TABLE raw.country_expenses_detail (
	country text NULL,
	city text NULL,
	"type" text NULL,
	amount text NULL,
	"range" text NULL,
	insert_date timestamp default (now() at time zone('utc')),
	update_date timestamp default (now() at time zone('utc'))
);