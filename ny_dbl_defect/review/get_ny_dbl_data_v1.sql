set search_path to files;

drop function if exists get_ny_dbl_data_v1(p_effective_date date);

create function get_ny_dbl_data_v1(p_effective_date date)
returns table
(
	ny_dbl_record_id bigint,
	pf_corp text,
	company text,
	legal_name text,
	dba_name text,
	company_address1 text,
	company_address2 text,
	company_city text,
	company_state text,
	company_zip text,
	naics text,
	federal_ein text,
	state_dol text,
	legal_status text,
	client_phone text,
	client_fax text,
	email_addr text,
	filing_descr text,
	location_address1 text,
	location_address2 text,
	location_city text,
	location_state text,
	location_zip text,
	dbl_males integer,
	dbl_females integer,
	pfl_males integer,
	pfl_females integer,
	covered_annual_payroll bigint,
	status text,
	status_date date,
	cancel_reason text)
set search_path to files,public
as $$
declare

begin

set search_path to files;

drop table if exists ny_dbl_data_temp;

create temp table ny_dbl_data_temp (
	ny_dbl_record_id bigserial,
	pf_corp text,
	company text,
	legal_name text,
	dba_name text,
	company_address1 text,
	company_address2 text,
	company_city text,
	company_state text,
	company_zip text,
	naics text,
	federal_ein text,
	state_dol text,
	legal_status text,
	client_phone text,
	client_fax text,
	email_addr text,
	filing_descr text,
	location_address1 text,
	location_address2 text,
	location_city text,
	location_state text,
	location_zip text,
	dbl_males integer,
	dbl_females integer,
	pfl_males integer,
	pfl_females integer,
	covered_annual_payroll bigint,
	status text,
	status_date date,
	cancel_reason text
);

set search_path to files;
-- STEP 1 GET ALL THE LOCATIONS FROM TABLE PS_LOCATION_TBL THAT HAVE A STATE VALUE OF 'NY'
drop table if exists ny_dbl_work_locations_data_temp;

-- create temp table to hold the results.
create temp table ny_dbl_work_locations_data_temp (
	location text,
	descr text,
	address1 text,
	address2 text,
	city text,
	state text,
	postal text
);

insert into ny_dbl_work_locations_data_temp (
	location,
	descr,
	address1,
	address2,
	city,
	state,
	postal
)
select distinct l.location, l.descr, l.address1, l.address2, l.city, l.state, l.postal
from p.ps_location_tbl l
where l.state = 'NY'
	and l.setid = 'USPEO'
	and l.eff_status = 'A'
	and l.effdt = (select max(l1.effdt) from p.ps_location_tbl l1
					where l.setid = l1.setid
						and l.location = l1.location
						and l.state = l1.state
						and l.eff_status = l1.eff_status
						and l1.effdt <= p_effective_date);

-- STEP2 TAKE THE LIST OF LOCATIONS TO PS_JOB AND COUNT ALL THE EMPLOYEES IN THOSE LOCATIONS BY COMPANY AND GENDER
drop table if exists ny_dbl_ee_counts_data_temp;

create temp table ny_dbl_ee_counts_data_temp (
	company text,
	location text,
	total_employees integer,
	dbl_males integer,
	dbl_females integer,
	covered_annual_payroll numeric(20,3)
);

insert into ny_dbl_ee_counts_data_temp (
	company,
	location,
	total_employees,
	dbl_males,
	dbl_females,
	covered_annual_payroll
)
select
	j.company,
	j.location,
	count(+1) as total_employees,
	count(case when pde.sex = 'M' or pde.sex = 'U' then +1 end) as dbl_males,
	count(case when pde.sex = 'F' then +1 end) as dbl_females,
	sum(j.annual_rt) as covered_annual_payroll
from ny_dbl_work_locations_data_temp a , p.ps_job j ,p.ps_pers_data_effdt pde
where a.location = j.location
  and j.emplid = pde.emplid
  and j.empl_status in ('A','L','P','S')  -- actives only, no terms
  and j.paygroup <> 'NP' -- trusted advisor
  and j.company <> '31T'  -- this is a test company
  and j.emplid not like 'C%'  -- this omits cobra employees
  and (j.empl_class not in ('K1','K1E','K1D') or j.empl_class is null) -- omit K1 employees -- RALCAF-3201
  and pde.effdt = (select max(pde1.effdt) from p.ps_pers_data_effdt pde1
                    where pde.emplid       = pde1.emplid
                      and pde1.effdt      <= p_effective_date)
  and j.effdt = (select max(j1.effdt) from p.ps_job j1
                where j.emplid       = j1.emplid
                  and j.empl_rcd     = j1.empl_rcd
                  and j1.effdt      <= p_effective_date)
 and j.effseq = (select max(j2.effseq) from p.ps_job j2
                  where j.emplid      = j2.emplid
                    and j.empl_rcd    = j2.empl_rcd
                  	and j2.effdt      = j.effdt)
group by rollup(j.company,j.location);

-- STEP 3 GET THE COMPANY TOTALS CALCULATED IN STEP 2
drop table if exists ny_dbl_company_totals_data_temp;

create temp table ny_dbl_company_totals_data_temp (
	company text,
	total_employees integer,
	dbl_males integer,
	dbl_females integer,
	covered_annual_payroll numeric(20,3)
);

insert into ny_dbl_company_totals_data_temp (
	company,
	total_employees,
	dbl_males,
	dbl_females,
	covered_annual_payroll
)
select ee_counts.company, ee_counts.total_employees, ee_counts.dbl_males, ee_counts.dbl_females, ee_counts.covered_annual_payroll
from ny_dbl_ee_counts_data_temp ee_counts
where location is null;

-- STEP 4 GET A SINGLE FILING LOCATION FOR EACH COMPANY BASED ON THE DATA PULLED IN STEPS 1 AND 2
drop table if exists ny_dbl_company_filing_location_data_temp;

create temp table ny_dbl_company_filing_location_data_temp (
	company text,
	location text
);
insert into ny_dbl_company_filing_location_data_temp (
	company,
	location
)
select d.company, min(d.location) as location -- keep(dense_rank first order by company) location
from ny_dbl_ee_counts_data_temp d, ny_dbl_work_locations_data_temp l
where d.location = l.location
	and upper(l.descr) not like ('%REMOTE%') -- USE A NON-REMOTE LOCATION FOR THE FILING ADDRESS
group by d. company
union
select d.company, min(d.location) as location -- keep(dense_rank first order by company) location
from ny_dbl_ee_counts_data_temp d, ny_dbl_work_locations_data_temp l
where d.location = l.location
	-- and l.address1 <> ' ' -- RALCAF-3201
	and upper(l.descr) like ('%REMOTE%') -- USE THE REMOTE LOCATION FOR THE FILING ADDRESS IF THERE IS ONLY ONE LOCATION AND IT IS REMOTE
                                       -- OR IF THERE ARE MORE THAN ONE LOCATION, BUT THEY ARE ALL REMOTE
	and not exists (select 'X' from ny_dbl_ee_counts_data_temp d1, ny_dbl_work_locations_data_temp l1
					where d.company = d1.company
                          and d1.location = l1.location
                          and upper(l1.descr) not like ('%REMOTE%'))
group by d.company;

-- STEP 5 GET THE NAICS CODE FOR EACH COMPANY
drop table if exists ny_dbl_company_naics_data_temp;

create temp table ny_dbl_company_naics_data_temp (
	estabid text,
	naics text
);
insert into ny_dbl_company_naics_data_temp (
	estabid,
	naics
)
select distinct es.estabid, es.naics
from p.ps_estab_tbl_usa es, ny_dbl_ee_counts_data_temp a
where es.estabid = a.company
	and es.effdt = (select max(es1.effdt) from p.ps_estab_tbl_usa es1
					where es.estabid = es1.estabid
						and es1.effdt <= p_effective_date);


insert into ny_dbl_data_temp (
	pf_corp,
	company,
	legal_name,
	dba_name,
	company_address1,
	company_address2,
	company_city,
	company_state,
	company_zip,
	naics,
	federal_ein,
	state_dol,
	legal_status,
	client_phone,
	client_fax,
	email_addr,
	filing_descr,
	location_address1,
	location_address2,
	location_city,
	location_state,
	location_zip,
	dbl_males,
	dbl_females,
	pfl_males,
	pfl_females,
	covered_annual_payroll,
	status,
	status_date,
	cancel_reason
)
select
	c.pf_corp
	,c.company
	,g.t2_legal_name
	,' '
	,c.address1
	,c.address2
	,c.city
	,c.state
	,c.postal
	,b.naics -- TRINET DOES NOT HAVE SIC CODE
	,to_char(c.pf_client_ein, '000000000')
	,' '
	, case when c.legal_type = '01' then 'Individual'
		   when c.legal_type = '02' then 'Partnership'
		   when c.legal_type = '03' then 'Corporation'
		   when c.legal_type = '04' then 'Association'
		   when c.legal_type = '05' then 'Limited Partnership'
		   when c.legal_type = '06' then 'Joint Venture'
		   when c.legal_type = '07' then 'Common Ownership'
		   when c.legal_type = '08' then 'Multiple Status'
		   when c.legal_type = '09' then 'Joint Employer'
		   when c.legal_type = '10' then 'Limited Liability Company'
		   when c.legal_type = '11' then 'Trust or Estate'
		   when c.legal_type = '12' then 'Executor or Trustee'
		   when c.legal_type = '13' then 'Limited Liability Partnership'
		   when c.legal_type = '14' then 'Government Entity'
		   when c.legal_type = '99' then 'Other'
		else 'Corporation'  end as legal_status
	,' ' -- client_phone
	,' ' -- client_fax
	,' ' -- email_addr
	,a.descr
	,a.address1
	,a.address2
	,a.city
	,a.state
	,a.postal
	,tot.dbl_males
	,tot.dbl_females
	,tot.dbl_males as pfl_males
	,tot.dbl_females as pfl_females
	,tot.covered_annual_payroll
	,case when (g.t2_comp_term_dt > p_effective_date) then 'CANCEL'
     		else 'ADD' end as status
	,p_effective_date as status_date
	,case when (g.t2_comp_term_dt > p_effective_date) then 'Client''s Request'
		else ' ' end as cancel_reason
from ny_dbl_work_locations_data_temp  a
	,p.ps_t2_cloptn_effdt g
	,p.ps_company_tbl c
	,ny_dbl_company_naics_data_temp b
	,ny_dbl_company_totals_data_temp tot
	,ny_dbl_company_filing_location_data_temp fl
where c.eff_status = 'A'
	and a.location = fl.location
	and c.company = fl.company
	and c.company = tot.company
	and c.company = b.estabid
	and c.company = g.company
	and c.pf_client = g.pf_client
	and c.effdt = (select max(c1.effdt) from p.ps_company_tbl c1
					where c.company    = c1.company
					 and c1.effdt    <= p_effective_date)
	and g.effdt =(select max (g1.effdt)
					from p.ps_t2_cloptn_effdt g1
				   where     g.pf_client = g1.pf_client
						 and g.company = g1.company
						 and g1.effdt <= p_effective_date)
; --order by company;

 return query select * from ny_dbl_data_temp;

 drop table if exists ny_dbl_data_temp;

end;
$$ language plpgsql;


/*
select * from files.get_ny_dbl_data_v1('2018-01-23');
*/