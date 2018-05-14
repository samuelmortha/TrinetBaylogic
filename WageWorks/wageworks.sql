-- SOI data pulled from the MFT 's.' schema.

select distinct
  null::varchar as record_type
, null::varchar as group_id
, syemsad.empl_id as employee_id
, syemsad.soc_sec_num as employee_ssn
, null::varchar as change_to_unique_id
, null::varchar as old_unique_id
, syemsad.last_name as last_name
, syemsad.first_name as first_name
, coalesce(substr(syemsad.middle_name, 0, 1), '') as middle_initial
, syemsad.birthdate as birth_date
, syemsad.sex_id as gender
, syemsad.address1 as street_address_1
, coalesce(syemsad.address2, '') as street_address_2
, syemsad.city as city
, syemsad.state as state
, syemsad.zip as zip_code  -- LIVE ZIP
, null::varchar as zip_code_extension
, sthclocra.postal_zip as work_zip_code  -- WORK ZIP
, null::varchar as email_address
, syemsad.phone as phone
, null::varchar as phone_extension
, syempyr.orig_hire_date as employment_hire_date
, syempyr.terminated as employment_termination_date
, null::varchar as benefit_group_code
, null::varchar as benefit_group_effective_date
, null::varchar as payroll_group_code
, null::varchar as payroll_group_effective_date
, sthclocra.location_id as location_code
, syempyr.payroll_type as company_code
, null::varchar as for_future_use_1
, null::varchar as for_future_use_2
, null::varchar as bank_name
, null::varchar as bank_account_number
, null::varchar as bank_routing_number
, null::varchar as bank_account_type
, null::varchar as payment_preference
, case when syempyr.terminated is null then 'Y' else 'N' end as commuter_program_eligibility_status
-- Additions for de-duping
, soemploc.client_id as client_id
, syemsad.payroll_type as payroll_type
into temp tmp_commuter_eligible_ees_combined
from s.soemploc AS soemploc
inner join s.sthclocra
on sthclocra.client_id = soemploc.client_id
and sthclocra.location_id = soemploc.location_id
inner join s.syemsad
on syemsad.empl_id = soemploc.empl_id
inner join s.syclntr
on syemsad.payroll_type = syclntr.payroll_type
and (syclntr.termination_date is null or
    ( (syclntr.termination_date >= ? and syclntr.termination_date <= ?) and
       syclntr.term_reason_code not in (
         select
           code_key_1
         from
           s.sthccdira
         where
           sthccdira.code_type in ('SOI_MIG_WSE','SOI_TERM_WSE')
        )
      )
    )
inner join s.syempyr
on syempyr.empl_id = syemsad.empl_id
and (syempyr.terminated is null or
    ( (syempyr.terminated >= ? and syempyr.terminated <= ?) and
       syempyr.term_reason_code not in (
         select
           code_key_1
         from
           s.sthccdira
         where
           sthccdira.code_type in ('SOI_MIG_WSE','SOI_TERM_WSE')
        )
      )
    )
and case when syempyr.terminated is null then ? else syempyr.terminated end between soemploc.effective_date and soemploc.expiry_date
and case when syempyr.terminated is null then ? else syempyr.terminated end between sthclocra.effective_date and sthclocra.expiry_date
and syempyr.empl_status in ('F', 'f', 'Y', 'y')
and not syempyr.empl_status in ('C', 'EXP-IC', 'IC')
and soemploc.primary_yn = 'Y'
and syemsad.country in ('USA', 'US')
where soemploc.client_id = syclntr.client_id
order by syemsad.empl_id asc;


select
  employee_id
, location_code
, company_code
, employment_hire_date
, employment_termination_date
into temp tmp_commuter_eligible_ees_dupes
from tmp_commuter_eligible_ees_combined
where employee_id in (
  select employee_id
  from tmp_commuter_eligible_ees_combined
  group by employee_id
  having count(*) > 1
)
group by 1,2,3,4,5;

delete from tmp_commuter_eligible_ees_combined
where not tmp_commuter_eligible_ees_combined.employment_termination_date is null
and tmp_commuter_eligible_ees_combined.employee_id in (
  select tmp_commuter_eligible_ees_dupes.employee_id
  from tmp_commuter_eligible_ees_dupes
  where tmp_commuter_eligible_ees_dupes.employment_termination_date is null
);

drop table tmp_commuter_eligible_ees_dupes;


select
  employee_id
, location_code
, company_code
, employment_hire_date
, employment_termination_date
into temp tmp_commuter_eligible_ees_dupes
from tmp_commuter_eligible_ees_combined
where employee_id in (
  select employee_id
  from tmp_commuter_eligible_ees_combined
  group by employee_id
  having count(*) > 1
)
group by 1,2,3,4,5;

select
  employee_id
, max(employment_termination_date) as employment_termination_date
into temp tmp_commuter_eligible_ees_dupes_terms_date
from tmp_commuter_eligible_ees_dupes
where employee_id in (
  select employee_id
  from tmp_commuter_eligible_ees_dupes
  where not employment_termination_date is null
  group by employee_id
  having count(*) > 1
)
group by 1;

delete from tmp_commuter_eligible_ees_combined
where not tmp_commuter_eligible_ees_combined.employment_termination_date = (
  select employment_termination_date
  from tmp_commuter_eligible_ees_dupes_terms_date
  where tmp_commuter_eligible_ees_combined.employee_id = tmp_commuter_eligible_ees_dupes_terms_date.employee_id
)
and tmp_commuter_eligible_ees_combined.employee_id in (
  select tmp_commuter_eligible_ees_dupes_terms_date.employee_id
  from tmp_commuter_eligible_ees_dupes_terms_date
);

drop table tmp_commuter_eligible_ees_dupes;


select
  employee_id
, location_code
, company_code
, employment_hire_date
, employment_termination_date
into temp tmp_commuter_eligible_ees_dupes
from tmp_commuter_eligible_ees_combined
where employee_id in (
  select employee_id
  from tmp_commuter_eligible_ees_combined
  group by employee_id
  having count(*) > 1
)
group by 1,2,3,4,5;

select
  employee_id
, min(company_code) as company_code
into temp tmp_commuter_eligible_ees_dupes_terms_payroll_type
from tmp_commuter_eligible_ees_dupes
where employee_id in (
  select employee_id
  from tmp_commuter_eligible_ees_dupes
  where not employment_termination_date is null
  group by employee_id
  having count(*) > 1
)
group by 1;

delete from tmp_commuter_eligible_ees_combined
where not tmp_commuter_eligible_ees_combined.company_code = (
  select company_code
  from tmp_commuter_eligible_ees_dupes_terms_payroll_type
  where tmp_commuter_eligible_ees_combined.employee_id = tmp_commuter_eligible_ees_dupes_terms_payroll_type.employee_id
)
and tmp_commuter_eligible_ees_combined.employee_id in (
  select tmp_commuter_eligible_ees_dupes_terms_payroll_type.employee_id
  from tmp_commuter_eligible_ees_dupes_terms_payroll_type
);

drop table tmp_commuter_eligible_ees_dupes;


select
  employee_id
, location_code
, company_code
, employment_hire_date
, employment_termination_date
into temp tmp_commuter_eligible_ees_dupes
from tmp_commuter_eligible_ees_combined
where employee_id in (
  select employee_id
  from tmp_commuter_eligible_ees_combined
  group by employee_id
  having count(*) > 1
)
group by 1,2,3,4,5;

select
  employee_id
, min(company_code) as company_code
into temp tmp_commuter_eligible_ees_dupes_active_payroll_type
from tmp_commuter_eligible_ees_dupes
where employee_id in (
  select employee_id
  from tmp_commuter_eligible_ees_dupes
  group by employee_id
  having count(*) > 1
)
group by 1;

delete from tmp_commuter_eligible_ees_combined
where not tmp_commuter_eligible_ees_combined.company_code = (
  select company_code
  from tmp_commuter_eligible_ees_dupes_active_payroll_type
  where tmp_commuter_eligible_ees_combined.employee_id = tmp_commuter_eligible_ees_dupes_active_payroll_type.employee_id
)
and tmp_commuter_eligible_ees_combined.employee_id in (
  select tmp_commuter_eligible_ees_dupes_active_payroll_type.employee_id
  from tmp_commuter_eligible_ees_dupes_active_payroll_type
);

drop table tmp_commuter_eligible_ees_dupes;


select
  employee_id
, location_code
, company_code
, employment_hire_date
, employment_termination_date
into temp tmp_commuter_eligible_ees_dupes
from tmp_commuter_eligible_ees_combined
where employee_id in (
  select employee_id
  from tmp_commuter_eligible_ees_combined
  group by employee_id
  having count(*) > 1
)
group by 1,2,3,4,5;

select
  employee_id
, min(location_code) as location_code
into temp tmp_commuter_eligible_ees_dupes_active_location_code
from tmp_commuter_eligible_ees_dupes
where employee_id in (
  select employee_id
  from tmp_commuter_eligible_ees_dupes
  group by employee_id
  having count(*) > 1
)
group by 1;

delete from tmp_commuter_eligible_ees_combined
where not tmp_commuter_eligible_ees_combined.location_code = (
  select location_code
  from tmp_commuter_eligible_ees_dupes_active_location_code
  where tmp_commuter_eligible_ees_combined.employee_id = tmp_commuter_eligible_ees_dupes_active_location_code.employee_id
)
and tmp_commuter_eligible_ees_combined.employee_id in (
  select tmp_commuter_eligible_ees_dupes_active_location_code.employee_id
  from tmp_commuter_eligible_ees_dupes_active_location_code
);

drop table tmp_commuter_eligible_ees_dupes;

-- PS data pulled from the MFT 'p.' schema (eligible)
select
pn.first_name,
pn.last_name,
pn.middle_name,
pp.birthdate,
Job.Emplid,
Job.EMPL_RCD,
Job.Company,
job.last_hire_dt,
job.termination_dt,
job.pf_client,
job.full_part_time,
job.reg_temp,
job.std_hours,
(select translate(b.phone,'1234567890-abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ `~!!&@#$%^*_+-=|\:,./}{|><"','1234567890') from p.PS_PERSONAL_PHONE b where b.PREF_PHONE_FLAG = 'Y' and b.emplid =job.emplid) home_phone,
(select ppn.national_id from p.ps_pers_nid ppn where ppn.emplid = job.emplid and ppn.primary_nid = 'Y') national_id,
(select ppd.sex from p.ps_pers_data_effdt ppd where ppd.emplid = job.emplid and ppd.effdt = (select max(b.effdt) from p.ps_pers_data_effdt b where ppd.emplid =b.emplid and b.effdt <= current_date)) sex,
'Y' as eligible,
-- Adding for address
PA.ADDRESS1,
PA.ADDRESS_TYPE,
PA.ADDRESS2,
PA.CITY,
PA.STATE,
PA.POSTAL home_postal,
PA2.POSTAL work_postal
into temp tmp_commuter_eligible_ees_ps
from
p.PS_JOB JOB,
p.ps_t2_cloptn_effdt b,
p.ps_names pn,
p.ps_person pp,
-- Adding for address (home and work)
P.PS_ADDRESSES PA,
P.PS_ADDRESSES PA2
where 1=1
and Job.Effdt = (select max(job1.Effdt) from p.PS_JOB Job1
                                        where Job.emplid = Job1.emplid
                                        and job.EMPL_RCD = Job1.EMPL_RCD
                                        and job1.effdt <= current_date)
and Job.Effseq = (select max(job2.effseq) from p.PS_JOB Job2
                                          where Job.emplid = Job2.emplid
                                          and job.EMPL_RCD = job2.EMPL_RCD
                                          and job.effdt = job2.effdt)
and Job.Empl_status in ('A','P','S')
and Job.EMPLID not like 'C%'
and ( (job.empl_class not in ('U')) or (job.empl_class is null) )
and job.std_hours::float >= 30
and job.full_part_time = 'F'
and job.reg_temp = 'R'
and b.company = job.company
and b.pf_client = job.pf_client
and b.t2_peo_id ='SOI'
and b.pf_corp <> 'PEOCN'
and b.effdt = (select max(c.effdt) from p.ps_t2_cloptn_effdt c
               where c.company = b.company
               and c.pf_client = b.pf_client
               and c.effdt <= current_date)
and pn.emplid = job.emplid
and pn.emplid = pp.emplid
and pn.name_type = 'PRI'
and pn.effdt = (select max (pn1.effdt)
                from p.ps_names pn1
                where pn1.emplid = pn.emplid
                and pn1.name_type = pn.name_type)
and ((job.emplid in ((select adr1.emplid
                       from p.ps_addresses adr1
                      where adr1.emplid =  job.emplid
                        and adr1.eff_status = 'A'
                        and adr1.country in ('USA')
                        and ( ((upper(adr1.city) not like 'SAN%FRAN%') and (upper(adr1.city) <> 'SFO')) or
                              (adr1.city is null) )
                        and  adr1.effdt =(select max(adr2.effdt)
                       from p.ps_addresses adr2
                      where adr1.emplid = adr2.emplid
                        and adr1.address_type = adr2.address_type
                        and adr2.effdt <= current_date
                      ))
    ))
and (job.tax_location_cd in ((select co35.tax_location_cd
                              from p.ps_tax_location1 co35
                              where job.tax_location_cd = co35.tax_location_cd
                        and ( ((upper(co35.city) not like 'SAN%FRAN%') and (upper(co35.city) <> 'SFO')) or
                              (co35.city is null) )
                            ))
    ))
-- Adding for address - home
and PA.EMPLID = JOB.EMPLID
and PA.EFFDT = (
    SELECT MAX(ADR2.EFFDT)
    FROM P.PS_ADDRESSES ADR2
    WHERE PA.EMPLID = ADR2.EMPLID
    AND PA.ADDRESS_TYPE = ADR2.ADDRESS_TYPE
    AND ADR2.EFFDT <= current_date
  )
AND PA.ADDRESS_TYPE in ('HOME')
AND PA.EFF_STATUS = 'A'
and PA2.EMPLID = JOB.EMPLID
and PA2.EFFDT = (
    SELECT MAX(ADR2.EFFDT)
    FROM P.PS_ADDRESSES ADR2
    WHERE PA2.EMPLID = ADR2.EMPLID
    AND PA2.ADDRESS_TYPE = ADR2.ADDRESS_TYPE
    AND ADR2.EFFDT <= current_date
  )
AND PA2.ADDRESS_TYPE in ('HOME')
AND PA2.EFF_STATUS = 'A'
union
select
pn.first_name,
pn.last_name,
pn.middle_name,
pp.birthdate,
Job.Emplid,
Job.EMPL_RCD,
Job.Company,
job.last_hire_dt,
job.termination_dt,
Job.pf_client,
Job.full_part_time ,
Job.reg_temp,
job.std_hours,
(select translate(b.phone,'1234567890-abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ `~!!&@#$%^*_+-=|\:,./}{|><"','1234567890') from p.PS_PERSONAL_PHONE b where b.PREF_PHONE_FLAG = 'Y' and b.emplid =Job.emplid) home_phone,
(select
ppn.national_id
from p.ps_pers_nid ppn
where ppn.emplid = job.emplid
and ppn.primary_nid = 'Y') national_id,
(select
  ppd.sex
from p.ps_pers_data_effdt  ppd
where ppd.emplid =job.emplid
and ppd.effdt =( select max(b.effdt)
                    from p.ps_pers_data_effdt b
                    where ppd.emplid =b.emplid
                    and b.effdt <= current_date)) sex,
'Y' as eligible,
-- Adding for address
PA.ADDRESS1,
PA.ADDRESS_TYPE,
PA.ADDRESS2,
PA.CITY,
PA.STATE,
PA.POSTAL home_postal,
PA2.POSTAL work_postal
from
p.PS_JOB JOB,
p.ps_t2_cloptn_effdt b,
p.ps_names pn,
p.ps_person pp,
-- Adding for address (home and work)
P.PS_ADDRESSES PA,
P.PS_ADDRESSES PA2
where 1=1
AND Job.Effdt  = (Select max(job1.Effdt) From p.PS_JOB Job1
                                         Where Job.emplid  = Job1.emplid
                                         and Job.EMPL_RCD = Job1.EMPL_RCD
                                         and job1.effdt    <= current_date)
AND Job.Effseq = (Select max(job2.effseq) From p.PS_JOB Job2
                                          where Job.emplid   = Job2.emplid
                                          and Job.EMPL_RCD  = job2.EMPL_RCD
                                          and Job.effdt      = job2.effdt)
and Job.Empl_status in ('A','P','S')
AND Job.EMPLID NOT LIKE 'C%'
and ( (job.empl_class not in ('U')) or (job.empl_class is null) )
and b.company = job.company
and b.pf_client = job.pf_client
and b.t2_peo_id ='SOI'
and b.pf_corp <> 'PEOCN'
and b.effdt = (select max(c.effdt) from p.ps_t2_cloptn_effdt c
                   where c.company = b.company
                   and c.pf_client = b.pf_client
                   and c.effdt <= current_date)
 and pn.emplid = job.emplid
and pn.emplid =pp.emplid
and pn.name_type = 'PRI'
and pn.effdt = (select max (pn1.effdt)
                  from p.ps_names pn1
                 where pn1.emplid = pn.emplid
                   and pn1.name_type = pn.name_type
                 )
and ((Job.emplid  in ((select adr1.emplid
                     from p.ps_addresses adr1
                    where adr1.emplid =  Job.emplid
                        and adr1.eff_status = 'A'
                        and adr1.address_type = 'HOME'
                        and adr1.country in ('USA')
                        and ( ((upper(adr1.city) not like 'SAN%FRAN%') and (upper(adr1.city) <> 'SFO')) or
                              (adr1.city is null) )
                        and  adr1.effdt =(select max(adr2.effdt)
                     from p.ps_addresses adr2
                    where adr1.emplid = adr2.emplid
                      and adr1.address_type = adr2.address_type
                      and adr2.effdt <= current_date
                      )))  ) or
( job.tax_location_cd  in ( (SELECT co35.tax_location_cd
            FROM p.ps_tax_location1 co35
           WHERE job.tax_location_cd = co35.tax_location_cd
            and ( ((upper(co35.city) not like 'SAN%FRAN%') and (upper(co35.city) <> 'SFO')) or
                  (co35.city is null) )
                ) )  )  )
-- Adding for address
and PA.EMPLID = JOB.EMPLID
and PA.EFFDT = (
    SELECT MAX(ADR2.EFFDT)
    FROM P.PS_ADDRESSES ADR2
    WHERE PA.EMPLID = ADR2.EMPLID
    AND PA.ADDRESS_TYPE = ADR2.ADDRESS_TYPE
    AND ADR2.EFFDT <= current_date
  )
AND PA.ADDRESS_TYPE in ('HOME')
AND PA.EFF_STATUS = 'A'
and PA2.EMPLID = JOB.EMPLID
and PA2.EFFDT = (
    SELECT MAX(ADR2.EFFDT)
    FROM P.PS_ADDRESSES ADR2
    WHERE PA2.EMPLID = ADR2.EMPLID
    AND PA2.ADDRESS_TYPE = ADR2.ADDRESS_TYPE
    AND ADR2.EFFDT <= current_date
  )
AND PA2.ADDRESS_TYPE in ('HOME')
AND PA2.EFF_STATUS = 'A'
union
Select
pn.first_name,
pn.last_name,
pn.middle_name,
pp.birthdate,
Job.Emplid,
Job.EMPL_RCD  ,
Job.Company ,
job.last_hire_dt,
job.termination_dt,
job.pf_client ,
job.full_part_time ,
job.reg_temp,
job.std_hours,
(select translate(b.phone,'1234567890-abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ `~!!&@#$%^*_+-=|\:,./}{|><"','1234567890') from p.PS_PERSONAL_PHONE b where b.PREF_PHONE_FLAG = 'Y' and b.emplid =job.emplid) home_phone,
(select
ppn.national_id
from p.ps_pers_nid ppn
where ppn.emplid = job.emplid
and ppn.primary_nid = 'Y') national_id,
(select
 ppd.sex
from p.ps_pers_data_effdt  ppd
where ppd.emplid =job.emplid
and ppd.effdt =( select max(b.effdt)
                    from p.ps_pers_data_effdt b
                    where ppd.emplid =b.emplid
                    and b.effdt <= current_date)) sex,
'Y' as eligible,
-- Adding for address
PA.ADDRESS1,
PA.ADDRESS_TYPE,
PA.ADDRESS2,
PA.CITY,
PA.STATE,
PA.POSTAL home_postal,
PA2.POSTAL work_postal
from
p.PS_JOB JOB,
p.ps_t2_cloptn_effdt b,
p.ps_names pn,
p.ps_person pp,
-- Adding for address (home and work)
P.PS_ADDRESSES PA,
P.PS_ADDRESSES PA2
where 1=1
AND Job.Effdt  = (Select max(job1.Effdt) From p.PS_JOB Job1
                                         Where Job.emplid  = Job1.emplid
                                         and job.EMPL_RCD = Job1.EMPL_RCD
                                         and job1.effdt    <= current_date)
AND Job.Effseq = (Select max(job2.effseq) From p.PS_JOB Job2
                                          where Job.emplid   = Job2.emplid
                                          and job.EMPL_RCD  = job2.EMPL_RCD
                                          and job.effdt      = job2.effdt)
and Job.Empl_status in ('A','P','S')
AND Job.EMPLID NOT LIKE 'C%'
and ( (job.empl_class not in ('U')) or (job.empl_class is null) )
and job.std_hours      < 30
and b.company = job.company
and b.pf_client = job.pf_client
and b.t2_peo_id ='SOI'
and b.pf_corp <> 'PEOCN'
and b.effdt = (select max(c.effdt) from p.ps_t2_cloptn_effdt c
                   where c.company = b.company
                   and c.pf_client = b.pf_client
                   and c.effdt <= current_date)
 and pn.emplid = job.emplid
and pn.emplid =pp.emplid
and pn.name_type = 'PRI'
and pn.effdt = (select max (pn1.effdt)
                  from p.ps_names pn1
                 where pn1.emplid = pn.emplid
                   and pn1.name_type = pn.name_type
                 )
and ((Job.emplid  in ((select adr1.emplid
                     from p.ps_addresses adr1
                    where adr1.emplid =  Job.emplid
                        and adr1.eff_status = 'A'
                        and adr1.address_type = 'HOME'
                        and adr1.country in ('USA')
                        and ( ((upper(adr1.city) not like 'SAN%FRAN%') and (upper(adr1.city) <> 'SFO')) or
                              (adr1.city is null) )
                        and  adr1.effdt =(select max(adr2.effdt)
                     from p.ps_addresses adr2
                    where adr1.emplid = adr2.emplid
                      and adr1.address_type = adr2.address_type
                      and adr2.effdt <= current_date
                      )))  ) or
( job.tax_location_cd  in ( (SELECT co35.tax_location_cd
            FROM p.ps_tax_location1 co35
           WHERE job.tax_location_cd = co35.tax_location_cd
                        and ( ((upper(co35.city) not like 'SAN%FRAN%') and (upper(co35.city) <> 'SFO')) or
                              (co35.city is null) )
                ) )  )  )
-- Adding for address
and PA.EMPLID = JOB.EMPLID
and PA.EFFDT = (
    SELECT MAX(ADR2.EFFDT)
    FROM P.PS_ADDRESSES ADR2
    WHERE PA.EMPLID = ADR2.EMPLID
    AND PA.ADDRESS_TYPE = ADR2.ADDRESS_TYPE
    AND ADR2.EFFDT <= current_date
  )
AND PA.ADDRESS_TYPE in ('HOME')
AND PA.EFF_STATUS = 'A'
and PA2.EMPLID = JOB.EMPLID
and PA2.EFFDT = (
    SELECT MAX(ADR2.EFFDT)
    FROM P.PS_ADDRESSES ADR2
    WHERE PA2.EMPLID = ADR2.EMPLID
    AND PA2.ADDRESS_TYPE = ADR2.ADDRESS_TYPE
    AND ADR2.EFFDT <= current_date
  )
AND PA2.ADDRESS_TYPE in ('HOME')
AND PA2.EFF_STATUS = 'A'
order by emplid,full_part_time desc
;

-- PS data pulled from the MFT 'p.' schema (ineligible)
select
pn.first_name,
pn.last_name,
pn.middle_name,
pp.birthdate,
Job.Emplid,
Job.EMPL_RCD,
Job.Company,
job.last_hire_dt,
job.termination_dt,
job.pf_client,
job.full_part_time,
job.reg_temp,
job.std_hours,
(select translate(b.phone,'1234567890-abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ `~!!&@#$%^*_+-=|\:,./}{|><"','1234567890') from p.PS_PERSONAL_PHONE b where b.PREF_PHONE_FLAG = 'Y' and b.emplid =job.emplid) home_phone,
(select ppn.national_id from p.ps_pers_nid ppn where ppn.emplid = job.emplid and ppn.primary_nid = 'Y') national_id,
(select ppd.sex from p.ps_pers_data_effdt ppd where ppd.emplid = job.emplid and ppd.effdt = (select max(b.effdt) from p.ps_pers_data_effdt b where ppd.emplid =b.emplid and b.effdt <= current_date)) sex,
'N' as eligible,
-- Adding for address
PA.ADDRESS1,
PA.ADDRESS_TYPE,
PA.ADDRESS2,
PA.CITY,
PA.STATE,
PA.POSTAL home_postal,
PA2.POSTAL work_postal
into temp tmp_commuter_non_eligible_ees_ps
from
p.PS_JOB JOB,
p.ps_t2_cloptn_effdt b,
p.ps_names pn,
p.ps_person pp,
-- Adding for address (home and work)
P.PS_ADDRESSES PA,
P.PS_ADDRESSES PA2
Where 1=1
AND Job.Effdt  = (Select max(job1.Effdt) From p.PS_JOB Job1
                                         Where Job.emplid  = Job1.emplid
                                         and job.EMPL_RCD = Job1.EMPL_RCD
                                         and job1.termination_dt between current_date -36 and current_date -1 )
AND Job.Effseq = (Select max(job2.effseq) From p.PS_JOB Job2
                                          where Job.emplid   = Job2.emplid
                                          and job.EMPL_RCD  = job2.EMPL_RCD
                                          and job.effdt      = job2.effdt)
and Job.Empl_status in ('T')
AND Job.EMPLID NOT LIKE 'C%'
and ( (job.empl_class not in ('U')) or (job.empl_class is null) )
and job.std_hours::float >= 30
and job.full_part_time = 'F'
and job.reg_temp = 'R'
and b.company = job.company
and b.pf_client = job.pf_client
and b.t2_peo_id ='SOI'
and b.pf_corp <> 'PEOCN'
and b.effdt = (select max(c.effdt) from p.ps_t2_cloptn_effdt c
               where c.company = b.company
               and c.pf_client = b.pf_client
               and c.effdt <= current_date)
and pn.emplid = job.emplid
and pn.emplid = pp.emplid
and pn.name_type = 'PRI'
and pn.effdt = (select max (pn1.effdt)
                from p.ps_names pn1
                where pn1.emplid = pn.emplid
                and pn1.name_type = pn.name_type)
and ((job.emplid in ((select adr1.emplid
                       from p.ps_addresses adr1
                      where adr1.emplid =  job.emplid
                        and adr1.eff_status = 'A'
                        and adr1.country in ('USA')
                        and ( ((upper(adr1.city) not like 'SAN%FRAN%') and (upper(adr1.city) <> 'SFO')) or
                              (adr1.city is null) )
                        and  adr1.effdt =(select max(adr2.effdt)
                       from p.ps_addresses adr2
                      where adr1.emplid = adr2.emplid
                        and adr1.address_type = adr2.address_type
                        and adr2.effdt <= current_date
                      ))
    ))
and (job.tax_location_cd in ((select co35.tax_location_cd
                              from p.ps_tax_location1 co35
                              where job.tax_location_cd = co35.tax_location_cd
                        and ( ((upper(co35.city) not like 'SAN%FRAN%') and (upper(co35.city) <> 'SFO')) or
                              (co35.city is null) )
                            ))
    ))
-- Adding for address - home
and PA.EMPLID = JOB.EMPLID
and PA.EFFDT = (
    SELECT MAX(ADR2.EFFDT)
    FROM P.PS_ADDRESSES ADR2
    WHERE PA.EMPLID = ADR2.EMPLID
    AND PA.ADDRESS_TYPE = ADR2.ADDRESS_TYPE
    AND ADR2.EFFDT <= current_date
  )
AND PA.ADDRESS_TYPE in ('HOME')
AND PA.EFF_STATUS = 'A'
and PA2.EMPLID = JOB.EMPLID
and PA2.EFFDT = (
    SELECT MAX(ADR2.EFFDT)
    FROM P.PS_ADDRESSES ADR2
    WHERE PA2.EMPLID = ADR2.EMPLID
    AND PA2.ADDRESS_TYPE = ADR2.ADDRESS_TYPE
    AND ADR2.EFFDT <= current_date
  )
AND PA2.ADDRESS_TYPE in ('HOME')
AND PA2.EFF_STATUS = 'A'
union
select
pn.first_name,
pn.last_name,
pn.middle_name,
pp.birthdate,
Job.Emplid,
Job.EMPL_RCD,
Job.Company,
job.last_hire_dt,
job.termination_dt,
Job.pf_client,
Job.full_part_time ,
Job.reg_temp,
job.std_hours,
(select translate(b.phone,'1234567890-abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ `~!!&@#$%^*_+-=|\:,./}{|><"','1234567890') from p.PS_PERSONAL_PHONE b where b.PREF_PHONE_FLAG = 'Y' and b.emplid =Job.emplid) home_phone,
(select
ppn.national_id
from p.ps_pers_nid ppn
where ppn.emplid = job.emplid
and ppn.primary_nid = 'Y') national_id,
(select
  ppd.sex
from p.ps_pers_data_effdt  ppd
where ppd.emplid =job.emplid
and ppd.effdt =( select max(b.effdt)
                    from p.ps_pers_data_effdt b
                    where ppd.emplid =b.emplid
                    and b.effdt <= current_date)) sex,
'N' as eligible,
-- Adding for address
PA.ADDRESS1,
PA.ADDRESS_TYPE,
PA.ADDRESS2,
PA.CITY,
PA.STATE,
PA.POSTAL home_postal,
PA2.POSTAL work_postal
from
p.PS_JOB JOB,
p.ps_t2_cloptn_effdt b,
p.ps_names pn,
p.ps_person pp,
-- Adding for address (home and work)
P.PS_ADDRESSES PA,
P.PS_ADDRESSES PA2
Where 1=1
AND Job.Effdt  = (Select max(job1.Effdt) From p.PS_JOB Job1
                                         Where Job.emplid  = Job1.emplid
                                         and job.EMPL_RCD = Job1.EMPL_RCD
                                         and job1.termination_dt between current_date -36 and current_date -1 )
AND Job.Effseq = (Select max(job2.effseq) From p.PS_JOB Job2
                                          where Job.emplid   = Job2.emplid
                                          and job.EMPL_RCD  = job2.EMPL_RCD
                                          and job.effdt      = job2.effdt)
and Job.Empl_status in ('T')
AND Job.EMPLID NOT LIKE 'C%'
and ( (job.empl_class not in ('U')) or (job.empl_class is null) )
and b.company = job.company
and b.pf_client = job.pf_client
and b.t2_peo_id ='SOI'
and b.pf_corp <> 'PEOCN'
and b.effdt = (select max(c.effdt) from p.ps_t2_cloptn_effdt c
                   where c.company = b.company
                   and c.pf_client = b.pf_client
                   and c.effdt <= current_date)
 and pn.emplid = job.emplid
and pn.emplid =pp.emplid
and pn.name_type = 'PRI'
and pn.effdt = (select max (pn1.effdt)
                  from p.ps_names pn1
                 where pn1.emplid = pn.emplid
                   and pn1.name_type = pn.name_type
                 )
and ((Job.emplid  in ((select adr1.emplid
                     from p.ps_addresses adr1
                    where adr1.emplid =  Job.emplid
                        and adr1.eff_status = 'A'
                        and adr1.address_type = 'HOME'
                        and adr1.country in ('USA')
                        and ( ((upper(adr1.city) not like 'SAN%FRAN%') and (upper(adr1.city) <> 'SFO')) or
                              (adr1.city is null) )
                        and  adr1.effdt =(select max(adr2.effdt)
                     from p.ps_addresses adr2
                    where adr1.emplid = adr2.emplid
                      and adr1.address_type = adr2.address_type
                      and adr2.effdt <= current_date
                      )))  ) or
( job.tax_location_cd  in ( (SELECT co35.tax_location_cd
            FROM p.ps_tax_location1 co35
           WHERE job.tax_location_cd = co35.tax_location_cd
                        and ( ((upper(co35.city) not like 'SAN%FRAN%') and (upper(co35.city) <> 'SFO')) or
                              (co35.city is null) )
                ) )  )  )
-- Adding for address
and PA.EMPLID = JOB.EMPLID
and PA.EFFDT = (
    SELECT MAX(ADR2.EFFDT)
    FROM P.PS_ADDRESSES ADR2
    WHERE PA.EMPLID = ADR2.EMPLID
    AND PA.ADDRESS_TYPE = ADR2.ADDRESS_TYPE
    AND ADR2.EFFDT <= current_date
  )
AND PA.ADDRESS_TYPE in ('HOME')
AND PA.EFF_STATUS = 'A'
and PA2.EMPLID = JOB.EMPLID
and PA2.EFFDT = (
    SELECT MAX(ADR2.EFFDT)
    FROM P.PS_ADDRESSES ADR2
    WHERE PA2.EMPLID = ADR2.EMPLID
    AND PA2.ADDRESS_TYPE = ADR2.ADDRESS_TYPE
    AND ADR2.EFFDT <= current_date
  )
AND PA2.ADDRESS_TYPE in ('HOME')
AND PA2.EFF_STATUS = 'A'
union
Select
pn.first_name,
pn.last_name,
pn.middle_name,
pp.birthdate,
Job.Emplid,
Job.EMPL_RCD  ,
Job.Company ,
job.last_hire_dt,
job.termination_dt,
job.pf_client ,
job.full_part_time ,
job.reg_temp,
job.std_hours,
(select translate(b.phone,'1234567890-abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ `~!!&@#$%^*_+-=|\:,./}{|><"','1234567890') from p.PS_PERSONAL_PHONE b where b.PREF_PHONE_FLAG = 'Y' and b.emplid =job.emplid) home_phone,
(select
ppn.national_id
from p.ps_pers_nid ppn
where ppn.emplid = job.emplid
and ppn.primary_nid = 'Y') national_id,
(select
 ppd.sex
from p.ps_pers_data_effdt  ppd
where ppd.emplid =job.emplid
and ppd.effdt =( select max(b.effdt)
                    from p.ps_pers_data_effdt b
                    where ppd.emplid =b.emplid
                    and b.effdt <= current_date)) sex,
'N' as eligible,
-- Adding for address
PA.ADDRESS1,
PA.ADDRESS_TYPE,
PA.ADDRESS2,
PA.CITY,
PA.STATE,
PA.POSTAL home_postal,
PA2.POSTAL work_postal
from
p.PS_JOB JOB,
p.ps_t2_cloptn_effdt b,
p.ps_names pn,
p.ps_person pp,
-- Adding for address (home and work)
P.PS_ADDRESSES PA,
P.PS_ADDRESSES PA2
Where 1=1
AND Job.Effdt  = (Select max(job1.Effdt) From p.PS_JOB Job1
                                         Where Job.emplid  = Job1.emplid
                                         and job.EMPL_RCD = Job1.EMPL_RCD
                                         and job1.termination_dt between current_date - 36 and current_date - 1)
AND Job.Effseq = (Select max(job2.effseq) From p.PS_JOB Job2
                                          where Job.emplid   = Job2.emplid
                                          and job.EMPL_RCD  = job2.EMPL_RCD
                                          and job.effdt      = job2.effdt)
and Job.Empl_status in ('T')
AND Job.EMPLID NOT LIKE 'C%'
and ( (job.empl_class not in ('U')) or (job.empl_class is null) )
and job.std_hours      < 30
and b.company = job.company
and b.pf_client = job.pf_client
and b.t2_peo_id ='SOI'
and b.pf_corp <> 'PEOCN'
and b.effdt = (select max(c.effdt) from p.ps_t2_cloptn_effdt c
                   where c.company = b.company
                   and c.pf_client = b.pf_client
                   and c.effdt <= current_date)
 and pn.emplid = job.emplid
and pn.emplid =pp.emplid
and pn.name_type = 'PRI'
and pn.effdt = (select max (pn1.effdt)
                  from p.ps_names pn1
                 where pn1.emplid = pn.emplid
                   and pn1.name_type = pn.name_type
                 )
and ((Job.emplid  in ((select adr1.emplid
                     from p.ps_addresses adr1
                    where adr1.emplid =  Job.emplid
                        and adr1.eff_status = 'A'
                        and adr1.address_type = 'HOME'
                        and adr1.country in ('USA')
                        and ( ((upper(adr1.city) not like 'SAN%FRAN%') and (upper(adr1.city) <> 'SFO')) or
                              (adr1.city is null) )
                        and  adr1.effdt =(select max(adr2.effdt)
                     from p.ps_addresses adr2
                    where adr1.emplid = adr2.emplid
                      and adr1.address_type = adr2.address_type
                      and adr2.effdt <= current_date
                      )))  ) or
( job.tax_location_cd  in ( (SELECT co35.tax_location_cd
            FROM p.ps_tax_location1 co35
           WHERE job.tax_location_cd = co35.tax_location_cd
                        and ( ((upper(co35.city) not like 'SAN%FRAN%') and (upper(co35.city) <> 'SFO')) or
                              (co35.city is null) )
                ) )  )  )
-- Adding for address
and PA.EMPLID = JOB.EMPLID
and PA.EFFDT = (
    SELECT MAX(ADR2.EFFDT)
    FROM P.PS_ADDRESSES ADR2
    WHERE PA.EMPLID = ADR2.EMPLID
    AND PA.ADDRESS_TYPE = ADR2.ADDRESS_TYPE
    AND ADR2.EFFDT <= current_date
  )
AND PA.ADDRESS_TYPE in ('HOME')
AND PA.EFF_STATUS = 'A'
and PA2.EMPLID = JOB.EMPLID
and PA2.EFFDT = (
    SELECT MAX(ADR2.EFFDT)
    FROM P.PS_ADDRESSES ADR2
    WHERE PA2.EMPLID = ADR2.EMPLID
    AND PA2.ADDRESS_TYPE = ADR2.ADDRESS_TYPE
    AND ADR2.EFFDT <= current_date
  )
AND PA2.ADDRESS_TYPE in ('HOME')
AND PA2.EFF_STATUS = 'A'
order by emplid,full_part_time desc
;

-- Grab data from all sources to return.
(select *
from tmp_commuter_eligible_ees_combined)
union
(select distinct
      null::varchar as record_type
    , null::varchar as group_id
    , Emplid as employee_id
    , national_id as employee_ssn
    , null::varchar as change_to_unique_id
    , null::varchar as old_unique_id
    , last_name as last_name
    , first_name as first_name
    , coalesce(substr(middle_name, 0, 1), '') as middle_initial
    , birthdate as birth_date
    , coalesce(nullif(sex, ''), 'U') as gender
    , ADDRESS1 street_address_1
    , ADDRESS2 as street_address_2
    , CITY as city
    , STATE as state
    , home_postal as zip_code  -- LIVE ZIP
    , null::varchar as zip_code_extension
    , work_postal as work_zip_code  -- WORK ZIP
    , null::varchar as email_address
    , home_phone as phone
    , null::varchar as phone_extension
    , last_hire_dt as employment_hire_date
    , termination_dt as employment_termination_date
    , null::varchar as benefit_group_code
    , null::varchar as benefit_group_effective_date
    , null::varchar as payroll_group_code
    , null::varchar as payroll_group_effective_date
    , pf_client as location_code
    -- Confirm PS fields to use.
    , Company as company_code
    , null::varchar as for_future_use_1
    , null::varchar as for_future_use_2
    , null::varchar as bank_name
    , null::varchar as bank_account_number
    , null::varchar as bank_routing_number
    , null::varchar as bank_account_type
    , null::varchar as payment_preference
    , eligible as commuter_program_eligibility_status
    , Company as client_id
    -- n/a in PS data I believe
    , null::varchar as payroll_type
from
tmp_commuter_eligible_ees_ps)
union
(select distinct
      null::varchar as record_type
    , null::varchar as group_id
    , Emplid as employee_id
    , national_id as employee_ssn
    , null::varchar as change_to_unique_id
    , null::varchar as old_unique_id
    , last_name as last_name
    , first_name as first_name
    , coalesce(substr(middle_name, 0, 1), '') as middle_initial
    , birthdate as birth_date
    , coalesce(nullif(sex, ''), 'U') as gender
    , ADDRESS1 street_address_1
    , ADDRESS2 as street_address_2
    , CITY as city
    , STATE as state
    , home_postal as zip_code  -- LIVE ZIP
    , null::varchar as zip_code_extension
    , work_postal as work_zip_code  -- WORK ZIP
    , null::varchar as email_address
    , home_phone as phone
    , null::varchar as phone_extension
    , last_hire_dt as employment_hire_date
    , termination_dt as employment_termination_date
    , null::varchar as benefit_group_code
    , null::varchar as benefit_group_effective_date
    , null::varchar as payroll_group_code
    , null::varchar as payroll_group_effective_date
    , pf_client as location_code
    -- Confirm PS fields to use.
    , Company as company_code
    , null::varchar as for_future_use_1
    , null::varchar as for_future_use_2
    , null::varchar as bank_name
    , null::varchar as bank_account_number
    , null::varchar as bank_routing_number
    , null::varchar as bank_account_type
    , null::varchar as payment_preference
    , eligible as commuter_program_eligibility_status
    , Company as client_id
    -- n/a in PS data I believe
    , null::varchar as payroll_type
from
tmp_commuter_non_eligible_ees_ps)
order by employee_id asc;
