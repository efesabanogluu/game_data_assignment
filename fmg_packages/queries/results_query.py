from fmg_packages.utils.constants import TARGET_CLEANSED_TABLE, START_DATE, END_DATE, COUNTRY, VERSION

results_query = """ WITH retention_dates as (
SELECT * FROM UNNEST(GENERATE_DATE_ARRAY("{START_DATE}", "{END_DATE}", INTERVAL 1 DAY)) AS retention_date
)
,day_zero as (
  SELECT user_pseudo_id, DATE(min(event_timestamp)) as zero FROM {TARGET_CLEANSED_TABLE}
  where country="{COUNTRY}" and version="{VERSION}" and DATE(event_timestamp) >= "2021-7-7" and event_name="finish_login"
  group by user_pseudo_id
),
all_day_one as (
  SELECT count(1) as sum_day_one, DATE_ADD(DATE(zero), INTERVAL 1 DAY) as retention_date
   FROM `day_zero` 
  where DATE_ADD(DATE(zero), INTERVAL 1 DAY) >= "{START_DATE}" and "{END_DATE}" >= DATE_ADD(DATE(zero) ,INTERVAL 1 DAY)
  group by DATE_ADD(DATE(zero), INTERVAL 1 DAY)
),
retained_day_one as (
   SELECT COUNT(DISTINCT rd.user_pseudo_id) as retained_day_one, DATE(event_timestamp) as retention_date 
  FROM {TARGET_CLEANSED_TABLE} rd 
  JOIN day_zero dz on rd.user_pseudo_id=dz.user_pseudo_id and DATE_DIFF(DATE(rd.event_timestamp), dz.zero, DAY)=1
  and rd.event_name="finish_login"
  WHERE DATE(event_timestamp) >= "{START_DATE}" and "{END_DATE}" >= DATE(event_timestamp) group by DATE(event_timestamp) 
),
all_day_fourteen as (
  SELECT count(1) as sum_day_fourteen, DATE_ADD(DATE(zero), INTERVAL 14 DAY) as retention_date
   FROM `day_zero` 
  where DATE_ADD(DATE(zero), INTERVAL 14 DAY) >= "{START_DATE}" and "{END_DATE}" >= DATE_ADD(DATE(zero) ,INTERVAL 14 DAY)
  group by DATE_ADD(DATE(zero), INTERVAL 14 DAY)
),
retained_day_fourteen as (
   SELECT COUNT(DISTINCT rd.user_pseudo_id) as retained_day_fourteen, DATE(event_timestamp) as retention_date
     FROM {TARGET_CLEANSED_TABLE} rd 
  JOIN day_zero dz on rd.user_pseudo_id=dz.user_pseudo_id and DATE_DIFF(DATE(rd.event_timestamp), dz.zero, DAY)=14
  and rd.event_name="finish_login"
  WHERE DATE(event_timestamp) >= "{START_DATE}" and "{END_DATE}" >= DATE(event_timestamp) group by DATE(event_timestamp) 
),
all_day_seven as (
  SELECT count(1) as sum_day_seven, DATE_ADD(DATE(zero), INTERVAL 7 DAY) as retention_date
   FROM `day_zero` 
  where DATE_ADD(DATE(zero), INTERVAL 7 DAY) >= "{START_DATE}" and "{END_DATE}" >= DATE_ADD(DATE(zero) ,INTERVAL 7 DAY)
  group by DATE_ADD(DATE(zero), INTERVAL 7 DAY)
),
retained_day_seven as (
   SELECT COUNT(DISTINCT rd.user_pseudo_id) as retained_day_seven, DATE(event_timestamp) as retention_date
  FROM {TARGET_CLEANSED_TABLE} rd 
  JOIN day_zero dz on rd.user_pseudo_id=dz.user_pseudo_id and DATE_DIFF(DATE(rd.event_timestamp), dz.zero, DAY)=7
  and rd.event_name="finish_login"
  WHERE DATE(event_timestamp) >= "{START_DATE}" and "{END_DATE}" >= DATE(event_timestamp) group by DATE(event_timestamp) 
)
select ado.retention_date , retained_day_one/sum_day_one as retention_rate_day_one,
 retained_day_seven/sum_day_seven as retention_rate_day_seven, 
 retained_day_fourteen/sum_day_fourteen as retention_rate_day_fourteen
from retention_dates
left join all_day_one ado using(retention_date)
left join retained_day_one rdo using(retention_date)
left join retained_day_seven rds using(retention_date)
left join all_day_seven ads using(retention_date)
left join retained_day_fourteen rdf using(retention_date)
left join all_day_fourteen adf using(retention_date)
order by retention_date asc """.format(TARGET_CLEANSED_TABLE=TARGET_CLEANSED_TABLE,START_DATE=START_DATE,END_DATE=END_DATE,
                                       COUNTRY=COUNTRY, VERSION = VERSION)