
create or replace TABLE EDW.D_DATE (
	CALENDAR_DATE DATE NOT NULL,
	WEEKDAY_NBR NUMBER(38,0) NOT NULL,
	WEEKDAY_NAME VARCHAR(9) NOT NULL,
	WEEK_NBR NUMBER(38,0) NOT NULL,
	WEEK_NAME VARCHAR(7) NOT NULL,
	MONTH_NBR NUMBER(38,0) NOT NULL,
	MONTH_NAME VARCHAR(10) NOT NULL,
	QUARTER_NBR NUMBER(38,0) NOT NULL,
	QUARTER_NAME VARCHAR(10) NOT NULL,
	YEAR NUMBER(38,0) NOT NULL,
	CURRENT_WEEK_START_DATE DATE NOT NULL,
	CURRENT_WEEK_END_DATE DATE NOT NULL,
	CURRENT_MONTH_START_DATE DATE NOT NULL,
	CURRENT_MONTH_END_DATE DATE NOT NULL,
	TRAILING_WEEK1_START_DATE DATE NOT NULL,
	TRAILING_WEEK2_START_DATE DATE NOT NULL,
	TRAILING_WEEK3_START_DATE DATE NOT NULL,
	TRAILING_WEEK4_START_DATE DATE NOT NULL,
	TRAILING_WEEK5_START_DATE DATE NOT NULL,
	TRAILING_WEEK6_START_DATE DATE NOT NULL,
	TRAILING_WEEK7_START_DATE DATE NOT NULL,
	TRAILING_WEEK8_START_DATE DATE NOT NULL,
	TRAILING_WEEK9_START_DATE DATE NOT NULL,
	TRAILING_WEEK10_START_DATE DATE NOT NULL,
	TRAILING_WEEK11_START_DATE DATE NOT NULL,
	TRAILING_WEEK12_START_DATE DATE NOT NULL,
	TRAILING_WEEK1_END_DATE DATE NOT NULL,
	TRAILING_WEEK2_END_DATE DATE NOT NULL,
	TRAILING_WEEK3_END_DATE DATE NOT NULL,
	TRAILING_WEEK4_END_DATE DATE NOT NULL,
	TRAILING_WEEK5_END_DATE DATE NOT NULL,
	TRAILING_WEEK6_END_DATE DATE NOT NULL,
	TRAILING_WEEK7_END_DATE DATE NOT NULL,
	TRAILING_WEEK8_END_DATE DATE NOT NULL,
	TRAILING_WEEK9_END_DATE DATE NOT NULL,
	TRAILING_WEEK10_END_DATE DATE NOT NULL,
	TRAILING_WEEK11_END_DATE DATE NOT NULL,
	TRAILING_WEEK12_END_DATE DATE NOT NULL,
	TRAILING_MONTH1_START_DATE DATE NOT NULL,
	TRAILING_MONTH2_START_DATE DATE NOT NULL,
	TRAILING_MONTH3_START_DATE DATE NOT NULL,
	TRAILING_MONTH4_START_DATE DATE NOT NULL,
	TRAILING_MONTH5_START_DATE DATE NOT NULL,
	TRAILING_MONTH6_START_DATE DATE NOT NULL,
	TRAILING_MONTH7_START_DATE DATE NOT NULL,
	TRAILING_MONTH8_START_DATE DATE NOT NULL,
	TRAILING_MONTH9_START_DATE DATE NOT NULL,
	TRAILING_MONTH10_START_DATE DATE NOT NULL,
	TRAILING_MONTH11_START_DATE DATE NOT NULL,
	TRAILING_MONTH12_START_DATE DATE NOT NULL,
	TRAILING_MONTH1_END_DATE DATE NOT NULL,
	TRAILING_MONTH2_END_DATE DATE NOT NULL,
	TRAILING_MONTH3_END_DATE DATE NOT NULL,
	TRAILING_MONTH4_END_DATE DATE NOT NULL,
	TRAILING_MONTH5_END_DATE DATE NOT NULL,
	TRAILING_MONTH6_END_DATE DATE NOT NULL,
	TRAILING_MONTH7_END_DATE DATE NOT NULL,
	TRAILING_MONTH8_END_DATE DATE NOT NULL,
	TRAILING_MONTH9_END_DATE DATE NOT NULL,
	TRAILING_MONTH10_END_DATE DATE NOT NULL,
	TRAILING_MONTH11_END_DATE DATE NOT NULL,
	TRAILING_MONTH12_END_DATE DATE NOT NULL,
	DW_INSERT_USER VARCHAR(512) NOT NULL,
	DW_UPDATE_USER VARCHAR(512) NOT NULL,
	DW_INSERT_DATE TIMESTAMP_NTZ(9) NOT NULL,
	DW_UPDATE_DATE TIMESTAMP_NTZ(9) NOT NULL
);


create or replace procedure edw.load_d_date(start_date date, end_date date)
returns varchar not null
language sql
COMMENT = ' author: Jim Miller
            created: 06 May 2022
            modified: 
            
            description: inserts rows into date dimension
                         includes 1900-01-01 to be used as the "unknown member"
                         does not update or delete rows
            
            examples: 
               call edw.load_d_date(null, null);
               call edw.load_d_date([start_date],[end_date]);
          '
as
declare
    number_of_days int;    
begin  
  if (:start_date is null) then
    start_date := CURRENT_DATE() - 1500;
  end if;
  
  if (:end_date is null) then
    end_date := CURRENT_DATE() + 366;
  end if;

  number_of_days := datediff(day, :start_date, :end_date);

create or replace transient table raw.d_date as (
with date_dim as 
  (
  select :end_date as date_id 
  )
  , DATE_RANGE AS 
  (
  SELECT date_id ,DATEADD(DAY, -1 * SEQ4(), date_dim.date_id ) AS USE_DATE
    FROM date_dim,TABLE(GENERATOR(ROWCOUNT => (:number_of_days) )) 
  )
  SELECT USE_DATE FROM DATE_RANGE order by USE_DATE desc);

 insert into raw.d_date values ('1900-01-01');

 insert into edw.d_date
 select USE_DATE as calendar_date
      , DATE_PART(dw, USE_DATE) + 1 as weekday_nbr
      , dayname(USE_DATE) as weekday_name     
      , DATE_PART(WK, USE_DATE) as week_nbr
      , 'Week ' || DATE_PART(WK, USE_DATE) as week_name     
      , DATE_PART(MM, USE_DATE) as month_nbr
      , monthname(USE_DATE) as month_name    
      , DATE_PART(Q, USE_DATE) as quarter_nbr
      , 'Qtr ' || DATE_PART(Q, USE_DATE) as quarter_name
      , DATE_PART(Y, USE_DATE) as year
      
      , last_day(USE_DATE+1, 'WK')-7 as current_week_start_date
      , last_day(USE_DATE+1, 'WK')-1 as current_week_end_date
      
      , last_day(dateadd('MM', -1, USE_DATE), 'MM')+1 as current_month_start_date
      , last_day(USE_DATE, 'MM') as current_month_end_date
            
      , last_day(USE_DATE-(7*1 )+1, 'WK')-7 as trailing_week1_start_date
      , last_day(USE_DATE-(7*2 )+1, 'WK')-7 as trailing_week2_start_date
      , last_day(USE_DATE-(7*3 )+1, 'WK')-7 as trailing_week3_start_date
      , last_day(USE_DATE-(7*4 )+1, 'WK')-7 as trailing_week4_start_date
      , last_day(USE_DATE-(7*5 )+1, 'WK')-7 as trailing_week5_start_date
      , last_day(USE_DATE-(7*6 )+1, 'WK')-7 as trailing_week6_start_date
      , last_day(USE_DATE-(7*7 )+1, 'WK')-7 as trailing_week7_start_date
      , last_day(USE_DATE-(7*8 )+1, 'WK')-7 as trailing_week8_start_date
      , last_day(USE_DATE-(7*9 )+1, 'WK')-7 as trailing_week9_start_date
      , last_day(USE_DATE-(7*10)+1, 'WK')-7 as trailing_week10_start_date
      , last_day(USE_DATE-(7*11)+1, 'WK')-7 as trailing_week11_start_date
      , last_day(USE_DATE-(7*12)+1, 'WK')-7 as trailing_week12_start_date
                    
      , last_day(USE_DATE-(7*1 )+1, 'WK')-1 as trailing_week1_end_date
      , last_day(USE_DATE-(7*2 )+1, 'WK')-1 as trailing_week2_end_date
      , last_day(USE_DATE-(7*3 )+1, 'WK')-1 as trailing_week3_end_date
      , last_day(USE_DATE-(7*4 )+1, 'WK')-1 as trailing_week4_end_date
      , last_day(USE_DATE-(7*5 )+1, 'WK')-1 as trailing_week5_end_date
      , last_day(USE_DATE-(7*6 )+1, 'WK')-1 as trailing_week6_end_date
      , last_day(USE_DATE-(7*7 )+1, 'WK')-1 as trailing_week7_end_date
      , last_day(USE_DATE-(7*8 )+1, 'WK')-1 as trailing_week8_end_date
      , last_day(USE_DATE-(7*9 )+1, 'WK')-1 as trailing_week9_end_date
      , last_day(USE_DATE-(7*10)+1, 'WK')-1 as trailing_week10_end_date
      , last_day(USE_DATE-(7*11)+1, 'WK')-1 as trailing_week11_end_date
      , last_day(USE_DATE-(7*12)+1, 'WK')-1 as trailing_week12_end_date                    
                         
      , last_day(dateadd('MM', -2, USE_DATE), 'MM')+1 as trailing_month1_start_date
      , last_day(dateadd('MM', -3, USE_DATE), 'MM')+1 as trailing_month2_start_date
      , last_day(dateadd('MM', -4, USE_DATE), 'MM')+1 as trailing_month3_start_date
      , last_day(dateadd('MM', -5, USE_DATE), 'MM')+1 as trailing_month4_start_date
      , last_day(dateadd('MM', -6, USE_DATE), 'MM')+1 as trailing_month5_start_date
      , last_day(dateadd('MM', -7, USE_DATE), 'MM')+1 as trailing_month6_start_date
      , last_day(dateadd('MM', -8, USE_DATE), 'MM')+1 as trailing_month7_start_date
      , last_day(dateadd('MM', -9, USE_DATE), 'MM')+1 as trailing_month8_start_date
      , last_day(dateadd('MM', -10,USE_DATE), 'MM')+1 as trailing_month9_start_date
      , last_day(dateadd('MM', -11,USE_DATE), 'MM')+1 as trailing_month10_start_date
      , last_day(dateadd('MM', -12,USE_DATE), 'MM')+1 as trailing_month11_start_date
      , last_day(dateadd('MM', -13,USE_DATE), 'MM')+1 as trailing_month12_start_date
      
      , last_day(dateadd('MM', -1, USE_DATE), 'MM') as trailing_month1_end_date     
      , last_day(dateadd('MM', -2, USE_DATE), 'MM') as trailing_month2_end_date     
      , last_day(dateadd('MM', -3, USE_DATE), 'MM') as trailing_month3_end_date     
      , last_day(dateadd('MM', -4, USE_DATE), 'MM') as trailing_month4_end_date     
      , last_day(dateadd('MM', -5, USE_DATE), 'MM') as trailing_month5_end_date     
      , last_day(dateadd('MM', -6, USE_DATE), 'MM') as trailing_month6_end_date     
      , last_day(dateadd('MM', -7, USE_DATE), 'MM') as trailing_month7_end_date     
      , last_day(dateadd('MM', -8, USE_DATE), 'MM') as trailing_month8_end_date     
      , last_day(dateadd('MM', -9, USE_DATE), 'MM') as trailing_month9_end_date     
      , last_day(dateadd('MM', -10,USE_DATE), 'MM') as trailing_month10_end_date     
      , last_day(dateadd('MM', -11,USE_DATE), 'MM') as trailing_month11_end_date     
      , last_day(dateadd('MM', -12,USE_DATE), 'MM') as trailing_month12_end_date     
      
      , CURRENT_USER() as dw_insert_user
      , CURRENT_USER() as dw_update_user
      
      , current_date() as dw_insert_date
      , current_date() as dw_update_date
      
   from raw.d_date d
   where not exists
     ( select 1 
	     from edw.d_date
		where calendar_date = d.USE_DATE );
   
   drop table raw.d_date;
  
  return 'rows inserted: ' || sqlrowcount::varchar;
end;


call edw.load_d_date(null, null);
