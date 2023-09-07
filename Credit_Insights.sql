-- Q1 write a query to print top 5 cities with highest spends and their percentage contribution of total credit card spends
with CTE1 as (
select City, sum(Amount) as Total from credit_insights
group by City order by Total desc limit 5),
CTE2 as (
select sum(Amount) as Total1 from credit_insights)
select CTE1.City, CTE1.Total,round(Total/Total1*100,2) as Percentage from CTE1,CTE2;

-- Q2 write a query to print highest spend month and amount spent in that month for each card type
with CTE1 as (
select extract(Month from Date) as Month_from_Date,
extract(Year from Date) as Year_from_Date, Card_Type, sum(Amount) as Total_Amount from credit_insights
group by Month_from_Date, Year_from_Date, Card_Type),
CTE2 as  (
select *, 
	dense_rank() over(partition by
    Card_Type order by total_amount desc) as rnk 
	from CTE1)
select * from CTE2 where rnk =1 ;

-- Q3 write a query to print the transaction details(all columns from the table) for each card type when it reaches a cumulative of 1000000 total spends(We should have 4 rows in the o/p one for each card type
with CTE1 as
(select *,
 sum(Amount) 
 over(partition by Card_Type
 order by Date)as cumulative_sum 
 from credit_insights),
CTE2 as (
	select *, dense_rank() 
	over(partition by Card_Type 
    order by cumulative_sum) as rnk 
	from cte1 
	where cumulative_sum >=1000000)
select * from CTE2 where rnk =1 ;

-- Q4 write a query to find city which had lowest percentage spend for gold card type
with CTE1 as
(select City,
  sum(Amount) as total_gold_amount
  from credit_insights
  where Card_Type ='Gold' 
  group by City),
CTE2 as (
	select City,
	sum(Amount) as trans_amount
    from credit_insights
	group by City),
CTE3 as (
	select c1.City,
	c1.total_gold_amount,
	c2.trans_amount,
    round(cast(c1.total_gold_amount as decimal) / c2.trans_amount * 100,2) as per_contribution
    from CTE1 c1 
	inner join cte2 c2 on c1.City = c2.City)
select * from CTE3
order by per_contribution limit 1;

-- Q5 write a query to print 3 columns: city, highest_expense_type , lowest_expense_type (example format : Delhi , bills, Fuel
with CTE1 as
(select city, 
 Exp_Type,
 sum(amount) as total_amount 
 from credit_insights
 group by city,Exp_Type),
 CTE2 as (
 select city, 
	 max(total_amount)  as highest_expense,
     min(total_amount) as lowest_expense 
	 from CTE1 group by city)
select c1.city,
   max(case when total_amount = highest_expense then Exp_Type end) as highest_expense_type,
   min(case when total_amount = lowest_expense then Exp_Type end) as lowest_expense_type
   from CTE1 c1
   inner join CTE2 c2 on c1.city= c2.city
   group by c1.city
   order by c1.city;
   
-- Q6 write a query to find percentage contribution of spends by females for each expense type
with CTE1 as(
select Exp_Type, sum(Amount) as total 
from credit_insights
where gender='F'
group by Exp_Type),
CTE2 as (
select Exp_Type, sum(Amount) as total_amount
from credit_insights
group by Exp_Type)
select c1.Exp_Type,c1.total,
c2.total_amount,round(cast(c1.total as decimal)/c2.total_amount * 100,2) as
percentage_spends from cte1 as c1
inner join cte2 as c2 on
c1.Exp_Type = c2.Exp_Type;

-- Q7 which card and expense type combination saw highest month over month growth in Jan-2014
with month_year_spend as (
	select 
	Card_Type, 
	Exp_Type, 
	Extract(month from date) as spend_month, 
	Extract(year from date) as spend_year,
	sum(amount) as spend
	from credit_insights
	Group By Card_Type, Exp_Type, spend_month, spend_year
)	
,get_prev_spend as (
	select *
    ,lag(spend,1)over(partition by Card_Type, Exp_Type order by spend_year, spend_month) as lag_spend
	from month_year_spend
)	
select *, 
(spend-lag_spend) as growth
from get_prev_spend
where spend_month = 1 and spend_year = 2014 and(spend-lag_spend) > 0 
order by (spend-lag_spend) desc limit 1;

-- Q8 during weekends which city has highest total spend to total no of transcations ratio
select city,
sum(amount) as total,
count(1) as total_no_of_trans,
sum(amount) / count(1) as ratio
from credit_insights
group by city
order by ratio desc
limit 1;

-- Q9 which city took least number of days to reach its 500th transaction after the first transaction in that city
with CTE1  AS (
	select c1.city, c1.date from  
( select city, date, dense_rank()over(partition by city order by date asc) rank_txns
from credit_insights) c1 where c1.rank_txns = 1
)
, CTE2 AS (
	select c1.city, c1.date, c1.rns from
( select city, date, ROW_NUMBER()over(partition by city order by date asc) rns 
from credit_insights) c1
where c1.rns = 500
)
select f.city as city, f.date as first_txn_date, l.date as txn_date_500th, l.date-f.date as days
from cte1 as f join cte2 as l on f.city = l.city
order by l.date-f.date limit 1;
   

