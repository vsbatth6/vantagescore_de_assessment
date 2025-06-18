-- Analytical Queries

-- Monthly Revenue: total amount per month for the last 3 months

-- where clause is most important in this query. I did minus 2 months becuase current month is factored as being the 3rd month.
-- That means previous 2 months need to be considered. After the 'minus 2 months' get the start of month which is the first of the month.
-- To complete the filter txn_date needs to be greater than or equal to the start of the first month.
-- Then simply group by the year_month and sum amount
select
strftime('%Y-%m', txn_date) as Year_Month,
sum(amount)
from fact_transaction
where txn_date >= date(date('now', '-2 months'), 'start of month')
GROUP BY strftime('%Y-%m', txn_date)
ORDER BY Year_Month;


-- New-Customer Spend: average spend within 30 days of signup
-- average amount based on days_since_signup less than or equal to 30
select avg(amount)
from fact_transaction
where days_since_signup <= 30;


-- Top 5 Customers: highest total spend in the last 90 days
-- group by customer_id and get total_spend per customer
with cte_total_spend as (
select customer_id, sum(amount) as total_spend
from fact_transaction
where txn_date > date('now', '-90 days')
group by customer_id
),
-- use dense_rank to account for ties in total_spend
cte_ranked_spend as (
select customer_id, total_spend, dense_rank() over(order by total_spend desc) as ranked_spend
from cte_total_spend
)
-- select top 5 customers based on dense_rank
select customer_id, total_spend
from cte_ranked_spend
where ranked_spend <= 5;
