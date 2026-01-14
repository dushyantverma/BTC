with CTE AS (
select 
out_address,
sum(out_value) as total_sent,
count(*) as tx_count
from {{ref("stg_btc_transactions")}}
where out_value > 10
group by out_address
order by total_sent desc
)

select 
A.out_address,
A.total_sent,
A.tx_count,
{{convert_to_usd('A.total_sent')}} AS total_sent_usd,
from CTE A 
order by total_sent desc