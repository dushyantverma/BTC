{{ config(materialized='ephemeral')}}

select * 
from {{ref('btc_outputs')}}
where is_coinbase = false