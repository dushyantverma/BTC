{{ config(materialized='incremental', incremental_strategy='append')}}


with CTE as (
select 
tx.hash_key,
tx.block_number,
tx.block_timestamp,
tx.is_coinbase,
f.value:address::string as out_address,
f.value:value::float  as out_value
from {{ref('stg_btc')}} tx,
LATERAL FLATTEN(input => outputs) f
where f.value:address is not null 



{% if is_incremental() %}


and block_timestamp >= (select max(block_timestamp) from {{ this }})

{% endif %}

)

select 
hash_key,
block_number,
block_timestamp,
is_coinbase,
out_address,
out_value
from cte