select f.*
from {{ ref('fct_store_sales') }} f
join {{ ref('dim_date') }} d
    on f.date_key = d.date_key
where d.date > current_date()