with

dati_delta as(
    select * from {{ source("dati_delta", "dati_delta")}}
),

dati_finali as(
    select * from {{ source("dati_finali", "dati_finali")}}
),

table_configuration as(
    select * from {{ source("dati_configuration", "table_configuration")}}
)

{{ procedura('dati_delta','dati_finali', 'table_configuration')}}

select * from dati_finali 