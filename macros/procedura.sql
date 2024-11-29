{% macro procedura(table_delta, table_finale, table_configuration) %}
 
 {%set querymode%}
 select update_mode from dati_configuration.{{table_configuration}}
 {%endset%}
 {% set result=run_query(querymode)%}

{% if result %}
    {% set update_mode_value = result[0].UPDATE_MODE %}
{% else %}
    {% set update_mode_value = 'default_value' %}
{% endif %}
    

    {% if update_mode_value=='full' %} 
        {% set query1 %}
            TRUNCATE TABLE dati_finali.{{table_finale }}
        {% endset %}
        {% do run_query(query1) %}
       
        {% set query %}
            INSERT INTO dati_finali.{{ table_finale }} 
            (SELECT * FROM dati_delta.{{ table_delta }})

        {% endset %}
        {% do run_query(query)%}
       
         
    {% elif update_mode_value == 'append' %} 
        {% set query %}
            INSERT INTO dati_finali.{{ table_finale }} 
            SELECT *
            FROM dati_delta.{{ table_delta }} AS delta
            WHERE NOT EXISTS (
            SELECT 1
            FROM dati_finali.{{ table_finale }} AS final
             WHERE delta.nome_prod = final.nome_prod
            AND delta.cod_prod = final.cod_prod
            AND delta.categoria_prod = final.categoria_prod
            AND delta.anno = final.anno
            )
        {% endset %}
        {% do run_query(query)%}


    {% elif update_mode_value == 'upsert' %} 
        {% set query %}
            MERGE INTO dati_finali.{{ table_finale }} AS final
            USING dati_delta.{{ table_delta }} AS delta
            ON final.cod_prod = delta.cod_prod
            WHEN MATCHED THEN
                UPDATE SET
                final.id=delta.id,
                    final.nome_prod = delta.nome_prod,
                    final.categoria_prod = delta.categoria_prod,
                    final.anno = delta.anno
            WHEN NOT MATCHED THEN
                INSERT (id,nome_prod,cod_prod, categoria_prod, anno)
                VALUES (delta.id,delta.nome_prod,delta.cod_prod, delta.categoria_prod, delta.anno)
        {% endset %}
        {%do  run_query(query)%}

        {% else %}
        {% set query %}
            MERGE INTO dati_finali.{{ table_finale }} AS final
            USING dati_delta.{{ table_delta }} AS delta
            ON final.cod_prod = delta.cod_prod
            WHEN MATCHED THEN DELETE
            WHEN NOT MATCHED THEN
                INSERT (id,nome_prod,cod_prod, categoria_prod, anno)
                VALUES (delta.id,delta.nome_prod,delta.cod_prod, delta.categoria_prod, delta.anno)
        {% endset %}
     {%do  run_query(query)%}
 {{ return(query)}}
    {% endif %}

{% endmacro %}
