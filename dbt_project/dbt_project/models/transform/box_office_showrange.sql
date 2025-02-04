WITH base_data AS (
    {% for table_name in var('table_names', []) %}
        -- 로그 추가: 현재 table_name 출력
        {% do log("Processing table_name: " ~ table_name, info=True) %}

        SELECT
            showRange,
            SUM("{{ table_name | replace('_box_office', '') }}_sales") AS total_sales,
            SUM("{{ table_name | replace('_box_office', '') }}_total_sales") AS total_total_sales,
            SUM("{{ table_name | replace('_box_office', '') }}_audience_num") AS total_audience_num,
            SUM("{{ table_name | replace('_box_office', '') }}_total_audience_num") AS total_total_audience_num,
            SUM("{{ table_name | replace('_box_office', '') }}_screen_num") AS total_screen_num,
            SUM("{{ table_name | replace('_box_office', '') }}_screen_show") AS total_screen_show
        FROM raw_data."{{ table_name }}"
        GROUP BY showRange
        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
)

SELECT
    showRange,
    SUM(total_sales) AS total_sales,
    SUM(total_total_sales) AS total_total_sales,
    SUM(total_audience_num) AS total_audience_num,
    SUM(total_total_audience_num) AS total_total_audience_num,
    SUM(total_screen_num) AS total_screen_num,
    SUM(total_screen_show) AS total_screen_show
FROM base_data
GROUP BY showRange
