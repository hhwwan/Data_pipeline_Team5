WITH base_data AS (
    {% for table_name in var('table_names', []) %}
        -- 로그 추가: 현재 table_name 출력
        {% do log("Processing table_name: " ~ table_name, info=True) %}
        
        SELECT
            title,
            code,
            -- table_name에서 'box_office'를 제외한 날짜 부분만 추출하여 컬럼 이름 생성
            "{{ table_name | replace('_box_office', '') }}_sales" AS sales,
            "{{ table_name | replace('_box_office', '') }}_total_sales" AS total_sales,
            "{{ table_name | replace('_box_office', '') }}_audience_num" AS audience_num,
            "{{ table_name | replace('_box_office', '') }}_total_audience_num" AS total_audience_num,
            '{{ table_name }}' AS source_table
        FROM raw_data."{{ table_name }}"
    {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %} 
)

SELECT
    title,
    code,
    {% for table_name in var('table_names', []) %}
        MAX(CASE WHEN source_table = '{{ table_name }}' THEN sales END) AS "{{ table_name | replace('_box_office', '') }}_sales",
        MAX(CASE WHEN source_table = '{{ table_name }}' THEN total_sales END) AS "{{ table_name | replace('_box_office', '') }}_total_sales",
        MAX(CASE WHEN source_table = '{{ table_name }}' THEN audience_num END) AS "{{ table_name | replace('_box_office', '') }}_audience_num",
        MAX(CASE WHEN source_table = '{{ table_name }}' THEN total_audience_num END) AS "{{ table_name | replace('_box_office', '') }}_total_audience_num"
        {% if not loop.last %} , {% endif %}
    {% endfor %}
FROM base_data
GROUP BY title, code
ORDER BY title, code
