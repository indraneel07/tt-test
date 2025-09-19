SELECT '{{ params.file_path }}' AS file_path,
       dag_id, target_table, module_name
FROM {{ params.db }}.AIRFLOW.DATA_LOADING_MANIFEST
WHERE is_active
  AND s3_uri = '{{ params.s3_uri }}'
LIMIT 1;
