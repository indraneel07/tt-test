SELECT
    STEPID,
    MODULEID,
    PARAMETERID,
    PARAMETERNAME,
    PARAMETERVALUE,
    TARGETID,
    STEPORDER,
    JOBID,
    FREQUENCYID
FROM {{ params.DB }}.{{ params.JOBS_SCHEMA }}.{{ params.JOBS_TABLE }}
WHERE 1=1
  AND TARGETID = '{{ params.TARGET_ID }}'
  AND STEPORDER = {{ params.STEPORDER }}
  AND FREQUENCYID = '{{ params.FREQUENCY_ID }}'
  AND JOBID = {{ ti.xcom_pull(task_ids='extract_metadata')['job_id'] }}
ORDER BY STEPID, PARAMETERID
