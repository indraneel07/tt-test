CALL {{ params.DB }}.UTILS.DATA_LOAD(
    S3_URI => '{{ params.FILE_PATH }}',
    TARGET_TABLE => '{{ params.SCHEMA }}.{{ params.TARGET_TABLE }}',
    MODULE_NAME => '{{ params.MODULE_NAME }}'
);
