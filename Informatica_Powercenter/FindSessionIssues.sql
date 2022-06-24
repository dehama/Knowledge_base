-- Get sessions that run multiple times in last load, or have rejects, or have failure
SELECT 
       RSL.SUBJECT_AREA AS FOLDER, 
       RW.WORKFLOW_NAME AS WORKFLOW,
       RSL.WORKFLOW_RUN_ID,
       RSL.SESSION_NAME AS SESSION_NAME, 
       to_char(RSL.ACTUAL_START,'YYYYMMDD'),
       count(1),
       max((RSL.SESSION_TIMESTAMP - RSL.ACTUAL_START)*24*60),
       sum(FAILED_ROWS),
       max(FIRST_ERROR_CODE)
FROM 
       REP_SESS_LOG RSL
    inner join
       REP_WORKFLOWS RW
    on RW.WORKFLOW_ID = RSL.WORKFLOW_ID 
       AND RW.SUBJECT_ID = RSL.SUBJECT_ID
WHERE 1=1
       --AND RSL.RUN_STATUS_CODE IN (3,4,5,14,15) 
       AND RSL.SUBJECT_AREA in ('PRD_EDW','030 PRD_DWH') --folder
       and RSL.SESSION_NAME not like 's_m_LOAD_EDW_K_JNR%' --remove specific sessions from list
       and RSL.ACTUAL_START > current_date-10
group by
        RSL.SUBJECT_AREA,
        RW.WORKFLOW_NAME,
        RSL.WORKFLOW_RUN_ID,
        RSL.SESSION_NAME,
        to_char(RSL.ACTUAL_START,'YYYYMMDD')
having
    count(1) > 1
    OR sum(FAILED_ROWS) > 1
    OR max(FIRST_ERROR_CODE) > 0
order by to_char(RSL.ACTUAL_START,'YYYYMMDD') desc, 7 desc;
