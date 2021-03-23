#Compute the N_ERRORS, and INFLOW grouped by month,contextid,error 
#Focused on kickcode errors only

WITH inflow AS (SELECT MONTH, CONTEXTID, ERROR,
    SUM(INFLOWCNT) N_ERRORS,
    (SELECT MAX(ATHENA_MONTH_CLMCNT)
    FROM `athena-interview.athena.inflow` i2
    WHERE i1.CONTEXTID = i2.CONTEXTID
    AND i1.MONTH = i2.MONTH) ATHENA_MONTH_CLMCNT,
    ROUND((SUM(INFLOWCNT) / (SELECT MAX(ATHENA_MONTH_CLMCNT)
                        FROM `athena-interview.athena.inflow` i2
                        WHERE i1.CONTEXTID = i2.CONTEXTID
                        AND i1.MONTH = i2.MONTH
                    )*100), 6) INFLOW
    FROM `athena-interview.athena.inflow` i1
    WHERE ERRORTYPE = "ATHENA - KICK" #Kickcodes only
    GROUP BY MONTH, CONTEXTID, ERROR
    ),

#Context/Error-level: Computes PREV_INFLOW for each contextid,error ordered by month
#Athena-level: Computes ATHENA_MONTH_INFLOW for each month
prev_inflow AS (SELECT *,
    IFNULL(LAG(inflow.INFLOW) OVER (PARTITION BY inflow.CONTEXTID, inflow.ERROR ORDER BY inflow.MONTH ASC), NULL) AS PREV_INFLOW,
    ROUND((AVG(inflow.INFLOW) OVER (PARTITION BY MONTH)), 6) ATHENA_MONTH_INFLOW
    FROM inflow),

#Context/Error-level: Computes D_INFLOW for each month,contextid,errror
#Athena-level: Computes ATHENA_MONTH_INFLOW_PREV ordered by month
d_inflow AS (SELECT *,
    ROUND(((prev_inflow.INFLOW-prev_inflow.PREV_INFLOW)/prev_inflow.PREV_INFLOW)*100, 6) D_INFLOW,
    IFNULL(LAG(ATHENA_MONTH_INFLOW) OVER (PARTITION BY CONTEXTID, ERROR ORDER BY MONTH ASC), NULL) AS ATHENA_MONTH_INFLOW_PREV
    FROM prev_inflow
    )

#Return all previous results and also D_ATHENA_INFLOW
SELECT *,
ROUND(((d_inflow.ATHENA_MONTH_INFLOW-d_inflow.ATHENA_MONTH_INFLOW_PREV)/d_inflow.ATHENA_MONTH_INFLOW_PREV)*100, 4) D_ATHENA_INFLOW
FROM d_inflow
WHERE EXTRACT(MONTH FROM d_inflow.MONTH) = 01
AND d_inflow.D_INFLOW > 100
ORDER BY N_ERRORS DESC