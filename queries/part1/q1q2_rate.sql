#Compute the change for all metrics, in Jan-2017
WITH metric AS (SELECT *,
                IFNULL(LAG(RATE) OVER (PARTITION BY CONTEXTID, METRIC ORDER BY REPORTDATE ASC), NULL) AS PREV_RATE,
                FROM
                    (SELECT *, ROUND((NUMERATOR / DENOMINATOR), 4) RATE
                    FROM `athena-interview.athena.metrics`
                    )
                )
SELECT *,
CASE WHEN metric.PREV_RATE = 0 THEN 0
    WHEN metric.PREV_RATE IS NULL THEN NULL
    ELSE ROUND(((metric.RATE-metric.PREV_RATE)/metric.PREV_RATE)*100, 2)
    END D_RATE
FROM metric
WHERE EXTRACT(MONTH FROM REPORTDATE) = 01 #Param
AND EXTRACT (YEAR FROM REPORTDATE) = 12 #Param
# AND METRIC = "DAR" #Param