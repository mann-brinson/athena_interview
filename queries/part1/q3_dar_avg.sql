#Focus on DAR from metrics table
WITH dar AS (SELECT *, ROUND((NUMERATOR / DENOMINATOR), 2) as `DAR`,
        FROM `athena-interview.athena.metrics`
        WHERE METRIC = "DAR"
        AND CONTEXTID != 5580 #This context is a DAR outlier, must address separately
        ),

#Compute PREV_DAR from Oct_2016 to Jan_2017 (3 lag units)
prev_dar AS (SELECT *,
    IFNULL(LAG(dar.DAR, 3) OVER (PARTITION BY CONTEXTID ORDER BY REPORTDATE ASC), NULL) AS PREV_DAR
    FROM dar
    ),

#Compute the D_DAR as the change in DAR from Oct_2016 to Jan_2017
d_dar AS (SELECT *,
    ROUND(((prev_dar.DAR-prev_dar.PREV_DAR)/prev_dar.PREV_DAR)*100, 4) D_DAR
    FROM prev_dar
    WHERE prev_dar.PREV_DAR IS NOT NULL
    )

#Compute the average context change in DAR (D_DAR_AVG) against each individual context's D_DAR
SELECT *, 
SUM(d_dar.D_DAR) OVER () D_DAR_SUM, 
COUNT(CONTEXTID) OVER () N_CONTEXTS,
ROUND((SUM(d_dar.D_DAR) OVER () / COUNT(CONTEXTID) OVER ()),4) D_DAR_AVG
FROM d_dar
ORDER BY D_DAR ASC