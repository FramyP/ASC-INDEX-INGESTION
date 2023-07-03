---------------------CREATE TEMP TABLE TO LOAD UPDATED ASC WAGE INDEX-------
DROP TABLE IF EXISTS ASC_WAGE_INDEX;

CREATE
TEMP TABLE ASC_WAGE_INDEX
(
    "CBSA"                         VARCHAR(256),
    "WI"                           NUMERIC(10,4),
    "CBSA NAME"                    VARCHAR(256));

SELECT *
FROM CROSSWALKS.ASC_WAGE_INDEX;

---------TO INGEST UPDATED WAGE INDEX INTO LOADING TABLE-------
COPY ASC_WAGE_INDEX
    From 's3://prh-analytics/CMS Fee Schedules/xWALKS/ASC_WAGE_INDEX/2022 NFRM ASC Wage Index.11012021.csv'
    iam_role 'arn:aws:iam::969774041496:role/myRedshiftRole4S3'
    trimblanks
    maxerror 30
    Ignoreheader 1
    dateformat 'auto'
    region 'us-east-2'
    csv;

--------------TO INSERT FILE INTO FINAL TABLE AND ADD YEAR AND QUARTER FIELD------
--------------PLEASE CHANGE YEAR TO THE CORRESPONDING YEAR OF THE FILE BEFORE RUNNING CODE-----------
INSERT INTO CROSSWALKS.ASC_WAGE_INDEX
SELECT *,
       '2022' AS YEAR,--------MAKE SURE TO CHANGE YEAR TO CORRESPONDING WAGE INDEX FILE TO BE INSERTED------
    'ALL'  AS QUARTER
FROM ASC_WAGE_INDEX;

-----------------------------------------DO NOT RUN THIS PORTION OF THE SQL SCRIPT---------------------------------------------
-----------------------------------------FOR INFORMATIONAL PURPOSES ONLY---------------------------------------------
DROP TABLE IF EXISTS CROSSWALKS....;
CREATE TABLE CROSSWALKS.ASC_WAGE_INDEX
(
    "CBSA"      VARCHAR(256),
    "WI"        NUMERIC(10, 4),
    "CBSA NAME" VARCHAR(256),
    "YEAR"      VARCHAR(256),
    "QUARTER"   VARCHAR(256)
);


------------------------------------FOR VALIDATION---------------------------------------
SELECT *
FROM CROSSWALKS.ASC_WAGE_INDEX;
SELECT distinct year, count (cbsa)
FROM CROSSWALKS.ASC_WAGE_INDEX
group by 1;
