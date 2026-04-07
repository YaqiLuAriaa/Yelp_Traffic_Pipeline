USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE YELP_DB;
USE SCHEMA RAW;

SELECT
    COUNT(*) AS total_rows,
    COUNT_IF(BUSINESS_ID IS NULL) AS missing_business_id,
    COUNT_IF(NAME IS NULL) AS missing_name,
    COUNT_IF(ADDRESS IS NULL) AS missing_address,
    COUNT_IF(CITY IS NULL) AS missing_city,
    COUNT_IF(STATE IS NULL) AS missing_state,
    COUNT_IF(POSTAL_CODE IS NULL) AS missing_postal_code,
    COUNT_IF(CATEGORIES IS NULL) AS missing_categories
FROM YELP_BUSINESS;

-- 创建clean.business--
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE YELP_DB;

CREATE SCHEMA IF NOT EXISTS CLEAN;

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE YELP_DB;

CREATE SCHEMA IF NOT EXISTS CLEAN;

CREATE OR REPLACE TABLE YELP_DB.CLEAN.BUSINESS_CLEAN AS
SELECT
    BUSINESS_ID,
    BUSINESS_NAME,
    ADDRESS,
    CITY,
    STATE,
    POSTAL_CODE,
    LATITUDE,
    LONGITUDE,
    BUSINESS_STARS,
    REVIEW_COUNT,
    IS_OPEN,
    CATEGORIES,
    ATTRIBUTES,
    HOURS,
    PRIMARY_CATEGORY
FROM (
    SELECT
        BUSINESS_ID,
        NAME AS BUSINESS_NAME,
        ADDRESS,
        CITY,
        STATE,
        POSTAL_CODE,
        LATITUDE,
        LONGITUDE,
        STARS AS BUSINESS_STARS,
        REVIEW_COUNT,
        IS_OPEN,
        COALESCE(CATEGORIES, 'Unknown') AS CATEGORIES,
        ATTRIBUTES,
        HOURS,

        CASE
            WHEN CATEGORIES ILIKE '%Restaurant%'
              OR CATEGORIES ILIKE '%Food%'
              OR CATEGORIES ILIKE '%Coffee%'
              OR CATEGORIES ILIKE '%Bar%'
              OR CATEGORIES ILIKE '%Pizza%'
              THEN 'Food & Restaurants'

            WHEN CATEGORIES ILIKE '%Health%'
              OR CATEGORIES ILIKE '%Medical%'
              OR CATEGORIES ILIKE '%Doctor%'
              OR CATEGORIES ILIKE '%Dentist%'
              THEN 'Health & Medical'

            WHEN CATEGORIES ILIKE '%Automotive%'
              OR CATEGORIES ILIKE '%Gas Station%'
              OR CATEGORIES ILIKE '%Auto%'
              THEN 'Automotive'

            WHEN CATEGORIES ILIKE '%Beauty%'
              OR CATEGORIES ILIKE '%Hair%'
              OR CATEGORIES ILIKE '%Nail%'
              OR CATEGORIES ILIKE '%Spa%'
              THEN 'Beauty & Personal Care'

            WHEN CATEGORIES ILIKE '%Shopping%'
              OR CATEGORIES ILIKE '%Store%'
              THEN 'Retail & Shopping'

            WHEN CATEGORIES ILIKE '%Home Services%'
              OR CATEGORIES ILIKE '%Plumbing%'
              OR CATEGORIES ILIKE '%Contractor%'
              THEN 'Home Services'

            WHEN CATEGORIES ILIKE '%Fitness%'
              OR CATEGORIES ILIKE '%Gym%'
              OR CATEGORIES ILIKE '%Yoga%'
              THEN 'Fitness'

            WHEN CATEGORIES ILIKE '%Education%'
              OR CATEGORIES ILIKE '%School%'
              OR CATEGORIES ILIKE '%College%'
              THEN 'Education'

            WHEN CATEGORIES ILIKE '%Financial%'
              OR CATEGORIES ILIKE '%Bank%'
              OR CATEGORIES ILIKE '%Insurance%'
              THEN 'Financial Services'

            WHEN CATEGORIES ILIKE '%Hotel%'
              OR CATEGORIES ILIKE '%Travel%'
              OR CATEGORIES ILIKE '%Tour%'
              THEN 'Travel & Hospitality'

            WHEN CATEGORIES ILIKE '%Entertainment%'
              OR CATEGORIES ILIKE '%Museum%'
              OR CATEGORIES ILIKE '%Cinema%'
              THEN 'Entertainment'

            ELSE 'Other'
        END AS PRIMARY_CATEGORY,

        ROW_NUMBER() OVER (
            PARTITION BY BUSINESS_ID
            ORDER BY STARS DESC, REVIEW_COUNT DESC, NAME
        ) AS rn
    FROM YELP_DB.RAW.YELP_BUSINESS
    WHERE BUSINESS_ID IS NOT NULL
)
WHERE rn = 1;

-- BUSINESS_STARS, CATEGORIES如果是空的话替换成“Unknown"

--check business_clean
SELECT COUNT(*) FROM CLEAN.BUSINESS_CLEAN;
SELECT * FROM CLEAN.BUSINESS_CLEAN LIMIT 30;



--philda top 5 business-- 
CREATE OR REPLACE TABLE YELP_DB.CLEAN.PHILADELPHIA_TOP_BUSINESSES AS
SELECT
    BUSINESS_ID,
    BUSINESS_NAME,
    ADDRESS,
    CITY,
    STATE,
    POSTAL_CODE,
    LATITUDE,
    LONGITUDE,
    PRIMARY_CATEGORY,
    BUSINESS_STARS,
    REVIEW_COUNT,
    RANK_IN_CATEGORY
FROM (
    SELECT
        BUSINESS_ID,
        BUSINESS_NAME,
        ADDRESS,
        CITY,
        STATE,
        POSTAL_CODE,
        LATITUDE,
        LONGITUDE,
        PRIMARY_CATEGORY,
        BUSINESS_STARS,
        REVIEW_COUNT,
        ROW_NUMBER() OVER (
            PARTITION BY PRIMARY_CATEGORY
            ORDER BY BUSINESS_STARS DESC, REVIEW_COUNT DESC, BUSINESS_NAME
        ) AS RANK_IN_CATEGORY
    FROM YELP_DB.CLEAN.BUSINESS_CLEAN
    WHERE CITY ILIKE '%philadel%'
      AND STATE = 'PA'
      AND PRIMARY_CATEGORY IN (
          'Food & Restaurants',
          'Beauty & Personal Care',
          'Health & Medical',
          'Retail & Shopping',
          'Home Services'
      )
)
WHERE RANK_IN_CATEGORY <= 5;
