CREATE SCHEMA modelResults;

CREATE
USER flaskServer WITH PASSWORD 'psqlpass';
GRANT ALL PRIVILEGES ON msnDB TO flaskserver
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA modelresults TO flaskserver;

CREATE
USER model WITH PASSWORD 'psqlpass';
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA modelresults TO model;

CREATE TABLE modelResults.attractiveParams
(
    query             BIGINT,
    "resultId"        BIGINT,
    "attrNumerator"   DOUBLE PRECISION,
    "attrDenominator" BIGINT,
    PRIMARY KEY (query, "resultId")
);

CREATE TABLE examinationParams
(
    rank              INTEGER PRIMARY KEY,
    "examNumerator"   DOUBLE PRECISION,
    "examDenominator" BIGINT
);