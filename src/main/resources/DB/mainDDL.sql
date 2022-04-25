CREATE SCHEMA modelResults;

CREATE
USER flaskServer WITH PASSWORD 'psqlpass';
GRANT ALL PRIVILEGES ON SCHEMA
msnDB.modelresults TO flaskserver;

CREATE
USER model WITH PASSWORD 'psqlpass';
GRANT ALL PRIVILEGES ON DATABASE
msnDB TO model;

CREATE TABLE modelResults.attractiveParams
(
    query             BIGINT,
    "resultId"        BIGINT,
    rank              INTEGER,
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