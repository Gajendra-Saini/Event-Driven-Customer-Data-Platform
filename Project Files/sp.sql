CALL SALES_DB.RAW_SCHEMA.PROCESS_CUSTOMER_STREAM();

SELECT COUNT(*) 
FROM PROCESSED_SCHEMA.CUSTOMER_CLEAN;

select * from PROCESSED_SCHEMA.CUSTOMER_CLEAN;


CREATE OR REPLACE PROCEDURE SALES_DB.RAW_SCHEMA.PROCESS_CUSTOMER_STREAM()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var start_time = new Date().toISOString();
var rows_processed = 0;

try {

    // --- MERGE ---
    var merge_sql = `
        MERGE INTO SALES_DB.PROCESSED_SCHEMA.CUSTOMER_CLEAN T
        USING (
            SELECT *
            FROM SALES_DB.RAW_SCHEMA.CUSTOMER_STREAM
        ) S
        ON T.USER_ID = TRY_TO_NUMBER(S.USERID)

        WHEN MATCHED THEN UPDATE SET
            EMAIL      = TRIM(S.USERNAME_),
            NAME       = TRIM(S.NAMESURNAME),
            STATUS     = TRY_TO_NUMBER(S.STATUS_),
            GENDER     = TRIM(S.USERGENDER),
            BIRTH_DATE = TRY_TO_DATE(S.USERBIRTHDATE),
            REGION     = TRIM(S.REGION),
            CITY       = TRIM(S.CITY),
            TOWN       = TRIM(S.TOWN),
            DISTRICT   = TRIM(S.DISTRICT),
            ADDRESS    = TRIM(S.ADDRESSTEXT)

        WHEN NOT MATCHED THEN INSERT (
            USER_ID, EMAIL, NAME, STATUS, GENDER,
            BIRTH_DATE, REGION, CITY, TOWN, DISTRICT, ADDRESS
        )
        VALUES (
            TRY_TO_NUMBER(S.USERID),
            TRIM(S.USERNAME_),
            TRIM(S.NAMESURNAME),
            TRY_TO_NUMBER(S.STATUS_),
            TRIM(S.USERGENDER),
            TRY_TO_DATE(S.USERBIRTHDATE),
            TRIM(S.REGION),
            TRIM(S.CITY),
            TRIM(S.TOWN),
            TRIM(S.DISTRICT),
            TRIM(S.ADDRESSTEXT)
        );
    `;

    var stmt = snowflake.createStatement({ sqlText: merge_sql });
    stmt.execute();

    // --- Get MERGE stats safely ---
    var stats_stmt = snowflake.createStatement({
        sqlText: `SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))`
    });

    var rs = stats_stmt.execute();

    if (rs.next()) {
        var columnCount = rs.getColumnCount();

        for (var i = 1; i <= columnCount; i++) {
            var val = rs.getColumnValue(i);
            if (val) {
                rows_processed += val;
            }
        }
    }

    var end_time = new Date().toISOString();

    // --- SUCCESS LOG ---
    snowflake.createStatement({
        sqlText: `
            INSERT INTO SALES_DB.LOG_SCHEMA.PIPELINE_LOG
            (PIPELINE_NAME, START_TIME, END_TIME, ROWS_PROCESSED, STATUS, ERROR_MESSAGE)
            VALUES ('CUSTOMER_PIPELINE', ?, ?, ?, 'SUCCESS', NULL)
        `,
        binds: [start_time, end_time, rows_processed]
    }).execute();

    return "SUCCESS - Rows Processed: " + rows_processed;

} catch (err) {

    var end_time = new Date().toISOString();

    snowflake.createStatement({
        sqlText: `
            INSERT INTO SALES_DB.LOG_SCHEMA.PIPELINE_LOG
            (PIPELINE_NAME, START_TIME, END_TIME, ROWS_PROCESSED, STATUS, ERROR_MESSAGE)
            VALUES ('CUSTOMER_PIPELINE', ?, ?, 0, 'FAILED', ?)
        `,
        binds: [start_time, end_time, err.message]
    }).execute();

    return "FAILED - " + err.message;
}
$$;