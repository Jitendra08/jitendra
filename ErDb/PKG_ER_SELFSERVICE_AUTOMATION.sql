CREATE OR REPLACE PACKAGE            PKG_ER_SELFSERVICE_AUTOMATION AS

PROCEDURE SPT_INSERT_ER_ACCRUALS(
    p_ER_ACCRUAL_DATA_IN IN REVENUE_DM.ER_ACCRUAL_IN,
    p_err_code_out OUT NUMBER,
    p_err_mesg_out OUT VARCHAR2 );


    type cursorType is ref cursor;

FUNCTION VALIDATE_ACCRUALS (accr_id varchar2) RETURN NUMBER;
END;
/


CREATE OR REPLACE PACKAGE BODY                         "PKG_ER_SELFSERVICE_AUTOMATION"
AS
  l_grail_ref_type   VARCHAR2(5) :='ACCCD';
  l_twins_ref_type   VARCHAR2(5) :='TACCD';
  l_reinmex_ref_type VARCHAR2(5) :='RACCD';
  l_as400_ref_type   VARCHAR2(5) :='FACCD';
  --l_as400_tty_gc_ref_type varchar2(5) :='AACCD';
  l_collins_gc_ref_type VARCHAR2(5) :='CACCD';
  l_load_num            NUMBER      := 0;
  l_timestamp           TIMESTAMP;
  l_msg                 VARCHAR2(115);
  l_cnt                 NUMBER;
PROCEDURE SPT_INSERT_ER_ACCRUALS(
    p_ER_ACCRUAL_DATA_IN IN REVENUE_DM.ER_ACCRUAL_IN,
    p_err_code_out OUT NUMBER,
    p_err_mesg_out OUT VARCHAR2 )
IS
  EX_COLLECTION_EMPTY EXCEPTION;
BEGIN
  IF p_ER_ACCRUAL_DATA_IN.EXISTS (1) THEN
    FOR i IN p_ER_ACCRUAL_DATA_IN.FIRST .. p_ER_ACCRUAL_DATA_IN.LAST
    LOOP
      INSERT
      INTO REVENUE_DM.ER_SELFSERVICE_ACCRUAL_RECORDS
        (
          ACCR_GL_ENTITY_CODE ,
          ACCR_GL_LOCATION_CODE ,
          ACCR_GL_DEPT_CODE ,
          ACCR_GL_PRODUCT_CODE ,
          ACCR_GL_BUSINESS_SEG_CODE ,
          ACCR_GL_PERIOD_MON_YEAR ,
          ACCR_ORG_CCY_CODE ,
          ACCR_CLIENT_NAME ,
          ACCR_AMT ,
          ACCR_ACCT_CODE ,
          ACCR_REVENUE_CODE ,
          ACCR_APP_ID ,
          ACCR_UNDERWRITING_YR ,
          C14 ,
          ACCR_ACTUAL_RESTATE ,
          CONTRACT_REF ,
          BUSINESS_CLASS_DSC ,
          CONTRACT_CLASS_DSC,
          FILE_ID,
          FK_ACCR_FILE_ID,
          USER_ID,
          CREATED_DT,
          STATUS,
          PK_ACCR_RECS_ID,
          R12_CODE
        )
        VALUES
        (
          p_ER_ACCRUAL_DATA_IN(i).ACCR_GL_ENTITY_CODE ,
          p_ER_ACCRUAL_DATA_IN(i).ACCR_GL_LOCATION_CODE ,
          p_ER_ACCRUAL_DATA_IN(i).ACCR_GL_DEPT_CODE ,
          p_ER_ACCRUAL_DATA_IN(i).ACCR_GL_PRODUCT_CODE ,
          p_ER_ACCRUAL_DATA_IN(i).ACCR_GL_BUSINESS_SEG_CODE ,
          p_ER_ACCRUAL_DATA_IN(i).ACCR_GL_PERIOD_MON_YEAR ,
          p_ER_ACCRUAL_DATA_IN(i).ACCR_ORG_CCY_CODE ,
          p_ER_ACCRUAL_DATA_IN(i).ACCR_CLIENT_NAME ,
          p_ER_ACCRUAL_DATA_IN(i).ACCR_AMT ,
          p_ER_ACCRUAL_DATA_IN(i).ACCR_ACCT_CODE ,
          p_ER_ACCRUAL_DATA_IN(i).ACCR_REVENUE_CODE ,
          p_ER_ACCRUAL_DATA_IN(i).ACCR_APP_ID ,
          p_ER_ACCRUAL_DATA_IN(i).ACCR_UNDERWRITING_YR ,
          p_ER_ACCRUAL_DATA_IN(i).C14 ,
          p_ER_ACCRUAL_DATA_IN(i).ACCR_ACTUAL_RESTATE ,
          p_ER_ACCRUAL_DATA_IN(i).CONTRACT_REF ,
          p_ER_ACCRUAL_DATA_IN(i).BUSINESS_CLASS_DSC ,
          p_ER_ACCRUAL_DATA_IN(i).CONTRACT_CLASS_DSC,
          p_ER_ACCRUAL_DATA_IN(i).FILE_ID,
          p_ER_ACCRUAL_DATA_IN(i).FK_ACCR_FILE_ID,
          p_ER_ACCRUAL_DATA_IN(i).USER_ID,
          SYSDATE,
          'Pending',
          ER_UTILITY_ACCR_SEQ.NEXTVAL,
          p_ER_ACCRUAL_DATA_IN(i).R12_CODE
        );
    END LOOP;
    COMMIT;
  ELSE
    RAISE EX_COLLECTION_EMPTY;
  END IF;
EXCEPTION
WHEN EX_COLLECTION_EMPTY THEN
  p_err_mesg_out := 'Error in REVENUE_DM.PKG_ER_SELFSERVICE_AUTOMATION.SPT_INSERT_ER_ACCRUALS => ' || 'COLLECTION IS EMPTY';
  RAISE_APPLICATION_ERROR (-20001, p_err_mesg_out, TRUE);
WHEN OTHERS THEN
  ROLLBACK;
  p_err_mesg_out := 'Error in REVENUE_DM.PKG_ER_SELFSERVICE_AUTOMATION.SPT_INSERT_ER_ACCRUALS => ' || 'WHILE INSERTING RECORDS INTO ER_SELFSERVICE_ACCRUAL_STG TABLE';
  RAISE_APPLICATION_ERROR (-20002, p_err_mesg_out, TRUE);
END SPT_INSERT_ER_ACCRUALS;
FUNCTION VALIDATE_ACCRUALS
  (
    accr_id VARCHAR2
  )
  RETURN NUMBER
IS
  l_process_count NUMBER := 0;
  l_file_handle utl_file.file_type;
  l_file_name   VARCHAR2(26) :='G_C_CORPORATE_ACCRUALS.csv';
  l_file_dir    VARCHAR2(50) ;
  l_file_exist  BOOLEAN;
  l_file_length NUMBER;
  l_block_size binary_integer;
  l_ext_handle utl_file.file_type;
  l_strBuffer VARCHAR2(200);
  l_file_cnt  NUMBER :=0;
  l_cnt       NUMBER :=0;
BEGIN
  dbms_output.enable( 1000000);
  --delete from REV_DM_STAGING.er_accural_log where dt_updated < sysdate - l_days;
  --commit;
  SELECT REV_DM_STAGING.er_accrual_load_seq.nextval
  INTO l_load_num
  FROM dual;
  -- select directory_path into l_file_dir from all_directories where directory_name ='FLATFILES_DIR01';
  -- need to verify the directory_path is part of utl_file_dir
  --SELECT directory_path into l_ext_tbl_dir FROM dba_directories where DIRECTORY_NAME ='TBL_DIR';
  ------------------
  -- validation start
  BEGIN
    INSERT INTO REV_DM_STAGING.ER_ACCRUAL_STG_ERROR
    SELECT a.ACCR_GL_ENTITY_CODE,
      a.ACCR_GL_LOCATION_CODE,
      a.ACCR_GL_DEPT_CODE,
      a.ACCR_GL_PRODUCT_CODE,
      a.ACCR_GL_BUSINESS_SEG_CODE,
      a.ACCR_GL_PERIOD_MON_YEAR,
      a.ACCR_ORG_CCY_CODE,
      a.ACCR_CLIENT_NAME,
      a.ACCR_AMT,
      a.ACCR_ACCT_CODE,
      a.ACCR_REVENUE_CODE,
      a.ACCR_APP_ID,
      a.ACCR_UNDERWRITING_YR,
      a.C14,
      a.ACCR_ACTUAL_RESTATE,
      'Profit Center or R12 code '
      ||nvl(rtrim(ACCR_GL_ENTITY_CODE)
      ||rtrim(ACCR_GL_LOCATION_CODE)
      || rtrim(ACCR_GL_DEPT_CODE)
      ||rtrim(ACCR_GL_PRODUCT_CODE)
      ||rtrim(ACCR_GL_BUSINESS_SEG_CODE),R12_CODE)
      || ' does not exist',
      sysdate,
      l_load_num,
      a.CONTRACT_REF,
      a.BUSINESS_CLASS_DSC,
      a.CONTRACT_CLASS_DSC,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      R12_CODE
    FROM REVENUE_DM.ER_SELFSERVICE_ACCRUAL_RECORDS a
    WHERE NOT EXISTS
      (SELECT 1
      FROM ercross.ER_GL_PROFIT_CENTER b
      WHERE NVL(a.R12_CODE,
           rtrim(REPLACE(a.ACCR_GL_ENTITY_CODE,''''))
        || rtrim(REPLACE(a.ACCR_GL_LOCATION_CODE,''''))
        || rtrim(REPLACE(a.ACCR_GL_DEPT_CODE,''''))
        || rtrim(REPLACE(a.ACCR_GL_PRODUCT_CODE,''''))
        || rtrim(REPLACE(a.ACCR_GL_BUSINESS_SEG_CODE,''''))) = nvl2(trim(a.R12_CODE),(select trim(HYPERION_RESPONSIBILITY_CENTER) from revenue_dm.profit_center_dim where HYPERION_RESPONSIBILITY_CENTER =a.R12_CODE ),b.SHORT_CODE)
      )
    AND a.FK_ACCR_FILE_ID = accr_id;
    l_cnt                := sql%rowcount;

    COMMIT;

    IF l_cnt              > 0 THEN
      grail_dm_part.PK_EMAIL.proc_email_notify('N', 'Short_Code not exist, check ER_ACCRUAL_STG_ERROR for load number :'||l_load_num, 7 );
    END IF;
    l_cnt := 0;
  EXCEPTION
  WHEN OTHERS THEN
    l_msg := SUBSTR(sqlerrm( SQLCODE ),1,115);
    INSERT
    INTO REV_DM_STAGING.er_accural_log
      (
        er_accrual_load_num,
        dt_updated,
        status,
        comments
      )
      VALUES
      (
        l_load_num,
        sysdate,
        'Error on validate',
        'ER_GL_PROFIT_CENTER validate; '
        ||l_msg
      );
    COMMIT;
    grail_dm_part.PK_EMAIL.proc_email_notify('N', 'Error Inserting ER_ACCRUAL_STG_ERROR (ProfCtr)', 7 );
    raise_application_error( -20002, sqlerrm( SQLCODE ) || ' AccrualLoad, chk ER_ACCURAL_LOG ' );
  END;
  BEGIN
    INSERT INTO REV_DM_STAGING.ER_ACCRUAL_STG_ERROR
    SELECT a.ACCR_GL_ENTITY_CODE,
      a.ACCR_GL_LOCATION_CODE,
      a.ACCR_GL_DEPT_CODE,
      a.ACCR_GL_PRODUCT_CODE,
      a.ACCR_GL_BUSINESS_SEG_CODE,
      a.ACCR_GL_PERIOD_MON_YEAR,
      a.ACCR_ORG_CCY_CODE,
      a.ACCR_CLIENT_NAME,
      a.ACCR_AMT,
      a.ACCR_ACCT_CODE,
      a.ACCR_REVENUE_CODE,
      a.ACCR_APP_ID,
      a.ACCR_UNDERWRITING_YR,
      a.C14,
      a.ACCR_ACTUAL_RESTATE,
      'Client Account Code'
      ||ACCR_ACCT_CODE
      ||' does not exists',
      sysdate,
      l_load_num,
      a.CONTRACT_REF,
      a.BUSINESS_CLASS_DSC,
      a.CONTRACT_CLASS_DSC,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      R12_CODE
    FROM REVENUE_DM.ER_SELFSERVICE_ACCRUAL_RECORDS a
    WHERE a.ACCR_APP_ID = 'G'
    AND NOT EXISTS
      (SELECT 1
      FROM ereposit.ER_TP_REFERENCES b
      WHERE ltrim(rtrim(REPLACE(a.ACCR_ACCT_CODE,''''))) = b.tp_reference
      AND b.GC_REFERENCE_TYPE                            = l_grail_ref_type
      )
    AND a.FK_ACCR_FILE_ID = accr_id;
    l_cnt                := sql%rowcount;

    COMMIT;

    IF l_cnt              > 0 THEN
      grail_dm_part.PK_EMAIL.proc_email_notify('N', 'CLIENT_CODE not exist for Grail, load: '||l_load_num, 7 );
    END IF;
    l_cnt := 0;
  EXCEPTION
  WHEN OTHERS THEN
    l_msg := SUBSTR(sqlerrm( SQLCODE ),1,115);
    INSERT
    INTO REV_DM_STAGING.er_accural_log
      (
        er_accrual_load_num,
        dt_updated,
        status,
        comments
      )
      VALUES
      (
        l_load_num,
        sysdate,
        'Error on validate',
        'ER_TP_REFERENCES for GRAIL validate; '
        ||l_msg
      );
    COMMIT;
    grail_dm_part.PK_EMAIL.proc_email_notify('N', 'Error Inserting ER_ACCRUAL_STG_ERROR (G TpRef)', 7 );
    raise_application_error( -20003, sqlerrm( SQLCODE ) || ' AccrualLoad, chk ER_ACCURAL_LOG ' );
  END;
  BEGIN
    INSERT INTO REV_DM_STAGING.ER_ACCRUAL_STG_ERROR
    SELECT a.ACCR_GL_ENTITY_CODE,
      a.ACCR_GL_LOCATION_CODE,
      a.ACCR_GL_DEPT_CODE,
      a.ACCR_GL_PRODUCT_CODE,
      a.ACCR_GL_BUSINESS_SEG_CODE,
      a.ACCR_GL_PERIOD_MON_YEAR,
      a.ACCR_ORG_CCY_CODE,
      a.ACCR_CLIENT_NAME,
      a.ACCR_AMT,
      a.ACCR_ACCT_CODE,
      a.ACCR_REVENUE_CODE,
      a.ACCR_APP_ID,
      a.ACCR_UNDERWRITING_YR,
      a.C14,
      a.ACCR_ACTUAL_RESTATE,
      'Client Account Code '
      ||ACCR_ACCT_CODE
      ||' does not exists',
      sysdate,
      l_load_num,
      a.CONTRACT_REF,
      a.BUSINESS_CLASS_DSC,
      a.CONTRACT_CLASS_DSC,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      R12_CODE
    FROM REVENUE_DM.ER_SELFSERVICE_ACCRUAL_RECORDS a
    WHERE a.ACCR_APP_ID = 'T'
    AND NOT EXISTS
      (SELECT 1
      FROM ereposit.er_tp_references b
      WHERE ltrim(rtrim(REPLACE(a.ACCR_ACCT_CODE,''''))) = b.tp_reference
      AND b.GC_REFERENCE_TYPE                            = l_twins_ref_type
      )
    AND a.FK_ACCR_FILE_ID = accr_id;
    l_cnt                := sql%rowcount;

    COMMIT;

    IF l_cnt              > 0 THEN
      grail_dm_part.PK_EMAIL.proc_email_notify('N', 'CLIENT_CODE not exist for Twins, load: '||l_load_num, 7 );
    END IF;
    l_cnt := 0;
  EXCEPTION
  WHEN OTHERS THEN
    l_msg := SUBSTR(sqlerrm( SQLCODE ),1,115);
    INSERT
    INTO REV_DM_STAGING.er_accural_log
      (
        er_accrual_load_num,
        dt_updated,
        status,
        comments
      )
      VALUES
      (
        l_load_num,
        sysdate,
        'Error on validate',
        'ER_TP_REFERENCES for TWINS validate; '
        ||l_msg
      );
    COMMIT;
    grail_dm_part.PK_EMAIL.proc_email_notify('N', 'Error Inserting ER_ACCRUAL_STG_ERROR (T TpRef)', 7 );
    raise_application_error( -20004, sqlerrm( SQLCODE ) || ' AccrualLoad, chk ER_ACCURAL_LOG ' );
  END;
  BEGIN
    INSERT INTO REV_DM_STAGING.ER_ACCRUAL_STG_ERROR
    SELECT a.ACCR_GL_ENTITY_CODE,
      a.ACCR_GL_LOCATION_CODE,
      a.ACCR_GL_DEPT_CODE,
      a.ACCR_GL_PRODUCT_CODE,
      a.ACCR_GL_BUSINESS_SEG_CODE,
      a.ACCR_GL_PERIOD_MON_YEAR,
      a.ACCR_ORG_CCY_CODE,
      a.ACCR_CLIENT_NAME,
      a.ACCR_AMT,
      a.ACCR_ACCT_CODE,
      a.ACCR_REVENUE_CODE,
      a.ACCR_APP_ID,
      a.ACCR_UNDERWRITING_YR,
      a.C14,
      a.ACCR_ACTUAL_RESTATE,
      'Client Account Code '
      ||ACCR_ACCT_CODE
      ||' does not exists',
      sysdate,
      l_load_num,
      a.CONTRACT_REF,
      a.BUSINESS_CLASS_DSC,
      a.CONTRACT_CLASS_DSC,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      R12_CODE
    FROM REVENUE_DM.ER_SELFSERVICE_ACCRUAL_RECORDS a
    WHERE a.ACCR_APP_ID = 'R'
    AND NOT EXISTS
      (SELECT 1
      FROM ereposit.er_tp_references b
      WHERE ltrim(rtrim(REPLACE(a.ACCR_ACCT_CODE,''''))) = b.tp_reference
      AND b.GC_REFERENCE_TYPE                            = l_reinmex_ref_type
      )
    AND ltrim(rtrim(REPLACE(a.ACCR_ACCT_CODE,''''))) <> '1'
    AND a.FK_ACCR_FILE_ID                             = accr_id;
    l_cnt                                            := sql%rowcount;

    COMMIT;

    IF l_cnt                                          > 0 THEN
      grail_dm_part.PK_EMAIL.proc_email_notify('N', 'CLIENT_CODE not exist for Reinmex, load: '||l_load_num, 7 );
    END IF;
    l_cnt := 0;
  EXCEPTION
  WHEN OTHERS THEN
    l_msg := SUBSTR(sqlerrm( SQLCODE ),1,115);
    INSERT
    INTO REV_DM_STAGING.er_accural_log
      (
        er_accrual_load_num,
        dt_updated,
        status,
        comments
      )
      VALUES
      (
        l_load_num,
        sysdate,
        'Error on validate',
        'ER_TP_REFERENCES for REINMEX validate; '
        ||l_msg
      );
    COMMIT;
    grail_dm_part.PK_EMAIL.proc_email_notify('N', 'Error Inserting ER_ACCRUAL_STG_ERROR (R TpRef)', 7 );
    raise_application_error( -20005, sqlerrm( SQLCODE ) || ' AccrualLoad, chk ER_ACCURAL_LOG ' );
  END;
  -- -- AMT validation
  BEGIN
    INSERT INTO REV_DM_STAGING.ER_ACCRUAL_STG_ERROR
    SELECT a.ACCR_GL_ENTITY_CODE,
      a.ACCR_GL_LOCATION_CODE,
      a.ACCR_GL_DEPT_CODE,
      a.ACCR_GL_PRODUCT_CODE,
      a.ACCR_GL_BUSINESS_SEG_CODE,
      a.ACCR_GL_PERIOD_MON_YEAR,
      a.ACCR_ORG_CCY_CODE,
      a.ACCR_CLIENT_NAME,
      a.ACCR_AMT,
      a.ACCR_ACCT_CODE,
      a.ACCR_REVENUE_CODE,
      a.ACCR_APP_ID,
      a.ACCR_UNDERWRITING_YR,
      a.C14,
      a.ACCR_ACTUAL_RESTATE,
      'Amount '
      ||ACCR_AMT
      ||' not Numeric',
      sysdate,
      l_load_num,
      a.CONTRACT_REF,
      a.BUSINESS_CLASS_DSC,
      a.CONTRACT_CLASS_DSC,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      R12_CODE
    FROM REVENUE_DM.ER_SELFSERVICE_ACCRUAL_RECORDS a
    WHERE (upper(SUBSTR(ACCR_AMT,1,1)) IN ('A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U', 'V','W','X','Y','Z','=','*','$','#','@','!',' ','_')
    OR ACCR_AMT                       IS NULL
    OR REGEXP_COUNT(ACCR_AMT, '\.') > 1
    )
    AND a.FK_ACCR_FILE_ID              = accr_id;
    l_cnt                             := sql%rowcount;

   COMMIT;

    IF l_cnt                           > 0 THEN
      grail_dm_part.PK_EMAIL.proc_email_notify('N', 'ACCR_AMT not numeric, load: '||l_load_num, 7 );
      --raise_application_error( -20010, 'AccrualLoad, chk ER_ACCURAL_LOG, non-numeric ACCR_AMT' );
    END IF;
    l_cnt := 0;
  EXCEPTION
  WHEN OTHERS THEN
    l_msg := SUBSTR(sqlerrm( SQLCODE ),1,115);
    INSERT
    INTO REV_DM_STAGING.er_accural_log
      (
        er_accrual_load_num,
        dt_updated,
        status,
        comments
      )
      VALUES
      (
        l_load_num,
        sysdate,
        'Error on validate',
        'ACCR_AMT not numeric; '
        ||l_msg
      );
    COMMIT;
    grail_dm_part.PK_EMAIL.proc_email_notify('N', 'Error Inserting ER_ACCRUAL_STG_ERROR (Amt)', 7 );
    raise_application_error( -20010, sqlerrm( SQLCODE ) || ' AccrualLoad, chk ER_ACCURAL_LOG ' );
  END;
  -- -- AMT validation
  BEGIN
    INSERT INTO REV_DM_STAGING.ER_ACCRUAL_STG_ERROR
    SELECT a.ACCR_GL_ENTITY_CODE,
      a.ACCR_GL_LOCATION_CODE,
      a.ACCR_GL_DEPT_CODE,
      a.ACCR_GL_PRODUCT_CODE,
      a.ACCR_GL_BUSINESS_SEG_CODE,
      a.ACCR_GL_PERIOD_MON_YEAR,
      a.ACCR_ORG_CCY_CODE,
      a.ACCR_CLIENT_NAME,
      a.ACCR_AMT,
      a.ACCR_ACCT_CODE,
      a.ACCR_REVENUE_CODE,
      a.ACCR_APP_ID,
      a.ACCR_UNDERWRITING_YR,
      a.C14,
      a.ACCR_ACTUAL_RESTATE,
      'Client Account Code '
      ||ACCR_ACCT_CODE
      ||' does not exists',
      sysdate,
      l_load_num,
      a.CONTRACT_REF,
      a.BUSINESS_CLASS_DSC,
      a.CONTRACT_CLASS_DSC,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      R12_CODE
    FROM REVENUE_DM.ER_SELFSERVICE_ACCRUAL_RECORDS a
    WHERE a.ACCR_APP_ID = 'F'
    AND NOT EXISTS
      (SELECT 1
      FROM ereposit.er_tp_references b
      WHERE ltrim(rtrim(REPLACE(a.ACCR_ACCT_CODE,''''))) = b.tp_reference
      AND b.GC_REFERENCE_TYPE                            = l_as400_ref_type
      )
    AND a.FK_ACCR_FILE_ID = accr_id;
    l_cnt                := sql%rowcount;

    COMMIT;

    IF l_cnt              > 0 THEN
      grail_dm_part.PK_EMAIL.proc_email_notify('N', 'CLIENT_CODE not exist for AS400 Fac, load: '||l_load_num, 7 );
    END IF;
    l_cnt := 0;
  EXCEPTION
  WHEN OTHERS THEN
    l_msg := SUBSTR(sqlerrm( SQLCODE ),1,115);
    INSERT
    INTO REV_DM_STAGING.er_accural_log
      (
        er_accrual_load_num,
        dt_updated,
        status,
        comments
      )
      VALUES
      (
        l_load_num,
        sysdate,
        'Error on validate',
        'ER_TP_REFERENCES for AS400 FAC validate; '
        ||l_msg
      );
    COMMIT;
    grail_dm_part.PK_EMAIL.proc_email_notify('N', 'Error Inserting ER_ACCRUAL_STG_ERROR (F TpRef)', 7 );
    raise_application_error( -20006, sqlerrm( SQLCODE ) || ' AccrualLoad, chk ER_ACCURAL_LOG ' );
  END;
  BEGIN
    INSERT INTO REV_DM_STAGING.ER_ACCRUAL_STG_ERROR
    SELECT a.ACCR_GL_ENTITY_CODE,
      a.ACCR_GL_LOCATION_CODE,
      a.ACCR_GL_DEPT_CODE,
      a.ACCR_GL_PRODUCT_CODE,
      a.ACCR_GL_BUSINESS_SEG_CODE,
      a.ACCR_GL_PERIOD_MON_YEAR,
      a.ACCR_ORG_CCY_CODE,
      a.ACCR_CLIENT_NAME,
      a.ACCR_AMT,
      a.ACCR_ACCT_CODE,
      a.ACCR_REVENUE_CODE,
      a.ACCR_APP_ID,
      a.ACCR_UNDERWRITING_YR,
      a.C14,
      a.ACCR_ACTUAL_RESTATE,
      'Client Account Code '
      ||ACCR_ACCT_CODE
      ||' does not exists',
      sysdate,
      l_load_num,
      a.CONTRACT_REF,
      a.BUSINESS_CLASS_DSC,
      a.CONTRACT_CLASS_DSC,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      R12_CODE
    FROM REVENUE_DM.ER_SELFSERVICE_ACCRUAL_RECORDS a
    WHERE a.ACCR_APP_ID = 'C'
    AND NOT EXISTS
      (SELECT 1
      FROM ereposit.er_tp_references b
      WHERE ltrim(rtrim(REPLACE(a.ACCR_ACCT_CODE,'''')))= b.tp_reference
      AND b.GC_REFERENCE_TYPE                           = l_collins_gc_ref_type
      )
    AND a.FK_ACCR_FILE_ID = accr_id;
    l_cnt                := sql%rowcount;

    COMMIT;

    IF l_cnt              > 0 THEN
      grail_dm_part.PK_EMAIL.proc_email_notify('N', 'CLIENT_CODE not exist for Collins, load: '||l_load_num, 7 );
    END IF;
    l_cnt := 0;
  EXCEPTION
  WHEN OTHERS THEN
    l_msg := SUBSTR(sqlerrm( SQLCODE ),1,115);
    INSERT
    INTO REV_DM_STAGING.er_accural_log
      (
        er_accrual_load_num,
        dt_updated,
        status,
        comments
      )
      VALUES
      (
        l_load_num,
        sysdate,
        'Error on validate',
        'ER_TP_REFERENCES for Collins validate; '
        ||l_msg
      );
    COMMIT;
    grail_dm_part.PK_EMAIL.proc_email_notify('N', 'Error Inserting ER_ACCRUAL_STG_ERROR (C TpRef)', 7 );
    raise_application_error( -20006, sqlerrm( SQLCODE ) || ' AccrualLoad, chk ER_ACCURAL_LOG ' );
  END;
  -------------------    03/24/10 Restate start
  BEGIN
    INSERT INTO REV_DM_STAGING.ER_ACCRUAL_STG_ERROR
    SELECT a.ACCR_GL_ENTITY_CODE,
      a.ACCR_GL_LOCATION_CODE,
      a.ACCR_GL_DEPT_CODE,
      a.ACCR_GL_PRODUCT_CODE,
      a.ACCR_GL_BUSINESS_SEG_CODE,
      a.ACCR_GL_PERIOD_MON_YEAR,
      a.ACCR_ORG_CCY_CODE,
      a.ACCR_CLIENT_NAME,
      a.ACCR_AMT,
      a.ACCR_ACCT_CODE,
      a.ACCR_REVENUE_CODE,
      a.ACCR_APP_ID,
      a.ACCR_UNDERWRITING_YR,
      a.C14,
      a.ACCR_ACTUAL_RESTATE,
      --Changes made by Rabish as per CR360352--
      'Revenue Type '
      ||ACCR_REVENUE_CODE
      ||' not valid (BSA, FEE, NCB, ACC, INS)',
      sysdate,
      l_load_num,
      a.CONTRACT_REF,
      a.BUSINESS_CLASS_DSC,
      a.CONTRACT_CLASS_DSC,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      R12_CODE
    FROM REVENUE_DM.ER_SELFSERVICE_ACCRUAL_RECORDS a
    WHERE ltrim(rtrim(upper(ACCR_ACTUAL_RESTATE))) = 'ACTUAL'
    AND nvl(a.ACCR_REVENUE_CODE,'XXXX') NOT                   IN
      (SELECT short_code FROM ERCROSS.ER_ACCRUALS_REVENUE_TYPE
      )
    AND a.FK_ACCR_FILE_ID = accr_id;
    l_cnt                := sql%rowcount;

    COMMIT;

    IF l_cnt              > 0 THEN
      grail_dm_part.PK_EMAIL.proc_email_notify('N', 'ACCR_REVENUE_CODE not valid for Actual, load: '||l_load_num, 7 );
    END IF;
    l_cnt := 0;
  EXCEPTION
  WHEN OTHERS THEN
    l_msg := SUBSTR(sqlerrm( SQLCODE ),1,115);
    INSERT
    INTO REV_DM_STAGING.er_accural_log
      (
        er_accrual_load_num,
        dt_updated,
        status,
        comments
      )
      VALUES
      (
        l_load_num,
        sysdate,
        'Error on validate',
        'REVENUE_CODE validation (RevCode); '
        ||l_msg
      );
    COMMIT;
    grail_dm_part.PK_EMAIL.proc_email_notify('N', 'Error Inserting ER_ACCRUAL_STG_ERROR', 7 );
    raise_application_error( -20007, sqlerrm( SQLCODE ) || ' AccrualLoad, chk ER_ACCURAL_LOG ' );
  END;
  BEGIN
    INSERT INTO REV_DM_STAGING.ER_ACCRUAL_STG_ERROR
    SELECT a.ACCR_GL_ENTITY_CODE,
      a.ACCR_GL_LOCATION_CODE,
      a.ACCR_GL_DEPT_CODE,
      a.ACCR_GL_PRODUCT_CODE,
      a.ACCR_GL_BUSINESS_SEG_CODE,
      a.ACCR_GL_PERIOD_MON_YEAR,
      a.ACCR_ORG_CCY_CODE,
      a.ACCR_CLIENT_NAME,
      a.ACCR_AMT,
      a.ACCR_ACCT_CODE,
      a.ACCR_REVENUE_CODE,
      a.ACCR_APP_ID,
      a.ACCR_UNDERWRITING_YR,
      a.C14,
      a.ACCR_ACTUAL_RESTATE,
      --Changes made by Rabish as per CR#60352--
      'Revenue Type  '
      ||ACCR_REVENUE_CODE
      ||' not valid (BSA, FEE, ACC, INS)',
      sysdate,
      l_load_num,
      a.CONTRACT_REF,
      a.BUSINESS_CLASS_DSC,
      a.CONTRACT_CLASS_DSC,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      R12_CODE
    FROM REVENUE_DM.ER_SELFSERVICE_ACCRUAL_RECORDS a
    WHERE ltrim(rtrim(upper(ACCR_ACTUAL_RESTATE))) IN ('RESTATE ACQUISITION', 'RESTATE RECLASSIFICATION', 'RESTATE OTHER' )
    AND
    nvl(a.ACCR_REVENUE_CODE,'xxxx') NOT                    IN
      (SELECT short_code FROM ERCROSS.ER_ACCRUALS_REVENUE_TYPE
      MINUS
      SELECT 'NCB' FROM dual
      )
    AND a.FK_ACCR_FILE_ID = accr_id;
    l_cnt                := sql%rowcount;

    COMMIT;

    IF l_cnt              > 0 THEN
      grail_dm_part.PK_EMAIL.proc_email_notify('N', 'ACCR_REVENUE_CODE not valid for Restate, load: '||l_load_num, 7 );
    END IF;
    l_cnt := 0;
  EXCEPTION
  WHEN OTHERS THEN
    l_msg := SUBSTR(sqlerrm( SQLCODE ),1,115);
    INSERT
    INTO REV_DM_STAGING.er_accural_log
      (
        er_accrual_load_num,
        dt_updated,
        status,
        comments
      )
      VALUES
      (
        l_load_num,
        sysdate,
        'Error on validate',
        'REVENUE_CODE validation (RevCode); '
        ||l_msg
      );
    COMMIT;
    grail_dm_part.PK_EMAIL.proc_email_notify('N', 'Error Inserting ER_ACCRUAL_STG_ERROR', 7 );
    raise_application_error( -20007, sqlerrm( SQLCODE ) || ' AccrualLoad, chk ER_ACCURAL_LOG ' );
  END;
  ------------   03/24/10 Restate end
  BEGIN

  INSERT INTO REV_DM_STAGING.ER_ACCRUAL_STG_ERROR
    SELECT a.ACCR_GL_ENTITY_CODE,
      a.ACCR_GL_LOCATION_CODE,
      a.ACCR_GL_DEPT_CODE,
      a.ACCR_GL_PRODUCT_CODE,
      a.ACCR_GL_BUSINESS_SEG_CODE,
      a.ACCR_GL_PERIOD_MON_YEAR,
      a.ACCR_ORG_CCY_CODE,
      a.ACCR_CLIENT_NAME,
      a.ACCR_AMT,
      a.ACCR_ACCT_CODE,
      a.ACCR_REVENUE_CODE,
      a.ACCR_APP_ID,
      a.ACCR_UNDERWRITING_YR,
      a.C14,
      a.ACCR_ACTUAL_RESTATE,
      'Underwriting Year '
      ||ACCR_UNDERWRITING_YR
      ||' is invalid',
      sysdate,
      l_load_num,
      a.CONTRACT_REF,
      a.BUSINESS_CLASS_DSC,
      a.CONTRACT_CLASS_DSC,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      R12_CODE
    FROM REVENUE_DM.ER_SELFSERVICE_ACCRUAL_RECORDS a
    WHERE
    a.FK_ACCR_FILE_ID    = accr_id AND
    (LENGTH(trim(REPLACE(a.ACCR_UNDERWRITING_YR,''''))) <> 2
    OR upper(SUBSTR(a.ACCR_UNDERWRITING_YR,1,1)) IN ('A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U', 'V','W','X','Y','Z','=','*','$','#','@','!',' ','_','-','.')
    OR ACCR_UNDERWRITING_YR IS NULL);

    l_cnt                   := sql%rowcount;

--    IF l_cnt = 0 THEN
--    INSERT INTO REV_DM_STAGING.ER_ACCRUAL_STG_ERROR
--    SELECT a.ACCR_GL_ENTITY_CODE,
--      a.ACCR_GL_LOCATION_CODE,
--      a.ACCR_GL_DEPT_CODE,
--      a.ACCR_GL_PRODUCT_CODE,
--      a.ACCR_GL_BUSINESS_SEG_CODE,
--      a.ACCR_GL_PERIOD_MON_YEAR,
--      a.ACCR_ORG_CCY_CODE,
--      a.ACCR_CLIENT_NAME,
--      a.ACCR_AMT,
--      a.ACCR_ACCT_CODE,
--      a.ACCR_REVENUE_CODE,
--      a.ACCR_APP_ID,
--      a.ACCR_UNDERWRITING_YR,
--      a.C14,
--      a.ACCR_ACTUAL_RESTATE,
--      'Underwriting Year '
--      ||ACCR_UNDERWRITING_YR
--      ||' is not 2 digit',
--      sysdate,
--      l_load_num,
--      a.CONTRACT_REF,
--      a.BUSINESS_CLASS_DSC,
--      a.CONTRACT_CLASS_DSC,
--      NULL,
--      NULL,
--      NULL,
--      NULL,
--      NULL
--    FROM REVENUE_DM.ER_SELFSERVICE_ACCRUAL_RECORDS a
--    WHERE to_number(REPLACE(a.ACCR_UNDERWRITING_YR,'''')) NOT BETWEEN 0 AND 99
--    AND a.FK_ACCR_FILE_ID    = accr_id;
--    l_cnt                   := sql%rowcount;
--    END IF;


   COMMIT;

    IF l_cnt                 > 0 THEN
      grail_dm_part.PK_EMAIL.proc_email_notify('N', 'UWY not valid, load: '||l_load_num, 7 );
    END IF;
    l_cnt := 0;
  EXCEPTION
  WHEN OTHERS THEN
    l_msg := SUBSTR(sqlerrm( SQLCODE ),1,115);
    INSERT
    INTO REV_DM_STAGING.er_accural_log
      (
        er_accrual_load_num,
        dt_updated,
        status,
        comments
      )
      VALUES
      (
        l_load_num,
        sysdate,
        'Error on validate',
        'UWY validate; '
        ||l_msg
      );
    COMMIT;
    grail_dm_part.PK_EMAIL.proc_email_notify('N', 'Error Inserting ER_ACCRUAL_STG_ERROR (UWY)', 7 );
    raise_application_error( -20008, sqlerrm( SQLCODE ) || ' AccrualLoad, chk ER_ACCURAL_LOG ' );
  END;
  BEGIN
    INSERT INTO REV_DM_STAGING.ER_ACCRUAL_STG_ERROR
    SELECT a.ACCR_GL_ENTITY_CODE,
      a.ACCR_GL_LOCATION_CODE,
      a.ACCR_GL_DEPT_CODE,
      a.ACCR_GL_PRODUCT_CODE,
      a.ACCR_GL_BUSINESS_SEG_CODE,
      a.ACCR_GL_PERIOD_MON_YEAR,
      a.ACCR_ORG_CCY_CODE,
      a.ACCR_CLIENT_NAME,
      a.ACCR_AMT,
      a.ACCR_ACCT_CODE,
      a.ACCR_REVENUE_CODE,
      a.ACCR_APP_ID,
      a.ACCR_UNDERWRITING_YR,
      a.C14,
      a.ACCR_ACTUAL_RESTATE,
      'Revenue Source '
      ||C14
      ||' not valid (N, P, R, O)',
      sysdate,
      l_load_num,
      a.CONTRACT_REF,
      a.BUSINESS_CLASS_DSC,
      a.CONTRACT_CLASS_DSC,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      R12_CODE
    FROM REVENUE_DM.ER_SELFSERVICE_ACCRUAL_RECORDS a
    WHERE NVL(ltrim(rtrim(upper(C14))),' ') NOT IN ('N','P','R','O')
    AND a.FK_ACCR_FILE_ID                        = accr_id;
    l_cnt                                       := sql%rowcount;

    COMMIT;

    IF l_cnt                                     > 0 THEN
      grail_dm_part.PK_EMAIL.proc_email_notify('N', 'SOURCE_OF_REVENUE not valid, load: '||l_load_num, 7 );
    END IF;
    l_cnt := 0;
  EXCEPTION
  WHEN OTHERS THEN
    l_msg := SUBSTR(sqlerrm( SQLCODE ),1,115);
    INSERT
    INTO REV_DM_STAGING.er_accural_log
      (
        er_accrual_load_num,
        dt_updated,
        status,
        comments
      )
      VALUES
      (
        l_load_num,
        sysdate,
        'Error on validate',
        'SOURCE_OF_REVENUE validate; '
        ||l_msg
      );
    COMMIT;
    grail_dm_part.PK_EMAIL.proc_email_notify('N', 'Error Inserting ER_ACCRUAL_STG_ERROR (RevSrc)', 7 );
    raise_application_error( -20009, sqlerrm( SQLCODE ) || ' AccrualLoad, chk ER_ACCURAL_LOG ' );
  END;
  BEGIN
    INSERT INTO REV_DM_STAGING.ER_ACCRUAL_STG_ERROR
    SELECT a.ACCR_GL_ENTITY_CODE,
      a.ACCR_GL_LOCATION_CODE,
      a.ACCR_GL_DEPT_CODE,
      a.ACCR_GL_PRODUCT_CODE,
      a.ACCR_GL_BUSINESS_SEG_CODE,
      a.ACCR_GL_PERIOD_MON_YEAR,
      a.ACCR_ORG_CCY_CODE,
      a.ACCR_CLIENT_NAME,
      a.ACCR_AMT,
      a.ACCR_ACCT_CODE,
      a.ACCR_REVENUE_CODE,
      a.ACCR_APP_ID,
      a.ACCR_UNDERWRITING_YR,
      a.C14,
      a.ACCR_ACTUAL_RESTATE,
      'Application ID '
      ||ACCR_APP_ID
      ||' not valid (C, F, G, R, T)',
      sysdate,
      l_load_num,
      a.CONTRACT_REF,
      a.BUSINESS_CLASS_DSC,
      a.CONTRACT_CLASS_DSC,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      R12_CODE
    FROM REVENUE_DM.ER_SELFSERVICE_ACCRUAL_RECORDS a
    WHERE (ltrim(rtrim(ACCR_APP_ID)) NOT IN ('C','F','G','R','T')
    OR ACCR_APP_ID                      IS NULL)
    AND a.FK_ACCR_FILE_ID                = accr_id;
    l_cnt                               := sql%rowcount;

    COMMIT;

    IF l_cnt                             > 0 THEN
      grail_dm_part.PK_EMAIL.proc_email_notify('N', 'ACCR_APP_ID not valid, load: '||l_load_num, 7 );
    END IF;
    l_cnt := 0;
  EXCEPTION
  WHEN OTHERS THEN
    l_msg := SUBSTR(sqlerrm( SQLCODE ),1,115);
    INSERT
    INTO REV_DM_STAGING.er_accural_log
      (
        er_accrual_load_num,
        dt_updated,
        status,
        comments
      )
      VALUES
      (
        l_load_num,
        sysdate,
        'Error on validate',
        'ACCR_APP_ID validate; '
        ||l_msg
      );
    COMMIT;
    grail_dm_part.PK_EMAIL.proc_email_notify('N', 'Error Inserting ER_ACCRUAL_STG_ERROR (AppID)', 7 );
    raise_application_error( -20010, sqlerrm( SQLCODE ) || ' AccrualLoad, chk ER_ACCURAL_LOG ' );
  END;
  BEGIN
    INSERT INTO REV_DM_STAGING.ER_ACCRUAL_STG_ERROR
    SELECT a.ACCR_GL_ENTITY_CODE,
      a.ACCR_GL_LOCATION_CODE,
      a.ACCR_GL_DEPT_CODE,
      a.ACCR_GL_PRODUCT_CODE,
      a.ACCR_GL_BUSINESS_SEG_CODE,
      a.ACCR_GL_PERIOD_MON_YEAR,
      a.ACCR_ORG_CCY_CODE,
      a.ACCR_CLIENT_NAME,
      a.ACCR_AMT,
      a.ACCR_ACCT_CODE,
      a.ACCR_REVENUE_CODE,
      a.ACCR_APP_ID,
      a.ACCR_UNDERWRITING_YR,
      a.C14,
      a.ACCR_ACTUAL_RESTATE,
      'Original Currency Code '
      ||ACCR_ORG_CCY_CODE
      ||' not found',
      sysdate,
      l_load_num,
      a.CONTRACT_REF,
      a.BUSINESS_CLASS_DSC,
      a.CONTRACT_CLASS_DSC,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      R12_CODE
    FROM REVENUE_DM.ER_SELFSERVICE_ACCRUAL_RECORDS a
    WHERE NOT EXISTS
      (SELECT 1
      FROM ereposit.ER_CURRENCY_INFO b
      WHERE ltrim(rtrim(a.ACCR_ORG_CCY_CODE)) = b.GC_CURRENCY_CODE
      AND b.GC_CURRENCY_CODE                 <> 'XXX'
      )
    AND a.FK_ACCR_FILE_ID = accr_id ;
    l_cnt                := sql%rowcount;

    COMMIT;

    IF l_cnt              > 0 THEN
      grail_dm_part.PK_EMAIL.proc_email_notify('N', 'ACCR_ORG_CCY_CODE not valid, load: '||l_load_num, 7 );
    END IF;
    l_cnt := 0;
  EXCEPTION
  WHEN OTHERS THEN
    l_msg := SUBSTR(sqlerrm( SQLCODE ),1,115);
    INSERT
    INTO REV_DM_STAGING.er_accural_log
      (
        er_accrual_load_num,
        dt_updated,
        status,
        comments
      )
      VALUES
      (
        l_load_num,
        sysdate,
        'Error on validate',
        'ACCR_ORG_CCY_CODE validate; '
        ||l_msg
      );
    COMMIT;
    grail_dm_part.PK_EMAIL.proc_email_notify('N', 'Error Inserting ER_ACCRUAL_STG_ERROR (CCY)', 7 );
    raise_application_error( -20010, sqlerrm( SQLCODE ) || ' AccrualLoad, chk ER_ACCURAL_LOG ' );
  END;
  BEGIN
    INSERT INTO REV_DM_STAGING.ER_ACCRUAL_STG_ERROR
    SELECT a.ACCR_GL_ENTITY_CODE,
      a.ACCR_GL_LOCATION_CODE,
      a.ACCR_GL_DEPT_CODE,
      a.ACCR_GL_PRODUCT_CODE,
      a.ACCR_GL_BUSINESS_SEG_CODE,
      a.ACCR_GL_PERIOD_MON_YEAR,
      a.ACCR_ORG_CCY_CODE,
      a.ACCR_CLIENT_NAME,
      a.ACCR_AMT,
      a.ACCR_ACCT_CODE,
      a.ACCR_REVENUE_CODE,
      a.ACCR_APP_ID,
      a.ACCR_UNDERWRITING_YR,
      a.C14,
      a.ACCR_ACTUAL_RESTATE,
      'Accouting Period '
      ||ACCR_GL_PERIOD_MON_YEAR
      ||'is in wrong format',
      sysdate,
      l_load_num,
      a.CONTRACT_REF,
      a.BUSINESS_CLASS_DSC,
      a.CONTRACT_CLASS_DSC,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      R12_CODE
    FROM REVENUE_DM.ER_SELFSERVICE_ACCRUAL_RECORDS a
    WHERE (
    a.ACCR_GL_PERIOD_MON_YEAR IS NULL OR
    SUBSTR(a.ACCR_GL_PERIOD_MON_YEAR,3,1) <> '-'
    OR SUBSTR(a.ACCR_GL_PERIOD_MON_YEAR,1,2) NOT BETWEEN '01' AND '12'
    OR ACCR_GL_PERIOD_MON_YEAR            = ' '
    OR LENGTH(a.ACCR_GL_PERIOD_MON_YEAR) <> 7)
    AND a.FK_ACCR_FILE_ID                 = accr_id;
    l_cnt                                := sql%rowcount;

    COMMIT;

    IF l_cnt                              > 0 THEN
      grail_dm_part.PK_EMAIL.proc_email_notify('N', 'ACCR_GL_PERIOD_MON_YEAR not valid, load: '||l_load_num, 7 );
    END IF;
    l_cnt := 0;
  EXCEPTION
  WHEN OTHERS THEN
    l_msg := SUBSTR(sqlerrm( SQLCODE ),1,115);
    INSERT
    INTO REV_DM_STAGING.er_accural_log
      (
        er_accrual_load_num,
        dt_updated,
        status,
        comments
      )
      VALUES
      (
        l_load_num,
        sysdate,
        'Error on validate',
        'ACCR_GL_PERIOD_MON_YEAR validate; '
        ||l_msg
      );
    COMMIT;
    grail_dm_part.PK_EMAIL.proc_email_notify('N', 'Error Inserting ER_ACCRUAL_STG_ERROR (PERIOD)', 7 );
    raise_application_error( -20010, sqlerrm( SQLCODE ) || ' AccrualLoad, chk ER_ACCURAL_LOG ' );
  END;
  --- 03/24/10 Restate start
  BEGIN
    INSERT INTO REV_DM_STAGING.ER_ACCRUAL_STG_ERROR
    SELECT a.ACCR_GL_ENTITY_CODE,
      a.ACCR_GL_LOCATION_CODE,
      a.ACCR_GL_DEPT_CODE,
      a.ACCR_GL_PRODUCT_CODE,
      a.ACCR_GL_BUSINESS_SEG_CODE,
      a.ACCR_GL_PERIOD_MON_YEAR,
      a.ACCR_ORG_CCY_CODE,
      a.ACCR_CLIENT_NAME,
      a.ACCR_AMT,
      a.ACCR_ACCT_CODE,
      a.ACCR_REVENUE_CODE,
      a.ACCR_APP_ID,
      a.ACCR_UNDERWRITING_YR,
      a.C14,
      a.ACCR_ACTUAL_RESTATE,
      'Actual/Restate '
      ||ACCR_ACTUAL_RESTATE
      ||' not valid (Actual, Restate Acquisition, Restate Reclassification, Restate Other)',
      sysdate,
      l_load_num,
      a.CONTRACT_REF,
      a.BUSINESS_CLASS_DSC,
      a.CONTRACT_CLASS_DSC,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      R12_CODE
    FROM REVENUE_DM.ER_SELFSERVICE_ACCRUAL_RECORDS a
    WHERE (upper(ACCR_ACTUAL_RESTATE) NOT IN ('ACTUAL', 'RESTATE ACQUISITION', 'RESTATE RECLASSIFICATION', 'RESTATE OTHER')
      --Added by Rabish as per CR#60578--
    OR upper(ACCR_ACTUAL_RESTATE) IS NULL)
    AND a.FK_ACCR_FILE_ID          = accr_id;
    l_cnt                         := sql%rowcount;

    COMMIT;

    IF l_cnt                       > 0 THEN
      grail_dm_part.PK_EMAIL.proc_email_notify('N', 'ACCR_ACTUAL_RESTATE not valid, load: '||l_load_num, 7 );
    END IF;
    l_cnt := 0;
  EXCEPTION
  WHEN OTHERS THEN
    l_msg := SUBSTR(sqlerrm( SQLCODE ),1,115);
    INSERT
    INTO REV_DM_STAGING.er_accural_log
      (
        er_accrual_load_num,
        dt_updated,
        status,
        comments
      )
      VALUES
      (
        l_load_num,
        sysdate,
        'Error on validate',
        'ACCR_ACTUAL_RESTATE validate; '
        ||l_msg
      );
    COMMIT;
    grail_dm_part.PK_EMAIL.proc_email_notify('N', 'Error Inserting ER_ACCRUAL_STG_ERROR (RevSrc)', 7 );
    raise_application_error( -20009, sqlerrm( SQLCODE ) || ' AccrualLoad, chk ER_ACCURAL_LOG ' );
  END;
  -- 03/24/10 Restate end
  --09/23/2010 contract ref validation start
  BEGIN
    INSERT INTO REV_DM_STAGING.ER_ACCRUAL_STG_ERROR
    SELECT a.ACCR_GL_ENTITY_CODE,
      a.ACCR_GL_LOCATION_CODE,
      a.ACCR_GL_DEPT_CODE,
      a.ACCR_GL_PRODUCT_CODE,
      a.ACCR_GL_BUSINESS_SEG_CODE,
      a.ACCR_GL_PERIOD_MON_YEAR,
      a.ACCR_ORG_CCY_CODE,
      a.ACCR_CLIENT_NAME,
      a.ACCR_AMT,
      a.ACCR_ACCT_CODE,
      a.ACCR_REVENUE_CODE,
      a.ACCR_APP_ID,
      a.ACCR_UNDERWRITING_YR,
      a.C14,
      a.ACCR_ACTUAL_RESTATE,
      'Contract Ref '
      ||contract_ref
      ||' not valid',
      sysdate,
      l_load_num,
      a.CONTRACT_REF,
      a.BUSINESS_CLASS_DSC,
      a.CONTRACT_CLASS_DSC,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      R12_CODE
    FROM REVENUE_DM.ER_SELFSERVICE_ACCRUAL_RECORDS a
    WHERE a.contract_ref IS NOT NULL
    AND NOT EXISTS
      (SELECT 1
      FROM revenue_dm.contract_dim b
        --   where ltrim(rtrim(replace(a.contract_ref,''''))) = b.gc_our_contract_ref) ;
      WHERE ltrim(rtrim(REPLACE((
        CASE
          WHEN SUBSTR(a.contract_ref,                                             -3) = 'MEX'
          THEN SUBSTR(a.contract_ref,1,INSTR(a.contract_ref,SUBSTR(a.contract_ref,-3))-2)
          WHEN SUBSTR(a.contract_ref,                                             -3) = 'CHL'
          THEN SUBSTR(a.contract_ref,1,INSTR(a.contract_ref,SUBSTR(a.contract_ref,-3))-2)
            || SUBSTR(a.contract_ref,                                             -3)
          ELSE a.contract_ref
        END ),''''))) = trim(b.gc_our_contract_ref)
      )
    AND a.FK_ACCR_FILE_ID = accr_id;
    l_cnt                := sql%rowcount;

    COMMIT;

    IF l_cnt              > 0 THEN
      grail_dm_part.PK_EMAIL.proc_email_notify('N', 'Contract reference not exist, load: '||l_load_num, 7 );
    END IF;
    l_cnt := 0;
  EXCEPTION
  WHEN OTHERS THEN
    l_msg := SUBSTR(sqlerrm( SQLCODE ),1,115);
    INSERT
    INTO REV_DM_STAGING.er_accural_log
      (
        er_accrual_load_num,
        dt_updated,
        status,
        comments
      )
      VALUES
      (
        l_load_num,
        sysdate,
        'Error on validate',
        'revenue_dm.contract_dim for contract_ref validate; '
        ||l_msg
      );
    COMMIT;
    grail_dm_part.PK_EMAIL.proc_email_notify('N', 'Error Inserting ER_ACCRUAL_STG_ERROR (contract ref)', 7 );
    raise_application_error( -20003, sqlerrm( SQLCODE ) || ' AccrualLoad, chk ER_ACCURAL_LOG ' );
  END;
  --09/23/2010 contract ref validation end
  --09/23/2010 business classification validation start
  BEGIN
    INSERT INTO REV_DM_STAGING.ER_ACCRUAL_STG_ERROR
    SELECT a.ACCR_GL_ENTITY_CODE,
      a.ACCR_GL_LOCATION_CODE,
      a.ACCR_GL_DEPT_CODE,
      a.ACCR_GL_PRODUCT_CODE,
      a.ACCR_GL_BUSINESS_SEG_CODE,
      a.ACCR_GL_PERIOD_MON_YEAR,
      a.ACCR_ORG_CCY_CODE,
      a.ACCR_CLIENT_NAME,
      a.ACCR_AMT,
      a.ACCR_ACCT_CODE,
      a.ACCR_REVENUE_CODE,
      a.ACCR_APP_ID,
      a.ACCR_UNDERWRITING_YR,
      a.C14,
      a.ACCR_ACTUAL_RESTATE,
      'Business class dsc '
      ||business_class_dsc
      ||' not valid',
      sysdate,
      l_load_num,
      a.CONTRACT_REF,
      a.BUSINESS_CLASS_DSC,
      a.CONTRACT_CLASS_DSC,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      R12_CODE
    FROM REVENUE_DM.ER_SELFSERVICE_ACCRUAL_RECORDS a
    WHERE a.BUSINESS_CLASS_DSC IS NOT NULL
    AND NOT EXISTS
      (SELECT 1
      FROM revenue_dm.business_classification_dim b
      WHERE upper(ltrim(rtrim(REPLACE(a.business_class_dsc,'''')))) = upper(b.busness_class_dsc)
      )
    AND a.FK_ACCR_FILE_ID = accr_id ;
    l_cnt                := sql%rowcount;

    COMMIT;

    IF l_cnt              > 0 THEN
      grail_dm_part.PK_EMAIL.proc_email_notify('N', 'Business Classification dsc not exist, load: '||l_load_num, 7 );
    END IF;
    l_cnt := 0;
  EXCEPTION
  WHEN OTHERS THEN
    l_msg := SUBSTR(sqlerrm( SQLCODE ),1,115);
    INSERT
    INTO REV_DM_STAGING.er_accural_log
      (
        er_accrual_load_num,
        dt_updated,
        status,
        comments
      )
      VALUES
      (
        l_load_num,
        sysdate,
        'Error on validate',
        'revenue_dm.business_classification_dim for business class dsc validate; '
        ||l_msg
      );
    COMMIT;
    grail_dm_part.PK_EMAIL.proc_email_notify('N', 'Error Inserting ER_ACCRUAL_STG_ERROR (Business class dsc)', 7 );
    raise_application_error( -20003, sqlerrm( SQLCODE ) || ' AccrualLoad, chk ER_ACCURAL_LOG ' );
  END;
  --09/23/2010 business classification validation end
  --09/23/2010 Contract classification validation start
  BEGIN
    INSERT INTO REV_DM_STAGING.ER_ACCRUAL_STG_ERROR
    SELECT a.ACCR_GL_ENTITY_CODE,
      a.ACCR_GL_LOCATION_CODE,
      a.ACCR_GL_DEPT_CODE,
      a.ACCR_GL_PRODUCT_CODE,
      a.ACCR_GL_BUSINESS_SEG_CODE,
      a.ACCR_GL_PERIOD_MON_YEAR,
      a.ACCR_ORG_CCY_CODE,
      a.ACCR_CLIENT_NAME,
      a.ACCR_AMT,
      a.ACCR_ACCT_CODE,
      a.ACCR_REVENUE_CODE,
      a.ACCR_APP_ID,
      a.ACCR_UNDERWRITING_YR,
      a.C14,
      a.ACCR_ACTUAL_RESTATE,
      'Contract class dsc '
      ||CONTRACT_CLASS_DSC
      ||' not valid',
      sysdate,
      l_load_num,
      a.CONTRACT_REF,
      a.BUSINESS_CLASS_DSC,
      a.CONTRACT_CLASS_DSC,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      R12_CODE
    FROM REVENUE_DM.ER_SELFSERVICE_ACCRUAL_RECORDS a
    WHERE a.CONTRACT_CLASS_DSC IS NOT NULL
    AND NOT EXISTS
      (SELECT 1
      FROM revenue_dm.contract_classification_dim b
      WHERE upper(ltrim(rtrim(REPLACE(a.contract_class_dsc,'''')))) = upper(b.contract_sub_type_dsc)
      )
    AND a.FK_ACCR_FILE_ID = accr_id;
    l_cnt                := sql%rowcount;

    COMMIT;

    IF l_cnt              > 0 THEN
      grail_dm_part.PK_EMAIL.proc_email_notify('N', 'Contract Classification dsc not exist, load: '||l_load_num, 7 );
    END IF;
    l_cnt := 0;
  EXCEPTION
  WHEN OTHERS THEN
    l_msg := SUBSTR(sqlerrm( SQLCODE ),1,115);
    INSERT
    INTO REV_DM_STAGING.er_accural_log
      (
        er_accrual_load_num,
        dt_updated,
        status,
        comments
      )
      VALUES
      (
        l_load_num,
        sysdate,
        'Error on validate',
        'revenue_dm.contract_classification_dim for business class dsc validate; '
        ||l_msg
      );
    COMMIT;
    grail_dm_part.PK_EMAIL.proc_email_notify('N', 'Error Inserting ER_ACCRUAL_STG_ERROR (Contract Class dsc)', 7 );
    raise_application_error( -20003, sqlerrm( SQLCODE ) || ' AccrualLoad, chk ER_ACCURAL_LOG ' );
  END;

   BEGIN
    INSERT INTO REV_DM_STAGING.ER_ACCRUAL_STG_ERROR
    SELECT a.ACCR_GL_ENTITY_CODE,
      a.ACCR_GL_LOCATION_CODE,
      a.ACCR_GL_DEPT_CODE,
      a.ACCR_GL_PRODUCT_CODE,
      a.ACCR_GL_BUSINESS_SEG_CODE,
      a.ACCR_GL_PERIOD_MON_YEAR,
      a.ACCR_ORG_CCY_CODE,
      a.ACCR_CLIENT_NAME,
      a.ACCR_AMT,
      a.ACCR_ACCT_CODE,
      a.ACCR_REVENUE_CODE,
      a.ACCR_APP_ID,
      a.ACCR_UNDERWRITING_YR,
      a.C14,
      a.ACCR_ACTUAL_RESTATE,
      'Cedent name '
      ||ACCR_CLIENT_NAME
      ||' not valid',
      sysdate,
      l_load_num,
      a.CONTRACT_REF,
      a.BUSINESS_CLASS_DSC,
      a.CONTRACT_CLASS_DSC,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      R12_CODE
    FROM REVENUE_DM.ER_SELFSERVICE_ACCRUAL_RECORDS a
    WHERE a.ACCR_CLIENT_NAME IS NULL
    AND a.FK_ACCR_FILE_ID = accr_id;
    l_cnt                := sql%rowcount;

    COMMIT;

    IF l_cnt              > 0 THEN
      grail_dm_part.PK_EMAIL.proc_email_notify('N', 'cedent name not exist, load: '||l_load_num, 7 );
    END IF;
    l_cnt := 0;
  EXCEPTION
  WHEN OTHERS THEN
    l_msg := SUBSTR(sqlerrm( SQLCODE ),1,115);
    INSERT
    INTO REV_DM_STAGING.er_accural_log
      (
        er_accrual_load_num,
        dt_updated,
        status,
        comments
      )
      VALUES
      (
        l_load_num,
        sysdate,
        'Error on validate',
        'cedent name '
        ||l_msg
      );
    COMMIT;
    grail_dm_part.PK_EMAIL.proc_email_notify('N', 'Error Inserting ER_ACCRUAL_STG_ERROR (cedent name)', 7 );
    raise_application_error( -20003, sqlerrm( SQLCODE ) || ' AccrualLoad, chk ER_ACCURAL_LOG ' );
  END;

  l_cnt := 0;
  SELECT COUNT(*)
  INTO l_cnt
  FROM REV_DM_STAGING.ER_ACCRUAL_STG_ERROR
  WHERE ER_ACCRUAL_LOAD_NUM = l_load_num;
  IF l_cnt                  = 0 THEN
    grail_dm_part.PK_EMAIL.proc_email_notify('N', 'SUCCESS, there is '||l_cnt||' rows on ER_ACCRUAL_STG_ERROR for load number '||l_load_num , 901 );
  ELSE
    grail_dm_part.PK_EMAIL.proc_email_notify('N', 'ERROR, there is '||l_cnt||' rows on ER_ACCRUAL_STG_ERROR for load number '||l_load_num , 7 );
  END IF;
  RETURN l_load_num;
EXCEPTION
WHEN OTHERS THEN

return -1;

END VALIDATE_ACCRUALS;

END PKG_ER_SELFSERVICE_AUTOMATION;
/
