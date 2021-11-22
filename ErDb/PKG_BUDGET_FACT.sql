CREATE OR REPLACE PACKAGE            PKG_BUDGET_FACT AS
PROCEDURE PROC_BUDGET_FACT(P_YEAR NUMBER) ;
PROCEDURE PROC_LOAD_ER_HYP_STG_1(P_YEAR NUMBER);
PROCEDURE PROC_LOAD_ER_HYP_STG_2;
PROCEDURE PROC_BUDGET_FACT_BLD(P_YEAR NUMBER);
PROCEDURE PROC_MAP_CON_DIM_ID;
PROCEDURE PROC_UPD_CLASS_OF_BUSNS(P_YEAR NUMBER); -- stt 56240. Added this procedure to populate CLASS_OF_BUSINESS column
PROCEDURE PROC_UPD_CNT_CLASS_DIM(P_YEAR NUMBER);
END;
/


CREATE OR REPLACE PACKAGE BODY            PKG_BUDGET_FACT IS
PROCEDURE PROC_BUDGET_FACT(P_YEAR NUMBER) IS
error_message VARCHAR2 (300);
BEGIN
	BEGIN
		PROC_LOAD_ER_HYP_STG_1(P_YEAR);
		PROC_LOAD_ER_HYP_STG_2;
		PROC_BUDGET_FACT_BLD(P_YEAR);
		PROC_MAP_CON_DIM_ID;
		PROC_UPD_CLASS_OF_BUSNS(P_YEAR);    -- STT 56240. Calling this procedure to populate CLASS_OF_BUSINESS column.
		PROC_UPD_CNT_CLASS_DIM(P_YEAR);
	EXCEPTION WHEN OTHERS
		THEN NULL;
	END;
	GRAIL_DM_PART.PK_EMAIL.proc_email_notify('N', 'Package PKG_BUDGET_FACT Completed Successsfully', 18 );
EXCEPTION WHEN OTHERS
		THEN NULL;
	error_message :=SUBSTR(SQLERRM, 1, 200);
  GRAIL_DM_PART.PK_EMAIL.proc_email_notify('N', 'PKG_BUDGET_FACT.PROC_BUDGET_FACT failed with the error '||SQLERRM, 17 );
  raise_application_error(-20102, 'PKG_BUDGET_FACT.PROC_BUDGET_FACT failed '||error_message);
END PROC_BUDGET_FACT;
-----------------------------------------------------------
--INSERTING DATA INTO EREPOSIT.ER_HYPERION_STAGE_1
-----------------------------------------------------------
PROCEDURE PROC_LOAD_ER_HYP_STG_1(P_YEAR  NUMBER) IS
error_message VARCHAR2 (300);
BEGIN
DELETE FROM EREPOSIT.ER_HYPERION_STAGE_1;
COMMIT;
INSERT INTO ereposit.er_hyperion_stage_1
select 'T',CLVR_ENTITY_MBR,
        case when CLVR_CONTRACT_MBR like '%_NA'
                  then replace(regexp_replace(regexp_replace(CLVR_CONTRACT_MBR,'[:^a-z:]'),'[:^A-Z:]'),'_')
             WHEN CLVR_CONTRACT_MBR LIKE '%_NA____'
                  THEN REPLACE(regexp_replace(regexp_replace(CLVR_CONTRACT_MBR,'[:^a-z:]'),'[:^A-Z:]'),'_')
             WHEN CLVR_CONTRACT_MBR LIKE '%(_)%'
                  THEN substr(CLVR_CONTRACT_MBR,instr(CLVR_CONTRACT_MBR,'_',-1)+1)
             when (regexp_like (CLVR_CONTRACT_MBR ,'[:^\().:]') or CLVR_CONTRACT_MBR like '%&%')
                  then trim(replace(replace(regexp_replace(regexp_replace(regexp_replace(CLVR_CONTRACT_MBR,'[:^_\().:]'),'[:^a-z:]'),'[:^A-Z:]'),'-'),'&'))
             when CLVR_CONTRACT_MBR = 'No_Customer'
                  then CLVR_CONTRACT_MBR
             else  substr(CLVR_CONTRACT_MBR,instr(CLVR_CONTRACT_MBR,'_',-1)+1) end,case when CLVR_CONTRACT_MBR like '%_NA' then CLVR_CONTRACT_MBR when (regexp_like (CLVR_CONTRACT_MBR ,'[:^\().:]') or CLVR_CONTRACT_MBR like '%&%') then 'Not_Available' when (CLVR_CONTRACT_MBR = 'No_Customer' or CLVR_CONTRACT_MBR = 'Unallocated' or CLVR_CONTRACT_MBR like 'Client%') then 'Not_Available' else substr(CLVR_CONTRACT_MBR,1,instr(CLVR_CONTRACT_MBR,'_',-1)-1) end,CLVR_ACCOUNT_MBR,ROUND(SUM(JAN_AMT),2),
  ROUND(SUM(FEB_AMT),2),  ROUND( SUM(MAR_AMT),2),  ROUND(SUM(APR_AMT),2),ROUND(SUM(MAY_AMT),2),ROUND(SUM(JUN_AMT),2),ROUND(SUM(JUL_AMT),2),ROUND(SUM(AUG_AMT),2),ROUND(SUM(SEP_AMT),2),  ROUND(SUM(OCT_AMT),2),ROUND(SUM(NOV_AMT),2),ROUND(SUM(DEC_AMT),2)
  FROM EPMAHYPER.CLVR_LEVEL0_EXTRACT
  WHERE CLVR_CURRENCY_TYPE_MBR = 'Reporting_USD'
    AND CLVR_SCENARIO_MBR = 'Budget'
    AND CLVR_VERSION_MBR = 'Final'
    AND CLVR_FISCAL_YEAR_MBR = 'FY'||SUBSTR(P_YEAR,-2)
    AND CLVR_ENTITY_MBR !='No_GC_Entity'
    AND CLVR_CONTRACT_MBR != 'No_Customer'
    AND CLVR_ACCOUNT_MBR not like 'FX%'
GROUP BY CLVR_CONTRACT_MBR, CLVR_ENTITY_MBR,CLVR_ACCOUNT_MBR;

 commit;
EXCEPTION
WHEN OTHERS THEN
  ROLLBACK;
  error_message :=SUBSTR(SQLERRM, 1, 200);
  GRAIL_DM_PART.PK_EMAIL.proc_email_notify('N', 'PKG_BUDGET_FACT.PROC_LOAD_ER_HYP_STG_1 failed with the error '||SQLERRM, 17 );
  raise_application_error(-20102, 'PKG_BUDGET_FACT.PROC_LOAD_ER_HYP_STG_1 failed '||error_message);
 END PROC_LOAD_ER_HYP_STG_1;
-----------------------------------------------------------
--INSERTING DATA INTO EREPOSIT.ER_HYPERION_STAGE_2
-----------------------------------------------------------
PROCEDURE PROC_LOAD_ER_HYP_STG_2 IS
error_message VARCHAR2 (300);
v_contract_id varchar2(100);
v_client_id integer;
v_source_id varchar2(100);
v_revenue_type_code varchar2(100);
v_transaction_type_code varchar2(100);
v_strStart integer;
v_strEnd integer;
v_strSearch varchar2(10);
v_strClient varchar2(100);
v_contract_decode  varchar2(100);
v_contract_code  varchar2(100);
v_profit_center_code varchar2(100);
v_profit_center VARCHAR2(100):='NULL_VALUE';
cursor stage1_curs is
select * from EREPOSIT.ER_HYPERION_STAGE_1 order by profit_center_dsc;
BEGIN
Delete from  EREPOSIT.ER_HYPERION_STAGE_2;
COMMIT;
begin
for stage1_rec in stage1_curs
loop
/* Extract keys from DSC fields */
v_contract_id := '';
v_client_id :='';
v_source_id :='';
v_revenue_type_code := '';
v_transaction_type_code := '';
v_strStart :=0;
v_strEnd :=0;
v_strSearch :='';
v_strClient := '';
v_contract_decode :='';
v_contract_code :='';
v_profit_center_code := '';
/* Extract Client key */
case
	 when stage1_rec.source_type = 'T' then
	 	  case
		    when (substr(stage1_rec.hyperion_client_name,1,3) = 'All' or
              stage1_rec.hyperion_client_name like 'Strategic%'   or
              stage1_rec.hyperion_client_name like 'New_%' or
			  stage1_rec.hyperion_client_name is NULL or
			  REGEXP_LIKE(stage1_rec.hyperion_client_name,'[:A-Z:]')
              )
               then
			   v_client_id := -2;
			when substr(stage1_rec.hyperion_client_name,1,3) = 'Orp' then
			   v_client_id := -1;
	        else

    --   dbms_output.PUT_LINE(substr(stage1_rec.hyperion_client_name,4,length(stage1_rec.hyperion_client_name)-3));
        --v_Client_id := to_number(substr(stage1_rec.hyperion_client_name,4,length(stage1_rec.hyperion_client_name)-3));
			v_Client_id := to_number(stage1_rec.hyperion_client_name);
	      end case;
	 when stage1_rec.source_type = 'F' then
	   case
       when (substr(stage1_rec.hyperion_client_name,1,3) = 'All' or
             stage1_rec.hyperion_client_name like 'Strategic%'   or
             stage1_rec.hyperion_client_name like 'New_%' or
			  stage1_rec.hyperion_client_name is NULL or
			  REGEXP_LIKE(stage1_rec.hyperion_client_name,'[:A-Z:]') )
        then
			   v_client_id := -2;
	   when substr(stage1_rec.hyperion_client_name,1,3) = 'Orp' then
			   v_client_id := -1;
	        else
--			dbms_output.PUT_LINE(substr(stage1_rec.hyperion_client_name,4,length(stage1_rec.hyperion_client_name)-3));
 --        v_Client_id := to_number(substr(stage1_rec.hyperion_client_name,4,length(stage1_rec.hyperion_client_name)-3));
			v_Client_id := to_number(stage1_rec.hyperion_client_name);
	   end case;
	end case;
	/* Extract Contract Reference */
case
when substr(stage1_rec.contract_dsc,1,3) <> 'CO_' or
     stage1_rec.contract_dsc like '%_NA' or
     stage1_rec.contract_dsc = 'Unallocated' or
     stage1_rec.contract_dsc like '%Orphan%'
     then
	 v_contract_decode :=  '-1';
else
   case
    when instr(stage1_rec.contract_dsc,'YY') > 0
    then
    case
      when length(stage1_rec.contract_dsc) < 20 and not (instr(stage1_rec.contract_dsc,'YY') > 0 and  instr(stage1_rec.contract_dsc,'YY') <=9)
      then
         v_contract_decode := substr(stage1_rec.contract_dsc,instr(stage1_rec.contract_dsc,'_',1)+1,instr(stage1_rec.contract_dsc,'YY',1)- instr(stage1_rec.contract_dsc,'_',1)-1) ;
	  when length(stage1_rec.contract_dsc) < 20 and (instr(stage1_rec.contract_dsc,'YY') > 0 and  instr(stage1_rec.contract_dsc,'YY') <=9)
      then
         v_contract_decode := substr(stage1_rec.contract_dsc,instr(stage1_rec.contract_dsc,'_',1)+1, instr(stage1_rec.contract_dsc,'_',-1)-4);
      else
        case
          when length(stage1_rec.contract_dsc)-length(replace(stage1_rec.contract_dsc,'-'))= 4 then
         v_contract_decode := substr(stage1_rec.contract_dsc,instr(stage1_rec.contract_dsc,'_',1)+1, length(stage1_rec.contract_dsc));
         v_contract_decode := replace(v_contract_decode,'YY');
         v_contract_decode := substr(v_contract_decode,1, instr(v_contract_decode,'-',-1)-1);
         else
         v_contract_decode := substr(stage1_rec.contract_dsc,instr(stage1_rec.contract_dsc,'_',1)+1, length(stage1_rec.contract_dsc));
         if (LENGTH(stage1_rec.contract_dsc)>=22 AND stage1_rec.contract_dsc LIKE '%YY-%') THEN
          v_contract_decode := replace(v_contract_decode,'YY-','-');
         ELSE
           v_contract_decode := replace(v_contract_decode,'YY');
         END IF;
         v_contract_decode := substr(v_contract_decode,1, instr(v_contract_decode,'_',-1)-1);
         if instr(v_contract_decode,'-',-1,1)-
            instr(v_contract_decode,'-',-1,2) = 2 then
         v_contract_decode := substr(v_contract_decode,1,instr(v_contract_decode,'-',-1,2))||
        '0' ||
        substr(v_contract_decode,instr(v_contract_decode,'-',-1,1)-1,length(v_contract_decode)-instr(v_contract_decode,'-',-1,2));
         end if;
        end case;
    end case;
	when length(stage1_rec.contract_dsc) = 9 and substr(stage1_rec.contract_dsc,1,3) = 'CO_' and stage1_rec.contract_dsc not like '%_NA' then
	--changed
	v_contract_decode := replace(stage1_rec.contract_dsc,'CO_');
   else
   -- ReinMex
     v_contract_decode := substr(stage1_rec.contract_dsc,instr(stage1_rec.contract_dsc,'_',1)+1,instr(stage1_rec.contract_dsc,'_',-1,1)- instr(stage1_rec.contract_dsc,'_',1)-1) ;
   end case;
end case;
/* Logic for mapping to Contract Ref goes here */
/* Decode Revenue Type  */
/*
case
     when trim(stage1_rec.Hyperion_Revenue_Type) in ('New-New','New to GC','Strategic Hires and Team Lift-Outs') then
				 v_source_id :='new';
	 when trim(stage1_rec.Hyperion_Revenue_Type) in ('New-Penetration','New-Expanded','New-Unallocated','New to Profit Center') then
				 v_source_id :='penetration';
	 when trim(stage1_rec.Hyperion_Revenue_Type) in ('Accrual','Broker Service Agreement','Fee',
				 'No Claims Bonus','Pay Away','Premium Adjustments','Reinstatement Premium','Standard Contract') then
				 v_source_id := 'renewal';
	else v_source_id :='-1';
 end case;
*/
/* Source Code Determination */
 BEGIN
     SELECT NVL(FK_ER_SOURCE_CODE, -1) INTO v_source_id
     FROM EREPOSIT.ER_BUDGET_REVENUE_TYPE_XREF
     WHERE ER_BUDGET_REVENUE_TYPE_CODE = stage1_rec.Hyperion_Revenue_Type;
 EXCEPTION
     WHEN NO_DATA_FOUND
     THEN v_source_id := -1;
 END;

 /* Revenue Code Determination */
 BEGIN
     SELECT NVL(FK_ER_REVENUE_TYPE_CODE, -1) INTO v_revenue_type_code
     FROM EREPOSIT.ER_BUDGET_REVENUE_TYPE_XREF
     WHERE ER_BUDGET_REVENUE_TYPE_CODE = stage1_rec.Hyperion_Revenue_Type;
 EXCEPTION
     WHEN NO_DATA_FOUND
     THEN v_revenue_type_code := -1;
 END;

 /* Transaction Code Determination */
 BEGIN
     SELECT NVL(FK_ER_TRANSACTION_TYPE_CODE, -1) INTO v_transaction_type_code
     FROM EREPOSIT.ER_BUDGET_REVENUE_TYPE_XREF
     WHERE ER_BUDGET_REVENUE_TYPE_CODE = stage1_rec.Hyperion_Revenue_Type;
 EXCEPTION
     WHEN NO_DATA_FOUND
     THEN v_transaction_type_code := -1;
 END;
 /* Lookup Profit Center Code */

 begin
 select DISTINCT nvl(er_profit_center_code,'-1') into v_profit_center_code
 from ereposit.er_dm_hyp_pc_xref
 where trim(er_hyperion_pc_dsc) = trim(stage1_rec.profit_center_dsc) AND
       NVL(ACTIVE_IND, 'N') = 'Y';

 exception
 		  when NO_DATA_FOUND then
		  v_profit_center_code := '-1';
If (v_profit_center<>stage1_rec.profit_center_dsc) then
GRAIL_DM_PART.PK_EMAIL.proc_email_notify('N', 'Profit center code not available in the EREPOSIT.ER_DM_HYP_PC_XREF table for '||stage1_rec.profit_center_dsc, 17 );
v_profit_center:=stage1_rec.profit_center_dsc;
end if;
 end;

insert into EREPOSIT.ER_HYPERION_STAGE_2
(
ER_HYPERION_STAGE_2_ID,
PROFIT_CENTER_DSC,
PROFIT_CENTER_CODE,
HYPERION_CLIENT_NAME,
CLIENT_CODE,
CONTRACT_DSC,
CONTRACT_DECODE,
CONTRACT_CODE,
REVENUE_TYPE,
SOURCE_ID,
JAN_BUDGET_AMT_USD,
FEB_BUDGET_AMT_USD,
MAR_BUDGET_AMT_USD,
APR_BUDGET_AMT_USD,
MAY_BUDGET_AMT_USD,
JUN_BUDGET_AMT_USD,
JUL_BUDGET_AMT_USD,
AUG_BUDGET_AMT_USD,
SEP_BUDGET_AMT_USD,
OCT_BUDGET_AMT_USD,
NOV_BUDGET_AMT_USD,
DEC_BUDGET_AMT_USD,
TRANSACTION_TYPE
)
values
(
EREPOSIT.ER_HYPERION_STAGE_2_ID_SEQ.nextval,
replace(stage1_rec.PROFIT_CENTER_DSC,'~',','),
v_profit_center_code,
replace(stage1_rec.HYPERION_CLIENT_NAME,'~',','),
v_client_id,
stage1_rec.CONTRACT_DSC,
v_contract_decode,
v_contract_code,
--v_source_id,
v_revenue_type_code,
v_source_id,
stage1_rec.JAN_BUDGET_AMT_USD,
stage1_rec.FEB_BUDGET_AMT_USD,
stage1_rec.MAR_BUDGET_AMT_USD,
stage1_rec.APR_BUDGET_AMT_USD,
stage1_rec.MAY_BUDGET_AMT_USD,
stage1_rec.JUN_BUDGET_AMT_USD,
stage1_rec.JUL_BUDGET_AMT_USD,
stage1_rec.AUG_BUDGET_AMT_USD,
stage1_rec.SEP_BUDGET_AMT_USD,
stage1_rec.OCT_BUDGET_AMT_USD,
stage1_rec.NOV_BUDGET_AMT_USD,
stage1_rec.DEC_BUDGET_AMT_USD,
v_transaction_type_code
);
commit;
end loop;
commit;
end;
EXCEPTION
WHEN OTHERS THEN
  ROLLBACK;

  error_message :=SUBSTR(SQLERRM, 1, 200);
  dbms_output.PUT_LINE(error_message );
  GRAIL_DM_PART.PK_EMAIL.proc_email_notify('N', 'PKG_BUDGET_FACT.PROC_LOAD_ER_HYP_STG_2 failed with the error '||SQLERRM, 17 );
  raise_application_error(-20102, 'PKG_BUDGET_FACT.PROC_LOAD_ER_HYP_STG_2 failed '||error_message);
END PROC_LOAD_ER_HYP_STG_2;
-----------------------------------
--INSERTING DATA INTO REVENUE_DM.BUDGET_FACT
-----------------------------------
PROCEDURE PROC_BUDGET_FACT_BLD(P_YEAR NUMBER) IS
error_message VARCHAR2 (300);
cursor ods_curs is
select * from ereposit.ER_HYPERION_STAGE_2 h2
where REGEXP_LIKE(h2.client_code, '[[:digit:]]')
and h2.CLIENT_CODE is not null;
v_budget_version_dim_id number;
v_source_dim_id number;
v_revenue_type_id VARCHAR2(10);
v_transaction_dim_id NUMBER;
v_revenue_period_dim_id number;
v_cedent_dim_id number;
v_contract_dim_id number;
v_profit_center_id number;
v_amt number;
v_commit_ctr number;
v_sql_text varchar2(1000);
begin
v_commit_ctr := 0;
v_sql_text := 'delete from BUDGET_FACT where round(revenue_date_dim_id/10000,0) = '|| to_char(p_year);
execute immediate v_sql_text;
for ods_rec in ods_curs loop
/* Calc Contract */
for i in  1..12 loop
v_commit_ctr := v_commit_ctr + 1;
v_amt := case
			 when i = 1  then ods_rec.JAN_BUDGET_AMT_USD
			 when i = 2  then ods_rec.FEB_BUDGET_AMT_USD
			 when i = 3  then ods_rec.MAR_BUDGET_AMT_USD
			 when i = 4  then ods_rec.APR_BUDGET_AMT_USD
			 when i = 5  then ods_rec.MAY_BUDGET_AMT_USD
			 when i = 6  then ods_rec.JUN_BUDGET_AMT_USD
			 when i = 7  then ods_rec.JUL_BUDGET_AMT_USD
			 when i = 8  then ods_rec.AUG_BUDGET_AMT_USD
			 when i = 9  then ods_rec.SEP_BUDGET_AMT_USD
			 when i = 10 then ods_rec.OCT_BUDGET_AMT_USD
			 when i = 11 then ods_rec.NOV_BUDGET_AMT_USD
			 when i = 12 then ods_rec.DEC_BUDGET_AMT_USD
		end;
/* Dont store zero values */
if v_amt <> 0 then
/* Get SOURCE ID */
begin
  select source_dim_id
	   into v_source_dim_id
	   from source_dim sd
	   where ods_rec.source_id = sd.source_code;
exception
  when NO_DATA_FOUND then
    v_source_dim_id := -1;
end;

/* Get REVENUE TYPE ID */
BEGIN
  SELECT TO_CHAR(REVENUE_TYPE_ID) INTO v_revenue_type_id
  FROM REVENUE_DM.REVENUE_TYPE_DIM
  WHERE REVENUE_TYPE_CODE = ods_rec.REVENUE_TYPE;
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    v_revenue_type_id := -1;
END;

/* Get TRANSACTION DIM ID */
BEGIN
  SELECT TRANSACTION_DIM_ID INTO v_transaction_dim_id
  FROM REVENUE_DM.TRANSACTION_DIM
  WHERE TRANSACTION_TYPE_CODE = ods_rec.TRANSACTION_TYPE;
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    v_transaction_dim_id := -1;
END;

/* Get Period ID
   Set to  first day of the period */
   v_revenue_period_dim_id := (p_year * 10000) + (i * 100) + 1;
/* Get Cedent ID */
Case when ods_rec.contract_dsc like 'CO_%_NA' then
	begin
		select tp_dim_id
	   into v_cedent_dim_id
	   from trading_partner_dim tp
	   where ods_rec.client_code = tp.tp_id;
	exception
		 when NO_DATA_FOUND then
		 v_cedent_dim_id := -1;
		 when TOO_MANY_ROWS then
		select max(tp_dim_id)
	   into v_cedent_dim_id
	   from trading_partner_dim tp
	   where tp.tp_id=ods_rec.client_code;
	end;
when ods_rec.contract_dsc like 'CL_%_NA' then
	begin
		select tp_dim_id
	   into v_cedent_dim_id
	   from trading_partner_dim tp
	   where tp.tp_ult_parent_id =ods_rec.client_code;
	exception
		 when NO_DATA_FOUND then
		  v_cedent_dim_id := -1;
		 when TOO_MANY_ROWS then
		  select max(tp_dim_id)
		   into v_cedent_dim_id
		   from trading_partner_dim tp
		   where tp.tp_ult_parent_id =ods_rec.client_code;
	end;
else
	begin
		select tp_dim_id
	   into v_cedent_dim_id
	   from trading_partner_dim tp
	   where ods_rec.client_code = tp.tp_id;
	exception
		when NO_DATA_FOUND then
		v_cedent_dim_id := -1;
		when TOO_MANY_ROWS then
		select max(tp_dim_id)
	   into v_cedent_dim_id
	   from trading_partner_dim tp
	   where tp.tp_id=ods_rec.client_code;
	end;
end case;

/* Get Profit Center ID */
begin
select 	 profit_center_dim_id
    into
	v_profit_center_id
	from profit_center_dim pc
	where  pc.PROFIT_CODE = ods_rec.profit_center_code;
exception
 when NO_DATA_FOUND then
  v_profit_center_id := -1;
 end;
/* Get Version ID */
v_budget_version_dim_id := -1;
insert into BUDGET_FACT
(
  BUDGET_FACT_ID ,
  CONTRACT_DIM_ID ,
  SOURCE_DIM_ID ,
  REVENUE_DATE_DIM_ID ,
  CEDENT_DIM_ID ,
  PROFIT_CENTER_DIM_ID,
  BUDGET_VERSION_DIM_ID ,
  BUDGET_AMT_USD ,
  ER_HYPERION_STAGE_2_ID,
  CREATED_DT ,
  UPDATE_DT,
  REVENUE_TYPE_ID,
  TRANSACTION_DIM_ID
)
values
(
BUDGET_FACT_ID_SEQ.nextval,
-1 ,
v_source_dim_id,
v_revenue_period_dim_id,
v_cedent_dim_id,
v_profit_center_id,
v_budget_version_dim_id,
v_amt,
ods_rec.ER_HYPERION_STAGE_2_ID,
sysdate,
sysdate,
v_revenue_type_id,
v_transaction_dim_id
);
if v_commit_ctr > 1000 then
   commit;
   v_commit_ctr := 0;
end if;
end if;
end loop;
end loop;
commit;
EXCEPTION
	WHEN OTHERS THEN
		ROLLBACK;
COMMIT;
  error_message :=SUBSTR(SQLERRM, 1, 200);
  GRAIL_DM_PART.PK_EMAIL.proc_email_notify('N', 'PKG_BUDGET_FACT.PROC_BUDGET_FACT_BLD failed with the error '||SQLERRM, 17 );
  raise_application_error(-20102, 'PKG_BUDGET_FACT.PROC_BUDGET_FACT_BLD failed '||error_message);
END PROC_BUDGET_FACT_BLD;
-----------------------------------
--Mapping contract_dim_ids
-----------------------------------
PROCEDURE PROC_MAP_CON_DIM_ID IS
error_message VARCHAR2 (300);
CURSOR budget
  IS
   /* SELECT
      h2.contract_dsc,
      h2.contract_decode,
      h2.source_id,
      h2.REVENUE_TYPE,
      h2.CONTRACT_CODE,
      h2.CLIENT_CODE,
      h2.PROFIT_CENTER_CODE,
      h2.HYPERION_CLIENT_NAME,
      h2.profit_center_dsc,
      h2.er_hyperion_stage_2_id,
      xr.er_profit_center_code,
      xr.er_profit_center_dsc
    FROM
      EREPOSIT.ER_HYPERION_STAGE_2 h2,
      EREPOSIT.er_dm_hyp_pc_xref xr
    WHERE
        h2.profit_center_dsc=xr.er_hyperion_pc_dsc
       --and xr.er_profit_center_code=axr.profit_code
    AND h2.contract_dsc NOT LIKE '%_NA'
    AND h2.contract_dsc NOT LIKE 'CL_%'
    AND h2.contract_dsc NOT LIKE 'Client%'
    AND h2.contract_dsc NOT LIKE 'Unallocated%'
    and h2.contract_dsc not like 'New_Client%'*/
    SELECT
      h2.contract_dsc,
      h2.contract_decode,
      h2.source_id,
      h2.REVENUE_TYPE,
      h2.CONTRACT_CODE,
      h2.CLIENT_CODE,
      h2.PROFIT_CENTER_CODE,
      h2.HYPERION_CLIENT_NAME,
      h2.profit_center_dsc,
      h2.er_hyperion_stage_2_id,
      xr.er_profit_center_code,
      xr.er_profit_center_dsc
     FROM
      EREPOSIT.ER_HYPERION_STAGE_2 h2,
      EREPOSIT.er_dm_hyp_pc_xref xr,
	  REVENUE_DM.BUDGET_FACT BF
    WHERE

	h2.profit_center_dsc=xr.er_hyperion_pc_dsc
	AND h2.er_hyperion_stage_2_id = bf.er_hyperion_stage_2_id
	--and xr.er_profit_center_code=axr.profit_code
    AND h2.contract_dsc NOT LIKE '%_NA'
    AND h2.contract_dsc NOT LIKE 'CL_%'
    AND h2.contract_dsc NOT LIKE 'Client%'
    AND h2.contract_dsc NOT LIKE 'Unallocated%'
    and h2.contract_dsc not like 'New_Client%'
	and h2.contract_dsc not like 'Not_Available'
	and bf.contract_dim_id = -1;


	 CURSOR get_app_ids (p_contract_dsc VARCHAR)
  IS
    SELECT
       axr.dd_application_id
    FROM
      EREPOSIT.ER_HYPERION_STAGE_2  h2,
      EREPOSIT.er_dm_hyp_pc_xref xr,
      EREPOSIT.er_pc_application_xref axr
    WHERE
      h2.profit_center_dsc      =xr.er_hyperion_pc_dsc
    AND xr.er_profit_center_code=axr.profit_code
    AND h2.contract_dsc         =p_contract_dsc ;
  cursor tie_break (p_contract_ref varchar, p_suffix varchar)
 is
 SELECT
      contract_dim_id
      FROM
        revenue_dm.contract_dim cd
      WHERE
        SUBSTR(cd.contract_ref,1,6)             = p_contract_ref
        AND SUBSTR(cd.contract_ref,7,3 ) =p_suffix
        order by CONTRACT_EFF_DT desc
 ;
 vAppId    NUMBER;
  vAppIdCtr NUMBER;
  /* Assumes only 2 Apps per PC */
  vAppId_1          NUMBER;
  vAppId_2          NUMBER;
  vContractRef      VARCHAR(30);
  v_contract_dim_id NUMBER;
  vCountryCode      VARCHAR(3);
  vOfficeCode       VARCHAR(2);
  vTwinsSuffix      VARCHAR2(100);
  vTwinsSuffixLength number;
BEGIN
    /* Loop throught Budget Contracts */
     FOR budget_rec IN budget
  LOOP
   vAppIdCtr := 0;
    /* check for multiple Application in this PC */
    SELECT
      COUNT(*)
    INTO
      vAppIdCtr
    FROM
      ereposit.er_pc_application_xref axr
    WHERE
      axr.profit_code=budget_rec.er_profit_center_code;
    CASE
    WHEN vAppIdCtr = 1
      THEN
      SELECT
        axr.dd_application_id
      INTO
        vAppId
      FROM
        ereposit.er_pc_application_xref axr
      WHERE
        axr.profit_code=budget_rec.er_profit_center_code;
      ELSE
      OPEN get_app_ids(budget_rec.contract_dsc);
      FETCH
        get_app_ids
      INTO
        vAppId_1 ;
      FETCH
        get_app_ids
      INTO
        vAppId_2 ;
      CLOSE get_app_ids;
--     dbms_output.put_line(budget_rec.contract_dsc);
--      dbms_output.put_line(to_char(vAppId_1)||' - '||to_char(vAppId_2));
      CASE
      WHEN
        (
          vAppId_1 = 13 OR vAppId_2 = 13
        )
        THEN
        IF instr(budget_rec.contract_dsc,'-')= 14 THEN
        vAppId                              := 1;
        ELSE
        vAppId := 13;
      END
      IF;
    WHEN
      (
        vAppId_1 = 14 OR vAppId_2 = 14
      )
      THEN
      IF instr(budget_rec.contract_dsc,'-')= 14 THEN
      vAppId                              := 1;
      ELSE
      vAppId := 14;
    END
    IF;
	WHEN
	 (vAppId_1 = 1 AND vAppId_2 = 1) THEN
	vAppId := 1;
	WHEN
	(vAppId_1 = 10 AND vAppId_2 = 10) THEN
	vAppId := 10;
	ELSE
	dbms_output.put_line('vAppId :' || vAppId);
	/*IF instr(budget_rec.contract_dsc,'-')= 14 THEN
      vAppId                              := 1;
      ELSE
      vAppId := 10;
    END
    IF;
	*/
  END
  CASE;
END
CASE;
CASE WHEN vAppId = 1 THEN
-- Old expressing to find the contract_ref value
/*  IF LENGTH(budget_rec.contract_decode) = 17 THEN
  vContractRef                         :=
  SUBSTR(budget_rec.contract_decode,1,9)
  ||'0'
  || SUBSTR(budget_rec.contract_decode,10,8);
  ELSE
  vContractref := budget_rec.contract_decode;
  END
IF;
  */
  -- New expresson to find the contract_ref value
		IF LENGTH(budget_rec.contract_decode) = 17 AND (budget_rec.contract_decode like '%-%') THEN
			vContractRef   :=	  SUBSTR(budget_rec.contract_decode,1,9) ||'0'|| SUBSTR(budget_rec.contract_decode,10,8);
		ELSIF
				 length(budget_rec.contract_decode) in (13,14,15) AND (budget_rec.contract_decode like '%-%') THEN
				vContractRef :=substr(budget_rec.contract_decode,1,9)||(case when length(substr(budget_rec.contract_decode,10,instr(substr(budget_rec.contract_decode,10,8),'-')-1))=3 then substr(budget_rec.contract_decode,10,instr(substr(	budget_rec.contract_decode,10,8),'-')-1) else '0'||substr(budget_rec.contract_decode,10,instr(substr(budget_rec.contract_decode,10,8),'-')-1) end)||'-'||(case when length(substr(budget_rec.contract_decode,instr(budget_rec.contract_decode,'-',-1)+1,2))=2 then substr(budget_rec.contract_decode,instr(budget_rec.contract_decode,'-',-1)+1,2) else '0'||substr(budget_rec.contract_decode,instr(budget_rec.contract_decode,'-',-1)+1,1) end)||'-00';
		ELSE
				vContractref := budget_rec.contract_decode;
		END IF;
		if regexp_like(budget_rec.contract_dsc,'.._........YY-..-..-..-._...') then
			vContractRef := substr(budget_rec.contract_decode,1,13)||substr(budget_rec.contract_dsc,instr(budget_rec.contract_dsc,'_',-1)-1,1)||
			substr(budget_rec.contract_decode,15,3);
			vContractRef := substr(vContractRef,1,9)||'0'||substr(vContractRef,10,8);
		end if;
		BEGIN
			SELECT	MAX(cd.contract_dim_id)  INTO v_contract_dim_id  FROM revenue_dm.contract_dim cd
			WHERE trim(cd.contract_ref)   =vContractref
			AND cd.under_writing_year =( SELECT MAX(c.under_writing_year)  FROM revenue_dm.contract_dim c  WHERE c.contract_ref = vContractref	);
		EXCEPTION
			WHEN NO_DATA_FOUND THEN
			  v_contract_dim_id := -1;
			WHEN TOO_MANY_ROWS THEN
			  v_contract_dim_id := -2;
		END;
WHEN vAppId = 10 THEN
	  CASE  WHEN
		  ( SUBSTR(budget_rec.contract_dsc,instr(budget_rec.contract_dsc,'_',-1)-2,2) = 'YY' OR
			SUBSTR(budget_rec.contract_dsc,instr(budget_rec.contract_dsc,'_',-1)-3,2) = 'YY'  OR
			SUBSTR(budget_rec.contract_dsc,instr(budget_rec.contract_dsc,'_',-1)-4,2) = 'YY' OR
			SUBSTR(budget_rec.contract_dsc,instr(budget_rec.contract_dsc,'_',-1)-5,2) = 'YY' OR
			SUBSTR(budget_rec.contract_dsc,instr(budget_rec.contract_dsc,'_',-1)-6,2) = 'YY'  OR
			SUBSTR(budget_rec.contract_dsc,instr(budget_rec.contract_dsc,'_',-1)-7,2) = 'YY'  OR
			SUBSTR(budget_rec.contract_dsc,instr(budget_rec.contract_dsc,'_',-1)-8,2) = 'YY')
THEN
			vContractRef := budget_rec.contract_decode;
			BEGIN
			  SELECT cd.contract_dim_id  INTO v_contract_dim_id  FROM revenue_dm.contract_dim cd
			  WHERE	SUBSTR(cd.contract_ref,1,6)             = vContractref
			  AND extract(YEAR FROM cd.CONTRACT_EFF_DT) =
				(
				  SELECT
					MAX(extract(YEAR FROM c.CONTRACT_EFF_DT))
				  FROM
					revenue_dm.contract_dim c
				  WHERE
					SUBSTR(c.contract_ref,1,6) =vContractref
				)
			  AND SUBSTR(cd.contract_ref,7,10 ) =
				(
				  SELECT
					MIN(SUBSTR(c.contract_ref,7,10))
				  FROM
					revenue_dm.contract_dim c
				  WHERE
					SUBSTR(c.contract_ref,1,6) =vContractref
					AND extract(YEAR FROM c.CONTRACT_EFF_DT) =
				(
				  SELECT
					MAX(extract(YEAR FROM c.CONTRACT_EFF_DT))
				  FROM
					revenue_dm.contract_dim c
				  WHERE
					SUBSTR(c.contract_ref,1,6) =vContractref
				)
				);
			EXCEPTION
			WHEN NO_DATA_FOUND THEN
			  v_contract_dim_id := -1;
			WHEN TOO_MANY_ROWS THEN
			  v_contract_dim_id := -2;
			END    ;
		ELSE
		vContractRef := budget_rec.contract_decode;
		vTwinsSuffixLength :=
		instr(budget_rec.contract_dsc,'_',-1) - instr(budget_rec.contract_dsc,'YY') - 2;
	  --  vTwinsSuffix := SUBSTR(budget_rec.contract_dsc,instr(
	  --  budget_rec.contract_dsc,'_',-1)-1*vTwinsSuffixLength,vTwinsSuffixLength) ;
		 case
			when vTwinsSuffixLength = 1 then
				   vTwinsSuffix := '00'||substr(budget_rec.contract_dsc,instr(budget_rec.contract_dsc,'YY')+2,vTwinsSuffixLength);
			when vTwinsSuffixLength = 2 then
				   vTwinsSuffix := '0'||substr(budget_rec.contract_dsc,instr(budget_rec.contract_dsc,'YY')+2,vTwinsSuffixLength);
			ELSE
			  vTwinsSuffix := substr(budget_rec.contract_dsc,instr(budget_rec.contract_dsc,'YY')+2,vTwinsSuffixLength);
		end case;
     BEGIN
--        IF VcONTRACTrEF = 'MRG005' THEN
--       DBMS_OUTPUT.PUT_LINE(budget_rec.contract_dsc);
--       DBMS_OUTPUT.PUT_LINE(vTwinsSuffix);
--       DBMS_OUTPUT.PUT_LINE(to_char(vTwinsSuffixLength));
--       end if;
      SELECT
        cd.contract_dim_id
      INTO
        v_contract_dim_id
      FROM
        revenue_dm.contract_dim cd
      WHERE
        SUBSTR(cd.contract_ref,1,6)             = vContractref
      AND SUBSTR(cd.contract_ref,7,3 ) =vTwinsSuffix;
--       IF VcONTRACTrEF = 'MRG005' THEN
--       DBMS_OUTPUT.PUT_LINE(budget_rec.contract_dsc);
--       DBMS_OUTPUT.PUT_LINE(vTwinsSuffix);
--       DBMS_OUTPUT.PUT_LINE(to_char(vTwinsSuffixLength));
--       end if;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
      v_contract_dim_id := -1;
    WHEN TOO_MANY_ROWS THEN
      open tie_break(vContractRef,vTwinsSuffix);
      fetch tie_break into v_contract_dim_id;
      close tie_break;
    END ;
  END CASE;
WHEN vAppId    = 13 THEN
  vOfficeCode := SUBSTR(budget_rec.contract_dsc,instr(budget_rec.contract_dsc,
  '_',-1,2)+1,2);
 -- dbms_output.put_line( vOfficeCode );
  CASE
  WHEN vOfficeCode = '81' THEN
    vCountryCode  := 'ARG';
  WHEN vOfficeCode = '82' THEN
    vCountryCode  := 'CHL';
  WHEN vOfficeCode = '88' THEN
    vCountryCode  := 'VEN';
  WHEN vOfficeCode = '93' THEN
    vCountryCode  := 'COL';
  WHEN vOfficeCode = '94' THEN
    vCountryCode  := 'DOM';
  WHEN vOfficeCode = '95' THEN
    vCountryCode  := 'MIA';
  WHEN vOfficeCode = '96' THEN
    vCountryCode  := 'PER';
  WHEN vOfficeCode = '97' THEN
    vCountryCode  := '';
  WHEN vOfficeCode = '98' THEN
    vCountryCode  := 'RIO';
    ELSE
    dbms_output.put_line(vOfficeCode);
    dbms_output.put_line(budget_rec.contract_dsc);
  END
  CASE;
  BEGIN
  vContractRef := substr(budget_rec.contract_decode,1,length(budget_rec.contract_decode)-3)||
  case when vOfficeCode in (82,97) then '' else '-' end||vCountryCode;
     SELECT
      cd.contract_dim_id
    INTO
      v_contract_dim_id
    FROM
      revenue_dm.contract_dim cd
    WHERE
      cd.contract_ref                         = vContractref
    AND extract(YEAR FROM cd.CONTRACT_EFF_DT) =
      (
        SELECT
          MAX(extract(YEAR FROM cd.CONTRACT_EFF_DT))
        FROM
          revenue_dm.contract_dim c
        WHERE
          c.contract_ref =vContractref
      );
  EXCEPTION
   WHEN NO_DATA_FOUND THEN
    v_contract_dim_id := -1;
  WHEN TOO_MANY_ROWS THEN
    v_contract_dim_id := -2;
	WHEN OTHERS THEN
	  v_contract_dim_id := -1;
  END ;
 WHEN  vAppId    = 14 THEN
 	vContractRef := budget_rec.contract_decode;
			BEGIN
				SELECT MAX(cd.contract_dim_id)  INTO v_contract_dim_id  FROM revenue_dm.contract_dim cd
				  WHERE	budget_ref           = vContractRef
				  AND extract(YEAR FROM cd.CONTRACT_EFF_DT) =
					(
					  SELECT
						MAX(extract(YEAR FROM c.CONTRACT_EFF_DT))
					  FROM
						revenue_dm.contract_dim c
					  WHERE
						budget_ref =vContractRef
					);
			EXCEPTION
			WHEN NO_DATA_FOUND THEN
			  v_contract_dim_id := -1;
			WHEN TOO_MANY_ROWS THEN
			  v_contract_dim_id := -2;
			END    ;
  ELSE
  vContractRef := budget_rec.contract_decode;
  BEGIN
    SELECT
      cd.contract_dim_id
    INTO
      v_contract_dim_id
    FROM
      revenue_dm.contract_dim cd
    WHERE
      cd.contract_ref                         = vContractref
    AND extract(YEAR FROM cd.CONTRACT_EFF_DT) =
      (
        SELECT
          MAX(extract(YEAR FROM cd.CONTRACT_EFF_DT))
        FROM
          revenue_dm.contract_dim c
        WHERE
          c.contract_ref =vContractref
      );
  EXCEPTION
  WHEN NO_DATA_FOUND THEN
    v_contract_dim_id := -1;
  WHEN TOO_MANY_ROWS THEN
    v_contract_dim_id := -2;
  END
  ;
END
CASE;
--IF ( vAppId    = 1 AND (v_contract_dim_id = -1 or v_contract_dim_id is null)) THEN
IF   (v_contract_dim_id = -1 or v_contract_dim_id is null) THEN

	 vContractRef := budget_rec.contract_decode;

      IF (length(budget_rec.contract_decode) in (12,13,14,15) AND (budget_rec.contract_decode like '%-%')) THEN
				vContractRef :=substr(budget_rec.contract_decode,1,9)||(case when length(substr(budget_rec.contract_decode,10,instr(substr(budget_rec.contract_decode,10,8),'-')-1))=3 then substr(budget_rec.contract_decode,10,instr(substr(	budget_rec.contract_decode,10,8),'-')-1) else '0'||substr(budget_rec.contract_decode,10,instr(substr(budget_rec.contract_decode,10,8),'-')-1) end);
        BEGIN

      SELECT	MAX(cd.contract_dim_id) INTO v_contract_dim_id   FROM revenue_dm.contract_dim cd
          WHERE trim(cd.BUDGET_REF)   = vContractref AND (CD.SECTION_NUMBER)=  substr(budget_rec.contract_decode,instr(budget_rec.contract_decode,'-',-1)+1,2)
          AND cd.under_writing_year =( SELECT MAX(c.under_writing_year)  FROM contract_dim c  WHERE trim(C.BUDGET_REF)   = vContractref AND (C.SECTION_NUMBER)=  substr(budget_rec.contract_decode,instr(budget_rec.contract_decode,'-',-1)+1,2));

        IF ( v_contract_dim_id = -1 OR v_contract_dim_id is null) THEN
               SELECT	MAX(cd.contract_dim_id) INTO v_contract_dim_id   FROM revenue_dm.contract_dim cd
          WHERE trim(cd.BUDGET_REF)   = budget_rec.contract_decode
          AND cd.under_writing_year =( SELECT MAX(c.under_writing_year)  FROM contract_dim c  WHERE trim(C.BUDGET_REF)   = budget_rec.contract_decode);
        END IF;

        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            v_contract_dim_id := -1;
          WHEN TOO_MANY_ROWS THEN
            v_contract_dim_id := -2;
        END;

      ELSE
      BEGIN
				SELECT MAX(cd.contract_dim_id)  INTO v_contract_dim_id  FROM revenue_dm.contract_dim cd
				  WHERE	budget_ref           = vContractRef
				  AND extract(YEAR FROM cd.CONTRACT_EFF_DT) =
					(
					  SELECT
						MAX(extract(YEAR FROM c.CONTRACT_EFF_DT))
					  FROM
						contract_dim c
					  WHERE
						budget_ref =vContractRef
					);
			EXCEPTION
			WHEN NO_DATA_FOUND THEN
			  v_contract_dim_id := -1;
			WHEN TOO_MANY_ROWS THEN
			  v_contract_dim_id := -2;
			END    ;
     END IF;
    v_contract_dim_id:=nvl(v_contract_dim_id,-1);

 END IF;

update BUDGET_FACT bf
set bf.contract_dim_id = v_contract_dim_id
 where budget_rec.er_hyperion_stage_2_id = bf.er_hyperion_stage_2_id;
    commit;
END
LOOP;
commit;
 update BUDGET_FACT bf set bf.contract_dim_id = -1 where bf.contract_dim_id is null;
 commit;
EXCEPTION
	WHEN OTHERS THEN
		ROLLBACK;
  error_message :=SUBSTR(SQLERRM, 1, 200);
  GRAIL_DM_PART.PK_EMAIL.proc_email_notify('N', 'PKG_BUDGET_FACT.PROC_MAP_CON_DIM_ID failed with the error '||SQLERRM, 17 );
  raise_application_error(-20102, 'PKG_BUDGET_FACT.PROC_MAP_CON_DIM_ID failed '||error_message);
END PROC_MAP_CON_DIM_ID;

-- stt 56240. Added this procedure to populate CLASS_OF_BUSINESS column
PROCEDURE PROC_UPD_CLASS_OF_BUSNS(P_YEAR  NUMBER) IS
error_message VARCHAR2 (300);

BEGIN

   UPDATE revenue_dm.budget_fact bf_upd
  SET bf_upd.business_classification_dim_id =
    (SELECT bdim.business_classification_dim_id
       FROM revenue_dm.contract_dim cdim         ,
      ereposit.er_Contract_section_class csc     ,
      ereposit.er_Contract c                     ,
      revenue_dm.budget_fact bf                  ,
      ereposit.er_Contract_section cs            ,
      REVENUE_DM.BUSINESS_CLASSIFICATION_DIM bdim,
      (SELECT gcbus.SHORT_CODE SHORT_CODE_FROM   ,
        revbus.SHORT_CODE SHORT_CODE_TO
         FROM ERCROSS.ER_GC_CLASS_OF_BUSINESS gcbus,
        ERCROSS.ER_REVENUE_DM_BUSINESS_CLASS revbus,
        ERCROSS.ER_CROSS_REFERENCE cref            ,
        ERCROSS.ER_TBL_NAME tbl_from               ,
        ERCROSS.ER_TBL_NAME tbl_to
        WHERE tbl_from.TBL_NAME_PHYSICAL = 'ER_GC_CLASS_OF_BUSINESS'
      AND tbl_to.TBL_NAME_PHYSICAL       = 'ER_REVENUE_DM_BUSINESS_CLASS'
      AND cref.FK_TBL_NAME_ID_FROM       = tbl_from.TBL_NAME_ID
      AND cref.FK_TBL_NAME_ID_TO         = tbl_to.TBL_NAME_ID
      AND cref.FK_TBL_CODE_ID_FROM       = gcbus.TBL_CODE_ID
      AND cref.FK_TBL_CODE_ID_TO         = revbus.TBL_CODE_ID
      ) xref
      WHERE bf.contract_dim_id        = cdim.contract_dim_id
    AND csc.fk_er_contract_Section_id = cs.er_contract_section_id
    AND cs.er_contract_section_id     = cdim.dd_er_section_id
    AND c.er_contract_id              = cdim.er_contract_id
    AND c.er_contract_id              = cs.fk_er_contract_id
    AND xref.SHORT_CODE_FROM          = csc.FK_GC_CLASS_OF_BUSINESS
    AND xref.SHORT_CODE_TO            = bdim.busness_class_code
    AND bf_upd.budget_fact_id         = bf.budget_fact_id
    AND cdim.contract_dim_id         != -1
    AND cdim.contract_dim_id         != -2
    AND round(bf.revenue_date_dim_id/10000, 0) = P_YEAR
    )
    WHERE EXISTS
    (SELECT 1
       FROM revenue_dm.contract_dim cdim         ,
      ereposit.er_Contract_section_class csc     ,
      ereposit.er_Contract c                     ,
      revenue_dm.budget_fact bf                  ,
      ereposit.er_Contract_section cs            ,
      REVENUE_DM.BUSINESS_CLASSIFICATION_DIM bdim,
      (SELECT gcbus.SHORT_CODE SHORT_CODE_FROM   ,
        revbus.SHORT_CODE SHORT_CODE_TO
         FROM ERCROSS.ER_GC_CLASS_OF_BUSINESS gcbus,
        ERCROSS.ER_REVENUE_DM_BUSINESS_CLASS revbus,
        ERCROSS.ER_CROSS_REFERENCE cref            ,
        ERCROSS.ER_TBL_NAME tbl_from               ,
        ERCROSS.ER_TBL_NAME tbl_to
        WHERE tbl_from.TBL_NAME_PHYSICAL = 'ER_GC_CLASS_OF_BUSINESS'
      AND tbl_to.TBL_NAME_PHYSICAL       = 'ER_REVENUE_DM_BUSINESS_CLASS'
      AND cref.FK_TBL_NAME_ID_FROM       = tbl_from.TBL_NAME_ID
      AND cref.FK_TBL_NAME_ID_TO         = tbl_to.TBL_NAME_ID
      AND cref.FK_TBL_CODE_ID_FROM       = gcbus.TBL_CODE_ID
      AND cref.FK_TBL_CODE_ID_TO         = revbus.TBL_CODE_ID
      ) xref
      WHERE bf.contract_dim_id        = cdim.contract_dim_id
    AND csc.fk_er_contract_Section_id = cs.er_contract_section_id
    AND cs.er_contract_section_id     = cdim.dd_er_section_id
    AND c.er_contract_id              = cdim.er_contract_id
    AND c.er_contract_id              = cs.fk_er_contract_id
    AND xref.SHORT_CODE_FROM          = csc.FK_GC_CLASS_OF_BUSINESS
    AND xref.SHORT_CODE_TO            = bdim.busness_class_code
    AND bf_upd.budget_fact_id         = bf.budget_fact_id
    AND cdim.contract_dim_id         != -1
    AND cdim.contract_dim_id         != -2
    AND round(bf.revenue_date_dim_id/10000, 0) = P_YEAR
    ) ;

   update revenue_dm.budget_fact bf_upd
  set bf_upd.business_classification_dim_id =
  case when bf_upd.contract_dim_id = -1 then -1
       when bf_upd.contract_dim_id = -2 then -2
       else NVL(bf_upd.business_classification_dim_id, -1)
       end;

  COMMIT;


EXCEPTION
	WHEN OTHERS THEN
		ROLLBACK;
  error_message := SUBSTR(SQLERRM, 1, 200);
  GRAIL_DM_PART.PK_EMAIL.proc_email_notify('N', 'PKG_BUDGET_FACT.PROC_UPD_CLASS_OF_BUSNS failed with the error '||SQLERRM, 17 );
  raise_application_error(-20102, 'PKG_BUDGET_FACT.PROC_UPD_CLASS_OF_BUSNS failed '||error_message);
END PROC_UPD_CLASS_OF_BUSNS;


PROCEDURE PROC_UPD_CNT_CLASS_DIM(P_YEAR NUMBER)
IS
commit_interval NUMBER;
cnt NUMBER;
BEGIN
FOR i in (SELECT f.BUDGET_FACT_ID,
                 f.CONTRACT_DIM_ID,
                 ecs.ER_CONTRACT_SECTION_ID,
                 ecs.FK_GC_COVER_TYPE,
                 xref.SHORT_CODE_FROM,
                 xref.SHORT_CODE_TO,
                 NVL(ccdim.CONTRACT_CLASSIFICATION_DIM_ID, -2) CONTRACT_CLASSIFICATION_DIM_ID
          FROM REVENUE_DM.BUDGET_FACT f
          LEFT OUTER JOIN REVENUE_DM.CONTRACT_DIM cdim ON (cdim.CONTRACT_DIM_ID = f.CONTRACT_DIM_ID)
          LEFT OUTER JOIN EREPOSIT.ER_CONTRACT_SECTION ecs ON (ecs.ER_CONTRACT_SECTION_ID = cdim.DD_ER_SECTION_ID)
          LEFT OUTER JOIN (SELECT gccc.SHORT_CODE SHORT_CODE_FROM,
                                  revcc.SHORT_CODE SHORT_CODE_TO
                           FROM ERCROSS.ER_GC_COVER_TYPE gccc,
                                ERCROSS.ER_REVENUE_DM_CONTRACT_SUBTYPE revcc,
                                ERCROSS.ER_CROSS_REFERENCE cref,
                                ERCROSS.ER_TBL_NAME tbl_from,
                                ERCROSS.ER_TBL_NAME tbl_to
                           WHERE tbl_from.TBL_NAME_PHYSICAL = 'ER_GC_COVER_TYPE' AND
                                 tbl_to.TBL_NAME_PHYSICAL  = 'ER_REVENUE_DM_CONTRACT_SUBTYPE' AND
                                 cref.FK_TBL_NAME_ID_FROM = tbl_from.TBL_NAME_ID AND
                                 cref.FK_TBL_NAME_ID_TO = tbl_to.TBL_NAME_ID AND
                                 cref.FK_TBL_CODE_ID_FROM = gccc.TBL_CODE_ID AND
                                 cref.FK_TBL_CODE_ID_TO = revcc.TBL_CODE_ID) xref ON (xref.SHORT_CODE_FROM = ecs.FK_GC_COVER_TYPE)
          LEFT OUTER JOIN REVENUE_DM.CONTRACT_CLASSIFICATION_DIM ccdim ON (ccdim.CONTRACT_SUB_TYPE_CODE = xref.SHORT_CODE_TO)
          WHERE TO_NUMBER(SUBSTR(TO_CHAR(f.REVENUE_DATE_DIM_ID), 1, 4)) = P_YEAR)
LOOP
      UPDATE REVENUE_DM.BUDGET_FACT
      SET CONTRACT_CLASSIFICATION_DIM_ID = i.CONTRACT_CLASSIFICATION_DIM_ID
      WHERE BUDGET_FACT_ID = i.BUDGET_FACT_ID;

      cnt := cnt + 1;
      IF cnt = commit_interval THEN
         COMMIT;
         cnt := 0;
      END IF;
END LOOP;
COMMIT;

EXCEPTION
WHEN OTHERS THEN
     ROLLBACK;
     GRAIL_DM_PART.PK_EMAIL.proc_email_notify('N', 'PKG_BUDGET_FACT.PROC_UPD_CNT_CLASS_DIM failed with the error '||SUBSTR(SQLERRM, 1, 200), 17 );
     raise_application_error(-20102, 'PKG_BUDGET_FACT.PROC_UPD_CNT_CLASS_DIM failed '||SUBSTR(SQLERRM, 1, 200));
END PROC_UPD_CNT_CLASS_DIM;


END PKG_BUDGET_FACT;
/
