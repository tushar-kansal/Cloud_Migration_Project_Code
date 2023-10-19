/*
 
 Dnb Data Replication
 
 Function name: fn_call_audit_table
 
 This function is used to store log or processing status of stored procedure in audit table
 This function is used in below stored procedure 
 -- sp_trade_case
 -- sp_pay_exp
 -- sp_pay_ref
 -- sp_case_rec
 -- sp_pif_directory
 -- sp_country_rec
 -- sp_country_sic_rec
 -- sp_sic_code_rec
 
*/

-------------------------------------------> Creating Function with Parameters <--------------------------------------------------

CREATE
OR REPLACE FUNCTION fn_call_audit_table (
  v_load_id INT,
  v_file_name VARCHAR,
  v_table_name VARCHAR,
  v_total_record_count INTEGER,
  v_insert_count INTEGER,
  v_update_count INTEGER,
  v_delete_count INTEGER,
  v_load_status VARCHAR,
  v_remark TEXT,
  v_proc_start_time TIMESTAMP,
  v_proc_end_time TIMESTAMP
) RETURNS void AS $BODY$

------------------------------> Declaring Variables <------------------------------

DECLARE v_sql TEXT;

------------------------------> Begin Block Start <------------------------------

BEGIN

----------------------------------------> Creating Statement for Inserting data Into Audit Table <----------------------------------------------

	v_sql = format('INSERT INTO audit_table (Load_id,File_name,Table_name,Total_Records_Count,Total_Records_Inserted_Count,
                  Total_Records_Updated_Count,Total_Records_Deleted_Count,Load_status,Remark,event_start_time,event_end_time)
                  VALUES (%L,%L,%L,%L,%L,%L,%L,%L,%L,%L,%L) on conflict (load_id) do update set Total_Records_Inserted_Count = %L,
                  Total_Records_Updated_Count = %L,Total_Records_Deleted_Count = %L,Load_status = %L,Remark = %L,event_start_time = %L,
                  event_end_time = %L', v_load_id, v_file_name, v_table_name, v_total_record_count, v_insert_count, 
                  v_update_count, v_delete_count, v_load_status, v_remark, v_proc_start_time, v_proc_end_time, v_insert_count, 
                  v_update_count, v_delete_count, v_load_status, v_remark, v_proc_start_time, v_proc_end_time);

------------------------------> Executing Statement <------------------------------

  EXECUTE v_sql;

------------------------------> Ending Function <------------------------------

END;

$BODY$
LANGUAGE plpgsql;

-- Function completed