-- <--------------------------------------------------> <---------------------------------------->
/*
 
 Dnb Data Replication
 
 Stored Procedure Name: sp_sic_code_rec
 Staging Table Name: sic_code_rec_stg (Glue will create this table)
 Target Table Name: sic_code_rec
 
 This procedure is used to move data from staging table to target table.
 
 Dependent Objects of this Procedure for Successful Completion:
 -- Staging table [sic_code_rec_stg] (Glue will create this table)
 -- Target table [sic_code_rec]
 -- Audit table [audit_table]
 -- Function [fn_call_audit_table]
 -- Function [fn_is_date]
 -- Sequence number [seq_audit_table_load_id]
 
 Procedure calling command: call sp_sic_code_rec ('full path received from glue job','load type received from glue job');
 
*/
-- <--------------------------------------------------> <---------------------------------------->


-- <--------------------------------------------------> creating procedure with inout parameter <---------------------------------------->

CREATE
OR REPLACE PROCEDURE sp_sic_code_rec (v_full_path VARCHAR, inout v_load_type VARCHAR) LANGUAGE plpgsql AS $STG_TARGET$

-- <--------------------------------------------------> declaring variables <---------------------------------------->

DECLARE 
v_load_id INTEGER = 0;
v_file_name VARCHAR = v_full_path;
v_table_name VARCHAR;
v_total_record_count INTEGER = 0;
v_insert_count INTEGER = 0;
v_update_count INTEGER = 0;
v_delete_count INTEGER = 0;
v_load_status VARCHAR;
v_remark Text = '-';
v_proc_start_time TIMESTAMP;
v_proc_end_time TIMESTAMP;
v_state TEXT;
v_msg TEXT;
v_no_update integer = 0;

-- <--------------------------------------------------> begin block start and store some info to variables <---------------------------------------->

BEGIN

	v_proc_start_time = clock_timestamp();


	select
	last_value into v_load_id
	from
	seq_audit_table_load_id;


	perform setval('seq_audit_table_load_id', (v_load_id + 1));


	v_table_name = 'sic_code_rec';

	-- <--------------------------------------------------> counting total number of records present in staging table <---------------------------------------->

	SELECT
	count(dbkey) INTO v_total_record_count
	FROM
	sic_code_rec_stg;

	-- <--------------------------------------------------> calling fn_call_audit_table function and storing starttime <---------------------------------------->

	perform fn_call_audit_table (
	v_load_id,
	v_file_name,
	v_table_name,
	v_total_record_count,
	v_insert_count,
	v_update_count,
	v_delete_count,
	'Process Started',
	'Process [sp_sic_code_rec] Started Successfully',
	v_proc_start_time,
	v_proc_end_time
	);

	-- <--------------------------------------------------> condition block start and checking load type <---------------------------------------->

	IF lower(v_load_type) = 'full' then

	-- <--------------------------------------------------> condition block start and checking record present in staging table or not <---------------------------------------->

			if v_total_record_count > 0 then

	-- <--------------------------------------------------> checking date either come in proper format or not, if not then we are updating to '1900-01-01' [fixing date issue] <---------------------------------------->

					UPDATE
					sic_code_rec_stg
					SET
					sic_date_last_update = sic_date_last_update_tmp;


					UPDATE
					sic_code_rec_stg
					SET
					sic_date_last_update = CASE
						WHEN length(sic_date_last_update) != 8 THEN '19000101'
						WHEN sic_date_last_update = '00000000' THEN NULL
						WHEN NOT fn_is_date(sic_date_last_update) THEN '19000101'
						ELSE sic_date_last_update
					END;

	-- <--------------------------------------------------> counting update records on based of dbkey value <---------------------------------------->

					SELECT
					count(sic_code_rec_stg.dbkey) INTO v_update_count
					FROM
					sic_code_rec_stg
					JOIN sic_code_rec ON sic_code_rec_stg.dbkey = sic_code_rec.dbkey
					WHERE
					sic_code_rec_stg.dbkey = sic_code_rec.dbkey;

	-- <--------------------------------------------------> Insert & Update Command, Mainframe Sic_code_rec Extract Change Data into Corresponding Target table <---------------------------------------->
					
					INSERT INTO
					sic_code_rec (
						dbkey,
						sic_nbr_sic,
						sic_qty_length_of_sic,
						sic_desc_line_of_business,
						sic_date_last_update
					)
					SELECT
					dbkey,
					sic_nbr_sic,
					sic_qty_length_of_sic,
					sic_desc_line_of_business,
					cast(sic_date_last_update as date)
					FROM
					sic_code_rec_stg ON CONFLICT(dbkey) DO
					UPDATE
					SET
					sic_nbr_sic = Excluded.sic_nbr_sic,
					sic_qty_length_of_sic = Excluded.sic_qty_length_of_sic,
					sic_desc_line_of_business = Excluded.sic_desc_line_of_business,
					sic_date_last_update = Excluded.sic_date_last_update,
					load_time = v_proc_start_time;
					
	-- <--------------------------------------------------> updating load_time column <---------------------------------------->

					UPDATE
					sic_code_rec
					SET
					load_time = v_proc_start_time
					WHERE
					load_time IS NULL;

	-- <--------------------------------------------------> calculating inserted records <---------------------------------------->

					v_insert_count = v_total_record_count - v_update_count;

	-- <--------------------------------------------------> calling fn_call_audit_table function and storing insert and update counts <---------------------------------------->

					perform fn_call_audit_table (
					v_load_id,
					v_file_name,
					v_table_name,
					v_total_record_count,
					v_insert_count,
					v_update_count,
					v_delete_count,
					'Insert and Update',
					'Process Insert and Update Successfully Completed',
					v_proc_start_time,
					v_proc_end_time
					);

	-- <--------------------------------------------------> executing else block when (v_total_record_count>0) condition failed <---------------------------------------->

			ELSE

	-- <--------------------------------------------------> calling fn_call_audit_table function and storing info for no record in staging table <---------------------------------------->

					v_proc_end_time = clock_timestamp();


					perform fn_call_audit_table (
					v_load_id,
					v_file_name,
					v_table_name,
					v_total_record_count,
					v_insert_count,
					v_update_count,
					v_delete_count,
					'Process Ended',
					'There is no Record Found in Staging Table',
					v_proc_start_time,
					v_proc_end_time
					);


					v_no_update = 1;


					v_load_type = 'There is no Record Found in Staging Table';
				
	-- <--------------------------------------------------> closing second condition block <---------------------------------------->

			END IF;			

	-- <--------------------------------------------------> executing else block when (lower(v_load_type) = 'incremental') condition failed <---------------------------------------->

	ELSE

	-- <--------------------------------------------------> calling fn_call_audit_table function and storing invalid parameter info <---------------------------------------->

		v_proc_end_time = clock_timestamp();


		perform fn_call_audit_table (
		v_load_id,
		v_file_name,
		v_table_name,
		v_total_record_count,
		v_insert_count,
		v_update_count,
		v_delete_count,
		'Failure!',
		'Failure! - Invalid Parameters',
		v_proc_start_time,
		v_proc_end_time
		);


		v_no_update = 1;


		v_load_type = 'Failure! - Invalid Parameters';

	-- <--------------------------------------------------> closing first condition block <---------------------------------------->

	END IF;

	-- <--------------------------------------------------> calling fn_call_audit_table function and storing endtime <---------------------------------------->

	if (v_no_update != 1) then 


	v_proc_end_time = clock_timestamp();


	perform fn_call_audit_table (
	v_load_id,
	v_file_name,
	v_table_name,
	v_total_record_count,
	v_insert_count,
	v_update_count,
	v_delete_count,
	'Process Ended',
	'Process [sp_sic_code_rec] Ended Successfully',
	v_proc_start_time,
	v_proc_end_time
	);

	-- <--------------------------------------------------> out parameter message, this message will pass to glue <---------------------------------------->

	v_load_type = concat(
	'Records inserted: ',
	v_insert_count,
	',  Records updated: ',
	v_update_count,
	',  Records deleted: ',
	v_delete_count
	);


	END IF;
	
	-- <--------------------------------------------------> Exception handling <---------------------------------------->

	EXCEPTION 

	-- <--------------------------------------------------> if any type of exception occur this block will capture that exception <---------------------------------------->

		WHEN OTHERS THEN 

	-- <--------------------------------------------------> Getting Diagnostic Data from Postgres <----------------------------------------> 

				get stacked diagnostics
					v_state   = returned_sqlstate,
					v_msg     = message_text;

	-- <--------------------------------------------------> calling fn_call_audit_table function and storing exception message <---------------------------------------->

				v_remark = concat('SQLMESSAGE: ', v_msg, ',  SQLSTATE: ', v_state);


				v_proc_end_time = clock_timestamp();


				perform fn_call_audit_table (
				v_load_id,
				v_file_name,
				v_table_name,
				v_total_record_count,
				0,
				0,
				0,
				'Error Encountered!',
				v_remark,
				v_proc_start_time,
				v_proc_end_time
				);

	-- <--------------------------------------------------> out parameter message, this message will pass to glue <---------------------------------------->

				v_load_type = v_remark;

	-- <--------------------------------------------------> begin block end <---------------------------------------->

END;
$STG_TARGET$;

-- Procedure [sp_sic_code_rec] completed