-- <--------------------------------------------------> <---------------------------------------->
/*
 
 Dnb Data Replication
 
 Stored Procedure Name: sp_trade_case
 Staging Table Name: trade_case_stg (Glue will create this table)
 Target Table Name: trade_case
 
 This procedure is used to move data from staging table to target table.
 
 Dependent Objects of this Procedure for Successful Completion:
 -- Staging table [trade_case_stg] (Glue will create this table)
 -- Target table [trade_case]
 -- Audit table [audit_table]
 -- Function [fn_call_audit_table]
 -- Function [fn_is_date]
 -- Sequence number [seq_audit_table_load_id]
 
 Procedure calling command: call sp_trade_case ('full path received from glue job','load type received from glue job');
 
*/
-- <--------------------------------------------------> <---------------------------------------->


-- <--------------------------------------------------> creating procedure with inout parameter <---------------------------------------->

CREATE
OR REPLACE PROCEDURE sp_trade_case (v_full_path VARCHAR, inout v_load_type VARCHAR) LANGUAGE plpgsql AS $STG_TARGET$

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


	v_table_name = 'trade_case';

	-- <--------------------------------------------------> counting total number of records present in staging table <---------------------------------------->

	SELECT
	count(dbkey) INTO v_total_record_count
	FROM
	trade_case_stg;

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
	'Process [sp_trade_case] Started Successfully',
	v_proc_start_time,
	v_proc_end_time
	);

	-- <--------------------------------------------------> condition block start and checking load type <---------------------------------------->

	IF lower(v_load_type) = 'incremental' then

	-- <--------------------------------------------------> condition block start and checking record present in staging table or not <---------------------------------------->

			if v_total_record_count > 0 then

	-- <--------------------------------------------------> checking date either come in proper format or not, if not then we are updating to '1900-01-01' [fixing date issue] <---------------------------------------->

					UPDATE
					trade_case_stg
					SET
					date_successor_report = date_successor_report_tmp;


					UPDATE
					trade_case_stg
					SET
					date_successor_report = CASE
						WHEN length(date_successor_report) != 8 THEN '19000101'
						WHEN date_successor_report = '00000000' THEN NULL
						WHEN NOT fn_is_date(date_successor_report) THEN '19000101'
						ELSE date_successor_report
					END;

	-- <--------------------------------------------------> counting update records on based of dbkey value <---------------------------------------->

					SELECT
					count(trade_case_stg.dbkey) INTO v_update_count
					FROM
					trade_case_stg
					JOIN trade_case ON trade_case_stg.dbkey = trade_case.dbkey
					WHERE
					trade_case_stg.dbkey = trade_case.dbkey;

	-- <--------------------------------------------------> Insert & Update Command, Mainframe Trade_case Extract Change Data into Corresponding Target table <---------------------------------------->
					
					INSERT INTO
					trade_case (
						dbkey,
						case_duns_number,
						qty_pay_exps,
						qty_pay_refs,
						qty_bank_refs,
						qty_bank_exps,
						last_start_request,
						last_start_complete,
						last_case_update,
						date_successor_report,
						year,
						month,
						current_pay_index_1,
						current_pay_index_2,
						current_pay_index_3,
						current_pay_index_4,
						current_pay_index_5,
						current_pay_index_6,
						current_pay_index_7,
						current_pay_index_8,
						current_pay_index_9,
						current_pay_index_10,
						current_pay_index_11,
						current_pay_index_12,
						prior_pay_index_1,
						prior_pay_index_2,
						prior_pay_index_3,
						prior_pay_index_4,
						prior_pay_index_5,
						prior_pay_index_6,
						prior_pay_index_7,
						prior_pay_index_8,
						prior_pay_index_9,
						prior_pay_index_10,
						prior_pay_index_11,
						prior_pay_index_12,
						case_status,
						reference_request_indicator,
						code_case_country,
						code_pay_summary
					)
					SELECT
					dbkey,
					case_duns_number,
					qty_pay_exps,
					qty_pay_refs,
					qty_bank_refs,
					qty_bank_exps,
					last_start_request,
					last_start_complete,
					last_case_update,
					to_date(date_successor_report:: TEXT, 'YYYYMMDD'),
					year,
					month,
					current_pay_index_1,
					current_pay_index_2,
					current_pay_index_3,
					current_pay_index_4,
					current_pay_index_5,
					current_pay_index_6,
					current_pay_index_7,
					current_pay_index_8,
					current_pay_index_9,
					current_pay_index_10,
					current_pay_index_11,
					current_pay_index_12,
					prior_pay_index_1,
					prior_pay_index_2,
					prior_pay_index_3,
					prior_pay_index_4,
					prior_pay_index_5,
					prior_pay_index_6,
					prior_pay_index_7,
					prior_pay_index_8,
					prior_pay_index_9,
					prior_pay_index_10,
					prior_pay_index_11,
					prior_pay_index_12,
					case_status,
					reference_request_indicator,
					code_case_country,
					code_pay_summary
					FROM
					trade_case_stg ON CONFLICT(dbkey) DO
					UPDATE
					SET
					case_duns_number = Excluded.case_duns_number,
					qty_pay_exps = Excluded.qty_pay_exps,
					qty_pay_refs = Excluded.qty_pay_refs,
					qty_bank_refs = Excluded.qty_bank_refs,
					qty_bank_exps = Excluded.qty_bank_exps,
					last_start_request = Excluded.last_start_request,
					last_start_complete = Excluded.last_start_complete,
					last_case_update = Excluded.last_case_update,
					date_successor_report = Excluded.date_successor_report,
					year = Excluded.year,
					month = Excluded.month,
					current_pay_index_1 = Excluded.current_pay_index_1,
					current_pay_index_2 = Excluded.current_pay_index_2,
					current_pay_index_3 = Excluded.current_pay_index_3,
					current_pay_index_4 = Excluded.current_pay_index_4,
					current_pay_index_5 = Excluded.current_pay_index_5,
					current_pay_index_6 = Excluded.current_pay_index_6,
					current_pay_index_7 = Excluded.current_pay_index_7,
					current_pay_index_8 = Excluded.current_pay_index_8,
					current_pay_index_9 = Excluded.current_pay_index_9,
					current_pay_index_10 = Excluded.current_pay_index_10,
					current_pay_index_11 = Excluded.current_pay_index_11,
					current_pay_index_12 = Excluded.current_pay_index_12,
					prior_pay_index_1 = Excluded.prior_pay_index_1,
					prior_pay_index_2 = Excluded.prior_pay_index_2,
					prior_pay_index_3 = Excluded.prior_pay_index_3,
					prior_pay_index_4 = Excluded.prior_pay_index_4,
					prior_pay_index_5 = Excluded.prior_pay_index_5,
					prior_pay_index_6 = Excluded.prior_pay_index_6,
					prior_pay_index_7 = Excluded.prior_pay_index_7,
					prior_pay_index_8 = Excluded.prior_pay_index_8,
					prior_pay_index_9 = Excluded.prior_pay_index_9,
					prior_pay_index_10 = Excluded.prior_pay_index_10,
					prior_pay_index_11 = Excluded.prior_pay_index_11,
					prior_pay_index_12 = Excluded.prior_pay_index_12,
					case_status = Excluded.case_status,
					reference_request_indicator = Excluded.reference_request_indicator,
					code_case_country = Excluded.code_case_country,
					code_pay_summary = Excluded.code_pay_summary,
					load_time = v_proc_start_time;

	-- <--------------------------------------------------> updating load_time column <---------------------------------------->

					UPDATE
					trade_case
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
	'Process [sp_trade_case] Ended Successfully',
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

-- Procedure [sp_trade_case] completed