-- <--------------------------------------------------> <---------------------------------------->
/*
 
 Dnb Data Replication
 
 Stored Procedure Name: sp_pif_directory
 Staging Table Name: pif_directory_stg (Glue will create this table)
 Target Table Name: pif_directory
 
 This procedure is used to move data from staging table to target table.
 
 Dependent Objects of this Procedure for Successful Completion:
 -- Staging table [pif_directory_stg] (Glue will create this table)
 -- Target table [pif_directory]
 -- Audit table [audit_table]
 -- Function [fn_call_audit_table]
 -- Function [fn_is_date]
 -- Sequence number [seq_audit_table_load_id] 
 
 Procedure calling command: call sp_pif_directory ('full path received from glue job','load type received from glue job');
 
*/
-- <--------------------------------------------------> <---------------------------------------->


-- <--------------------------------------------------> creating procedure with inout parameter <---------------------------------------->

CREATE
OR REPLACE PROCEDURE sp_pif_directory (v_full_path VARCHAR, inout v_load_type VARCHAR) LANGUAGE plpgsql AS $STG_TARGET$

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


	v_table_name = 'pif_directory';

	-- <--------------------------------------------------> counting total number of records present in staging table <---------------------------------------->

	SELECT
	count(dbkey) INTO v_total_record_count
	FROM
	pif_directory_stg;

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
	'Process [sp_pif_directory] Started Successfully',
	v_proc_start_time,
	v_proc_end_time
	);

	-- <--------------------------------------------------> condition block start and checking load type <---------------------------------------->

	IF lower(v_load_type) = 'incremental' then 

	-- <--------------------------------------------------> condition block start and checking record present in staging table or not <---------------------------------------->

			if v_total_record_count > 0 then

	-- <--------------------------------------------------> checking date either come in proper format or not, if not then we are updating to '1900-01-01' [fixing date issue] <---------------------------------------->

					UPDATE
					pif_directory_stg
					SET
					pfdir_experience_date = pfdir_experience_date_tmp,
					pfdir_date_sent_duns_return = pfdir_date_sent_duns_return_tmp;


					UPDATE
					pif_directory_stg
					SET
					pfdir_experience_date = CASE
						WHEN length(pfdir_experience_date) != 8 THEN '19000101'
						WHEN pfdir_experience_date = '00000000' THEN NULL
						WHEN NOT fn_is_date(pfdir_experience_date) THEN '19000101'
						ELSE pfdir_experience_date
					END,
					pfdir_date_sent_duns_return = CASE
						WHEN length(pfdir_date_sent_duns_return) != 8 THEN '19000101'
						WHEN pfdir_date_sent_duns_return = '00000000' THEN NULL
						WHEN NOT fn_is_date(pfdir_date_sent_duns_return) THEN '19000101'
						ELSE pfdir_date_sent_duns_return
					END;

	-- <--------------------------------------------------> deleting records and counting deleted records <---------------------------------------->	

					WITH
					deleted AS (
						DELETE FROM
						pif_directory using pif_directory_stg
						WHERE
						pif_directory.dbkey = pif_directory_stg.dbkey returning pif_directory.dbkey
					)
					SELECT
					count(dbkey) INTO v_delete_count
					FROM
					deleted;

	-- <--------------------------------------------------> calling fn_call_audit_table function and storing deleted records <---------------------------------------->

					perform fn_call_audit_table (
					v_load_id,
					v_file_name,
					v_table_name,
					v_total_record_count,
					v_insert_count,
					v_update_count,
					v_delete_count,
					concat('Records Deleted: ', v_delete_count),
					'Process Deletion Successfully Completed',
					v_proc_start_time,
					v_proc_end_time
					);

	-- <--------------------------------------------------> inserting records and counting inserted records <---------------------------------------->	

					WITH
					inserted AS (
						INSERT INTO
						pif_directory (
							dbkey,
							pfdir_nbr_participant,
							pfdir_ref_country_abbrev,
							pfdir_vta_nbr_duns,
							pfdir_current_start_dbkey,
							pfdir_current_end_dbkey,
							pfdir_date_sent_duns_return,
							pfdir_ind_update_type,
							pfdir_experience_date,
							pfdir_date_last_update,
							pfdir_ind_reference_type,
							pfdir_ind_cpi_candidate,
							pfdir_ind_post_sum_cand,
							pfdir_ind_duns_return_cand,
							pfdir_time_duns_ret_weeks,
							pfdir_pct_duns_return,
							pfdir_ind_duns_return_force,
							pfdir_ind_duns_return_media,
							pfdir_code_name_match_source,
							pfdir_code_name_match_route,
							pfdir_code_name_match_scan,
							pfdir_match_timeout_days,
							pfdir_code_approval,
							pfdir_send_to_or,
							pfdir_ind_relookup,
							pfdir_ind_accept_notrade,
							pfdir_text_bkfd_ctry_codes,
							filler,
							pfdir_ind_newrec_id,
							pfdir_qty_new_accounts,
							pfdir_qty_perfects,
							pfdir_qty_probables,
							pfdir_qty_no_finds,
							pfdir_qty_personals,
							pfdir_qty_foreign,
							pfdir_qty_garbage,
							pfdir_qty_b_match_relook,
							pfdir_qty_c_match_relook,
							pfdir_qty_p_match_relook,
							pfdir_qty_na_change,
							pfdir_qty_byp_no_trade,
							pfdir_qty_total_masters
						)
						SELECT
						dbkey,
						pfdir_nbr_participant,
						pfdir_ref_country_abbrev,
						pfdir_vta_nbr_duns,
						pfdir_current_start_dbkey,
						pfdir_current_end_dbkey,
						to_date(pfdir_date_sent_duns_return:: text, 'YYYYMMDD'),
						pfdir_ind_update_type,
						to_date(pfdir_experience_date:: text, 'YYYYMMDD'),
						pfdir_date_last_update,
						pfdir_ind_reference_type,
						pfdir_ind_cpi_candidate,
						pfdir_ind_post_sum_cand,
						pfdir_ind_duns_return_cand,
						pfdir_time_duns_ret_weeks,
						pfdir_pct_duns_return,
						pfdir_ind_duns_return_force,
						pfdir_ind_duns_return_media,
						pfdir_code_name_match_source,
						pfdir_code_name_match_route,
						pfdir_code_name_match_scan,
						pfdir_match_timeout_days,
						pfdir_code_approval,
						pfdir_send_to_or,
						pfdir_ind_relookup,
						pfdir_ind_accept_notrade,
						pfdir_text_bkfd_ctry_codes,
						filler,
						pfdir_ind_newrec_id,
						pfdir_qty_new_accounts,
						pfdir_qty_perfects,
						pfdir_qty_probables,
						pfdir_qty_no_finds,
						pfdir_qty_personals,
						pfdir_qty_foreign,
						pfdir_qty_garbage,
						pfdir_qty_b_match_relook,
						pfdir_qty_c_match_relook,
						pfdir_qty_p_match_relook,
						pfdir_qty_na_change,
						pfdir_qty_byp_no_trade,
						pfdir_qty_total_masters
						FROM
						pif_directory_stg returning pif_directory.dbkey
					)
					SELECT
					count(dbkey) INTO v_insert_count
					FROM
					inserted;
					
	-- <--------------------------------------------------> updating load_time column <---------------------------------------->

					UPDATE
					pif_directory
					SET
					load_time = v_proc_start_time
					WHERE
					load_time IS NULL;

	-- <--------------------------------------------------> calling fn_call_audit_table function and storing inserted records <---------------------------------------->

					perform fn_call_audit_table (
					v_load_id,
					v_file_name,
					v_table_name,
					v_total_record_count,
					v_insert_count,
					v_update_count,
					v_delete_count,
					concat('Records Inserted: ', v_insert_count),
					'Process Insertion Successfully Completed',
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
	'Process [sp_pif_directory] Ended Successfully',
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

-- Procedure [sp_pif_directory] completed