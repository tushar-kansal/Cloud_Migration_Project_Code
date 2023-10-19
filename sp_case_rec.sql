-- <--------------------------------------------------> <---------------------------------------->
/*
 
 Dnb Data Replication
 
 Stored Procedure Name: sp_case_rec
 Staging Table Name: case_rec_stg (Glue will create this table)
 Target Table Name: case_rec
 
 This procedure is used to move data from staging table to target table.
 
 Dependent Objects of this Procedure for Successful Completion:
 -- Staging table [case_rec_stg] (Glue will create this table)
 -- Target table [case_rec]
 -- Audit table [audit_table]
 -- Function [fn_call_audit_table]
 -- Function [fn_is_date]
 -- Sequence number [seq_audit_table_load_id]
 
 Procedure calling command: call sp_case_rec ('full path received from glue job','load type received from glue job');
 
*/
-- <--------------------------------------------------> <---------------------------------------->


-- <--------------------------------------------------> creating procedure with inout parameter <---------------------------------------->

CREATE
OR REPLACE PROCEDURE sp_case_rec (v_full_path VARCHAR, inout v_load_type VARCHAR) LANGUAGE plpgsql AS $STG_TARGET$

-- <--------------------------------------------------> declaring variables <---------------------------------------->

DECLARE 
v_load_id INTEGER = 0;
v_file_name VARCHAR = v_full_path;
v_table_name VARCHAR;
v_total_record_count INTEGER = 0;
v_insert_count INTEGER = 0;
v_update_count INTEGER = 0;
v_temp_update_count INTEGER = 0;
v_delete_count INTEGER = 0;
v_load_status VARCHAR;
v_remark Text = '-';
v_proc_start_time TIMESTAMP;
v_proc_end_time TIMESTAMP;
v_state TEXT;
v_msg TEXT;
v_no_update integer = 0;
v_nbr_duns_count INTEGER = 0;

-- <--------------------------------------------------> begin block start and store some info to variables <---------------------------------------->

BEGIN

	v_proc_start_time = clock_timestamp();


	select
	last_value into v_load_id
	from
	seq_audit_table_load_id;


	perform setval('seq_audit_table_load_id', (v_load_id + 1));


	v_table_name = 'case_rec';

	-- <--------------------------------------------------> counting total number of records present in staging table <---------------------------------------->

	SELECT
	count(1) INTO v_total_record_count
	FROM
	case_rec_stg;

	-- <--------------------------------------------------> condition block start and checking load type <---------------------------------------->

	IF lower(v_load_type) = 'incremental' then

	-- <--------------------------------------------------> counting total number of records present in staging table <---------------------------------------->

		SELECT
		count(dbkey) INTO v_total_record_count
		FROM
		case_rec_stg;

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
		'Process [sp_case_rec] Started Successfully',
		v_proc_start_time,
		v_proc_end_time
		);

	-- <--------------------------------------------------> condition block start and checking record present in staging table or not <---------------------------------------->

			if v_total_record_count > 0 then

	-- <--------------------------------------------------> checking date either come in proper format or not, if not then we are updating to '1900-01-01' [fixing date issue] <---------------------------------------->

					UPDATE
					case_rec_stg
					SET
					date_out_business = date_out_business_tmp,
					date_base = date_base_tmp;


					UPDATE
					case_rec_stg
					SET
					date_out_business = CASE
						WHEN length(date_out_business) != 8 THEN '19000101'
						WHEN date_out_business = '00000000' THEN NULL
						WHEN NOT fn_is_date(date_out_business) THEN '19000101'
						ELSE date_out_business
					END,
					date_base = CASE
						WHEN length(date_base) != 8 THEN '19000101'
						WHEN date_base = '00000000' THEN NULL
						WHEN NOT fn_is_date(date_base) THEN '19000101'
						ELSE date_base
					END;

	-- <--------------------------------------------------> deleting records and counting deleted records <---------------------------------------->	

					WITH
					deleted AS (
						DELETE FROM
						case_rec using case_rec_stg
						WHERE
						case_rec.dbkey = case_rec_stg.dbkey returning case_rec.dbkey
					)
					SELECT
					count(dbkey) INTO v_temp_update_count
					FROM
					deleted;

	-- <--------------------------------------------------> inserting records and counting inserted records <---------------------------------------->	

					WITH
					inserted AS (
						INSERT INTO
						case_rec (
							dbkey,
							nbr_duns,
							country_abbrev,
							business_no,
							ind_stop_distribution,
							ind_out_business,
							date_base,
							date_out_business,
							total_employees,
							total_sales,
							nbr_case_telephone,
							name_primary,
							addr_primary_street_1,
							addr_primary_street_2,
							name_primary_city,
							name_primary_county,
							code_country_abbrev,
							code_postal,
							nbr_sic_1,
							nbr_sic_2,
							nbr_sic_3,
							nbr_sic_4,
							nbr_sic_5,
							nbr_sic_6,
							nbr_hdq_duns,
							nbr_parent_duns,
							ind_location_function,
							fiscal_code
						)
						SELECT
						dbkey,
						nbr_duns,
						country_abbrev,
						business_no,
						ind_stop_distribution,
						ind_out_business,
						to_date(date_base:: Text, 'YYYYMMDD'),
						to_date(date_out_business:: Text, 'YYYYMMDD'),
						total_employees,
						total_sales,
						nbr_case_telephone,
						name_primary,
						addr_primary_street_1,
						addr_primary_street_2,
						name_primary_city,
						name_primary_county,
						code_country_abbrev,
						code_postal,
						nbr_sic_1,
						nbr_sic_2,
						nbr_sic_3,
						nbr_sic_4,
						nbr_sic_5,
						nbr_sic_6,
						nbr_hdq_duns,
						nbr_parent_duns,
						ind_location_function,
						fiscal_code
						FROM
						case_rec_stg returning case_rec.dbkey
					)
					SELECT
					count(dbkey) INTO v_insert_count
					FROM
					inserted;
					
	-- <--------------------------------------------------> updating load_time column <---------------------------------------->

					UPDATE
					case_rec
					SET
					load_time = v_proc_start_time
					WHERE
					load_time IS NULL;

	-- <--------------------------------------------------> calling fn_call_audit_table function and storing inserted records <---------------------------------------->

					v_update_count = v_temp_update_count;


					v_insert_count = v_insert_count - v_temp_update_count;


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

	-- <--------------------------------------------------> condition block start and checking load type <---------------------------------------->

	elsif lower(v_load_type) = 'delete' then

	-- <--------------------------------------------------> counting total number of nbr_duns present in staging table <---------------------------------------->

		SELECT
		count(nbr_duns) INTO v_total_record_count
		FROM
		case_rec_stg;

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
		'Process [sp_case_rec] Started Successfully',
		v_proc_start_time,
		v_proc_end_time
		);

	-- <--------------------------------------------------> condition block start and checking record present in staging table or not <---------------------------------------->

			if v_total_record_count > 0 then

	-- <--------------------------------------------------> deleting records and counting deleted records <---------------------------------------->	

					WITH
					deleted AS (
						DELETE FROM
						case_rec using case_rec_stg
						WHERE
						case_rec.nbr_duns = case_rec_stg.nbr_duns returning case_rec.nbr_duns
					)
					SELECT
					count(nbr_duns) INTO v_delete_count
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

	-- <--------------------------------------------------> executing else block when (lower(v_load_type) = 'incremental' or lower(v_load_type) = 'delete') condition failed <---------------------------------------->

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
	'Process [sp_case_rec] Ended Successfully',
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

-- Procedure [sp_case_rec] completed