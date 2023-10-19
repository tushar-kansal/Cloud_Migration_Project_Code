-- <--------------------------------------------------> <---------------------------------------->
/*
 
 Dnb Data Replication
 
 Stored Procedure Name: sp_country_sic_rec
 Staging Table Name: country_sic_rec_stg (Glue will create this table)
 Target Table Name: country_sic_rec
 
 This procedure is used to move data from staging table to target table.
 
 Dependent Objects of this Procedure for Successful Completion:
 -- Staging table [country_sic_rec_stg] (Glue will create this table)
 -- Target table [country_sic_rec]
 -- Audit table [audit_table]
 -- Function [fn_call_audit_table]
 -- Function [fn_is_date]
 -- Sequence number [seq_audit_table_load_id]
 -- Master Tables [Country_rec, Sic_code_rec]
 
 Procedure calling command: call sp_country_sic_rec ('full path received from glue job','load type received from glue job');
 
*/
-- <--------------------------------------------------> <---------------------------------------->


-- <--------------------------------------------------> creating procedure with inout parameter <---------------------------------------->

CREATE
OR REPLACE PROCEDURE sp_country_sic_rec (v_full_path VARCHAR, inout v_load_type VARCHAR) LANGUAGE plpgsql AS $STG_TARGET$

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


	v_table_name = 'country_sic_rec';

	-- <--------------------------------------------------> counting total number of records present in staging table <---------------------------------------->

	SELECT
	count(dbkey) INTO v_total_record_count
	FROM
	country_sic_rec_stg;

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
	'Process [sp_country_sic_rec] Started Successfully',
	v_proc_start_time,
	v_proc_end_time
	);

	-- <--------------------------------------------------> condition block start and checking load type <---------------------------------------->

	IF lower(v_load_type) = 'full' then

	-- <--------------------------------------------------> condition block start and checking record present in staging table or not <---------------------------------------->

			if v_total_record_count > 0 then

	-- <--------------------------------------------------> checking date either come in proper format or not, if not then we are updating to '1900-01-01' [fixing date issue] <---------------------------------------->

					UPDATE
					country_sic_rec_stg
					SET
					ctysc_date_last_update = ctysc_date_last_update_tmp;


					UPDATE
					country_sic_rec_stg
					SET
					ctysc_date_last_update = CASE
						WHEN length(ctysc_date_last_update) != 8 THEN '19000101'
						WHEN ctysc_date_last_update = '00000000' THEN NULL
						WHEN NOT fn_is_date(ctysc_date_last_update) THEN '19000101'
						ELSE ctysc_date_last_update
					END;

	-- <--------------------------------------------------> deleting records and counting deleted records <---------------------------------------->	

					WITH
					deleted AS (
						DELETE FROM
						country_sic_rec returning country_sic_rec.dbkey
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
						country_sic_rec (
							dbkey,
							sic_ctysc_owner,
							order_sic_ctysc,
							cntry_ctysc_owner,
							order_cntry_ctysc,
							ctysc_nbr_sic,
							ctysc_code_country_abbrev,
							ctysc_qty_companies_in_sic_1,
							ctysc_nbr_rating_low_qrtile_1,
							ctysc_nbr_rating_med_qrtile_1,
							ctysc_nbr_rating_upp_qrtile_1,
							ctysc_qty_companies_in_sic_2,
							ctysc_nbr_rating_low_qrtile_2,
							ctysc_nbr_rating_med_qrtile_2,
							ctysc_nbr_rating_upp_qrtile_2,
							ctysc_qty_companies_in_sic_3,
							ctysc_nbr_rating_low_qrtile_3,
							ctysc_nbr_rating_med_qrtile_3,
							ctysc_nbr_rating_upp_qrtile_3,
							ctysc_qty_companies_in_sic_4,
							ctysc_nbr_rating_low_qrtile_4,
							ctysc_nbr_rating_med_qrtile_4,
							ctysc_nbr_rating_upp_qrtile_4,
							ctysc_qty_companies_in_sic_5,
							ctysc_nbr_rating_low_qrtile_5,
							ctysc_nbr_rating_med_qrtile_5,
							ctysc_nbr_rating_upp_qrtile_5,
							ctysc_qty_companies_in_sic_6,
							ctysc_nbr_rating_low_qrtile_6,
							ctysc_nbr_rating_med_qrtile_6,
							ctysc_nbr_rating_upp_qrtile_6,
							ctysc_qty_companies_in_sic_7,
							ctysc_nbr_rating_low_qrtile_7,
							ctysc_nbr_rating_med_qrtile_7,
							ctysc_nbr_rating_upp_qrtile_7,
							ctysc_qty_companies_in_sic_8,
							ctysc_nbr_rating_low_qrtile_8,
							ctysc_nbr_rating_med_qrtile_8,
							ctysc_nbr_rating_upp_qrtile_8,
							ctysc_date_last_update
						)
						SELECT
						dbkey,
						sic_ctysc_owner,
						order_sic_ctysc,
						cntry_ctysc_owner,
						order_cntry_ctysc,
						ctysc_nbr_sic,
						ctysc_code_country_abbrev,
						ctysc_qty_companies_in_sic_1,
						ctysc_nbr_rating_low_qrtile_1,
						ctysc_nbr_rating_med_qrtile_1,
						ctysc_nbr_rating_upp_qrtile_1,
						ctysc_qty_companies_in_sic_2,
						ctysc_nbr_rating_low_qrtile_2,
						ctysc_nbr_rating_med_qrtile_2,
						ctysc_nbr_rating_upp_qrtile_2,
						ctysc_qty_companies_in_sic_3,
						ctysc_nbr_rating_low_qrtile_3,
						ctysc_nbr_rating_med_qrtile_3,
						ctysc_nbr_rating_upp_qrtile_3,
						ctysc_qty_companies_in_sic_4,
						ctysc_nbr_rating_low_qrtile_4,
						ctysc_nbr_rating_med_qrtile_4,
						ctysc_nbr_rating_upp_qrtile_4,
						ctysc_qty_companies_in_sic_5,
						ctysc_nbr_rating_low_qrtile_5,
						ctysc_nbr_rating_med_qrtile_5,
						ctysc_nbr_rating_upp_qrtile_5,
						ctysc_qty_companies_in_sic_6,
						ctysc_nbr_rating_low_qrtile_6,
						ctysc_nbr_rating_med_qrtile_6,
						ctysc_nbr_rating_upp_qrtile_6,
						ctysc_qty_companies_in_sic_7,
						ctysc_nbr_rating_low_qrtile_7,
						ctysc_nbr_rating_med_qrtile_7,
						ctysc_nbr_rating_upp_qrtile_7,
						ctysc_qty_companies_in_sic_8,
						ctysc_nbr_rating_low_qrtile_8,
						ctysc_nbr_rating_med_qrtile_8,
						ctysc_nbr_rating_upp_qrtile_8,
						to_date(ctysc_date_last_update:: text, 'YYYYMMDD')
						FROM
						country_sic_rec_stg returning country_sic_rec.dbkey
					)
					SELECT
					count(dbkey) INTO v_insert_count
					FROM
					inserted;
					
	-- <--------------------------------------------------> updating load_time column <---------------------------------------->

					UPDATE
					country_sic_rec
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
	'Process [sp_country_sic_rec] Ended Successfully',
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

-- Procedure [sp_country_sic_rec] completed