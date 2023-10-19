-- <--------------------------------------------------> <---------------------------------------->
/*
 
 Dnb Data Replication
 
 Stored Procedure Name: sp_pay_exp
 Staging Table Name: pay_exp_stg (Glue will create this table)
 Target Table Name: pay_exp
 
 This procedure is used to move data from staging table to target table.
 
 Dependent Objects of this Procedure for Successful Completion:
 -- Staging table [pay_exp_stg] (Glue will create this table)
 -- Target table [pay_exp]
 -- Audit table [audit_table]
 -- Function [fn_call_audit_table]
 -- Function [fn_is_date]
 -- Sequence number [seq_audit_table_load_id]
 -- Master Table [trade_case]
 -- Table [trade_case_stg, pay_exp_dlink]
 
 Procedure calling command: call sp_pay_exp ('full path received from glue job','load type received from glue job');
 
*/
-- <--------------------------------------------------> <---------------------------------------->


-- <--------------------------------------------------> creating procedure with inout parameter <---------------------------------------->

CREATE
OR REPLACE PROCEDURE sp_pay_exp (v_full_path VARCHAR, inout v_load_type VARCHAR) LANGUAGE plpgsql AS $STG_TARGET$

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
v_delete_dlink_count INTEGER = 0;
v_delete_dlink_count_older_than_six_month INTEGER = 0;
v_insert_dlink_count INTEGER = 0;
v_dlink_remark TEXT = '-';

-- <--------------------------------------------------> begin block start and store some info to variables <---------------------------------------->

BEGIN

	v_proc_start_time = clock_timestamp();


	select
	last_value into v_load_id
	from
	seq_audit_table_load_id;


	perform setval('seq_audit_table_load_id', (v_load_id + 1));


	v_table_name = 'pay_exp';

	-- <--------------------------------------------------> counting total number of records present in staging table <---------------------------------------->

	SELECT
	count(dbkey) INTO v_total_record_count
	FROM
	pay_exp_stg;

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
	'Process [sp_pay_exp] Started Successfully',
	v_proc_start_time,
	v_proc_end_time
	);

	-- <--------------------------------------------------> condition block start and checking load type <---------------------------------------->


IF lower(v_load_type) = 'incremental' then

	-- <--------------------------------------------------> condition block start and checking record present in staging table or not <---------------------------------------->

			if v_total_record_count > 0 then

	-- <--------------------------------------------------> checking date either come in proper format or not, if not then we are updating to '1900-01-01' [fixing date issue] <---------------------------------------->

                    UPDATE
                    pay_exp_stg
                    SET
                    pay_experience_date = pay_experience_date_tmp;


                    UPDATE
                    pay_exp_stg
                    SET
                    pay_experience_date = CASE
                        WHEN length(pay_experience_date) != 8 THEN '19000101'
                        WHEN pay_experience_date = '00000000' THEN NULL
                        WHEN NOT fn_is_date(pay_experience_date) THEN '19000101'
                        ELSE pay_experience_date
                    END;

	-- <--------------------------------------------------> deleting records on the bases of trade_case_stg and counting deleted records <---------------------------------------->

                    WITH
                    deleted AS (
                        DELETE FROM
                        pay_exp using trade_case_stg
                        WHERE
                        trade_case_stg.dbkey = pay_exp.trade_data_owner returning pay_exp.dbkey
                    )
                    SELECT
                    count(dbkey) INTO v_delete_count
                    FROM
                    deleted;

    -- <--------------------------------------------------> inserting dlink records into pay_exp_dlink table before deleting from pay_exp table <---------------------------------------->

                    INSERT INTO
                    pay_exp_dlink (
                        idx,
                        ovrflow,
                        trade_data_owner,
                        order_trade_data,
                        pay_ref_duns_number,
                        pay_experience_date,
                        pay_document_number,
                        days_slow_low,
                        days_slow_high,
                        high_credit,
                        total_owing,
                        owed_0_30,
                        owed_31_60,
                        owed_61_90,
                        owed_over_90,
                        dbkey,
                        last_sale_within,
                        ind_pay_exp_display,
                        detrimental_trade_ind,
                        pay_experience_source,
                        pay_disc_percentage_low,
                        pay_disc_percentage_high,
                        currency_code,
                        payment_manner,
                        payment_notes,
                        pay_disc_days_low,
                        pay_disc_days_high,
                        pay_due_days,
                        past_due_indicator,
                        special_terms
                    )
                    SELECT
                    idx,
                    ovrflow,
                    trade_data_owner,
                    order_trade_data,
                    pay_ref_duns_number,
                    pay_experience_date,
                    pay_document_number,
                    days_slow_low,
                    days_slow_high,
                    high_credit,
                    total_owing,
                    owed_0_30,
                    owed_31_60,
                    owed_61_90,
                    owed_over_90,
                    dbkey,
                    last_sale_within,
                    ind_pay_exp_display,
                    detrimental_trade_ind,
                    pay_experience_source,
                    pay_disc_percentage_low,
                    pay_disc_percentage_high,
                    currency_code,
                    payment_manner,
                    payment_notes,
                    pay_disc_days_low,
                    pay_disc_days_high,
                    pay_due_days,
                    past_due_indicator,
                    special_terms
                    FROM
                    pay_exp
                    WHERE
                    dbkey in (
                        SELECT
                        DISTINCT(dbkey)
                        FROM
                        pay_exp_stg
                    );

    -- <--------------------------------------------------> deleting dlink records from pay_exp table and counting deleted records <---------------------------------------->

                    WITH
                    delete_dlink AS (
                        DELETE FROM
                        pay_exp
                        WHERE
                        dbkey IN (
                            SELECT
                            DISTINCT (dbkey)
                            FROM
                            pay_exp_stg
                        ) returning pay_exp.dbkey
                    )
                    SELECT
                    count(dbkey) INTO v_delete_dlink_count
                    FROM
                    delete_dlink;

					v_insert_dlink_count = v_delete_dlink_count;

    -- <--------------------------------------------------> deleting dlink records from pay_exp_dlink table which are older than 6 months from current date <---------------------------------------->

					WITH
					dlink_records_older_than_six_months AS (
						DELETE FROM
						pay_exp_dlink
						WHERE
						(load_time:: DATE) < (clock_timestamp() - interval '6 months'):: DATE returning pay_exp_dlink.dbkey
					)
					SELECT
					count(dbkey) into v_delete_dlink_count_older_than_six_month
					from
					dlink_records_older_than_six_months;
                    
	-- <--------------------------------------------------> calling fn_call_audit_table function and storing deleted records <---------------------------------------->

                    
                    v_delete_count = v_delete_count + v_delete_dlink_count;

                    
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
						pay_exp (
							idx,
							ovrflow,
							trade_data_owner,
							order_trade_data,
							pay_ref_duns_number,
							pay_experience_date,
							pay_document_number,
							days_slow_low,
							days_slow_high,
							high_credit,
							total_owing,
							owed_0_30,
							owed_31_60,
							owed_61_90,
							owed_over_90,
							dbkey,
							last_sale_within,
							ind_pay_exp_display,
							detrimental_trade_ind,
							pay_experience_source,
							pay_disc_percentage_low,
							pay_disc_percentage_high,
							currency_code,
							payment_manner,
							payment_notes,
							pay_disc_days_low,
							pay_disc_days_high,
							pay_due_days,
							past_due_indicator,
							special_terms
						)
						SELECT
						idx,
						ovrflow,
						trade_data_owner,
						order_trade_data,
						pay_ref_duns_number,
						to_date(pay_experience_date:: text, 'YYYYMMDD'),
						pay_document_number,
						days_slow_low,
						days_slow_high,
						high_credit,
						total_owing,
						owed_0_30,
						owed_31_60,
						owed_61_90,
						owed_over_90,
						dbkey,
						last_sale_within,
						ind_pay_exp_display,
						detrimental_trade_ind,
						pay_experience_source,
						pay_disc_percentage_low,
						pay_disc_percentage_high,
						currency_code,
						payment_manner,
						payment_notes,
						pay_disc_days_low,
						pay_disc_days_high,
						pay_due_days,
						past_due_indicator,
						special_terms
						FROM
						pay_exp_stg returning pay_exp.dbkey
					)
					SELECT
					count(dbkey) INTO v_insert_count
					FROM
					inserted;

	-- <--------------------------------------------------> updating load_time column <---------------------------------------->

					UPDATE
					pay_exp
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


	v_dlink_remark = concat(
	'In pay_exp_dlink table,  Records inserted: ',
	v_insert_dlink_count,
	',  Records deleted which were older than six months: ',
	v_delete_dlink_count_older_than_six_month,
	'.'
	);


	perform fn_call_audit_table (
	v_load_id,
	v_file_name,
	v_table_name,
	v_total_record_count,
	v_insert_count,
	v_update_count,
	v_delete_count,
	'Process Ended',
	concat(v_dlink_remark,'  Process [sp_pay_exp] Ended Successfully'),
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
	v_delete_count,
	'.  ',
	v_dlink_remark
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

-- Procedure [sp_pay_exp] completed