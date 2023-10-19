\echo
\echo 'Executing postgresql database components....'

\echo
\echo 'Executing ddl_drop_all_staging_tables.sql....'
\i ddl_drop_all_staging_tables.sql

\echo
\echo 'Executing ddl_audit_table.sql....'
\i ddl_audit_table.sql

\echo
\echo 'Executing ddl_seq_audit_table_load_id.sql....'
\i ddl_seq_audit_table_load_id.sql

\echo
\echo 'Executing ddl_pay_exp_dlink_table.sql....'
\i ddl_pay_exp_dlink_table.sql

\echo
\echo 'Executing ddl_pay_ref_dlink_table.sql....'
\i ddl_pay_ref_dlink_table.sql

\echo
\echo 'Executing fn_call_audit_table.sql....'
\i fn_call_audit_table.sql

\echo
\echo 'Executing fn_is_date.sql....'
\i fn_is_date.sql

\echo
\echo 'Executing sp_trade_case.sql....'
\i sp_trade_case.sql

\echo
\echo 'Executing sp_pay_exp.sql....'
\i sp_pay_exp.sql

\echo
\echo 'Executing sp_pay_ref.sql....'
\i sp_pay_ref.sql

\echo
\echo 'Executing sp_country_rec.sql....'
\i sp_country_rec.sql

\echo
\echo 'Executing sp_sic_code_rec.sql....'
\i sp_sic_code_rec.sql

\echo
\echo 'Executing sp_country_sic_rec.sql....'
\i sp_country_sic_rec.sql

\echo
\echo 'Executing sp_case_rec.sql....'
\i sp_case_rec.sql

\echo
\echo 'Executing sp_pif_directory.sql....'
\i sp_pif_directory.sql

\echo
\echo 'All postgresql database components are completed.'