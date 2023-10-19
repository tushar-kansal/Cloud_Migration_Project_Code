/*
 
 Dnb Data Replication

 Sequence name: seq_audit_table_load_id 
 
 This sequence number is used for audit_table load_id column.
 This sequence number is used by below stored procedure
 -- sp_trade_case
 -- sp_pay_exp
 -- sp_pay_ref
 -- sp_case_rec
 -- sp_pif_directory
 -- sp_country_rec
 -- sp_country_sic_rec
 -- sp_sic_code_rec

 
*/

----------------------------------------------------> Creating Sequence Number for audit table column - load_id <--------------------------------------------

CREATE SEQUENCE seq_audit_table_load_id 
INCREMENT BY 1 
MINVALUE 1 
MAXVALUE 200000000
START 1 
CACHE 1
NO CYCLE;

-- Creation of sequence number completed