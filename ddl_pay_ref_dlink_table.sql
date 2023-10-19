/*
 
 Dnb Data Replication
 
 Table name: pay_ref_dlink
 
 This table is used to store dlink records from pay_ref to pay_ref_dlink.
 This table is used by below stored procedure
 -- sp_pay_ref
 
 
 If this table already exist or you want to delete this table then use below query
 drop table pay_ref_dlink;
 
*/

---------------------------------------------> Creating pay_ref_dlink table <---------------------------------------


CREATE TABLE
  pay_ref_dlink (
    dbkey int4 NOT NULL,
    ovrflow bool NULL,
    trade_data_owner int4 NULL,
    order_trade_data int4 NULL,
    supplier_duns_number int4 NULL,
    code_reference_country bpchar(2) NULL,
    pay_reference_use_status bpchar(1) NULL,
    pay_reference_status bpchar(1) NULL,
    pay_ref_solicit_pgm bpchar(1) NULL,
    pay_reference_source bpchar(1) NULL,
    pay_experience_source bpchar(1) NULL,
    pay_experience_date date NULL,
    pay_account_number bpchar(10) NULL,
    pay_document_number int4 NULL,
    pay_reference_sic int2 NULL,
    qty_pay_exps_reported int2 NULL,
    idx int4 NOT NULL,
    load_time timestamp NULL DEFAULT CURRENT_TIMESTAMP
  );