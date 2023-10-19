/*
 
 Dnb Data Replication
 
 Table name: pay_exp_dlink
 
 This table is used to store dlink records from pay_exp to pay_exp_dlink.
 This table is used by below stored procedure
 -- sp_pay_exp
 
 
 If this table already exist or you want to delete this table then use below query
 drop table pay_exp_dlink;
 
*/

---------------------------------------------> Creating pay_exp_dlink table <---------------------------------------

CREATE TABLE
  pay_exp_dlink (
    dbkey int4 NOT NULL,
    ovrflow bool NULL,
    trade_data_owner int4 NULL,
    order_trade_data int4 NULL,
    pay_ref_duns_number int4 NULL,
    pay_experience_source bpchar(1) NULL,
    pay_experience_date date NULL,
    pay_document_number int4 NULL,
    currency_code bpchar(3) NULL,
    payment_manner bpchar(1) NULL,
    payment_notes bpchar(1) NULL,
    days_slow_low int4 NULL,
    days_slow_high int4 NULL,
    high_credit int4 NULL,
    total_owing int4 NULL,
    owed_0_30 int4 NULL,
    owed_31_60 int4 NULL,
    owed_61_90 int4 NULL,
    owed_over_90 int4 NULL,
    pay_disc_percentage_low bpchar(1) NULL,
    pay_disc_percentage_high bpchar(1) NULL,
    pay_disc_days_low bpchar(1) NULL,
    pay_disc_days_high bpchar(1) NULL,
    pay_due_days bpchar(1) NULL,
    past_due_indicator bpchar(1) NULL,
    special_terms bpchar(1) NULL,
    last_sale_within bpchar(1) NULL,
    ind_pay_exp_display bpchar(1) NULL,
    detrimental_trade_ind bpchar(1) NULL,
    idx int4 NOT NULL,
    load_time timestamp NULL DEFAULT CURRENT_TIMESTAMP
  );