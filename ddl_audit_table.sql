/*
 
 Dnb Data Replication
 
 Table name: audit_table 
 
 This table is used to store log or processing status of stored procedure
 This table is used by below stored procedure
 -- sp_trade_case
 -- sp_pay_exp
 -- sp_pay_ref
 -- sp_case_rec
 -- sp_pif_directory
 -- sp_country_rec
 -- sp_country_sic_rec
 -- sp_sic_code_rec
 
 
 If this table already exist or you want to delete this table then use below query
 drop table audit_table;
 
*/

---------------------------------------------> Creating Audit Table <---------------------------------------

CREATE TABLE
  audit_table (
    Load_id INT PRIMARY KEY,
    File_name VARCHAR,
    Table_name VARCHAR(20),
    Total_Records_Count INT,
    Total_Records_Inserted_Count INT,
    Total_Records_Updated_Count INT,
    Total_Records_Deleted_Count INT,
    Load_status VARCHAR(60),
    Remark TEXT,
    Event_start_time TIMESTAMP,
    Event_end_time TIMESTAMP
  );

-- Creation of table completed