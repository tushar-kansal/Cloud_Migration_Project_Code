/*

For glue code re-deployement, staging tables needs to be deleted.

*/


-- dropping trade_case_stg table 
drop table trade_case_stg;

-- dropping pay_exp_stg table 
drop table pay_exp_stg;

-- dropping pay_ref_stg table 
drop table pay_ref_stg;

-- dropping case_rec_stg table 
drop table case_rec_stg;

-- dropping pif_directory_stg table 
drop table pif_directory_stg;

-- dropping country_rec_stg table 
drop table country_rec_stg;

-- dropping sic_code_rec_stg table 
drop table sic_code_rec_stg;

-- dropping country_sic_rec_stg table 
drop table country_sic_rec_stg;
