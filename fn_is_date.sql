/*
 
 Dnb Data Replication
 
 Function name: fn_is_date
 
 This function is used for date_validation
 This function is used by below stored procedure 
 -- sp_trade_case
 -- sp_pay_exp
 -- sp_pay_ref
 -- sp_case_rec
 -- sp_pif_directory
 -- sp_country_sic_rec
 -- sp_sic_code_rec
 
*/

-------------------------------------------> Creating Function with Parameters <--------------------------------------------------

CREATE
OR replace FUNCTION fn_is_date (s TEXT) RETURNS boolean AS $DATEFIX$

BEGIN

	perform s::DATE;
	RETURN true;
	
	exception when others then
	RETURN false;

END;

$DATEFIX$ 
LANGUAGE plpgsql;

-- Function Ended