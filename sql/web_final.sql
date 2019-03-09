
	SELECT  * FROM

			(SELECT * 

	FROM 

			ALL_TEMP_TABLES  --rresult 

                        --left join #temp_table ttable on (rresult.unique_key = ttable.unique_key)

where    

    post_tran_id NOT IN
         (SELECT  post_tran_id FROM tbl_late_reversals (NOLOCK)  WHERE CONVERT(date, recon_business_date) > =  '20171106'   AND  CONVERT(date, recon_business_date) <=  '20171106' )
	     and
          not(
          
          source_node_name in ('SWTNCS2src','SWTSHOPRTsrc','SWTNCSKIMsrc','SWTNCSKI2src','SWTFBPsrc','SWTUBAsrc','SWTZIBsrc','SWTPLATsrc')
          and unique_key  IN (SELECT unique_key FROM ALL_TEMP_TABLES WITH (NOLOCK) WHERE   source_node_name in ('SWTASPPOSsrc','SWTASGTVLsrc'))
          )
          AND NOT (
         sink_node_name = 'ASPPOSLMCsnk'
         and
         totals_group in ('MCCGroup','CITIDEMCC')
          AND 
          source_node_name in (
          
          'SWTNCS2src','SWTSHOPRTsrc','SWTNCSKIMsrc','SWTNCSKI2src','SWTFBPsrc','SWTUBAsrc','SWTZIBsrc','SWTPLATsrc')
           
           )

          and left(pan,1) <>'4'

	)
	A
	
	
	ORDER BY 

			datetime_tran_local,source_node_name