
DECLARE @temp_table_1  TABLE   (aggregate_column varchar(200), counts float )

INSERT INTO @temp_table_1 select aggregate_column, count(aggregate_column) from ALL_TEMP_TABLES
GROUP BY aggregate_column

/*CREATE INDEX ix_aggregate_column ON @temp_table_1  ( aggregate_column) */
 
 SELECT * FROM(
SELECT
       [Warning]
      ,[StartDate]
      ,[EndDate]
      ,[SourceNodeAlias]
      ,[pan]
      ,[terminal_id]
      ,[acquiring_inst_id_code]
      ,[terminal_owner]
      ,[merchant_type]
      ,[extended_tran_type_reward]
      ,[Category_name]
      ,[Fee_type]
      ,[merchant_disc]
      ,[fee_cap]
      ,[amount_cap]
      ,[bearer]
      ,[card_acceptor_id_code]
      ,[card_acceptor_name_loc]
      ,[source_node_name]
      ,[sink_node_name]
      ,[tran_type]
      ,[rsp_code_rsp]
      ,[message_type]
      ,[datetime_req]
      ,[settle_amount_req]
      ,[settle_amount_rsp]
      ,[settle_tran_fee_rsp]
      ,[TranID]
      ,[prev_post_tran_id]
      ,[system_trace_audit_nr]
      ,[message_reason_code]
      ,[retrieval_reference_nr]
      ,[datetime_tran_local]
      ,[from_account_type]
      ,[to_account_type]
      ,[settle_currency_code]
      ,[settle_amount_impact]
      , tran_type_desciption = CASE WHEN aggregate_column in (select aggregate_column from  @temp_table_1 where counts >2)  THEN   tran_type_desciption+ '_M' ELSE  [tran_type_desciption] END
      ,[rsp_code_description]
      ,[settle_nr_decimals]
      ,[currency_alpha_code]
      ,[currency_name]
      ,[isPurchaseTrx]
      ,[isWithdrawTrx]
      ,[isRefundTrx]
      ,[isDepositTrx]
      ,[isInquiryTrx]
      ,[isTransferTrx]
      ,[isOtherTrx]
      ,[tran_reversed]
      ,[pan_encrypted]
      ,[from_account_id]
      ,[to_account_id]
      ,[payee]
      ,[extended_tran_type]
      ,[rdm_amount]
      ,[Reward_Discount]
      ,[Addit_Charge]
      ,[Addit_Party]
      ,[Amount_Cap_RD]
      ,[Fee_Cap_RD]
      ,[Fee_Discount_RD]
      ,[Totalsgroup]
      ,[aggregate_column]
      ,[Unique_key]
      ,[tran_cash_req]
      ,[tran_cash_rsp]
      ,[tran_tran_fee_rsp]
      ,[tran_currency_code]
	FROM 
			ALL_TEMP_TABLES 
where    

        not  (source_node_name in ('SWTNCS2src','SWTSHOPRTsrc','SWTNCSKIMsrc','SWTNCSKI2src','SWTFBPsrc','SWTUBAsrc','SWTZIBsrc','SWTPLATsrc','SWTFBNsrc','SWTHBCsrc','SWTWEMsrc','SWTDBLPOSsrc','SWTSBPsrc','SWTSHOPsrc')
          and unique_key  IN ( select unique_key from ALL_TEMP_TABLES where source_node_name in ('SWTASPPOSsrc','SWTASGTVLsrc')))
      
      )a
	ORDER BY 
			source_node_name, datetime_req, message_type
			 OPTION (RECOMPILE, OPTIMIZE FOR UNKNOWN)



















