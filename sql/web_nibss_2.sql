 
DECLARE
@report_date_end DATETIME,

@report_date_start  DATETIME


SELECT  @report_date_start    =  @StartDate 

SELECT  @report_date_start    =  @StartDate 

DECLARE  @list_of_source_nodes  TABLE(source_node	VARCHAR(30)) ;

INSERT INTO  @list_of_source_nodes SELECT part FROM usf_split_string(@SourceNodes,',');

SELECT 
				NULL AS Warning,
				@StartDate as StartDate,  
				@EndDate as EndDate, 
				SourceNodeAlias = 
				(CASE 
					WHEN t.source_node_name IN (SELECT source_node from @list_of_source_nodes) THEN '-Our ATMs-'
										ELSE t.source_node_name
				END),
				t.pan,
				t.terminal_id, 
				t.acquiring_inst_id_code,
				t.terminal_owner,
			merchant_type =	ISNULL(t.merchant_type,'VOID'),
                                extended_tran_type_reward = 
                                Case When t.terminal_id in 
                                (select terminal_id from tbl_reward_OutOfband (nolock))
                                 and dbo.fn_rpt_CardGroup (t.PAN) in ('1','4')

                                 then substring(o.r_code,1,4) 
                                  else ISNULL(substring(y.extended_trans_type,1,4),'0000')end,
				Category_name =case when dbo.fn_rpt_MCC_Visa (t.merchant_type,t.terminal_id,T.tran_type,t.PAN) in ('1','2','3') then ISNULL(v.Category_name,'VOID') 
				else ISNULL(m.Category_name,'VOID') end,
				Fee_type =  case when dbo.fn_rpt_MCC_Visa (t.merchant_type,t.terminal_id,T.tran_type,t.PAN) in ('1','2','3') then ISNULL(v.fee_type,'VOID') 
				else ISNULL(m.Fee_type,'VOID') end,
			merchant_disc =	case when dbo.fn_rpt_MCC_Visa (t.merchant_type,t.terminal_id,T.tran_type,t.PAN) in ('1','2','3') then ISNULL(v.merchant_disc,0.0) 
				else ISNULL(m.merchant_disc,0.0) end,
				fee_cap  = case when dbo.fn_rpt_MCC_Visa (t.merchant_type,t.terminal_id,T.tran_type,t.PAN) in ('1','2','3') then ISNULL(v.fee_cap,0) 
				else ISNULL(m.fee_cap,0) end,
				amount_cap =case when dbo.fn_rpt_MCC_Visa (t.merchant_type,t.terminal_id,T.tran_type,t.PAN) in ('1','2','3') then ISNULL(v.amount_cap,999999999999.99) 
				else ISNULL(m.amount_cap,999999999999.99) end,
				bearer =case when dbo.fn_rpt_MCC_Visa (t.merchant_type,t.terminal_id,T.tran_type,t.PAN) in ('1','2','3') then ISNULL(v.bearer,'M') 
				else ISNULL(m.bearer,'M') end,
				t.card_acceptor_id_code, t.card_acceptor_name_loc, t.source_node_name,t.sink_node_name, t.tran_type, t.rsp_code_rsp,t.message_type, t.datetime_req, 
					dbo.formatAmount(t.settle_amount_req, t.settle_currency_code) AS settle_amount_req, 
				dbo.formatAmount(t.settle_amount_rsp, t.settle_currency_code) AS settle_amount_rsp,
				dbo.formatAmount(t.settle_tran_fee_rsp, t.settle_currency_code) AS settle_tran_fee_rsp,				t.post_tran_cust_id as TranID,
				t.prev_post_tran_id,t.system_trace_audit_nr,t.message_reason_code, t.retrieval_reference_nr,t.datetime_tran_local, t.from_account_type, 
				t.to_account_type, 
				t.settle_currency_code, 
				dbo.formatAmount( 			
					CASE
						WHEN (t.tran_type = '51') THEN -1 * t.settle_amount_impact
						ELSE t.settle_amount_impact
					END
					, t.settle_currency_code ) AS settle_amount_impact,				
				dbo.formatTranTypeStr(t.tran_type, t.extended_tran_type, t.message_type) as tran_type_desciption,
				dbo.formatRspCodeStr(t.rsp_code_rsp) as rsp_code_description,
				dbo.currencyNrDecimals(t.settle_currency_code) AS settle_nr_decimals,
				dbo.currencyAlphaCode(t.settle_currency_code) AS currency_alpha_code,
				dbo.currencyName(t.settle_currency_code) AS currency_name,
				dbo.fn_rpt_isPurchaseTrx(t.tran_type) 	AS isPurchaseTrx,
				dbo.fn_rpt_isWithdrawTrx(t.tran_type) 	AS isWithdrawTrx,
				dbo.fn_rpt_isRefundTrx(t.tran_type) 		AS isRefundTrx,
				dbo.fn_rpt_isDepositTrx(t.tran_type) 		AS isDepositTrx,
				dbo.fn_rpt_isInquiryTrx(t.tran_type) 		AS isInquiryTrx,
				dbo.fn_rpt_isTransferTrx(t.tran_type) 	AS isTransferTrx,
				dbo.fn_rpt_isOtherTrx(t.tran_type) 		AS isOtherTrx,
				t.tran_reversed,
				t.pan_encrypted,
				t.from_account_id,
				t.to_account_id,
				payee = isnull(t.payee,0),
				extended_tran_type = isnull(t.extended_tran_type,0),
                               rdm_amount = 0,
                                R.Reward_Discount,
                                R.Addit_Charge,
                                R.Addit_Party,
Amount_Cap_RD =R.Amount_Cap,
               Fee_Cap_RD =                  R.Fee_Cap,
               Fee_Discount_rd=                 R.Fee_Discount,
                                t.totals_group,
                aggregate_column  =  t.retrieval_reference_nr+'_'+t.terminal_id+'_'+'000000'+'_'+cast((abs(t.settle_amount_impact)) as varchar(50))+'_'+t.pan,
                Unique_key=  t.retrieval_reference_nr+'_'+t.system_trace_audit_nr+'_'+t.terminal_id+'_'+ cast((t.settle_amount_impact) as varchar(50))+'_'+t.message_type,
				
                                dbo.formatAmount(t.tran_cash_req,t.tran_currency_code) AS tran_cash_req, 
				dbo.formatAmount(t.tran_cash_req,t.tran_currency_code) AS tran_cash_rsp,
				dbo.formatAmount(t.tran_cash_req,t.tran_currency_code) AS tran_tran_fee_rsp,
		
				dbo.formatAmount(t.tran_cash_req,t.tran_currency_code) AS tran_currency_code
         into    #report_result_2   
                 	
	FROM
				 ( SELECT * FROM  post_tran_summary (NOLOCK)  WHERE
				  tran_completed = 1
				and  CONVERT(VARCHAR(8),recon_business_date,112)  >=  CONVERT(VARCHAR(8),@report_date_start,112) AND CONVERT(VARCHAR(8),recon_business_date,112)  <=  CONVERT(VARCHAR(8),@report_date_end,112)
				AND
				tran_postilion_originated = 0
				AND
				(message_type IN ('0220','0200', '0400', '0420') )

				AND
				source_node_name IN (select source_node from  @list_of_source_nodes)
				AND 
					(
					(CHARINDEX (  '3IWP', terminal_id) > 0 )OR
					(CHARINDEX (  '3ICP', terminal_id) > 0 ) OR
					(LEFT(terminal_id,1) = '2')OR--(t.terminal_id like '2%' AND t.source_node_name IN ('SWTASPPOSsrc', 'SWTASPTAMsrc'))
					(LEFT(terminal_id,1) = '5')  OR
                    (CHARINDEX ( '31WP', terminal_id) > 0 ) OR
					(CHARINDEX ( '31CP', terminal_id) > 0) OR
					( LEFT(terminal_id,1) = '6')
					)
				AND
				sink_node_name NOT IN ('CCLOADsnk','GPRsnk','PAYDIRECTsnk','SWTMEGAsnk','VTUsnk','VTUSTOCKsnk')
				AND
				tran_type NOT IN ('31','50','21')
                and merchant_type not in ('5371','2501','2504','2505','2506','2507','2508','2509','2510','2511')
                                AND  NOT  (source_node_name in ('SWTNCS2src','SWTSHOPRTsrc','SWTNCSKIMsrc','SWTNCSKI2src','SWTFBPsrc','SWTUBAsrc','SWTZIBsrc','SWTPLATsrc','SWTFBNsrc','SWTHBCsrc','SWTWEMsrc','SWTDBLPOSsrc','SWTSBPsrc','SWTSHOPsrc') AND sink_node_name--+LEFT(totals_group,3)
                               not in ('ASPPOSLMCsnk') AND not LEFT(pan,1) = '4' and not tran_type = '01')
                                and NOT (totals_group in ('VISAGroup') and acquiring_inst_id_code = '627787')
			                   and NOT (totals_group in ('VISAGroup') and sink_node_name not in ('ASPPOSVINsnk')
			                           and not(source_node_name in ('SWTFBPsrc','SWTUBAsrc','SWTZIBsrc','SWTPLATsrc','SWTFBNsrc','SWTHBCsrc','SWTWEMsrc','SWTDBLPOSsrc','SWTSBPsrc','SWTSHOPsrc')  and sink_node_name = 'ASPPOSVISsnk'))	
			                   AND NOT (source_node_name in ('SWTNCS2src','SWTSHOPRTsrc','SWTNCSKIMsrc','SWTNCSKI2src','SWTFBPsrc','SWTUBAsrc','SWTZIBsrc','SWTPLATsrc','SWTFBNsrc','SWTHBCsrc','SWTWEMsrc','SWTDBLPOSsrc','SWTSBPsrc','SWTSHOPsrc') AND sink_node_name = 'ASPPOSLMCsnk'
                                         and totals_group in ('MCCGroup','CITIDEMCC'))	

                AND
              LEFT( source_node_name,2)  <> 'SB'
              AND LEFT( sink_node_name,2)  <> 'SB'
				 and 
				      post_tran_id NOT IN (	SELECT  post_tran_id  FROM  tbl_late_reversals with  (NOLOCK) WHERE  CONVERT(VARCHAR(8),recon_business_date,112)  >=  CONVERT(VARCHAR(8),@report_date_start,112) AND CONVERT(VARCHAR(8),recon_business_date,112)  <=  CONVERT(VARCHAR(8),@report_date_end,112)
				   )
				 )t
				left JOIN tbl_merchant_category m (NOLOCK)
				ON t.merchant_type = m.category_code 
				left JOIN tbl_merchant_category_visa v (NOLOCK)
				ON t.merchant_type = v.category_code 
				left JOIN tbl_merchant_account a (NOLOCK)
				ON t.card_acceptor_id_code = a.card_acceptor_id_code   
				left JOIN tbl_xls_settlement y (NOLOCK)
				ON (t.terminal_id= y.terminal_id 
				AND t.retrieval_reference_nr = y.rr_number 
				AND substring (CAST (t.datetime_req AS VARCHAR(8000)), 1, 10)
				= substring(CAST (y.trans_date AS VARCHAR(8000)), 1, 10)
				and y.terminal_id not in (select terminal_id from tbl_reward_OutOfBand(nolock)))
				left JOIN tbl_reward_OutOfBand O (NOLOCK)
				ON t.terminal_id = o.terminal_id
				left JOIN Reward_Category r (NOLOCK)
				ON (substring(y.extended_trans_type,1,4) = r.reward_code or (substring(o.r_code,1,4) = r.reward_code  
				and dbo.fn_rpt_CardGroup (t.PAN) in ('1','4')))
				OPTION (OPTIMIZE FOR UNKNOWN)
			