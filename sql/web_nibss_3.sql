SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

DECLARE  @list_of_source_nodes  TABLE(source_node	VARCHAR(30)) ;

INSERT INTO  @list_of_source_nodes SELECT part FROM usf_split_string('',',');

 SELECT 
		     				NULL AS Warning,
		     				@StartDate as StartDate,  
		     				@EndDate as EndDate, 
		     				SourceNodeAlias = 
		     				(CASE 
		     					WHEN q.source_node_name IN (SELECT source_node FROM @list_of_source_nodes) THEN '-Our ATMs-'
		     					ELSE q.source_node_name
		     				END),
		     				dbo.fn_rpt_PanForDisplay(q.pan, null) AS pan,
		     				q.terminal_id, 
		     				q.acquiring_inst_id_code,
		     				q.terminal_owner,
		     				--q.merchant_type,
		     				ISNULL(q.merchant_type,'VOID')as merchant_type,
		                                     extended_tran_type_reward = 
		                                     Case When q.terminal_id in 
		                                     (select terminal_id from tbl_reward_OutOfband (NOLOCK))
		                                      and dbo.fn_rpt_CardGroup (q.PAN) in ('1','4')
		     
		                                      then substring(o.r_code,1,4) 
		                                       else ISNULL(substring(y.extended_trans_type,1,4),'0000')end,
		case when dbo.fn_rpt_MCC_Visa (q.merchant_type,q.terminal_id,q.tran_type,q.PAN) in ('1','2','3') then ISNULL(v.Category_name,'VOID') 
				     				else ISNULL(m.Category_name,'VOID') end as Category_name,
				     				case when dbo.fn_rpt_MCC_Visa (q.merchant_type,q.terminal_id,q.tran_type,q.PAN) in ('1','2','3') then ISNULL(v.fee_type,'VOID') 
				     				else ISNULL(m.Fee_type,'VOID') end as Fee_type,
				     				case when dbo.fn_rpt_MCC_Visa (q.merchant_type,q.terminal_id,q.tran_type,q.PAN) in ('1','2','3') then ISNULL(v.merchant_disc,0.0) 
				     				else ISNULL(m.merchant_disc,0.0) end as merchant_disc,
				     				case when dbo.fn_rpt_MCC_Visa (q.merchant_type,q.terminal_id,q.tran_type,q.PAN) in ('1','2','3') then ISNULL(v.fee_cap,0) 
				     				else ISNULL(m.fee_cap,0) end as fee_cap,
				     				case when dbo.fn_rpt_MCC_Visa (q.merchant_type,q.terminal_id,q.tran_type,q.PAN) in ('1','2','3') then ISNULL(v.amount_cap,999999999999.99) 
				     				else ISNULL(m.amount_cap,999999999999.99) end as amount_cap,
				     				case when dbo.fn_rpt_MCC_Visa (q.merchant_type,q.terminal_id,q.tran_type,q.PAN) in ('1','2','3') then ISNULL(v.bearer,'M') 
		     				else ISNULL(m.bearer,'M') end as bearer,
		     				
		     				
		     				
		     				q.card_acceptor_id_code, 
		     				q.card_acceptor_name_loc, 
		     				q.source_node_name,
		     				q.sink_node_name, 
		     				q.tran_type, 
		     				q.rsp_code_rsp, 
		     				q.message_type, 
		     				q.datetime_req, 
		     				
		     				
		     				dbo.formatAmount(q.settle_amount_req, q.settle_currency_code) AS settle_amount_req, 
		     				dbo.formatAmount(q.settle_amount_rsp, q.settle_currency_code) AS settle_amount_rsp,
		     				dbo.formatAmount(q.settle_tran_fee_rsp, q.settle_currency_code) AS settle_tran_fee_rsp,
		     				
		     				q.post_tran_cust_id as TranID,
		     				q.prev_post_tran_id, 
		     				q.system_trace_audit_nr, 
		     				q.message_reason_code, 
		     				q.retrieval_reference_nr, 
		     				q.datetime_tran_local, 
		     				q.from_account_type, 
		     				q.to_account_type, 
		     				q.settle_currency_code, 
		     				
		     				
		     				--dbo.formatAmount(q.settle_amount_impact, q.settle_currency_code) as settle_amount_impact,
		     				
		     				dbo.formatAmount( 			
		     					CASE
		     						WHEN (q.tran_type = '51') THEN -1 * q.settle_amount_impact
		     						ELSE q.settle_amount_impact
		     					END
		     					, q.settle_currency_code ) AS settle_amount_impact,				
		     				
		     				dbo.formatTranTypeStr(q.tran_type, q.extended_tran_type, q.message_type) as tran_type_desciption,
		     				dbo.formatRspCodeStr(q.rsp_code_rsp) as rsp_code_description,
		     				dbo.currencyNrDecimals(q.settle_currency_code) AS settle_nr_decimals,
		     				dbo.currencyAlphaCode(q.settle_currency_code) AS currency_alpha_code,
		     				dbo.currencyName(q.settle_currency_code) AS currency_name,
		     				
		     				dbo.fn_rpt_isPurchaseTrx(tran_type) 	AS isPurchaseTrx,
		     				dbo.fn_rpt_isWithdrawTrx(tran_type) 	AS isWithdrawTrx,
		     				dbo.fn_rpt_isRefundTrx(tran_type) 		AS isRefundTrx,
		     				dbo.fn_rpt_isDepositTrx(tran_type) 		AS isDepositTrx,
		     				dbo.fn_rpt_isInquiryTrx(tran_type) 		AS isInquiryTrx,
		     				dbo.fn_rpt_isTransferTrx(tran_type) 	AS isTransferTrx,
		     				dbo.fn_rpt_isOtherTrx(tran_type) 		AS isOtherTrx,
		     				q.tran_reversed,
		     				q.pan_encrypted,
		     				q.from_account_id,
		     				q.to_account_id,
		     				isnull(q.payee,0)as payee,
		     				isnull(q.extended_tran_type,0) extended_tran_type ,
		                                     0  rdm_amount,
		                                     R.Reward_Discount,
		                                     R.Addit_Charge,
		                                     R.Addit_Party,
		                                    Amount_Cap_RD = R.Amount_Cap,
		                               Fee_Cap_RD =    R.Fee_Cap,
		                               Fee_Discount_RD=      R.Fee_Discount,
		                                     q.totals_group,
		                      q.retrieval_reference_nr+'_'+q.terminal_id+'_'+'000000'+'_'+cast((abs(q.settle_amount_impact)) as varchar(50))+'_'+q.pan as aggregate_column,
		                      q.retrieval_reference_nr+'_'+q.system_trace_audit_nr+'_'+q.terminal_id+'_'+ cast((q.settle_amount_impact) as varchar(50))+'_'+q.message_type as unique_key,
		     				
		                                     dbo.formatAmount(q.tran_cash_req,q.tran_currency_code) AS tran_cash_req, 
		     				dbo.formatAmount(q.tran_cash_req,q.tran_currency_code) AS tran_cash_rsp,
		     				dbo.formatAmount(q.tran_cash_req,q.tran_currency_code) AS tran_tran_fee_rsp,
		     		
		     				dbo.formatAmount(q.tran_cash_req,q.tran_currency_code) AS tran_currency_code
		            
		     	FROM
		     				 (  
							    SELECT  * FROM  asp_visa_pos (NOLOCK) WHERE tran_completed = 1
and
CONVERT(VARCHAR(8),recon_business_date,112)  >=  CONVERT(VARCHAR(8),@report_date_start,112) AND CONVERT(VARCHAR(8),recon_business_date,112)  <=  CONVERT(VARCHAR(8),@report_date_end,112)
AND
tran_postilion_originated = 0--oremeyi changed this from '1'- 04032009 
AND
(message_type IN ('0220','0200', '0400', '0420') )

AND     				    						    				
sink_node_name NOT IN ('CCLOADsnk','GPRsnk','PAYDIRECTsnk','SWTMEGAsnk','VTUsnk','VTUSTOCKsnk')------ij added SWTMEGAsnk
AND
tran_type NOT IN ('31','50','21')
and merchant_type not in ('5371','2501','2504','2505','2506','2507','2508','2509','2510','2511')
AND
LEFT( source_node_name,2)  <> 'SB'
AND LEFT( sink_node_name,2)  <> 'SB' --AND not( sink_node_name   LIKE 'SB%')

							 )q
		     				left JOIN tbl_merchant_category m (NOLOCK)
		     				ON q.merchant_type = m.category_code 
		     				left JOIN tbl_merchant_category_visa v (NOLOCK)
		     				ON q.merchant_type = v.category_code 
		     				left JOIN tbl_merchant_account a (NOLOCK)
		     				ON q.card_acceptor_id_code = a.card_acceptor_id_code   
		     				left JOIN tbl_xls_settlement y (NOLOCK)
		     				
		                                     ON (q.terminal_id= y.terminal_id 
		                                         AND q.retrieval_reference_nr = y.rr_number 
		                                         --AND q.system_trace_audit_nr = y.stan
		                                         --AND (-1 * q.settle_amount_impact)/100 = y.amount
		                                         AND substring (CAST (q.datetime_req AS VARCHAR(8000)), 1, 10)
		                                         = substring(CAST (y.trans_date AS VARCHAR(8000)), 1, 10)
		                                      and y.terminal_id not in (select terminal_id from tbl_reward_OutOfBand(NOLOCK)))
		                                     left JOIN tbl_reward_OutOfBand O (NOLOCK)
		                                     
		                                      ON q.terminal_id = o.terminal_id
		                                     left JOIN Reward_Category r (NOLOCK)
		                                     ON (substring(y.extended_trans_type,1,4) = r.reward_code or (substring(o.r_code,1,4) = r.reward_code  
		                                                                                                 and dbo.fn_rpt_CardGroup (q.PAN) in ('1','4')))              
              
              OPTION (RECOMPILE, OPTIMIZE FOR UNKNOWN)

				