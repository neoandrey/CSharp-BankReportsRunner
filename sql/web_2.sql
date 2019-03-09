DECLARE
@StartDate DATETIME,

@EndDate  DATETIME


SELECT  @StartDate            = @report_date_start    

SELECT  @EndDate              = @report_date_end  

DECLARE  @list_of_source_nodes  TABLE(source_node	VARCHAR(30)) ;

INSERT INTO  @list_of_source_nodes SELECT part FROM usf_split_string(@SourceNodes,',');

DECLARE  @list_of_sink_nodes  TABLE(sink_node	VARCHAR(30)) ;

INSERT INTO  @list_of_sink_nodes SELECT part FROM usf_split_string(@SinkNodes,',');


DECLARE  @list_of_ETT  TABLE(ETT	VARCHAR(30)) ;

INSERT INTO  @list_of_ETT SELECT part FROM usf_split_string( @extended_tran_type,',');


SELECT

				NULL AS Warning,

				@StartDate as StartDate,  

				@EndDate as EndDate, 

				SourceNodeAlias = 

				(CASE 

					WHEN q.source_node_name IN (SELECT source_node FROM @list_of_source_nodes) THEN '-Our ATMs-'

					ELSE q.source_node_name

				END),

				dbo.fn_rpt_PanForDisplay(q.pan, @show_full_pan) AS pan,

				q.terminal_id, 

				q.acquiring_inst_id_code,

				q.terminal_owner,

				ISNULL(q.merchant_type,'VOID'),

                                extended_trans_type = Case When q.terminal_id in 

                                (select terminal_id from tbl_reward_OutOfband)

                                 and dbo.fn_rpt_CardGroup (q.PAN) in ('1','4')

                                 then substring(o.r_code,1,4) 

                                  else ISNULL(substring(y.extended_trans_type,1,4),'0000')end,

				ISNULL(m.Category_name,'VOID'),

				ISNULL(m.Fee_type,'VOID'),

				ISNULL(m.merchant_disc,0.0),

				ISNULL(m.fee_cap,0),

				ISNULL(m.amount_cap,999999999999.99),

				ISNULL(m.bearer,'M'),

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

				

				q.tran_nr as TranID,

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

				

				dbo.fn_rpt_isPurchaseTrx(q.tran_type) 	AS isPurchaseTrx,

				dbo.fn_rpt_isWithdrawTrx(q.tran_type) 	AS isWithdrawTrx,

				dbo.fn_rpt_isRefundTrx(q.tran_type) 		AS isRefundTrx,

				dbo.fn_rpt_isDepositTrx(q.tran_type) 		AS isDepositTrx,

				dbo.fn_rpt_isInquiryTrx(q.tran_type) 		AS isInquiryTrx,

				dbo.fn_rpt_isTransferTrx(q.tran_type) 	AS isTransferTrx,


				dbo.fn_rpt_isOtherTrx(q.tran_type) 		AS isOtherTrx,

				q.tran_reversed,

				q.pan_encrypted,

				q.from_account_id,

				q.to_account_id,

				q.payee,


				q.extended_tran_type,

                                ISNULL(y.rdm_amt,0),

                                R.Reward_Discount,

                                R.Addit_Charge,

                                R.Addit_Party,

                                R.Amount_Cap,

                                R.Fee_Cap,

                                R.Fee_Discount,

                                Late_Reversal_id = CASE

						WHEN (q.post_tran_cust_id < @rpt_tran_id1 and q.message_type = '0420') THEN 1

						ELSE 0

					        END,	

				q.totals_group,

                                q.retrieval_reference_nr+'_'+q.terminal_id+'_'+'000000'+'_'+cast((abs(q.settle_amount_impact)) as varchar(12))+'_'+q.pan as aggregate_column,

                                q.retrieval_reference_nr+'_'+q.system_trace_audit_nr+'_'+q.terminal_id+'_'+ cast((q.settle_amount_impact) as varchar(12))+'_'+q.message_type,

                q.auth_id_rsp,



                dbo.formatAmount(q.tran_cash_req,q.tran_currency_code) AS tran_cash_req, 

				dbo.formatAmount(q.tran_cash_req,q.tran_currency_code) AS tran_cash_rsp,

				dbo.formatAmount(q.tran_cash_req,q.tran_currency_code) AS tran_tran_fee_rsp,

				dbo.formatAmount(q.tran_cash_req,q.tran_currency_code) AS tran_currency_code

	 	

	FROM

				asp_visa_pos q (NOLOCK)

				left JOIN tbl_merchant_category m (NOLOCK)

				ON q.merchant_type = m.category_code 

				left JOIN tbl_merchant_account a (NOLOCK)

				ON q.card_acceptor_id_code = a.card_acceptor_id_code   

				left JOIN tbl_xls_settlement y (NOLOCK)

				

                                ON (q.terminal_id= y.terminal_id 

                                    AND q.retrieval_reference_nr = y.rr_number 


                                    AND substring (CAST (q.datetime_req AS VARCHAR(8000)), 1, 10)

                                    = substring(CAST (y.trans_date AS VARCHAR(8000)), 1, 10)

                                    and y.terminal_id not in (select terminal_id from tbl_reward_OutOfBand))

                                left JOIN tbl_reward_OutOfBand O (NOLOCK)

                                ON q.terminal_id = o.terminal_id

                                left JOIN Reward_Category r (NOLOCK)

                                ON (substring(y.extended_trans_type,1,4) = r.reward_code or (substring(o.r_code,1,4) = r.reward_code

                                                                                             and dbo.fn_rpt_CardGroup (q.PAN) in ('1','4')))

				

	WHERE 			

				

				

				--q.post_tran_cust_id >= @rpt_tran_id--'81530747'	

				--AND

				q.tran_completed = 1

				AND

				(q.recon_business_date >= @report_date_start) 

				AND 

				(q.recon_business_date <= @report_date_end) 

				AND

				q.tran_postilion_originated = 0--oremeyi changed this from '1'- 04032009 

				AND

				(

				(q.message_type IN ('0220','0200', '0400', '0420')) 


				)



				AND 

				((substring(q.totals_group,1,3) in (select substring(sink_node,4,3) from @list_of_sink_nodes)))

				AND 

					(
					
					LEFT(q.terminal_id,1 ) IN  ('2','5','6')
					OR

					LEFT(q.terminal_id,4) IN ('3IWP', '3ICP','31WP','31CP')
			
					

										)

				AND

				q.sink_node_name NOT IN ('CCLOADsnk','GPRsnk','VTUsnk','VTUSTOCKsnk','PAYDIRECTsnk','SWTMEGAsnk')------ij added SWTMEGAsnk

				AND

				q.tran_type NOT IN ('31','50')


                           --     and q.merchant_type not in ('5371')	

                                and  (

                                ((ISNULL(y.rdm_amt,0) = 0) 

                                or ((ISNULL(y.rdm_amt,0) <> 0)) and (ISNULL(y.amount,0) not in ('0','0.00'))) 

                                or 

                                (substring(y.extended_trans_type,1,4) = '1000')

                                )
                                and q.totals_group not in ('VISAGroup')

                AND

             LEFT(q.source_node_name,2) != 'SB'

             AND

             LEFT(q.sink_node_name,2) !='SB'

	AND q.source_node_name  <> 'SWTMEGAsrc'AND q.source_node_name  <> 'SWTMEGADSsrc'
				
                                                                           
                                  

				
          