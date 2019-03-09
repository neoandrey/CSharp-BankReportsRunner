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

					WHEN c.source_node_name IN (SELECT source_node FROM @list_of_source_nodes) THEN '-Our ATMs-'

					ELSE c.source_node_name

				END),

				dbo.fn_rpt_PanForDisplay(c.pan, @show_full_pan) AS pan,

				c.terminal_id, 

				t.acquiring_inst_id_code,

				c.terminal_owner,

				merchant_type = ISNULL(c.merchant_type,'VOID'),

                extended_trans_type = Case When  dbo.fn_rpt_CardGroup (t.PAN) in ('1','4')
                                 and   c.terminal_id in  (select terminal_id from tbl_reward_OutOfband ) 

                                 then substring(o.r_code,1,4) 

                                  else ISNULL(substring(y.extended_trans_type,1,4),'0000')end,

				ISNULL(m.Category_name,'VOID'),

				ISNULL(m.Fee_type,'VOID'),

				ISNULL(m.merchant_disc,0.0),

				ISNULL(m.fee_cap,0),

				ISNULL(m.amount_cap,999999999999.99),

				ISNULL(m.bearer,'M'),

				c.card_acceptor_id_code, 

				c.card_acceptor_name_loc, 

				c.source_node_name,

				t.sink_node_name, 

				t.tran_type, 

				t.rsp_code_rsp, 

				t.message_type, 

				t.datetime_req, 

				

				dbo.formatAmount(t.settle_amount_req, t.settle_currency_code) AS settle_amount_req, 

				dbo.formatAmount(t.settle_amount_rsp, t.settle_currency_code) AS settle_amount_rsp,

				dbo.formatAmount(t.settle_tran_fee_rsp, t.settle_currency_code) AS settle_tran_fee_rsp,

				

				t.post_tran_id as TranID,

				t.prev_post_tran_id, 

				t.system_trace_audit_nr, 

				t.message_reason_code, 

				t.retrieval_reference_nr, 

				t.datetime_tran_local, 

				t.from_account_type, 

				t.to_account_type, 

				t.settle_currency_code, 

				

				--dbo.formatAmount(t.settle_amount_impact, t.settle_currency_code) as settle_amount_impact,

				

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

				c.pan_encrypted,

				t.from_account_id,

				t.to_account_id,

				t.payee,


				t.extended_tran_type,

                                ISNULL(y.rdm_amt,0),

                                R.Reward_Discount,

                                R.Addit_Charge,

                                R.Addit_Party,

                                R.Amount_Cap,

                                R.Fee_Cap,

                                R.Fee_Discount,

                                Late_Reversal_id = CASE

						WHEN t.post_tran_cust_id IN (SELECT  tran_post_tran_cust_id FROM tbl_late_reversals l  where  CONVERT(DATE, recon_business_date) = '20171106' ) THEN 1

						ELSE 0

					        END,	

				c.totals_group,

                                t.retrieval_reference_nr+'_'+c.terminal_id+'_'+'000000'+'_'+cast((abs(t.settle_amount_impact)) as varchar(12))+'_'+t.pan,

                                t.retrieval_reference_nr+'_'+t.system_trace_audit_nr+'_'+c.terminal_id+'_'+ cast((t.settle_amount_impact) as varchar(12))+'_'+t.message_type,

                t.auth_id_rsp,



dbo.formatAmount(t.tran_cash_req,t.tran_currency_code) AS tran_cash_req, 

				dbo.formatAmount(t.tran_cash_req,t.tran_currency_code) AS tran_cash_rsp,

				dbo.formatAmount(t.tran_cash_req,t.tran_currency_code) AS tran_tran_fee_rsp,

				dbo.formatAmount(t.tran_cash_req,t.tran_currency_code) AS tran_currency_code

	 	

FROM 

( SELECT post_tran_cust_id FROM  post_tran  WITH  (NOLOCK) WHERE  CONVERT(DATE, recon_business_date) = '20171106' ) d 
JOIN 
 (SELECT 
[post_tran_id]
      ,[post_tran_cust_id]
      ,[prev_post_tran_id]
      ,[sink_node_name]
      ,[tran_postilion_originated]
      ,[tran_completed]
      ,[message_type]
      ,[tran_type]
      ,[tran_nr]
      ,[system_trace_audit_nr]
      ,[rsp_code_req]
      ,[rsp_code_rsp]
      ,[abort_rsp_code]
      ,[auth_id_rsp]
      ,[retention_data]
      ,[acquiring_inst_id_code]
      ,[message_reason_code]
      ,[retrieval_reference_nr]
      ,[datetime_tran_gmt]
      ,[datetime_tran_local]
      ,[datetime_req]
      ,[datetime_rsp]
      ,[realtime_business_date]
      ,[recon_business_date]
      ,[from_account_type]
      ,[to_account_type]
      ,[from_account_id]
      ,[to_account_id]
      ,[tran_amount_req]
      ,[tran_amount_rsp]
      ,[settle_amount_impact]
      ,[tran_cash_req]
      ,[tran_cash_rsp]
      ,[tran_currency_code]
      ,[tran_tran_fee_req]
      ,[tran_tran_fee_rsp]
      ,[tran_tran_fee_currency_code]
      ,[settle_amount_req]
      ,[settle_amount_rsp]
      ,[settle_tran_fee_req]
      ,[settle_tran_fee_rsp]
      ,[settle_currency_code]
      ,[tran_reversed]
      ,[prev_tran_approved]
      ,[extended_tran_type]
      ,[payee]
      ,[online_system_id]
      ,[receiving_inst_id_code]
      ,[routing_type]
       
       FROM  post_tran tt WITH  (NOLOCK)
       
       )t
       on
       d.post_tran_cust_id = t.post_tran_cust_id
       JOIN  
       
       (SELECT [post_tran_cust_id],[source_node_name],[pan] ,[card_seq_nr],[expiry_date],[terminal_id],[terminal_owner],[card_acceptor_id_code],[merchant_type],[card_acceptor_name_loc],[address_verification_data],[totals_group],[pan_encrypted]  
         FROM  post_tran_cust c WITH (NOLOCK) ) c
         on
         c.post_tran_cust_id =d.post_tran_cust_id
         and 
          post_tran_id NOT IN
         (SELECT  post_tran_id FROM tbl_late_reversals (NOLOCK)  WHERE CONVERT(date, recon_business_date) =  '20171106'  )
         
         JOIN tbl_merchant_category m (NOLOCK)

				ON c.merchant_type = m.category_code 

				left JOIN tbl_merchant_account a (NOLOCK)

				ON c.card_acceptor_id_code = a.card_acceptor_id_code   

				left JOIN tbl_xls_settlement y (NOLOCK)

				

                                ON (c.terminal_id= y.terminal_id 

                                    AND t.retrieval_reference_nr = y.rr_number 

                                    --AND t.system_trace_audit_nr = y.stan

                                    --AND (-1 * t.settle_amount_impact)/100 = y.amount

                                    AND substring (CAST (t.datetime_req AS VARCHAR(8000)), 1, 10)

                                    = substring(CAST (y.trans_date AS VARCHAR(8000)), 1, 10)

                                    and y.terminal_id not in (select terminal_id from tbl_reward_OutOfBand))

                                left JOIN tbl_reward_OutOfBand O (NOLOCK)

                                ON c.terminal_id = o.terminal_id

                                left JOIN Reward_Category r (NOLOCK)

                                ON (substring(y.extended_trans_type,1,4) = r.reward_code or (substring(o.r_code,1,4) = r.reward_code

                                                                                             and dbo.fn_rpt_CardGroup (c.PAN) in ('1','4')))
                                                                                             
  WHERE
  

				t.tran_completed = 1

				AND

				t.tran_postilion_originated = 0--oremeyi changed this from '1'- 04032009 

				AND

				(

				(t.message_type IN ('0220','0200', '0400', '0420')) 

				)



				AND 

				((substring(c.totals_group,1,3) in (select substring(sink_node,4,3) from @list_of_sink_nodes))

               or ((substring (t.sink_node_name,4,3) in (select substring(sink_node,4,3) from @list_of_sink_nodes)) and t.sink_node_name <> 'SWTASPPOSsnk')
               or (t.extended_tran_type in (select ETT from @list_of_ETT ) and t.sink_node_name = 'ESBCSOUTsnk'))

                            



				AND

				c.source_node_name IN (SELECT source_node FROM @list_of_source_nodes)

				AND 

					(

					LEFT(c.terminal_id,1 ) IN  ('2','5','6')
					OR

					LEFT(c.terminal_id,4) IN ('3IWP', '3ICP','31WP','31CP')
			

										)

				AND

				t.sink_node_name NOT IN ('CCLOADsnk','GPRsnk','VTUsnk','VTUSTOCKsnk','PAYDIRECTsnk','SWTMEGAsnk')------ij added SWTMEGAsnk

				AND

				t.tran_type NOT IN ('31','50')


                               -- and t.merchant_type not in ('5371')	

                                and  (

                                ((ISNULL(y.rdm_amt,0) = 0) 

                                or ((ISNULL(y.rdm_amt,0) <> 0)) and (ISNULL(y.amount,0) not in ('0','0.00'))) 

                                or 

                                (substring(y.extended_trans_type,1,4) = '1000')

                                )

                                AND  NOT  (c.source_node_name in ('SWTNCS2src','SWTSHOPRTsrc','SWTNCSKIMsrc','SWTNCSKI2src','SWTFBPsrc') AND t.sink_node_name--+LEFT(totals_group,3)
                               not in ('ASPPOSLMCsnk') AND not LEFT(c.pan,1) = '4' and not t.tran_type = '01' and not t.sink_node_name in ('ASPPOSMICsnk'))

                                and c.totals_group not in ('VISAGroup')

                AND

             LEFT( c.source_node_name,2)!='SB'

             AND

             LEFT(t.sink_node_name,2)!='SB'

	AND c.source_node_name  <> 'SWTMEGAsrc'AND c.source_node_name  <> 'SWTMEGADSsrc'
option(recompile) 
                                                                                             
                                  

				
          