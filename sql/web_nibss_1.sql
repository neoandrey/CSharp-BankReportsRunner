SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

SELECT
Warning	 = 'Warning',
StartDate = 	CONVERT(VARCHAR(8), DATEADD(D, 1,@report_date_start),112),  
EndDate   =	CONVERT(VARCHAR(8), DATEADD(D, 1,@report_date_start),112), 
SourceNodeAlias = 'Reward',
pan = 	dbo.fn_rpt_PanForDisplay(y.pan, null) ,
terminal_id	=y.terminal_id, 
acquiring_inst_id_code =	y.acquiring_inst_id_code,
terminal_owner   = 	'Reward',
merchant_type  = 	'5310',
extended_tran_type_reward     =  'BURN',
Category_name	='Discount Stores',
Fee_type	='P',
merchant_disc	=0.007500,
fee_cap	=1200,
amount_cap=	160000,
bearer	='M',
card_acceptor_id_code=	y.merchant_id, 
card_acceptor_name_loc=	substring(y.card_acceptor_name_loc,1,40), 
source_node_name=	'Reward',
sink_node_name	='Reward', 
tran_type	='00', 
rsp_code_rsp	='00', 
message_type=	'0200', 
datetime_req	=y.trans_date,
settle_amount_req=	0, 
settle_amount_rsp=	0,
settle_tran_fee_rsp=	0,
TranID	=0 ,
prev_post_tran_id = 	0, 
system_trace_audit_nr	=y.stan, 
message_reason_code	=0, 
retrieval_reference_nr	=y.rr_number, 
datetime_tran_local=	y.trans_date, 
from_account_type=	0, 
to_account_type=	0, 
settle_currency_code=	'566', 
settle_amount_impact=	0,
tran_type_desciption=	'Goods and Services',
rsp_code_description=	'Approved' ,
settle_nr_decimals	=2 ,
currency_alpha_code=	'NGN' ,
currency_name	='Naira', 
isPurchaseTrx	=1,
isWithdrawTrx=	0,
isRefundTrx	=0,
isDepositTrx	=0,
isInquiryTrx	=0,
isTransferTrx=	0,
isOtherTrx	=0,
tran_reversed=	0,
pan_encrypted=	0,
from_account_id=	0,
to_account_id	=0,
payee	=0,
extended_tran_type=	'0000',
rdm_amount            =	ISNULL(y.rdm_amt,0),
Reward_Discount       =	R.Reward_Discount,
Addit_Charge            =	R.Addit_Charge,
Addit_Party               =	R.Addit_Party,
Amount_Cap_RD            =	R.Amount_Cap,
Fee_Cap_RD              =	R.Fee_Cap,
Fee_Discount_RD         =	R.Fee_Discount,
Totalsgroup ='Reward',
aggregate_column       =	y.rr_number+'_'+y.terminal_id+'_'+'000000'+'_'+cast((abs(ISNULL(y.rdm_amt,0))) as varchar(50))+'_'+y.pan,
Unique_key=y.rr_number+'_'+'000000'+'_'+y.terminal_id+'_'+ cast((abs(ISNULL(y.rdm_amt,0))) as varchar(50))+'_'+'0200',
tran_cash_req =	0, 
tran_cash_rsp  =	0 ,
tran_tran_fee_rsp  =	0 ,
tran_currency_code    =   	0  



FROM

(
SELECT * FROM  tbl_xls_settlement(NOLOCK) WHERE
CONVERT(VARCHAR(8), trans_date,112) >= CONVERT(VARCHAR(8), DATEADD(D, 1,@report_date_end),112) AND  CONVERT(VARCHAR(8), trans_date,112) <=  CONVERT(VARCHAR(8), DATEADD(D, 1,@report_date_start),112)
and ISNULL(rdm_amt,0) <>0
and LEFT(terminal_id,1) = '2'
and extended_trans_type is not null
)y 


left JOIN Reward_Category r (NOLOCK)
ON substring(y.extended_trans_type,1,4) = r.reward_code
						
OPTION (RECOMPILE, OPTIMIZE FOR UNKNOWN)
