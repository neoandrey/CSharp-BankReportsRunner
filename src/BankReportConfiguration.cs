
using  System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace BankReportRunner
{

    public class BankReportConfiguration
    {

            public         string     source_server                { set; get;} 
            public string     source_database                      { set; get;} 
            public int        source_port                   		  { set; get;} 
            public string     destination_server                  { set; get;} 
            public string     destination_database                 { set; get;} 
            public int        destination_port                    { set; get;} 
            public ArrayList  source_scripts                   { set; get;} 
            public ArrayList    script_output_tables                {set; get;}
            public string       final_output_table_name              {set; get;} 
            public bool        create_indexes_on_output_tables      {set; get;}
            public Dictionary<string,string>    output_table_index_script_map            {set; get;}
            public  ArrayList main_tables_in_script             {set; get;}
            public ArrayList  date_filter_fields                {set; get;}
            public  ArrayList partition_fields_for_main_tables  {set;  get;}
            public bool     has_date_range                         {set; get;}
            public string    report_start_date_value               {set; get;}
            public string    report_end_date_value                  {set; get;}
            public string    report_start_date_field              {set; get;}
            public string    report_end_date_field                  {set; get;}

            public  Dictionary<string,string>  report_parameter_to_value_map             {set; get;}
            public  string  final_script_path                   {set; get;}

              public int      concurrent_threads                   { set; get;} 
             public int       batch_size                         { set; get;}
            public string     to_address                   		{ set; get;} 
            public string     from_address                  		{ set; get;} 
            public string     bcc_address                  		{ set; get;} 
            public string     cc_address                   		{ set; get;} 
            public string     smtp_server                   		{ set; get;} 
            public int        smtp_port                  		{ set; get;} 
            public string     sender                   		{ set; get;} 
            public string     sender_password                   	{ set; get;} 
            public bool       is_ssl_enabled                   	  {set; get;} 
            public string     alternate_row_colour                 { set; get;} 
            public string     email_font_family                  { set; get;} 
            public string     email_font_size                  	{ set; get;} 
            public string     color                 			{ set; get;} 
            public string     border_color                   		{ set; get;} 
            public bool       attach_report_to_mail                { set; get;} 
            public bool       embed_report_in_mail                 { set; get;} 
            public bool       send_email_notification              { set; get;}     
            public int        report_output_method                  { set; get;} 
            public int         wait_interval                      { set; get;}
            public string      colour                             { set; get;}
            public string      border_colour                      { set; get;}
             public string     temporary_table_name              { set; get; }
             public string     output_file_name                  { set; get; }
             public string     output_table_name                 { set; get;} 

             public Dictionary<string,string>  script_order_map  {set; get;}

             public  int   connection_packet_size               {set; get;}

             public bool  break_all_queries_into_threads        {set; get;}       

             public  bool save_passwords  {set; get;}
             
             
            
    }
}