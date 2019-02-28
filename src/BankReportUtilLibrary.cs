using System; 
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.Server;
using System.Data.SqlTypes;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Data;
using System.Globalization;
using System.Net.Mail;
using Newtonsoft.Json.Linq;

namespace BankReportRunner{


       public class   BankReportUtilLibrary{

                public  static string 					    sourceServer        				 = "";
                public  static string 					    sourceDatabase       				 = "";
                public  static int 					        sourcePort           				 = 1433;
                public  static System.IO.StreamWriter 	    fs;
                public  static string 					    logFile								 = Directory.GetCurrentDirectory()+"\\log\\report_generator_"+DateTime.Now.ToString("yyyyMMdd_HH_mm_ss")+".log";
                public  static string 					    configFileName                       = Directory.GetCurrentDirectory()+"\\conf\\report_generator_config.json";
                public  static int  					    batchSize       				     = 100;
                public  static string 					    fromAddress   	    				 = "BankReport@interswitchgroup.com";
                public  static string 					    bccAddress    	   					 = "";
                public  static string 				        toAddress                            = "";
                public  static string 					    ccAddress     	   				     = "";
                public  static string 					    smtpServer    						 = "172.16.10.223";
                public  static int 					        smtpPort     	    			     = 25;
                public  static string 					    sender             					 = "BankReport@interswitchgroup.com";
                public  static string 					    senderPassword 	   					 = "";
                public  static bool 					    isSSLEnabled  					     = false;
	
                public  static string 					    finalOutputTableName		    	 = "";
                public  static bool 					    createIndexesOnOutputTables	         =   false;
                public  static int                          concurrentThreads                    =  1;
				public  static Dictionary<string, string>   outputTableIndexScriptMap	 			 = new  Dictionary<string, string> ();
		        public  static ConnectionProperty 		    sourceConnectionProps;
                public  static ConnectionProperty 		    destinationConnectionProps;
                public  static string                       alternateRowColour                   = "#cce0ff";
                public  static string                       emailFontFamily                      = "arial,times new roman,verdana,sans-serif;";
                public  static string                       emailFontSize                        = "11px";
                public  static string                       colour                               = "#333333";
                public  static string                       borderColour                         = "gray";
                public  static bool                         attachReportToMail                   =  false;
                public  static bool                         embedReportInMail                    =  false;
                public  static bool                         sendEmailNotification                =  false;
                public  static BankReportConfiguration      reportConfig                         =  new BankReportConfiguration();
                public  static int                          reportOutputMethod                   =  0;
                public  static string                       reportFileName                       =  "";
                public  const int                           TABLE_OUTPUT_METHOD                  =   0;
                public   const int                          EXCEL_OUTPUT_METHOD                  =   1;
                public   const int                          CSV_OUTPUT_METHOD                    =   2;
                public   const int                          MAIL_OUTPUT_METHOD                   =   3;
                public   const int                          TEXT_OUTPUT_METHOD                   =   4;
                public   const int                          DAILY_PARTITION_MODE                 =   0;
                public   const int                          WEEKLY_PARTITION_MODE                =   1;
                public   const int                          MONTHLY_PARTITION_MODE               =   2;
                public   const int                          HOURLY_PARTITION_MODE                =   4;
                public   const int                          SECONDS_PARTITION_MODE               =   5;
                public   const int                          YEARLY_PARTITION_MODE                =   6;
                public  const  int                          MINUTE_PARTITION_MODE                =   7;
                public  static long                         partitionSize                        =   0;
                public  static bool                         isDatePartitioned                    =   false;
                public  static int                          datePartitonedMode                   =   0;
                public  static string                       reportStartParameter                 =   "";
                public  static string                       reportEndParameter                   =   "";
                public static  string                       reportTableName                      =   "";
                public static  int                          WAIT_INTERVAL                        =   1000;
                public static  string                       destinationServer 			         =    "";	
                public static  string                       destinationDatabase       	         =    "";
                public static  int                          destinationPort                 	 =     0;
                public static String                        temporaryTableName                   =    "report_temp_table";
                public  static ArrayList    			    sourceScripts		                 =   new ArrayList();
				public  static ArrayList 					scriptOutputTables			         =   new ArrayList();
                public  static ArrayList                    mainTablesInScript                   =   new ArrayList();

                public static readonly object               locker                               =   new object();  

                 public static bool                       startWebServer                         =   false;

                public static ArrayList    dateFilterFields              				 	     = new ArrayList();
                public static ArrayList partitionFieldsForMainTables 					         = new ArrayList();
                public static bool      hasDateRange                 				             = true;
                public static string    reportStartDateValue                                     = "";
                public static string    reportEndDateValue                                       = "";
                public static string    reportStartDateField                                     = "";
                public static string    reportEndDateField       				                 = "";
                public static Dictionary<string,string>    reportParameterToValueMap            = new Dictionary<string,string>();
                public static string    finalScriptPath                                          = "";
                public static  Dictionary<string,string>     scriptOrderMap                       = new  Dictionary<string,string> ();           
        public   BankReportUtilLibrary(){

                       initBankReportUtilLibrary();

                }
			
      			public   BankReportUtilLibrary(string  cfgFile){
					
					   if(!string.IsNullOrEmpty(cfgFile) ){

						   string  nuCfgFile  = "";
                           Console.WriteLine("Logging report activities to file: "+logFile);
                           Console.WriteLine("");
						   Console.WriteLine("Loading configurations in  configuration file: "+cfgFile);
						   nuCfgFile           =  cfgFile.Contains("\\\\")? cfgFile:cfgFile.Replace("\\", "\\\\");

						   try{
							   if(File.Exists(nuCfgFile)){

								configFileName     = nuCfgFile;
								initBankReportUtilLibrary();

							   }
						   }catch(Exception e){
							    
								Console.WriteLine("Error reading configuration file: "+e.Message);
								Console.WriteLine(e.StackTrace);
								writeToLog("Error reading configuration file: "+e.Message);
								writeToLog(e.StackTrace);
							
						   }
					   }
				 	
		       	         		
				}
                public  void  initBankReportUtilLibrary(){

					if (!File.Exists(logFile))  {
                        
							fs = File.CreateText(logFile);
					
                    }else{
					
                    		fs = File.AppendText(logFile);
					
                    } 
					log("===========================Started Bank Report Session at "+DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")+"==============================");
					readConfigFile(configFileName);

                    Console.WriteLine("sourceServer: "+sourceServer);
                    Console.WriteLine("sourceDatabase: "+sourceDatabase);
				    if (!String.IsNullOrEmpty(sourceServer) &&  !String.IsNullOrEmpty(sourceDatabase)){

                          sourceConnectionProps      = new ConnectionProperty( sourceServer, sourceDatabase );

                    } else {

                        Console.WriteLine("Source connection details are not complete");
                        writeToLog("Source connection details are not complete");

                    }

                    if (!String.IsNullOrEmpty(destinationServer) &&  !String.IsNullOrEmpty(destinationDatabase)){

                          destinationConnectionProps      = new ConnectionProperty( destinationServer, destinationDatabase );

                    } else {

                        Console.WriteLine("Source connection details are not complete");
                        writeToLog("Destination connection details are not complete");

                    }
                     
                }

				public  static  void readConfigFile(string configFileName){
					
				    writeToLog("Reading configurations from "+configFileName+"  ...");
                    Console.WriteLine("Reading configurations from "+configFileName+"  ...");
                    try{
					    string  propertyString       = File.ReadAllText(configFileName);
                        reportConfig                 = Newtonsoft.Json.JsonConvert.DeserializeObject<BankReportConfiguration>(propertyString);  
                        sourceServer 			     = reportConfig.source_server;       	
                        sourceDatabase       		 = reportConfig.source_database;
                        sourcePort           		 = reportConfig.source_port;
                        destinationServer 			 = reportConfig.destination_server;       	
                        destinationDatabase       	 = reportConfig.destination_database;
                        destinationPort           	 = reportConfig.destination_port;
                        batchSize       		     = reportConfig.batch_size;
                        fromAddress   	    	     = reportConfig.from_address;
                        bccAddress    	   	         = reportConfig.bcc_address;
                        toAddress                    = reportConfig.to_address;
                        ccAddress     	   	         = reportConfig.cc_address;
                        smtpServer    		         = reportConfig.smtp_server;
                        smtpPort     	    		 = reportConfig.smtp_port;
                        sender             		     = reportConfig.sender;
                        senderPassword 	   	         = reportConfig.sender_password;
                        isSSLEnabled  		         = reportConfig.is_ssl_enabled;
                        sourceScripts   		     = reportConfig.source_scripts; 
                        scriptOutputTables	         = reportConfig.script_output_tables; 
                        finalOutputTableName         = reportConfig.final_output_table_name; 
                        mainTablesInScript           = reportConfig.main_tables_in_script;
                        dateFilterFields             = reportConfig.date_filter_fields;
                        partitionFieldsForMainTables = reportConfig.partition_fields_for_main_tables;
                        hasDateRange                 = reportConfig.has_date_range;
                        reportStartDateValue         = reportConfig.report_start_date_value;
                        reportEndDateValue           = reportConfig.report_end_date_value;
                        reportStartDateField         = reportConfig.report_start_date_field;
                        reportEndDateField           = reportConfig.report_end_date_field;
                        reportParameterToValueMap    = reportConfig.report_parameter_to_value_map;
                        finalScriptPath              = reportConfig.final_script_path;
                        createIndexesOnOutputTables	 = reportConfig.create_indexes_on_output_tables; 
                        concurrentThreads            = reportConfig.concurrent_threads;
                        outputTableIndexScriptMap    = reportConfig.output_table_index_script_map;                                           //readJSONMap(reportConfig.report_extra_parameter_map);
                        alternateRowColour           = reportConfig.alternate_row_colour;
                        emailFontFamily              = reportConfig.email_font_family;
                        emailFontSize                = reportConfig.email_font_size;
                        colour                       = reportConfig.colour;
                        borderColour                 = reportConfig.border_colour;
                        attachReportToMail           = reportConfig.attach_report_to_mail;
                        embedReportInMail            = reportConfig.embed_report_in_mail;
                        sendEmailNotification        = reportConfig.send_email_notification; 
                        reportOutputMethod           = reportConfig.report_output_method; 
                        reportFileName               = reportConfig.output_file_name;
                        reportTableName		         = reportConfig.output_table_name;
                        WAIT_INTERVAL                = reportConfig.wait_interval;
                        temporaryTableName           = reportConfig.temporary_table_name;
                        scriptOrderMap               = reportConfig.script_order_map;
    
                        Console.WriteLine("sourceServer: "+sourceServer);
                        Console.WriteLine("sourceDatabase: "+sourceDatabase);
						Console.WriteLine("Configurations have been successfully initialised.");

                writeToLog("Configurations have been successfully initialised.");
					
                }catch(Exception e){
                    
                    Console.WriteLine("Error reading configuration file: "+e.Message);
                    Console.WriteLine(e.StackTrace);
                    writeToLog("Error reading configuration file: "+e.Message);
                    writeToLog(e.StackTrace);
                }
            
            }
            public static void  writeToLog(string logMessage){
            lock (locker)
            {
                 fs.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")+"=>"+logMessage);
            }
            }
			 public static Dictionary<string,string>readJSONMap(ArrayList rawMap){

                    Dictionary<string, string> tempDico = new  Dictionary<string, string>();
                    string tempVal  ="";
                    if(rawMap!=null)
                    foreach(var keyVal in rawMap){
                                
                                   tempVal = keyVal.ToString();
                                   if(!string.IsNullOrEmpty(tempVal)){
                                        tempVal = tempVal.Replace("{","").Replace("}","").Replace("\"","").Trim();
                                        Console.WriteLine("tempVal: "+tempVal);
                                     if(tempVal.Split(':').Count() ==2)tempDico.Add(tempVal.Split(':')[0].Trim(),tempVal.Split(':')[1].Trim());  
                                   }  

                    }
                  return tempDico;
            }

			public static  void log(string logMessage){

				fs.WriteLine(logMessage);
				fs.Flush();
				
			}
			public static void closeLogFile(){
				fs.Close();
			}


       }

}