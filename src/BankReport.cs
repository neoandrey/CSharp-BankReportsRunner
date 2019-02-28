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
using System.Data.OleDb;
using System.Data.DataSetExtensions;
using System.Data.SQLite;

namespace BankReportRunner{

            public class  BankReport {

                 internal  static System.Data.DataTable[]  	       reportStagingTables;
				 internal  static Thread[]						   reportStagingThreads;
                 internal  static Dictionary<string, string>       connectionStringMap     			    = new Dictionary<string,string>();	
			     internal  static Dictionary<string, string>       partitionSizeIntervalMap   			= new Dictionary<string,string>();	    
				 internal  static string                           sourceServerConnectionString;   
				 internal  static string                           destServerConnectionString;
                 internal  static int 							   datePartitionInterval			    = 0;
				 internal  static Dictionary<int,Thread>           reportFetchThreadMap                 = new Dictionary<int,Thread>();
				 internal  static HashSet<Thread>          		   runningThreadSet                     = new HashSet<Thread>();
				 internal  static Dictionary<Thread, DataTable>    threadTableMap     			        = new Dictionary<Thread, DataTable>();
				 internal  static int                              numberOfTables						= 0; 
				 internal  static  Dictionary<int, bool>		   isFirstInsertMap                   = new Dictionary<int, bool>();   
                 internal  static StringBuilder					   emailError                          =  new StringBuilder();

				 internal  static  Dictionary<int,int>             indexSizeMap						   = new Dictionary<int,int> ();
				 internal  static  DataTable					   schemaTable                          =  new DataTable();
				 internal  static  Dictionary<int,long>            scriptToPartMinMap                        =   new Dictionary<string,string> (); 
				 internal  static  Dictionary<int,long>           scriptToPartMaxMap                        =   new Dictionary<string,string> ();               
			      static readonly object locker = new object();
				 internal static  HashSet<Thread> minMaxThreadSet                                        = new HashSet<Thread>();
				 internal static  HashSet<Thread> scriptThreadSet                                        = new HashSet<Thread>();
				 internal static  HashSet<Thread> mainReportThreadSet                                       = new  HashSet<Thread>();
				 internal static DataTable[]  reportPartitionTables; 
				 internal  static Object 		 insertLock 							= new Object();
				 public BankReport(){

						new  BankReportUtilLibrary();
						startReportGeneration();
            			
					 

				 }

                public BankReport(string  config){
				  
					
					Console.WriteLine("Starting  web server  on port: " + BankReportUtilLibrary.httpPort.ToString());
                    BankReportUtilLibrary.writeToLog("Starting  web server  on port: " + BankReportUtilLibrary.httpPort);
					string  nuConfig   = config.Contains("\\\\")? config:config.Replace("\\", "\\\\");
                 
					if(File.Exists(nuConfig)){

						   new  BankReportUtilLibrary(config);
                   		   startReportGeneration();
               			
					} else{
						
						Console.WriteLine("The specified configuration file: "+nuConfig+" does not exist. Please review configuration file parameter( -c ).");
						BankReportUtilLibrary.writeToLog("The specified configuration file: "+nuConfig+" does not exist. Please review configuration file parameter( -c ).");			
					}
					
			  }

			   public bool hasQuotes(String rawString){

				   	return rawString.StartsWith("\'") &&rawString.EndsWith("\'");

			   }
		
                public void startReportGeneration(){
                   try{ 
                        
                        initConnectionStrings();

						if(!validateParameterSet()){

							 Console.WriteLine("Report Configuration file contains invalid or incomplete parameters. Please check the file and try again.");
							 BankReportUtilLibrary.writeToLog("Report Configuration file contains invalid or incomplete parameters. Please check the file and try again.");
							 Environment.Exit(0);
						}

						if( serverIsReachable(connectionStringMap["source"])){
							if(BankReportUtilLibrary.reportOutputMethod == BankReportUtilLibrary.TABLE_OUTPUT_METHOD && string.IsNullOrEmpty(BankReportUtilLibrary.reportTableName)){
								Console.WriteLine("No table has been specified for the report data");
								BankReportUtilLibrary.writeToLog("No table has been specified for the report data");
                                Environment.Exit(0);
							}else if(BankReportUtilLibrary.reportOutputMethod == BankReportUtilLibrary.TABLE_OUTPUT_METHOD && !string.IsNullOrEmpty(BankReportUtilLibrary.reportTableName) && !serverIsReachable(connectionStringMap["destination"])){
								Console.WriteLine("Unable to connect to destination server.");
								BankReportUtilLibrary.writeToLog("Unable to connect to destination server.");
                                Environment.Exit(0);
							}else  if((BankReportUtilLibrary.reportOutputMethod == BankReportUtilLibrary.EXCEL_OUTPUT_METHOD || BankReportUtilLibrary.reportOutputMethod == BankReportUtilLibrary.TEXT_OUTPUT_METHOD ||  BankReportUtilLibrary.reportOutputMethod == BankReportUtilLibrary.CSV_OUTPUT_METHOD) && string.IsNullOrEmpty(BankReportUtilLibrary.reportFileName )){
								Console.WriteLine("No  filename has been specified for the report data");
								BankReportUtilLibrary.writeToLog("No filename  has been specified for the report data");
                                Environment.Exit(0);
							}
                         

							 string startParameterValue	     = BankReportUtilLibrary.reportStartDateValue;// hasQuotes(BankReportUtilLibrary.partitioningParameterMinVal)?BankReportUtilLibrary.partitioningParameterMinVal:quoteString(BankReportUtilLibrary.partitioningParameterMinVal) ;
							 string endParameterValue   	 = BankReportUtilLibrary.reportEndDateValue; //hasQuotes(BankReportUtilLibrary.partitioningParameterMaxVal)?BankReportUtilLibrary.partitioningParameterMaxVal:quoteString(BankReportUtilLibrary.partitioningParameterMaxVal) ;
							 int  index                      = 0;
							 minMaxThreadSet.Clear();
							 foreach(string table in BankReportUtilLibrary.mainTablesInScript){
								  isFirstInsertMap.Add(index, true);                          
								  string rangeField       =  BankReportUtilLibrary.dateFilterFields[index].ToString();
                                  string tableNolock      =  table+" WITH (NOLOCK) WHERE "+rangeField+" >= "+startParameterValue+" AND  "+rangeField+" < "+endParameterValue+" OPTION(RECOMPILE, OPTIMIZE FOR UNKNOWN)" ;
								  string partitionField   =  BankReportUtilLibrary.partitionFieldsForMainTables[index].ToString();
                                    
					
								   Thread  minThread = 	new Thread(()=> {
									   scriptToPartMinMap.Add( index, getAggregate("MIN",partitionField, tableNolock,connectionStringMap["source"]));
									
									});
									minThread.Name      = "min_thread_for_"+table+"."+index.ToString();
                                   
								    Thread  maxThread = 	new Thread(()=> {
									 scriptToPartMinMap.Add( index, getAggregate("MAX",partitionField, tableNolock,connectionStringMap["source"]));

									});
									maxThread.Name      = "max_thread_for_"+table+"."+index.ToString();
								
									minMaxThreadSet.Add(minThread);
									minMaxThreadSet.Add(maxThread);
								   ++index;
                        		}
								foreach(Thread minMaxThread in minMaxThreadSet){

									minMaxThread.Start();

								}
								 waitForMinMaxThreads(minMaxThreadSet);

                  			    string finalReportScript = File.ReadAllText(BankReportUtilLibrary.finalScriptPath);
                                
								StringBuilder finalScriptBuilder = new StringBuilder();
                                finalScriptBuilder.Append("(");
								foreach(string outTable in  BankReportUtilLibrary.scriptOutputTables){
                                  finalScriptBuilder.Append(" SELECT * FROM  "+outTable+" WITH  (NOLOCK) ");  
								  finalScriptBuilder.Append("UNION ALL"); 
								}
								for(int i=0; i<9; i++){
									finalScriptBuilder.Length--;
								}
								 finalScriptBuilder.Append(") finTable");
								  finalReportScript    =  finalReportScript.Replace("ALL_TEMP_TABLES", finalScriptBuilder.ToString();
                    		
                                bulkCopyDataFromRemoteServer(finalReportScript,BankReportUtilLibrary.finalOutputTableName);

						}else{
								Console.WriteLine("Unable to connect to source database: "+BankReportUtilLibrary.sourceServer);
								BankReportUtilLibrary.writeToLog("Unable to connect to source database: "+BankReportUtilLibrary.sourceServer);
								Environment.Exit(0);

						}

						
						
                   }catch(Exception e){

                         Console.WriteLine("Error: " + e.ToString());
						  Console.WriteLine("Error Message: " + e.Message);
                         Console.WriteLine(e.StackTrace);
						 BankReportUtilLibrary.writeToLog("Error: " + e.ToString());
						 BankReportUtilLibrary.writeToLog("Error Message: " + e.Message);
						 Console.WriteLine(e.ToString());
                   }
				    // BankReportUtilLibrary.closeLogFile();
              }
           			  public static void initConnectionStrings(){
 				  
 				    sourceServerConnectionString   =  "Network Library=DBMSSOCN;Data Source=" +  BankReportUtilLibrary.sourceConnectionProps.getSourceServer() + ","+BankReportUtilLibrary.sourcePort+";database=" + BankReportUtilLibrary.sourceConnectionProps.getSourceDatabase()+ ";User id=" + BankReportUtilLibrary.sourceConnectionProps.getSourceUser()+ ";Password=" + BankReportUtilLibrary.sourceConnectionProps.getSourcePassword() + ";Connection Timeout=0;Pooling=false;Packet Size=8192;";     
                     destServerConnectionString     =  "Network Library=DBMSSOCN;Data Source=" +  BankReportUtilLibrary.destinationConnectionProps.getSourceServer() + ","+BankReportUtilLibrary.destinationPort+";database=" + BankReportUtilLibrary.destinationConnectionProps.getSourceDatabase()+ ";User id=" +  BankReportUtilLibrary.destinationConnectionProps.getSourceUser()+ ";Password=" + BankReportUtilLibrary.destinationConnectionProps.getSourcePassword() + ";Connection Timeout=0;Pooling=false;Packet Size=8192;";    
     				connectionStringMap.Add("source",sourceServerConnectionString);
 					connectionStringMap.Add("destination",destServerConnectionString);
  
			  }


			  public static void createSQLTableFromDataTable(string destTableName, DataTable  reportTable, string connectionString){
                
			      StringBuilder destinationTableCreateBuilder   =  new  StringBuilder();
				  destinationTableCreateBuilder.Append( "IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].["+destTableName+"]') AND type in (N'U')) BEGIN  DROP TABLE ["+destTableName+"] END");
				  destinationTableCreateBuilder.Append(" CREATE TABLE ["+destTableName+"] ( ");


				  string colDataType  = "";
				  foreach(DataColumn col in reportTable.Columns){
					
					
									if(col.ColumnName.Contains("structured_data") || col.ColumnName.Contains("icc_data")){
										colDataType = "VARCHAR(MAX)";
									}else {
										if(col.DataType.Name.ToString()=="Boolean")
										{
										colDataType = "BIT";
										}
										if(col.DataType.Name.ToString()=="Byte")
										{
										colDataType = "VARBINARY(MAX)";
										}
										if(col.DataType.Name.ToString()=="Char")
										{
										colDataType = "CHAR(1)";
										}
										if(col.DataType.Name.ToString()=="DateTime")
										{
										colDataType = "DATETIME";
										}
										if(col.DataType.Name.ToString()=="Decimal")
										{
										colDataType = "DECIMAL(20,2)";
										}
										if(col.DataType.Name.ToString()=="Double")
										{
										colDataType = "FLOAT";
										}
										if(col.DataType.Name.ToString()=="Int16")
										{
										colDataType = "INT";
										}
										if(col.DataType.Name.ToString()=="Int32")
										{
										colDataType = "INT";
										}
										if(col.DataType.Name.ToString()=="Int64")
										{
										colDataType = "BIGINT";
										}
										if(col.DataType.Name.ToString()=="SByte")
										{
										colDataType = "VARBINARY(MAX)";
										}
										if(col.DataType.Name.ToString()=="Single")
										{
										colDataType = "VARBINARY(MAX)";
										}
										if(col.DataType.Name.ToString()=="String")
										
										{
											colDataType = "VARCHAR(300)";
										}
										if(col.DataType.Name.ToString()=="TimeSpan")
										
										{
											colDataType = "BIT";
										}
										if(col.DataType.Name.ToString()=="UInt16")
										
										{
											colDataType = "DATETIME";
										}
										if(col.DataType.Name.ToString()=="UInt32")
										
										{
											colDataType = "BIGINT";
										}
										if(col.DataType.Name.ToString()=="UInt64")
										
										{
											colDataType = "BIGINT";
										}
									}

                                               destinationTableCreateBuilder.Append(col.ColumnName);
											   destinationTableCreateBuilder.Append("\t");
											   destinationTableCreateBuilder.Append(colDataType);
											   destinationTableCreateBuilder.Append(",");
									
								}
									destinationTableCreateBuilder.Length--;
									destinationTableCreateBuilder.Append(" );");
									executeScript(destinationTableCreateBuilder.ToString(),  connectionString);
			  }
			  public bool serverIsReachable(string connectionStr){
				  bool isReachable   = false;

				   try{
					
                    using (SqlConnection serverConnection =  new SqlConnection(connectionStr)){
                                 serverConnection.Open();
								 isReachable   = true;
					}
                         
				   }catch(Exception e){
								   BankReportUtilLibrary.writeToLog("Error while connecting to server: " + e.Message);
								   BankReportUtilLibrary.writeToLog(e.StackTrace);
								   BankReportUtilLibrary.writeToLog(e.ToString());
								   Console.WriteLine(e.ToString());
                        }
						return isReachable;
			  }
			  
              public   DataTable  getDataFromSQL(string theScript, string targetConnectionString ){
	         
                DataTable  dt = new DataTable();

                try{
					
                    using (SqlConnection serverConnection =  new SqlConnection(targetConnectionString)){
                    SqlCommand cmd = new SqlCommand(theScript, serverConnection);
                    Console.WriteLine("Executing script: "+theScript);
                    BankReportUtilLibrary.writeToLog("Executing script: "+theScript);
                    cmd.CommandTimeout =0;
                    serverConnection.Open();
                    SqlDataReader  reader = cmd.ExecuteReader();
                    dt.Load(reader);	
                    cmd.Dispose();
					
                   }
                } catch(Exception e){
					
					if(e.Message.ToLower().Contains("transport")){
						
						 Console.WriteLine("Error while running script: "+theScript+". The error is: "+e.Message);
						 Console.WriteLine("The fetch session would now be restarted");
						 BankReportUtilLibrary.writeToLog("Error while running fetch script: "+theScript+". The error is: "+e.Message);
						 BankReportUtilLibrary.writeToLog("The data fetch session would now be restarted");
						 getDataFromSQL( theScript,  targetConnectionString );
						 BankReportUtilLibrary.writeToLog(e.ToString());
					     emailError.AppendLine("<div style=\"color:red\">Error while running fetch script: "+theScript+". The error is: "+e.Message);
						 emailError.AppendLine("<div style=\"color:red\">(Restarted):"+e.StackTrace);
				
					} else {
						
						Console.WriteLine("Error while running script: " + e.Message);
						Console.WriteLine(e.StackTrace);
						 BankReportUtilLibrary.writeToLog(e.ToString());
						 BankReportUtilLibrary.writeToLog("Error while running script: " + e.Message);
						 BankReportUtilLibrary.writeToLog(e.StackTrace);
						 Console.WriteLine(e.ToString());
						 BankReportUtilLibrary.writeToLog(e.ToString());
						emailError.AppendLine("<div style=\"color:red\">Error while running script: " + e.Message);
						emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
								
						 }
					}
					return  dt;
			  }
			  


             public System.Data.DataTable   getDataFromSourceDatabase (string script){
                            System.Data.DataTable dt = new DataTable();
                            try{

                                using (SqlConnection serverConnection =  new SqlConnection(connectionStringMap["source"])){
									SqlCommand cmd = new SqlCommand(script, serverConnection);
									BankReportUtilLibrary.writeToLog("Executing script: "+script+" on source database.");
									cmd.CommandTimeout =0;
									serverConnection.Open();
									SqlDataReader  reader = cmd.ExecuteReader();
									dt.Load(reader);	
									cmd.Dispose();
                        }
                        }catch(Exception e){
								   BankReportUtilLibrary.writeToLog("Error while running script: " + e.Message);
								   BankReportUtilLibrary.writeToLog(e.StackTrace);
								   BankReportUtilLibrary.writeToLog(e.ToString());
								   Console.WriteLine(e.ToString());
                        }
                        return dt;
                }

              				public static void  executeScript( string script, string  targetConnectionString){

						try{
							using (SqlConnection serverConnection =  new SqlConnection(targetConnectionString)){
									SqlCommand cmd = new SqlCommand(script, serverConnection);
									Console.WriteLine("Executing script: "+script);
									BankReportUtilLibrary.writeToLog("Executing script: "+script);
									cmd.CommandTimeout =0;
									serverConnection.Open();
									cmd.ExecuteNonQuery();
							}
						}catch(Exception e){

									 Console.WriteLine("Error while running script: " + e.Message);
									 Console.WriteLine(e.StackTrace);
									 BankReportUtilLibrary.writeToLog("Error while running script: " + e.Message);
									 BankReportUtilLibrary.writeToLog(e.StackTrace);
									 BankReportUtilLibrary.writeToLog(e.ToString());
									  Console.WriteLine(e.ToString());
									 emailError.AppendLine("<div style=\"color:red\">Error while running script: " + e.Message);
									 emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);

							}
				}


				
             public static void bulkCopyTableData(string server, DataTable dTab , string  destTable){

                        string  connectionStr = connectionStringMap[server];
                    
                        try{

                        using (SqlConnection bulkCopyConnection =  new SqlConnection(connectionStr)){
                            
                            using (var bulkCopy = new SqlBulkCopy(bulkCopyConnection, SqlBulkCopyOptions.TableLock|SqlBulkCopyOptions.UseInternalTransaction | SqlBulkCopyOptions.KeepNulls,null))
                            {
                                bulkCopyConnection.Open();
                                foreach (DataColumn col in dTab.Columns){
									
											bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
									
                                }						
								bulkCopy.BulkCopyTimeout = 0;
								bulkCopy.BatchSize =  BankReportUtilLibrary.batchSize;
								bulkCopy.DestinationTableName = destTable;
								bulkCopy.WriteToServer(dTab);
                            }
                         }	
                    }catch(Exception e){
                       	
								Console.WriteLine("Error while running bulk insert: " + e.Message);
								Console.WriteLine(e.StackTrace);
								BankReportUtilLibrary.writeToLog(e.ToString());
								 Console.WriteLine(e.ToString());
								BankReportUtilLibrary.writeToLog("Error while running bulk insert: " + e.Message);
								BankReportUtilLibrary.writeToLog(e.StackTrace);
								emailError.AppendLine("<div style=\"color:red\">Error while running bulk insert: " + e.Message);
								emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
						 
					}

                    
                }


      public  static  void  exportTableToCSV(DataTable dt, string  fileName){

					StringBuilder sb = new StringBuilder(); 

					if(File.Exists(fileName))
						{
							File.Delete(fileName);
						}

					IEnumerable<string> columnNames = dt.Columns.Cast<DataColumn>().
					Select(column => column.ColumnName);
					sb.AppendLine(string.Join(",", columnNames));

					foreach (DataRow row in dt.Rows)
					{
					IEnumerable<string> fields = row.ItemArray.Select(field => field.ToString());
					sb.AppendLine(string.Join(",", fields));
					}

					File.WriteAllText(fileName, sb.ToString());

	  }

        public static DataTable readStoredTableData(string strFilePath)
        {
            DataTable dt = new DataTable();
            using (StreamReader sr = new StreamReader(strFilePath))
            {
                string[] headers = sr.ReadLine().Split(new string[] { BankReportUtilLibrary.temporaryFileFieldDelimeter }, StringSplitOptions.None);
                foreach (string header in headers)
                {
                    dt.Columns.Add(header);
                }
                while (!sr.EndOfStream)
                {
                    string[] rows = sr.ReadLine().Split(new string[] { BankReportUtilLibrary.temporaryFileFieldDelimeter }, StringSplitOptions.None);
                    DataRow dr = dt.NewRow();
                    for (int i = 0; i < headers.Length; i++)
                    {
                        dr[i] = rows[i];
                    }
                    dt.Rows.Add(dr);
                }

            }
            return dt;
        }

        public static string getTempFilePath(int fileIndex)
        {

            int mapSize = BankReportUtilLibrary.tempFileDriveMap.Count;
            int modulo = fileIndex % mapSize;
            return BankReportUtilLibrary.tempFilePathMap[modulo];

        }

		public  void  exportToSQLite(int  index){
		
		string dbFileName       =  getTempFilePath(index)+ "\\" + BankReportUtilLibrary.temporaryFileNamePrefix + "_" + index.ToString() + ".sqlite";
		string sqliteTableName  =  BankReportUtilLibrary.temporaryTableName+"_"+index.ToString();
		if (!File.Exists(dbFileName)){
		 try{
            using( SQLiteConnection liteConnect = new SQLiteConnection("Data Source="+dbFileName+";Version=3;")){
			liteConnect.Open();
			string sql = "SELECT 1 FROM sqlite_master WHERE type='table' AND name='"+sqliteTableName+"';";
			Console.WriteLine("Running: "+sql);

			SQLiteCommand command = new SQLiteCommand(sql, liteConnect);
			Object result = command.ExecuteScalar();
			command.Dispose();
			Console.WriteLine("result: "+result.ToString());
			
			if(result.ToString() != "1"){
				
			  StringBuilder  createSqlBuilder   = new StringBuilder();
			  createSqlBuilder.Append(" CREATE TABLE ["+sqliteTableName+"] ( ");
			  string colDataType  = "";
			  foreach(DataColumn col in reportStagingTables[index].Columns){
		 
		 		if(col.DataType.Name.ToString()=="Boolean")
		 		{
		 		colDataType = "INTEGER";
		 		}
		 		if(col.DataType.Name.ToString()=="Byte")
		 		{
		 		colDataType = "BLOB";
		 		}
		 		if(col.DataType.Name.ToString()=="Char")
		 		{
		 		    colDataType = "TEXT";
		 		}
		 		if(col.DataType.Name.ToString()=="TEXT")
		 		{
		 		colDataType = "TEXT";
		 		}
		 		if(col.DataType.Name.ToString()=="Decimal")
		 		{
		 		colDataType = "REAL";
		 		}
		 		if(col.DataType.Name.ToString()=="Double")
		 		{
		 		colDataType = "REAL";
		 		}
		 		if(col.DataType.Name.ToString()=="Int16")
		 		{
		 		colDataType = "INTEGER";
		 		}
		 		if(col.DataType.Name.ToString()=="Int32")
		 		{
		 		colDataType = "INTEGER";
		 		}
		 		if(col.DataType.Name.ToString()=="Int64")
		 		{
		 		colDataType = "INTEGER";
		 		}
		 		if(col.DataType.Name.ToString()=="SByte")
		 		{
		 		colDataType = "BLOB";
		 		}
		 		if(col.DataType.Name.ToString()=="Single")
		 		{
		 		colDataType = "BLOB";
		 		}
		 		if(col.DataType.Name.ToString()=="String")
		 
		 		{
		 			colDataType = "TEXT";
		 		}
		 		if(col.DataType.Name.ToString()=="TimeSpan")
		 
		 		{
		 			colDataType = "TEXT";
		 		}
		 		if(col.DataType.Name.ToString()=="UInt16")
		 
		 		{
		 			colDataType = "TEXT";
		 		}
		 		if(col.DataType.Name.ToString()=="UInt32")
		 
		 		{
		 			colDataType = "INTEGER";
		 		}
		 		if(col.DataType.Name.ToString()=="UInt64")
		 
		 		{
		 			colDataType = "INTEGER";
		 		}
		 		
					createSqlBuilder.Append(col.ColumnName);
					createSqlBuilder.Append("\t");
					createSqlBuilder.Append(colDataType);
					createSqlBuilder.Append(",");

		 	 	 }
				createSqlBuilder.Length--;
				createSqlBuilder.Append(" );");		  			    
				string sql2 = createSqlBuilder.ToString();
				BankReportUtilLibrary.writeToLog("Running: "+sql2);
				SQLiteCommand command2 = new SQLiteCommand(sql2, liteConnect);
				command2.CommandTimeout = -1;
				command2.ExecuteNonQuery();
				command2.Dispose();
			
			}
			StringBuilder  tableColumnString  = new StringBuilder();
			  foreach (DataColumn column in reportStagingTables[index].Columns){
			  
			      tableColumnString.Append(column.ColumnName.ToString()).Append(",");
			 
			 }
			 tableColumnString.Length--;
			StringBuilder  insertBuilder      = new StringBuilder();
			
		   foreach (DataRow row in reportStagingTables[index].Rows){
		            insertBuilder.Append("INSERT INTO ").Append(sqliteTableName).Append("( ").Append(tableColumnString.ToString()).Append(" ) VALUES ( ");
			    foreach (DataColumn column in reportStagingTables[index].Columns){
			      
			      if(column.DataType.Name.ToString().Contains("TEXT") || column.DataType.Name.ToString().Contains("Char") || column.DataType.Name.ToString().Contains("String") ) {
			      
			        insertBuilder.Append("\'").Append(row[column].ToString()).Append(", ").Append("\'");
			    
			       } else{
			         insertBuilder.Append(row[column].ToString()).Append(", ");
			       }
			    
			      }
			      insertBuilder.Length--;
			      insertBuilder.Append(") ");
			      SQLiteCommand command3 = new SQLiteCommand(insertBuilder.ToString(), liteConnect);
			      command3.CommandTimeout = -1;
			      command3.ExecuteNonQuery();
			      command3.Dispose();
			      
			    }
			//	reportStagingTables[index].Clear();
			}	
		}catch(Exception e){
				BankReportUtilLibrary.writeToLog("Error saving table with  index "+index+" to SQLite. Message : " + e.Message);
				Console.WriteLine("Error saving table with  index "+index+" to SQLite. Message : " + e.Message);
				BankReportUtilLibrary.writeToLog(e.StackTrace);
				BankReportUtilLibrary.writeToLog(e.ToString());
				Console.WriteLine(e.StackTrace);
				Console.WriteLine(e.ToString());
				 
			  }
		}
		}


	public  static  void  loadDataFromSQLite(int  index){
	
		string dbFileName       =  getTempFilePath(index)+ "\\" + BankReportUtilLibrary.temporaryFileNamePrefix + "_" + index.ToString() + ".sqlite";
		string sqliteTableName  =  BankReportUtilLibrary.temporaryTableName+"_"+index.ToString();	
		if (File.Exists(dbFileName)){
		try{
                    using (SQLiteConnection liteConnect = new SQLiteConnection("Data Source=" + dbFileName + ";Version=3;"))
                    {
                        liteConnect.Open();
                        string sql = "SELECT  * FROM " + sqliteTableName + ";";
                        Console.WriteLine("Running: " + sql);
                        SQLiteCommand command = new SQLiteCommand(sql, liteConnect);
                        reportStagingTables[index] = new DataTable();
                        SQLiteDataReader reader = command.ExecuteReader();
                        reportStagingTables[index].Load(reader);
		}
	}catch(Exception  e){
                    BankReportUtilLibrary.writeToLog("Error reading data for table with  index " + index + " from SQLite. Message : " + e.Message);
                    Console.WriteLine("Error reading data for table with " + index + " from SQLite. Message : " + e.Message);
                    BankReportUtilLibrary.writeToLog(e.StackTrace);
                    BankReportUtilLibrary.writeToLog(e.ToString());
                    Console.WriteLine(e.StackTrace);
                    Console.WriteLine(e.ToString());
		
	}

	}else{
	  Console.WriteLine("Could not find file "+dbFileName);
                BankReportUtilLibrary.writeToLog("Could not find file "+dbFileName);
	}
			}
        public static void bulkCopyDataFromRemoteServer( string copyScript, string destTable)
        {

            createSQLTableFromDataTable(destTable, schemaTable, destServerConnectionString);

            //string  connectionStr = @"Network Library=DBMSSOCN;Data Source="+ server+","+defaultDBPort+";database=postilion_office;User id=sa;Password=Interswitch@10;Connection Timeout=0;Pooling=false;";
            try
            {
                using (SqlConnection serverConnection = new SqlConnection(sourceServerConnectionString))
                {
                   
                    SqlCommand cmd = new SqlCommand(copyScript , serverConnection);

                    cmd = new SqlCommand(copyScript, serverConnection);
                    Console.WriteLine("Executing script: " + copyScript);
                    BankReportUtilLibrary.writeToLog("Executing script: " + copyScript);
                    cmd.CommandTimeout = 0;
                    if (serverConnection != null && serverConnection.State == ConnectionState.Closed)
                    {

                        serverConnection.Open();
                    }
                    SqlDataReader reader = cmd.ExecuteReader();
                    using (SqlConnection bulkCopyConnection = new SqlConnection(destServerConnectionString))
                    {
                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(bulkCopyConnection, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction, null))
                        {
                            bulkCopyConnection.Open();
                            bulkCopy.BulkCopyTimeout	  = 0;
                            bulkCopy.BatchSize 			  = BankReportUtilLibrary.batchSize;
							bulkCopy.EnableStreaming      = false;
                            bulkCopy.DestinationTableName = destTable;
                            bulkCopy.WriteToServer(reader);
                        }
                    }
                    reader.Close();
                }
            }
            catch (Exception e)
            {

					if(!e.Message.ToLower().Contains("filegroup")){
								Console.WriteLine("Error while running fetch script: "+copyScript+". The error is: "+e.Message);
								Console.WriteLine("The data fetch session would now be restarted");
								BankReportUtilLibrary.writeToLog("Error while running fetch script: "+copyScript+". The error is: "+e.Message);
								BankReportUtilLibrary.writeToLog("The data fetch session would now be restarted");
								BankReportUtilLibrary.writeToLog(e.ToString());
								Console.WriteLine(e.ToString());
								bulkCopyDataFromRemoteServer( copyScript,  destTable); 
								emailError.AppendLine("<div style=\"color:red\">Error while running fetch script: "+copyScript +". The error is: "+e.Message);
								emailError.AppendLine("<div style=\"color:red\">(Restarted):"+e.StackTrace);
					} else {

							Console.WriteLine("Error running bulk copy: " + e.Message);
							Console.WriteLine(e.StackTrace);
							BankReportUtilLibrary.writeToLog("Error running bulk copy: " + e.Message);
							BankReportUtilLibrary.writeToLog(e.StackTrace);

					}
               
            }
        }

        public static void getSchemaTable(string sqlScript)
        {
           try{
            sqlScript =sqlScript.Contains("#")? sqlScript:"SET FMTONLY ON \n" + sqlScript;
            string targetConnectionString = sourceServerConnectionString.Replace("Network Library=DBMSSOCN", "Provider=SQLOLEDB");
            using (OleDbDataAdapter oda = new OleDbDataAdapter(sqlScript, targetConnectionString))
            {
                oda.SelectCommand.CommandTimeout = 0;
                DataSet ds = new DataSet();
                oda.Fill(ds);
                schemaTable = ds.Tables[0];
            }
            }
            catch (Exception e)
            {

                Console.WriteLine("Error getting  table schema from server with script:\n"+sqlScript +". \nError " + e.Message);
                Console.WriteLine(e.StackTrace);
                BankReportUtilLibrary.writeToLog("Error getting  table schema from server with script:\n" + sqlScript + ". \nError " + e.Message);
                BankReportUtilLibrary.writeToLog(e.StackTrace);
             


            }


        }

			 public long getAggregate (string aggr, string columnName,string tableName, string server){
				  
				   string script                       = "SELECT  aggr_val  = "+aggr+"("+columnName+") FROM "+tableName+" WITH (NOLOCK)";
				   System.Data.DataTable aggregateVal  = getDataFromSQL(script, connectionStringMap[server]);
				   return string.IsNullOrEmpty(aggregateVal.Rows[0]["aggr_val"].ToString())?0: long.Parse(aggregateVal.Rows[0]["aggr_val"].ToString());    

			}
		public static void  createFinalView(int totalTables){

		    StringBuilder  viewBuilder  = new StringBuilder();
            executeScript("IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[" + BankReportUtilLibrary.reportTableName + "]') AND type in (N'V')) BEGIN  DROP VIEW [" + BankReportUtilLibrary.reportTableName + "] END ",destServerConnectionString);
            viewBuilder.Append(" CREATE  VIEW [").Append(BankReportUtilLibrary.reportTableName).Append("]  AS ");
			for (int i = 0; i < totalTables; i++ ){
                viewBuilder.Append(" SELECT * FROM ").Append(BankReportUtilLibrary.temporaryTableName+"_"+i.ToString()).Append(" WITH (NOLOCK) UNION ALL ");
			}
            viewBuilder.Length= viewBuilder.Length -11;
            viewBuilder.Append(";");
            executeScript(viewBuilder.ToString(), destServerConnectionString);

		}
  
        public static bool validateParameterSet(){

		   bool isValidParameterSet     = true;
		   int numberOfScripts          = BankReportUtilLibrary.sourceScripts.Count;
		   int numberOfOutputTables     = BankReportUtilLibrary.scriptOutputTables.Count;
		   int numberOfPartitionFields  = BankReportUtilLibrary.partitionFieldsForMainTables.Count;
		   int numberOfDateFilterFields = BankReportUtilLibrary.dateFilterFields.Count;
		   int numberOfIndexScripts     = BankReportUtilLibrary.outputTableIndexScriptMap.Count;
		   int numberOfMainTables       = BankReportUtilLibrary.mainTablesInScript.Count;

		   if(numberOfScripts!=numberOfMainTables){
			   		isValidParameterSet  = false;
					Console.WriteLine("The number of main tables does not match the number of scripts. Please review. ");
					BankReportUtilLibrary.writeToLog("The number of main tables does not match the number of scripts. Please review.");
		   }
		   
		  if(numberOfPartitionFields != numberOfMainTables) {
	 			isValidParameterSet  = false;
				Console.WriteLine("The number of main tables does not match the number of partition Fields. Please review. ");
			    BankReportUtilLibrary.writeToLog("The number of main tables does not match the number of partition Fields. Please review.");
		  }
		  if(numberOfDateFilterFields!=numberOfScripts){
              	isValidParameterSet  = false;
				Console.WriteLine("The number of date filter fields does not match the number of scripts. Please review. ");
			    BankReportUtilLibrary.writeToLog("The number of date filter fields does not match the number of scripts. Please review.");

		   }

		   foreach(string script in BankReportUtilLibrary.sourceScripts){
			    if(!File.Exists(script)){
					 isValidParameterSet  = false;
					 Console.WriteLine("The script with path: "+script+" does not exist. Please review");
			         BankReportUtilLibrary.writeToLog("The script with path: "+script+" does not exist. Please review");
					 break;
				}
		   }
		   if(string.IsNullOrWhiteSpace(BankReportUtilLibrary.finalOutputTableName)){
			    isValidParameterSet  = false;
			    Console.WriteLine("The final output table has not been  provided. Please review. ");
			    BankReportUtilLibrary.writeToLog("The final output table has not been  provided. Please review.");

		   }

		   if(BankReportUtilLibrary.createIndexesOnOutputTables && numberOfIndexScripts !=numberOfOutputTables ){
              
			   isValidParameterSet  = false;
			   Console.WriteLine("The number of index scripts does not match the number of output tables. Please review. ");
			    BankReportUtilLibrary.writeToLog("The number of index scripts does not match the number of output tables. Please review.");	   

		   }else if (BankReportUtilLibrary.createIndexesOnOutputTables){
			   
   			foreach(KeyValuePair<string, string>  param in BankReportUtilLibrary.outputTableIndexScriptMap){
			    if(!File.Exists(param.Value)){
					 isValidParameterSet  = false;
					 Console.WriteLine("The index script ("+param.Value+") specified for the "+param.Key+" table does not exist. Please review. ");
			   		 BankReportUtilLibrary.writeToLog("The index script ("+param.Value+") specified for the "+param.Key+" table does not exist. Please review.");
					 break;
				}
		    }

		   }
		   if(!File.Exists(BankReportUtilLibrary.finalScriptPath)){

               isValidParameterSet  = false;
			    Console.WriteLine("The final script with path:"+BankReportUtilLibrary.finalScriptPath+" does not exist. Please review. ");
			   	BankReportUtilLibrary.writeToLog("The final script with path:"+BankReportUtilLibrary.finalScriptPath+" does not exist. Please review. ");
					

		   }



		   return isValidParameterSet;
		}

	  public static void waitForMinMaxThreads(HashSet<Thread> threadSet){

				  int activeThreadCount               =  threadSet.Count;
				  HashSet<Thread> completedThreadSet  =  new  HashSet<Thread>();


				  while(activeThreadCount > 0){

                        Console.WriteLine("Waiting for  " + BankReportUtilLibrary.WAIT_INTERVAL.ToString());
                     
                        Thread.Sleep(BankReportUtilLibrary.WAIT_INTERVAL);  
						activeThreadCount = 0;
						completedThreadSet.Clear();
						 Console.WriteLine("Checking for running threads...");
						 BankReportUtilLibrary.writeToLog("Checking for running threads...");
						 
						foreach(Thread queryThread  in threadSet){
								if(queryThread.IsAlive){
										++activeThreadCount;
										Console.WriteLine(queryThread.Name.ToString()+" is still running..");
						                BankReportUtilLibrary.writeToLog(queryThread.Name.ToString()+" is still running..");
                      				 
								}else{
									completedThreadSet.Add(queryThread);

								}
					
						}
        

				
						foreach(Thread completedThread  in completedThreadSet){

						      string completedThreadName  =       completedThread.Name;
							  if (completedThreadName.ToLower().Contains("min")){
							  
							   
							   
								foreach (Thread correspondingThread in threadSet){
								  if(correspondingThread.Name.ToLower() == completedThreadName.Replace("min", "max") ){
									   threadSet.Remove(completedThread);
								       threadSet.Remove(correspondingThread);
									   int scriptID =  int.Parse(completedThread.Name.Split('.')[1]);
									    Thread reportThread = 	new Thread(()=>{ 
									  		 runReportScript(scriptID);
										});
										reportThread.Name  = completedThreadName.Replace("min_thread_for_", "main_report_thread_for_");
										mainReportThreadSet.Add(reportThread);
										reportThread.Start();
									   break;
								  }
								}
                           
							   
							   }else if(completedThreadName.ToLower().Contains("max")){
									
									foreach (Thread correspondingThread in threadSet){
									  if(correspondingThread.Name.ToLower() == completedThreadName.Replace("max", "min") ){
										threadSet.Remove(correspondingThread);
										threadSet.Remove(completedThread);
										int scriptID =  int.Parse(completedThread.Name.Split('.')[1]);
									    Thread reportThread = 	new Thread(()=>{ 
									  		 runReportScript(scriptID);
										});
										reportThread.Name  = completedThreadName.Replace("max_thread_for_", "main_report_thread_for_");
										mainReportThreadSet.Add(reportThread);
										reportThread.Start();
										break;
									  }
									}
							     
								}
							  
							  

	

						}
                    

				  }

                  waitForMainReportThreads();
			  }      

			  public static  void  waitForMainReportThreads(){

                int activeThreadCount               =  mainReportThreadSet.Count;
				  HashSet<Thread> completedThreadSet  =  new  HashSet<Thread>();
			
				  while(activeThreadCount > 0){

                        Console.WriteLine("Waiting for  " + BankReportUtilLibrary.WAIT_INTERVAL.ToString());
                     
                        Thread.Sleep(BankReportUtilLibrary.WAIT_INTERVAL);  
						activeThreadCount = 0;
						 Console.WriteLine("Checking for running threads...");
						 BankReportUtilLibrary.writeToLog("Checking for running threads...");
						 
						foreach(Thread queryThread  in mainReportThreadSet){
								if(queryThread.IsAlive){
										++activeThreadCount;
										Console.WriteLine(queryThread.Name.ToString()+" is still running..");
						                BankReportUtilLibrary.writeToLog(queryThread.Name.ToString()+" is still running..");
                      				 
								}
					
						}

			  
			  }
			   Console.WriteLine("Report threads have finished Running.  The results would now be exported to output table:{0} with the script located at:{1}  ",BankReportUtilLibrary.finalOutputTableName, BankReportUtilLibrary.finalScriptPath);
               BankReportUtilLibrary.writeToLog("Report threads have finished Running.  The results would now be exported to output table:"+BankReportUtilLibrary.finalOutputTableName+" with the script located at:"+BankReportUtilLibrary.finalScriptPath);
			  }
			  

			  public static void  runReportScript(int scriptID){

				 try{

				  string scriptPath                    = BankReportUtilLibrary.sourceScripts[scriptID].ToString();
				  StringBuilder scriptBuilder          = new StringBuilder().Append(File.ReadAllText(scriptPath));
				  string scriptMainTable               = BankReportUtilLibrary.mainTablesInScript[scriptID].ToString();
				  long  minParamVal                    = scriptToPartMinMap[scriptID];
				  long  maxParamVal                    = scriptToPartMaxMap[scriptID];
                  int   numberOfThreads                = BankReportUtilLibrary.concurrentThreads;
				  long surplusRecordCount              = (maxParamVal - minParamVal) % numberOfThreads;
				  long noRecordsPerTable               = (maxParamVal - minParamVal) / numberOfThreads;
				  reportPartitionTables                = new DataTable[numberOfThreads];
				  long startRecordID                   = 0;
				  long endRecordID                     = 0;
				  string partitionField                = BankReportUtilLibrary.partitionFieldsForMainTables[scriptID].ToString();
				  scriptThreadSet                      = new HashSet<Thread>();	
				  for (int i = 1; i<= numberOfThreads; i++){
                    int  index    = (i-1);
					startRecordID = minParamVal + index*noRecordsPerTable;

					if(i < numberOfThreads){

					endRecordID  = startRecordID + noRecordsPerTable;

					}else if(i == numberOfThreads){

					endRecordID  = startRecordID + noRecordsPerTable+surplusRecordCount ;

					}

					string replacementScript = " (SELECT * FROM "+scriptMainTable+" WITH  (NOLOCK) WHERE {0}>={1} AND {3}<={4})temp ",partitionField,startRecordID,partitionField,endRecordID);
                    ;

				  Thread  scriptPartionThread = 	new Thread(()=>{ 
						  loadPartitionDataTables( "source", scriptBuilder.Replace(scriptMainTable, replacementScript), index, scriptID );
							 
						 });
						 scriptPartionThread.Name    =  "script_partition_thread_"+i.ToString()+"_for_"+scriptMainTable+"_of_script_"+scriptID.ToString();
						 scriptThreadSet.Add(scriptPartionThread);
						 scriptPartionThread.Start(); 
				  }

				  foreach(Thread  scriptThread in scriptThreadSet){

							scriptPartionThread.Join();

				  }
				  createIndexesOnScriptTable(scriptID);
				 }catch(Exception e){
					 
					Console.WriteLine("Error running script number "+scriptID.ToString()+". Details: "+e.Message);
                    writeToLog("Error running script number "+scriptID.ToString()+". Details: "+e.Message);

				 }

                  



			  }

			  public   void  loadPartitionDataTables(string sourceServer,  string bulkQuery, int tabInd, int scriptID ){
                    				
					string   targetConnectionString         = connectionStringMap[sourceServer];
				    using(SqlConnection conn = new SqlConnection(targetConnectionString)){
						reportPartitionTables[tabInd]            = new DataTable();
						
						try{ 
							if(conn.State==System.Data.ConnectionState.Closed) conn.Open();
						    using(SqlCommand cmd = new SqlCommand(bulkQuery, conn)){
								SqlDataReader dr   = cmd.ExecuteReader(CommandBehavior.SchemaOnly);
								DataTable dtSchema = dr.GetSchemaTable();

								 if (dtSchema != null){
									foreach (DataRow drow in dtSchema.Rows){
										string columnName = System.Convert.ToString(drow["ColumnName"]);
										DataColumn column = new DataColumn(columnName, (Type)(drow["DataType"]));
										column.Unique = (bool)drow["IsUnique"];
										column.AllowDBNull = (bool)drow["AllowDBNull"];
										column.AutoIncrement = (bool)drow["IsAutoIncrement"];
										if(reportPartitionTables[tabInd].Columns.Contains(columnName)) {
											reportPartitionTables[tabInd].Columns.Add(column);
										}
									}
								 }
						}
					     targetConnectionString                  = targetConnectionString.Replace("Network Library=DBMSSOCN","Provider=SQLOLEDB");
						 Console.WriteLine("Running report script: "+bulkQuery);
						 BankReportUtilLibrary.writeToLog("Running report script: "+bulkQuery);

						 using (OleDbDataAdapter oda = new OleDbDataAdapter(bulkQuery, targetConnectionString)){
								oda.SelectCommand.CommandTimeout = 0; 
								DataSet ds = new DataSet();
								oda.Fill(ds);
								reportPartitionTables[tabInd]   = ds.Tables[0];
						}
						 

					 }catch(Exception e){

							if(e.Message.ToLower().Contains("transport")){
									Console.WriteLine("Error while running report script: "+bulkQuery+". The error is: "+e.Message);
									Console.WriteLine("The data fetch session would now be restarted");
									BankReportUtilLibrary.writeToLog("Error while running report script: "+bulkQuery+". The error is: "+e.Message);
									BankReportUtilLibrary.writeToLog(e.ToString());
									BankReportUtilLibrary.writeToLog("The data fetch session would now be restarted");
									loadPartitionDataTables( sourceServer,   bulkQuery,tabInd );
									emailError.AppendLine("<div style=\"color:red\">Error while running report script: "+bulkQuery+". The error is: "+e.Message);
									emailError.AppendLine("<div style=\"color:red\">(Restarted):"+e.StackTrace);
								}else{
									Console.WriteLine("Error while running bulk insert for table "+reportPartitionTables[tabInd-1].ToString()+": " + e.Message);
									Console.WriteLine(e.StackTrace);
									BankReportUtilLibrary.writeToLog(e.ToString());
									 Console.WriteLine(e.ToString());
									BankReportUtilLibrary.writeToLog("Error while running bulk insert script "+bulkQuery+": " + e.Message);
									BankReportUtilLibrary.writeToLog(e.StackTrace);
									emailError.AppendLine("<div style=\"color:red\">Error while running bulk insert script "+bulkQuery+": " + e.Message);
									emailError.AppendLine("<div style=\"color:red\">"+e.StackTrace);
								}
					}
					}
                      bool isFirstInsert        = isFirstInsertMap[scriptID];					
					  string  outputTableName   = BankReportUtilLibrary.scriptOutputTables[scriptID].ToString();
		              
					  lock (insertLock)  
				     {  

						string server  = "destination";
						if(isFirstInsert){

								createSQLTableFromDataTable(outputTableName, reportPartitionTables[tabInd], connectionStringMap[server]);	
								isFirstInsertMap[scriptID] = false;				
						} 

						bulkCopyTableData("destination",  reportPartitionTables[tabInd], outputTableName);
				
					}
					
	  
}
public  void createIndexesOnScriptTable (int  scriptID){

      string tableName             =  BankReportUtilLibrary.scriptOutputTables[scriptID].ToString();
	  string indexScriptPath       =  BankReportUtilLibrary.outputTableIndexScriptMap[scriptID].ToString();
	  string  indexScript   	   =  File.ReadAllText(indexScriptPath).Replace("TABLE_NAME",tableName);
	  executeScript(indexScript,connectionStringMap["destination"]);
	  
}

			    public static void Main (string[] args){
				
				 string configFile 		= ""; 

					try {	
						for(int i =0; i< args.Length; i++){
							
							if (args[0].ToLower()=="-h" ||args[0].ToLower()=="help" || args[0].ToLower()=="/?" || args[0].ToLower()=="?" ){
								
								Console.WriteLine(" This application automates and optimizes the generation of Reports");
								Console.WriteLine(" Usage: ");	
								Console.WriteLine(" -c: This parameter is used to specify the configuration file to be used.");
                        		Console.WriteLine(" -s: This parameter is used to start the webserver.");
                       		    Console.WriteLine(" -h: This parameter is used to print this help message.");	

																
						   } else{
                    		
                           if(args[i].ToLower()=="-c" && (args[(i+1)] != null && args[(i+1)].Length!=0)){
								configFile =  args[(i+1)];	
					     	} 
					}   
								
							
						}
		
			
					 if(string.IsNullOrEmpty(configFile)){
						 
						 new  BankReport();
						 
					 }else {					
							new  BankReport(configFile);
					 }
					
					}catch(Exception e) {
					   
					   Console.WriteLine(e.Message);

					   Console.WriteLine(e.StackTrace);
                	 
					
					}
				

                }

            

}
			}