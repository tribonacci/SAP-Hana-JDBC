package test;
/*
 * running Hana Queries in parallel
 * using java multithreding
 * Author: Vikash Singh
 */
import java.sql.*;
public class test_tmp extends Thread
{
	//One thread per query
	  private static int NUM_OF_THREADS = 28;
	  //String array of queries
	  static String query[] = new String[] {"CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_VENDOR_ASSESSMENT\" ('11717', '11720', '1=1', '3', '1=1');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_ANALYSIS_BOX\"('11717', '11720', 'DC_OH', '1=1', 'WEEK', '0', '0', '0', '0');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_ANALYSIS_BOX\"('11717', '11720', 'COST_QTY', '1=1', 'WEEK', '0', '0', '0', '0');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_ANALYSIS_BOX\"('11717', '11720', 'COST', '1=1', 'WEEK', '0', '0', '0', '0');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_VENDOR_ASSESSMENT\" ('11717', '11720', '1=1', '5', '1=1');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_ANALYSIS_BOX\"('11717', '11720', 'INV_QTY', '1=1', 'WEEK', '0', '0', '0', '0');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_VENDOR_ASSESSMENT\" ('11717', '11720', '1=1', '6', '1=1');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_VENDOR_ASSESSMENT\" ('11717', '11720', '1=1', '8', '1=1');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_ANALYSIS_BOX\"('11717', '11720', 'INV_DOL', '1=1', 'WEEK', '0', '0', '0', '0');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_VENDOR_ASSESSMENT\" ('11717', '11720', '1=1', '4', '1=1');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_ANALYSIS_BOX\"('11717', '11720', 'IMU_PER', '1=1', 'WEEK', '0', '0', '0', '0');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_HR_CHART\"('11717', '11720', '1000', ' PRIVATE_BRAND_IND_COALESCE,  CHANNEL_CODE', '1=1', '0', '0');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_ANALYSIS_BOX\"('11717', '11720', 'AUC', '1=1', 'WEEK', '0', '0', '0', '0');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_VENDOR_ASSESSMENT\" ('11717', '11720', '1=1', '20', '1=1');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_VENDOR_ASSESSMENT\" ('11717', '11720', '1=1', '2', '1=1');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_VENDOR_ASSESSMENT\" ('11717', '11720', '1=1', '1', '1=1');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_VENDOR_ASSESSMENT\" ('11717', '11720', '1=1', '15', '1=1');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_ANALYSIS_BOX\"('11717', '11720', 'AUR', '1=1', 'WEEK', '0', '0', '0', '0');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_ANALYSIS_BOX\"('11717', '11720', 'SALES_QTY', '1=1', 'WEEK', '0', '0', '0', '0');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_HR_CHART\"('11717', '11720', '1000', ' PRIVATE_BRAND_IND_COALESCE,  CHANNEL_CODE,  ACCTG_DEPT_NBR,  ACCTG_DEPT_DESC', '1=1', '0', '0');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_HR_CHART\"('11717', '11720', '1000', 'PRIVATE_BRAND_IND_COALESCE', '1=1', '0', '0');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_ANALYSIS_BOX\"('11717', '11720', 'STORE_OH', '1=1', 'WEEK', '0', '0', '0', '0');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_ANALYSIS_BOX\"('11717', '11720', 'SALES', '1=1', 'WEEK', '0', '0', '0', '0');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_ANALYSIS_BOX\"('11717', '11720', 'MD_DOL', '1=1', 'WEEK', '0', '0', '0', '0');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_VENDOR_ASSESSMENT\" ('11717', '11720', '1=1', '13', '1=1');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_VENDOR_ASSESSMENT\" ('11717', '11720', '1=1', '14', '1=1');",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_FETCH_CALENDAR_DATE\"(?);",
			  "CALL \"_SYS_BIC\".\"skp-bcbs-stage-hpc::SP_VENDOR_ASSESSMENT\" ('11717', '11720', '1=1', '17', '1=1');"};
	//Thread ID used for accessing array of queries as well
	int tid;
	static  int c_nextId = 0;
	public static void main(String[] argv) throws SQLException, InterruptedException 
	{

    	 Thread[] threadList = new Thread[NUM_OF_THREADS];
         for (int i = 0; i < NUM_OF_THREADS; i++)
         {
             threadList[i] = new test_tmp();
             //System.out.println("connected " + i);
             threadList[i].start();
             
         }
         for (int i = 0; i < NUM_OF_THREADS; i++)
         {
             threadList[i].join();
         }
      }
	  synchronized static int getNextId()
	  {
	      return c_nextId++;
	  }
	  public test_tmp()
	  {
	     super();
	     // Assign an Id to the thread
	     tid = getNextId();
	     
	  }
	  public void run()
	  {
		Connection conn = null;
		try {
			//JDBC connection to Hana Load balancer/server
			 conn = DriverManager.getConnection("jdbc:sap://hpclb.wal-mart.com:30415/","WMT_DBA_ADMIN","WMTeim04");
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	    ResultSet     rs   = null;
	    Statement  stmt = null;
	    try
	    {    
	    	try{
	        stmt = conn.createStatement ();
	    	}
	    	catch(Exception e){
	    		e.printStackTrace();
	    	}
	    	System.out.println(query[this.tid]);
	        rs = stmt.executeQuery (query[this.tid]);	          
	      // Loop through the results
	      while (rs.next())
	      {
	        //System.out.println("Thread " + tid);
	        yield();  // Yield To other threads
	      }
	          
	      // Close all the resources
	      rs.close();
	      rs = null;
	  
	      // Close the statement
	      stmt.close();
	      stmt = null;
	  
	      System.out.println("Thread " + tid +  " is finished. ");
	    }
	    catch (Exception e)
	    {
	      System.out.println("Thread " + tid + " got Exception: " + e);
	      e.printStackTrace();
	      return;
	    }
	    try {
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    conn=null;
	  }
}
