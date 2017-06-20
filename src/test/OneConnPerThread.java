package test;
/*
 * running Hana Queries in parallel
 * using java multithreding
 * Author: Vikash Singh
 */
import java.sql.*;
public class OneConnPerThread extends Thread
{
	//One thread per query
	  private static int NUM_OF_THREADS = 00;
	  //String array of queries
	  static String query[] = new String[] {};
	//Thread ID used for accessing array of queries as well
	int tid;
	static  int c_nextId = 0;
	public static void main(String[] argv) throws SQLException, InterruptedException 
	{

    	 Thread[] threadList = new Thread[NUM_OF_THREADS];
         for (int i = 0; i < NUM_OF_THREADS; i++)
         {
             threadList[i] = new OneConnPerThread();
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
	  public OneConnPerThread()
	  {
	     super();
	     // Assign an Id to the thread
	     tid = getNextId();
	     
	  }
	  public void run()
	  {
		Connection conn = null;
		try {
			//JDBC connection to Hana Load balancer/server xx = instance of Hana system yyy=system fqdn
			 conn = DriverManager.getConnection("jdbc:sap://yyy.com:3xx15/","User","Pass");
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
