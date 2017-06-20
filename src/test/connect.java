package test;
import java.sql.*;

public class connect extends Thread
{
	  private static int NUM_OF_THREADS = 29;
	  int m_myId;
	  static  int c_nextId = 1;
	  static  Connection conn = null;
	 //static  boolean  share_connection = false;
	public static void main(String[] argv) throws SQLException, InterruptedException 
	{
	      try 
	      {             
	    	  //connection = DriverManager.ConnectionSapDBFinalize("AKALADY");
	         conn = DriverManager.getConnection("jdbc:sap://hpclb.wal-mart.com:30415/","WMT_DBA_ADMIN","WMTeim04");  
	         
	      }catch (SQLException e) {
	    	  e.printStackTrace();
	         System.err.println("Connection Failed. User/Passwd Error?");
	         return;
	      }
	      if (conn != null) 
	      {
//	         try 
//	         {
//	            System.out.println("Connection to HANA successful!");
//	            Statement stmt = conn.createStatement();
//	            ResultSet resultSet = stmt.executeQuery("Select count(*) from"+" "+"DUMMY");
//	            resultSet.next();
//	            String hello = resultSet.getString(1);
//	            System.out.println(hello);
	        	 Thread[] threadList = new Thread[NUM_OF_THREADS];
	             for (int i = 0; i < NUM_OF_THREADS; i++)
	             {
	                 threadList[i] = new connect();
	                 threadList[i].start();
	             }
	             for (int i = 0; i < NUM_OF_THREADS; i++)
	             {
	                 threadList[i].join();
	             }

                 conn.close();
                 conn = null;
//          }
//	         catch (SQLException e) 
//	         {
//	          System.err.println("Query failed!");
//	          }
	       }
	 }
	  synchronized static int getNextId()
	  {
	      return c_nextId++;
	  }
	  public connect()
	  {
	     super();
	     // Assign an Id to the thread
	     m_myId = getNextId();
	  }
	  public void run()
	  {
	    ResultSet rs = null;
	    Statement stmt = null;
	    try
	    {    
	      // Get the connection

//	      if (share_connection)
	    
	        stmt = conn.createStatement (); // Create a Statement
//	      else
//	      {
//	        conn = DriverManager.getConnection("jdbc:oracle:oci8:@", 
//	                                           "scott","tiger");
//	        stmt = conn.createStatement (); // Create a Statement
//	      }

	      // Execute the Query
	      rs = stmt.executeQuery ("select * from DUMMY");
	          
	      // Loop through the results
	      while (rs.next())
	      {
	        System.out.println("Thread " + m_myId );
	        yield();  // Yield To other threads
	      }
	          
	      // Close all the resources
	      rs.close();
	      rs = null;
	  
	      // Close the statement
	      stmt.close();
	      stmt = null;
	  
	      // Close the local connection
//	      if ((!share_connection) && (conn != null))
//	      {
	         conn.close();
	         conn = null;
//	      }
	      System.out.println("Thread " + m_myId +  " is finished. ");
	    }
	    catch (Exception e)
	    {
	      System.out.println("Thread " + m_myId + " got Exception: " + e);
	      e.printStackTrace();
	      return;
	    }
	  }
}
