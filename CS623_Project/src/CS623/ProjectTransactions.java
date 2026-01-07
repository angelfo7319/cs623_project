package CS623;
import java.sql.*;
public class ProjectTransactions{
	
	private static final String URL =
			"jdbc:postgresql://localhost:5432/postgres";
	private static final String USER = "";
	private static final String PASS = "";
	
	//Method to check connection to PostgreSQL database
	public static void testConnection() {
	    try (Connection conn =
	         DriverManager.getConnection(URL, USER, PASS)) {
	        System.out.println("Connected to PostgreSQL!!!");
	    } catch (SQLException e) {
	        System.out.println("Connection failed X");
	        e.printStackTrace();
	    }
	}
	
	/**
	 * Transaction 1: Method to delete product p1 from Product and Stock
	 * Related rows in Stock table are deleted automatically
	 * because of ON DELETE CASCADE
	 */
	public static void transaction1() {
		Connection conn = null;
		Statement stmt = null;
		
		try {
			//Connection to PostgreSQL database
			conn = DriverManager.getConnection(URL,USER,PASS);
			//Atomicity
			conn.setAutoCommit(false);
			//Isolation
			conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			
			//SQL Statements
			//Create sql statement object. 
			stmt = conn.createStatement();
			//Transaction statement
			stmt.executeUpdate("DELETE FROM product WHERE prod_id = 'p1'");

			conn.commit();
			System.out.println("Transaction 1 committed");
			
			stmt.close();
			conn.close();
			
		}catch (SQLException e) {
			System.out.println("An exception was thrown");
			e.printStackTrace();

			try{
				//Atomicity
				conn.rollback();
				stmt.close();
				conn.close();
			}catch(SQLException ex) {
				ex.printStackTrace();
			}
		}
	}
	
	/**
	 * Transaction 2: Method to delete depot d1 from Depot and Stock
	 * Related rows in Stock table are deleted automatically
	 * because of ON DELETE CASCADE
	 */
	public static void transaction2() {
		Connection conn = null;
		Statement stmt = null;
		
		try {
			//Connection to PostgreSQL database
			conn = DriverManager.getConnection(URL,USER,PASS);
			//Atomicity
			conn.setAutoCommit(false);
			//Isolation
			conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			
			//SQL Statements
			//Create sql statement object. 
			stmt = conn.createStatement();
			//Transaction statement
			stmt.executeUpdate("DELETE FROM depot WHERE dep_id = 'd1'");

			conn.commit();
			System.out.println("Transaction 2 committed");
			stmt.close();
			conn.close();
			
		}catch (SQLException e) {
			System.out.println("An exception was thrown");
			e.printStackTrace();

			try{
				//Atomicity
				conn.rollback();
				stmt.close();
				conn.close();
			}catch(SQLException ex) {
				ex.printStackTrace();
			}
		}
	}
	
	/**
	 * Method to change name of product p1 to pp1 in Product and Stock
	 * Stock updates automatically via ON UPDATE CASCADE
	 */
	public static void transaction3() {
		Connection conn = null;
		Statement stmt = null;
		
		try {
			//Connection to PostgreSQL database
			conn = DriverManager.getConnection(URL,USER,PASS);
			//Atomicity
			conn.setAutoCommit(false);
			//Isolation
			conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			
			//SQL Statements
			//Create sql statement object. 
			stmt = conn.createStatement();
			//Transaction statement
			stmt.executeUpdate("UPDATE product SET prod_id = 'pp1' WHERE prod_id = 'p1'");

			conn.commit();
			System.out.println("Transaction 3 committed");
			stmt.close();
			conn.close();
			
		}catch (SQLException e) {
			System.out.println("An exception was thrown");
			e.printStackTrace();
			
			try{
				//Atomicity
				conn.rollback();
				stmt.close();
				conn.close();
			}catch(SQLException ex) {
				ex.printStackTrace();
			}
		}
	}
	
	/**
	 * Transaction 4: Method to change name of depot d1 to dd1 in Depot and Stock
	 * Related rows in Stock are updated automatically via ON UPDATE CASCASE
	 */
	public static void transaction4() {
		Connection conn = null;
		Statement stmt = null;
		
		try {
			//Connection to PostgreSQL database
			conn = DriverManager.getConnection(URL,USER,PASS);
			//Atomicity
			conn.setAutoCommit(false);
			//Isolation
			conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			
			//SQL Statements
			//Create sql statement object. 
			stmt = conn.createStatement();
			//Transaction statement
			stmt.executeUpdate("UPDATE depot SET dep_id = 'dd1' WHERE dep_id = 'd1'");


			conn.commit();
			System.out.println("Transaction 4 committed");
			stmt.close();
			conn.close();
			
		}catch (SQLException e) {
			System.out.println("An exception was thrown");
			e.printStackTrace();
			
			try{
				//Atomicity
				conn.rollback();
				stmt.close();
				conn.close();
			}catch(SQLException ex) {
				ex.printStackTrace();
			}
		}
	}
	
	/**
	 * Transaction 5: Method to:
	 * Add product(p100, cd, 5) in Product
	 * Add product(p100, d2, 50) in Stock
	 */
	public static void transaction5() {
		Connection conn = null;
		Statement stmt = null;
		
		try {
			//Connection to PostgreSQL database
			conn = DriverManager.getConnection(URL,USER,PASS);
			//Atomicity
			conn.setAutoCommit(false);
			//Isolation
			conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			
			//SQL Statements
			//Create sql statement object. 
			stmt = conn.createStatement();
			stmt.executeUpdate(
					"INSERT INTO product (prod_id, pname, price) VALUES ('p100','cd', 5)"
					);
			
			stmt.executeUpdate(
					"INSERT INTO stock (prod_id, dep_id, quantity) VALUES ('p100', 'd2', 50)"
					);

			conn.commit();
			System.out.println("Transaction 5 committed");
			stmt.close();
			conn.close();
			
		}catch (SQLException e) {
			System.out.println("An exception was thrown");
			e.printStackTrace();

			try{
				//Atomicity
				conn.rollback();
				stmt.close();
				conn.close();
			}catch(SQLException ex) {
				ex.printStackTrace();
			}
		}
	}
	
	/**
	 * Transaction 6: Method to:
	 * Add depot(d100, Chicago, 100) in Depot
	 * Add stock(p1, d100, 100) in Stock
	 */
	public static void transaction6() {
		Connection conn = null;
		Statement stmt = null;
		
		try {
			//Connection to PostgreSQL database
			conn = DriverManager.getConnection(URL,USER,PASS);
			//Atomicity
			conn.setAutoCommit(false);
			//Isolation
			conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			
			//SQL Statements
			//Create sql statement object. 
			stmt = conn.createStatement();
			//Returned query result
			stmt.executeUpdate(
					"INSERT INTO depot (dep_id, addr, volume) VALUES ('d100', 'Chicago', 100)"
					);
			
			stmt.executeUpdate(
					"INSERT INTO stock (prod_id, dep_id, quantity) VALUES ('p1', 'd100', 100)"
					);

			conn.commit();
			System.out.println("Transaction 6 committed");
			stmt.close();
			conn.close();
			
		}catch (SQLException e) {
			System.out.println("An exception was thrown");
			e.printStackTrace();
			
			try{
				//Atomicity
				conn.rollback();
				stmt.close();
				conn.close();
			}catch(SQLException ex) {
				ex.printStackTrace();
			}
		}
	}


}
