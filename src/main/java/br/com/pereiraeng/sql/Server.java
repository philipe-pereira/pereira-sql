package br.com.pereiraeng.sql;

/**
 * Tipos de conectores SQL
 * 
 * @author Philipe PEREIRA
 *
 */
public enum Server {
	MySQL("com.mysql.jdbc.Driver"), SQLserver("com.microsoft.sqlserver.jdbc.SQLServerDriver"),
	SASIOM("com.sas.rio.MVADriver"), SQLite("org.sqlite.JDBC"), Oracle("oracle.jdbc.OracleDriver"),
	UCanAccess("net.ucanaccess.jdbc.UcanaccessDriver"), SQLoffLine(null), PostgreSQL("org.postgresql.Driver");

	private final String driverName;

	private Server(String driverName) {
		this.driverName = driverName;
	}

	public String getDriverName() {
		return driverName;
	}
}
