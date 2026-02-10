package br.com.pereiraeng.sql;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DataTruncation;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.sqlite.jdbc4.JDBC4PreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.sas.rio.MVAPreparedStatement;

import br.com.pereiraeng.core.BinaryUtils;
import br.com.pereiraeng.core.StringUtils;
import br.com.pereiraeng.core.TimeUtils;
import br.com.pereiraeng.core.collections.ListUtils;
import br.com.pereiraeng.core.collections.MapUtils;
import br.com.pereiraeng.io.IOutils;
import net.ucanaccess.jdbc.UcanaccessPreparedStatement;
import oracle.jdbc.OraclePreparedStatement;

/**
 * Classe dos objetos que facilitam a manipulação de bases de dados SQL,
 * provendo contendo métodos e guardando a referência ao {@link #conn conector}
 * 
 * @author Philipe PEREIRA
 *
 */
public class SQLadapter {

	public static boolean debug = false;

	private static final String FORMAT_URL = "jdbc:%s:%s%s%s%s%s%s%s";

	public static final int NO_ACCESS = 0, ON_LINE = 1, OFF_LINE = 4;

	public static final int MAX_ROW_INSERT = 1000;

	private static String MYSQL_FOLDER = null;

	/**
	 * The constant in the Java programming language, sometimes referred to as a
	 * type code, that identifies the generic <strong>MySQL</strong> type
	 * <code>MEDIUMINT</code>.
	 */
	public static final int MEDIUMINT = 9;

	/**
	 * Padrão de escrita do fuso horário que o arquivo foi escrito
	 */
	protected static final Pattern TZ_PAT = Pattern.compile("[+-]\\d{1,2}:\\d{2}");

	public static final String LAST_UPDATE_TABLE = "updates";

	public static final String LAST_UPDATE_SYSTEM = "bd";

	public static final String LAST_UPDATE_DATE = "last";

	// --------- tipo e coordenadas da base de dados ---------

	// default(s)

	private List<SQLconfig> configs;

	private transient int c = 0;

	// current

	private transient Server type;

	protected transient String server, port, login, password;

	private transient String db;

	private transient Properties espConfig;

	// --------- objetos de manobra ---------

	// on-line

	/**
	 * Conector à base de dados SQL
	 */
	protected transient Connection conn;

	// off-line

	/**
	 * Arquivo no formato SQL contendo todas os dados da tabela
	 */
	private String offline;

	/**
	 * Objeto que faz a interface offline com a base de dados
	 */
	protected SQLoffline sqlOffline;

	// --------- compatibilização do conteúdo on-line com o off-line ---------

	/**
	 * Coordenadas da base de dados onde se encontra a informação de quando esta
	 * base de dados foi atualizada: tabela
	 * 
	 */
	private String lastUpdateTable;

	/**
	 * Coordenadas da base de dados onde se encontra a informação de quando esta
	 * base de dados foi atualizada: chave primária
	 */
	private String lastUpdateSystem;

	/**
	 * Coordenadas da base de dados onde se encontra a informação de quando esta
	 * base de dados foi atualizada, na forma: campo do tempo
	 */
	private String lastUpdateDate;

	/**
	 * <code>true</code> se a cada vez que este objeto for configurado efetua-se a
	 * atualização com relação ao arquivo off-line associado
	 */
	private boolean autoOffline;

	// --------- particionar o arquivo off-line ---------

	private String splitFolder;

	// --------- construtores ---------

	/**
	 * Construtor do objeto que faz a interface com uma dada base de dados SQL
	 * 
	 * @param sql objeto a compartilhar a conexão a base de dados
	 */
	protected SQLadapter(SQLadapter sql) {
		this.conn = sql.conn;
		this.server = sql.server;
		this.type = sql.type;
		this.db = sql.db;
	}

	/**
	 * Construtor do objeto que faz a interface com uma dada base de dados SQL
	 * 
	 * @param typeDefault     tipo de servidor
	 * @param serverDefault   endereço do servidor
	 * @param portDefault     porta
	 * @param loginDefault    usuário
	 * @param passwordDefault senha
	 * @param dbDefault       banco de dados
	 */
	public SQLadapter(Server typeDefault, String serverDefault, String portDefault, String loginDefault,
			String passwordDefault, String dbDefault) {
		addConfig(new SQLconfig(typeDefault, serverDefault, portDefault, loginDefault, passwordDefault, dbDefault));
	}

	/**
	 * Construtor do objeto que faz a interface com uma dada base de dados SQL
	 * 
	 * @param typeDefault     tipo de base de dados
	 * @param serverDefault   endereço do servidor
	 * @param portDefault     porta
	 * @param dbDefault       banco de dados
	 * @param loginDefault    usuário
	 * @param passwordDefault senha
	 * @param offline         arquivo .SQL com o qual será feita a conexão off-line
	 */
	public SQLadapter(Server typeDefault, String serverDefault, String portDefault, String dbDefault,
			String loginDefault, String passwordDefault, String offline) {
		this(typeDefault, serverDefault, portDefault, loginDefault, passwordDefault, dbDefault);
		addConfig(new SQLconfig(Server.SQLoffLine, offline, null, null, null, null));
	}

	/**
	 * Construtor do objeto que faz a interface com uma dada base de dados SQL
	 * 
	 * @param xmlfile caminho arquivo <code>xml</code> que contem as informações de
	 *                conexão à base de dados
	 */
	public SQLadapter(String xmlfile) {
		this(new File(xmlfile));
	}

	/**
	 * Construtor do objeto que faz a interface com uma dada base de dados SQL
	 * 
	 * @param xmlfile arquivo <code>xml</code> que contem as informações de conexão
	 *                à base de dados
	 */
	public SQLadapter(File xmlfile) {
		if (xmlfile.exists())
			addConfig(SQLconfig.loadConfig(xmlfile));
	}

	/**
	 * Construtor do objeto que faz a interface com uma dada base de dados SQL
	 * 
	 * @param config objeto com as configurações
	 */
	public SQLadapter(SQLconfig config) {
		addConfig(config);
	}

	/**
	 * Função que estabelece o nome da base de dados que será carregada. Se o driver
	 * permitir o uso da função {@link Connection#setCatalog(String)}, este será
	 * usado; do contrário, a conexão será desfeita e uma nova à base de dados
	 * designada será feita.
	 * 
	 * @param db sequência de caracteres que indica o nome da base de dados
	 */
	public void setDb(String db) {
		this.db = db;
		boolean changed = false;
		try {
			conn.setCatalog(this.db);
			changed = !this.db.equals(conn.getCatalog());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if (changed)
			this.reconnectDB();
		else if (debug)
			System.out.printf("Acessando à base de dados %s.\n", db);
	}

	public void checkDb(String db, InputStream is, File tempFile) {
		boolean dbExist = existDb(db);
		if (!dbExist) {
			createBD(conn, db, this.getType());
			setDb(db);
			try {
				FileOutputStream outStream = new FileOutputStream(tempFile);

				byte[] buffer = new byte[8 * 1024];
				int bytesRead;
				while ((bytesRead = is.read(buffer)) != -1)
					outStream.write(buffer, 0, bytesRead);
				outStream.close();

				importSQL(tempFile);

				Files.delete(tempFile.toPath());
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else
			setDb(db);
	}

	public Server getType() {
		return type;
	}

	/**
	 * Função que retorna o nome da base de dados que está carregada
	 * 
	 * @return sequência de caracteres que indica o nome da base de dados
	 */
	public String getDB() {
		return db;
	}

	public String getServer() {
		return server;
	}

	/**
	 * Função que estabelece o arquivo .SQL da conexão off-line
	 * 
	 * @param filename arquivo os registros no formato SQL
	 */
	public void setOffLine(String filename) {
		this.offline = filename;
	}

	public String getOffline() {
		return offline;
	}

	/**
	 * Função que estabelece as coordenadas da base de dados on-line onde serão
	 * guardadas a data e hora em que a base de dados foi atualizada.
	 * 
	 * @param lastUpdateTable
	 * @param lastUpdateSystem
	 * @param lastUpdateDate
	 */
	public void setDateLocation(String lastUpdateTable, String lastUpdateSystem, String lastUpdateDate) {
		this.setDateLocation(false, lastUpdateTable, lastUpdateSystem, lastUpdateDate);
	}

	/**
	 * Função que estabelece as coordenadas da base de dados on-line onde serão
	 * guardadas a data e hora em que a base de dados foi atualizada.
	 * 
	 * @param autoOffline      true se a cada vez que este objeto for configurado
	 *                         efetua-se a atualização com relação ao
	 *                         {@link SQLadapter#offline arquivo off-line associado}
	 * @param lastUpdateTable
	 * @param lastUpdateSystem
	 * @param lastUpdateDate
	 */
	public void setDateLocation(boolean autoOffline, String lastUpdateTable, String lastUpdateSystem,
			String lastUpdateDate) {
		this.autoOffline = autoOffline;
		this.lastUpdateTable = lastUpdateTable;
		this.lastUpdateSystem = lastUpdateSystem;
		this.lastUpdateDate = lastUpdateDate;
	}

	/**
	 * Função que retorna a data em que a base de dados foi carregada a partir do
	 * arquivo off-line
	 * 
	 * @return objeto que indica a data em que a base foi atualizada a partir do
	 *         {@link SQLadapter#offline arquivo off-line}
	 */
	public Calendar getTimeOnLine() {
		boolean ok = createLastUpdateTableIfNotExist();
		if (!ok)
			return null;
		Object o = search(this.lastUpdateDate, this.lastUpdateTable, this.lastUpdateSystem, "main");
		if (o != null)
			return getCalendar(o);
		else
			return null;
	}

	public Map<String, Calendar> getLastUpdates() {
		Map<String, Object> query = new HashMap<>();
		boolean ok = createLastUpdateTableIfNotExist();
		if (!ok)
			return null;
		table(query, this.lastUpdateTable, this.lastUpdateSystem, this.lastUpdateDate);
		Map<String, Calendar> out = new HashMap<>();
		for (Entry<String, Object> e : query.entrySet())
			out.put(e.getKey(), getCalendar(e.getValue()));
		return out;
	}

	private boolean createLastUpdateTableIfNotExist() {
		if (this.lastUpdateTable == null)
			this.lastUpdateTable = LAST_UPDATE_TABLE;
		if (this.lastUpdateSystem == null)
			this.lastUpdateSystem = LAST_UPDATE_SYSTEM;
		if (this.lastUpdateDate == null)
			this.lastUpdateDate = LAST_UPDATE_DATE;

		boolean exist = this.exist(lastUpdateTable);
		if (!exist) {
			exist = this.update(String.format(
					"CREATE TABLE IF NOT EXISTS `%s` (`%s` char(10) NOT NULL, `%s` char(20) NOT NULL, PRIMARY KEY (`%2$s`));",
					lastUpdateTable, lastUpdateSystem, lastUpdateDate));
			if (this.getType() == Server.SQLite)
				exist = this.exist(lastUpdateTable);
			if (exist)
				this.update(String.format("INSERT INTO `%s` (`%s`, `%s`) VALUES ('main', '%tF %<tT')", lastUpdateTable,
						lastUpdateSystem, lastUpdateDate, Calendar.getInstance()));
		}
		return exist;
	}

	public void refreshLastUpdate(Object timeObject) {
		this.refreshLastUpdate("main", timeObject);
	}

	public void refreshLastUpdate(String system, Object timeObject) {
		boolean ok = createLastUpdateTableIfNotExist();
		if (!ok)
			return;
		boolean update = this.exist(this.lastUpdateTable, this.lastUpdateSystem, system);
		if (update)
			this.update(String.format("UPDATE `%s` SET `%s`=%s WHERE `%s`='%s'", this.lastUpdateTable,
					this.lastUpdateDate, getStatement(getCalendar(timeObject)), this.lastUpdateSystem, system));
		else
			this.update(String.format("INSERT INTO `%s` (`%s`, `%s`) VALUES ('%s',%s)", this.lastUpdateTable,
					this.lastUpdateSystem, this.lastUpdateDate, system, getStatement(getCalendar(timeObject))));
	}

	private static Calendar getCalendar(Object o) {
		if (o instanceof Date)
			return TimeUtils.date2Calendar((Date) o);
		else if (o instanceof Integer)
			return TimeUtils.toCalendar((Integer) o);
		else // yyyy-MM-dd HH:mm:ss
			return TimeUtils.string2Calendar((String) o);
	}

	private String getStatement(Calendar c) {
		Object[] fields = getFieldData(this.lastUpdateTable, this.lastUpdateDate);
		int type = (int) fields[0];
		switch (type) {
		case Types.CHAR:
		case Types.NCHAR:
		case Types.VARCHAR:
		case Types.NVARCHAR:
		case Types.DATE:
		case Types.TIME:
		case Types.TIMESTAMP:
			return String.format("'%tF %<tT'", c);
		case Types.INTEGER:
			return String.valueOf(TimeUtils.toInt(c));
		default:
			return null;
		}
	}

	/**
	 * Função que retorna a data da geração do arquivo off-line
	 * 
	 * @return objeto que indica a data de geração do {@link SQLadapter#offline
	 *         arquivo off-line}
	 */
	public Calendar getTimeOffLine() {
		if (this.sqlOffline != null)
			return this.sqlOffline.getTime();
		else
			return null;
	}

	public void setSplitFolder(String splitFolder) {
		this.splitFolder = splitFolder;
	}

	// ============================ CONFIGURAÇÃO ============================

	/**
	 * Função que carrega as configurações padrão da base de dados a ser carregada
	 * (ao contrário da função {@link SQLconfig#loadConfig(String)}, que carrega as
	 * configurações personalizadas).
	 * 
	 * @return <code>true</code> se a configuração foi bem-sucedida
	 */
	public boolean config() {
		if (this.configs == null)
			return false;
		if (c >= this.configs.size())
			return false;
		SQLconfig sqlConfig = this.configs.get(c++);
		Server type = sqlConfig.getType();

		if (type == Server.SQLoffLine && this.offline == null) {
			this.type = type;
			this.offline = sqlConfig.getServer();
		} else if (this.conn == null)
			this.setConfig(type, sqlConfig.getServer(), sqlConfig.getPort(), sqlConfig.getLogin(),
					sqlConfig.getPassword(), sqlConfig.getDb());

		return true;
	}

	protected void setConfig(Server type, String server, String port, String login, String password, String db) {
		this.type = type;

		this.server = server;
		this.port = port;
		this.login = login;
		this.password = password;

		this.db = db;
	}

	public SQLconfig getConfig() {
		return new SQLconfig(this.type, this.server, this.port, this.db, this.login, this.password);
	}

	public void addConfig(SQLconfig sqLconfig) {
		if (configs == null)
			configs = new ArrayList<>();
		this.configs.add(sqLconfig);
	}

	protected void setEspConfig(Properties espConfig) {
		this.espConfig = espConfig;
	}

	public Connection getConn() {
		return conn;
	}

	protected void setConn(Connection conn) {
		this.conn = conn;
	}

	// ============================ CONECTIVIDADE ============================

	/**
	 * Função que estabelece a conexão com a base de dados
	 * 
	 * @return <code>true</code> se a conexão foi bem sucedida, <code>false</code>
	 *         se não
	 */
	public boolean connectDB() {
		boolean okOn = false, okOff = false;

		// primeira tentativa
		conn = conn(type, server, port, login, password, db, espConfig);
		while (true) {
			// além da primeira configuração, tenta-se novamente, mas com
			// configurações alternativas (se houver)
			boolean conf = config();
			if (conf) {
				// há outra configuração alternativa: tentar novamente
				if (this.type == Server.SQLoffLine) {
					if (this.sqlOffline == null) // se já conectou, pula
						this.sqlOffline = conn(this.offline);
				} else {
					if (this.conn == null) // se já conectou, pula
						this.conn = conn(type, server, port, login, password, db, espConfig);
				}
			} else
				break;
		}
		c = 0;
		okOn = conn != null;
		okOff = sqlOffline != null;

		// -------------------------------------------------------------------------------------

		// conexão off-line

		if (autoOffline && this.lastUpdateTable != null && okOn && okOff) {
			// se as duas conexões foram devidamente estabelecidas, vê se a
			// versão on-line é
			// a mais atualizada
			Calendar timeOff = this.sqlOffline.getTime();
			Calendar timeOn = this.getTimeOnLine();
			if (timeOn != null ? timeOn.before(timeOff) : true) {
				this.restoreOffline(true);
				this.refreshLastUpdate(timeOff);
			}
		}
		return okOn || okOff;
	}

	private static Connection conn(Server type, String server, String port, String login, String password, String db,
			Properties props) {
		if (type == null || server == null)
			return null;
		// _____ type // servidor porta database [?;] login password
		// jdbc: %s : %s____%s______%s_____%s_____%s____%s_____%s
		String url = null;
		if (type == Server.Oracle) {
			url = String.format(FORMAT_URL + "%s%s%s", type.name().toLowerCase(), "thin:", login, "/", password, "@",
					server, ":", port, ":", db);
		} else {
			if (type == Server.MySQL && "".equals(port)) {
				url = String.format(FORMAT_URL, type.name().toLowerCase(), "//", server, "", "/" + db, "", "", "");
				props = new Properties();
				props.setProperty("user", login);
				props.setProperty("password", password);
			} else {
				url = String.format(FORMAT_URL, type.name().toLowerCase(), type == Server.SQLite ? "" : "//", server,
						"".equals(port) ? "" : ":" + port,
						db == null || "".equals(db) ? (type == Server.PostgreSQL ? "/" : "")
								: ((type == Server.SQLite ? ("".equals(server) ? "" : File.separator)
										: (type == Server.MySQL ? "/"
												: (type == Server.UCanAccess ? ""
														: (type == Server.PostgreSQL ? "/" : ";databaseName="))))
										+ db),
						type == Server.UCanAccess ? ""
								: (type == Server.SQLite ? ".db" : (type == Server.SQLserver ? ";" : "?")),
						"".equals(login)
								? (type == Server.SQLite || type == Server.UCanAccess ? "" : "integratedSecurity=true")
								: "user=" + login,
						"".equals(password) ? ""
								: (type == Server.SASIOM || type == Server.MySQL || type == Server.PostgreSQL ? '&'
										: ';') + "password=" + password /* + (";useSSL=false") */);
			}
		}
		if (debug)
			System.out.println(url);
		try {
			Class.forName(type.getDriverName());
			DriverManager.setLoginTimeout(2);
			Connection conn = null;
			if (props == null)
				conn = DriverManager.getConnection(url);
			else
				conn = DriverManager.getConnection(url, props);
			if (debug)
				System.out.println("Conexão on-line estabelecida!");
			return conn;
		} catch (SQLException e1) {
			System.err.println("Não foi possível se conectar à base de dados: " + e1.getLocalizedMessage());
		} catch (ClassNotFoundException e2) {
			System.err.println("Os jars do conector " + e2.getMessage() + " não foram encontrados.");
		}
		return null;
	}

	private SQLoffline conn(String offline) {
		if (debug)
			System.out.println("Tentando conexão off-line...");

		SQLoffline sqlOffline = new SQLoffline(offline);

		if (sqlOffline.size() != 0) {
			sqlOffline.setSplit(this.splitFolder);
			if (debug)
				System.out.println("Conexão off-line estabelecida!");
		} else {
			sqlOffline = null;
			System.err.println(
					"Não foi possível se conectar à base de dados off-line. Dados não puderam ser carregados.");
		}
		return sqlOffline;
	}

	/**
	 * Função que fecha a conexão com a base de dados
	 */
	public void disconnectDB() {
		try {
			if (conn != null) {
				conn.close();
				conn = null;
			}
			if (sqlOffline != null) {
				sqlOffline.close();
				sqlOffline = null;
			}
			if (debug)
				System.out.println("Desconexão da base de dados efetuada.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Função que desconecta e reconecta a base de dados
	 * 
	 * @return <code>true</code> se a reconexão foi bem sucedida, <code>false</code>
	 *         se não
	 */
	public boolean reconnectDB() {
		if (conn != null || sqlOffline != null)
			disconnectDB();
		return connectDB();
	}

	/**
	 * Função que retorna o estado de conectividade do objeto que faz a interface
	 * com a base de dados. O valor da função é igual a soma:
	 * 
	 * <ul>
	 * <li>1 se estiver operando a interface on-line</i>
	 * <li>4 se estiver operando a interface off-line</i>
	 * </ul>
	 * 
	 * Se não houver nenhuma conexão, então a função retornará 0
	 * 
	 * @return inteiro que representa o estado das conexões
	 */
	public int getStatus() {
		int out = NO_ACCESS;
		if (conn != null)
			out += ON_LINE;
		if (sqlOffline != null)
			out += OFF_LINE;
		return out;
	}

	/**
	 * Função que retorna a conexão principal a ser utilizada preferencialmente
	 * 
	 * @return
	 *         <ul>
	 *         <li>1 para a conexão on-line</i>
	 *         <li>4 para a conexão off-line</i>
	 *         </ul>
	 */
	public int getMainConn() {
		int out = getStatus();
		if ((out & ON_LINE) > 0)
			return ON_LINE;
		if ((out & OFF_LINE) > 0)
			return OFF_LINE;
		return NO_ACCESS;
	}

	public File getDbFile() {
		if (type == Server.SQLite)
			return new File(String.format("%s%c%s.db", this.server, File.separatorChar, this.db));
		else
			return null;
	}

	/**
	 * Função que retorna os metadados da base de dados
	 * 
	 * @return
	 */
	public DatabaseMetaData getMetaData() {
		if (conn != null) {
			try {
				return conn.getMetaData();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	// ============================ META-FUNÇÕES ============================

	// tipos de dados

	public static String getTypeName(Server dest, int type, boolean pk, int size, int digits, boolean autoincrement) {
		return getTypeName(null, dest, type, pk, size, digits, autoincrement);
	}

	/**
	 * Função que retorna o nome tipo da variável de dados do SQL
	 * 
	 * @param srcType       tipo de servidor SQL fonte
	 * @param destType      tipo de servidor SQL destino
	 * @param type          inteiro que designa o tipo do dado (ver {@link Types})
	 * @param primaryKey    <code>true</code> para já indicar que esta variável será
	 *                      chave primária, adaptando assim o tipo de variável que
	 *                      ela será
	 * @param size          tamanho de bytes ou caracteres
	 * @param digits        número de dígitos decimais (para numerais de ponto
	 *                      flutuante)
	 * @param autoincrement <code>true</code> para auto incrementação,
	 *                      <code>false</code> se não
	 * @return nome da variável segundo o padrão do SQL
	 */
	public static String getTypeName(Server srcType, Server destType, int type, boolean primaryKey, int size,
			int digits, boolean autoincrement) {
		String str = null;
		switch (type) {
		// ----------- Integer -----------
		case Types.TINYINT:
			if (destType == Server.SQLite)
				str = "INTEGER(1)";
			else if (destType == Server.PostgreSQL)
				str = "int2";
			else
				str = "tinyint";
			break;
		case Types.SMALLINT:
			if (destType == Server.SQLite)
				str = "INTEGER(2)";
			else
				str = "smallint";
			break;
		case MEDIUMINT:
			if (destType == Server.SQLite)
				str = "INTEGER(3)";
			else if (destType == Server.MySQL)
				str = "mediumint";
			else
				str = "int";
			break;
		case Types.INTEGER:
			if (destType == Server.SQLite) {
				if (autoincrement)
					str = "INTEGER";
				else
					str = "INTEGER(4)";
			} else {
				if (srcType == Server.SQLite)
					str = "bigint";
				else
					str = "int";
			}
			break;
		case Types.BIGINT:
			if (destType == Server.SQLite)
				str = "INTEGER(8)";
			else
				str = "bigint";
			break;
		// ----------- Decimal -----------
		case Types.NUMERIC:
			if (autoincrement) {
				type = Types.INTEGER;
				if (destType == Server.SQLite)
					str = "INTEGER";
				else
					str = "int";
			} else
				str = "numeric";
			break;
		case Types.DECIMAL:
			str = "decimal";
			break;
		case Types.FLOAT:
			if (destType == Server.SQLite)
				str = "REAL";
			else
				str = "float";
			break;
		case Types.REAL:
			str = "real";
			break;
		case Types.DOUBLE:
			switch (destType) {
			case PostgreSQL:
				str = "real";
				break;
			case SQLserver:
				str = "float(53)";
				break;
			case MySQL:
			case UCanAccess:
			case Oracle:
				str = "double";
				break;
			case SASIOM:
			case SQLoffLine:
				return null;
			case SQLite:
				return "REAL";
			}
			break;
		// ----------- Date and Time -----------
		case Types.TIMESTAMP:
			str = "datetime";
			break;
		case Types.DATE:
			if (destType == Server.SQLite)
				str = "TEXT";
			else
				str = "date";
			break;
		case Types.TIME:
			str = "time";
			break;
		// ----------- String -----------
		case Types.CHAR:
			if (destType == Server.SQLite)
				str = "TEXT";
			else
				str = "char";
			break;
		case Types.NCHAR:
			switch (destType) {
			case SQLserver:
				str = "nchar";
				break;
			case MySQL:
			case UCanAccess:
			case Oracle:
			case PostgreSQL:
				str = "char";
				break;
			case SASIOM:
			case SQLoffLine:
				return null;
			case SQLite:
				return "TEXT";
			}
			break;
		case Types.VARCHAR: // TODO esse é o melhor até agora para o caso pk
			switch (destType) {
			case SQLserver:
				str = "varchar";
				break;
			case PostgreSQL:
				str = "text";
				break;
			case MySQL:
			case UCanAccess:
			case Oracle:
				if (primaryKey) {
					if (size < 256)
						str = "binary(255)";
					else
						str = "varbinary(3000)";
				} else {
					// if (src == Server.SQLite) {
					// if (size < 256)
					// s = "tinyblob";
					// else if (size < 65536)
					// s = "blob";
					// else if (size < 16777216)
					// s = "mediumblob";
					// else if (size <= Integer.MAX_VALUE)
					// s = "longblob";
					// } else {
					if (size < 256)
						str = "tinytext";
					else if (size < 65536)
						str = "text";
					else if (size < 16777216)
						str = "mediumtext";
					else if (size <= Integer.MAX_VALUE)
						str = "longtext";
					// }
				}
				return str;
			case SQLite:
				return "TEXT";
			case SASIOM:
			case SQLoffLine:
				return null;
			}
			break;
		case Types.NVARCHAR:
			switch (destType) {
			case SQLserver:
				str = "nvarchar";
				break;
			case MySQL:
			case UCanAccess:
			case Oracle:
			case PostgreSQL:
				if (size < 256)
					if (primaryKey)
						str = "varchar";
					else
						return "tinytext";
				else if (size < 65536)
					return "text";
				else if (size < 16777216)
					return "mediumtext";
				else if (size <= Integer.MAX_VALUE)
					return "longtext";
				break;
			case SASIOM:
			case SQLoffLine:
				return null;
			case SQLite:
				return "TEXT";
			}
			break;
		case Types.LONGVARCHAR:
			switch (destType) {
			case SQLserver:
			case PostgreSQL:
				str = "text";
				break;
			case MySQL:
			case UCanAccess:
			case Oracle:
				if (size < 256)
					str = "tinytext";
				else if (size < 65536)
					str = "text";
				else if (size < 16777216)
					str = "mediumtext";
				else if (size <= Integer.MAX_VALUE)
					str = "longtext";
				break;
			case SASIOM:
			case SQLoffLine:
				return null;
			case SQLite:
				return "TEXT";
			}
			break;
		case Types.LONGNVARCHAR:
			switch (destType) {
			case SQLserver:
				str = "ntext";
				break;
			case MySQL:
			case UCanAccess:
			case Oracle:
			case PostgreSQL:
				if (size < 256)
					str = "tinytext";
				else if (size < 65536)
					str = "text";
				else if (size < 16777216)
					str = "mediumtext";
				else if (size <= Integer.MAX_VALUE)
					str = "longtext";
				break;
			case SASIOM:
			case SQLoffLine:
				return null;
			case SQLite:
				return "TEXT";
			}
			break;
		// ----------- Binary -----------
		case Types.BIT:
			switch (destType) {
			case SQLserver:
				str = "bit";
				break;
			case MySQL:
			case UCanAccess:
			case Oracle:
				return "tinyint(1)";
			case SASIOM:
			case SQLoffLine:
				return null;
			case SQLite:
				return "INTEGER(1)";
			case PostgreSQL:
				return "boolean";
			}
			break;
		case Types.BINARY:
			if (destType == Server.SQLite)
				str = "BLOB";
			else if (destType == Server.PostgreSQL)
				return "bytea";
			else
				str = "binary";
			break;
		case Types.BLOB:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			switch (destType) {
			case SQLserver:
				str = "varbinary";
				break;
			case MySQL:
			case UCanAccess:
			case Oracle:
			case PostgreSQL:
				if (size < 256)
					return "tinyblob";
				else if (size < 65536)
					return "blob";
				else if (size < 16777216)
					return "mediumblob";
				else if (size <= Integer.MAX_VALUE)
					return "longblob";
				break;
			case SASIOM:
			case SQLoffLine:
				return null;
			case SQLite:
				return "BLOB";
			}
			break;
		// ----------- MISC -----------
		case Types.OTHER:
			str = "blob";
			break;
		case Types.STRUCT:
			if (destType == Server.Oracle)
				str = "GEOMETRY";
			else
				str = "text";
			break;
		}

		// tamanho e casas decimais
		if (needsSizeSpecification(type)) {
			if (size < 1)
				size = 10;
			if (isDecimal(type)) {
				if (digits < 0)
					digits = 0;
				str += String.format("(%d,%d)", size, digits);
			} else {
				if (size == Integer.MAX_VALUE)
					str += "(max)";
				else {
					if (destType == Server.PostgreSQL || destType == Server.MySQL) {
						str += String.format("(%d)", size);
					} else {
						long sizeL = size;
						if (sizeL < 256)
							sizeL = 255;
						else if (size < 65536)
							sizeL = 65535;
						else if (size < 16777216)
							sizeL = 16777215;
						else if (size < Integer.MAX_VALUE)
							sizeL = 2147483647;
						else
							sizeL = Long.MAX_VALUE;
						str += String.format("(%d)", sizeL);
					}
				}
			}
		}
		return str;
	}

	private static boolean isDecimal(int type) {
		switch (type) {
		// Decimal
		case Types.NUMERIC:
		case Types.FLOAT:
		case Types.DOUBLE:
		case Types.REAL:
			return true;
		}
		return false;
	}

	private static boolean needsSizeSpecification(int type) {
		switch (type) {
		// Integer
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
		case SQLadapter.MEDIUMINT:
		case Types.BIGINT:
			// Decimal
		case Types.FLOAT:
		case Types.DOUBLE:
		case Types.REAL:
			// Date and Time
		case Types.TIMESTAMP:
			// String
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
		case Types.LONGNVARCHAR:
			// Binary
		case Types.BIT:
			return false;
		// Data
		case Types.DATE:
		case Types.TIME:
			return false;
		}
		return true;
	}

	// --------------------- Criar base de dados ---------------------

	public static final String COLLATION_MYSQL = "utf8_general_ci", COLLATION_SQL_SERVER = "Latin1_General_CI_AS";

	/**
	 * Função que cria uma base de dados no servidor SQL
	 * 
	 * @return <code>true</code> se a base de dados foi criada, <code>false</code>
	 *         se não
	 */
	public boolean createBD() {
		return createBD(this.getType(), this.getServer(), this.port, this.login, this.password, this.getDB());
	}

	/**
	 * Função que cria uma base de dados no servidor SQL
	 * 
	 * @param type     tipo de base de dados (MySQL, SQLserver ou SAS. Não se faz
	 *                 nada se o argumento for {@link Server#SQLite}, pois para ele
	 *                 o arquivo já é criado simplesmente ao se conectar à base de
	 *                 dados)
	 * @param server   servidor
	 * @param port     porta
	 * @param login    usuário
	 * @param password senha
	 * @param db       nome da base de dados a ser criada
	 * @return <code>true</code> se a base de dados foi criada, <code>false</code>
	 *         se não
	 */
	public static boolean createBD(Server type, String server, String port, String login, String password, String db) {
		if (type == Server.SQLite)
			return true;
		String url = String.format(FORMAT_URL, type.name().toLowerCase(), "//", server,
				"".equals(port) ? "" : ":" + port, "", type == Server.SQLserver ? ";" : "?",
				"".equals(login) ? "integratedSecurity=true" : "user=" + login, "".equals(password) ? ""
						: (type == Server.SASIOM || type == Server.MySQL ? '&' : ';') + "password=" + password);
		if (debug)
			System.out.println(url);
		try {
			Connection conn = DriverManager.getConnection(url);
			boolean out = createBD(conn, db, type);
			conn.close();
			return out;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * Função que cria uma base de dados no servidor SQL
	 * 
	 * @param conn conector do driver
	 * @param db   nome da base de dados a ser criada
	 * @param type tipo de base de dados (MySQL, SQLserver ou SAS. Não se faz nada
	 *             se o argumento for {@link Server#SQLite}, pois para ele o arquivo
	 *             já é criado simplesmente ao se conectar à base de dados)
	 * @return <code>true</code> se a base de dados foi criada, <code>false</code>
	 *         se não
	 */
	protected static boolean createBD(Connection conn, String db, Server type) {
		try {
			Statement statement = conn.createStatement();

			String ifNotExist = " IF NOT EXISTS", collate = "";
			switch (type) {
			case MySQL:
				collate = " COLLATE " + COLLATION_MYSQL;
				break;
			case SQLserver:
				collate = " COLLATE " + COLLATION_SQL_SERVER;
				break;
			case PostgreSQL:
				ifNotExist = "";

				Statement st = conn.createStatement();
				ResultSet rs = st.executeQuery("SELECT FROM pg_database WHERE datname='" + db + "'");

				if (rs.next()) {
					rs.close();
					return true;
				}
				rs.close();
			default:
				ifNotExist = "";
				break;
			}
			String update = String.format("CREATE DATABASE%s %s%s", ifNotExist, db, collate);
			if (debug)
				System.out.println(update);
			statement.executeUpdate(update);
			statement.close();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	// ----------------- transferir dados entre base de dados -----------------

	/**
	 * <ol start="-2">
	 * <li>muito lento: os dados já estão em um arquivo .sql, sendo lido e o
	 * conteúdo repassado para a base de dados de destino. <strong>No final, o
	 * arquivo temporário não é apagado</strong>;</i>
	 * <li>muito lento: os dados são transferidos primeiramente para um arquivo .sql
	 * e em seguida este arquivo é lido, repassando para a base de dados de destino.
	 * <strong>No final, o arquivo temporário não é apagado</strong>;</i>
	 * <li>muito lento: os dados são transferidos primeiramente para um arquivo .sql
	 * e em seguida este arquivo é lido, repassando para a base de dados de destino.
	 * No final, o arquivo temporário é apagado;</i>
	 * <li>rápido: os dados são transferidos em blocos para a base de dados de
	 * destino;</i>
	 * <li>lento: os dados são transferidos para a base de dados de destino
	 * unitariamente, entrada por entrada.</i>
	 * </ol>
	 */
	public static int TRANSF_OPT_DEFAULT = 1;

	/**
	 * Função que faz a transferência de todas as tabelas de uma base de dados para
	 * outra. O método de transferência utilizado é definido pela variável ajustavel
	 * {@link #TRANSF_OPT_DEFAULT}.
	 * 
	 * @param type     {@link Server tipo} de base de dados
	 * @param server   endereço do servidor
	 * @param port     porta
	 * @param db       nome da base de dados
	 * @param login    usuário
	 * @param password senha
	 * @param tables   vetor de tabelas da base de dados da fonte que serão copiadas
	 *                 (se for vazio, todas as tabelas serão copiadas)
	 */
	public void transfer(Server type, String server, String port, String login, String password, String db,
			String... tables) {
		SQLadapter dest = new SQLadapter(type, server, port, login, password, db);
		dest.config();
		dest.connectDB();

		this.transfer(dest, tables);

		dest.disconnectDB();
	}

	/**
	 * Função que faz a transferência de algumas tabelas de um dado esquema de uma
	 * base de dados para outra. O método de transferência utilizado é definido pela
	 * variável ajustavel {@link #TRANSF_OPT_DEFAULT}.
	 * 
	 * @param library schema (ou library - SAS)
	 * @param dest    objeto de conexão à base de dados de destino dos dados
	 * @param tables  tabelas (se este vetor for vazio,
	 *                {@link #transfer(String, int, SQLadapter) transfere-se todas
	 *                as tabelas do esquema})
	 */
	public void transfer(String library, SQLadapter dest, String... tables) {
		if (tables.length == 0) {
			this.transfer(library, TRANSF_OPT_DEFAULT, dest);
		} else {
			String[] temp = new String[tables.length];
			for (int i = 0; i < tables.length; i++)
				temp[i] = library + "." + tables[i];
			this.transfer(dest, temp);
		}
	}

	/**
	 * Função que faz a transferência de todas as tabelas de um dado esquema de uma
	 * base de dados para outra
	 * 
	 * @param library  schema (ou library - SAS)
	 * @param transfer
	 *                 <ol start="-2">
	 *                 <li>muito lento: os dados já estão em um arquivo .sql, sendo
	 *                 lido e o conteúdo repassado para a base de dados de destino.
	 *                 <strong>No final, o arquivo temporário não é
	 *                 apagado</strong>;</i>
	 *                 <li>muito lento: os dados são transferidos primeiramente para
	 *                 um arquivo .sql e em seguida este arquivo é lido, repassando
	 *                 para a base de dados de destino. <strong>No final, o arquivo
	 *                 temporário não é apagado</strong>;</i>
	 *                 <li>muito lento: os dados são transferidos primeiramente para
	 *                 um arquivo .sql e em seguida este arquivo é lido, repassando
	 *                 para a base de dados de destino. No final, o arquivo
	 *                 temporário é apagado;</i>
	 *                 <li>rápido: os dados são transferidos em blocos para a base
	 *                 de dados de destino;</i>
	 *                 <li>lento: os dados são transferidos para a base de dados de
	 *                 destino unitariamente, entrada por entrada.</i>
	 *                 </ol>
	 * @param dest     objeto de conexão à base de dados de destino dos dados
	 */
	public void transfer(String library, final int transfer, SQLadapter dest) {
		LinkedList<String> tables = new LinkedList<>();
		try {
			DatabaseMetaData dbmd = conn.getMetaData();

			ResultSet rs = dbmd.getTables(null, library, "%", null);

			while (rs.next())
				tables.add(library + "." + rs.getString(3).trim());
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		this.transfer(dest, transfer, tables.toArray(new String[tables.size()]));
	}

	/**
	 * Função que faz a transferência das tabelas de uma base de dados para outra. O
	 * método de transferência utilizado é definido pela variável ajustavel
	 * {@link #TRANSF_OPT_DEFAULT}.
	 * 
	 * @param dest   objeto de conexão à base de dados de destino dos dados
	 * @param tables vetor de tabelas da base de dados da fonte que serão copiadas
	 *               (se for vazio, todas as tabelas serão copiadas)
	 */
	public void transfer(SQLadapter dest, String... tables) {
		this.transfer(dest, TRANSF_OPT_DEFAULT, tables);
	}

	public void transferSQLite(String tempFolder, String finalDestination)
			throws IllegalArgumentException, IOException {
		if (tempFolder == null)
			throw new IllegalArgumentException("Caminho nulo até o diretório temporário");

		File tempFolderFile = new File(tempFolder);
		if (!Files.isWritable(tempFolderFile.toPath()))
			throw new IllegalArgumentException(
					"O programa não foi capaz de escrever no disco. Não é possível baixar a base de dados.");

		String dbName = this.getDB();
		SQLadapter tempSQLite = new SQLadapter(new SQLconfig(tempFolderFile, dbName));
		tempSQLite.connectDB();
		this.transfer(tempSQLite);
		tempSQLite.disconnectDB();

		if (!tempFolder.endsWith(File.separator))
			tempFolder += File.separator;
		Path tempPath = new File(tempFolder + dbName + ".db").toPath();

		if (!finalDestination.endsWith(File.separator))
			finalDestination += File.separator;
		Path destPath = new File(finalDestination + dbName + ".db").toPath();

		Files.move(tempPath, destPath, StandardCopyOption.REPLACE_EXISTING);
	}

	public static final String INSERT = "INSERT INTO";

	/**
	 * This is non-standard MySQL syntax. After the brackets that enclose the list
	 * of columns and indexes, MySQL allows table options.
	 */
	private static final String TABLE_OPTIONS = " ENGINE=InnoDB DEFAULT CHARSET=utf8";

	/**
	 * Função que faz a transferência das tabelas de uma base de dados para outra
	 * 
	 * @param dest     objeto de conexão à base de dados de destino dos dados
	 * @param transfer
	 *                 <ol start="-2">
	 *                 <li>muito lento: os dados já estão em um arquivo .sql, sendo
	 *                 lido e o conteúdo repassado para a base de dados de destino.
	 *                 <strong>No final, o arquivo temporário não é
	 *                 apagado</strong>;</i>
	 *                 <li>muito lento: os dados são transferidos primeiramente para
	 *                 um arquivo .sql e em seguida este arquivo é lido, repassando
	 *                 para a base de dados de destino. <strong>No final, o arquivo
	 *                 temporário não é apagado</strong>;</i>
	 *                 <li>muito lento: os dados são transferidos primeiramente para
	 *                 um arquivo .sql e em seguida este arquivo é lido, repassando
	 *                 para a base de dados de destino. No final, o arquivo
	 *                 temporário é apagado;</i>
	 *                 <li>rápido: os dados são transferidos em blocos para a base
	 *                 de dados de destino;</i>
	 *                 <li>lento: os dados são transferidos para a base de dados de
	 *                 destino unitariamente, entrada por entrada.</i>
	 *                 </ol>
	 * @param tables   vetor de tabelas da base de dados da fonte que serão copiadas
	 *                 (se for vazio, todas as tabelas serão copiadas)
	 */
	public void transfer(SQLadapter dest, final int transfer, String... tables) {
		if (tables.length == 0 && type == Server.SASIOM) {
			System.err.println(
					"Para transferências do SAS, indicar a library a partir da função #transfer(String,SQLadapter)");
			return;
		}

		if (tables.length != 1) {
			List<String> ts = null;

			if (tables.length == 0)
				ts = this.getTables(true);
			else
				ts = Arrays.asList(tables);

			for (String table : ts)
				this.transfer(dest, transfer, table);

			if (transfer <= 0) {
				// depois de criadas as tabelas, gera-se um arquivo .sql e transfere somente a
				// parte relativa ao conteúdo (INSERT INTO). As partes relativas às estruturas
				// (CREATE TABLE) podem não coincidir entre os diferentes tipos de SQL
				File file = new File(this.db + ".sql");

				if (transfer > -2) { // o arquivo não foi exportado previamente
					if (file.exists())
						file.delete();
					this.exportSQL(file);
				}

				try {
					RandomAccessFile raf = new RandomAccessFile(file, "r");
					String query;
					while ((query = raf.readLine()) != null)
						if (query.startsWith(INSERT))
							dest.update(query.replaceAll("\\\\'", "''"));
					raf.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

				if (transfer == 0)
					file.delete();
			}
		} else { // ------------- estrutura da tabela -------------

			int p = tables[0].indexOf('.');

			StringBuilder query = new StringBuilder(
					String.format("CREATE TABLE IF NOT EXISTS `%s` (", p < 0 ? tables[0] : tables[0].substring(p + 1)));

			Map<String, Object[]> fieldsData = this.getFieldsNData(tables[0]);

			if (fieldsData == null)
				return;

			String[] fields = new String[fieldsData.size()];
			int[] types = new int[fieldsData.size()];

			List<String> primary = new LinkedList<>(), index = new LinkedList<>();
			int i = 0;
			for (Entry<String, Object[]> e : fieldsData.entrySet()) {
				Object[] os = e.getValue();
				int type = (int) os[0];
				int size = (int) os[1];
				boolean pk = (boolean) os[4];
				boolean ind = (boolean) os[6];
				if (pk || ind) {
					if (dest.type == Server.MySQL && type == Types.VARCHAR && size > 65535)
						size = 255; // TODO que coisa feia... estuda essa porra direito
				}
				boolean ai = (boolean) os[7];

				query.append(String.format("`%s` %s", e.getKey(),
						getTypeName(this.type, dest.type, type, pk || ind, size, (int) os[2], ai)));

				if ((type == Types.TINYINT || type == Types.SMALLINT || type == Types.INTEGER || type == Types.BIGINT)
						&& !(boolean) os[5] && (dest.type != Server.SQLite && dest.type != Server.PostgreSQL)) // unsigned
					query.append(" UNSIGNED");

				if (!(boolean) os[3] && !ai) // not null
					query.append(" NOT NULL");

				if (pk) { // é PK?
					if (ai) {// auto incremental
						if (dest.type == Server.MySQL) {
							query.append(" NOT NULL AUTO_INCREMENT");
							primary.add(e.getKey()); // MySQL:NN AI[...] primary
						} else // sqlite eu sei que é assim...
							query.append(" PRIMARY KEY AUTOINCREMENT");
					} else
						primary.add(e.getKey());
				} else if (ind) // índice?
					index.add(e.getKey());

				query.append(", ");

				fields[i] = e.getKey();
				types[i] = type;
				i++;
			}

			// remove a última vírgula
			query.delete(query.length() - 2, query.length());

			if (primary.size() > 0) // chave(s) primárias
				query.append(", PRIMARY KEY (`" + StringUtils.addSeparator(primary, "`, `") + "`)");

			if (dest.type != Server.SQLite && dest.type != Server.PostgreSQL)
				for (String k : index) // índice
					query.append(String.format(", KEY `%1$s` (`%1$s`)", k));

			query.append(")");

			if (dest.type != Server.SQLite && dest.type != Server.PostgreSQL)
				query.append(TABLE_OPTIONS);
			query.append(";");
			if (debug)
				System.out.println(query);
			boolean ct = dest.update(query.toString());

			if ((dest.type == Server.SQLite || dest.type == Server.PostgreSQL) && index.size() > 0/* && ct */) { // índice
				// TODO sqlite retorna false quando se cria tabelas
				for (String k : index) {
					String q = String.format("CREATE INDEX idx_%1$s_%2$s ON %1$s (%2$s);",
							p < 0 ? tables[0] : tables[0].substring(p + 1), k);
					if (debug)
						System.out.println(q);
					dest.update(q);
				}
			}

			// ------------- conteúdo da tabela -------------

			if (!ct) // se a tabela não foi criada (já existia), limpá-la
				dest.update(String.format("TRUNCATE `%s`", p < 0 ? tables[0] : tables[0].substring(p + 1)));

			if (transfer > 0) {
				// o conteúdo da tabela só é transferido aqui para os casos rápido (1) e lento
				// (2): no caso muito lento (-1 ou 0), os dados são transferidos depois que
				// todas as tabelas forem criadas, de modo que o arquivo .sql a ser criado
				// temporariamente seja percorrido somente uma vez
				try {
					Object[] newValues = new Object[fieldsData.size()];
					ResultSet rs = this.query("SELECT * FROM " + tables[0]);
					if (transfer == 1) {
						// rápido, mas pode ter problemas em bloco
						final String insert = String.format("%s `%s`(`%s`) VALUES ", INSERT,
								p < 0 ? tables[0] : tables[0].substring(p + 1),
								StringUtils.addSeparator(fields, "`, `"));
						query = new StringBuilder(insert);
						String values = "(" + StringUtils.addSeparator("%s", ",", newValues.length) + "),";
						int[] ts = new int[newValues.length];
						int c = 0;
						for (Object[] os : fieldsData.values())
							ts[c++] = (int) os[0];

						c = 0;
						while (rs.next()) {
							for (int j = 0; j < newValues.length; j++)
								newValues[j] = rs.getObject(j + 1);
							query.append(String.format(Locale.US, values, prepareObjects(newValues, dest.type, ts)));
							c++;
							if (c == SQLadapter.MAX_ROW_INSERT) {
								dest.update(query.substring(0, query.length() - 1));
								query = new StringBuilder(insert);
								c = 0;
							}
						}
						rs.close();
						if (c > 0)
							dest.update(query.substring(0, query.length() - 1));
					} else if (transfer == 2) {
						// lento, mas os problemas são tratados individualmente
						String q = String.format("%s `%s`(`%s`) VALUES (%s)", INSERT,
								p < 0 ? tables[0] : tables[0].substring(p + 1),
								StringUtils.addSeparator(fields, "`, `"),
								StringUtils.addSeparator("?", ",", newValues.length));
						while (rs.next()) {
							for (int j = 0; j < newValues.length; j++)
								newValues[j] = rs.getObject(j + 1);
							dest.update(q, types, newValues);
						}
						rs.close();
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}

				// tentar limpar a memória...
				System.gc();

				Runtime runtime = Runtime.getRuntime();
				SQLmemoryManager sqlmm = new SQLmemoryManager();

				if (sqlmm.isMoreMemoryNeeded(runtime)) {
					if (debug)
						System.out.println("Evitando excesso de memória. Tentando recomeçar...");
					disconnectDB();
					System.gc();
					// restart...
					connectDB();
				}
			}
		}
	}

	// --------------------- Tabelas e campos ---------------------

	/**
	 * Função que retorna uma lista com o nome de todos as tabelas e view's da base
	 * de dados
	 * 
	 * @return lista com o nome das tabelas e view's
	 */
	public List<String> getTables() {
		return getTables(false);
	}

	/**
	 * Função que retorna uma lista com o nome de todos as tabelas da base de dados
	 * 
	 * @param tablesOnly <code>true</code> para que a lista contenha somente
	 *                   tabelas, <code>false</code> para tabelas e view's
	 * @return lista com o nome das tabelas (e das view's, caso indicado)
	 */
	public List<String> getTables(boolean tablesOnly) {
		return getTables(tablesOnly, null);
	}

	/**
	 * Função que retorna uma lista com o nome de todos as tabelas e view's da base
	 * de dados
	 * 
	 * @param library a schema name; must match the schema name as it is stored in
	 *                the database; "" retrieves those without a schema; null means
	 *                that the schema name should not be used to narrow the search
	 * @return lista com o nome das tabelas e das view's
	 */
	public List<String> getTables(String library) {
		return getTables(false, library);
	}

	/**
	 * Função que retorna uma lista com o nome de todos as tabelas da base de dados
	 * 
	 * @param tablesOnly <code>true</code> para que a lista contenha somente
	 *                   tabelas, <code>false</code> para tabelas e view's
	 * @param library    a schema name; must match the schema name as it is stored
	 *                   in the database; "" retrieves those without a schema; null
	 *                   means that the schema name should not be used to narrow the
	 *                   search
	 * @return lista com o nome das tabelas (e das view's, caso indicado)
	 */
	public List<String> getTables(boolean tablesOnly, String library) {
		List<String> tables = new LinkedList<>();

		if (conn != null) {
			try {
				DatabaseMetaData dbmd = conn.getMetaData();
				ResultSet rs = dbmd.getTables(null,
						library == null ? (this.getType() == Server.Oracle || this.getType() == Server.PostgreSQL ? null
								: "dbo") : library,
						"%", tablesOnly ? new String[] { "TABLE" } : new String[] { "TABLE", "VIEW" });

				while (rs.next())
					tables.add((this.getType() == Server.Oracle ? (rs.getString(2) + ".") : "") + rs.getString(3));

				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		return tables;
	}

	/**
	 * Função que lista o nome de todos os campo de uma das tabelas da base de dados
	 * 
	 * @param table nome da tabela
	 * @return lista com o nome dos campos
	 */
	public List<String> getFields(String table) {
		List<String> out = new LinkedList<>();

		if (conn != null) {
			try {
				DatabaseMetaData dbmd = conn.getMetaData();
				ResultSet rs = dbmd.getColumns(null, null, table, "%");

				while (rs.next())
					out.add(rs.getString("COLUMN_NAME"));

				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return out;
	}

	/**
	 * Função que retorna os nomes dos campos que são chave primária de uma dada
	 * tabela
	 * 
	 * @param table nome da tabela
	 * @return nomes dos campos que são chaves primárias
	 */
	public List<String> getPrimaryKeys(String table) {
		List<String> out = new LinkedList<>();

		if (conn != null) {
			try {
				DatabaseMetaData dbmd = conn.getMetaData();
				ResultSet rs = dbmd.getPrimaryKeys(null, null, table);

				while (rs.next())
					out.add(rs.getString("COLUMN_NAME"));

				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		return out;
	}

	public Map<String, Integer> getFieldsNTypes(String table) {
		Map<String, Integer> out = new LinkedHashMap<>();

		if (conn != null) {
			try {
				DatabaseMetaData dbmd = conn.getMetaData();
				ResultSet rs = dbmd.getColumns(null, null, table, "%");

				while (rs.next())
					out.put(rs.getString("COLUMN_NAME"), rs.getInt("DATA_TYPE"));

				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		return out;
	}

	/**
	 * 
	 * @param table nome da tabela
	 * @return tabela de dispersão que associa para cada nome de campo da tabela um
	 *         vetor de objetos com os seguintes dados:
	 *         <ol start="0">
	 *         <li>inteiro que indica o tipo de dado ({@link Types});</i>
	 *         <li>tamanho do dado;</i>
	 *         <li>número de dígitos decimais (quando aplicável);</i>
	 *         <li><code>true</code> se o campo aceitar valores nulos,
	 *         <code>false</code> senão;</i>
	 *         <li><code>true</code> se for uma chave primária, <code>false</code>
	 *         senão;</i>
	 *         <li><code>true</code> se for um número e se ele for sinalizado,
	 *         <code>false</code> senão;</i>
	 *         <li><code>true</code> se ele pertencer ao índice, <code>false</code>
	 *         senão;</i>
	 *         <li><code>true</code> se for auto-incremental, <code>false</code>
	 *         senão.</i>
	 *         </ol>
	 * 
	 */
	public Map<String, Object[]> getFieldsNData(String table) {
		Map<String, Object[]> out = new LinkedHashMap<>();

		if (conn != null) {
			try {
				DatabaseMetaData dbmd = conn.getMetaData();
				ResultSet rs = null;
				int p = table.indexOf('.');
				if (p < 0)
					rs = dbmd.getColumns(null, null, table, "%");
				else
					rs = dbmd.getColumns(null, table.substring(0, p), table.substring(p + 1), "%");

				while (rs.next()) {
					if (this.type == Server.SASIOM) {
						String colName = rs.getString(4).trim();
						int data_type = rs.getInt(5);
						int decimal_digits = rs.getInt(9);
						int is_nullable = rs.getInt(11);
						out.put(colName, new Object[] { data_type, rs.getInt(7), decimal_digits, 1 == is_nullable,
								false, false, false, false });
					} else {
						String colName = rs.getString("COLUMN_NAME");
						out.put(colName,
								new Object[] { rs.getInt("DATA_TYPE"), rs.getInt("COLUMN_SIZE"),
										rs.getInt("DECIMAL_DIGITS"), "YES".equals(rs.getString("IS_NULLABLE")), false,
										false, false, "YES".equals(rs.getString("IS_AUTOINCREMENT")) });
					}
				}
				rs.close();

				// chaves primárias
				rs = dbmd.getPrimaryKeys(null, null, table);
				while (rs.next())
					out.get(rs.getString("COLUMN_NAME"))[4] = true;
				rs.close();

				// signed
				if (this.type != Server.SASIOM) {
					rs = this
							.query(this.getType() == Server.SQLserver ? String.format("SELECT TOP 1 * FROM `%s`", table)
									: (this.getType() == Server.Oracle
											? String.format("SELECT * FROM %s WHERE ROWNUM <= 1", table)
											: String.format("SELECT * FROM `%s` LIMIT 1", table)));
					ResultSetMetaData rsmd = rs.getMetaData();
					for (int i = 1; i <= rsmd.getColumnCount(); i++)
						out.get(rsmd.getColumnName(i))[5] = rsmd.isSigned(i);
					rs.close();
				}

				// índices
				if (p < 0)
					rs = dbmd.getIndexInfo(null, null, table, false, false);
				else
					rs = dbmd.getIndexInfo(null, table.substring(0, p), table.substring(p + 1), false, false);

				while (rs.next()) {
					String s = rs.getString("COLUMN_NAME");
					if (s != null)
						out.get(s)[6] = true;
				}
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return out;
	}

	/**
	 * 
	 * @param table
	 * @param field
	 * @return vetor de objetos com os seguintes dados:
	 *         <ol start="0">
	 *         <li>inteiro que indica o tipo de dado ({@link Types});</i>
	 *         <li>tamanho do dado;</i>
	 *         <li>número de dígitos decimais (quando aplicável);</i>
	 *         <li><code>true</code> se o campo aceitar valores nulos,
	 *         <code>false</code> senão;</i>
	 *         <li><code>true</code> se for uma chave primária, <code>false</code>
	 *         senão;</i>
	 *         <li><code>true</code> se for um número e se ele for sinalizado,
	 *         <code>false</code> senão;</i>
	 *         <li><code>true</code> se ele pertencer ao índice, <code>false</code>
	 *         senão.</i>
	 *         </ol>
	 */
	public Object[] getFieldData(String table, String field) {
		Object[] out = null;

		if (conn != null) {
			try {
				DatabaseMetaData dbmd = conn.getMetaData();
				ResultSet rs = dbmd.getColumns(null, null, table, field);

				if (rs.next())
					out = new Object[] { rs.getInt("DATA_TYPE"), rs.getInt("COLUMN_SIZE"), rs.getInt("DECIMAL_DIGITS"),
							"YES".equals(rs.getString("IS_NULLABLE")), false, false, false };
				else {
					rs.close();
					return null;
				}

				// chaves primárias
				rs = dbmd.getPrimaryKeys(null, null, table);
				if (rs.next())
					if (rs.getString("COLUMN_NAME").equals(field))
						out[4] = true;

				// signed
				rs = this.query(String.format("SELECT * FROM `%s` LIMIT 1", table));
				if (rs == null)
					return null;
				ResultSetMetaData rsmd = rs.getMetaData();
				for (int i = 1; i <= rsmd.getColumnCount(); i++)
					if (rsmd.getColumnName(i).equals(field))
						out[5] = rsmd.isSigned(i);

				// índices
				rs = dbmd.getIndexInfo(null, null, table, false, false);
				if (rs.next())
					if (rs.getString("COLUMN_NAME").equals(field))
						out[6] = true;

				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return out;
	}

	/**
	 * Função que retorna o nome de uma coluna de uma tabela. É a função inversa da
	 * {@link SQLadapter#getFieldIndex(String, String)}.
	 * 
	 * @param table  nome da tabela
	 * @param column índice da coluna da tabela (column >= 1)
	 * @return nome da coluna
	 */
	public String getField(String table, int column) {
		String out = null;

		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM " + table);
			ResultSetMetaData rsmd = rs.getMetaData();

			out = rsmd.getColumnName(column);

			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return out;
	}

	/**
	 * Função que retorna o índice da coluna da tabela cujo nome é repassado. É a
	 * função inversa da {@link SQLadapter#getField(String, int)}.
	 * 
	 * @param table nome da tabela
	 * @param field nome do campo
	 * @return índice da coluna da tabela
	 */
	public int getFieldIndex(String table, String field) {
		int index = -1;

		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM " + table);
			ResultSetMetaData rsmd = rs.getMetaData();

			int columns = rsmd.getColumnCount();
			for (int i = 1; i <= columns && index < 0; i++)
				if (field.equals(rsmd.getColumnName(i)))
					index = i;

			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return index;
	}

	/**
	 * Função que retorna o tipo de dado guardado numa dada coluna de uma tabela
	 * 
	 * @param table  nome da tabela
	 * @param column índice da coluna da tabela (column >= 1)
	 * @return tipo SQL segundo {@link Types}, ou -1 em caso de erro
	 */
	public int getFieldType(String table, int column) {
		int type = Integer.MAX_VALUE;

		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM " + table);
			ResultSetMetaData rsmd = rs.getMetaData();

			type = rsmd.getColumnType(column);

			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return type;
	}

	// ------------------ Fuso horário ------------------

	private void setTZ(TimeZone tz) {
		try {
			String s = null;
			if (TimeZone.getDefault().equals(tz)) {
				s = "SYSTEM";
			} else {
				s = tz.getID();
				if (s.startsWith("GMT")) {
					s = s.substring(3);
				} else {
					Calendar c = new GregorianCalendar(0, 0, 1);
					long l = tz.getRawOffset() + tz.getDSTSavings();
					c.add(Calendar.MILLISECOND, (int) Math.abs(l));
					s = String.format("%c%tR", l < 0 ? '-' : '+', c);
				}
			}
			Statement stmt = conn.createStatement();
			stmt.execute("SET time_zone = \'" + s + "\'");
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private TimeZone getTZ() {
		TimeZone out = null;
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT @@session.time_zone");

			if (rs.next()) {
				String s = rs.getString(1);
				if ("SYSTEM".equals(s))
					out = TimeZone.getDefault();
				else if (TZ_PAT.matcher(s).find())
					out = TimeZone.getTimeZone("GMT" + s);
				else
					out = TimeZone.getTimeZone(s);
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return out;
	}

	// ========================== EXPORT E IMPORT ==========================

	// --------------------------------- IN --------------------------------

	/**
	 * Função que carrega no servidor on-line todos os dados que estão no off-line.
	 * 
	 * @param estr <code>true</code> se há diferenças estruturais nas tabelas
	 *             on-line e off-line (nesse caso destrói-se as tabelas e elas são
	 *             reconstruídas antes de serem preenchidas).
	 */
	public void restoreOffline(boolean estr) {
		restoreOffline(estr, this.offline);
	}

	/**
	 * 
	 * Função que carrega no servidor on-line todos os dados que estão em um dado
	 * arquivo.
	 * 
	 * @param estr <code>true</code> se há diferenças estruturais nas tabelas
	 *             on-line e off-line (nesse caso destrói-se as tabelas e elas são
	 *             reconstruídas antes de serem preenchidas).
	 * @param file caminho do arquivo .sql que será carregado
	 */
	public void restoreOffline(boolean estr, String file) {
		if (file != null && conn != null) {
			// deve ser possível fazer as duas conexões (a on-line e a off-line)

			Iterator<String> it = getTables().iterator();

			// pega o fuso original
			TimeZone currentTZ = getTZ();

			// ==================================================
			// limpar base de dados

			if (estr) {
				// se for para mudar até mesmo a estrutura das tabelas, estas
				// devem ser eliminadas
				String query = "";
				if (it.hasNext()) {
					query += "DROP TABlE `" + it.next() + "`";
					while (it.hasNext())
						query += ", `" + it.next() + "`";
					update(query);
				}
			} else {
				// se não for para mudar a estrutura das tabelas, estas deverão
				// ser apenas esvaziadas
				while (it.hasNext())
					update("TRUNCATE `" + it.next() + "`");
			}

			// --------------------------------------------------
			// carregar base de dados

			Scanner sc = IOutils.getSc(file, "UTF-8");
			String s = null, statement = null;
			boolean flag = false;
			// varrendo o arquivo
			while (sc.hasNext()) {
				s = sc.nextLine().trim();

				// fuso horário em que as datas foram escritas
				if (s.startsWith("SET time_zone = \"") || s.startsWith("/*!40103 SET TIME_ZONE='")) {
					Matcher m = SQLadapter.TZ_PAT.matcher(s);
					m.find();
					setTZ(TimeZone.getTimeZone("GMT" + m.group()));
				}

				// inserindo dado ou criando uma tabela
				if (s.startsWith("INSERT") || (estr && s.startsWith("CREATE TABLE"))) {
					flag = true;
					statement = "";
				}

				if (flag) {
					statement += s;
					if (s.endsWith(";")) {
						update(statement);
						flag = false;
					}
				}
			}

			// ==================================================

			// volta para o fuso original
			setTZ(currentTZ);
		}
	}

	private final Pattern BLOB_HEX = Pattern.compile("0x\\p{XDigit}{2,}");

	private final Pattern BLOB_BIN = Pattern.compile("b'[01]{2,}'");

	public boolean importSQL(File sqlFile) {
		if (type == Server.MySQL) {
			if (MYSQL_FOLDER == null)
				if (!findMySqlFolder())
					return false;

			String[] executeCmd = new String[] { MYSQL_FOLDER + "mysql", "--user=" + this.login,
					"--password=" + this.password, this.db, "-e", " source " + sqlFile.getAbsolutePath() };
			try {
				Process process = Runtime.getRuntime().exec(executeCmd);
				int processComplete = process.waitFor();
				if (processComplete == 0) {
					System.out.println("Backup restored successfully");
					return true;
				} else
					System.err.println("Could not restore the backup");
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		} else if (type == Server.PostgreSQL) {
			ProcessBuilder builder = new ProcessBuilder("psql", "-U", this.login, "-d", this.db, "-f",
					sqlFile.getAbsolutePath());
			Map<String, String> env = builder.environment();
			env.put("PGPASSWORD", this.password);
			try {
				Process process = builder.start();
				int processComplete = process.waitFor();
				if (processComplete == 0) {
					System.out.println("Backup restored successfully");
				} else
					System.err.println("Could not restore the backup");
				return true;
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		} else if (type == Server.SQLite) {
			Scanner sc = IOutils.getSc(sqlFile, "UTF-8");

			int flag = 0;
			StringBuilder block = null;
			String tableName = null;
			Set<String> index = null;
			while (sc.hasNext()) {
				String s = sc.nextLine();

				if (s.startsWith("--") || s.startsWith("/*") || s.startsWith("SET") || s.startsWith("DROP")
						|| s.startsWith("LOCK") || s.startsWith("UNLOCK")) // comentários e demais comandos
					continue;
				boolean create = s.startsWith("CREATE");
				if (create || s.startsWith(INSERT)) {
					flag = create ? 1 : 2;
					block = new StringBuilder();
				}
				if (flag > 0) {
					if (flag == 1) { // fields
						int pos = s.indexOf("COMMENT");
						if (pos >= 0) { // SQLite não aceita comentários
							int e = s.indexOf('\'', pos + 9) + 1;
							s = s.substring(0, pos) + s.substring(e);
						}
						pos = s.indexOf("enum(");
						if (pos >= 0) { // SQLite não aceita enumerações
							int e = s.indexOf(')') + 1;
							s = s.substring(0, pos) + "tinytext" + s.substring(e);
						}
						pos = s.indexOf("AUTO_INCREMENT");
						if (pos >= 0) // SQLite não aceita auto incrementação
							s = s.substring(0, pos) + s.substring(pos + 14);
						pos = s.indexOf("unsigned");
						if (pos >= 0) // SQLite não aceita unsigned
							s = s.substring(0, pos) + s.substring(pos + 8);
						if (s.startsWith("  KEY")) { // índice
							if (index == null) { // primeiro campo do índice
								index = new HashSet<>();
								// guarda o nome da tabela (para nomear o índice)
								tableName = block.toString();
								int b = tableName.indexOf('`') + 1;
								int e = StringUtils.ordinalIndexOf(tableName, '`', 1);
								tableName = tableName.substring(b, e);
								// remove a vírgula da linha anterior
								block.setLength(block.length() - 1);
							}
							index.add(s.substring(s.indexOf('(') + 2, s.indexOf(')') - 1));
							s = "";
						}
					} else { // table content
						// trocar os blob na forma 0xhhhh+ por x'hhhh'

						Matcher m = BLOB_HEX.matcher(s);
						StringBuffer sb = new StringBuffer(s.length());
						while (m.find()) {
							String g = m.group();
							g = "x'" + g.substring(2) + "'";
							m.appendReplacement(sb, Matcher.quoteReplacement(g));
						}
						m.appendTail(sb);
						s = sb.toString();

						// trocar os binários na forma b'bbbbbbbb' por x'hh'
						m = BLOB_BIN.matcher(s);
						sb = new StringBuffer(s.length());
						while (m.find()) {
							String g = m.group();
							g = "x'" + StringUtils.toHex(BinaryUtils.toBytesArray(g.substring(2, g.length() - 1)))
									+ "'";
							m.appendReplacement(sb, Matcher.quoteReplacement(g));
						}
						m.appendTail(sb);

						s = sb.toString().replaceAll("\\\\'", "''");
					}
					block.append(s);
				}
				if (s.endsWith(";")) {
					String update = block.toString();
					if (flag == 1) { // remove table options
						int pos = update.lastIndexOf(')') + 1;
						update = update.substring(0, pos) + ";";
					}
					this.update(update);
					flag = 0;
					block = null;
					if (index != null) {
						for (String k : index)
							this.update(String.format("CREATE INDEX idx_%1$s_%2$s ON %1$s (%2$s);", tableName, k));
						index = null;
					}
				}
			}

			sc.close();
		} else
			new UnsupportedOperationException(
					"Não é possível ainda importar bases de dados que não sejam MySQL, PostgreSQL e SQLite");
		return false;

	}

	// --------------------------------- OUT ---------------------------------

	/**
	 * Função que faz a exportação da base de dados SQL para um arquivo .sql
	 * 
	 * @param file arquivo .sql
	 * @param args argumentos extras necessários (e.g., para o banco Oracle do
	 *             Atlatis: ATL_ORA_ELECTRIC)
	 */
	public void exportSQL(File file, String... args) {
		if (type == Server.MySQL) {
			if (MYSQL_FOLDER == null)
				if (!findMySqlFolder())
					return;

			boolean pw = !"".equals(this.password);

			String dumpCommand = String.format("%smysqldump -h %s -u %s%s --hex-blob %s", MYSQL_FOLDER, this.server,
					this.login, pw ? " -p --password=" + this.password : "", this.db);
			if (debug)
				System.out.println(dumpCommand);
			Runtime rt = Runtime.getRuntime();

			try {
				Process p = rt.exec(dumpCommand);
				PrintStream ps = new PrintStream(file);
				InputStream in = p.getInputStream();
				int ch;
				while ((ch = in.read()) != -1)
					ps.write(ch);

				InputStream err = p.getErrorStream();
				while ((ch = err.read()) != -1)
					System.out.write(ch);

				ps.close();
			} catch (Exception exc) {
				exc.printStackTrace();
			}
		} else if (type == Server.Oracle) {
			if (args.length == 0)
				new IllegalArgumentException("Para bancos Oracle, precisa-se indicar qual biblioteca");

			try {
				DatabaseMetaData dbmd = conn.getMetaData();
				ResultSet rs = dbmd.getTables(null, args[0], "%", new String[] { "TABLE" });

				while (rs.next()) {
					String tableName = rs.getString(3);

					OraclePreparedStatement ps = (OraclePreparedStatement) conn
							.prepareStatement("CALL SYSCS_UTIL.SYSCS_EXPORT_TABLE (?,?,?,?,?,?)");

					ps.setString(1, null);
					ps.setString(2, tableName);
					ps.setString(3, tableName + ".dat");
					ps.setString(4, "%");
					ps.setString(5, null);
					ps.setString(6, null);
					ps.execute();

					ps.close();
				}
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} else if (type == Server.PostgreSQL) {
			ProcessBuilder builder = new ProcessBuilder("pg_dump", "-U", this.login, "-d", this.db, "-f",
					file.getAbsolutePath());
			Map<String, String> env = builder.environment();
			env.put("PGPASSWORD", this.password);
			try {
				Process p = builder.start();
				PrintStream ps = new PrintStream(file);
				InputStream in = p.getInputStream();
				int ch;
				while ((ch = in.read()) != -1)
					ps.write(ch);

				InputStream err = p.getErrorStream();
				while ((ch = err.read()) != -1)
					System.out.write(ch);

				ps.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		} else // só funciona para MySQL, PostGreSQL e Oracle
			new UnsupportedOperationException(
					"Não é possível ainda exportar bases de dados que não sejam MySQL e Oracle");
	}

	private static boolean findMySqlFolder() {
		File[] fs = (new File("C:\\")).listFiles();
		for (int i = 0; i < fs.length; i++) {
			File f0 = fs[i];
			if (f0.getName().startsWith("wamp")) {
				File f = new File(f0.getAbsolutePath() + "\\bin\\mysql\\");
				if (f.exists() ? f.listFiles().length > 0 : false) {
					f = f.listFiles()[0];
					f = new File(f.getAbsolutePath() + "\\bin\\");

					MYSQL_FOLDER = f.getAbsolutePath() + "\\";
					return true;
				} else {
					f = new File(f0.getAbsolutePath() + "\\bin\\mariadb\\");
					if (f.exists() ? f.listFiles().length > 0 : false) {
						f = f.listFiles()[0];
						f = new File(f.getAbsolutePath() + "\\bin\\");

						MYSQL_FOLDER = f.getAbsolutePath() + "\\";
						return true;
					}
				}
			}
		}
		return false;
	}

	/**
	 * Função que baixa os valores de uma tabela de uma base de dados SQL e os
	 * escreve na forma de um arquivo txt, onde as colunas são separadas por
	 * tabulação e as linhas por quebra
	 * 
	 * @param folder caminho do diretório de destino
	 * @param table  nome da tabela a ser baixada
	 */
	public void sql2txt(String folder, String table) {
		File f = new File(folder + "/" + table + ".txt");
		try {
			RandomAccessFile raf = new RandomAccessFile(f, "rw");

			String query = "SELECT * FROM " + table;

			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(query);

			// cabeçalho
			ResultSetMetaData rsmd = rs.getMetaData();
			int columns = rsmd.getColumnCount();
			for (int j = 1; j <= columns; j++)
				raf.writeBytes(rsmd.getColumnName(j) + "\t");

			// para cada entrada da BD
			while (rs.next()) {
				raf.writeBytes("\n");
				for (int j = 1; j <= columns; j++)
					raf.writeBytes(rs.getString(j) + "\t");
			}
			rs.close();
			stmt.close();

			raf.close();
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
	}

	// ================================ QUERIES ================================

	// conjunto de resultados

	/**
	 * Função que retorna o conjunto de resultados de uma query
	 * 
	 * @param query linha de comando SQL
	 * @return conjunto de resultados
	 */
	public ResultSet query(String query) {
		ResultSet rs = null;
		int i = getMainConn();
		switch (i) {
		case ON_LINE:
			try {
				switch (this.type) {
				case SQLite:
					query = query.replace("b'", "'");
				case SQLserver:
				case SASIOM:
					query = query.replace("`", "");
					break;
				case PostgreSQL:
					query = query.replace("`", "").replace(" WHERE 1", " WHERE TRUE").replace("b'1'", "TRUE")
							.replace("b'1'", "FALSE");
					break;
				default:
					break;
				}
				Statement st = conn.createStatement();
				rs = st.executeQuery(query);
			} catch (SQLRecoverableException e) {
				if (debug)
					System.out.println(
							"Houve desconexão com a base de dados por inatividade prolongada. Reconectando...");
				reconnectDB();
				return query(query);
			} catch (SQLException e) {
				e.printStackTrace();
			}
			break;
		case OFF_LINE:
			rs = new OffLineRS(sqlOffline.executeQuery(query));
			break;
		}
		return rs;
	}

	// busca horizontal

	/**
	 * Função que retorna o primeiro objeto associado ao valor de um dado campo da
	 * tabela
	 * 
	 * @param fieldOut campo do objeto a ser retornado
	 * @param table    nome da tabela
	 * @param fieldIn  campo da tabela
	 * @param valueIn  valor do campo
	 * @return objeto correspondente (<code>null</code> caso não seja encontrado)
	 */
	public Object search(String fieldOut, String table, String fieldIn, Object valueIn) {
		Object[] out = search(table, fieldIn, valueIn, fieldOut);
		if (out != null)
			return out[0];
		else
			return null;
	}

	/**
	 * Função que retorna os objetos associados ao valor de uma dada entrada da
	 * tabela
	 * 
	 * @param table     nome da tabela
	 * @param fieldIn   campo da tabela (preferencialmente a chave primária, de modo
	 *                  a segurar que a query retorna um só elemento)
	 * @param valueIn   valor do campo
	 * @param fieldsOut vetor de nomes dos campos dos objetos a serem retornados (se
	 *                  o vetor está vazio, seleciona-se todos os campos)
	 * @return vetor de objetos procurados
	 */
	public Object[] search(String table, String fieldIn, Object valueIn, String... fieldsOut) {
		return search(table, new String[] { fieldIn }, new Object[] { valueIn }, fieldsOut);
	}

	/**
	 * Função que retorna os objetos associados ao valor de uma dada entrada da
	 * tabela
	 * 
	 * @param table     nome da tabela
	 * @param fieldsIn  vetor com os campos a serem procurados
	 * @param valuesIn  vetor com os valores a serem procurados
	 * @param fieldsOut vetor de nomes dos campos dos objetos a serem retornados (se
	 *                  o vetor está vazio, seleciona-se todos os campos)
	 * @return vetor de objetos procurados
	 */
	public Object[] search(String table, String[] fieldsIn, Object[] valuesIn, String... fieldsOut) {
		String query = null;
		// -------------- preparar statement --------------
		if (fieldsOut.length == 0)
			query = "SELECT *";
		else
			query = "SELECT `" + StringUtils.addSeparator(fieldsOut, "`, `") + "`";

		// tabela e condições
		query += String.format(" FROM `%s`%s", table, getWhere(fieldsIn, valuesIn, this.type));

		return search(query);
	}

	/**
	 * Função que retorna os objetos associados a uma entrada, localizada numa dada
	 * posição
	 * 
	 * @param table     nome da tabela
	 * @param pos       inteiro não negativo indicando a posição na busca SQL
	 * @param fieldsOut vetor de nomes dos campos dos objetos a serem retornados (se
	 *                  o vetor está vazio, seleciona-se todos os campos)
	 * @return vetor de objetos procurados
	 */
	public Object[] search(String table, int pos, String... fieldsOut) {
		String query = null;
		// -------------- preparar statement --------------

		if (this.type == Server.MySQL) {
			// campos
			if (fieldsOut.length == 0)
				query = "SELECT *";
			else
				query = "SELECT `" + StringUtils.addSeparator(fieldsOut, "`, `") + "`";

			// tabela e condições
			query += String.format(" FROM `%s` LIMIT %d, 1", table, pos);
		} else {
			// o comando 'LIMIT' não faz parte do SQL padrão, logo para se obter o elemento
			// numa dada posição, deve-se improvisar a criação de uma tabela intermediária
			// com a numeração
			List<String> fs = getFields(table);
			String[] fields = fs.toArray(new String[fs.size()]);
			String select = StringUtils.addSeparator(fieldsOut.length == 0 ? fields : fieldsOut, ", ");
			query = String.format("SELECT " + select + " FROM (SELECT ROW_NUMBER() OVER(ORDER BY %s) NUM, " + select
					+ " FROM %s) A WHERE NUM=%d", fields[0], table, pos + 1);
		}

		return search(query);
	}

	/**
	 * Função que retorna os objetos associados a uma dada entrada obtida da busca
	 * SQL definida numa dada query
	 * 
	 * @param query comando SQL <code>SELECT</code>
	 * @return vetor de objetos relacionados a somente primeira entrada da busca (se
	 *         nada for encontrado, retorna-se um vetor de elementos nulos com o
	 *         número de posições igual ao número de campos requisitados na query)
	 */
	public Object[] search(String query) {
		Object[] out = null;
		int i = getMainConn();
		switch (i) {
		case ON_LINE:
			// --------------- on-line ---------------
			try {
				ResultSet rs = query(query);
				if (rs != null) {
					out = new Object[rs.getMetaData().getColumnCount()];
					if (rs.next())
						for (int j = 0; j < out.length; j++)
							out[j] = rs.getObject(j + 1);
					rs.getStatement().close();
					rs.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			break;
		case OFF_LINE:
			// --------------- off-line ---------------

			List<Object[]> os = sqlOffline.executeQuery(query);
			if (os.size() > 0)
				out = os.get(0);
			else
				out = new Object[sqlOffline.getColumnCount(query)];
			break;
		}
		return out;
	}

	// busca vertical

	/**
	 * Função que retorna a lista de todos objetos de uma dada coluna
	 * 
	 * @param c     coleção que será preenchida com os objetos da busca
	 * @param table nome da tabela do SQL
	 * @param field campo a ser relacionado
	 * @return lista dos objetos deste campo
	 */
	public <V> void list(Collection<V> c, String table, String field) {
		list(c, String.format("SELECT `%s` FROM `%s`", field, table));
	}

	/**
	 * Função que retorna a lista de todos objetos de uma dada coluna que cumpram
	 * com uma dada condição
	 * 
	 * @param c       coleção que será preenchida com os objetos da busca
	 * @param table   nome da tabela do SQL
	 * @param field   campo a ser relacionado
	 * @param fieldIn campo da tabela (se for a chave primária, a lista conterá
	 *                somente um só elemento)
	 * @param valueIn valor do campo
	 * @return
	 */
	public <V> void list(Collection<V> c, String table, String field, String fieldIn, Object valueIn) {
		list(c, table, field, new String[] { fieldIn }, new Object[] { valueIn });
	}

	public <V> void list(Collection<V> c, String table, String field, String[] fieldsIn, Object[] valuesIn) {
		list(c, String.format("SELECT `%s` FROM `%s`%s", field, table, getWhere(fieldsIn, valuesIn, this.type)));
	}

	/**
	 * Função que retorna a lista de todos objetos de uma query
	 * 
	 * @param c     coleção que será preenchida com os objetos da busca
	 * @param query expressão SQL da busca
	 * @return lista de objetos do primeiro campo da busca
	 */
	@SuppressWarnings("unchecked")
	public <V> void list(Collection<V> c, String query) {
		int i = getMainConn();
		switch (i) {
		case ON_LINE: // --------------- on-line ---------------
			ResultSet rs = query(query);
			if (rs != null) {
				try {
					while (rs.next())
						c.add((V) rs.getObject(1));

					rs.getStatement().close();
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			break;
		case OFF_LINE: // --------------- off-line ---------------
			List<Object[]> os = sqlOffline.executeQuery(query);
			for (Object[] o : os)
				c.add((V) o[0]);
			break;
		}
	}

	// tabelas

	/**
	 * 
	 * @param map
	 * @param table      nome da tabela do SQL
	 * @param fieldKey   campo do objeto a ser usado como chave
	 * @param fieldValue campo do objeto a ser usado como valor
	 */
	public <K, V> void table(Map<K, V> map, String table, String fieldKey, String fieldValue) {
		table(map, String.format("SELECT `%s`, `%s` FROM `%s`", fieldKey, fieldValue, table));
	}

	/**
	 * 
	 * @param map
	 * @param table      nome da tabela do SQL
	 * @param fieldKey   campo do objeto a ser usado como chave
	 * @param fieldValue campo do objeto a ser usado como valor
	 * @param fieldIn    campo de busca (se for a chave primária, a tabela conterá
	 *                   somente um só elemento)
	 * @param valueIn    valor do campo de busca
	 */
	public <K, V> void table(Map<K, V> map, String table, String fieldKey, String fieldValue, String fieldIn,
			Object valueIn) {
		table(map, table, fieldKey, fieldValue, new String[] { fieldIn }, new Object[] { valueIn });
	}

	public <K, V> void table(Map<K, V> map, String table, String fieldKey, String fieldValue, String[] fieldsIn,
			Object[] valuesIn) {
		table(map, String.format("SELECT `%s`, `%s` FROM `%s`%s", fieldKey, fieldValue, table,
				getWhere(fieldsIn, valuesIn, this.type)));
	}

	@SuppressWarnings("unchecked")
	public <K, V> void table(Map<K, V> map, String query) {
		int i = getMainConn();
		switch (i) {
		case ON_LINE:
			// --------------- on-line ---------------
			ResultSet rs = query(query);
			if (rs == null)
				return;
			try {
				while (rs.next())
					map.put((K) rs.getObject(1), (V) rs.getObject(2));
				rs.getStatement().close();
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			break;
		case OFF_LINE:
			// --------------- off-line ---------------
			List<Object[]> os = sqlOffline.executeQuery(query);
			for (Object[] o : os)
				map.put((K) o[0], (V) o[1]);
			break;
		}
	}

	// existência de uma dada tabela

	/**
	 * Função que indica se uma dada tabela existe ou não numa base de dados
	 * 
	 * @param table nome da tabela
	 * @return <code>true</code> caso exista, <code>false</code> senão
	 */
	public boolean exist(String table) {
		if (table == null)
			return false;
		String[] d = table.split("\\.");
		try {
			DatabaseMetaData md = conn.getMetaData();
			ResultSet rs = null;
			if (d.length > 1)
				rs = md.getTables(null, d[0], d[1], null);
			else
				rs = md.getTables(null, null, d[0], null);
			boolean out = rs.next();
			rs.close();
			return out;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * Função que indica se um campo existe ou não em uma dada tabela numa base de
	 * dados
	 * 
	 * @param table nome da tabela
	 * @param field nome do campo
	 * @return <code>true</code> caso exista, <code>false</code> senão
	 */
	public boolean exist(String table, String field) {
		if (table == null)
			return false;
		String[] d = table.split("\\.");
		try {
			DatabaseMetaData md = conn.getMetaData();
			ResultSet rs = null;
			if (d.length > 1)
				rs = md.getColumns(null, d[0], d[1], field);
			else
				rs = md.getColumns(null, null, d[0], field);
			boolean out = rs.next();
			rs.close();
			return out;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	// existência de uma dada entrada

	/**
	 * Função que procura na base de dados SQL se uma dada entrada está contida
	 * 
	 * @param table nome da tabela do SQL
	 * @param field campo a ser procurado
	 * @param value valor a ser procurado
	 * @return <code>true</code> se a entrada consta na base de dados,
	 *         <code>false</code> senão
	 */
	public boolean exist(String table, String field, Object value) {
		return exist(table, new String[] { field }, new Object[] { value });
	}

	/**
	 * Função que procura na base de dados SQL se uma dada entrada está contida
	 * 
	 * @param table  nome da tabela do SQL
	 * @param fields vetor com os campos a serem procurados
	 * @param values vetor com os valores a serem procurados
	 * @return <code>true</code> se a entrada consta na base de dados,
	 *         <code>false</code> senão
	 */
	public boolean exist(String table, String[] fields, Object[] values) {
		if (values.length == 0)
			throw new IllegalArgumentException(
					"Para verificar existência do registro, deve haver pelo menos uma condição.");

		boolean out = false;

		if (getType() != Server.SASIOM)
			table = "`" + table + "`";

		// query
		String query = String.format("SELECT * FROM %s%s", table, getWhere(fields, values, this.type));

		int i = getMainConn();
		switch (i) {
		case ON_LINE:
			try {
				Statement st = (Statement) conn.createStatement();
				ResultSet rs = st.executeQuery(query);
				out = rs.next();
				rs.close();
				st.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			break;
		case OFF_LINE:
			out = sqlOffline.executeQuery(query).size() > 0;
			break;
		default:
			break;
		}
		return out;
	}

	public boolean existDb(String db) {
		String query = null;
		if (type == Server.PostgreSQL) {
			query = "SELECT 1 FROM pg_database WHERE datname='" + db + "'";
		} else if (type == Server.MySQL) {
			query = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '" + db + "'";
		}
		boolean out = false;
		if (query != null) {
			try {
				Statement st = (Statement) conn.createStatement();
				ResultSet rs = st.executeQuery(query);
				out = rs.next();
				rs.close();
				st.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return out;
	}

	/**
	 * Função que retorna o número total de entradas de uma tabela SQL
	 * 
	 * @param table nome da tabela
	 * @return número de entradas
	 */
	public int getRowCount(String table) {
		return getRowCount(table, "");
	}

	/**
	 * Função que retorna o número total de entradas de uma tabela SQL que atendem a
	 * uma dada condição
	 * 
	 * @param table nome da tabela
	 * @param where sequência de caracteres na forma ' WHERE '...
	 * @return número de entradas
	 */
	public int getRowCount(String table, String where) {
		int out = -1;
		int i = getMainConn();

		if (getType() != Server.Oracle && getType() != Server.SASIOM)
			table = "`" + table + "`";

		switch (i) {
		case ON_LINE: // --------------- on-line ---------------
			ResultSet rs = query("SELECT COUNT(*) FROM " + table + where);
			try {
				if (rs.next())
					out = rs.getInt(1);

				rs.getStatement().close();
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			break;
		default:
			System.err.println("Não foi possível se conectar à base de dados para contagem do número de entradas.\n");
			break;
		}
		return out;
	}

	/**
	 * Função que procura por uma chave primária na forma de número inteiro que
	 * ainda não esteja presente na tabela. A chave possuirá o menor valor em módulo
	 * possível.
	 * 
	 * @param key   nome do campo que está varrido
	 * @param table nome da tabela onde está feita a busca
	 * @return número inteiro da chave não utilizada
	 */
	public int getVacantKey(String key, String table) {
		return getVacantKey(key, table, 0);
	}

	/**
	 * Função que procura por uma chave primária na forma de número inteiro que
	 * ainda não esteja presente na tabela.
	 * 
	 * @param key   nome do campo que está varrido
	 * @param table nome da tabela onde está feita a busca
	 * @param min   valor mínimo que a chave pode ser
	 * @return número inteiro da chave não utilizada
	 */
	public int getVacantKey(String key, String table, int min) {
		int[] out = getVacantKeys(key, table, min, 1);
		if (out != null)
			return out[0];
		else
			return min - 1;
	}

	/**
	 * Função que procura por um conjunto de chaves primárias na forma de números
	 * inteiros que ainda não estejam presentes na tabela
	 * 
	 * @param key   nome do campo que está varrido
	 * @param table nome da tabela onde está feita a busca
	 * @param min   valor mínimo que a chave pode ser
	 * @param num   número de chaves a serem retornadas
	 * @return vetor com as chaves não utilizadas
	 */
	public int[] getVacantKeys(String key, String table, int min, int num) {
		// pelo menos uma chave deve ser retornada
		if (num < 1)
			return null;

		int[] out = new int[num];
		int i = 0;
		out[i] = min;

		int s = getMainConn();
		switch (s) {
		case ON_LINE:
			try {
				Statement st = (Statement) conn.createStatement();
				ResultSet rs = st.executeQuery(String.format("SELECT %1$s FROM %2$s ORDER BY %1$s ASC", key, table));

				// por os resultados num conjunto impede que haja elementos repetidos (caso
				// `key` não seja uma chave primária...)
				TreeSet<Integer> already = new TreeSet<>();
				while (rs.next())
					already.add(rs.getInt(1));

				// procurar chave
				for (Integer k : already) {
					while (out[i] != k) {
						// se a proxima da lista não for a anterior mais um, há
						// um espaço vago
						i++;

						// se encheu-se a lista, devolve o vetor
						if (i == num) {
							st.close();
							rs.close();
							return out;
						} else {
							out[i] = out[i - 1] + 1;
						}
					}
					out[i] = k + 1;
				}

				while (++i != num)
					out[i] = out[i - 1] + 1;

				st.close();
				rs.close();
				return out;
			} catch (SQLException e) {
				e.printStackTrace();
			}
			return null;
		case OFF_LINE:
			List<Object[]> os = sqlOffline.executeQuery("SELECT `" + key + "` FROM `" + table + "`");

			// primeiro valor
			TreeSet<Integer> keys = new TreeSet<>();

			// ordenar
			for (Object[] o : os)
				keys.add((Integer) o[0]);

			// procurar chave
			for (Integer k : keys) {
				while (out[i] != k) {
					// se a proxima da lista não for a anterior mais um, há um espaco vago
					i++;

					// se encheu-se a lista, devolve o vetor
					if (i == num)
						return out;
					else
						out[i] = out[i - 1] + 1;
				}
				out[i] = k + 1;
			}

			while (++i != num)
				out[i] = out[i - 1] + 1;

			return out;
		default:
			System.err.println("Não foi possível se conectar à base de dados.");
			return null;
		}
	}

	/**
	 * Função que retorna o número identificador da última linha inserida
	 * 
	 * @return número da última linha inserida
	 */
	public int getLastInsertId() {
		ResultSet rs = query(
				"SELECT " + (this.getType() == Server.SQLite ? "last_insert_rowid" : "LAST_INSERT_ID") + "()");
		if (rs == null)
			return 0;
		int out = 0;
		try {
			rs.next();
			out = rs.getInt(1);
			rs.close();
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		return out;
	}

	public void removeDuplicate(String table, String... fields) {
		try {
			Statement st = (Statement) conn.createStatement();
			ResultSet rs = st.executeQuery(String.format("SELECT * FROM `%s`", table));

			ResultSetMetaData md = rs.getMetaData();
			int cols = md.getColumnCount();
			int[] types = new int[cols];
			StringBuilder fs = new StringBuilder(md.getColumnName(1));
			types[0] = md.getColumnType(1);
			for (int i = 1; i < cols; i++) {
				fs.append("`, `");
				fs.append(md.getColumnName(i + 1));
				types[i] = md.getColumnType(i + 1);
			}
			final String ru = String.format("INSERT INTO `%s`(`%s`) VALUES (%s)", table, fs,
					StringUtils.addSeparator("?", ",", cols));

			if (fields.length == 1) { // se só há um campo-chave onde se procura duplicatas
				Set<Object> keys = new HashSet<>();

				Set<Object> apagar;
				Collection<Object[]> reinserir;
				if (fields.length == cols) { // se este único campo é o único da tabela
					Set<Object> duplicateRows = new HashSet<>();
					while (rs.next()) {
						Object k = rs.getObject(fields[0]);
						boolean c = !keys.add(k);
						if (c)
							duplicateRows.add(k);
					}
					apagar = duplicateRows;
					reinserir = null;
				} else { // se este único campo não é o único campo da tabela
					Map<Object, Object[]> duplicateRows = new HashMap<>();
					while (rs.next()) {
						Object k = rs.getObject(fields[0]);
						boolean c = !keys.add(k);
						if (c) {
							Object[] os = new Object[cols];
							for (int i = 0; i < os.length; i++)
								os[i] = rs.getObject(i + 1);
							duplicateRows.put(k, os);
						}
					}
					apagar = duplicateRows.keySet();
					reinserir = duplicateRows.values();
				}

				// apaga...
				update(String.format("DELETE FROM `%s`%s", table, getWhere(fields[0], apagar, getType())));

				// ... e reinsere
				if (reinserir != null)
					for (Object[] e : reinserir)
						update(ru, types, e);
				else
					for (Object e : apagar)
						update(ru, types, e);
			} else { // se há mais de um campo-chave onde se procura duplicatas
				Set<List<Object>> keys = new HashSet<>();

				Set<List<Object>> apagar;
				Collection<Object[]> reinserir;

				if (fields.length == cols) { // se os campos-chaves são os
												// únicos da tabela
					Set<List<Object>> duplicateRows = new HashSet<>();
					while (rs.next()) {
						List<Object> ks = new ArrayList<>(fields.length);
						for (int i = 0; i < fields.length; i++)
							ks.add(rs.getObject(fields[i]));
						boolean c = !keys.add(ks);
						if (c)
							duplicateRows.add(ks);
					}
					apagar = duplicateRows;
					reinserir = null;
				} else { // se os campos-chaves não são os únicos campos da
							// tabela
					Map<List<Object>, Object[]> duplicateRows = new HashMap<>();
					while (rs.next()) {
						List<Object> ks = new ArrayList<>(fields.length);
						for (int i = 0; i < fields.length; i++)
							ks.add(rs.getObject(fields[i]));
						boolean c = !keys.add(ks);
						if (c) {
							Object[] os = new Object[cols];
							for (int i = 0; i < os.length; i++)
								os[i] = rs.getObject(i + 1);
							duplicateRows.put(ks, os);
						}
					}
					apagar = duplicateRows.keySet();
					reinserir = duplicateRows.values();
				}

				// apaga...
				StringBuilder where = new StringBuilder();
				for (List<Object> ks : apagar) {
					where.append("(`");
					where.append(fields[0]);
					where.append("`=");
					where.append(ks.get(0));
					for (int i = 1; i < fields.length; i++) {
						where.append(" AND `");
						where.append(fields[i]);
						where.append("`=");
						where.append(ks.get(i));
					}
					where.append(") OR ");
				}
				update(String.format("DELETE FROM `%s` WHERE %s", table, where.substring(0, where.length() - 4)));

				// ... e reinsere
				if (reinserir != null)
					for (Object[] e : reinserir)
						update(ru, types, e);
				else
					for (List<Object> e : apagar)
						update(ru, types, e.toArray(new Object[e.size()]));
			}
			st.close();
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// ================================ UPDATE ================================

	/**
	 * Função que altera dados na base de dados a partir de uma expressão de comando
	 * SQL (seja do tipo 'INSERT' ou 'UPDATE').
	 * 
	 * @param update linha de comando SQL
	 * @return <code>true</code> se a alteração for bem sucedida, <code>false</code>
	 *         senão
	 */
	public boolean update(String update) {
		if (update == null)
			return false;
		int i = getMainConn();
		switch (i) {
		case ON_LINE:
			try {
				PreparedStatement statement = null;
				update = adjustStatement(update, this.type);
				switch (this.type) {
				case MySQL:
					statement = (com.mysql.jdbc.PreparedStatement) conn.prepareStatement(update);
					break;
				case SQLserver:
					statement = (SQLServerPreparedStatement) conn.prepareStatement(update);
					break;
				case SASIOM:
					statement = (MVAPreparedStatement) conn.prepareStatement(update);
					break;
				case SQLite:
					statement = (JDBC4PreparedStatement) conn.prepareStatement(update);
					break;
				case Oracle:
					statement = (OraclePreparedStatement) conn.prepareStatement(update);
					break;
				case UCanAccess:
					statement = (UcanaccessPreparedStatement) conn.prepareStatement(update);
					break;
				case PostgreSQL:
					statement = conn.prepareStatement(update);
					break;
				case SQLoffLine:
					break;
				}

				int out = statement.executeUpdate();
				statement.close();
				return out > 0;
			} catch (SQLRecoverableException e) {
				if (debug)
					System.out.println(
							"Houve desconexão com a base de dados por inatividade prolongada. Reconectando...");
				reconnectDB();
				return update(update);
			} catch (DataTruncation e) {
				System.err.println(
						"MysqlDataTruncation: " + (update.length() > 1200 ? update.substring(0, 1200) : update));
			} catch (SQLException e) {
				System.err.println("SQLException. Statement: " + update + "\nMessage: " + e.getMessage());
			}
			break;
		default:
			System.err.println("Não foi possível se conectar à base de dados para alteração dos dados.\n" + update);
			break;
		}
		return false;
	}

	/**
	 * Função que altera dados na base de dados a partir de uma expressão de comando
	 * SQL (seja do tipo 'INSERT' ou 'UPDATE').
	 * 
	 * @param update linha de comando SQL, com '?' para cada objeto a ser
	 *               substituído
	 * @param types
	 * @param objs   objetos a serem substituídos na expressão do comando SQL
	 * @return <code>true</code> se a alteração for bem sucedida, <code>false</code>
	 *         senão
	 */
	public boolean update(String update, int[] types, Object... objs) {
		int i = getMainConn();
		switch (i) {
		case ON_LINE:
			try {
				PreparedStatement statement = null;

				update = adjustStatement(update, this.type);
				switch (this.type) {
				case MySQL:
					statement = (com.mysql.jdbc.PreparedStatement) conn.prepareStatement(update);
					break;
				case SQLserver:
					statement = (SQLServerPreparedStatement) conn.prepareStatement(update);
					break;
				case SASIOM:
					statement = (MVAPreparedStatement) conn.prepareStatement(update);
					break;
				case SQLite:
					statement = (JDBC4PreparedStatement) conn.prepareStatement(update);
					break;
				case Oracle:
					statement = (OraclePreparedStatement) conn.prepareStatement(update);
					break;
				case UCanAccess:
					statement = (UcanaccessPreparedStatement) conn.prepareStatement(update);
					break;
				case PostgreSQL:
					statement = conn.prepareStatement(update);
					break;
				case SQLoffLine:
					break;
				}
				replace(statement, types, objs);

				int out = statement.executeUpdate();
				statement.close();
				return out > 0;
			} catch (SQLRecoverableException e) {
				if (debug)
					System.out.println(
							"Houve desconexão com a base de dados por inatividade prolongada. Reconectando...");
				reconnectDB();
				return update(update, types, objs);
			} catch (DataTruncation e) {
				System.err.println(
						"MysqlDataTruncation: " + (update.length() > 1200 ? update.substring(0, 1200) : update));
			} catch (SQLException e) {
				System.err.println("SQLException. Statement: " + update + "\nMessage: " + e.getMessage());
			}
			break;
		default:
			System.err.println("Não foi possível se conectar à base de dados para alteração dos dados.\n" + update);
			break;
		}
		return false;
	}

	private static void replace(PreparedStatement ps, int[] types, Object... objs) throws SQLException {
		for (int i = 0; i < objs.length; i++) {
			Object object = objs[i];
			if (object == null)
				ps.setNull(i + 1, types[i]);
			else {
				if (types == null) {
					if (object instanceof Integer)
						ps.setInt(i + 1, (int) object);
					else if (object instanceof Float)
						ps.setFloat(i + 1, (float) object);
					else if (object instanceof Double)
						ps.setDouble(i + 1, (double) object);
					else if (object instanceof String)
						ps.setString(i + 1, (String) object);
					else if (object instanceof Timestamp)
						ps.setTimestamp(i + 1, (Timestamp) object);
					else if (object instanceof Calendar)
						ps.setTimestamp(i + 1, new Timestamp(((Calendar) object).getTimeInMillis()));
					else if (object instanceof Date)
						ps.setDate(i + 1, new java.sql.Date(((Date) object).getTime()));
					else if (object instanceof byte[])
						ps.setBytes(i + 1, (byte[]) object);
					else if (object instanceof BigDecimal)
						ps.setBigDecimal(i + 1, (BigDecimal) object);
					else
						ps.setObject(i + 1, object);
				} else {
					switch (types[i]) {
					case Types.CHAR:
					case Types.VARCHAR:
					case Types.LONGVARCHAR:
					case Types.LONGNVARCHAR:
					case Types.NVARCHAR:
					case Types.LONGVARBINARY:
					case Types.VARBINARY:
						String str = null;
						if (object instanceof String)
							str = (String) object;
						else if (object instanceof byte[])
							str = new String((byte[]) object);
						else if (object instanceof Long) {
							byte[] obj = new byte[8];
							ByteBuffer.wrap(obj).putLong((Long) object);
							str = new String((byte[]) obj);
						} else
							str = object.toString();
						ps.setString(i + 1, str);
						break;
					case Types.INTEGER:
					case Types.TINYINT:
					case Types.SMALLINT:
					case Types.BIGINT:
						if (object instanceof Short)
							ps.setShort(i + 1, (short) object);
						else if (object instanceof Long)
							ps.setLong(i + 1, (long) object);
						else if (object instanceof Integer)
							ps.setInt(i + 1, (int) object);
						else
							throw new IllegalArgumentException("ven");
						break;
					case Types.BIT:
						if (object instanceof Boolean)
							ps.setBoolean(i + 1, (Boolean) object);
						else
							throw new IllegalArgumentException("ven");
						break;
					case Types.DATE:
						if (object instanceof Date)
							ps.setDate(i + 1, new java.sql.Date(((Date) object).getTime()));
						else
							throw new IllegalArgumentException("ven");
						break;
					case Types.TIMESTAMP:
						if (object instanceof Timestamp)
							ps.setTimestamp(i + 1, (Timestamp) object);
						else if (object instanceof oracle.sql.TIMESTAMP)
							ps.setTimestamp(i + 1, ((oracle.sql.TIMESTAMP) object).timestampValue());
						else
							throw new IllegalArgumentException("ven");
						break;
					case Types.FLOAT:
					case Types.NUMERIC:
					case Types.DOUBLE:
					case Types.REAL:
						if (object instanceof Number)
							ps.setDouble(i + 1, ((Number) object).doubleValue());
						else
							throw new IllegalArgumentException("ven");
						break;
					case Types.TIME:
						if (object instanceof Time)
							ps.setTime(i + 1, (Time) object);
						else
							throw new IllegalArgumentException("ven");
						break;
					case Types.BINARY:
						if (object instanceof byte[])
							ps.setBytes(i + 1, (byte[]) object);
						else
							throw new IllegalArgumentException("ven");
						break;
					case Types.STRUCT:
						if (object instanceof Struct) {
							Struct struct = (Struct) object;
							if (OracleSpatialAdapter.SPATIAL_SQL_TYPE.equals(struct.getSQLTypeName()))
								ps.setString(i + 1, OracleSpatialAdapter.struct2string(struct));
							else
								throw new IllegalArgumentException("ven");
						} else
							throw new IllegalArgumentException("ven");
						break;
					default:
						throw new IllegalArgumentException(
								"ir em https://android.googlesource.com/platform/libcore2/+/refs/heads/master/luni/src/main/java/java/sql/Types.java e ver procurar: "
										+ types[i] + "\t" + object.getClass());
					}
				}
			}
		}
	}

	private static String adjustStatement(String update, Server type) {
		switch (type) {
		case PostgreSQL:
			update = update.replace("`", "").replace("b'1'", "TRUE").replace("b'0'", "FALSE");
			if (update.contains("ON DUPLICATE KEY UPDATE")) {
//				"ON CONFLICT (name) DO UPDATE SET";
				// TODO
				throw new IllegalArgumentException("Para PostgreSQL, UPSERT é diferente");
			}
			break;
		case SQLserver:
		case SASIOM:
			update = update.replace("`", "");
			break;
		case SQLite:
			update = update.replace("`", "").replace("b'1'", "1").replace("b'0'", "0");
			if (update.contains("ON DUPLICATE KEY UPDATE")) {
				// quando lançarem o SQLite 3.26 para Java, usar UPSERT
				update = "INSERT OR REPLACE INTO "
						+ update.substring(update.indexOf('O') + 2, update.lastIndexOf(')') + 1);
				// se houver algum valor que não seja chave e que não será TODO
				// alterado, usar o código abaixo (não há casos assim ainda no
				// Subespaco, pois RARAMENTE usamos o NULL para os campos (ou
				// seja, o INSERT traz tudo que precisa), logo ignorar
				// INSERT OR REPLACE INTO Employee (id, name, role) VALUES
				// (1,'Susan Bar',COALESCE((SELECT role FROM Employee WHERE id =
				// 1), 'Benchwarmer'));
			}
			if (update.startsWith("TRUNCATE")) {
				update = update.substring(8).trim();
				if (update.startsWith("TABLE"))
					update = update.substring(5).trim();
				update = "DELETE FROM " + update;
			}
			break;
		default:
			break;
		}
		return update;
	}

	private static final Pattern SET_REMOVE = Pattern.compile("SET|REMOVE");

	protected void updateNoSQL(String tableName, String keyName, Object keyValue,
			Map<String, String> expressionAttributeNames, Map<String, Object> expressionAttributeValues,
			String updateExpression, boolean upsert) {
		for (Entry<String, String> e : expressionAttributeNames.entrySet())
			updateExpression = updateExpression.replaceAll(e.getKey(), e.getValue());

		if (expressionAttributeValues != null)
			for (Entry<String, Object> e : expressionAttributeValues.entrySet())
				updateExpression = updateExpression.replaceAll(e.getKey(), prepareObject(e.getValue(), this.getType()));

		Matcher m = SET_REMOVE.matcher(updateExpression);
		LinkedHashMap<String, String> field2value = new LinkedHashMap<>();
		int s = -1, status = -1;
		while (m.find()) {
			if (s != -1) {
				String[] ss = updateExpression.substring(s, m.start()).split(",");
				for (int i = 0; i < ss.length; i++) {
					if (status == 0) {
						// set
						String[] t = ss[i].split(" = ");
						field2value.put(t[0], t[1]);
					} else {
						// remove
						field2value.put(ss[i], null);
					}
				}
			}
			s = m.end();
			status = "SET".equals(m.group()) ? 0 : 1;
		}

		// separa por vírgulas cada um dos campos a serem editados, exceto quando as
		// vírgulas estão entre "'"
		String[] ss = updateExpression.substring(s).split(",(?=(?:(?:[^']*'){2})*[^']*$)");
		for (int i = 0; i < ss.length; i++) {
			if (status == 0) { // set
				// separa por igual o nome do campo do seu valor, exceto quando o sinal igual
				// está entre "'"
				String[] t = ss[i].split("=(?=(?:(?:[^']*'){2})*[^']*$)");
				field2value.put(t[0].trim(), t[1].trim());
			} else // remove
				field2value.put(ss[i], null);
		}

		// -------------------------------------------------

		String u = null;
		if (upsert) {
			String s1 = "INSERT INTO " + tableName + " (" + keyName;
			String s2 = " VALUES (" + prepareObject(keyValue, this.getType());
			String s3 = "";

			for (Entry<String, String> e : field2value.entrySet()) {
				String v = e.getValue();
				if (v != null) {
					s1 += "," + e.getKey();
					s2 += "," + e.getValue();
				}
				s3 += "," + e.getKey() + "=" + (v != null ? v : "NULL");
			}
			u = s1 + ")" + s2 + ") ON DUPLICATE KEY UPDATE " + s3.substring(1);
		} else {
			u = "UPDATE `" + tableName + "` SET ";

			String s3 = "";
			for (Entry<String, String> e : field2value.entrySet()) {
				String v = e.getValue();
				s3 += "," + e.getKey() + "=" + (v != null ? v : "NULL");
			}

			u += s3.substring(1) + " WHERE `" + keyName + "`=" + prepareObject(keyValue, this.getType());
		}
		update(u);
	}

	/**
	 * Função que destrói as tabelas de uma base de dados
	 * 
	 * @param tables nomes das tabelas (caso o vetor estava vazio, todas as tabelas
	 *               da base de dados)
	 */
	public void drop(String... tables) {
		clear(true, tables);
	}

	/**
	 * Função que esvazia as tabelas de uma base de dados
	 * 
	 * @param tables nomes das tabelas (caso o vetor estava vazio, todas as tabelas
	 *               da base de dados)
	 */
	public void empty(String... tables) {
		clear(false, tables);
	}

	private void clear(boolean drop, String... tables) {
		List<String> ts = null;

		if (tables.length == 0)
			ts = this.getTables(true);
		else
			ts = Arrays.asList(tables);

		if (this.type == Server.SQLite || !drop) {
			for (String t : ts)
				this.update((drop ? "DROP" : "TRUNCATE") + " TABLE " + t);
		} else if (ts.size() > 0)
			this.update("DROP TABLE `" + StringUtils.addSeparator(ts, "`, `") + "`");
	}

	// ============================ AUXILIARES ============================

	/**
	 * Função que isola uma dada coluna de uma lista de objetos
	 * 
	 * @param in     lista de vetores de objetos
	 * @param column posição no vetor de objeto onde está o elemento a ser isolado
	 * @return lista de vetores que estão numa dada posição
	 */
	public static ArrayList<Object> getColumn(ArrayList<Object[]> in, int column) {
		ArrayList<Object> out = new ArrayList<>(in.size());
		for (Object[] objs : in)
			out.add(objs[column]);
		return out;
	}

	/**
	 * Função que retorna o bloco 'SET' do statement SQL para a query onde os
	 * valores de uma(s) entrada(s) da tabela são estabelecidos
	 * 
	 * @param fields vetor com os campos
	 * @param values vetor com os valores
	 * @return sequência de caracteres na forma '`[field1]`=[value1], ... ,
	 *         `[fieldN]`=[valueN]'
	 */
	public static String getSet(String[] fields, Object[] values, Server type) {
		if (fields.length != values.length)
			throw new IllegalArgumentException("O número de campos deve ser igual ao de valores.");
		String out = "";
		for (int i = 0; i < fields.length; i++)
			out += String.format("`%s`=%s, ", fields[i], prepareObject(values[i], type));
		return out.substring(0, out.length() - 2);
	}

	/**
	 * Função que retorna o bloco 'SET' do statement SQL para a query onde os
	 * valores de uma(s) entrada(s) da tabela são estabelecidos. Somente os valores
	 * que diferem dos originais entraram na query.
	 * 
	 * @param fields    vetor com os campos
	 * @param newValues vetor com os novos valores
	 * @param oldValues vetor com os valores antigos
	 * @return sequência de caracteres na forma '`[field1]`=[value1], ... ,
	 *         `[fieldN]`=[valueN]'
	 */
	public static String getSet(String[] fields, Object[] newValues, Object[] oldValues, Server type) {
		if (fields.length != newValues.length)
			throw new IllegalArgumentException("O número de campos deve ser igual ao de valores.");

		// ver quais os valores foram alterados
		List<String> changedFields = new LinkedList<>();
		List<Object> changedValues = new LinkedList<>();
		for (int i = 0; i < oldValues.length; i++) {
			if (!oldValues[i].equals(newValues[i])) {
				changedFields.add(fields[i]);
				changedValues.add(newValues[i]);
			}
		}
		return getSet(changedFields.toArray(new String[changedValues.size()]),
				changedValues.toArray(new Object[changedValues.size()]), type);
	}

	public static String[] prepareObjects(Object[] objs, Server type, int[] types) {
		String[] out = new String[types.length];
		for (int i = 0; i < types.length; i++)
			out[i] = prepareObject(objs[i], type, types[i]);
		return out;
	}

	protected static String[] prepareObjects(Object[] objs, Server type) {
		String[] out = new String[objs.length];
		for (int i = 0; i < objs.length; i++)
			out[i] = prepareObject(objs[i], type);
		return out;
	}

	protected static String prepareObject(Object obj, Server type, int keyType) {
		if (obj == null)
			return "NULL";
		String out = null;
		switch (keyType) {
		case Types.VARCHAR:
			if (obj instanceof byte[]) {
				out = "x'" + StringUtils.toHex((byte[]) obj) + "'";
				break;
			}
		case Types.CHAR:
		case Types.NCHAR:
		case Types.NVARCHAR:
		case Types.LONGVARCHAR:
		case Types.LONGNVARCHAR:
			if (type == Server.SQLite)
				out = "'" + obj.toString().replace("'", "''") + "'";
			else
				out = "'" + obj.toString().replace("\\", "\\\\").replace("'", "\\'") + "'";
			break;
		case Types.TIMESTAMP:
			if (obj instanceof java.sql.Timestamp)
				out = String.format("'%1$tF %1$tT'", obj);
			else if (obj instanceof oracle.sql.TIMESTAMP)
				out = String.format("'%s'", obj);
			else
				out = String.format("'%1$tF %1$tT'", obj);
			break;
		case Types.BOOLEAN:
		case Types.BIT:
			if (obj instanceof Boolean) {
				if (type == Server.PostgreSQL)
					out = obj.toString().toUpperCase();
				else {
					out = String.valueOf((boolean) obj ? 1 : 0);
					if (type != Server.SQLite)
						out = "b'" + out + "'";
				}
			}
			break;
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			out = "x'" + StringUtils.toHex((byte[]) obj) + "'";
			break;
		case Types.TIME:
			out = "'" + obj.toString() + "'";
			break;
		case Types.STRUCT:
			if (obj instanceof Struct) {
				Struct struct = (Struct) obj;
				try {
					if (OracleSpatialAdapter.SPATIAL_SQL_TYPE.equals(struct.getSQLTypeName()))
						out = "'" + OracleSpatialAdapter.struct2string(struct) + "'";
				} catch (SQLException e) {
					e.printStackTrace();
				}
			} else
				throw new IllegalArgumentException("ven");
			break;
		default:
			out = obj.toString();
			break;
		}
		return out;
	}

	protected static String prepareObject(Object value, Server type) {
		if (value == null)
			return "NULL";
		String out = null;

		// transformação inicial dos collections
		if (value instanceof Map)
			value = MapUtils.toString((Map<?, ?>) value);
		else if (value instanceof Set)
			value = ListUtils.toString((Set<?>) value);
		else if (value instanceof List)
			value = ListUtils.toString((List<?>) value);

		if (value instanceof String || value instanceof Character) {
			if (type == Server.SQLite)
				out = "'" + value.toString().replace("'", "''") + "'";
			else
				out = "'" + value.toString().replace("\\", "\\\\").replace("'", "\\'") + "'";
		} else if (value instanceof Date)
			out = String.format("'%1$tF %1$tT'", value);
		else if (value instanceof Boolean) {
			if (type == Server.PostgreSQL)
				out = value.toString().toUpperCase();
			else {
				out = String.valueOf((boolean) value ? 1 : 0);
				if (type != Server.SQLite)
					out = "b'" + out + "'";
			}
		} else if (value instanceof Struct) {
			Struct struct = (Struct) value;
			try {
				if (OracleSpatialAdapter.SPATIAL_SQL_TYPE.equals(struct.getSQLTypeName()))
					out = "'" + OracleSpatialAdapter.struct2string(struct) + "'";
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} else
			out = value.toString();
		return out;
	}

	protected static Object defaultObject(int keyType) {
		Object out = null;
		switch (keyType) {
		case Types.CHAR:
		case Types.NCHAR:
		case Types.VARCHAR:
		case Types.NVARCHAR:
		case Types.LONGVARCHAR:
		case Types.LONGNVARCHAR:
			out = "";
			break;
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
		case Types.BIGINT:
			out = 0;
			break;
		case Types.FLOAT:
			out = 0f;
			break;
		case Types.DOUBLE:
			out = 0.;
			break;
		default:// TODO BYTE[]
			System.err.println("Não se definiu um objeto padrão para o tipo SQL " + keyType);
			break;
		}
		return out;
	}

	public static Object[] defaultObjects(int[] types) {
		Object[] out = new Object[types.length];
		for (int i = 0; i < types.length; i++)
			out[i] = defaultObject(types[i]);
		return out;
	}

	public static String getNames(int keyType) {
		switch (keyType) {
		case Types.CHAR:
		case Types.NCHAR:
		case Types.VARCHAR:
		case Types.NVARCHAR:
		case Types.LONGVARCHAR:
		case Types.LONGNVARCHAR:
			return "TEXT";
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
		case Types.BIGINT:
			return "INT";
		case Types.FLOAT:
		case Types.DOUBLE:
			return "REAL";
		case Types.DATE:
		case Types.TIME:
		case Types.TIMESTAMP:
			return "DATE";
		default:
			return "BLOB";
		}
	}

	/**
	 * Função que retorna o bloco 'WHERE' do statement SQL para a query onde uma
	 * lista de campos e seus respectivos valores é procurado.
	 * 
	 * @param fields vetor com os campos a serem procurados
	 * @param values vetor com os valores a serem procurados
	 * @return sequência de caracteres na forma ' WHERE ([field1]=[value1] AND ...
	 *         AND [fieldN]=[valueN])'
	 */
	public static String getWhere(String[] fields, Object[] values, Server type) {
		if (fields.length != values.length)
			throw new IllegalArgumentException("O número de campos deve ser igual ao de valores.");
		String out = getEqualClause(fields[0], values[0], type);
		for (int i = 1; i < values.length; i++)
			out += (" AND " + getEqualClause(fields[i], values[i], type));
		return " WHERE " + out;
	}

	/**
	 * Função que retorna a operação SQL que equivale a igualdade
	 * 
	 * @param field nome do campo
	 * @param value valor do campo
	 * @return sequência de caracteres para ser usada na clause 'WHERE'
	 */
	public static String getEqualClause(String field, Object value, Server type) {
		return String.format(type == Server.SASIOM || type == Server.Oracle ? "%s=%s" : "`%s`=%s", field,
				prepareObject(value, type));
	}

	/**
	 * Função que retorna o bloco 'WHERE' do statement SQL para a query onde um
	 * conjunto de valores de um dado campo é procurado. O resultado é a associação
	 * inclusiva de todos os valores buscados no campo.
	 * 
	 * @param field campo no qual o valor está sendo buscado
	 * @param array vetor com os valores a serem procurados
	 * @param type  tipo do conector SQL
	 * @return sequência de caracteres na forma ' WHERE ([field]=[value1] OR ... OR
	 *         [field]=[valueN])'
	 */
	public static String getWhere(String field, Object[] array, Server type) {
		return getWhere(field, array, type, true);
	}

	/**
	 * Função que retorna o bloco 'WHERE' do statement SQL para a query onde um
	 * conjunto de valores de um dado campo é procurado. O resultado é a associação
	 * inclusiva de todos os valores buscados no campo.
	 * 
	 * @param field campo no qual o valor está sendo buscado
	 * @param array vetor com os valores a serem procurados
	 * @param type  tipo do conector SQL
	 * @param where <code>true</code> se já for incluído o termo 'WHERE' na
	 *              expressão, <code>false</code> senão
	 * @return sequência de caracteres na forma '([field]=[value1] OR ... OR
	 *         [field]=[valueN])', podendo ou não ser precedido por um 'WHERE' a
	 *         depender do argumento da função, uma sequência de caracteres vazia
	 *         para um conjunto vazio, <code>null</code> para um conjunto nulo
	 */
	public static String getWhere(String field, Object[] array, Server type, boolean where) {
		return getWhere(field, new TreeSet<>(Arrays.asList(array)), type, where);
	}

	/**
	 * Função que retorna o bloco 'WHERE' do statement SQL para a query onde um
	 * conjunto de valores de um dado campo é procurado. O resultado é a associação
	 * inclusiva de todos os valores buscados no campo.
	 * 
	 * @param field campo no qual o valor está sendo buscado
	 * @param set   valores a serem procurados
	 * @param type  tipo do conector SQL
	 * @return sequência de caracteres na forma ' WHERE ([field]=[value1] OR ... OR
	 *         [field]=[valueN])'
	 */
	public static String getWhere(String field, Collection<?> set, Server type) {
		return getWhere(field, set, type, true);
	}

	/**
	 * Número de elementos a partir do qual utiliza-se a função IN ao invés de
	 * múltiplos OR's
	 */
	private static final int IN_USE = 4;

	/**
	 * Função que retorna o bloco 'WHERE' do statement SQL para a query onde um
	 * conjunto de valores de um dado campo é procurado. O resultado é a associação
	 * inclusiva de todos os valores buscados no campo.
	 * 
	 * @param field campo no qual o valor está sendo buscado
	 * @param set   valores a serem procurados
	 * @param type  tipo do conector SQL
	 * @param where <code>true</code> se já for incluído o termo 'WHERE' na
	 *              expressão, <code>false</code> senão
	 * @return sequência de caracteres na forma '([field]=[value1] OR ... OR
	 *         [field]=[valueN])', podendo ou não ser precedido por um 'WHERE' a
	 *         depender do argumento da função, uma sequência de caracteres vazia
	 *         para um conjunto vazio, <code>null</code> para um conjunto nulo
	 */
	public static String getWhere(String field, Collection<?> set, Server type, boolean where) {
		if (set == null)
			return null;
		if (set.size() == 0)
			return where ? " WHERE 0" : "0";

		StringBuilder out = new StringBuilder(where ? " WHERE (" : "(");
		if (set.size() < IN_USE) {
			for (Object obj : set) {
				out.append(getEqualClause(field, obj, type));
				out.append(" OR ");
			}
			out.setLength(out.length() - 4);
			out.append(")");
		} else {
			if (type != Server.SASIOM)
				out.append("`");
			out.append(field);
			if (type != Server.SASIOM)
				out.append("`");
			out.append(" IN (");
			for (Object obj : set) {
				out.append(prepareObject(obj, type));
				out.append(",");
			}
			out.setLength(out.length() - 1);
			out.append("))");
		}

		return out.toString();
	}

	/**
	 * Função que retorna o bloco 'WHERE' do statement SQL para a query onde um
	 * conjunto de valores de um dado campo é procurado. O resultado é a associação
	 * inclusiva de todos os valores buscados no campo. Os objeto são considerados
	 * como uma sequência de caracteres (ou seja, na hora de entrar na expressão
	 * SQL, ficarão entre aspas)
	 * 
	 * @param field    campo no qual o valor está sendo buscado
	 * @param set      valores a serem procurados
	 * @param type
	 * @param where    <code>true</code> se já for incluído o termo 'WHERE' na
	 *                 expressão, <code>false</code> senão
	 * @param toString para inteiros maiores que 0, supõe-se que os objetos sejam
	 *                 inteiros, cujo tamanho será igual ao do número passado como
	 *                 argumento (adicionando-se zeros se necessário). Caso seja
	 *                 menor ou igual a 0, aplica-se a função {@link #toString()} em
	 *                 cada objeto
	 * @return
	 */
	public static String getWhere(String field, Collection<?> set, Server type, boolean where, int toString) {
		if (set == null)
			return null;
		if (set.size() == 0)
			return where ? " WHERE 0" : "0";

		String format = null;
		if (toString > 0)
			format = "%0" + toString + "d";

		StringBuilder out = new StringBuilder(where ? " WHERE (" : "(");
		if (set.size() < IN_USE) {
			for (Object obj : set) {
				out.append(getEqualClause(field, format == null ? obj.toString() : String.format(format, obj), type));
				out.append(" OR ");
			}
			out.setLength(out.length() - 4);
			out.append(")");
		} else {
			if (type != Server.SASIOM)
				out.append("`");
			out.append(field);
			if (type != Server.SASIOM)
				out.append("`");
			out.append(" IN (");
			for (Object obj : set) {
				out.append(prepareObject(format == null ? obj.toString() : String.format(format, obj), type));
				out.append(",");
			}
			out.setLength(out.length() - 1);
			out.append("))");
		}

		return out.toString();
	}

	// WHEN

	// contínuo

	// data e hora em campos diferentes

	/**
	 * 
	 * @param fieldD
	 * @param fieldH
	 * @param cs     períodos de tempo, definidos por uma matriz de duas colunas,
	 *               onde os elementos da primeira coluna indicam o começo de um
	 *               subintervalo e os da segunda indicam seu final
	 * @return
	 */
	public static String getWhen(String fieldD, String fieldH, Calendar[][] cs) {
		String out = null;
		if (cs.length == 1)
			out = getWhen(cs[0][0], cs[0][1], fieldD, fieldH);
		else {
			out = "";
			for (Calendar[] c : cs)
				out += " OR " + getWhen(c[0], c[1], fieldD, fieldH);
			out = out.substring(4);
		}
		return out;
	}

	/**
	 * Função que retorna a expressão SQL que delimita um campo de data e outro de
	 * hora para que esteja dentro de um período
	 * 
	 * @param begin  limite inferior do período
	 * @param end    limite superior do período
	 * @param fieldD nome do campo para a data
	 * @param fieldH nome do campo para a hora
	 * @return sequência de caracteres com o comando SQL
	 */
	public static String getWhen(Calendar begin, Calendar end, String fieldD, String fieldH) {
		if (TimeUtils.isSameDay(begin, end))
			return String.format("%3$s='%1$td%1$tb%1$tY'd AND %4$s GE '%1$tT't AND %4$s < '%2$tT't", begin, end, fieldD,
					fieldH);
		else
			return String.format("(%s AND %s)", getWhen(fieldD, fieldH, begin, true),
					getWhen(fieldD, fieldH, end, false));
	}

	private static String getWhen(String fieldD, String fieldH, Calendar c, boolean gt) {
		if (c.get(Calendar.HOUR_OF_DAY) == 0 && c.get(Calendar.MINUTE) == 0)
			return String.format(Locale.US, "(%s%s'%3$td%3$tb%3$tY'd)", fieldD, gt ? " GE " : "<", c);
		else if (!gt && c.get(Calendar.HOUR_OF_DAY) == 23 && c.get(Calendar.MINUTE) == 59)
			return String.format(Locale.US, "(%1$s LE '%2$td%2$tb%2$tY'd)", fieldD, c);
		else
			return String.format(Locale.US,
					"(%2$s%4$c'%1$td%1$tb%1$tY'd OR (%2$s='%1$td%1$tb%1$tY'd AND %3$s%5$s'%1$tT't))", c, fieldD, fieldH,
					gt ? '>' : '<', gt ? " GE " : "<");
	}

	/**
	 * Função que retorna a expressão SQL que delimita um campo de data e outro de
	 * hora para que a interseção entre eles não seja nula
	 * 
	 * @param begin   começo do intervalo
	 * @param end     fim do intervalo
	 * @param fieldDb nome do campo para a data do começo do intervalo
	 * @param fieldHb nome do campo para a hora do começo do intervalo
	 * @param fieldDe nome do campo para a data do fim do intervalo
	 * @param fieldHe nome do campo para a hora do fim do intervalo
	 * @return
	 */
	public static String getWhen(Calendar begin, Calendar end, String fieldDb, String fieldHb, String fieldDe,
			String fieldHe) {
		return String.format("(%s AND %s)", getWhen(fieldDb, fieldHb, end, false),
				getWhen(fieldDe, fieldHe, begin, true));
	}

	public static String getWhen(Calendar[][] cs, String fieldY, String fieldM) {
		String out = null;
		if (cs.length == 1)
			out = getWhen(fieldY, fieldM, cs[0][0], cs[0][1]);
		else {
			out = "";
			for (Calendar[] c : cs)
				out += " OR " + getWhen(fieldY, fieldM, c[0], c[1]);
			out = out.substring(4);
		}
		return out;
	}

	public static String getWhen(String fieldY, String fieldM, Calendar begin, Calendar end) {
		if (TimeUtils.isSameYear(begin, end)) {
			if (begin.get(Calendar.MONTH) == end.get(Calendar.MONTH))
				return String.format("%2$s=%1$tY AND %3$s = %1$tm", begin, fieldY, fieldM);
			else
				return String.format("%3$s=%1$tY AND %4$s >= %1$tm AND %4$s <= %2$tm", begin, end, fieldY, fieldM);
		} else
			return String.format("(%s AND %s)", getWhen(begin, fieldY, fieldM, true),
					getWhen(end, fieldY, fieldM, false));
	}

	private static String getWhen(Calendar c, String fieldY, String fieldM, boolean gt) {
		if (c.get(Calendar.MONTH) == Calendar.JANUARY)
			return String.format(Locale.US, "(%s%s%tY)", fieldY, gt ? ">=" : "<", c);
		else if (!gt && c.get(Calendar.MONTH) == Calendar.DECEMBER)
			return String.format(Locale.US, "(%s <= %tY)", fieldY, c);
		else
			return String.format(Locale.US, "(%2$s%4$c%1$tY OR (%2$s=%1$tY AND %3$s%5$s%1$tm))", c, fieldY, fieldM,
					gt ? '>' : '<', gt ? ">=" : "<=");
	}

	// data e hora no mesmo campo

	/**
	 * Função que retorna a expressão SQL que delimita um campo de data e hora para
	 * que esteja dentro de um ou mais períodos
	 * 
	 * @param field nome do campo (único para data e hora, ver
	 *              {@link #getWhen(Calendar, Calendar, String, String)})
	 * @param cs    matriz de duas colunas, onde na primeira coluna estão os limites
	 *              inferiores dos períodos e na segunda coluna estão os limites
	 *              superiores
	 * @return sequência de caracteres com o comando SQL
	 */
	public static String getWhen(String field, Calendar[][] cs) {
		return getWhen(field, cs, true, false);
	}

	/**
	 * 
	 * @param field
	 * @param cs    períodos de tempo, definidos por uma matriz de duas colunas,
	 *              onde os elementos da primeira coluna indicam o começo de um
	 *              subintervalo e os da segunda indicam seu final
	 * @param ge
	 * @param le
	 * @return
	 */
	public static String getWhen(String field, Calendar[][] cs, boolean ge, boolean le) {
		if (cs == null)
			return "1";
		else {
			if (cs.length == 1)
				return String.format("`%1$s`>%4$s'%2$tF %2$tT' AND `%1$s`<%5$s'%3$tF %3$tT'", field, cs[0][0], cs[0][1],
						ge ? "=" : "", le ? "=" : "");
			else {
				StringBuilder out = new StringBuilder();
				for (Calendar[] c : cs)
					out.append(String.format("(`%1$s`>%4$s'%2$tF %2$tT' AND `%1$s`<%5$s'%3$tF %3$tT') OR ", field, c[0],
							c[1], ge ? "=" : "", le ? "=" : ""));
				return out.substring(0, out.length() - 4);
			}
		}
	}

	// discreto

	// data e hora em campos diferentes

	/**
	 * Função que retorna a expressão SQL que delimita um campo de data e outro de
	 * hora para que seja igual a certos instantes
	 * 
	 * @param fieldD nome do campo para a data
	 * @param fieldH nome do campo para a hora
	 * @param cs     vetor com os instantes de tempo procurados
	 * @return sequência de caracteres com o comando SQL
	 */
	public static String getWhen(String fieldD, String fieldH, Calendar... cs) {
		String where = "";
		for (Calendar c : cs)
			where += String.format(Locale.US, "(" + fieldD + "='%1$td%1$tb%1$tY'd AND " + fieldH + "='%1$tT't) OR ", c);
		return where.substring(0, where.length() - 4);
	}

	// data e hora no mesmo campo

	/**
	 * Função que retorna a expressão SQL que delimita um campo de data e hora para
	 * que seja igual a certos instantes
	 * 
	 * @param field nome do campo (único para data e hora, ver
	 *              {@link #getWhen(String, String, Calendar...)})
	 * @param cs    vetor com os instantes de tempo procurados
	 * @return sequência de caracteres com o comando SQL
	 */
	public static String getWhen(String field, Calendar... cs) {
		String out = "";
		for (Calendar c : cs)
			out += String.format("`%s`='%2$tF %2$tT' OR ", field, c);
		out = out.substring(0, out.length() - 4);
		return out;
	}

	/**
	 * Função que carrega os dados de uma tabela no formato txt com a largura das
	 * colunas definida na segunda linha, com o número de traços
	 * 
	 * @param file    arquivo texto
	 * @param columns colunas selecionadas
	 * @return lista de objetos
	 */
	public static List<Object[]> load(File file, String... columns) {
		List<Object[]> out = new LinkedList<>();
		try {
			RandomAccessFile raf = new RandomAccessFile(file, "r");

			String s = raf.readLine().substring(3);
			String[] header = s.split("\\s+");

			int[] pos = new int[columns.length];
			for (int i = 0; i < columns.length; i++)
				for (int j = 0; j < header.length; j++)
					if (header[j].equals(columns[i]))
						pos[i] = j;

			s = raf.readLine();
			String[] ss = s.split(" ");
			int[] width = new int[ss.length + 1];
			for (int i = 0; i < ss.length; i++)
				width[i + 1] = ss[i].length() + width[i] + 1;

			while ((s = raf.readLine()) != null) {
				if ("".equals(s))
					break;
				Object[] objs = new Object[pos.length];
				for (int i = 0; i < pos.length; i++)
					objs[i] = s.substring(width[pos[i]], width[pos[i] + 1] - 1).trim();

				for (int i = 0; i < objs.length; i++) {
					String o = (String) objs[i];
					if ("NULL".equals(o) || "".equals(o))
						objs[i] = null;
					else {
						try {
							if (o.indexOf('.') >= 0)
								objs[i] = Float.parseFloat(o);
							else
								objs[i] = Integer.parseInt(o);
						} catch (NumberFormatException e) {
						}
					}
				}
				out.add(objs);
			}
			raf.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return out;
	}

	private static final char FIRST_ALIAS = 'p';

	/**
	 * Função que retorna a sequência de caracteres do código SQL para busca da mais
	 * alta entrada de uma tabela onde os campos estão hierarquizados
	 * 
	 * @param table  nome da tabela
	 * @param fields campos cujos máximos será buscado, na ordem hierárquica do mais
	 *               significativo para o menos
	 * @return query que permite obter a entrada de mais alto valor
	 */
	public static String getLimitMultiFields(String table, String... fields) {
		String out = String.format("(SELECT MAX(`%s`) max%1$s FROM `%2$s`)", fields[0], table);

		for (int i = 1; i < fields.length; i++) {
			String field = fields[i];

			String s1 = String.format("(SELECT MAX(`%1$s`) max%1$s", field);
			for (int j = i - 1; j >= 0; j--)
				s1 += String.format(", %s`%s`", i - j > 1 ? "`" + table + "`." : "", fields[j]);

			String s2 = "";
			for (int j = 0; j < i; j++)
				s2 += String.format(" AND `%s`.`%s`=%c.%s", table, fields[j], (char) (FIRST_ALIAS + i),
						j == i - 1 ? "max" + fields[j] : ("`" + fields[j] + "`"));

			out = s1 + " FROM " + out + String.format(" %c INNER JOIN `%s` ON ", (char) (FIRST_ALIAS + i), table)
					+ s2.substring(5) + ")";
		}
		return out;
	}
}