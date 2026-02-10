package br.com.pereiraeng.sql;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import br.com.pereiraeng.core.CryptoUtils;
import br.com.pereiraeng.core.EditFields;
import br.com.pereiraeng.core.Password;
import br.com.pereiraeng.core.StringUtils;

/**
 * Classe do objeto que comporta as informações necessárias para configurar a
 * conexão a uma base de dados SQL
 * 
 * @author Philipe PEREIRA
 *
 */
public class SQLconfig {

	public static final String[] HEADER = { "Tipo", "Server", "Porta", "Login", "Senha", "Base de dados" };

	public static final String LOCAL_HOST = "127.0.0.1";

	public static final int PORT = 3306;

	public static final String USER = "root";

	public static final String PASS = "root";

	public static final Object[] NULL = { Server.MySQL, LOCAL_HOST, PORT, USER, new Password(PASS), "" };

	private static String dbKey;

	private final Server type;

	private final String server, port;

	private final String db;

	private String login;

	private String password;

	/**
	 * Construtor do objeto com os parâmetros para conexão a uma base de dados
	 * 
	 * @param type     tipo de base de dados
	 * @param server   endereço do servidor
	 * @param port     porta
	 * @param login    usuário
	 * @param password senha
	 * @param db       nome da base de dados
	 */
	public SQLconfig(Server type, String server, String port, String login, String password, String db) {
		this.type = type;

		switch (this.type) {
		// arquivos: não há senha ou porta...
		case UCanAccess:
		case SQLite:
			this.server = server;
			this.port = "";
			this.login = "";
			this.password = "";
			break;
		default: // servidores
			this.server = server;
			this.port = port;
			this.login = login;
			this.password = password;
			break;
		}

		this.db = db;
	}

	/**
	 * Construtor do objeto com os parâmetros para conexão a uma base de dados
	 * {@link Server#SQLite sem servidor}
	 * 
	 * @param folder diretório com o arquivo
	 * @param file   nome do arquivo (sem a terminação .sql)
	 */
	public SQLconfig(File folder, String file) {
		this(Server.SQLite, folder == null ? ""
				: (folder.exists()
						? (folder.isDirectory() ? (File.separator.equals(folder.getPath()) ? "" : folder.getPath())
								: null /* erro */)
						: null /* erro */),
				null, null, null, file);
	}

	/**
	 * Construtor do objeto com os parâmetros para conexão a uma base de dados
	 * {@link Server#SQLite sem servidor}
	 * 
	 * @param url local do recurso
	 */
	public SQLconfig(URL url) {
		this((File) null, StringUtils.chop(url.getFile(), 3));
	}

	/**
	 * Construtor do objeto com os parâmetros para conexão a uma base de dados
	 * {@link Server#UCanAccess de um arquivo MS-Access}
	 * 
	 * @param accessFile arquivo MDB
	 */
	public SQLconfig(File accessFile) {
		this(Server.UCanAccess, "", "", "", "", accessFile.getAbsolutePath());
	}

	// ---------------------------------------------

	/**
	 * Construtor do objeto com os parâmetros para conexão a uma base de dados do
	 * {@link Server#MySQL servidor MySQL} local (127.0.0.1:3306)
	 * 
	 * @param db nome da base de dados
	 */
	public SQLconfig(String db) {
		this(Server.MySQL, LOCAL_HOST, String.valueOf(PORT), USER, PASS, db);
	}

	/**
	 * Construtor do objeto com os parâmetros para conexão a uma base de dados do
	 * {@link Server#MySQL servidor MySQL} do UOL
	 * 
	 * @param user     usuário
	 * @param password senha
	 * @param db       nome da base de dados
	 */
	public SQLconfig(String user, String password, String db) {
		this(Server.MySQL, db.replace('_', '-') + ".mysql.uhserver.com", "", user, password, db);
	}

	/**
	 * Construtor do objeto com os parâmetros para conexão a uma base de dados do
	 * {@link Server#PostgreSQL servidor PostgreSQL} local (localhost:5432)
	 * 
	 * @param password senha
	 * @param database nome da base de dados
	 */
	public SQLconfig(String password, String database) {
		this(Server.PostgreSQL, "localhost", "5432", "postgres", password, database);
	}

	// ---------------------------------------------

	public Server getType() {
		return type;
	}

	public String getServer() {
		return server;
	}

	public String getPort() {
		return port;
	}

	public void setLogin(String login) {
		this.login = login;
	}

	public String getLogin() {
		return login;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getPassword() {
		return password;
	}

	public String getDb() {
		return db;
	}

	public boolean hasLogin() {
		return login != null && password != null;
	}

	public Object[] getObjects() {
		return new Object[] { this.type, this.server, this.port, this.login, this.password, this.db };
	}

	// ------------------------------------------------------------

	/**
	 * Função que carrega as configurações personalizadas, vindas de um recurso, da
	 * base de dados a ser carregada
	 * 
	 * @param xmlResource URL do recurso <code>xml</code> que contem as informações
	 *                    de conexão à base de dados
	 * @return objeto de configuração de base de dados
	 */
	public static SQLconfig loadConfig(URL xmlResource) {
		if (xmlResource == null)
			throw new IllegalArgumentException("resource null");
		try {
			return loadConfig(xmlResource.openStream());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static SQLconfig loadConfig(String xmlFilename) {
		return SQLconfig.loadConfig(new File(xmlFilename));
	}

	/**
	 * Função que carrega as configurações personalizadas, vindas de um arquivo, da
	 * base de dados a ser carregada
	 * 
	 * @param xmlfile nome do arquivo <code>xml</code> que contem as informações de
	 *                conexão à base de dados
	 * @return objeto de configuração de base de dados
	 */
	public static SQLconfig loadConfig(File xmlfile) {
		if (xmlfile == null)
			throw new IllegalArgumentException("file null");
		if (!xmlfile.exists())
			throw new IllegalArgumentException(xmlfile + " does not exist");
		try {
			return loadConfig(new FileInputStream(xmlfile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}

	private static SQLconfig loadConfig(InputStream xmlInputStream) {
		Properties props = new Properties();
		try {
			props.loadFromXML(xmlInputStream);
			xmlInputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		String password = props.getProperty("SQL_PASSWD");
		if (password == null) {
			String passwordCrypto = props.getProperty("SQL_PASSWD_CRYPTO");
			if (passwordCrypto != null && dbKey != null) {
				try {
					password = CryptoUtils.decrypt(passwordCrypto, dbKey);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return new SQLconfig(Server.valueOf(props.getProperty("SQL")), props.getProperty("SQL_SERVER"),
				props.getProperty("SQL_PORT"), props.getProperty("SQL_LOGIN"), password, props.getProperty("SQL_DB"));
	}

	/**
	 * Função estática que permite estabelecer uma chave de decriptação para os
	 * acessos às bases de dados
	 * 
	 * @param dbKey chave para leitura das senhas das bases de dados
	 */
	public static void setDbKey(String dbKey) {
		SQLconfig.dbKey = dbKey;
	}

	/**
	 * Função que pede o login e senha para o usuário para inserí-la no objeto de
	 * configuração de base de dados
	 * 
	 * @param editFields objeto de edição do login e do password
	 * @param config     objeto de configuração de base de dados
	 * @param confirm    mesmo se a senha e login tenham sido fornecidos, mostra-se
	 *                   a caixa de diálogo
	 */
	public static void askLogin(EditFields editFields, SQLconfig config, boolean confirm) {
		String login = config.getLogin();
		String password = config.getPassword();
		if (login == null || password == null ? true : confirm) {
			Object[] objects = editFields.editFields("Configurações da base de dados",
					new String[] { HEADER[3], HEADER[4] }, new Object[] { login, new Password(password) });
			if (objects != null) {
				login = (String) objects[0];
				if (login.length() > 0)
					config.setLogin(login);
				password = (String) objects[1];
				if (password.length() > 0)
					config.setPassword(password);
			}
		}
	}
}
