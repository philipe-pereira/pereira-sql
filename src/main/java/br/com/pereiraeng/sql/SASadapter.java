package br.com.pereiraeng.sql;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import com.sas.iom.SAS.ILanguageService;
import com.sas.iom.SAS.IWorkspace;
import com.sas.iom.SAS.IWorkspaceHelper;
import com.sas.iom.SAS.ILanguageServicePackage.CarriageControlSeqHolder;
import com.sas.iom.SAS.ILanguageServicePackage.LineTypeSeqHolder;
import com.sas.iom.SASIOMDefs.GenericError;
import com.sas.iom.SASIOMDefs.StringSeqHolder;
import com.sas.rio.MVAConnection;
import com.sas.services.connection.BridgeServer;
import com.sas.services.connection.ConnectionFactoryConfiguration;
import com.sas.services.connection.ConnectionFactoryException;
import com.sas.services.connection.ConnectionFactoryInterface;
import com.sas.services.connection.ConnectionFactoryManager;
import com.sas.services.connection.ConnectionInterface;
import com.sas.services.connection.ManualConnectionFactoryConfiguration;

/**
 * Classe dos objetos que facilitam a manipulação de bases de dados SQL do SAS
 * 
 * @author Philipe PEREIRA
 *
 */
public class SASadapter extends SQLadapter {

	private Set<String> assigned;

	public static final String DATETIME_FORMAT = "'%td%<tb%<tY:%<tT'dt";

	public static final String DATE_FORMAT = "'%td%<tb%<tY'd";

	@Override
	public boolean connectDB() {
		assigned = new HashSet<>();
		return super.connectDB();
	}

	/**
	 * Construtor do objeto que faz a interface com uma dada base de dados SAS-SQL
	 * 
	 * @param serverDefault   endereço do servidor
	 * @param portDefault     porta
	 * @param loginDefault    usuário
	 * @param passwordDefault senha
	 */
	public SASadapter(String server, String port, String login, String password) {
		super(Server.SASIOM, server, port, login, password, "");
	}

	/**
	 * Construtor do objeto que faz a interface com uma dada base de dados SAS-SQL
	 * 
	 * @param config objeto com as configurações
	 */
	public SASadapter(SQLconfig config) {
		super(Server.SASIOM, config.getServer(), config.getPort(), config.getLogin(), config.getPassword(), "");
	}

	/**
	 * Função que assinala uma biblioteca do SAS
	 * 
	 * @param library  nome da biblioteca
	 * @param location local da biblioteca
	 * @return <code>true</code> caso o assinalamento tenho sido bem sucedido,
	 *         <code>false</code> senão
	 */
	public boolean assign(String library, String location) {
		try {
			if (!assigned.contains(library)) {
				String classID = com.sas.services.connection.Server.CLSID_SAS;

				// Connect to the server and retrieve an IWorkSpace
				com.sas.services.connection.Server server = new BridgeServer(classID, super.server,
						Integer.parseInt(super.port));
				ConnectionFactoryConfiguration cxfConfig = new ManualConnectionFactoryConfiguration(server);
				ConnectionFactoryManager cxfManager = new ConnectionFactoryManager();
				ConnectionFactoryInterface cxf = cxfManager.getFactory(cxfConfig);
				ConnectionInterface cx = cxf.getConnection(super.login, super.password);
				org.omg.CORBA.Object obj = cx.getObject();
				IWorkspace sasWorkspace = IWorkspaceHelper.narrow(obj);

				// Retrieve the ILanguageService. This will allow you to submit
				// any SAS code
				// We will use it to assign a libname
				ILanguageService sasLanguage = sasWorkspace.LanguageService();
				// sasLanguage.Submit("libname appdev 'c:\\data\\tables';");
				String code = "libname " + library;
				if (location != null)
					code += " base \"" + location + "\";";
				sasLanguage.Submit(code);
				// Retrieve the SAS log and print it out
				CarriageControlSeqHolder logCarriageControlHldr = new CarriageControlSeqHolder();
				LineTypeSeqHolder logLineTypeHldr = new LineTypeSeqHolder();
				StringSeqHolder logHldr = new StringSeqHolder();
				sasLanguage.FlushLogLines(Integer.MAX_VALUE, logCarriageControlHldr, logLineTypeHldr, logHldr);
				String[] logLines = logHldr.value;
				for (int i = 0; i < logLines.length; i++)
					System.out.println(logLines[i]);

				super.conn = new MVAConnection(sasWorkspace, new Properties());
				System.out.println("Conexão on-line estabelecida! Novas bibliotecas foram atribuídas.");
				assigned.add(library);
				return true;
			} else
				return true;
		} catch (ConnectionFactoryException e) {
			e.printStackTrace();
		} catch (GenericError e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public void disconnectDB() {
		if (assigned != null)
			assigned.clear();
		super.disconnectDB();
	}

}
