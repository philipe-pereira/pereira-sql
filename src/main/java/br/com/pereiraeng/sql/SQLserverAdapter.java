package br.com.pereiraeng.sql;

/**
 * Classe do objeto conector à uma base de dados e que também contendo um objeto
 * de interface com o programa que a utiliza
 * 
 * @author Philipe PEREIRA
 *
 */
public abstract class SQLserverAdapter extends SQLadapter {

	protected final DataFlowCtrl dfc;

	public SQLserverAdapter(SQLconfig config, DataFlowCtrl dfc) {
		super(config);
		this.dfc = dfc;
	}
}
