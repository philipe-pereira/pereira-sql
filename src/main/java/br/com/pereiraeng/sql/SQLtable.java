package br.com.pereiraeng.sql;

import java.util.LinkedHashMap;

/**
 * Classe do objeto que representa uma tabela SQL. Tal objeto é uma tabela de
 * dispersão que associa para cada nome do campo o seu tipo
 * 
 * @author Philipe PEREIRA
 *
 */
public class SQLtable extends LinkedHashMap<String, String> {
	private static final long serialVersionUID = 1L;

	private String primary;

	public String getPrimary() {
		return primary;
	}

	public void setPrimary(String primary) {
		this.primary = primary;
	}

	@Override
	public String toString() {
		return super.toString() + " PRIMARY: " + this.primary;
	}
}
