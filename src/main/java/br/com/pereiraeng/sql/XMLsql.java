package br.com.pereiraeng.sql;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.Stack;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import br.com.pereiraeng.xml.XMLadapter;

/**
 * Classe do objeto leitor de arquivos XML com as informações sobre a estrutura
 * de uma ou mais tabelas de uma base de dados SQL
 * 
 * @author Philipe PEREIRA
 *
 */
public class XMLsql extends XMLadapter {

	private String dbName, mainTable, primary;
	private Stack<String> table;
	private boolean pri;

	private Set<String> select, from, where;

	// =============================== GETTER ===============================

	public String getDbName() {
		return dbName;
	}

	public String getMainTable() {
		return mainTable;
	}

	public String getPrimary() {
		return primary;
	}

	public String[] getFields() {
		return getFields(true);
	}

	public String[] getFields(boolean alias) {
		String[] out = new String[select.size()];
		int i = 0;
		for (String s : select)
			out[i++] = s.split(" AS ")[alias ? 1 : 0];
		return out;
	}

	/**
	 * Função que retorna a expressão de uma query que retorna todas as chaves
	 * primárias
	 * 
	 * @return sequência de caracteres que pode ser interpretada como uma 'query' do
	 *         SQL capaz de retornar todas as chaves dessa tabela
	 */
	public String getQueryV() {
		// select
		String out = String.format("SELECT %s FROM %s", primary, mainTable);

		// where
		out += " WHERE ";
		for (String s : where)
			out += s + " AND ";

		out = out.substring(0, out.length() - 5);

		// resultado
		return out;
	}

	/**
	 * Função que retorna a expressão de uma query que retorna todos os dados para
	 * um dado valor da chave primária
	 * 
	 * @param keys valor da chave primária
	 * @return sequência de caracteres que pode ser interpretada como uma 'query' do
	 *         SQL capaz de retornar os valores associados à uma dada chave primária
	 */
	public String getQueryH(Object key) {
		// select
		String out = "SELECT";
		for (String s : select)
			out += " " + s + ",";

		out = out.substring(0, out.length() - 1);

		// from
		String f = mainTable;
		for (String s : from)
			f = String.format("(%s %s)", f, s);

		f = f.substring(1, f.length() - 1);
		out += " FROM " + f;

		// where
		out += " WHERE ";
		if (!pri)
			for (String s : where)
				out += s + " AND ";
		if (key != null)
			out += mainTable + "." + primary + "=" + key;
		else
			out = out.substring(0, out.length() - 5);

		// resultado
		return out;
	}

	/**
	 * Função que retorna a expressão de uma query que retorna todos os dados para
	 * todas as chaves primárias (ou seja, todas as entradas existentes)
	 * 
	 * @return sequência de caracteres que pode ser interpretada como uma 'query' do
	 *         SQL capaz de retornar as entradas da tabela
	 */
	public String getAllQuery() {
		return getQueryH(null);
	}

	// =============================== LEITURA ===============================

	private transient String temp1, temp2;

	@Override
	public void startDocument() throws SAXException {
		this.select = new LinkedHashSet<>();
		this.from = new LinkedHashSet<>();
		this.where = new LinkedHashSet<>();
	}

	@Override
	public void startElement(String qName, Attributes atts) {
		this.temp1 = qName;

		switch (qName) {
		case "sqlDB":
			this.dbName = atts.getValue("name");
			this.mainTable = atts.getValue("main");
			this.primary = atts.getValue("primary");
			this.pri = atts.getValue("pri").equals("1");

			// nível mais baixo...
			this.table = new Stack<String>();
			this.table.push(mainTable);

			break;
		case "filter":
			// campo a ser filtrado
			this.temp2 = atts.getValue("field");
			break;
		case "field":
			String fieldName = atts.getValue("name");
			String alias = atts.getValue("as");

			// campo a ser selecionado
			String f = String.format("%s.%s", this.table.peek(), fieldName);
			if (alias != null)
				f += String.format(" AS %s", alias);
			this.select.add(f);
			break;
		case "sub":
			fieldName = atts.getValue("name");
			String table = atts.getValue("table");
			String sinc = atts.getValue("sinc");

			String join = atts.getValue("join");
			if (join == null)
				join = "INNER";

			alias = atts.getValue("as");
			if (alias != null)
				table = String.format("%s %s", table, alias);
			else
				alias = table;

			// faz o inner join
			this.from.add(String.format("%s JOIN %s ON %s.%s = %s.%s", join, table, this.table.peek(), fieldName, alias,
					sinc));

			// sobe de nível...
			this.table.push(alias);
			break;
		}
	}

	@Override
	public void characters(String s) {
		if ("filter".equals(temp1)) {
			// ---------- nós dos terminais ----------
			this.where.add(temp2 + s);
			this.temp2 = null;
		}
	}

	@Override
	public void endElement(String qName) {
		switch (qName) {
		case "sub":
			// desce de nível...
			this.table.pop();
			break;
		case "sqlDB":
			this.table = null;
			break;
		}

		this.temp1 = null;
	}

	@Override
	public void endDocument() throws SAXException {
	}
}
