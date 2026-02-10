package br.com.pereiraeng.sql;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import br.com.pereiraeng.core.TimeUtils;
import br.com.pereiraeng.core.collections.comparison.ArrayComparator;
import br.com.pereiraeng.io.IOutils;
import br.com.pereiraeng.math.expression.ExpressaoB;
import br.com.pereiraeng.math.expression.discret.VariavelNB;
import br.com.pereiraeng.math.expression.discret.VariavelNB.TypeValue;

/**
 * Classe que permite a leitura de arquivos no formato SQL e consequentemente
 * fazer leituras off-line de registros
 * 
 * @author Philipe PEREIRA
 *
 */
public class SQLoffline extends HashMap<String, SQLtable> {
	private static final long serialVersionUID = 1L;

	/**
	 * Endereço do arquivo .SQL onde estão os registros
	 */
	private String file;

	/**
	 * Endereço da pasta onde ficarão os arquivos separados das tabelas
	 */
	private String split;

	private Calendar time;

	private final static Pattern ASPAS = Pattern.compile("`\\p{Graph}+?`");

	private final static Pattern KEYWORDS = Pattern.compile("(SELECT)|(FROM)|(WHERE)|(ORDER BY)|(LIMIT)");

	private final static Pattern JOINS = Pattern.compile("(INNER|LEFT|RIGHT|FULL) JOIN");

	/**
	 * Construtor do leitor SQL off-line
	 * 
	 * @param file local do arquivo .SQL onde estão os registro a serem lidos
	 */
	public SQLoffline(String file) {
		this.file = file;

		Scanner sc = IOutils.getSc(this.file, "UTF-8");

		String s = null;
		boolean newTable = false;
		String tableName = null, fieldName = null;

		if (sc != null) {
			TimeZone tz = null;
			while (sc.hasNext()) {
				s = sc.nextLine().trim();

				if (this.time == null) {
					// ao se achar a linha que indica o instante de geração do arquivo SQL
					if (s.startsWith("-- Generation Time: ")) {
						String time = s.substring(20);
						this.time = TimeUtils.string2Calendar(time, "dd-MMM-yyyy' às 'HH:mm");
						if (tz != null)
							this.time = TimeUtils.setTimeZone(this.time, tz);
					} else if (s.startsWith("-- Dump completed on ")) {
						String time = s.substring(21);
						this.time = TimeUtils.string2Calendar(time);
						if (tz != null)
							this.time = TimeUtils.setTimeZone(this.time, tz);
					}
				}

				if (s.startsWith("SET time_zone = \"") || s.startsWith("/*!40103 SET TIME_ZONE=")) {
					// ao se achar a linha que indica o fuso horário
					Matcher m = SQLadapter.TZ_PAT.matcher(s);
					if (m.find())
						tz = TimeZone.getTimeZone("GMT" + m.group());
					else
						tz = Calendar.getInstance().getTimeZone();
					if (this.time != null)
						this.time = TimeUtils.setTimeZone(this.time, tz);
				}

				// novas tabelas
				if (!newTable) {
					newTable = s.startsWith("CREATE TABLE");
					// se aparecer o código de criação de uma nova tabela
					if (newTable) {
						Matcher m = ASPAS.matcher(s);
						m.find();
						tableName = m.group();
						tableName = tableName.substring(1, tableName.length() - 1);

						super.put(tableName, new SQLtable());
					}
				}
				if (newTable) {
					newTable = !s.startsWith(")");
					// se ainda não terminou a tabela nem for a linha inicial
					if (newTable && !s.startsWith("CREATE")) {
						// procura campo
						Matcher m = ASPAS.matcher(s);
						if (m.find()) {
							fieldName = m.group();
							fieldName = fieldName.substring(1, fieldName.length() - 1);
							if (!s.startsWith("PRIMARY")) {
								super.get(tableName).put(fieldName, s.split(" ")[1]);
							} else {
								super.get(tableName).setPrimary(fieldName);
							}
						}
					}
				}
			}
			sc.close();
		}
	}

	/**
	 * Função que retorna a data da geração do arquivo off-line
	 * 
	 * @return objeto que indica a data de geração do arquivo
	 */
	public Calendar getTime() {
		return time;
	}

	private static final String LAST_FILE = "_time_";

	public void setSplit(String split) {
		this.split = split;
		if (this.split != null) {
			// se for indicado um diretório...

			File time = new File(this.split + File.separator + LAST_FILE
					+ this.file.substring(this.file.lastIndexOf(File.separator) + 1, this.file.indexOf('.')));

			// ... criá-lo (se for necessário)...
			File folder = new File(this.split);
			boolean doSplit = !folder.exists();
			if (doSplit)
				folder.mkdir();
			else {
				// se já há um diretório, ver qual a data dos arquivos particionados

				if (time.exists()) {
					String s = null;
					try {
						RandomAccessFile raf = new RandomAccessFile(time, "r");
						s = raf.readLine();
						raf.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					Calendar c = TimeUtils.string2Calendar(s);

					// se os arquivos particionados existentes no diretório estiverem
					// desatualizados, efetuar particionamento
					if (c.before(getTime()))
						doSplit = true;
				} else
					doSplit = true;
			}

			// ... e separar os arquivos (se necessário)
			if (doSplit) {
				// apagar arquivos antigos...
				File[] old = folder.listFiles();
				for (int i = 0; i < old.length; i++)
					old[i].delete();

				// escrever um arquivo de texto com a data
				try {
					RandomAccessFile raf = new RandomAccessFile(time, "rw");
					Calendar c = (Calendar) getTime().clone();
					raf.writeBytes(String.format("%1$tF %1$tT", c));
					raf.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

				// ler o arquivo-mãe a ser quebrado
				Scanner sc = IOutils.getSc(this.file, "UTF-8");
				Writer w = null;
				try {
					while (sc.hasNext()) {
						String s = sc.nextLine().trim();

						if (s.startsWith("INSERT INTO `")) {
							// começo

							// nome da tabela
							String t = s.substring(13);
							t = t.substring(0, t.indexOf("` "));

							// arquivo da tabela
							File tf = new File(this.split + "/" + t + ".sql");
							boolean existing = tf.exists();
							w = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tf, existing), "UTF-8"));
						}

						// meio
						if (w != null)
							w.write(s + "\n");

						// fim
						if (s.endsWith(");")) {
							w.close();
							w = null;
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public void close() {
		this.time = null;
		this.file = null;
		this.clear();
	}

	/**
	 * Função que executa uma busca na base de dados off-line
	 * 
	 * @param sql sequência de caracteres representando o comando SQL da busca
	 * @return lista de vetor dos objetos extraídos da base de dados
	 */
	public List<Object[]> executeQuery(String sql) {

		// ********** INTERPRETAÇÃO DOS COMANDOS DA QUERY **********

		// campos que se quer obter
		String[] fields = null;
		boolean all = false;
		// tabela onde se faz a busca
		String table = null;
		// restrições sobre as entradas
		ExpressaoB restriction = null;
		LinkedHashMap<String, Integer> fields2variable = new LinkedHashMap<>();
		// faixa de leitura
		int beg = -1;
		int n = Integer.MAX_VALUE;

		// ler cada um dos comandos
		Matcher m = KEYWORDS.matcher(sql);
		m.find();
		String stat = m.group(), nextStat = null;
		int e = m.end(), b = -1;
		boolean flag = true;

		// ordenação (se houver)
		int order = -1;
		boolean asc = true;

		while (flag) {
			if (m.find()) {
				nextStat = m.group();

				// posição do começo do próximo comando
				b = m.start();
			} else {
				flag = false;
				// não há próximo comando
				nextStat = null;

				// argumentos do próximo comando vão até o final do string
				b = sql.length();
			}

			String arg = sql.substring(e, b).trim();

			// comando anterior
			if (stat.equals("SELECT")) {
				// seleção dos campos que comporão a saída
				if (arg.equals("*")) {
					all = true;
				} else {
					// se for uma lista
					fields = arg.split(",");

					for (int i = 0; i < fields.length; i++) {
						// remover espaços
						fields[i] = fields[i].trim();

						// em branco e aspas (se houver)
						if (fields[i].charAt(0) == '`' && fields[i].charAt(fields[i].length() - 1) == '`')
							fields[i] = fields[i].substring(1, fields[i].length() - 1);
					}
				}
			} else if (stat.equals("FROM")) {
				Matcher m1 = JOINS.matcher(arg);
				if (m1.find()) {
					// TODO joins...
					System.out.println(arg);
				} else {
					// remover espaços
					table = arg.trim();

					// em branco e aspas (se houver)
					if (table.charAt(0) == '`' && table.charAt(table.length() - 1) == '`')
						table = table.substring(1, table.length() - 1);

					if (all) {
						// se forem todos os campos (tem de se saber de qual
						// tabela se quer todos, por isso a lista é reunida
						// aqui, e não em "SELECT")
						Set<String> s = this.get(table).keySet();
						fields = s.toArray(new String[s.size()]);
					}
				}
			} else if (stat.equals("WHERE") && !arg.equals("1")) {
				if (arg.equals("0"))
					return new LinkedList<>();

				// adicionar o parênteses no começo (tem de ter algum caracter que delimite
				// pelas extremidades o nome do campo)
				arg = "(" + arg + ")";

				// identificar quais são as variáveis, ou seja, os campos que estão no comando
				// WHERE
				int i = 0;
				for (String f : this.get(table).keySet()) {
					Matcher mat = Pattern.compile("([\\( ]" + f + "[\\) ])|(`" + f + "`)").matcher(arg);
					if (mat.find())
						fields2variable.put(f, i++);
				}

				// procurar qual é o tipo de cada variável e substituir na expressão o nome do
				// campo pela letra 'x' numerada
				TypeValue[] typesIn = new TypeValue[fields2variable.size()];
				i = 0;
				for (String f : fields2variable.keySet()) {
					Matcher mat = Pattern.compile("(?<=[\\( ])(" + f + ")(?=[\\) ])|(`" + f + "`)").matcher(arg);
					arg = mat.replaceAll("x" + fields2variable.get(f));
					typesIn[i++] = TypeValue.getType(this.get(table).get(f));
				}
				// retirar os parênteses iniciais
				arg = arg.substring(1, arg.length() - 1);
				// montar expressão
				restriction = new ExpressaoB(arg, typesIn);
			} else if (stat.equals("LIMIT")) {
				// limitações quanto ao número e posição dos registros procurados
				String[] ls = arg.split(",");

				beg = Integer.parseInt(ls[0].trim());
				n = Integer.parseInt(ls[1].trim());
			} else if (stat.equals("ORDER BY")) {
				// ordenar saída
				String[] fieldOrd = arg.split(" ");

				// se o campo estiver indicado junto com sua tabela, pega somente o campo
				if (fieldOrd[0].contains("."))
					fieldOrd[0] = fieldOrd[0].split("\\.")[1];

				// remove aspas (se houver)
				if (fieldOrd[0].charAt(0) == '`' && fieldOrd[0].charAt(fieldOrd[0].length() - 1) == '`')
					fieldOrd[0] = fieldOrd[0].substring(1, fieldOrd[0].length() - 1);

				// procura quais dos campos será o utilizado na ordenação do
				// vetor
				for (int i = 0; i < fields.length; i++) {
					if (fields[i].equals(fieldOrd[0])) {
						order = i;
						break;
					}
				}
				asc = fieldOrd[1].equals("ASC");
			}

			stat = nextStat;
			if (flag)
				e = m.end();
		}

		// ********** PROCURAR NO ARQUIVO SQL OS REGISTRO **********

		List<Object[]> out = null;

		if (table != null) {
			// **** caso 1: todas as informações estão numa só tabela ****

			// campos a cumprir com as restrições
			int[] posRest = getFieldsPositions(this.get(table),
					fields2variable.keySet().toArray(new String[fields2variable.size()]));

			// campos que comporão a saída posição
			int[] posOut = getFieldsPositions(this.get(table), fields);
			// tipo da saída
			TypeValue[] typesOut = new TypeValue[fields.length];
			for (int j = 0; j < fields.length; j++)
				typesOut[j] = TypeValue.getType(this.get(table).get(fields[j]));

			// lista de dados, saída da busca (linked pois não se sabe a priori
			// quantos serão encontrados)
			out = new LinkedList<>();

			// contador e variáveis auxiliares
			int c = 0;
			String s = null;
			boolean selectedTable = false;

			// varrendo o arquivo
			Scanner sc = null;
			if (split == null)
				sc = IOutils.getSc(file, "UTF-8");
			else
				sc = IOutils.getSc(split + "/" + table + ".sql", "UTF-8");
			if (sc != null) {
				// se o arquivo existir (tabelas vazias não geram arquivos
				// temporários no modo split)
				while (sc.hasNext()) {
					s = sc.nextLine().trim();

					// ainda procurando a tabela
					int pos = 1;
					if (!selectedTable) {
						// se aparecer o código de criação de uma nova tabela
						selectedTable = s.startsWith("INSERT INTO `" + table + "` ");
						if (selectedTable)
							pos = s.length() - s.indexOf(" VALUES") - 7;
					}

					// tabela já achada
					if (selectedTable && pos > 0) {
						boolean g = true;
						Matcher matcher = null;
						if (pos > 1) {
							// se houver mais de uma entrada por linha, lê aos
							// poucos separando por vírgulas não 'nestedadas'
							Pattern p = Pattern.compile(",(?=[\\(\\)](?=([^']*'[^']*')*[^']*$))");
							matcher = p.matcher(s);
							pos = s.length() - pos + 1;
						}

						while (g) {
							String entry = null;
							if (matcher == null) {
								// se houver somente uma entrada na linha...
								g = false;
								entry = s;
							} else {
								// se houver mais de uma entrada na linha...
								g = matcher.find();
								if (g) {
									entry = s.substring(pos, matcher.start());
									pos = matcher.end();
								} else
									entry = s.substring(pos, s.length());
							}

							// se há restrições, ver se o registro é compatível
							boolean found = true;
							if (restriction != null)
								found = restriction.f(getFields(entry, posRest));

							if (found) {
								// se o registro cumpre com as restrições (ou se
								// não há restrições)

								if (c >= beg && c <= beg + n) {
									// se o registro está dentro dos limites da
									// busca

									// listar valores dos campos da saída
									String[] values = getFields(entry, posOut);
									// converter a sequência de caracteres em
									// objetos
									Object[] os = new Object[values.length];
									for (int j = 0; j < values.length; j++)
										os[j] = VariavelNB.getValueFromString(values[j], typesOut[j]);

									// adicionar na lista de saída
									out.add(os);
								}
								c++;
							}
						}

						// indicador de fim da tabela
						selectedTable = !s.endsWith(");");
					}
				}
				// encerrar leitura do arquivo
				sc.close();
			}
		} else {
			// **** caso 2: as informações estão em mais de uma tabela ****
			// TODO inner join
		}

		// caso tenha sido incluído o comando de ordenar...
		if (order >= 0)
			Collections.sort(out, new ArrayComparator.Object(order, asc));

		return out;
	}

	/**
	 * Função que procura as posições dos campos dentro de uma tabela SQl
	 * 
	 * @param table  mapa que relaciona todos os campos da tabela SQL
	 * @param fields vetor com os campos procurados
	 * @return vetor com as posições dos campos
	 */
	public static int[] getFieldsPositions(LinkedHashMap<String, String> table, String... fields) {
		if (fields == null)
			return null;

		int[] out = new int[fields.length];

		for (int k = 0; k < fields.length; k++) {
			int i = 0;
			for (String f : table.keySet()) {
				if (fields[k].equals(f)) {
					out[k] = i;
					break;
				}
				i++;
			}
		}
		return out;
	}

	/**
	 * Função que extrai os dados de um dado entrada, sendo estes em certas posições
	 * 
	 * @param entry  entrada no formato SQL (entre parênteses e separado por
	 *               vírgula)
	 * @param fields vetor com as posições dos campos procurados
	 * @return vetor com as sequências de caracteres representativas dos objetos da
	 *         entrada
	 */
	public static String[] getFields(String entry, int[] fields) {
		// remove ',' ou ';' do final
		char c = entry.charAt(entry.length() - 1);
		if (c == ',' || c == ';')
			entry = entry.substring(0, entry.length() - 1);
		// remove parênteses, depois quebra a String pulando as aspas
		String[] values = entry.substring(1, entry.length() - 1).split(",(?=([^']*'[^']*')*[^']*$)");
		String[] out = new String[fields.length];
		for (int i = 0; i < fields.length; i++)
			// remover espaços em branco
			out[i] = values[fields[i]].trim();
		return out;
	}

	/**
	 * Função que retorna o número de colunas a serem retornadas para uma dada busca
	 * 
	 * @param query expressão da busca
	 * @return número de coluna que seriam retornadas pela busca
	 */
	public int getColumnCount(String query) {
		// achar a seleção de campos da busca
		Matcher m = Pattern.compile("(SELECT)[\\p{Graph}\\p{Space}]+?(FROM)").matcher(query);
		m.find();
		String fields = m.group();
		fields = fields.substring(6, fields.length() - 4).trim();

		if (fields.equals("*")) {
			// se a busca quer que se retorne todos os campos, achar o nome da
			// tabela e procurar na tabela de dispersão deste objeto
			Matcher m2 = Pattern.compile("(FROM) `[\\p{Graph}\\p{Space}]+?`").matcher(query);
			m2.region(m.start(), query.length());
			m2.find();
			String table = m2.group();
			return this.get(table.substring(6, table.length() - 1)).size();
		} else {
			// se não se deseja todos os campos, contar aqueles que estão
			// separados por vírgula
			return fields.split(",").length;
		}
	}
}