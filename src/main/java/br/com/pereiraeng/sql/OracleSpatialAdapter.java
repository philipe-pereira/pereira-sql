package br.com.pereiraeng.sql;

import java.sql.SQLException;
import java.sql.Struct;

import oracle.spatial.util.GeometryExceptionWithContext;
import oracle.spatial.util.WKT;

public class OracleSpatialAdapter {

	public static final String SPATIAL_SQL_TYPE = "MDSYS.SDO_GEOMETRY";

	public static String struct2string(Struct struct) {
		String str = null;
		try {
			str = new String(new WKT().fromStruct(struct));
		} catch (GeometryExceptionWithContext | SQLException e) {
			e.printStackTrace();
		}
		return str;
	}

}
