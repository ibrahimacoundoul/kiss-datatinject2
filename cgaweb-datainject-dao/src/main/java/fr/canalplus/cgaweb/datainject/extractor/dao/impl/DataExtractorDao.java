package fr.canalplus.cgaweb.datainject.extractor.dao.impl;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;

import javax.inject.Inject;
import javax.sql.DataSource;

import org.apache.commons.collections4.map.LinkedMap;
import org.dbunit.database.AmbiguousTableNameException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.dbunit.database.search.ExportedKeysSearchCallback;
import org.dbunit.database.search.ForeignKeyRelationshipEdge;
import org.dbunit.database.search.ImportedKeysSearchCallback;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.Columns;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.NoSuchColumnException;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.util.search.SearchException;
import org.springframework.stereotype.Repository;

import fr.canalplus.cgaweb.datainject.config.DataInjectConfig;
import fr.canalplus.cgaweb.datainject.config.utils.AppPropertiesUtils;
import fr.canalplus.cgaweb.datainject.exceptions.TechnicalException;
import fr.canalplus.cgaweb.datainject.exceptions.TechnicalException.ErrorCode;
import fr.canalplus.cgaweb.datainject.extractor.customer.dao.KISSFlatXmlDataSet;
import fr.canalplus.cgaweb.datainject.extractor.customer.dao.KISSTablesDependencyHelper;
import fr.canalplus.cgaweb.datainject.extractor.dao.IDataExtractorDao;

@Repository
public class DataExtractorDao implements IDataExtractorDao {

	private static final Set<String> ENCRYPTED_COLUMN_LIST = new HashSet<>(Arrays.asList("NCPTE", "NUMCB", "IBAN"));

	@Inject
	private DataSource dataSourceSourceDB;
	@Inject
	private DataSource dataSourceDestDB;

	@Inject
	private AppPropertiesUtils appPropertiesUtils;

	private IDatabaseConnection getConnection(final DataSource dataSource) throws TechnicalException {
		try {
			String schema = "WEBUSER";
			//Connection jdbcConnection = dataSourceSourceDB.getConnection();
			Connection jdbcConnection = dataSource.getConnection();
			IDatabaseConnection connection = new DatabaseConnection(jdbcConnection, schema);
			return connection;
		} catch (Exception e) {
			throw new TechnicalException(ErrorCode.ERR_TECH_GETTING_CONNECTION, e);
		}
	}

	public void extractData(long numabo) throws TechnicalException {
		System.out.println("\n******************************** Extraction de l'abonné :" + numabo + "**************************************");
		System.out.println("\nBegin : Data extraction in progress...");
		generateParentData(numabo);
		generateDependentDataByPk(numabo);
		System.out.println("\nEnd : Data extraction...\n");
	}

	public void extractDataBis(long numabo) throws TechnicalException, Exception {
		try {
			//new HashSet(Arrays.asList(new new BigDecimal(numabo));
			//allowedPks.add(numabo);
			//		String[] depTableNames = TablesDependencyHelper.getAllDependentTables(getConnection(dataSourceSourceDB),
			//				DataInjectConfig.ABO_ABONN_TABLE_NAME);
			//		
			IDataSet dataSet = KISSTablesDependencyHelper.getAllDataset(getConnection(dataSourceSourceDB), DataInjectConfig.ABO_ABONN_TABLE_NAME,
					new HashSet(Arrays.asList(new BigDecimal(numabo))));
			//IDataSet depDataSet = getConnection(dataSourceSourceDB).createDataSet(depTableNames);
			//KISSAbstractDataSet kissDataSet = (KISSAbstractDataSet) dataSet;
			KISSFlatXmlDataSet.write(dataSet,
					new FileOutputStream(new StringBuffer(appPropertiesUtils.getValueByKey("outputDir")).append("dependents.xml").toString()));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private void generateParentData(long numabo) throws TechnicalException {
		generateParentDataByPk(DataInjectConfig.ABO_ABONN_TABLE_NAME, DataInjectConfig.ABO_ABONN_TABLE_PK_NAME, numabo);
	}

	private void generateParentDataByPk(String rootTableName, String rootTablePkName, Long rootTablePkValue) throws TechnicalException {
		try {
			ImportedKeysSearchCallback importedKeysSearchCallback = new ImportedKeysSearchCallback(getConnection(dataSourceDestDB));
			SortedSet<ForeignKeyRelationshipEdge> edg = importedKeysSearchCallback.getEdges(rootTableName);

			QueryDataSet partialDataSet = new QueryDataSet(getConnection(dataSourceDestDB));

			for (ForeignKeyRelationshipEdge object : edg) {
				String tableNameCurrent = object.getTo().toString();

				String queryCurrent = "select * from " + tableNameCurrent + " where " + object.getPKColumn() + "= (select " + object.getFKColumn()
						+ " from " + object.getFrom() + " where " + rootTablePkName + " = " + "'" + rootTablePkValue + "'" + ")";

				partialDataSet.addTable(tableNameCurrent, queryCurrent);
			}

			String outputFileName = DataInjectConfig.PARENT_DATA_FILE_NAME + DataInjectConfig.ABO_ABONN_TABLE_NAME + "_" + rootTablePkValue
					+ DataInjectConfig.XML_FILE_EXTENTION;
			String filePath = new StringBuffer(appPropertiesUtils.getValueByKey("outputDir")).append(outputFileName).toString();
			writeTofile(partialDataSet, filePath);
		} catch (AmbiguousTableNameException e) {
			e.printStackTrace();
			throw new TechnicalException(ErrorCode.ERR_TECH_AMBIGUOUS_TABLE_NANE, e);
		} catch (SearchException e) {
			e.printStackTrace();
			throw new TechnicalException(ErrorCode.ERR_TECH_AMBIGUOUS_TABLE_NANE, e);
		} catch (TechnicalException e) {
			throw e;
		} catch (Exception e) {
			throw new TechnicalException(ErrorCode.ERR_TECH_GEN_DEFAULT, e);
		}
	}

	private void generateDependentDataByPk(long numabo) throws TechnicalException {
		dependentDataByPk(DataInjectConfig.ABO_ABONN_TABLE_NAME, numabo, DataInjectConfig.ABO_ABONN_TABLE_PK_NAME);
	}

	public void dependentDataByPk(String initialTableName, Object pk, String pkName) throws TechnicalException {
		System.out.println("DEBUT: generation des datas descendants en cours....");
		final IDatabaseConnection dbConnection = getConnection(dataSourceDestDB);

		ExportedKeysSearchCallback keysSearchCallback = new ExportedKeysSearchCallback(dbConnection);

		try {
			Map<String, String> tableQueryMap = new LinkedHashMap<>();

			String initialQuery = buildQueryByPk(initialTableName, pk, pkName, null, null, null, dbConnection);

			//Map<String, String> tableQueryTempMap = new LinkedHashMap<>();
			LinkedMap<String, String> tableQueryTempMap = new LinkedMap<>();
			tableQueryTempMap.put(initialTableName, initialQuery);
			while (!tableQueryTempMap.isEmpty()) {
				//final Map.Entry<String, String> tableQuery = tableQueryTempMap.entrySet().iterator().next();
				//final String table = tableQuery.getKey();
				final String table = tableQueryTempMap.firstKey();
				final String query = tableQueryTempMap.get(table);

				tableQueryTempMap.remove(table);

				//tableQueryMap.put(table, tableQuery.getValue());
				tableQueryMap.put(table, query);

				//Adds a table and it's associated query
				//getDataSetFkRelationshipEdge(tableQuery, pk, keysSearchCallback, dbConnection, tableQueryTempMap, tableQueryMap);
				getDataSetFkRelationshipEdge(table, query, pk, keysSearchCallback, dbConnection, tableQueryTempMap, tableQueryMap);

			}

			QueryDataSet partialDataSet = new QueryDataSet(getConnection(dataSourceSourceDB));
			for (Map.Entry<String, String> tableQuery : tableQueryMap.entrySet()) {
				/*
				final String table = tableQuery.getKey();
				
				final Column[] primaryKeys = dbConnection.createDataSet().getTableMetaData(table).getPrimaryKeys();
				
				if(primaryKeys.length > 0){
					partialDataSet.addTable(table, "select * from(" + tableQuery.getValue() + ") order by " + primaryKeys[0].getColumnName());
				}else{
					partialDataSet.addTable(tableQuery.getKey(), tableQuery.getValue());
				}
				*/

				partialDataSet.addTable(tableQuery.getKey(), tableQuery.getValue());
			}

			StringBuffer outputFilePath = new StringBuffer(appPropertiesUtils.getValueByKey("outputDir"))
					.append(DataInjectConfig.DEPENDANT_DATA_FILE_NAME).append(DataInjectConfig.ABO_ABONN_TABLE_NAME + "_" + (Long) pk)
					.append(DataInjectConfig.XML_FILE_EXTENTION);
			String filePath = outputFilePath.toString();
			try {
				writeTofile(partialDataSet, filePath);
				System.out.println("FIN: generation des datas descendants en cours....");
			} catch (Exception e) {
				throw e;
			}
		} catch (AmbiguousTableNameException e) {
			e.printStackTrace();
			throw new TechnicalException(ErrorCode.ERR_TECH_AMBIGUOUS_TABLE_NANE, e);
		} catch (SearchException e) {
			e.printStackTrace();
			throw new TechnicalException(ErrorCode.ERR_TECH_SERACH_EXCEPTION, e);
		} catch (Exception e) {
			throw new TechnicalException(ErrorCode.ERR_TECH_GEN_DEFAULT, e);
		}
	}

	private void getDataSetFkRelationshipEdge(final String table, String query, Object pk, ExportedKeysSearchCallback keysSearchCallback,
			final IDatabaseConnection dbConnection, Map<String, String> tableQueryTempMap, Map<String, String> tableQueryMap) throws SearchException,
			DataSetException, SQLException, AmbiguousTableNameException {

		final Column[] primaryKeys = dbConnection.createDataSet().getTableMetaData(table).getPrimaryKeys();

		if (primaryKeys.length > 0) {
			query = query.replace(query.substring("SELECT DISTINCT ".length(), query.indexOf(" FROM")), table + "." + primaryKeys[0].getColumnName());
		} else {
			query = null;
		}

		LinkedMap<ForeignKeyRelationshipEdge, Set<String>> edgeMap = new LinkedMap<>();

		Map<String, ForeignKeyRelationshipEdge> initialTableEdgeMap = new HashMap<>();
		SortedSet<ForeignKeyRelationshipEdge> edgeSet = keysSearchCallback.getEdges(table);
		for (final ForeignKeyRelationshipEdge edge : edgeSet) {
			final String edgeTable = edge.getFrom().toString();

			initialTableEdgeMap.put(edgeTable, edge);

			Set<String> tableSet = new HashSet<String>();
			SortedSet<ForeignKeyRelationshipEdge> edges = keysSearchCallback.getEdges(edgeTable);
			for (ForeignKeyRelationshipEdge edge2 : edges) {
				tableSet.add(edge2.getFrom().toString());
			}

			edgeMap.put(edge, tableSet);
		}

		Set<ForeignKeyRelationshipEdge> rightOrderEdgeSet = new LinkedHashSet<>();
		while (!initialTableEdgeMap.isEmpty()) {
			final Entry<String, ForeignKeyRelationshipEdge> initialTableEdge = initialTableEdgeMap.entrySet().iterator().next();

			initialTableEdgeMap.remove(initialTableEdge.getKey());

			Set<ForeignKeyRelationshipEdge> edgeRemoveSet = new HashSet<>();
			for (final Entry<ForeignKeyRelationshipEdge, Set<String>> edgeEntry : edgeMap.entrySet()) {
				if (!rightOrderEdgeSet.contains(edgeEntry)) {
					final ForeignKeyRelationshipEdge edge = edgeEntry.getKey();
					final Set<String> tableSet = edgeEntry.getValue();

					if (tableSet.contains(initialTableEdge.getKey())) {
						if (!rightOrderEdgeSet.contains(edge)) {
							rightOrderEdgeSet.add(edge);

							initialTableEdgeMap.remove(initialTableEdge);

							edgeRemoveSet.add(edge);
						}
					}
				}
			}

			rightOrderEdgeSet.add(initialTableEdge.getValue());

			for (final ForeignKeyRelationshipEdge edgeRemove : edgeRemoveSet) {
				edgeMap.remove(edgeRemove);
			}
		}

		for (ForeignKeyRelationshipEdge edge : rightOrderEdgeSet) {
			String tableNameCurrent = edge.getFrom().toString();
			String pkNameCurrent = edge.getFKColumn();

			//Retrieving eventual composite foreign key
			Set<String> compositeFkSet = probabilisticRetrievalCompositeForeignKey(primaryKeys, tableNameCurrent, dbConnection);

			String currentRequest = buildQueryByPk(tableNameCurrent, pk, pkNameCurrent, table, compositeFkSet, query, dbConnection);

			if (!tableQueryMap.containsKey(tableNameCurrent)) {
				if (!tableQueryTempMap.containsKey(tableNameCurrent)) {
					tableQueryTempMap.put(tableNameCurrent, currentRequest);
				} else {
					tableQueryTempMap.put(tableNameCurrent, tableQueryTempMap.get(tableNameCurrent) + " UNION " + currentRequest);
				}
			}
		}
	}

	private Set<String> probabilisticRetrievalCompositeForeignKey(final Column[] primaryKeys, final String tableNameCurrent,
			final IDatabaseConnection dbConnection) throws DataSetException, SQLException {
		Set<String> compositeFKSet = new HashSet<>();

		try {
			Column[] fkColumns = null;
			ITableMetaData tableCurrentMetaData = dbConnection.createDataSet().getTableMetaData(tableNameCurrent);
			fkColumns = Columns.findColumnsByName(primaryKeys, tableCurrentMetaData);

			if (fkColumns != null) {
				for (Column columnFK : fkColumns) {
					compositeFKSet.add(columnFK.getColumnName());
				}
			}
		} catch (NoSuchColumnException e) {
			//TODO: Adding log
			//System.out.println(e.getMessage());
			//System.out.println("No column " + Arrays.asList(primaryKeys) + "on table " + tableNameCurrent);
		}

		return compositeFKSet;
	}

	private String buildQueryByPk(final String currentTable, Object pk, String pkName, String table, Set<String> compositeFkSet,
			String previousQuery, final IDatabaseConnection dbConnection) throws DataSetException, SQLException {
		final Boolean isCompositeFkSet = compositeFkSet != null && compositeFkSet.remove(pkName) && !compositeFkSet.isEmpty();

		StringBuilder query = new StringBuilder("SELECT DISTINCT ");

		query.append(buildSelectQuery(currentTable, dbConnection));

		query.append(" FROM ").append(currentTable);
		if (isCompositeFkSet) {
			query.append(", ").append(table);
		}
		query.append(" WHERE ");
		if (isCompositeFkSet) {
			query.append(currentTable).append('.');
		}
		query.append(pkName).append(" IN (");
		if (previousQuery == null) {
			query.append(pk);
		} else {
			query.append(previousQuery);
		}
		query.append(")");

		if (isCompositeFkSet) {
			for (final String compositeFk : compositeFkSet) {
				query.append(" AND ").append(currentTable).append('.').append(compositeFk).append(" = ").append(table).append('.')
						.append(compositeFk);
			}
		}

		return query.toString();
	}

	private String buildSelectQuery(final String table, final IDatabaseConnection dbConnection) throws DataSetException, SQLException {
		String selectQuery = table + ".*";

		boolean isCustomized = false;
		StringBuilder customizedSelectQuery = new StringBuilder();

		final Column[] columns = dbConnection.createDataSet().getTableMetaData(table).getColumns();
		for (Column column : columns) {
			final String columnName = column.getColumnName();

			StringBuilder clause = new StringBuilder(table + "." + columnName);

			if (customizedSelectQuery.length() > 0) {
				customizedSelectQuery.append(", ");
			}

			if (column.getSqlTypeName().equals("CLOB")) {
				isCustomized = true;
				clause.setLength(0);
				clause.append("TO_CHAR(").append(table).append(".").append(columnName).append(") AS ").append(columnName);
			}

			if (ENCRYPTED_COLUMN_LIST.contains(columnName)) {
				isCustomized = true;
				clause.setLength(0);
				clause.append("P_CRYPTAGE.CRY_DECRYPT(").append(table).append(".").append(columnName).append(") AS ").append(columnName);
			}

			customizedSelectQuery.append(clause);
		}

		if (isCustomized) {
			selectQuery = customizedSelectQuery.toString();
		}

		return selectQuery;
	}

	private void writeTofile(QueryDataSet partialDataSet, String filePath) throws TechnicalException {
		try {
			System.out.println(partialDataSet);
			FlatXmlDataSet.write(partialDataSet, new FileOutputStream(filePath));
			System.out.println("Dataset written to :" + filePath);
		} catch (DataSetException e) {
			e.printStackTrace();
			throw new TechnicalException(ErrorCode.ERR_TECH_DATA_SET, e);
		} catch (FileNotFoundException e) {
			throw new TechnicalException(ErrorCode.ERR_TECH_FILE_NOT_FOUND, e);
		} catch (IOException e) {
			throw new TechnicalException(ErrorCode.ERR_TECH_IO, e);
		} catch (Exception e) {
			throw new TechnicalException(ErrorCode.ERR_TECH_GEN_DEFAULT, e);
		}
	}

}
