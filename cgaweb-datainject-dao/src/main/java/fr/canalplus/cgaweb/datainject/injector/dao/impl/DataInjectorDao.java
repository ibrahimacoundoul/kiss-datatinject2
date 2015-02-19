package fr.canalplus.cgaweb.datainject.injector.dao.impl;

import java.io.File;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.sql.DataSource;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ReplacementDataSet;
import org.dbunit.dataset.filter.IColumnFilter;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.springframework.stereotype.Repository;

import fr.canalplus.cgaweb.datainject.config.DataInjectConfig;
import fr.canalplus.cgaweb.datainject.config.utils.AppPropertiesUtils;
import fr.canalplus.cgaweb.datainject.exceptions.TechnicalException;
import fr.canalplus.cgaweb.datainject.exceptions.TechnicalException.ErrorCode;
import fr.canalplus.cgaweb.datainject.injector.dao.IDataInjectorDao;

@Repository
public class DataInjectorDao implements IDataInjectorDao {

	@Inject
	private DataSource dataSourceDestDB;

	@Inject
	private AppPropertiesUtils appPropertiesUtils;
	
	private IDatabaseConnection getConnection() throws TechnicalException {
		try {
			String schema = "WEBUSER";
			Connection jdbcConnection = dataSourceDestDB.getConnection();
			IDatabaseConnection connection = new DatabaseConnection(jdbcConnection, schema);
			return connection;
		} catch (Exception e) {
			throw new TechnicalException(ErrorCode.ERR_TECH_GETTING_CONNECTION, e);
		}
	}

	public void injectDataFromFile(long numabo, Map<String, String> keyValueParam, DatabaseOperation databaseOperation) throws TechnicalException {

		try {

			System.out.println("\n******************************** Injection de l'abonné :" + numabo + "**************************************");

			System.out.println("Begin : DataInjector.injectDataFromFileWithVariables");

			String dependentDataFile = new StringBuffer(appPropertiesUtils.getValueByKey("outputDir"))
					.append(DataInjectConfig.DEPENDANT_DATA_FILE_NAME).append(DataInjectConfig.ABO_ABONN_TABLE_NAME + "_" + numabo).toString();

			IDataSet dataSet = new FlatXmlDataSetBuilder().build(new File(dependentDataFile));
			ReplacementDataSet replacementDataSet = new ReplacementDataSet(dataSet);
			Iterator<String> it = keyValueParam.keySet().iterator();
			while (it.hasNext()) {
				String key = (String) it.next();
				replacementDataSet.addReplacementSubstring(key, keyValueParam.get(key));
			}
			
			IDatabaseConnection dbConnection = getConnection();
			dbConnection.getConfig().setProperty(DatabaseConfig.PROPERTY_PRIMARY_KEY_FILTER, configPkFilter(dataSet));
			
			databaseOperation.execute(dbConnection, replacementDataSet);//ico
		} catch (DatabaseUnitException e) {
			e.printStackTrace();
			throw new TechnicalException(ErrorCode.ERR_TECH_DATABASE_UNIT, e);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new TechnicalException(ErrorCode.ERR_TECH_SQL_EXCEPTION, e);
		} catch (MalformedURLException e) {
			e.printStackTrace();
			throw new TechnicalException(ErrorCode.ERR_TECH_MALFORMED_URL, e);
		}
		System.out.println("End : DataInjector.injectDataFromFileWithVariables");
	}

	public void injectDataFromFileWithVariables(long numabo, Map<String, String> keyValueParam, DatabaseOperation databaseOperation) throws TechnicalException {

		try {

			System.out.println("\n******************************** Injection de l'abonné :" + numabo + "**************************************");

			System.out.println("\nBegin : Data injection in progress...");
			StringBuffer variabilizedFile = new StringBuffer(appPropertiesUtils.getValueByKey("outputDir")).append("Variable_")
					.append(DataInjectConfig.DEPENDANT_DATA_FILE_NAME).append(DataInjectConfig.ABO_ABONN_TABLE_NAME + "_" + numabo)
					.append(DataInjectConfig.XML_FILE_EXTENTION);

			IDataSet dataSet = new FlatXmlDataSetBuilder().build(new File(variabilizedFile.toString()));
			ReplacementDataSet replacementDataSet = new ReplacementDataSet(dataSet);
			Iterator<String> it = keyValueParam.keySet().iterator();
			while (it.hasNext()) {
				String key = (String) it.next();
				replacementDataSet.addReplacementSubstring(key, keyValueParam.get(key));
			}
			
			IDatabaseConnection dbConnection = getConnection();
			dbConnection.getConfig().setProperty(DatabaseConfig.PROPERTY_PRIMARY_KEY_FILTER, configPkFilter(dataSet));

			databaseOperation.execute(dbConnection, replacementDataSet);
		} catch (DatabaseUnitException e) {
			throw new TechnicalException(ErrorCode.ERR_TECH_DATABASE_UNIT, e);
		} catch (SQLException e) {
			throw new TechnicalException(ErrorCode.ERR_TECH_SQL_EXCEPTION, e);
		} catch (MalformedURLException e) {
			throw new TechnicalException(ErrorCode.ERR_TECH_MALFORMED_URL, e);
		} catch (Exception e) {
			throw new TechnicalException(ErrorCode.ERR_TECH_GEN_DEFAULT, e);
		}
		System.out.println("\nEND : Data injection\n");
	}

	private Object configPkFilter(IDataSet dataSet) throws DataSetException, SQLException {
		final Map<String, List<Column>> pseudotablePkMap = new HashMap<>();
		
		for(String tableName: dataSet.getTableNames()){
			final Column[] primaryKeys = getConnection().createDataSet().getTableMetaData(tableName).getPrimaryKeys(); 
			
			if(primaryKeys.length > 0){
				pseudotablePkMap.put(tableName, Arrays.asList(primaryKeys));
			}  
		}
		
		Object pkFilter = new IColumnFilter() {
			@Override
			   public boolean accept(String tableName, Column column) {
			      if (pseudotablePkMap.containsKey(tableName)) {
			         return pseudotablePkMap.get(tableName).contains(column);
			      } else {
			         return column.getColumnName().equalsIgnoreCase("NUMABO");
			      }
			   }
			};
		
		return pkFilter;
	}
}
