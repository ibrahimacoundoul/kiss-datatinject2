package fr.canalplus.cgaweb.datainject.injector.dao;

import java.util.Map;

import org.dbunit.operation.DatabaseOperation;

public interface IDataInjectorDao {

	void injectDataFromFile(long numabo, Map<String, String> keyValueParam, DatabaseOperation databaseOperation) throws Exception;

	void injectDataFromFileWithVariables(long numabo, Map<String, String> keyValueParam, DatabaseOperation databaseOperation) throws Exception;

}
