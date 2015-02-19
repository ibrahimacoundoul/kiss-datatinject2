package fr.canalplus.cgaweb.datainject.services.injector;

import java.util.Map;

import org.dbunit.operation.DatabaseOperation;

public interface IDataInjectInjectorService {

	void injectDataFromFile(long rootTablePkValue, Map<String, String> keyValueParam, DatabaseOperation databaseOperation) throws Exception;

	void injectDataFromFileWithVariables(long rootTablePkValue, Map<String, String> keyValueParam, DatabaseOperation databaseOperation) throws Exception;

}
