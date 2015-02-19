package fr.canalplus.cgaweb.datainject.services.injector.impl;

import java.util.Map;

import javax.inject.Inject;

import org.dbunit.operation.DatabaseOperation;
import org.springframework.stereotype.Service;

import fr.canalplus.cgaweb.datainject.injector.dao.IDataInjectorDao;
import fr.canalplus.cgaweb.datainject.services.injector.IDataInjectInjectorService;

@Service("dataInjectInjector")
public class DataInjectInjectorService implements IDataInjectInjectorService {

	@Inject
	private IDataInjectorDao dataInjectorDao;

	public void injectDataFromFile(long rootTablePkValue, Map<String, String> keyValueParam, DatabaseOperation databaseOperation) throws Exception {
		dataInjectorDao.injectDataFromFile(rootTablePkValue, keyValueParam, databaseOperation);
	}

	public void injectDataFromFileWithVariables(long rootTablePkValue, Map<String, String> keyValueParam, DatabaseOperation databaseOperation) throws Exception {
		dataInjectorDao.injectDataFromFileWithVariables(rootTablePkValue, keyValueParam, databaseOperation);
	}
}
