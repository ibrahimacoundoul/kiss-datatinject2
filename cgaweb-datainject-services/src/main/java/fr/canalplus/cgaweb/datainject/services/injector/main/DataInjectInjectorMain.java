package fr.canalplus.cgaweb.datainject.services.injector.main;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.dbunit.operation.DatabaseOperation;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Component;

import fr.canalplus.cgaweb.datainject.services.common.AbstractDataInjectMain;
import fr.canalplus.cgaweb.datainject.services.injector.IDataInjectInjectorService;
import fr.canalplus.cgaweb.datainject.variabilisation.impl.DataInjectVarialibisation;

@Component
public class DataInjectInjectorMain extends AbstractDataInjectMain {

	@Inject
	private DataInjectVarialibisation dataInjectnVarialibisation;

	@Inject
	private IDataInjectInjectorService dataInjectInjector;

	public static void main(String[] args) throws Exception {
		ApplicationContext context = null;
		try {
			context = new ClassPathXmlApplicationContext("classpath:spring-context.xml");
			DataInjectInjectorMain dataInjectInjectorMain = context.getBean(DataInjectInjectorMain.class);

			List<Long> numaboToExtract = dataInjectInjectorMain.getNumaboToExtract();

			for (Long numabo : numaboToExtract) {
				//variabilze data
				Map<String, String> keyValueParam = dataInjectInjectorMain.variabilizeData(numabo);
				//inject data
				dataInjectInjectorMain.injectData(numabo, keyValueParam);

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			((ConfigurableApplicationContext) context).close();
		}
	}

	private Map<String, String> variabilizeData(long rootTablePkValue) throws Exception {
		return dataInjectnVarialibisation.getVariabilizedColumns(rootTablePkValue);
	}

	private void injectData(long rootTablePkValue, Map<String, String> keyValueParam) throws Exception {
		//dataInjectInjector.injectDataFromFile(keyValueParam, true);
		dataInjectInjector.injectDataFromFileWithVariables(rootTablePkValue, keyValueParam, DatabaseOperation.REFRESH);
	}

}
