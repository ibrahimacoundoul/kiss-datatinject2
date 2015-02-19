package fr.canalplus.cgaweb.datainject.main;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.dbunit.operation.DatabaseOperation;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Component;

import fr.canalplus.cgaweb.datainject.services.common.AbstractDataInjectMain;
import fr.canalplus.cgaweb.datainject.services.extractor.IDataInjectExtractorService;
import fr.canalplus.cgaweb.datainject.services.injector.IDataInjectInjectorService;
import fr.canalplus.cgaweb.datainject.variabilisation.impl.DataInjectVarialibisation;

@Component
public class DataInjectMain extends AbstractDataInjectMain {

	@Inject
	private IDataInjectExtractorService dataInjectExtractor;

	@Inject
	private DataInjectVarialibisation dataInjectnVarialibisation;

	@Inject
	private IDataInjectInjectorService dataInjectInjector;

	public static void main(String[] args) throws Exception {
		ApplicationContext context = null;
		try {
			context = new ClassPathXmlApplicationContext("classpath:spring-context.xml");
			DataInjectMain dataInjectMain = context.getBean(DataInjectMain.class);

			List<Long> pkList = dataInjectMain.getNumaboToExtract();

			for (long pk : pkList) {
				System.out.println("DBEUT: ******** Extraction et Injection de l'abbonné : " + pk + "\n");
				//Exatract data
				dataInjectMain.extractData(pk);
				//variabilze data
				Map<String, String> keyValueParam = dataInjectMain.variabilizeData(pk);
				//inject data
				dataInjectMain.injectData(pk, keyValueParam);

				System.out.println("FIN: ******** Extraction et Injection de l'abbonné : " + pk + "\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			((ConfigurableApplicationContext) context).close();
		}
	}

	private void extractData(long numabo) throws Exception {
		dataInjectExtractor.extractData(numabo);
	}

	private Map<String, String> variabilizeData(long rootTablePkValue) throws Exception {
		return dataInjectnVarialibisation.getVariabilizedColumns(rootTablePkValue);
	}

	private void injectData(long rootTablePkValue, Map<String, String> keyValueParam) throws Exception {
		//dataInjectInjector.injectDataFromFile(keyValueParam, true);
		dataInjectInjector.injectDataFromFileWithVariables(rootTablePkValue, keyValueParam, DatabaseOperation.REFRESH);
	}

}
