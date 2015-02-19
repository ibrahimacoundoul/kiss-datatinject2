package fr.canalplus.cgaweb.datainject.services.extractor.main;

import java.util.List;

import javax.inject.Inject;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Component;

import fr.canalplus.cgaweb.datainject.services.common.AbstractDataInjectMain;
import fr.canalplus.cgaweb.datainject.services.extractor.IDataInjectExtractorService;

@Component
public class DataInjectExtractorMain extends AbstractDataInjectMain {

	@Inject
	private IDataInjectExtractorService dataInjectExtractor;

	public static void main(String[] args) throws Exception {
		ApplicationContext context = null;
		try {
			context = new ClassPathXmlApplicationContext("classpath:spring-context.xml");
			DataInjectExtractorMain dataInjectExtractorMain = context.getBean(DataInjectExtractorMain.class);

			List<Long> pkList = dataInjectExtractorMain.getNumaboToExtract();
			//Exatract data
			for (Long pk : pkList) {
				dataInjectExtractorMain.extractData(pk);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			((ConfigurableApplicationContext) context).close();
		}
	}

	private void extractData(long numabo) throws Exception {
		//dataInjectExtractor.extractData(numabo);
		dataInjectExtractor.extractDataBis(numabo);
	}
}