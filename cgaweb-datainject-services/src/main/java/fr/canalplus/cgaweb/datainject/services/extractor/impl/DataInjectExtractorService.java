package fr.canalplus.cgaweb.datainject.services.extractor.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import fr.canalplus.cgaweb.datainject.exceptions.TechnicalException;
import fr.canalplus.cgaweb.datainject.extractor.dao.IDataExtractorDao;
import fr.canalplus.cgaweb.datainject.services.extractor.IDataInjectExtractorService;

@Service("dataInjectExtractor")
public class DataInjectExtractorService implements IDataInjectExtractorService {

	@Inject
	private IDataExtractorDao dataExtractorDao;

	public void extractData(long numabo) throws Exception {
		dataExtractorDao.extractData(numabo);
	}

	public void extractDataBis(long numabo) throws TechnicalException, Exception {
		// TODO Auto-generated method stub
		dataExtractorDao.extractDataBis(numabo);

	}
}
