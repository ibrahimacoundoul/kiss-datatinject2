package fr.canalplus.cgaweb.datainject.services.extractor;

import fr.canalplus.cgaweb.datainject.exceptions.TechnicalException;

public interface IDataInjectExtractorService {

	void extractData(long numabo) throws Exception;

	public void extractDataBis(long numabo) throws TechnicalException, Exception;
}
