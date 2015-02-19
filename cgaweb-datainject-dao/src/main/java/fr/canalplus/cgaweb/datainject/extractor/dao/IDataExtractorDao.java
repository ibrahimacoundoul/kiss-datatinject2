package fr.canalplus.cgaweb.datainject.extractor.dao;

import fr.canalplus.cgaweb.datainject.exceptions.TechnicalException;

public interface IDataExtractorDao {

	void extractData(long numabo) throws TechnicalException;

	public void extractDataBis(long numabo) throws TechnicalException, Exception;
}
