package fr.canalplus.cgaweb.datainject.variabilisation;

import java.util.Map;

public interface IDataInjectnVarialibisation {

	Map<String, String> getVariabilizedColumns(long rootTablepkValue) throws Exception;
}
