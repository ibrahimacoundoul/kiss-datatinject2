package fr.canalplus.cgaweb.datainject.services.common;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import fr.canalplus.cgaweb.datainject.config.utils.DataInjectProppertiesUtils;

@Component
public class AbstractDataInjectMain {

	@Inject
	private DataInjectProppertiesUtils dataInjectProppertiesUtils;

	protected List<Long> getNumaboToExtract() {

		String numabos = dataInjectProppertiesUtils.getValueByKey("numabo");

		String[] tab = numabos.split("-");

		List<Long> lisNumabos = new ArrayList();

		for (String n : tab) {
			lisNumabos.add(Long.valueOf(n.trim()));
		}
		return lisNumabos;
	}
}
