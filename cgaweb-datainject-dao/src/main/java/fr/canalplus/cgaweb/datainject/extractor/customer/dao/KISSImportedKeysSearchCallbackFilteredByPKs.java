package fr.canalplus.cgaweb.datainject.extractor.customer.dao;

import java.sql.ResultSet;

import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.search.ForeignKeyRelationshipEdge;
import org.dbunit.database.search.ImportedKeysSearchCallback;
import org.dbunit.database.search.ImportedKeysSearchCallbackFilteredByPKs;
import org.dbunit.dataset.filter.ITableFilter;
import org.dbunit.util.search.IEdge;
import org.dbunit.util.search.SearchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.canalplus.cgaweb.datainject.extractor.customer.dao.KISSPrimaryKeyFilter.PkTableMap;

public class KISSImportedKeysSearchCallbackFilteredByPKs extends ImportedKeysSearchCallback {

	/**
	 * Logger for this class
	 */
	private static final Logger logger = LoggerFactory.getLogger(ImportedKeysSearchCallbackFilteredByPKs.class);

	// primary key filter associated with the call back
	private final KISSPrimaryKeyFilter pksFilter;

	/**
	 * Default constructor.
	 * @param connection database connection
	 * @param allowedPKs map of allowed rows, based on the primary keys (key is the name
	 * of a table; value is a Set with allowed primary keys for that table)
	 */
	public KISSImportedKeysSearchCallbackFilteredByPKs(IDatabaseConnection connection, PkTableMap allowedPKs) {
		super(connection);
		this.pksFilter = new KISSPrimaryKeyFilter(connection, allowedPKs, false);
	}

	/**
	 * Get the primary key filter associated with the call back
	 * @return primary key filter associated with the call back
	 */
	public ITableFilter getFilter() {
		return this.pksFilter;
	}

	public void nodeAdded(Object node) throws SearchException {
		logger.debug("nodeAdded(node={}) - start", node);

		this.pksFilter.nodeAdded(node);
	}

	protected IEdge newEdge(ResultSet rs, int type, String from, String to, String fkColumn, String pkColumn) throws SearchException {
		if (logger.isDebugEnabled()) {
			logger.debug("newEdge(rs={}, type={}, from={}, to={}, fkColumn={}, pkColumn={}) - start", new Object[] { rs, String.valueOf(type), from,
					to, fkColumn, pkColumn });
		}
		ForeignKeyRelationshipEdge edge = createFKEdge(rs, type, from, to, fkColumn, pkColumn);
		this.pksFilter.edgeAdded(edge);
		return edge;
	}

}
