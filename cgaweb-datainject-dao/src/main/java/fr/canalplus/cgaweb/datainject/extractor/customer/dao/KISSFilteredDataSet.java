package fr.canalplus.cgaweb.datainject.extractor.customer.dao;

import org.dbunit.database.AmbiguousTableNameException;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableIterator;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.NoSuchTableException;
import org.dbunit.dataset.filter.ITableFilter;
import org.dbunit.dataset.filter.SequenceTableFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KISSFilteredDataSet extends KISSAbstractDataSet {

	/**
	 * Logger for this class
	 */
	private static final Logger logger = LoggerFactory.getLogger(KISSFilteredDataSet.class);

	private final IDataSet _dataSet;
	private final ITableFilter _filter;

	/**
	 * Creates a FilteredDataSet that decorates the specified dataset and
	 * exposes only the specified tables using {@link SequenceTableFilter} as
	 * filtering strategy.
	 * @throws AmbiguousTableNameException If the given tableNames array contains ambiguous names
	 */
	public KISSFilteredDataSet(String[] tableNames, IDataSet dataSet) throws AmbiguousTableNameException {
		_filter = new SequenceTableFilter(tableNames, dataSet.isCaseSensitiveTableNames());
		_dataSet = dataSet;
	}

	/**
	 * Creates a FilteredDataSet that decorates the specified dataset and
	 * exposes only the tables allowed by the specified filter.
	 *
	 * @param dataSet the filtered dataset
	 * @param filter the filtering strategy
	 */
	public KISSFilteredDataSet(ITableFilter filter, IDataSet dataSet) {
		_dataSet = dataSet;
		_filter = filter;
	}

	////////////////////////////////////////////////////////////////////////////
	// AbstractDataSet class

	protected ITableIterator createIterator(boolean reversed) throws DataSetException {
		if (logger.isDebugEnabled())
			logger.debug("createIterator(reversed={}) - start", String.valueOf(reversed));

		return _filter.iterator(_dataSet, reversed);
	}

	////////////////////////////////////////////////////////////////////////////
	// IDataSet interface

	public String[] getTableNames() throws DataSetException {
		return _filter.getTableNames(_dataSet);
	}

	public ITableMetaData getTableMetaData(String tableName) throws DataSetException {
		if (!_filter.accept(tableName)) {
			throw new NoSuchTableException(tableName);
		}

		return _dataSet.getTableMetaData(tableName);
	}

	public ITable getTable(String tableName) throws DataSetException {
		logger.debug("getTable(tableName={}) - start", tableName);

		if (!_filter.accept(tableName)) {
			throw new NoSuchTableException(tableName);
		}

		return _dataSet.getTable(tableName);
	}
}
