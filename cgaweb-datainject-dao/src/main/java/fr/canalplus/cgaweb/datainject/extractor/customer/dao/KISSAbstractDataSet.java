package fr.canalplus.cgaweb.datainject.extractor.customer.dao;

import org.dbunit.dataset.AbstractDataSet;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableIterator;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.NoSuchTableException;
import org.dbunit.dataset.OrderedTableNameMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class KISSAbstractDataSet implements IDataSet {
	//TODO (matthias) Use a DataSetBuilder PLUS IDataSet to avoid this ugly lazy initialization with loads of protected internals a user must know...

	protected OrderedTableNameMap _orderedTableNameMap;

	/**
	 * Whether or not table names of this dataset are case sensitive.
	 * By default case-sensitivity is set to false for datasets
	 */
	private boolean _caseSensitiveTableNames = false;

	/**
	 * Logger for this class
	 */
	private static final Logger logger = LoggerFactory.getLogger(AbstractDataSet.class);

	/**
	 * Default constructor
	 */
	public KISSAbstractDataSet() {
	}

	/**
	 * Constructor
	 * @param caseSensitiveTableNames Whether or not table names should be case sensitive
	 * @since 2.4
	 */
	public KISSAbstractDataSet(boolean caseSensitiveTableNames) {
		_caseSensitiveTableNames = caseSensitiveTableNames;
	}

	/**
	 * @return <code>true</code> if the case sensitivity of table names is used in this dataset.
	 * @since 2.4
	 */
	public boolean isCaseSensitiveTableNames() {
		return this._caseSensitiveTableNames;
	}

	/**
	 * Creates and returns a new instance of the table names container.
	 * Implementors should use this method to retrieve a map which stores
	 * table names which can be linked with arbitrary objects.
	 * @return a new empty instance of the table names container
	 * @since 2.4
	 */
	protected OrderedTableNameMap createTableNameMap() {
		return new OrderedTableNameMap(this._caseSensitiveTableNames);
	}

	/**
	 * Initializes the tables of this dataset
	 * @throws DataSetException
	 * @since 2.4
	 */
	private void initialize() throws DataSetException {
		logger.debug("initialize() - start");

		if (_orderedTableNameMap != null) {
			logger.debug("The table name map has already been initialized.");
			// already initialized
			return;
		}

		// Gather all tables in the OrderedTableNameMap which also makes the duplicate check
		_orderedTableNameMap = this.createTableNameMap();
		ITableIterator iterator = createIterator(false);
		while (iterator.next()) {
			ITable table = iterator.getTable();
			_orderedTableNameMap.add(table.getTableMetaData().getTableName(), table);
		}
	}

	//    protected ITable[] cloneTables(ITable[] tables)
	//    {
	//        logger.debug("cloneTables(tables={}) - start", tables);
	//
	//        ITable[] clones = new ITable[tables.length];
	//        for (int i = 0; i < tables.length; i++)
	//        {
	//            clones[i] = tables[i];
	//        }
	//        return clones;
	//    }

	/**
	 * Creates an iterator which provides access to all tables of this dataset
	 * @param reversed Whether the created iterator should be a reversed one or not
	 * @return The created {@link ITableIterator}
	 * @throws DataSetException
	 */
	protected abstract ITableIterator createIterator(boolean reversed) throws DataSetException;

	////////////////////////////////////////////////////////////////////////////
	// IDataSet interface

	public String[] getTableNames() throws DataSetException {
		logger.debug("getTableNames() - start");

		initialize();

		return this._orderedTableNameMap.getTableNames();
	}

	public ITableMetaData getTableMetaData(String tableName) throws DataSetException {
		logger.debug("getTableMetaData(tableName={}) - start", tableName);

		return getTable(tableName).getTableMetaData();
	}

	public ITable getTable(String tableName) throws DataSetException {
		logger.debug("getTable(tableName={}) - start", tableName);

		initialize();

		ITable found = (ITable) _orderedTableNameMap.get(tableName);
		if (found != null) {
			return found;
		} else {
			throw new NoSuchTableException(tableName);
		}
	}

	public ITable[] getTables() throws DataSetException {
		logger.debug("getTables() - start");

		initialize();

		return (ITable[]) this._orderedTableNameMap.orderedValues().toArray(new ITable[0]);
	}

	public ITableIterator iterator() throws DataSetException {
		logger.debug("iterator() - start");

		return createIterator(false);
	}

	public ITableIterator reverseIterator() throws DataSetException {
		logger.debug("reverseIterator() - start");

		return createIterator(true);
	}

	////////////////////////////////////////////////////////////////////////////
	// Object class

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("AbstractDataSet[");
		sb.append("_orderedTableNameMap=").append(_orderedTableNameMap);
		sb.append("]");
		return sb.toString();
	}

}
