package org.duckdb;

import static java.lang.System.lineSeparator;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class DuckDBDatabaseMetaData implements DatabaseMetaData {

    private static final int QUERY_SB_DEFAULT_CAPACITY = 512;
    private static final String TRAILING_COMMA = ", ";
    DuckDBConnection conn;

    public DuckDBDatabaseMetaData(DuckDBConnection conn) {
        this.conn = conn;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return JdbcUtils.unwrap(this, iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return conn.url;
    }

    @Override
    public String getUserName() throws SQLException {
        return "";
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return conn.isReadOnly();
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "DuckDB";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("PRAGMA version")) {
            rs.next();
            String result = rs.getString(1);
            return result;
        }
    }

    @Override
    public String getDriverName() throws SQLException {
        return "DuckDBJ";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return "1.0";
    }

    @Override
    public int getDriverMajorVersion() {
        return 1;
    }

    @Override
    public int getDriverMinorVersion() {
        return 0;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return true;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "\"";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        try (Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery("SELECT keyword_name FROM duckdb_keywords()")) {
            StringBuilder sb = new StringBuilder();
            while (rs.next()) {
                sb.append(rs.getString(1));
                sb.append(',');
            }
            return sb.toString();
        }
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        try (Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery("SELECT DISTINCT function_name FROM duckdb_functions() "
                                                   + "WHERE parameter_types[1] ='DECIMAL'"
                                                   + "OR parameter_types[1] ='DOUBLE'"
                                                   + "OR parameter_types[1] ='SMALLINT'"
                                                   + "OR parameter_types[1] = 'BIGINT'")) {
            StringBuilder sb = new StringBuilder();
            while (rs.next()) {
                sb.append(rs.getString(1));
                sb.append(',');
            }
            return sb.toString();
        }
    }

    @Override
    public String getStringFunctions() throws SQLException {
        try (Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery(
                 "SELECT DISTINCT function_name FROM duckdb_functions() WHERE parameter_types[1] = 'VARCHAR'")) {
            StringBuilder sb = new StringBuilder();
            while (rs.next()) {
                sb.append(rs.getString(1));
                sb.append(',');
            }
            return sb.toString();
        }
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        try (Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery(
                 "SELECT DISTINCT function_name FROM duckdb_functions() WHERE length(parameter_types) = 0")) {
            StringBuilder sb = new StringBuilder();
            while (rs.next()) {
                sb.append(rs.getString(1));
                sb.append(',');
            }
            return sb.toString();
        }
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        try (Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery(
                 "SELECT DISTINCT function_name FROM duckdb_functions() WHERE parameter_types[1] LIKE 'TIME%'")) {
            StringBuilder sb = new StringBuilder();
            while (rs.next()) {
                sb.append(rs.getString(1));
                sb.append(',');
            }
            return sb.toString();
        }
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "schema";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "catalog";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {

        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_REPEATABLE_READ;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return level < Connection.TRANSACTION_SERIALIZABLE;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        Statement statement = conn.createStatement();
        statement.closeOnCompletion();
        return statement.executeQuery(
            "SELECT DISTINCT catalog_name AS 'TABLE_CAT' FROM information_schema.schemata ORDER BY \"TABLE_CAT\"");
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        Statement statement = conn.createStatement();
        statement.closeOnCompletion();
        return statement.executeQuery(
            "SELECT schema_name AS 'TABLE_SCHEM', catalog_name AS 'TABLE_CATALOG' FROM information_schema.schemata ORDER BY \"TABLE_CATALOG\", \"TABLE_SCHEM\"");
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        StringBuilder sb = new StringBuilder(QUERY_SB_DEFAULT_CAPACITY);
        sb.append("SELECT").append(lineSeparator());
        sb.append("schema_name AS 'TABLE_SCHEM'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("catalog_name AS 'TABLE_CATALOG'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("FROM information_schema.schemata").append(lineSeparator());
        sb.append("WHERE TRUE").append(lineSeparator());
        boolean hasCatalogParam = appendEqualsQual(sb, "catalog_name", catalog);
        boolean hasSchemaParam = appendLikeQual(sb, "schema_name", schemaPattern);
        sb.append("ORDER BY").append(lineSeparator());
        sb.append("\"TABLE_CATALOG\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("\"TABLE_SCHEM\"").append(lineSeparator());

        PreparedStatement ps = conn.prepareStatement(sb.toString());
        int paramIdx = 0;
        if (hasCatalogParam) {
            ps.setString(++paramIdx, catalog);
        }
        if (hasSchemaParam) {
            ps.setString(++paramIdx, schemaPattern);
        }
        ps.closeOnCompletion();
        return ps.executeQuery();
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        String[] tableTypesArray = new String[] {"BASE TABLE", "LOCAL TEMPORARY", "VIEW"};
        StringBuilder stringBuilder = new StringBuilder(128);
        boolean first = true;
        for (String tableType : tableTypesArray) {
            if (!first) {
                stringBuilder.append("\nUNION ALL\n");
            }
            stringBuilder.append("SELECT '");
            stringBuilder.append(tableType);
            stringBuilder.append("'");
            if (first) {
                stringBuilder.append(" AS 'TABLE_TYPE'");
                first = false;
            }
        }
        stringBuilder.append("\nORDER BY TABLE_TYPE");
        Statement statement = conn.createStatement();
        statement.closeOnCompletion();
        return statement.executeQuery(stringBuilder.toString());
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
        throws SQLException {
        StringBuilder sb = new StringBuilder(QUERY_SB_DEFAULT_CAPACITY);

        sb.append("SELECT").append(lineSeparator());
        sb.append("table_catalog AS 'TABLE_CAT'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("table_schema AS 'TABLE_SCHEM'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("table_name AS 'TABLE_NAME'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("table_type AS 'TABLE_TYPE'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("TABLE_COMMENT AS 'REMARKS'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("NULL::VARCHAR AS 'TYPE_CAT'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("NULL::VARCHAR AS 'TYPE_SCHEM'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("NULL::VARCHAR AS 'TYPE_NAME'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("NULL::VARCHAR AS 'SELF_REFERENCING_COL_NAME'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("NULL::VARCHAR AS 'REF_GENERATION'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("FROM information_schema.tables").append(lineSeparator());
        sb.append("WHERE table_name LIKE ? ESCAPE '\\'").append(lineSeparator());
        boolean hasCatalogParam = appendEqualsQual(sb, "table_catalog", catalog);
        boolean hasSchemaParam = appendLikeQual(sb, "table_schema", schemaPattern);

        if (types != null && types.length > 0) {
            sb.append("AND table_type IN (").append(lineSeparator());
            for (int i = 0; i < types.length; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append('?');
            }
            sb.append(')');
        }

        // ordered by TABLE_TYPE, TABLE_CAT, TABLE_SCHEM and TABLE_NAME.
        sb.append("ORDER BY").append(lineSeparator());
        sb.append("table_type").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("table_catalog").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("table_schema").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("table_name").append(lineSeparator());

        PreparedStatement ps = conn.prepareStatement(sb.toString());

        int paramIdx = 1;
        ps.setString(paramIdx++, nullPatternToWildcard(tableNamePattern));

        if (hasCatalogParam) {
            ps.setString(paramIdx++, catalog);
        }
        if (hasSchemaParam) {
            ps.setString(paramIdx++, schemaPattern);
        }

        if (types != null && types.length > 0) {
            for (int i = 0; i < types.length; i++) {
                ps.setString(paramIdx + i, types[i]);
            }
        }
        ps.closeOnCompletion();
        return ps.executeQuery();
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
        throws SQLException {
        StringBuilder sb = new StringBuilder(QUERY_SB_DEFAULT_CAPACITY);
        sb.append("SELECT").append(lineSeparator());
        sb.append("table_catalog AS 'TABLE_CAT'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("table_schema AS 'TABLE_SCHEM'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("table_name AS 'TABLE_NAME'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("column_name as 'COLUMN_NAME'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append(makeDataMap("regexp_replace(c.data_type, '\\(.*\\)', '')", "DATA_TYPE"))
            .append(TRAILING_COMMA)
            .append(lineSeparator());
        sb.append("c.data_type AS 'TYPE_NAME'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("numeric_precision AS 'COLUMN_SIZE'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("NULL AS 'BUFFER_LENGTH'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("numeric_scale AS 'DECIMAL_DIGITS'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("10 AS 'NUM_PREC_RADIX'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("CASE WHEN is_nullable = 'YES' THEN 1 else 0 END AS 'NULLABLE'")
            .append(TRAILING_COMMA)
            .append(lineSeparator());
        sb.append("COLUMN_COMMENT as 'REMARKS'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("column_default AS 'COLUMN_DEF'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("NULL AS 'SQL_DATA_TYPE'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("NULL AS 'SQL_DATETIME_SUB'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("NULL AS 'CHAR_OCTET_LENGTH'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("ordinal_position AS 'ORDINAL_POSITION'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("is_nullable AS 'IS_NULLABLE'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("NULL AS 'SCOPE_CATALOG'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("NULL AS 'SCOPE_SCHEMA'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("NULL AS 'SCOPE_TABLE'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("NULL AS 'SOURCE_DATA_TYPE'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("'' AS 'IS_AUTOINCREMENT'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("'' AS 'IS_GENERATEDCOLUMN'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("FROM information_schema.columns c").append(lineSeparator());
        sb.append("WHERE TRUE").append(lineSeparator());
        boolean hasCatalogParam = appendEqualsQual(sb, "table_catalog", catalog);
        boolean hasSchemaParam = appendLikeQual(sb, "table_schema", schemaPattern);
        sb.append("AND table_name LIKE ? ESCAPE '\\'").append(lineSeparator());
        sb.append("AND column_name LIKE ? ESCAPE '\\'").append(lineSeparator());
        sb.append("ORDER BY").append(lineSeparator());
        sb.append("\"TABLE_CAT\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("\"TABLE_SCHEM\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("\"TABLE_NAME\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("\"ORDINAL_POSITION\"").append(lineSeparator());

        PreparedStatement ps = conn.prepareStatement(sb.toString());

        int paramIdx = 1;
        if (hasCatalogParam) {
            ps.setString(paramIdx++, catalog);
        }
        if (hasSchemaParam) {
            ps.setString(paramIdx++, schemaPattern);
        }
        ps.setString(paramIdx++, nullPatternToWildcard(tableNamePattern));
        ps.setString(paramIdx++, nullPatternToWildcard(columnNamePattern));
        ps.closeOnCompletion();
        return ps.executeQuery();
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
        throws SQLException {
        throw new SQLFeatureNotSupportedException("getColumnPrivileges");
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
        throws SQLException {
        throw new SQLFeatureNotSupportedException("getTablePrivileges");
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
        throws SQLException {
        Statement statement = conn.createStatement();
        statement.closeOnCompletion();
        return statement.executeQuery("SELECT NULL WHERE FALSE");
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
                                         String columnNamePattern) throws SQLException {
        Statement statement = conn.createStatement();
        statement.closeOnCompletion();
        return statement.executeQuery("SELECT NULL WHERE FALSE");
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
        throws SQLException {
        throw new SQLFeatureNotSupportedException("getBestRowIdentifier");
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("getVersionColumns");
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        StringBuilder sb = new StringBuilder(QUERY_SB_DEFAULT_CAPACITY);
        sb.append("WITH constraint_columns AS (").append(lineSeparator());
        sb.append("SELECT").append(lineSeparator());
        sb.append("database_name AS \"TABLE_CAT\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("schema_name AS \"TABLE_SCHEM\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("table_name AS \"TABLE_NAME\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("unnest(constraint_column_names) AS \"COLUMN_NAME\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("NULL::VARCHAR AS \"PK_NAME\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("FROM duckdb_constraints").append(lineSeparator());
        sb.append("WHERE constraint_type = 'PRIMARY KEY'").append(lineSeparator());
        boolean hasCatalogParam = appendEqualsQual(sb, "database_name", catalog);
        boolean hasSchemaParam = appendEqualsQual(sb, "schema_name", schema);
        sb.append("AND table_name = ?").append(lineSeparator());
        sb.append(")").append(lineSeparator());
        sb.append("SELECT").append(lineSeparator());
        sb.append("\"TABLE_CAT\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("\"TABLE_SCHEM\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("\"TABLE_NAME\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("\"COLUMN_NAME\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("CAST(ROW_NUMBER() OVER (").append(lineSeparator());
        sb.append("PARTITION BY").append(lineSeparator());
        sb.append("\"TABLE_CAT\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("\"TABLE_SCHEM\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("\"TABLE_NAME\"").append(lineSeparator());
        sb.append(") AS INT) AS \"KEY_SEQ\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("\"PK_NAME\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("FROM constraint_columns").append(lineSeparator());
        sb.append("ORDER BY").append(lineSeparator());
        sb.append("\"TABLE_CAT\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("\"TABLE_SCHEM\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("\"TABLE_NAME\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("\"KEY_SEQ\"").append(lineSeparator());

        PreparedStatement ps = conn.prepareStatement(sb.toString());
        int paramIdx = 1;

        if (hasCatalogParam) {
            ps.setString(paramIdx++, catalog);
        }
        if (hasSchemaParam) {
            ps.setString(paramIdx++, schema);
        }
        ps.setString(paramIdx++, table);
        ps.closeOnCompletion();
        return ps.executeQuery();
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("getImportedKeys");
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("getExportedKeys");
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable)
        throws SQLException {
        throw new SQLFeatureNotSupportedException("getCrossReference");
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        throw new SQLFeatureNotSupportedException("getTypeInfo");
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
        throws SQLException {
        StringBuilder sb = new StringBuilder(QUERY_SB_DEFAULT_CAPACITY);
        sb.append("SELECT").append(lineSeparator());
        sb.append("database_name AS 'TABLE_CAT'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("schema_name AS 'TABLE_SCHEM'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("table_name AS 'TABLE_NAME'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("index_name AS 'INDEX_NAME'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("CASE WHEN is_unique THEN 0 ELSE 1 END AS 'NON_UNIQUE'")
            .append(TRAILING_COMMA)
            .append(lineSeparator());
        sb.append("NULL AS 'TYPE'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("NULL AS 'ORDINAL_POSITION'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("NULL AS 'COLUMN_NAME'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("NULL AS 'ASC_OR_DESC'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("NULL AS 'CARDINALITY'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("NULL AS 'PAGES'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("NULL AS 'FILTER_CONDITION'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("FROM duckdb_indexes()").append(lineSeparator());
        sb.append("WHERE TRUE").append(lineSeparator());
        boolean hasCatalogParam = appendEqualsQual(sb, "database_name", catalog);
        boolean hasSchemaParam = appendEqualsQual(sb, "schema_name", schema);
        sb.append("AND table_name = ?").append(lineSeparator());
        if (unique) {
            sb.append("AND is_unique = TRUE").append(lineSeparator());
        }
        sb.append("ORDER BY").append(lineSeparator());
        sb.append("\"TABLE_CAT\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("\"TABLE_SCHEM\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("\"TABLE_NAME\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("\"NON_UNIQUE\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("\"INDEX_NAME\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("\"ORDINAL_POSITION\"").append(lineSeparator());

        PreparedStatement ps = conn.prepareStatement(sb.toString());
        int paramIdx = 1;
        if (hasCatalogParam) {
            ps.setString(paramIdx++, catalog);
        }
        if (hasSchemaParam) {
            ps.setString(paramIdx++, schema);
        }
        ps.setString(paramIdx++, table);
        ps.closeOnCompletion();
        return ps.executeQuery();
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException("ownUpdatesAreVisible");
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException("ownDeletesAreVisible");
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException("ownInsertsAreVisible");
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException("othersUpdatesAreVisible");
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException("othersDeletesAreVisible");
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException("othersInsertsAreVisible");
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException("updatesAreDetected");
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException("deletesAreDetected");
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        throw new SQLFeatureNotSupportedException("insertsAreDetected");
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
        throws SQLException {
        throw new SQLFeatureNotSupportedException("getUDTs");
    }

    @Override
    public Connection getConnection() throws SQLException {
        return conn;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSuperTypes");
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSuperTables");
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("getAttributes");
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException("getResultSetHoldability");
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 1;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 1;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        throw new SQLFeatureNotSupportedException("getSQLStateType");
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        throw new SQLFeatureNotSupportedException("locatorsUpdateCopy");
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        throw new SQLFeatureNotSupportedException("getRowIdLifetime");
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        throw new SQLFeatureNotSupportedException("autoCommitFailureClosesAllResultSets");
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        throw new SQLFeatureNotSupportedException("getClientInfoProperties");
    }

    /**
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        <code>null</code> means that the catalog name should not be used to
     * narrow the search
     * @param schemaPattern a schema name pattern; must match the schema name
     *        as it is stored in the database; "" retrieves those without a
     * schema; <code>null</code> means that the schema name should not be used to
     * narrow the search
     * @param functionNamePattern a function name pattern; must match the
     *        function name as it is stored in the database
     * FUNCTION_CAT String => function catalog (may be null)
     * FUNCTION_SCHEM String => function schema (may be null)
     * FUNCTION_NAME String => function name. This is the name used to invoke the
     * function REMARKS String => explanatory comment on the function
     * FUNCTION_TYPE short => kind of function:
     *  - functionResultUnknown - Cannot determine if a return value or table will
     * be returned
     *  - functionNoTable- Does not return a table
     *  - functionReturnsTable - Returns a table
     * SPECIFIC_NAME String => the name which uniquely identifies this function
     * within its schema. This is a user specified, or DBMS generated, name that
     * may be different then the FUNCTION_NAME for example with overload functions
     */
    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
        throws SQLException {
        StringBuilder sb = new StringBuilder(QUERY_SB_DEFAULT_CAPACITY);
        sb.append("SELECT").append(lineSeparator());
        sb.append("NULL as 'FUNCTION_CAT'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("function_name as 'FUNCTION_NAME'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("schema_name as 'FUNCTION_SCHEM'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("description as 'REMARKS'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("CASE function_type").append(lineSeparator());
        sb.append("WHEN 'table' THEN ").append(functionReturnsTable).append(lineSeparator());
        sb.append("WHEN 'table_macro' THEN ").append(functionReturnsTable).append(lineSeparator());
        sb.append("ELSE ").append(functionNoTable).append(lineSeparator());
        sb.append("END as 'FUNCTION_TYPE'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("FROM duckdb_functions()").append(lineSeparator());
        sb.append("WHERE TRUE").append(lineSeparator());
        boolean hasCatalogParam = appendEqualsQual(sb, "database_name", catalog);
        boolean hasSchemaParam = appendLikeQual(sb, "schema_name", schemaPattern);
        sb.append("AND function_name LIKE ? ESCAPE '\\'").append(lineSeparator());
        sb.append("ORDER BY").append(lineSeparator());
        sb.append("\"FUNCTION_CAT\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("\"FUNCTION_SCHEM\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("\"FUNCTION_NAME\"").append(lineSeparator());

        PreparedStatement ps = conn.prepareStatement(sb.toString());
        int paramIdx = 1;
        if (hasCatalogParam) {
            ps.setString(paramIdx++, catalog);
        }
        if (hasSchemaParam) {
            ps.setString(paramIdx++, schemaPattern);
        }
        ps.setString(paramIdx++, nullPatternToWildcard(functionNamePattern));
        ps.closeOnCompletion();
        return ps.executeQuery();
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
                                        String columnNamePattern) throws SQLException {
        Statement statement = conn.createStatement();
        statement.closeOnCompletion();
        return statement.executeQuery("SELECT NULL WHERE FALSE");
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
                                      String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("getPseudoColumns");
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        throw new SQLFeatureNotSupportedException("generatedKeyAlwaysReturned");
    }

    static String dataMap;
    static {
        dataMap = makeCase(
            Arrays.stream(DuckDBColumnType.values())
                .collect(Collectors.toMap(ty -> ty.name().replace("_", " "), DuckDBResultSetMetaData::type_to_int)));
    }

    private static <T> String makeCase(Map<String, T> values) {
        return values.entrySet()
            .stream()
            .map(ty -> {
                T value = ty.getValue();
                return String.format("WHEN '%s' THEN %s ", ty.getKey(),
                                     value instanceof String ? String.format("'%s'", value) : value);
            })
            .collect(Collectors.joining());
    }

    /**
     * @param srcColumnName
     * @param destColumnName
     * @return
     * @see DuckDBResultSetMetaData#type_to_int(DuckDBColumnType)
     */
    private static String makeDataMap(String srcColumnName, String destColumnName) {
        return String.format("CASE %s %s ELSE %d END as %s", srcColumnName, dataMap, Types.OTHER, destColumnName);
    }

    private static boolean appendEqualsQual(StringBuilder sb, String colName, String value) {
        // catalog - a catalog name; must match the catalog name as it is stored in
        // the database;
        // "" retrieves those without a catalog;
        // null means that the catalog name should not be used to narrow the search
        boolean hasParam = false;
        if (value != null) {
            sb.append("AND ");
            sb.append(colName);
            if (value.isEmpty()) {
                sb.append(" IS NULL");
            } else {
                sb.append(" = ?");
                hasParam = true;
            }
            sb.append(lineSeparator());
        }
        return hasParam;
    }

    private static boolean appendLikeQual(StringBuilder sb, String colName, String pattern) {
        // schemaPattern - a schema name pattern; must match the schema name as it
        // is stored in the database;
        // "" retrieves those without a schema;
        // null means that the schema name should not be used to narrow the search
        boolean hasParam = false;
        if (pattern != null) {
            sb.append("AND ");
            sb.append(colName);
            if (pattern.isEmpty()) {
                sb.append(" IS NULL");
            } else {
                sb.append(" LIKE ? ESCAPE '\\'");
                hasParam = true;
            }
            sb.append(lineSeparator());
        }
        return hasParam;
    }

    private static String nullPatternToWildcard(String pattern) {
        // tableNamePattern - a table name pattern; must match the table name as it
        // is stored in the database
        // columnNamePattern - a column name pattern; must match the table name as it
        // is stored in the database
        if (pattern == null) {
            // non-standard behavior.
            return "%";
        }
        return pattern;
    }
}
