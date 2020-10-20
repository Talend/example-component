package com.examplepostgres.talend.components.source;

import java.sql.Statement;
import java.sql.Types;
import java.util.Date;
import java.util.stream.IntStream;
import java.io.Serializable;
import java.math.BigDecimal;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import static java.sql.ResultSetMetaData.columnNoNulls;
import static org.talend.sdk.component.api.record.Schema.Type.*;

import com.examplepostgres.talend.components.service.ExampleComponentService;

@Documentation("TODO fill the documentation for this source")
public class ExamplePostgresSource implements Serializable {
    private final ExamplePostgresMapperConfiguration configuration;
    private final ExampleComponentService service;
    private final RecordBuilderFactory builderFactory;
    private final Connection connection;
    private ResultSet resultSet;
    private transient Schema schema;
    public ExamplePostgresSource(@Option("configuration") final ExamplePostgresMapperConfiguration configuration,
                        final ExampleComponentService service,
                        final RecordBuilderFactory builderFactory) {
        this.configuration = configuration;
        this.service = service;
        this.builderFactory = builderFactory;
        try {
            this.connection = configuration.getDataset().getDatastore().getConnection();
        } catch(Exception e) {
            throw new IllegalStateException(e.getMessage());
        }
    }

    @PostConstruct
    public void init() {
        // this method will be executed once for the whole component execution,
        // this is where you can establish a connection for instance
        try {
            Statement stmt = connection.createStatement();
            String table = configuration.getDataset().getTable();
            resultSet = stmt.executeQuery("select * from " + table);
        } catch(SQLException e) {
            throw new IllegalStateException(e.getErrorCode() + " " + e.getMessage());
        }

    }

    @Producer
    public Record next() {
        // this is the method allowing you to go through the dataset associated
        // to the component configuration
        //
        // return null means the dataset has no more data to go through
        // you can use the builderFactory to create a new Record.
        try {
            if (!resultSet.next())
                return null;
            
            final ResultSetMetaData metaData = resultSet.getMetaData();
            if (schema == null) {
                final Schema.Builder schemaBuilder = builderFactory.newSchemaBuilder(RECORD);
                IntStream.rangeClosed(1, metaData.getColumnCount()).forEach(index -> addField(schemaBuilder, metaData, index));
                schema = schemaBuilder.build();
            }

            final Record.Builder recordBuilder = builderFactory.newRecordBuilder(schema);
            IntStream.rangeClosed(1, metaData.getColumnCount()).forEach(index -> addColumn(recordBuilder, metaData, index));

            return recordBuilder.build();
        } catch(SQLException e) {
            throw new IllegalStateException(e.getMessage());
        }
    }

    @PreDestroy
    public void release() {
        // this is the symmetric method of the init() one,
        // release potential connections you created or data you cached
        try {
            connection.close();
        } catch(Exception e) {}
    }


    private void addField(final Schema.Builder builder, final ResultSetMetaData metaData, final int columnIndex) {
        try {
            final String javaType = metaData.getColumnClassName(columnIndex);
            final int sqlType = metaData.getColumnType(columnIndex);
            final Schema.Entry.Builder entryBuilder = builderFactory.newEntryBuilder();
            entryBuilder.withName(metaData.getColumnName(columnIndex))
                    .withNullable(metaData.isNullable(columnIndex) != columnNoNulls);
            switch (sqlType) {
                case Types.SMALLINT:
                case Types.TINYINT:
                case Types.INTEGER:
                    if (javaType.equals(Integer.class.getName())) {
                        builder.withEntry(entryBuilder.withType(INT).build());
                    } else {
                        builder.withEntry(entryBuilder.withType(LONG).build());
                    }
                    break;
                case Types.FLOAT:
                case Types.DECIMAL:
                case Types.NUMERIC:
                case Types.REAL:
                    builder.withEntry(entryBuilder.withType(FLOAT).build());
                    break;
                case Types.DOUBLE:
                    builder.withEntry(entryBuilder.withType(DOUBLE).build());
                    break;
                case Types.BOOLEAN:
                    builder.withEntry(entryBuilder.withType(BOOLEAN).build());
                    break;
                case Types.TIME:
                case Types.DATE:
                case Types.TIMESTAMP:
                    builder.withEntry(entryBuilder.withType(DATETIME).build());
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    builder.withEntry(entryBuilder.withType(BYTES).build());
                    break;
                case Types.BIGINT:
                    builder.withEntry(entryBuilder.withType(LONG).build());
                    break;
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CHAR:
                default:
                    builder.withEntry(entryBuilder.withType(STRING).build());
                    break;
            }
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }
    }


    private void addColumn(final Record.Builder builder, final ResultSetMetaData metaData, final int columnIndex) {
        try {
            final String javaType = metaData.getColumnClassName(columnIndex);
            final String integerType = Integer.class.getName();
            final int sqlType = metaData.getColumnType(columnIndex);
            final Object value = resultSet.getObject(columnIndex);
            final Schema.Entry.Builder entryBuilder = builderFactory.newEntryBuilder();
            entryBuilder.withName(metaData.getColumnName(columnIndex))
                    .withNullable(metaData.isNullable(columnIndex) != columnNoNulls);
            switch (sqlType) {
                case Types.SMALLINT:
                case Types.TINYINT:
                case Types.INTEGER:
                    if (value != null) {
                        if (javaType.equals(integerType)) {
                            builder.withInt(entryBuilder.withType(INT).build(), (Integer) value);
                        } else {
                            builder.withLong(entryBuilder.withType(LONG).build(), (Long) value);
                        }
                    }
                    break;
                case Types.FLOAT:
                case Types.DECIMAL:
                case Types.NUMERIC:
                case Types.REAL:
                    if (value != null) {
                        builder.withFloat(entryBuilder.withType(FLOAT).build(), javaType.equals("java.math.BigDecimal") ? ((BigDecimal) value).floatValue() : (Float) value);
                    }
                    break;
                case Types.DOUBLE:
                    if (value != null) {
                        builder.withDouble(entryBuilder.withType(DOUBLE).build(), (Double) value);
                    }
                    break;
                case Types.BOOLEAN:
                    if (value != null) {
                        builder.withBoolean(entryBuilder.withType(BOOLEAN).build(), (Boolean) value);
                    }
                    break;

                case Types.DATE:
                    builder.withDateTime(entryBuilder.withType(DATETIME).build(),
                            value == null ? null : new Date(((java.sql.Date) value).getTime()));
                    break;
                case Types.TIME:
                    builder.withDateTime(entryBuilder.withType(DATETIME).build(),
                            value == null ? null : new Date(((java.sql.Time) value).getTime()));
                    break;
                case Types.TIMESTAMP:
                    builder.withDateTime(entryBuilder.withType(DATETIME).build(),
                            value == null ? null : new Date(((java.sql.Timestamp) value).getTime()));
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    builder.withBytes(entryBuilder.withType(BYTES).build(), value == null ? null : (byte[]) value);
                    break;
                case Types.BIGINT:
                    builder.withLong(entryBuilder.withType(LONG).build(), (Long) value);
                    break;
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CHAR:
                default:
                    builder.withString(entryBuilder.withType(STRING).build(), value == null ? null : String.valueOf(value));
                    break;
            }
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}