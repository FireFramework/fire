package org.apache.flink.connector.clickhouse;

import org.apache.flink.connector.clickhouse.internal.AbstractClickHouseInputFormat;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseReadOptions;
import org.apache.flink.connector.clickhouse.util.FilterPushDownHelper;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/** ClickHouse table source. */
public class ClickHouseDynamicTableSource
        implements ScanTableSource,
                SupportsProjectionPushDown,
                SupportsLimitPushDown,
                SupportsFilterPushDown {

    private final ClickHouseReadOptions readOptions;

    private final Properties connectionProperties;

    private final CatalogTable catalogTable;

    private TableSchema physicalSchema;

    private String filterClause;

    private long limit = -1L;

    public ClickHouseDynamicTableSource(
            ClickHouseReadOptions readOptions,
            Properties properties,
            CatalogTable catalogTable,
            TableSchema physicalSchema) {
        this.readOptions = readOptions;
        this.connectionProperties = properties;
        this.catalogTable = catalogTable;
        this.physicalSchema = physicalSchema;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        AbstractClickHouseInputFormat.Builder builder =
                new AbstractClickHouseInputFormat.Builder()
                        .withOptions(readOptions)
                        .withConnectionProperties(connectionProperties)
                        .withFieldNames(physicalSchema.getFieldNames())
                        .withFieldTypes(physicalSchema.getFieldDataTypes())
                        .withRowDataTypeInfo(
                                runtimeProviderContext.createTypeInformation(
                                        physicalSchema.toRowDataType()))
                        .withFilterClause(filterClause)
                        .withLimit(limit);
        return InputFormatProvider.of(builder.build());
    }

    @Override
    public DynamicTableSource copy() {
        ClickHouseDynamicTableSource source =
                new ClickHouseDynamicTableSource(
                        readOptions, connectionProperties, catalogTable, physicalSchema);
        source.filterClause = filterClause;
        source.limit = limit;
        return source;
    }

    @Override
    public String asSummaryString() {
        return "ClickHouse table source";
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        this.filterClause = FilterPushDownHelper.convert(filters);
        return Result.of(new ArrayList<>(filters), new ArrayList<>(filters));
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.physicalSchema = projectSchema(physicalSchema, projectedFields);
    }

    public static boolean containsPhysicalColumnsOnly(TableSchema schema) {
        Preconditions.checkNotNull(schema);
        return schema.getTableColumns().stream().allMatch(TableColumn::isPhysical);
    }

    public static TableSchema projectSchema(TableSchema tableSchema, int[][] projectedFields) {
        Preconditions.checkArgument(containsPhysicalColumnsOnly(tableSchema), "Projection is only supported for physical columns.");
        TableSchema.Builder builder = TableSchema.builder();
        FieldsDataType fields = (FieldsDataType) DataTypeUtils.projectRow(tableSchema.toRowDataType(), projectedFields);
        RowType topFields = (RowType)fields.getLogicalType();

        for(int i = 0; i < topFields.getFieldCount(); ++i) {
            builder.field((String)topFields.getFieldNames().get(i), (DataType)fields.getChildren().get(i));
        }

        return builder.build();
    }
}
