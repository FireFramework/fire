package com.zto.fire.flink.core.bean;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.*;
import java.util.stream.IntStream;

import static org.apache.flink.table.api.DataTypes.*;
import static org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * flink表模式，支持序列化
 * @author ChengLong 2020年1月16日 16:56:23
 */
public class FlinkTableSchema implements Serializable {

	private static final String ATOMIC_TYPE_FIELD_NAME = "f0";

	private final String[] fieldNames;

	private final DataType[] fieldDataTypes;

	private final Map<String, Integer> fieldNameToIndex;

	public FlinkTableSchema(TableSchema schema) {
		this(schema.getFieldNames(), schema.getFieldDataTypes());
	}

	private FlinkTableSchema(String[] fieldNames, DataType[] fieldDataTypes) {
		this.fieldNames = Preconditions.checkNotNull(fieldNames);
		this.fieldDataTypes = Preconditions.checkNotNull(fieldDataTypes);

		if (fieldNames.length != fieldDataTypes.length) {
			throw new TableException(
				"Number of field names and field data types must be equal.\n" +
					"Number of names is " + fieldNames.length + ", number of data types is " + fieldDataTypes.length + ".\n" +
					"List of field names: " + Arrays.toString(fieldNames) + "\n" +
					"List of field data types: " + Arrays.toString(fieldDataTypes));
		}

		// validate and create name to index mapping
		fieldNameToIndex = new HashMap<>();
		final Set<String> duplicateNames = new HashSet<>();
		final Set<String> uniqueNames = new HashSet<>();
		for (int i = 0; i < fieldNames.length; i++) {
			// check for null
			Preconditions.checkNotNull(fieldDataTypes[i]);
			final String fieldName = Preconditions.checkNotNull(fieldNames[i]);

			// collect indices
			fieldNameToIndex.put(fieldName, i);

			// check uniqueness of field names
			if (uniqueNames.contains(fieldName)) {
				duplicateNames.add(fieldName);
			} else {
				uniqueNames.add(fieldName);
			}
		}
		if (!duplicateNames.isEmpty()) {
			throw new TableException(
				"Field names must be unique.\n" +
					"List of duplicate fields: " + duplicateNames.toString() + "\n" +
					"List of all fields: " + Arrays.toString(fieldNames));
		}
	}

	/**
	 * @deprecated Use the {@link Builder} instead.
	 */
	@Deprecated
	public FlinkTableSchema(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		this(fieldNames, fromLegacyInfoToDataType(fieldTypes));
	}

	/**
	 * Returns a deep copy of the table schema.
	 */
	public FlinkTableSchema copy() {
		return new FlinkTableSchema(fieldNames.clone(), fieldDataTypes.clone());
	}

	/**
	 * Returns all field data types as an array.
	 */
	public DataType[] getFieldDataTypes() {
		return fieldDataTypes;
	}

	/**
	 * @deprecated This method will be removed in future versions as it uses the old type system. It
	 *             is recommended to use {@link #getFieldDataTypes()} instead which uses the new type
	 *             system based on {@link DataTypes}. Please make sure to use either the old or the new
	 *             type system consistently to avoid unintended behavior. See the website documentation
	 *             for more information.
	 */
	@Deprecated
	public TypeInformation<?>[] getFieldTypes() {
		return fromDataTypeToLegacyInfo(fieldDataTypes);
	}

	/**
	 * Returns the specified data type for the given field index.
	 *
	 * @param fieldIndex the index of the field
	 */
	public Optional<DataType> getFieldDataType(int fieldIndex) {
		if (fieldIndex < 0 || fieldIndex >= fieldDataTypes.length) {
			return Optional.empty();
		}
		return Optional.of(fieldDataTypes[fieldIndex]);
	}

	/**
	 * @deprecated This method will be removed in future versions as it uses the old type system. It
	 *             is recommended to use {@link #getFieldDataType(int)} instead which uses the new type
	 *             system based on {@link DataTypes}. Please make sure to use either the old or the new
	 *             type system consistently to avoid unintended behavior. See the website documentation
	 *             for more information.
	 */
	@Deprecated
	public Optional<TypeInformation<?>> getFieldType(int fieldIndex) {
		return getFieldDataType(fieldIndex)
			.map(TypeConversions::fromDataTypeToLegacyInfo);
	}

	/**
	 * Returns the specified data type for the given field name.
	 *
	 * @param fieldName the name of the field
	 */
	public Optional<DataType> getFieldDataType(String fieldName) {
		if (fieldNameToIndex.containsKey(fieldName)) {
			return Optional.of(fieldDataTypes[fieldNameToIndex.get(fieldName)]);
		}
		return Optional.empty();
	}

	/**
	 * @deprecated This method will be removed in future versions as it uses the old type system. It
	 *             is recommended to use {@link #getFieldDataType(String)} instead which uses the new type
	 *             system based on {@link DataTypes}. Please make sure to use either the old or the new
	 *             type system consistently to avoid unintended behavior. See the website documentation
	 *             for more information.
	 */
	@Deprecated
	public Optional<TypeInformation<?>> getFieldType(String fieldName) {
		return getFieldDataType(fieldName)
			.map(TypeConversions::fromDataTypeToLegacyInfo);
	}

	/**
	 * Returns the number of fields.
	 */
	public int getFieldCount() {
		return fieldNames.length;
	}

	/**
	 * Returns all field names as an array.
	 */
	public String[] getFieldNames() {
		return fieldNames;
	}

	/**
	 * Returns the specified name for the given field index.
	 *
	 * @param fieldIndex the index of the field
	 */
	public Optional<String> getFieldName(int fieldIndex) {
		if (fieldIndex < 0 || fieldIndex >= fieldNames.length) {
			return Optional.empty();
		}
		return Optional.of(fieldNames[fieldIndex]);
	}

	/**
	 * Converts a table schema into a (nested) data type describing a {@link DataTypes#ROW(Field...)}.
	 */
	public DataType toRowDataType() {
		final Field[] fields = IntStream.range(0, fieldDataTypes.length)
			.mapToObj(i -> FIELD(fieldNames[i], fieldDataTypes[i]))
			.toArray(Field[]::new);
		return ROW(fields);
	}

	/**
	 * @deprecated Use {@link #toRowDataType()} instead.
	 */
	@Deprecated
	@SuppressWarnings("unchecked")
	public TypeInformation<Row> toRowType() {
		return (TypeInformation<Row>) fromDataTypeToLegacyInfo(toRowDataType());
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("root\n");
		for (int i = 0; i < fieldNames.length; i++) {
			sb.append(" |-- ").append(fieldNames[i]).append(": ").append(fieldDataTypes[i]).append('\n');
		}
		return sb.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		FlinkTableSchema schema = (FlinkTableSchema) o;
		return Arrays.equals(fieldNames, schema.fieldNames) &&
			Arrays.equals(fieldDataTypes, schema.fieldDataTypes);
	}

	@Override
	public int hashCode() {
		int result = Arrays.hashCode(fieldNames);
		result = 31 * result + Arrays.hashCode(fieldDataTypes);
		return result;
	}

	/**
	 * Creates a table schema from a {@link TypeInformation} instance. If the type information is
	 * a {@link CompositeType}, the field names and types for the composite type are used to
	 * construct the {@link FlinkTableSchema} instance. Otherwise, a table schema with a single field
	 * is created. The field name is "f0" and the field type the provided type.
	 *
	 * @param typeInfo The {@link TypeInformation} from which the table schema is generated.
	 * @return The table schema that was generated from the given {@link TypeInformation}.
	 *
	 * @deprecated This method will be removed soon. Use {@link DataTypes} to declare types.
	 */
	@Deprecated
	public static FlinkTableSchema fromTypeInfo(TypeInformation<?> typeInfo) {
		if (typeInfo instanceof CompositeType<?>) {
			final CompositeType<?> compositeType = (CompositeType<?>) typeInfo;
			// get field names and types from composite type
			final String[] fieldNames = compositeType.getFieldNames();
			final TypeInformation<?>[] fieldTypes = new TypeInformation[fieldNames.length];
			for (int i = 0; i < fieldTypes.length; i++) {
				fieldTypes[i] = compositeType.getTypeAt(i);
			}
			return new FlinkTableSchema(fieldNames, fieldTypes);
		} else {
			// create table schema with a single field named "f0" of the given type.
			return new FlinkTableSchema(
				new String[]{ATOMIC_TYPE_FIELD_NAME},
				new TypeInformation<?>[]{typeInfo});
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Builder for creating a {@link FlinkTableSchema}.
	 */
	public static class Builder {

		private List<String> fieldNames;

		private List<DataType> fieldDataTypes;

		public Builder() {
			fieldNames = new ArrayList<>();
			fieldDataTypes = new ArrayList<>();
		}

		/**
		 * Add a field with name and data type.
		 *
		 * <p>The call order of this method determines the order of fields in the schema.
		 */
		public Builder field(String name, DataType dataType) {
			Preconditions.checkNotNull(name);
			Preconditions.checkNotNull(dataType);
			fieldNames.add(name);
			fieldDataTypes.add(dataType);
			return this;
		}

		/**
		 * Add an array of fields with names and data types.
		 *
		 * <p>The call order of this method determines the order of fields in the schema.
		 */
		public Builder fields(String[] names, DataType[] dataTypes) {
			Preconditions.checkNotNull(names);
			Preconditions.checkNotNull(dataTypes);

			fieldNames.addAll(Arrays.asList(names));
			fieldDataTypes.addAll(Arrays.asList(dataTypes));
			return this;
		}

		/**
		 * @deprecated This method will be removed in future versions as it uses the old type system. It
		 *             is recommended to use {@link #field(String, DataType)} instead which uses the new type
		 *             system based on {@link DataTypes}. Please make sure to use either the old or the new
		 *             type system consistently to avoid unintended behavior. See the website documentation
		 *             for more information.
		 */
		@Deprecated
		public Builder field(String name, TypeInformation<?> typeInfo) {
			return field(name, fromLegacyInfoToDataType(typeInfo));
		}

		/**
		 * Returns a {@link FlinkTableSchema} instance.
		 */
		public FlinkTableSchema build() {
			return new FlinkTableSchema(
				fieldNames.toArray(new String[0]),
				fieldDataTypes.toArray(new DataType[0]));
		}
	}
}
