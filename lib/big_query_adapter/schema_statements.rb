module BigQueryAdapter
  # Includes helper methods
  module SchemaStatements
    # Returns an array of table names, for database tables visible on the
    # current connection.
    def tables(_name = nil)
      raw_connection.tables
    end

    # Returns an array of view names defined in the database.
    def views
      []
    end

    # Returns an array of indexes for the given table.
    def indexes(_table_name, _name = nil)
      []
    end

    # Returns an array of Column objects for the table specified by
    # +table_name+.
    # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
    def columns(table_name, _name = nil)
      result = @connection.columns(table_name.to_s)

      result.each_with_object([]) do |field, cols|
        col_name = field.name
        col_sql_type = native_database_types.invert[name: field.type]
        col_nullable = (field.mode == 'NULLABLE')

        args = { sql_type: col_sql_type, type: col_sql_type, limit: nil }
        args[:scale] = nil
        args[:precision] = nil

        sql_type_metadata =
          ActiveRecord::ConnectionAdapters::SqlTypeMetadata.new(**args)

        cols << ActiveRecord::ConnectionAdapters::Column.new(
          col_name,
          nil,
          sql_type_metadata,
          col_nullable,
          table_name
        )
      end
    end
    # rubocop:enable Metrics/MethodLength, Metrics/AbcSize

    # Returns just a table's primary key
    def primary_key(_table_name)
      []
    end

    def foreign_keys(_table_name)
      []
    end

    # Ensure it's shorter than the maximum identifier length for the current
    # dbms
    def index_name(table_name, options)
      maximum = database_metadata.max_identifier_len || 255
      super(table_name, options)[0...maximum]
    end

    def current_database
      database_metadata.database_name.strip
    end
  end
end
