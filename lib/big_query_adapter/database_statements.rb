module BigQueryAdapter
  # Includes helper methods
  module DatabaseStatements
    NATIVE_DATABASE_TYPES = {
      boolean:      { name: 'BOOL' },
      integer:      { name: 'INTEGER' },
      float:        { name: 'FLOAT' },
      string:       { name: 'STRING' },
      datetime:     { name: 'DATETIME' },
      date:         { name: 'DATE' },
      timestamp:    { name: 'TIMESTAMP' },
      time:         { name: 'TIME' }
    }.freeze

    def native_database_types
      NATIVE_DATABASE_TYPES
    end

    def valid_type?(type) # :nodoc:
      !native_database_types[type].nil?
    end

    # Executes the SQL statement in the context of this connection.
    # Returns the number of rows affected.
    def execute(sql, name = nil, _binds = [])
      log(sql, name) do
        @connection.do(sql)
      end
    end

    # Executes +sql+ statement in the context of this connection using
    # +binds+ as the bind substitutes. +name+ is logged along with
    # the executed +sql+ statement.
    # rubocop:disable Lint/UnusedMethodArgument
    def exec_query(sql, name = 'SQL', _binds = [], prepare: false)
      log(sql, name) do
        result = @connection.run(sql)
        ActiveRecord::Result.new(result.columns, result.rows)
      end
    end
    # rubocop:enable Lint/UnusedMethodArgument

    def supports_ddl_transactions
      false
    end
  end
end
