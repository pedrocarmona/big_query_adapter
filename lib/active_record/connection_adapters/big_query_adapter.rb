require 'active_record'
require 'arel/visitors/bind_visitor'

require 'big_query_adapter/connection'
require 'big_query_adapter/database_statements'
require 'big_query_adapter/error'
require 'big_query_adapter/quoting'
require 'big_query_adapter/schema_statements'

require 'big_query_adapter/version'

module ActiveRecord
  # no-doc
  class Base
    class << self
      # Build a new BigQuery connection with the given configuration.
      def big_query_connection(config)
        config = config.symbolize_keys
        params = {
          keyfile: config[:keyfile],
          project: config[:project],
          datasets: config[:datasets]
        }
        connection = ::BigQueryAdapter::Connection.new(params)
        ConnectionAdapters::BigQueryAdapter
          .new(connection, logger, config, {})
      end
    end
  end

  module ConnectionAdapters
    # Adapter in the active record namespace
    class BigQueryAdapter < AbstractAdapter
      include ::BigQueryAdapter::DatabaseStatements
      include ::BigQueryAdapter::Quoting
      include ::BigQueryAdapter::SchemaStatements

      ADAPTER_NAME = 'BigQuery'.freeze

      ERR_DUPLICATE_KEY_VALUE     = 23_505
      ERR_QUERY_TIMED_OUT         = 57_014
      ERR_QUERY_TIMED_OUT_MESSAGE = /Query has timed out/

      # The object that stores the information that is fetched from the DBMS
      # when a connection is first established.
      attr_reader :database_metadata

      def initialize(connection, logger, config, database_metadata)
        super(connection, logger, config)
        @database_metadata = database_metadata
      end

      # Returns the human-readable name of the adapter.
      def adapter_name
        ADAPTER_NAME
      end

      # CONNECTION MANAGEMENT ====================================

      # Checks whether the connection to the database is still active. This
      # includes checking whether the database is actually capable of
      # responding, i.e. whether the connection isn't stale.
      def active?
        @connection.run('SELECT TRUE AS active')
      end

      # Disconnects from the database if already connected, and establishes a
      # new connection with the database.
      def reconnect!
        disconnect!
        @connection = Base.big_query_connection(@config)
        super
      end
      alias reset! reconnect!

      # Disconnects from the database if already connected. Otherwise, this
      # method does nothing.
      def disconnect!
        false
      end

      protected

      # Build the type map for ActiveRecord
      def initialize_type_map(map)
        super
      end

      # Translate an exception from the native DBMS to something usable by
      # ActiveRecord.
      def translate_exception(exception, message)
        error_number = exception.message[/^\d+/].to_i

        if error_number == ERR_DUPLICATE_KEY_VALUE
          ActiveRecord::RecordNotUnique.new(message, exception)
        # rubocop:disable Metrics/LineLength
        elsif error_number == ERR_QUERY_TIMED_OUT || exception.message =~ ERR_QUERY_TIMED_OUT_MESSAGE
          ::BigQueryAdapter::QueryTimeoutError.new(message, exception)
        # rubocop:enable Metrics/LineLength
        else
          super
        end
      end

      # no-doc
      class BindSubstitution < Arel::Visitors::ToSql
        include Arel::Visitors::BindVisitor
      end

      # Using a BindVisitor so that the SQL string gets substituted before it is
      # sent to the DBMS (to attempt to get as much coverage as possible for
      # DBMSs we don't support).
      def arel_visitor
        BindSubstitution.new(self)
      end

      # Explicitly turning off prepared_statements in the null adapter because
      # there isn't really a standard on which substitution character to use.
      def prepared_statements
        false
      end

      # Turning off support for migrations because there is no information to
      # go off of for what syntax the DBMS will expect.
      def supports_migrations?
        false
      end

      private

      # Can't use the built-in ActiveRecord map#alias_type because it doesn't
      # work with non-string keys, and in our case the keys are (almost) all
      # numeric
      def alias_type(map, new_type, old_type)
        map.register_type(new_type) do |_, *args|
          map.lookup(old_type, *args)
        end
      end
    end
  end
end
