module BigQueryAdapter
  # Includes helper methods
  module Quoting
    # Quotes a string, escaping any ' (single quote) characters.
    def quote_string(string)
      string.gsub(/\'/, "''")
    end

    # Quotes the table name. Defaults to column name quoting.
    def quote_table_name(table_name)
      "`#{table_name}`"
    end
  end
end
