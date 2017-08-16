module BigQueryAdapter
  class QueryTimeoutError < ActiveRecord::StatementInvalid
  end
end
