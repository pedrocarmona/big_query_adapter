require 'google/cloud/bigquery'
require 'date'

module BigQueryAdapter
  # Driver for bigquery connection
  class Connection
    Result = Struct.new(:columns, :rows)

    attr_reader :project

    def initialize(project:, keyfile:, timeout: nil, datasets: [])
      @project = project
      @bigquery = Google::Cloud::Bigquery.new(
        project: project,
        keyfile: keyfile
      )
      @dataset_ids = datasets
      @timeout = timeout.to_i if timeout
    end

    def run(statement)
      columns = []
      rows = []

      options = {}
      options[:timeout] = @timeout if @timeout
      results = @bigquery.query(statement, options) # ms
      if results.complete?
        columns = results.first.keys.map(&:to_s) unless results.empty?
        rows = results.map(&:values)
      end

      Result.new(columns, rows)
    end

    def tables
      table_refs
        .map { |table_ref| table_ref_name(table_ref) }
        .group_by { |table_ref_name| table_ref_wildcard_name(table_ref_name) }
        .keys
    end

    def columns(table_name)
      table_schema = table_schema(table_name)
      return [] if table_schema.fields.nil?
      table_schema.fields
    end

    private

    def table_ref_name(table_ref)
      "#{table_ref.project_id}.#{table_ref.dataset_id}.#{table_ref.table_id}"
    end

    def table_ref_wildcard_name(table_ref_name)
      if partitioned_table?(table_ref_name)
        base_name = table_ref_name.split('_')
        base_name.pop
        base_name.join('_') << '_*'
      else
        table_ref_name
      end
    end

    def partitioned_table?(table_ref_name)
      return false if table_ref_name.split('_').size < 2
      date_str = table_ref_name.split('_').last
      date = Date.strptime(date_str, '%Y%m%d')
      return !date.nil?
    rescue StandardError => _error
      return nil
    end

    def datasets
      return @bigquery.datasets if @dataset_ids.empty?
      @bigquery.datasets.select do |dataset|
        @dataset_ids.include?(dataset.dataset_id)
      end
    end

    def table_refs
      datasets
        .map(&:tables)
        .flat_map { |table_list| table_list.map(&:table_ref) }
    end

    def table_ref(table_name)
      if table_name.ends_with?('_*')
        table_name = table_name[0...-1]
        table_refs.find do |table_ref|
          table_ref_name(table_ref) =~ /#{table_name}[0-9]{8}/
        end
      else
        table_refs.find { |table_ref| table_ref_name(table_ref) == table_name }
      end
    end

    def table_schema(table_name)
      table_ref = table_ref(table_name)
      @bigquery.service.get_table(
        table_ref.dataset_id,
        table_ref.table_id
      ).schema
    end
  end
end
