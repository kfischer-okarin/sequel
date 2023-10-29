# frozen-string-literal: true

require 'duckdb'

DuckDB::Result.use_chunk_each = true

module Sequel
  module DuckDB
    class Database < Sequel::Database
      set_adapter_scheme :duckdb

      def supports_create_table_if_not_exists?
        true
      end

      def execute(sql, opts=OPTS, &block)
        synchronize(opts[:server]) do |conn|
          conn.execute(sql, &block)
        end
      rescue ::DuckDB::Error => e
        raise_error(e)
      end

      def schema_parse_table(table_name, opts=OPTS)
        result = []
        execute("DESCRIBE #{quote_identifier(table_name)}").each do |row|
          db_type = row[1]
          type = schema_column_type(db_type)
          schema = {
            db_type: db_type,
            type: type,
            allow_null: row[2] == 'YES',
            default: row[4],
            ruby_default: nil,
            primary_key: row[3] == 'PRI'
          }
          if type == :integer && schema[:default] =~ /\A\d+\z/
            schema[:ruby_default] = schema[:default].to_i
          elsif type == :datetime && schema[:default] == 'CURRENT_TIMESTAMP'
            schema[:ruby_default] = Sequel::CURRENT_TIMESTAMP
          elsif type == :date && schema[:default] == 'CURRENT_DATE'
            schema[:ruby_default] = Sequel::CURRENT_DATE
          elsif type == :string && schema[:default]
            schema[:ruby_default] = schema[:default][1..-2]
          end
          result << [row[0].downcase.to_sym, schema]
        end
        result
      end

      def connect(server)
        @instance ||= ::DuckDB::Database.open
        Connection.new(@instance.connect, server)
      end

      def database_error_classes
        [::DuckDB::Error]
      end

      def dataset_class_default
        Dataset
      end

      def database_specific_error_class(exception, opts)
        case exception.message
        when /Duplicate key.+violates.+constraint/
          UniqueConstraintViolation
        when /CHECK constraint failed/
          CheckConstraintViolation
        when /NOT NULL constraint failed/
          NotNullConstraintViolation
        when /Violates foreign key constraint/
          ForeignKeyConstraintViolation
        else
          super
        end
      end

      # DuckDB does not support AUTOINCREMENT, so it's not included in the options
      def serial_primary_key_options
        {:primary_key => true, :type => Integer}
      end

      def create_table_sql(name, generator, options)
        sql_statements = []
        primary_key_column = generator.columns.find { |column| column[:primary_key] }
        if primary_key_column
          sequence_name = default_sequence_name(name, primary_key_column[:name])
          sql_statements << create_sequence_sql(sequence_name)
          primary_key_column[:default] = Sequel::LiteralString.new("nextval('#{sequence_name}')")
        end

        sql_statements << super

        sql_statements.join(';')
      end

      def default_sequence_name(table_name, column_name)
        "seq_#{table_name}_#{column_name}"
      end

      def create_sequence_sql(name)
        "CREATE SEQUENCE #{quote_identifier(name)}"
      end
    end

    class Connection
      def initialize(duckdb_connection, server)
        @duckdb_connection = duckdb_connection
        @server = server
      end

      def execute(sql, opts=OPTS, &block)
        result = @duckdb_connection.query(sql)
        if block
          result.each(&block)
        else
          result
        end
      end

      def close
        @duckdb_connection.close
      end
    end

    class Dataset < Sequel::Dataset
      def fetch_rows(sql)
        self.columns = fetch_columns
        db.execute(sql).each do |row|
          yield columns.zip(row).to_h
        end
      end

      private

      def fetch_columns
        return opts[:select] if opts[:select]
        raise NotImplementedError, "Multiple tables not yet supported" if @opts[:from].size > 1

        table_name = @opts[:from].first
        result = []
        db.execute("DESCRIBE #{quote_identifier(table_name)}").each do |row|
          result << row[0].downcase.to_sym
        end
        result
      end
    end
  end
end
