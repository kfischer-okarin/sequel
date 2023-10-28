# frozen-string-literal: true

module Sequel
  module DuckDB
    class Database < Sequel::Database
      set_adapter_scheme :duckdb
    end
  end
end
