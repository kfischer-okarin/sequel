SEQUEL_ADAPTER_TEST = :duckdb

require_relative 'spec_helper'

describe "A DuckDB database" do
  after do
    DB.drop_table?(:items)
  end

  it 'supports auto incrementing primary keys' do
    DB.create_table!(:items) do
      primary_key :id
    end

    DB[:items].insert
    DB[:items].insert
    DB[:items].insert

    DB[:items].map(:id).sort.must_equal [1, 2, 3]
  end
end
