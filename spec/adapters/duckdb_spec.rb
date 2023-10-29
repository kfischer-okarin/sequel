SEQUEL_ADAPTER_TEST = :duckdb

require_relative 'spec_helper'

describe "A DuckDB database" do
  it 'supports auto incrementing primary keys' do
    DB.create_table!(:items) do
      primary_key :id
      String :name
    end

    DB[:items].insert(:name => 'abc')
    DB[:items].insert(:name => 'def')
    DB[:items].insert(:name => 'ghi')

    DB[:items].map(:id).sort.must_equal [1, 2, 3]
  ensure
    DB.drop_table?(:items)
  end
end
