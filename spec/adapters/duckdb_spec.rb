SEQUEL_ADAPTER_TEST = :duckdb

require_relative 'spec_helper'

describe "A DuckDB database" do
  describe 'primary key' do
    before do
      DB.create_table!(:items) do
        primary_key :id
      end
    end

    after do
      DB.drop_table?(:items)
    end

    it 'supports auto incrementing primary keys' do
      DB[:items].insert
      DB[:items].insert
      DB[:items].insert

      DB[:items].map(:id).sort.must_equal [1, 2, 3]
    end

    it 'restarts auto incrementing primary key from 1 after dropping' do
      DB[:items].insert

      DB.drop_table(:items)

      DB.create_table!(:items) do
        primary_key :id
      end

      DB[:items].insert

      DB[:items].map(:id).sort.must_equal [1]
    end
  end

  describe 'Dataset' do
    before do
      DB.create_table!(:items) do
        primary_key :id
        Integer :number
      end
      @dataset = DB[:items]
    end

    after do
      DB.drop_table?(:items)
    end

    it 'can handle aliased expressions' do
      @dataset.insert(:number => 1)

      @dataset.select(Sequel[:number].as(:b)).to_a.must_equal [{:b => 1}]
    end

    it 'returns primary key on insert' do
      @dataset.insert(:number => 22).must_equal 1
      @dataset.insert(:number => 33).must_equal 2
    end

    it 'returns number of updated rows on update' do
      @dataset.insert(:number => 22)

      @dataset.update(:number => 33).must_equal 1
    end
  end
end
