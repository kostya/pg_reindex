$cfg = {'adapter' => 'postgresql', 'database' => 'pgre_test', 'encoding' => 'utf8', 'username' => 'kostya', 'password' => 'password'}
ActiveRecord::Base.establish_connection $cfg

class Test < ActiveRecord::Base
  self.table_name = 'tests'
end

def pg_create_schema
  ActiveRecord::Migration.create_table :tests do |t|
    t.integer :a
    t.integer :b
    t.integer :c
  end
  
  ActiveRecord::Migration.add_index :tests, :a
  ActiveRecord::Migration.add_index :tests, [:b, :c]
  ActiveRecord::Migration.execute "create unique index a_b_c on tests using btree(a,b,c) where a > 0 and b < 0"
end

def pg_drop_data
  ActiveRecord::Migration.drop_table :tests
end

pg_drop_data rescue nil
pg_create_schema
