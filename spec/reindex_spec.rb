require File.dirname(__FILE__) + '/spec_helper'

describe PgReindex do
  before :each do
    @pgre = PgReindex.new($cfg)
  end
  
  def row(name)
    @pgre.filter_relations(name).first
  end  

  def drop_swap_for_reindex
    @pgre.exec("drop FUNCTION swap_for_pkey(text, text, text)") rescue nil
  end

  it "get_raw_relations" do
    res = @pgre.get_raw_relations
    v = []
    res.each{|h| v << h }
    
    v.map{|el| el['table']}.uniq.should == ['tests']
    v.map{|el| el['index']}.uniq.sort.should == ["a_b_c", "index_tests_on_a", "index_tests_on_b_and_c", "tests_pkey"]
    v.map{|el| el['index_oid']}.sum.to_i.should > 0
  end
  
  describe "filter_relations" do
    it "1" do
      res = @pgre.filter_relations('a_b_c')
      res.size.should == 1
      res[0]['index'].should == 'a_b_c'
    end
    
    it "2" do 
      res = @pgre.filter_relations('a_b_c,index_tests_on_a,tests_pkey,xxx')
      res.size.should == 3
      res.map{|el| el['index']}.sort.should == ["a_b_c", "index_tests_on_a", "tests_pkey"]
    end
    
    it "3" do
      res = @pgre.filter_relations('tests,xxx')
      res.size.should == 4
      res.map{|el| el['index']}.sort.should == ["a_b_c", "index_tests_on_a", "index_tests_on_b_and_c", "tests_pkey"]
    end
    
    it "4" do
      res = @pgre.filter_relations('xxx')
      res.size.should == 0
    end
    
    it "5" do
      res = @pgre.filter_relations('tests,a_b_c')
      res.size.should == 4
      res.map{|el| el['index']}.sort.should == ["a_b_c", "index_tests_on_a", "index_tests_on_b_and_c", "tests_pkey"]
    end                
  end
  
  it "database_size" do
    s = @pgre.database_size('pgre_test')
    s.to_i.should > 1
  end
  
  describe "index sqls" do
    it "index sqls for a" do
      @pgre.index_sqls(row('index_tests_on_a')).should == ["CREATE INDEX CONCURRENTLY index_tests_on_a_2 ON tests USING btree (a)", 
        "ANALYZE tests", "DROP INDEX index_tests_on_a", "ALTER INDEX index_tests_on_a_2 RENAME TO index_tests_on_a"]
    end
  
    it "index sqls for b,c" do
      @pgre.index_sqls(row('index_tests_on_b_and_c')).should == ["CREATE INDEX CONCURRENTLY index_tests_on_b_and_c_2 ON tests USING btree (b, c)", 
        "ANALYZE tests", "DROP INDEX index_tests_on_b_and_c", "ALTER INDEX index_tests_on_b_and_c_2 RENAME TO index_tests_on_b_and_c"]
    end
  
    it "index sqls for a,b,c" do
      @pgre.index_sqls(row('a_b_c')).should == ["CREATE UNIQUE INDEX CONCURRENTLY a_b_c_2 ON tests USING btree (a, b, c) WHERE ((a > 0) AND (b < 0))", 
        "ANALYZE tests", "DROP INDEX a_b_c", "ALTER INDEX a_b_c_2 RENAME TO a_b_c"]
    end
  
    it "index sqls for pkey" do
      @pgre.index_sqls(row('tests_pkey')).should == ["CREATE UNIQUE INDEX CONCURRENTLY tests_pkey_2 ON tests USING btree (id)", 
        "ANALYZE tests", "SELECT swap_for_pkey('public', 'tests_pkey', 'tests_pkey_2')"] 
    end
  end
  
  it "index_sql with save name" do
    @pgre.stub!(:index_def).and_return("CREATE INDEX locked_by ON delayed_jobs USING btree (locked_by)")
    sql = @pgre.index_sql(0, "locked_by", "locked_by_2")
    sql.should == "CREATE INDEX CONCURRENTLY locked_by_2 ON delayed_jobs USING btree (locked_by)"
  end
  
  it "index def" do
    r = row('a_b_c')
    @pgre.index_def(r['index_oid']).should == "CREATE UNIQUE INDEX a_b_c ON tests USING btree (a, b, c) WHERE ((a > 0) AND (b < 0))"
  end

  it "check swap for pkey" do
    drop_swap_for_reindex
    @pgre.check_swap_for_pkey.should == false
    @pgre.install_swap_for_pkey
    @pgre.check_swap_for_pkey.should == true
  end
  
  it "rebuilds index" do
    r = row('a_b_c')
    r['index_oid'].to_i.should > 0
    def1 = @pgre.index_def(r['index_oid'])
    
    sqls = @pgre.index_sqls(r)
    sqls.each do |sql|
      @pgre.exec sql
    end
    
    r2 = row('a_b_c')
    r2['index_oid'].to_i.should > 0
    
    r2['index_oid'].should_not == r['index_oid']
    r2['index'].should == r['index']
    
    def2 = @pgre.index_def(r2['index_oid'])
    def1.should == def2
    
    # index should be
    res = @pgre.filter_relations('tests')
    res.size.should == 4
    res.map{|el| el['index']}.sort.should == ["a_b_c", "index_tests_on_a", "index_tests_on_b_and_c", "tests_pkey"]    
  end
  
  it "rebuilds pkey" do
    # install swap_for_pkey
    @pgre.install_swap_for_pkey
  
    r = row('tests_pkey')
    r['index_oid'].to_i.should > 0
    def1 = @pgre.index_def(r['index_oid'])
        
    sqls = @pgre.index_sqls(r)
    if @pgre.have_rebuild_for_relation?('pg_catalog.pg_class')
      sqls.each do |sql|
        @pgre.exec sql
      end      
                          
      r2 = row('tests_pkey')
      r2['index_oid'].to_i.should > 0

      r2['index_oid'].should == r['index_oid']
      def1.should == @pgre.index_def(r2['index_oid'])
                                      
      # index should be
      res = @pgre.filter_relations('tests')
      res.size.should == 4
      res.map{|el| el['index']}.sort.should == ["a_b_c", "index_tests_on_a", "index_tests_on_b_and_c", "tests_pkey"]
    else
      puts "you havn't permission to rebuild pkey, test impossible!"
    end
  end
  
  it "have_rebuild_for_relation? should be true for created table" do
    @pgre.have_rebuild_for_relation?('tests').should == true
  end

end