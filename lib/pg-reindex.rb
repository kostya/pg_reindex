require 'pg'

class PgReindex

  MIN_SIZE = 50 # megabyte min, total table size

  def initialize(conf)
    @cfg = {:host => conf['host'] || '127.0.0.1', :dbname => conf['database'],
           :user => conf['username'] || `whoami`.chop, :password => conf['password'] || 'password', :port => conf['port'].to_s}

    @conn = PGconn.new @cfg
  end
  
  def get_struct_relations(min_size = nil)
    res = get_raw_relations
 
    result = {}
    res.each do |row|
      next if row['total_size'].to_i < ((min_size || MIN_SIZE).to_f * 1024 * 1024).to_i
      result[row['table']] ||= []
      result[row['table']] << row
    end
 
    result = result.sort_by{|el| el[1][0]['total_size'].to_i}.reverse
     
    result = result.map do |table, res|
      [table, res]
    end
    
    result
  end
  
  def filter_relations(filter) # filter is a string with ,
    return [] if !filter || filter.empty?
    
    filter = filter.split(",")

    get_raw_relations.select do |row|
      filter.include?(row['index']) || filter.include?(row['table'])
    end
  end
  
  def get_raw_relations
    res = @conn.exec <<-SQL
      SELECT C.relname AS "table",
        i.relname "index",
        i.oid "index_oid",
        pg_relation_size(C.oid) AS "size",
        pg_total_relation_size(C.oid) AS "total_size",
        pg_size_pretty(pg_relation_size(C.oid)) AS "size_p",
        pg_size_pretty(pg_total_relation_size(C.oid)) AS "total_size_p",
        pg_size_pretty(pg_total_relation_size(C.oid) - pg_relation_size(C.oid)) AS "total_i_size_p",
        pg_relation_size(i.oid) as "i_size",
        pg_size_pretty(pg_relation_size(i.oid)) as "i_size_p",
        ix.indisprimary as "primary"
      FROM pg_class C, pg_class i, pg_index ix, pg_namespace N
      WHERE nspname IN ('public') AND
              C.oid = ix.indrelid and i.oid = ix.indexrelid
              AND C.oid = ix.indrelid and i.oid = ix.indexrelid
              AND C.relname not like 'pg_%'
              AND N.oid = C.relnamespace
      ORDER BY c.relname, i.relname
    SQL
  end
  
  def get_index_size(relation_name)
    res = @conn.exec "SELECT pg_relation_size(oid) as i_size, pg_size_pretty(pg_relation_size(oid)) as i_size_p 
                        from pg_class WHERE relname = E'#{relation_name}'"
    res[0]
  rescue 
    {}
  end
  
  def install_swap_for_pkey
    @conn.exec("create language plpgsql") rescue nil
    @conn.exec(swap_for_pkey_sql)
  end
  
  def check_swap_for_pkey
    res = @conn.exec <<-SQL
      SELECT  proname FROM pg_catalog.pg_namespace n JOIN pg_catalog.pg_proc p ON pronamespace = n.oid
      WHERE   nspname = 'public' and proname = 'swap_for_pkey'
    SQL
    
    res.count == 1
  end
  
  def swap_for_pkey_sql
    <<-SQL
CREATE OR REPLACE FUNCTION swap_for_pkey(text,text,text) returns integer
AS  
$$  
   DECLARE  
     cmd text;  
     oid1 integer;  
     oid2 integer;  
     filenode1 integer;  
     filenode2 integer;  
     relation text;  
   BEGIN  
      select oid::integer into oid1 from pg_class where relname=$2 and relnamespace = (select oid from pg_namespace where nspname=$1);  
     RAISE NOTICE 'PKEY OID: %',oid1;  
      select relfilenode::integer into filenode1 from pg_class where oid=oid1;  
      select oid::integer into oid2 from pg_class where relname=$3 and relnamespace = (select oid from pg_namespace where nspname=$1);  
     RAISE NOTICE 'PKEY OID: %',oid2;  
      select relfilenode::integer into filenode2 from pg_class where oid=oid2;  
      select (indrelid::regclass)::text into relation from pg_index where indexrelid=oid1;  
    RAISE NOTICE 'RELATION NAME: %',relation;  
      cmd:='LOCK '||relation||';';  
      RAISE NOTICE 'Executing :- %',cmd;  
      Execute cmd;        
      cmd:='UPDATE pg_class SET relfilenode='||filenode2|| ' WHERE oid='||oid1||';';  
      RAISE NOTICE 'Executing :- %',cmd;  
      Execute cmd;        
      cmd:='UPDATE pg_class SET relfilenode='||filenode1|| ' WHERE oid='||oid2||';';  
      RAISE NOTICE 'Executing :- %',cmd;  
      Execute cmd;  
      cmd:='DROP INDEX '||$1||'.'||$3||';';  
      RAISE NOTICE 'Executing :- %',cmd;  
      Execute cmd;  
      return 0;  
   END;  
$$language plpgsql;  
    SQL
  end
  
  def database_size(database)
    res = @conn.exec("SELECT pg_size_pretty(pg_database_size('#{database}')) as db_size")
    res.first['db_size']
  end

  def index_sqls(index_row)
    index_sql_array(index_row['table'], index_row['index_oid'], index_row['index'], index_row['primary'] == 't')
  end
    
  def index_sql_array(table, oid, name, primary = false)
    new_name = if name.size < 61  
      name + "_2"
    else
      name[0..-3] + "_2"
    end
    
    if primary
      [
        index_sql(oid, name, new_name),
        "ANALYZE #{table}",
        "SELECT swap_for_pkey('public', '#{name}', '#{new_name}')"
      ]
    else
      [
        index_sql(oid, name, new_name),
        "ANALYZE #{table}",
        "DROP INDEX #{name}",
        "ALTER INDEX #{new_name} RENAME TO #{name}"
      ]
    end
  end
    
  def index_sql(oid, name, new_name)
    str = index_def(oid).sub(name, new_name)
    
    pos = str.index(new_name)
            
    before = str[0..pos-2]
    after = str[pos..-1]
                       
    "#{before} CONCURRENTLY #{after}"
  end
  
  def index_def(oid)
    sql = "SELECT pg_get_indexdef(#{oid}) as q"
    exec(sql)[0]['q']
  end
  
  def exec(sql)
    @conn.exec sql
  end
  
  def queries
    @conn.exec <<-SQL
    SELECT procpid,  now() - query_start as age, current_query
    FROM
        pg_stat_activity AS a JOIN
        pg_locks AS l ON a.procpid = l.pid
    WHERE 
        virtualxid IS NOT NULL order by age desc;
    SQL
  end
  
  def cancel(procpid)
    @conn.exec("select pg_cancel_backend(#{procpid.to_i})")
  end
  
  def have_rebuild_for_relation?(relation)
    res = @conn.exec "select has_table_privilege(E'#{@cfg[:user]}', E'#{relation}', 'update,references')"
    res[0]['has_table_privilege'] == 't' ? true : false
  rescue
    false
  end
  
end