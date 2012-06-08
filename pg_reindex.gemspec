# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = %q{pg_reindex}
  s.version = "0.1.2"

  s.authors = ["Makarchev Konstantin"]
 
  s.description = %q{Console utility for gracefully rebuild indexes/pkeys for PostgreSQL, with minimal locking in semi-auto mode.}
  s.summary = %q{Console utility for gracefully rebuild indexes/pkeys for PostgreSQL, with minimal locking in semi-auto mode.}

  s.email = %q{kostya27@gmail.com}
  s.homepage = %q{http://github.com/kostya/pg_reindex}

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  s.add_dependency 'thor'
  s.add_dependency 'pg'
  s.add_development_dependency "rspec"
  s.add_development_dependency "rake"
  s.add_development_dependency 'activerecord'
  
end