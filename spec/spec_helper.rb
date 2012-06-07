require 'rubygems'
require "bundler"
Bundler.setup
ENV['RAILS_ENV'] ||= 'test'

$:.unshift(File.dirname(__FILE__) + '/../lib')
require 'pg-reindex'

require 'active_record'

require File.dirname(__FILE__) + '/spec_support.rb'