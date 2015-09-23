#!/usr/bin/env ruby

require File.expand_path("../_base.rb", __FILE__)
include RedisTools

runner = ClusterRunner.new
output = Output.new ["SEQ", "ADDR", "STATE", "SLAVEOF", "SLOTS", "ID"]

runner.nodes.each do |n|
  output.push [n.code, n.addr, n.state, n.slaveof, n.coverage, n.id]
end
output.write_to(STDOUT)
