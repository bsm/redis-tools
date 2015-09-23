#!/usr/bin/env ruby

require File.expand_path("../_base.rb", __FILE__)
include RedisTools

runner = ClusterRunner.new
output = Output.new ["ADDR", "ID", "STATE", "MASTER", "SLOTS", "MEM", "DETAILS"]

runner.nodes.map do |node|
  Thread.new(node) do |node|
    info = { :state => node.state }
    begin
      ni = runner.connect(node.addr, connect_timeout: 2).info
      info.update \
        :mem     => ni['used_memory_human'],
        :details => ni['db0'],
        :state   => ni['role'],
        :master  => ni.values_at('master_host', 'master_port').compact.join(':')
    rescue
    end
    [node.addr, node.id, info[:state], info[:master], node.coverage, info[:mem], info[:details]]
  end
end.each do |thread|
  output.push thread.value
end
output.write_to STDOUT
