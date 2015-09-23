#!/usr/bin/env ruby

require File.expand_path("../_base.rb", __FILE__)
include RedisTools

runner = SingleRunner.new :sample => 10_000, :percentiles => [50,60,70,80,90] do |config, opts|
  opts.on "--sample NUM", Integer, "Sample size. Default: 10_000" do |v|
    config[:sample] = v
  end
  opts.on "--percentiles PC1[,PC2]", Array, "Percentiles to check. Default: #{config[:percentiles].join(',')}" do |v|
    config[:percentiles] = v.map {|pc| pc.to_i }
  end
end
output = Output.new ["%-ILE", "IDLE-TIME", "IDLE-TIME-HUMAN"]

keys = runner.client.pipelined do
  runner.config[:sample].times do
    runner.client.randomkey
  end
end

sres = runner.client.pipelined do
  keys.each do |key|
    runner.client.object :idletime, key
  end
end.sort

runner.config[:percentiles].each do |pc|
  n = (runner.config[:sample] * pc / 100.0).round
  output.push [pc, sres[n], RedisTools.sec_to_human(sres[n])]
end
output.write_to STDOUT
