require 'optparse'
require 'redis'

module RedisTools

  def sec_to_human(sec)
    sec = sec.to_i

    case sec / 60
    when 0..1
      "#{sec} sec"
    when 2...90
      "#{sec.fdiv(60).round(1)} min"
    when 90...1440
      "#{sec.fdiv(3600).round(1)} hours"
    else
      "#{sec.fdiv(3600*24).round(1)} days"
    end
  end

  class Runner
    attr_reader :config

    def initialize(defaults = {}, &block)
      @config = defaults
      OptionParser.new do |opts|
        opts.banner = "Usage: #{$0} [options]"

        block.call(@config, opts) if block

        opts.on '-n', '--db NUM', Integer, "DB number" do |v|
          config[:db] = v
        end

        opts.on '--password TOKEN', "Password" do |v|
          config[:password] = v
        end

        opts.on('-h', '--help', 'Displays help') do
          puts opts
          exit
        end
      end.parse!
    end

    def connect(addr, opts = {})
      host, port = addr.split(":")
      opts = {host: host, port: port, db: config[:db], password: config[:password]}.merge(opts)
      client = Redis.new(opts)
      client.ping
      client
    end

    def connect!(addr, opts = {})
      connect(addr, opts)
    rescue => e
      abort " ! failed to connect to #{addr}: #{e.message}"
    end

  end

  class SingleRunner < Runner
    attr_reader :client

    def initialize(defaults = {}, &block)
      defaults = {:addr => "127.0.0.1:6379"}.merge(defaults)
      super defaults do |config, opts|
        opts.on '-a', '--addr HOSTPORT', "Redis address. Default: #{config[:addr]}" do |v|
          config[:addr] = v
        end
        block.call(config, opts) if block
      end

      @client = connect! config[:addr]
    end

  end

  class ClusterRunner < Runner
    attr_reader :nodes

    def initialize(defaults = {}, &block)
      defaults = {:seeds => ["127.0.0.1:6379"]}.merge(defaults)
      super defaults  do |config, opts|
        opts.on '-a', '--addrs HOSTPORT[,HOSTPORT]', Array, "Cluster address seeds. Default: #{config[:seeds].join(',')}" do |v|
          config[:seeds] = v
        end
        block.call(config, opts) if block
      end

      @nodes = ClusterNodes.new
      primary = nil
      config[:seeds].each do |seed|
        primary = connect(seed) rescue nil
        break if primary
      end
      abort " ! unable to connect to any of the cluster seeds: #{config[:seeds].join(',')}" unless primary

      primary.cluster(:nodes).each_line do |line|
        id, addr, state, slaveof, _, _, _, _, *ranges = line.split
        nodes.push ClusterNode.new(id, addr, state.sub("myself,", ""), slaveof, ranges || [])
      end
      nodes.finalize!
    end
  end

  class ClusterNodes < Array

    def index
      @index ||= inject({}) {|h, n| h[n.id] = n; h }
    end

    def finalize!
      num_masters = select(&:master?).sort_by {|n| n.addr }.each_with_index {|n, i| n.pos = i+1 }.size
      each do |node|
        master = index[node.slaveof]
        next unless master

        node.pos = num_masters+master.pos.to_i
        node.slaveof = master.code
      end
      sort_by! {|n| n.pos }
    end

  end

  class ClusterNode < Struct.new(:id, :addr, :state, :slaveof, :ranges)
    attr_accessor :pos

    def master?
      state == "master"
    end

    def short_id
      id.to_s[0..6]
    end

    def code
      return "-" unless pos
      pos.to_s.rjust(3, "0")
    end

    def coverage
      return "-" unless master?
      ranges.inject(0) {|s, r| min, max = r.split("-").map(&:to_i); s + Range.new(min, max).count rescue s }
    end
  end

  class Output < Array

    def initialize(header)
      super []
      push header
    end

    def write_to(io)
      return if empty?

      widths = first.map{|c| c.size }
      slice(1..-1).each do |item|
        item.each_with_index do |s, i|
          n = s.to_s.size
          widths[i] = n if n > widths[i]
        end
      end

      each do |item|
        line = []
        item.each_with_index do |s, i|
          line[i] = s.to_s.ljust(widths[i])
        end
        io.puts line.join(" ")
      end
    end

  end
end
