require 'optparse'

require_relative 'graphdb.rb'

def parse_asn asn
    # to make cache not caching nil, we use 0 to indicate nil
    return nil if asn.empty?
    asn = asn.chomp
    asn = asn[0...asn.index('_')] if asn.include? '_'
    asn = asn[1...-1] if asn[0] == '{'
    asn = asn[0...asn.index(',')] if asn.include? ','
    if asn.include? '.'
        x, y = asn.split '.'
        asn = (x.to_i << 16) + y.to_i
    end
    return asn.to_i
end

if $0 == __FILE__
    options = {}
    optparse = OptionParser.new do |opts|
        options[:as] = nil
        opts.on("-a", "--as AS", "Specify the prefix to AS mapping file, and create nodes in the database") do |as|
            options[:as] = as
        end
        options[:prefix] = nil
        opts.on("-p", "--pref PREF", "Specify the prefix to AS mapping file") do |pref|
            options[:prefix] = pref
        end
        options[:link] = nil
        opts.on("-l", "--link LINK", "Specify the AS link file to insert AS links into the database") do |link|
            options[:link] = link
        end
    end
    optparse.parse!

    db = GraphDatabase.new

    if not options[:as].nil?
        start = Time.now
        count = 0
        File.open(options[:as]).each_line do |line|
            next if not line.start_with? "<a href"
            #line.encode!("UTF-8", :invalid=>:replace)
            line = line.force_encoding("ISO-8859-1").encode("utf-8", replace: nil)
            line = line[line.index('>')+1..-1]
            asn = line[2..line.index('<')].to_i
            desc = line[line.index('>')+1..-1].strip
            desc.gsub!("\\", "\\\\\\\\")
            desc.gsub!("'", "\\\\'")
            db.insert_as(asn, desc)
            count += 1
        end
        finish = Time.now
        puts "Insert #{count} ASes in #{finish-start} sec"
    elsif not options[:prefix].nil?
        start = Time.now
        count = 0
        File.open(options[:prefix]).each_line do |line|
            tokens = line.chomp.split
            prefix = tokens.shift
            asn = parse_asn tokens.shift
            db.insert_prefix(prefix, asn)
            count += 1
        end
        finish = Time.now
        puts "Insert #{count} IPv4 prefixes and edges in #{finish-start} sec"
    elsif not options[:link].nil?
        start = Time.now
        count = 0
        date = File.basename(options[:link]).gsub("ASLink", '').gsub(".txt", '').to_i
        File.open(options[:link]).each_line do |line|
            asn1, asn2 = line.chomp.split
            asn1 = asn1.to_i
            asn2 = asn2.to_i
            db.insert_as_link(asn1, asn2, date)
            count += 1
        end
        finish = Time.now
        puts "Insert #{count} AS Links in #{finish-start} sec"
    else
        puts "No file is provided."
    end
end
 
