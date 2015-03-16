require 'optparse'

require_relative 'lib/asmapper.rb'
require_relative 'lib/graphdb.rb'
require_relative 'lib/relationdb.rb'

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
        options[:db] = nil
        opts.on("--db DATABASE", [:graph, :relation], "Specify the type of datebase (graph, relation) (required)") do |db|
            options[:db] = db
        end
    end
    optparse.parse!
    puts "Database: #{options[:db]}"

    if options[:db].nil?
        puts "No database specified"
        exit
    end
    
    if options[:db] == :graph
        db = GraphDatabase.new
    else
        db = RelationDatabase.new
    end

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
            asn = ASMapper::parse_asn tokens.shift
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
 
