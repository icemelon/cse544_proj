require_relative 'lib/asmapper.rb'
require_relative 'lib/graphdb.rb'
require_relative 'lib/relationdb.rb'

if $0 == __FILE__
    if ARGV.size < 1
        puts "#{$0} database"
        puts "Option:"
        puts "    database\t\tSelect from (relation, graph)"
        exit
    end
    
    dbtype = ARGV[0]
    if dbtype == "relation"
        db = RelationDatabase.new
        puts "Connected to MySQL database"
    elsif dbtype == "graph"
        db = GraphDatabase.new
        puts "Connected to Neo4j database"
    else
        puts "Wrong database type"
    end

    while true
        print '> '
        query = $stdin.gets.chomp.downcase
        break if query == "exit"
        tokens = query.split
        cmd = tokens.shift
        correct = true
        if cmd == 'from'
            srcip = tokens.shift
            cmd = tokens.shift
            if cmd != 'to'
                puts "Wrong query at #{cmd}"
                next
            end
            dstip = tokens.shift
            date = nil
            allpaths = false
            while not tokens.empty?
                cmd = tokens.shift
                if cmd == 'on'
                    date = tokens.shift.to_i
                elsif cmd == 'all'
                    allpaths = true
                else
                    puts "Wrong query at #{cmd}"
                    correct = false
                    break
                end
            end
            next if not correct
            src_asn = ASMapper::query_asn srcip
            dst_asn = ASMapper::query_asn dstip
            if src_asn.nil?
                puts "Cannot find ASN for #{srcip}"
                next
            end
            if dst_asn.nil?
                puts "Cannot find ASN for #{dstip}"
                next
            end
            puts "src: #{srcip} (AS#{src_asn}), dst: #{dstip} (AS#{dst_asn})"
            if allpaths
                db.find_all_shortest_paths(src_asn, dst_asn, date)
            else
                db.find_shortest_path(src_asn, dst_asn, date)
            end
        else
            puts "Wrong query at #{cmd}"
        end
    end
end
