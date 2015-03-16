require 'mysql2'

class RelationDatabase
    def initialize
        @client = Mysql2::Client.new(:host => "localhost", :username => "root", :password => "root", :database => "internet")
    end

    def convert_date date
        year = (date / 10000).to_s
        date = date % 10000
        month = (date / 100).to_s
        day = (date % 100).to_s
        year + "-" + month + "-" + day
    end

    def insert_as(asn, desc)
        @client.query("INSERT INTO astable values(#{asn}, '#{desc}')")
    end

    def insert_as_link(asn1, asn2, date)
        date = convert_date date
        @client.query("INSERT INTO aslink values(#{asn1}, #{asn2}, '#{date}')")
    end
    
    def query q
        @client.query q
    end

    def get_as_name asn
        results = @client.query("SELECT description from astable where asn=#{asn}")
        results.first["description"]
    end

    def find_shortest_path(src_asn, dst_asn, date=nil)
        tovisit = [src_asn]
        added = Set.new
        added << src_asn
        distance = {src_asn => [0, nil]}

        start = Time.now
        date = convert_date date if not date.nil?
        cnt = 0
        while not tovisit.empty?
            src = tovisit.shift
            dist = distance[src][0]
            cnt += 2
            if date.nil?
                result1 = @client.query("SELECT asn2 from aslink where asn1=#{src}")
                result2 = @client.query("SELECT asn1 from aslink where asn2=#{src}")
            else
                result1 = @client.query("SELECT asn2 from aslink where asn1=#{src} and date='#{date}'")
                result2 = @client.query("SELECT asn1 from aslink where asn2=#{src} and date='#{date}'")
            end

            result1.each do |row|
                dst = row["asn2"]
                if not added.include? dst
                    tovisit << dst
                    added << dst
                    distance[dst] = [dist + 1, src]
                end
            end
            result2.each do |row|
                dst = row["asn1"]
                if not added.include? dst
                    tovisit << dst
                    added << dst
                    distance[dst] = [dist + 1, src]
                end
            end
            #puts tovisit.size
            break if distance.has_key? dst_asn
        end
        path = []
        if distance.has_key? dst_asn
            asn = dst_asn
            while not asn.nil?
                path.unshift [asn, get_as_name(asn)]
                cnt += 1
                asn = distance[asn][1]
            end
        end
        finish = Time.now
        if path.empty?
            puts "No path found from AS#{src_asn} to AS#{dst_asn}"
        else
            print "AS#{path.first[0]} (#{path.first[1]})"
            path[1..-1].each { |asn, desc| print " -> AS#{asn} (#{desc})" }
            puts
        end
        puts "Query finishes in #{(finish-start).round(3)} sec (#{cnt} queries in total)"
    end
end

if $0 == __FILE__
    db = RelationDatabase.new
    result = db.query "show tables"
    result.each { |row| puts row }
    db.insert_as(1, "aaa")
    db.insert_as(2, "bbb")
    db.insert_as_link(1, 2, "2015-01-19")
end
