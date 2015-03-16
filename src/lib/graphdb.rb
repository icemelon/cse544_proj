require 'neo4j-core'
require 'set'
require 'ipaddr'

class GraphDatabase
    def initialize
        @session = Neo4j::Session.open(:server_db, 'http://localhost:7474')
        # TODO(cs): possible a performance problem to call this at every
        # initialization.
        # delay the constraints after initial insertion
    end

    def create_index
        @session.query("CREATE INDEX ON :AS(asn)")
    end

    def place_constraints
        @session.query("CREATE CONSTRAINT ON (p:PREFIX) ASSERT p.prefix IS UNIQUE")
        @session.query("CREATE CONSTRAINT ON (as:AS) ASSERT as.asn IS UNIQUE")
        # TODO(cs): create constraints on links as well?
    end

    def select_all_nodes
        return @session.query("MATCH (as:AS) RETURN as")
        #.each { |n| puts n.as[:asn]; break }
    end

    def select_all_links
        return @session.query("MATCH (:AS)-[link:Link]->(:AS) RETURN link")
    end

    def delete_all
        @session.query <<-eos
            MATCH (n)
            OPTIONAL MATCH (n)-[r]-()
            DELETE n,r
        eos
    end

    def insert_prefix(pref, asn)
        prefix = IPAddre.new pref
        first = prefix.to_i
        last = prefix.to_range.last.to_i
        @session.query("CREATE (:PREFIX {prefix:'#{pref}', first:#{first}, last:#{last}})")
        @session.query <<-eos
            MATCH (p:PREFIX {prefix:'#{pref}'}), (as:AS {asn:#{asn}}) 
            CREATE (p)-[:BELONG]->(as)
        eos
    end

    def insert_as(asn, desc)
        begin
            @session.query("CREATE (:AS {asn: #{asn}, desc: '#{desc}'})")
        rescue Neo4j::Server::CypherResponse::ResponseError => error
            if not error.to_s.include? "already exists"
                # ignore repeated node error
                raise error
            end
        end
    end

    def insert_as_link(asn1, asn2, date)
        @session.query <<-eos
            MATCH (as1:AS {asn: #{asn1}}), (as2:AS {asn: #{asn2}}) 
            CREATE (as1)-[:Date#{date}]->(as2)
        eos
    end

    def print_path path
        first = true 
        path.each do |n|
            if first
                print "AS#{n[:asn]} (#{n[:desc]})"
                first = false
            else
                print " -> AS#{n[:asn]} (#{n[:desc]})"
            end
        end
        print "\n"
    end

    def find_shortest_path(src_asn, dst_asn, date=nil)
        start = Time.now
        if date.nil?
            result = @session.query <<-eos
                match (s:AS {asn: #{src_asn}}), (d:AS {asn: #{dst_asn}}), 
                path=shortestPath(s-[*]-d) 
                return NODES(path)
            eos
        else 
            result = @session.query <<-eos
                match (s:AS {asn: #{src_asn}}), (d:AS {asn: #{dst_asn}}), 
                path=shortestPath(s-[:Date#{date}*]-d) 
                return NODES(path)
            eos
        end
        finish = Time.now
        result = result.first
        if result.nil?
            puts "No path found from AS#{src_asn} to AS#{dst_asn}"
        else
            print_path result['NODES(path)']
        end
        puts "Query finish in #{(finish-start).round(3)} sec"
    end

    def find_all_shortest_paths(src_asn, dst_asn, date=nil)
        start = Time.now
        if date.nil?
            result = @session.query <<-eos
                match (s:AS {asn: #{src_asn}}), (d:AS {asn: #{dst_asn}}), 
                paths=allShortestPaths(s-[*]-d) 
                return NODES(paths)
            eos
        else 
            result = @session.query <<-eos
                match (s:AS {asn: #{src_asn}}), (d:AS {asn: #{dst_asn}}), 
                paths=allShortestPaths(s-[:Date#{date}*]-d) 
                return NODES(paths)
            eos
        end
        finish = Time.now
        first = true 
        num = 0
        allpaths = Set.new
        result.each do |item|
            path = item['NODES(paths)']
            pathid = []
            path.each { |n| pathid << n[:asn] }
            if not allpaths.include? pathid
                allpaths << pathid
                print_path path
                num += 1
            end
        end
        if num == 0
            puts "No path found from AS#{src_asn} to AS#{dst_asn}"
        else
            puts "Total #{num} paths from AS#{src_asn} to AS#{dst_asn}"
        end
        puts "Query finish in #{(finish-start).round(3)} sec"
    end


    def find_shortest_path_slow(asn1, asn2, date=nil)
        tovisit = [asn1]
        distance = {asn1 => [0, nil]}
        start = Time.now
        cnt = 0
        while not tovisit.empty?
            src = tovisit.shift
            dist = distance[src][0]
            cnt += 1
            result = @session.query("MATCH (:AS {asn: #{src}})-[:Link {date: #{date}}]->(n) RETURN n")
            result.each do |n|
                dst = n[:n][:asn]
                if not distance.has_key? dst
                    tovisit << dst
                    distance[dst] = [dist + 1, src]
                end
            end
            break if distance.has_key? asn2
        end
        finish = Time.now
        puts "#{distance[asn2][0]} (finish in #{(finish-start).round(2)} sec)"
        puts "#{cnt} queries in total"
        as = asn2
        while not as.nil?
            puts as
            as = distance[as][1]
        end
        return 
        ret = query.exec
        p ret
        result = query.response
        p result.data
        return
        query = @session.query.match("(as1:AS {asn: #{asn1}})-[r:Link]->(as2:AS {asn: #{asn2}})").return('r')
        puts query.to_cypher
        ret = query.exec
        puts ret
        result = query.response
        p result.methods
        #p result.to_json.to_s
        puts 'data'
        p result.data
        puts 'entity'
        p result.entity_data
        puts 'row'
        result.each_data_row { |row| puts row }
    end

    def find_all_path(asn1, asn2)
    end
end

if $0 == __FILE__
    db = GraphDatabase.new
    db.
    #db.delete_all

    db.insert_as(1, "a")
    db.insert_as(2, "b")
    db.select_all_nodes.each do |node|
        puts "AS#{node.as[:asn]}: #{node.as[:desc]}"
    end
end
