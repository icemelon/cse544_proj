require 'neo4j-core'
require 'ipaddr'

class GraphDatabase
    def initialize
        @session = Neo4j::Session.open(:server_db, 'http://localhost:7474')
        # TODO(cs): possible a performance problem to call this at every
        # initialization.
        # delay the constraints after initial insertion
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
        return @session.query("MATCH (as1)-[link:Link]->(as2) RETURN link")
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
        @session.query("MATCH (as1:AS {asn: #{asn1}}), (as2:AS {asn: #{asn2}}) CREATE (as1)-[:Link {date: #{date}}->(as2)")
    end
end

if $0 == __FILE__
    db = GraphDatabase.new
    #db.delete_all

    db.insert_as(1, "a")
    db.insert_as(2, "b")
    db.select_all_nodes.each do |node|
        puts "AS#{node.as[:asn]}: #{node.as[:desc]}"
    end
end
