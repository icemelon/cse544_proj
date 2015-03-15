require 'neo4j-core'

class ASGraphDatabase
    def initialize
        @session = Neo4j::Session.open(:server_db, 'http://localhost:7474')
        # TODO(cs): possible a performance problem to call this at every
        # initialization.
        place_constraints
    end

    def place_constraints
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

    def insert_node(asn)
        begin
            @session.query("CREATE (:AS {asn: #{asn}})")
        rescue Neo4j::Server::CypherResponse::ResponseError => error
            if not error.to_s.include? "already exists"
                # ignore repeated node error
                raise error
            end
        end
    end

    def insert_link(asn1, asn2, date)
        @session.query("MATCH (as1:AS {asn: #{asn1}}), (as2:AS {asn: #{asn2}}) CREATE (as1)-[:Link {date: #{date}}->(as2)")
    end
end

if $0 == __FILE__
    db = ASGraphDatabase.new
    db.delete_all

    db.insert_node(1)
    db.insert_node(2)
    db.select_all_nodes.each do |node|
        puts node.as[:asn]
    end
    db.insert_node(1)  
end
