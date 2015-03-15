require 'mysql2'

class RelationDatabase
    def initialize
        @client = Mysql2::Client.new(:host => "localhost", :username => "root", :password => "root", :database => "internet")
    end

    def insert_as(asn, desc)
        @client.query("INSERT INTO astable values(#{asn}, '#{desc}')")
    end

    def insert_as_link(asn1, asn2, date)
        @client.query("INSERT INTO aslink values(#{asn1}, #{asn2}, '#{date}')")
    end
    
    def query q
        @client.query q
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
