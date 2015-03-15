require_relative 'asgraph.rb'

if $0 == __FILE__
    optioons = {}
    optparse = OptionParser.new do |opts|
        options[:as] = nil
        opts.on("-n", "--as AS", "Specify the AS file to insert ASes into the database") do |as|
            options[:as] = date
        end
        options[:link] = nil
        opts.on("-l", "--link LINK", "Specify the AS link file to insert AS links into the database") do |link|
            options[:link] = link
        end
    end
    optparse.parse!

    db = ASGraphDatabase.new

    if not options[:as].nil?
        File.open(options[:as]).each_line do |line|
            asn = line.to_i
            db.insert_node asn
        end
    elsif not options[:link].nil?
        date = options[:link].gsub("ASLink", '').gsub(".txt", '').to_i
        File.open(options[:link]).each_line do |line|
            asn1, asn2 = line.chomp.split
            asn1 = asn1.to_i
            asn2 = asn2.to_i
            db.insert_link(asn1, asn2, date)
        end
    else
        puts "No file is provided."
    end
end
 
