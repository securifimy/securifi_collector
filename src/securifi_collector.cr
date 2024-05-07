# TODO: Write documentation for `SecurifiCollector`
# This is crystal source file. but I will try to make it ruby compatible. no need to set #!


# This program will process securifi/adguard querylog and send it to timescaleDB for storage.
# It is comprise of 4 sections.
# 1. Distributor - Create and distribute processing job into time bucket and send it to worker.
# 2. Worker (processing) - The actual procesing will be done by this function. its will agregate,
#    count and process all data that match time bucket assigned to it. Currently, only qname, ip
#    and count will be process but I plan to add country too using the GeoIP extension
#    https://github.com/delef/geoip2.cr
# 3. Sender - Sender will prepare data from worker and send it to timescaleDB. if failed it will cache
#    the data to disk.
# 4. Transporter - Will monitor for any cache data and send to timescaleDB independantly if the timescaleDB
#    server is available.
# Because its going to be a binary excutable. I will use a configuration file to configure some of the
# things use by the excutable. This file is going to use YML or JSON. I will select the best one that
# easy to use and reparse but still readable/editable by humans.

# Dependencies declaration
require "option_parser"
require "yaml"
require "json"
require "geoip2"
require "digest/md5"
require "halite"


# Variable Declaration
settings_path = ""
settings_yaml = ""
timescaledb_host = ""
timescaledb_name = ""
unknown_log_line_debug = [] of String
hostname = System.hostname
info_log = false
querylog_size = nil
connection_error = 0

# Excutable argument settings.
# -c path to settings file.
# https://crystal-lang.org/api/0.18.7/OptionParser.html
OptionParser.parse do |parser|
  parser.banner = "Usage: #{PROGRAM_NAME} [arguments]"
  parser.on("-v", "--verbose", "Verbose mode to debug error") { info_log = true }
  parser.on("-c", "--settings=SETTINGS_PATH", "Define path to the settings file") { |settings_arg| settings_path = settings_arg }
  parser.on("-l", "--querylog_size=SIZE", "Number of data to get from api") { |querylog_size_arg| querylog_size = querylog_size_arg }
  parser.on("-h", "--help", "Show this help") { puts parser ; exit 0}
end

# Configuration file setings.
if settings_path.empty?
  puts "Unconfigured settings path, using default path [./settings.yml]."
  settings_path = "settings.yml"
end

unless File.file?(settings_path)
  puts "Error!! Settings file not found at #{settings_path}"
  exit 1
else
  settings_yaml = YAML.parse(File.read("#{settings_path}"))
end

## securifi/adguard api endpoint configuration
begin
  if settings_yaml["securifi"]["api"]? && settings_yaml["securifi"]["api"]["host"]? && settings_yaml["securifi"]["api"]["port"]? && settings_yaml["securifi"]["api"]["uri_path"]? && settings_yaml["securifi"]["api"]["username"]? && settings_yaml["securifi"]["api"]["password"]? && settings_yaml["securifi"]["api"]["secure"]?
      api_host = settings_yaml["securifi"]["api"]["host"].as_s
      api_port = settings_yaml["securifi"]["api"]["port"].as_i
      api_uri_path = settings_yaml["securifi"]["api"]["uri_path"].as_s
      api_username = settings_yaml["securifi"]["api"]["username"].as_s
      api_password = settings_yaml["securifi"]["api"]["password"].as_s
      api_secure = settings_yaml["securifi"]["api"]["secure"].as_bool
      if api_secure
        api_url = "https://#{api_host}:#{api_port}#{api_uri_path}"
      else
        api_url = "http://#{api_host}:#{api_port}#{api_uri_path}"
      end
  else
       puts "Invalid settings for securifi API. exiting.."
       exit 1
  end
rescue e
  if info_log
      puts "Exception:#{e}"
  end
  puts "Invalid settings for securifi API. exiting.."
  exit 1
end

## configure timescaledb 
begin
  if settings_yaml["timescaledb"]["host"]? && settings_yaml["timescaledb"]["port"]? && settings_yaml["timescaledb"]["database"]? && settings_yaml["timescaledb"]["username"]? && settings_yaml["timescaledb"]["password"]? && settings_yaml["timescaledb"]["batch_size"]?
      timescaledb_host = settings_yaml["timescaledb"]["host"].as_s
      timescaledb_port = settings_yaml["timescaledb"]["port"].as_i
      timescaledb_username = settings_yaml["timescaledb"]["username"].as_s
      timescaledb_password = settings_yaml["timescaledb"]["password"].as_s
      timescaledb_dbname = settings_yaml["timescaledb"]["database"].as_s
      timescaledb_sslmode = settings_yaml["timescaledb"]["sslmode"].as_s?
      timescaledb_batch_size = settings_yaml["timescaledb"]["batch_size"].as_i
      if timescaledb_sslmode
          timescaledb_url = "postgres://#{timescaledb_username}:#{timescaledb_password}@#{timescaledb_host}:#{timescaledb_port}/#{timescaledb_dbname}?sslmode=#{timescaledb_sslmode}"
      else
          timescaledb_url = "postgres://#{timescaledb_username}:#{timescaledb_password}@#{timescaledb_host}:#{timescaledb_port}/#{timescaledb_dbname}"
      end
      if info_log
          puts "timescaledb connection url: #{timescaledb_url}"
      end
  else
      puts "Invalid settings for timescaledb. exiting.."
      exit 1
  end
rescue e
  if info_log
      puts "Exception:#{e}"
  end
  puts "Invalid settings for timescaledb. exiting.."
  exit 1
end

#temp save path.
begin
  save_path = "#{settings_yaml["securifi_collector"]["save_path"].as_s.chomp("/")}/securifi_collector_save/"
  unless Dir.exists?("#{save_path}")
      Dir.mkdir("#{save_path}")
  end
rescue e
  if info_log
      puts "Exception:#{e}"
  end
  unless Dir.exists?("#{Dir.current}/securifi_collector_save/")
      Dir.mkdir("#{Dir.current}/securifi_collector_save/")
  end
  save_path = "#{Dir.current}/securifi_collector_save/"
end

## configure time setting from settings_yaml
begin
  if settings_yaml["securifi_collector"]["time_bucket_size"]?
      time_bucket_size = settings_yaml["securifi_collector"]["time_bucket_size"].as_i
  else
      puts "Invalid value for time_bucket_size, accept interger. Using default option [60]"
      time_bucket_size = 60
  end
rescue e
  if info_log
      puts "Exception:#{e}"
  end
  puts "Failed to read from #{settings_path}. Using default value [60]"
  time_bucket_size = 60 # time bucket size in sec, better set this in minute boundary.
end

begin
  if settings_yaml["securifi_collector"]["transporter_timeout"]?
      transporter_timeout = settings_yaml["securifi_collector"]["transporter_timeout"].as_i
  else
      puts "Invalid value for transporter_timeout, accept interger. Using default option [300]"
      transporter_timeout = 300
  end
rescue e
  if info_log
      puts "Exception:#{e}"
  end
  puts "Failed to read from #{settings_path}. Using default value [300]"
  transporter_timeout = 300 # timeout use to wait for next try for transporter module. 
end
begin
  if settings_yaml["securifi_collector"]["time_adjustment"]?
      time_adjustment = settings_yaml["securifi_collector"]["time_adjustment"].as_i
  else
      puts "Invalid value for time_adjustment, accept interger. Using default option [1]"
      time_adjustment = 1
  end
rescue e
  if info_log
      puts "Exception:#{e}"
  end
  puts "Failed to read from #{settings_path}. Using default value [1]"
  time_adjustment = 1 # adjustment time to make sure the bucket does not overlapse each other.
end

## configure force restart from yml
begin
  if settings_yaml["securifi_collector"]["db_connection_error_count"]?
      force_restart_count = settings_yaml["securifi_collector"]["db_connection_error_count"].as_i
  else
      puts "Invalid value for db_connection_error_count, accept interger. Using default option [10]"
      force_restart_count = 10
  end

rescue e
  if info_log
      puts "Exception:#{e}"
  end 
  puts "Failed to read from #{settings_path}. Using default option [10]"
  force_restart_count = 10
end

## configure geoip from settings_yaml
begin
  if settings_yaml["geoip"]["geoIP_DB_path"]?
      geoIP_path = settings_yaml["geoip"]["geoIP_DB_path"].as_s
  else
      puts "Invalid value for geoIP_DB_path. Aborting.."
      exit 1
  end
  unless File.file?(geoIP_path)
      "Invalid file or file not found for #{geoIP_path}. Aborting.."
      exit 1
  end

rescue e
  if info_log
      puts "Exception:#{e}"
  end 
  puts "Failed to read from #{settings_path}. Aborting.."
  exit 1
end

## configure batch processing query from setting_yaml
unless querylog_size.nil?
  begin
      if settings_yaml["securifi_collector"]["querylog_size"]?
        querylog_size = settings_yaml["securifi_collector"]["querylog_size"]
      else
          puts "Invalid value for querylog_size, accept interget. Using default value [1000]"
          querylog_size = 1000
      end
  rescue e
      if info_log
          puts "Exception:#{e}"
      end
      puts "Failed to read from #{settings_path}. Using default value [1000]"
      querylog_size = 1000 
  end
end

## Initializer
# Configure GeoIP
begin
  geoIP_db = GeoIP2.open(geoIP_path)
rescue e
  if info_log
      puts "Exception:#{e}"
  end 
  puts "Invalid GeoIP Database.. Exiting.."
  exit 1
end

#Configure socket
sock = Socket.tcp(Socket::Family::INET)

if info_log
  puts "Configs"
  puts "settings_path: #{settings_path}"
  puts "save_path: #{save_path}"
  puts "API_url: #{api_url} Username: #{api_username} Password: #{api_password}"
  puts "Timescaledb Host: #{timescaledb_host}"
  puts "Timescaledb Port: #{timescaledb_port}"
  puts "Timescaledb Password: #{timescaledb_password}"
  puts "Timescaledb Dabtabase Name: #{timescaledb_dbname}"
  puts "Timescaledb Batch Size: #{timescaledb_batch_size}"
  puts "Time bucket size: #{time_bucket_size}"
  puts "Transporter Timeout: #{transporter_timeout}"
  puts "Time Addjustment: #{time_adjustment}"
  puts "Query Log Buffer Size: #{querylog_size}"
end

## Configure timescaledb connection
Granite::Connections << Granite::Adapter::Pg.new(name: "timescaledb", url: timescaledb_url)

## Load Granite here as it require the above statement before load
require "granite"
require "granite/adapter/pg"

# Model
class SecurifiQueryLog < Granite::Base
  connection timescaledb
  table securifi_query_log

  column time : Time, primary: true, auto: false
  column value : Int64
  column node : String
  column view : String
  column qname : String
  column qtype : String
  column client_ip : String
  column iface_ip : String
  column country : String, null: true 
end

# Functions
#def

# Main code below here. Beware!!

#The Transporter
# Save data uploader worker
# spawn do
#   puts "[Transporter] Starting save data uploader worker.." 
#   while true
#       #Get the oldest datapoint from the save_path 
#       if Dir.children(save_path).empty?
#           puts "[Transporter] No saved datapoint found. Wating for #{transporter_timeout} seconds till next check."
#           sleep transporter_timeout
#           next
#       end
#       saved_datapoint = Dir.children(save_path).sort.first.to_u32?
#       saved_datapoint_file = save_path + "/#{saved_datapoint}"
#       #Check if the saved_datapoint is a file. 
#       if File.file?(saved_datapoint_file)
#           #check if the saved datapoint has valid time. 
#           if ! saved_datapoint.nil? && saved_datapoint.is_a?(UInt32)
#               begin
#                   datapoint_timestamp = Time.unix(saved_datapoint)
#               rescue e
#                   if info_log
#                       puts "Exception:#{e}"
#                   end
#                   puts "[Transporter] Invalid datapoint timestamp. Removing file."
#                   begin
#                       File.delete(saved_datapoint_file)
#                   rescue e
#                       if info_log
#                           puts "Exception:#{e}"
#                       end
#                       puts "[Transporter] File not exist. Skipping.."
#                   end
#                   next
#               end
#               #Check if the timescaledb is available or not. 
#               begin
#                   sock.connect(timescaledb_host, timescaledb_port)
#               rescue e
#                   if info_log
#                       puts "Exception:#{e}"
#                   end
#                   if connection_error == force_restart_count
#                       puts "Detecting too much failure in connection shutting down.."
#                       exit 0
#                   else
#                       connection_error += 1
#                   end
#                   puts "[Transporter] Failed to connect to the timescaledb. Retrying after #{transporter_timeout} seconds."
#                   sleep transporter_timeout
#                   next
#               end
#               #Parse the JSON file of the saved_datapoint
#               saved_datapoint_json=File.open(saved_datapoint_file) do |file|
#                   JSON.parse(file)
#               end
#               if saved_datapoint_json
#                   saved_data = saved_datapoint_json.as_h?
#               else
#                   saved_data = nil
#               end
#               begin
#                   if ! saved_data.nil?
#                       data_counter = 0
#                       save_points = [] of BindQueryLog
#                       saved_data.each do |qname, qdata|
#                           next if qname.bytesize > 253
#                           qdata.as_h.each do |qchecksum, qinfo|
#                               if info_log
#                                   puts "[Transporter] #{qname} - key: #{qinfo}"
#                               end
#                               client_ip = qinfo["metadata"]["client_ip"].as_s.split("/").first.split("%").first
#                               view = qinfo["metadata"]["view"].as_s
#                               qtype = qinfo["metadata"]["query_type"].as_s
#                               iface_ip = qinfo["metadata"]["iface_ip"].as_s.split("/").first.split("%").first
#                               count = qinfo["count"].as_s.to_i64
#                               country = ""
#                               #lookup ips in database
#                               begin
#                                   geoIP = geoIP_db.country("#{client_ip}")
#                                   country = geoIP.country.iso_code.to_s
#                               rescue e
#                                   if info_log
#                                       puts "Exception:#{e}"
#                                       puts "[Transporter] [#{client_ip}] unknown ip or not in database"
#                                   end
#                                   country = "Unknown"
#                               end
#                               if info_log
#                                   puts "[Transporter] view: #{view} query: #{qname} RR: #{qtype} client: #{client_ip} iface: #{iface_ip} country: #{country} value: #{count}"
#                               end
#                               save_points << BindQueryLog.new(time: datapoint_timestamp.to_utc, value: count, node: hostname, view: view, qname: qname, qtype: qtype, client_ip: client_ip, iface_ip: iface_ip, country: country)
#                               data_counter += 1
#                           end
#                       end
#                       transporter_start = Time.local
#                       puts "[Transporter] Curent time start sending data #{transporter_start}"
#                       timescaledb_batch_size ? BindQueryLog.import(save_points, batch_size: timescaledb_batch_size) : BindQueryLog.import(save_points)
#                       #Successfull upload data point. removing the point.
#                       puts "[Transporter] Successfully upload [#{data_counter}] datapoint: #{saved_datapoint} to timescaledb"
#                       transporter_end = Time.local
#                       puts "[Transporter] Curent time finish sending data #{transporter_end}"
#                       puts "[Transporter] Time taken to send data to timescale #{transporter_end - transporter_start}"
#                       #cleanup 
#                       save_points = nil
#                       saved_data = nil
#                   end
#               rescue e
#                   puts "[Transporter] Exception:#{e}"
#                   if connection_error == force_restart_count
#                       puts "Detecting too much failure in connection shutting down.."
#                       exit 0
#                   else
#                       connection_error += 1
#                   end
#                   puts "[Transporter] Failed to upload data to timescaledb. Retrying after #{transporter_timeout} seconds. "
#                   sleep transporter_timeout
#                   next
#               end
#               #Successfull upload data point. removing the point.
#               puts "[Transporter] Successfully upload [#{data_counter}] datapoint from: #{saved_datapoint} to timescaledb"
#               begin
#                   File.delete(saved_datapoint_file)
#               rescue e
#                   if info_log
#                       puts "[Transporter] Exception:#{e}"
#                   end
#                   puts "[Transporter] File not exist. Skipping.."
#               end
#           end
#       else
#           puts "[Transporter] Invalid file type present in #{save_path}"
#           puts "[Transporter] Removing..#{saved_datapoint_file}"
#           begin
#               File.delete(saved_datapoint_file)
#           rescue e
#               if info_log
#                   puts "[Transporter] Exception:#{e}"
#               end
#               puts "[Transporter] File not exist. Skipping.."
#           end
#       end
#       sleep 1
#   end
#   puts "[Transporter] Something when wrong!! Should not reach here."
# end

# Start main loop..
fiber_id = 0
# The Distributer
while true
    # Get current time so we can calculate the time bucket based on current time. convert current time to epoch.
    cur_time = Time.local
    cur_epoch_time = cur_time.to_unix
    cur_time_sec = cur_time.second
    time_bucket_end = cur_epoch_time - cur_time_sec
    time_bucket_start = time_bucket_end - time_bucket_size
    time_bucket_end = time_bucket_end - time_adjustment
    bucket_time = (time_bucket_start - (time_bucket_size/2)).to_i
    if info_log
        puts "[Distributer]"
        puts cur_time
        puts cur_time_sec
        puts cur_epoch_time
        puts time_bucket_end
        puts time_bucket_start
        puts bucket_time
        puts Time.unix(time_bucket_end).to_local
        puts Time.unix(time_bucket_start).to_local
        puts Time.unix(bucket_time).to_local
    end

    #The Worker
    # Spawn a fiber to do the processing based on the time bucket calculated.
    puts "[Distributer] Spawning new worker fiber."
    spawn do
        # log_line = 0
        # process_line = 0
        # bad_log_line = 0
        # unknown_log_line = 0
        # fiber_id += 1
        # cur_fiber_id = fiber_id
        # fiber_start = Time.local
        # puts "[Worker] Curent time Fiber[#{cur_fiber_id}] start #{fiber_start}"
        # puts "[Worker] [Fiber #{cur_fiber_id}] Bucket start time #{time_bucket_start}"
        # puts "[Worker] [Fiber #{cur_fiber_id}] Bucket end time #{time_bucket_end}"
        # if File.file?("#{query_log_path}")
        #     data = {} of String => Hash(String,Hash(String, Hash(String, String) | Int64))
        #     File.each_line("#{query_log_path}") do |line|
        #         #hashed_line = %(#{line})
        #         log_line += 1
        #         begin
        #             log_data = {} of String => String
        #             #break the line
        #             splited_line = line.split(" ")
        #             if splited_line.size == 13
        #                 datetime = splited_line[0] + " " + splited_line[1]
        #                 #"14-Nov-2019 22:15:46.570"
        #                 time_stamp = Time.parse_local(datetime, "%d-%b-%Y %T.%L" ).to_unix
        #                 query = splited_line[8]
        #                 next if query.bytesize > 253
        #                 log_data = {"client_ip" => splited_line[5].split("#").first, "view" => "_BIND", "query_type" => splited_line[10], "iface_ip" => splited_line[12].delete("()").split("/").first.split("%").first}
        #             elsif splited_line.size == 14
        #                 datetime = splited_line[0] + " " + splited_line[1]
        #                 #"14-Nov-2019 22:15:46.570"
        #                 time_stamp = Time.parse_local(datetime, "%d-%b-%Y %T.%L" ).to_unix
        #                 query = splited_line[9]
        #                 next if query.bytesize > 253
        #                 log_data = {"client_ip" => splited_line[4].split("#").first, "view" => splited_line[7].delete(":"), "query_type" => splited_line[11], "iface_ip" => splited_line[13].delete("()").split("/").first.split("%").first}
        #             elsif splited_line.size == 15
        #                 datetime = splited_line[0] + " " + splited_line[1]
        #                 #"14-Nov-2019 22:15:46.570"
        #                 time_stamp = Time.parse_local(datetime, "%d-%b-%Y %T.%L" ).to_unix
        #                 query = splited_line[10]
        #                 next if query.bytesize > 253
        #                 log_data = {"client_ip" => splited_line[5].split("#").first, "view" => splited_line[8].delete(":"), "query_type" => splited_line[12], "iface_ip" => splited_line[14].delete("()").split("/").first.split("%").first}
        #             elsif splited_line.size == 16
        #                 datetime = splited_line[0] + " " + splited_line[1]
        #                 #"14-Nov-2019 22:15:46.570"
        #                 time_stamp = Time.parse_local(datetime, "%d-%b-%Y %T.%L" ).to_unix
        #                 query = splited_line[11]
        #                 next if query.bytesize > 253
        #                 log_data = {"client_ip" => splited_line[5].split("#").first, "view" => splited_line[9].delete(":"), "query_type" => splited_line[13], "iface_ip" => splited_line[15].delete("()").split("/").first.split("%").first}
        #             elsif splited_line.size == 17
        #                 datetime = splited_line[0] + " " + splited_line[1]
        #                 #"14-Nov-2019 22:15:46.570"
        #                 time_stamp = Time.parse_local(datetime, "%d-%b-%Y %T.%L" ).to_unix
        #                 query = splited_line[10]
        #                 next if query.bytesize > 253
        #                 log_data = {"client_ip" => splited_line[5].split("#").first, "view" => splited_line[8].delete(":"), "query_type" => splited_line[12], "iface_ip" => splited_line[14].delete("()").split("/").first.split("%").first}
                    
        #             else
        #                 if info_log
        #                     puts "[Worker] [Fiber #{cur_fiber_id}] Unkown log data.. Skipping.."
        #                 end
        #                 unknown_log_line += 1
        #                 unknown_log_line_debug << line
        #                 next
        #             end
        #             if info_log
        #                 puts "[Worker] [Fiber #{cur_fiber_id}] Log line #{log_line}:#{log_data}"
        #             end
        #         rescue e
        #             if info_log
        #                 puts "[Worker] [Fiber #{cur_fiber_id}] Exception:#{e}"
        #             end
        #             if info_log
        #             puts "[Worker] [Fiber #{cur_fiber_id}] Bad log data.. Skipping.."
        #             end
        #             bad_log_line += 1
        #             next
        #         end
        #         if info_log
        #             puts "[Worker] [Fiber #{cur_fiber_id}] Log line timestamp: #{time_stamp}"
        #         end
        #         if time_stamp >= time_bucket_start && time_stamp <= time_bucket_end
        #             checksum = Digest::MD5.hexdigest("#{query.downcase + ':' + log_data["client_ip"] + ':' + log_data["view"] + ':' + log_data["query_type"] + ':' + log_data["iface_ip"]}")
        #             if info_log
        #                 puts "[Worker] [Fiber #{cur_fiber_id}]"
        #                 puts time_stamp
        #                 puts log_data["client_ip"]
        #                 puts log_data["view"]
        #                 puts query
        #                 puts log_data["query_type"]
        #                 puts log_data["iface_ip"]
        #             end
        #             process_line += 1
        #             if data.empty?
        #                 data["#{query.downcase}"] = {"#{checksum}" =>{"metadata" => log_data, "count" => 1.to_i64}}
        #             elsif !data.has_key?("#{query.downcase}")
        #                 data["#{query.downcase}"] = {"#{checksum}" =>{"metadata" => log_data, "count" => 1.to_i64}}
        #             elsif !data["#{query.downcase}"].has_key?(checksum)
        #                 data["#{query.downcase}"].merge!({"#{checksum}" =>{"metadata" => log_data, "count" => 1.to_i64}})
        #             else
        #                 count = data["#{query.downcase}"][checksum]["count"].as(Int64)
        #                 data["#{query.downcase}"].merge!({"#{checksum}" =>{"metadata" => log_data, "count" => count +1 }})
        #             end
        #         end
        #     end
        #     puts "[Worker] [Fiber #{cur_fiber_id}] Truncate log file.. "
        #     File.open "#{query_log_path}", "w+" { }
        #     puts "[Worker] [Fiber #{cur_fiber_id}] Total log line: #{log_line}"
        #     puts "[Worker] [Fiber #{cur_fiber_id}] Total processed line: #{process_line}"
        #     puts "[Worker] [Fiber #{cur_fiber_id}] Total unkown line: #{unknown_log_line}"
        #     puts "[Worker] [Fiber #{cur_fiber_id}] Total bad line: #{bad_log_line}"
        #     worker_end = Time.local
        #     puts "[Worker] [Fiber #{cur_fiber_id}] Curent time finish globing data #{worker_end}"
        #     puts "[Worker] [Fiber #{cur_fiber_id}] Time taken to glob data #{worker_end - fiber_start}"
        #     if info_log
        #         puts "[Worker] [Fiber #{cur_fiber_id}] Query: #{data}"
        #         puts "[Worker] [Fiber #{cur_fiber_id}] Unknown Log Line: #{unknown_log_line_debug}"
        #     end

        #     #The Sender
        #     begin
        #         if ! data.empty?
        #             data_counter = 0
        #             points = [] of BindQueryLog
        #             data.each do |qname, qdata|
        #                 next if qname.bytesize > 253
        #                 qdata.each do |qchecksum, qinfo|
        #                     client_ip = qinfo["metadata"].as(Hash)["client_ip"]
        #                     if client_ip == "127.0.0.1"
        #                         next if discard_local
        #                     end
        #                     view = qinfo["metadata"].as(Hash)["view"]
        #                     qtype = qinfo["metadata"].as(Hash)["query_type"]
        #                     iface_ip = qinfo["metadata"].as(Hash)["iface_ip"]
        #                     count = qinfo["count"].as(Int64)
        #                     country = ""
        #                     #lookup ips in database
        #                     begin
        #                         geoIP = geoIP_db.country("#{client_ip}")
        #                         country = geoIP.country.iso_code.to_s
        #                     rescue e
        #                         if info_log
        #                             puts "[Sender] [Fiber #{cur_fiber_id}] Exception:#{e}"
        #                             puts "[Sender] [Fiber #{cur_fiber_id}] [#{client_ip}] unknown ip or not in database"
        #                         end
        #                         country = "Unknown"
        #                     end
        #                     if info_log
        #                         puts "[Sender] [Fiber #{cur_fiber_id}] view: #{view} query: #{qname} RR: #{qtype} client: #{client_ip} iface: #{iface_ip} country: #{country} value: #{count}"
        #                     end
        #                     points << BindQueryLog.new(time: Time.unix(bucket_time).to_utc, value: count.to_i64, node: hostname, view: view, qname: qname, qtype: qtype, client_ip: client_ip, iface_ip: iface_ip, country: country)
        #                     data_counter += 1
        #                 end
        #             end
        #             sender_start = Time.local
        #             puts "[Sender] [Fiber #{cur_fiber_id}] Curent time start sending data #{sender_start}"
        #             timescaledb_batch_size ? BindQueryLog.import(points, batch_size: timescaledb_batch_size) : BindQueryLog.import(points)
        #             puts "[Sender] [Fiber #{cur_fiber_id}] Successfully upload [#{data_counter}] datapoint #{Time.unix(bucket_time).to_local} to timescaledb."
        #             sender_end = Time.local
        #             puts "[Sender] [Fiber #{cur_fiber_id}] Curent time finish sending data #{sender_end}"
        #             puts "[Sender] [Fiber #{cur_fiber_id}] time taken to send data to timescale #{sender_end - sender_start}"
        #         end
        #     rescue e
        #         puts "[Sender] Exception:#{e}"
        #         puts "[Sender] Exception backtrace:#{e.backtrace?}"
        #         puts "[Sender] [Fiber #{cur_fiber_id}] Failed to connect to timescaledb.."
        #         puts "[Sender] [Fiber #{cur_fiber_id}] Saving to file.."
        #         unless data.empty?
        #         json_data = data.to_json
        #         end
        #         begin
        #         File.write("#{save_path}/#{bucket_time}", json_data)
        #             puts "[Sender] [Fiber #{cur_fiber_id}] Data successfully saved to #{save_path}/#{bucket_time}"
        #         rescue e
        #             if info_log
        #                 puts "[Sender] [Fiber #{cur_fiber_id}] Exception:#{e}"
        #             end
        #             puts "[Sender] [Fiber #{cur_fiber_id}] Failed to save data to #{save_path}/#{bucket_time}"
        #             puts "[Sender] [Fiber #{cur_fiber_id}] Data will be purge.."
        #         end
        #     end
        # else
        #     puts "[Worker] Log file #{query_log_path} not exist.. we may try again on next fiber."
        # end
        # fiber_end = Time.local
        # puts "[Worker] [Fiber #{cur_fiber_id}] Current time finish #{fiber_end}"
        # puts "[Worker] [Fiber #{cur_fiber_id}] Elapse time #{fiber_end - fiber_start}"
        # #cleanup
        # data = nil
        # points = nil
        # json_data = nil
    end
    puts "[Distributer] Waiting and syncronizing time. Spawning new fibers in #{(time_bucket_size - cur_time_sec)} seconds"
    sleep (time_bucket_size - cur_time_sec)
end
