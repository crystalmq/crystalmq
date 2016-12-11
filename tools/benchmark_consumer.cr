require "socket"
require "./lib.cr"

topic = ARGV[0]
channel = ARGV[1]
  
start = Time.now
total_start = Time.now
total_messages_received = 1

spawn do
  CMQ::Consumer.new("localhost", topic, channel).consume do |message|
    total_messages_received = total_messages_received + 1
  end
end

loop do
  print "Received #{total_messages_received} in #{(Time.now - total_start).to_f}s (#{total_messages_received/(Time.now - total_start).to_f}/sec)\r"
  sleep 0.5
end
