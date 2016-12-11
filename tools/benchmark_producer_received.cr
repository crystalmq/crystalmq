require "socket"
require "./lib.cr"

channel = ARGV[0]
message = ARGV[1]

producer = CMQ::Producer.new("localhost", ARGV[0])
  
start = Time.now
total_start = Time.now
total_messages_received = 1

spawn do
  CMQ::Consumer.new("localhost", ARGV[0], ARGV[1]).consume do |message|
    total_messages_received = total_messages_received + 1
#    print "\rMessage Received #{(Time.now - start).to_f * 1000}ms\n"
    print "Received #{total_messages_received} in #{(Time.now - total_start).to_f}s (#{total_messages_received/(Time.now - total_start).to_f}/sec)\r"
    #puts total_messages_received/(Time.now - total_start).to_f
  end
end
  
loop do
  start = Time.now
  producer.write("This is a test string")
  sleep 0.000001
end

producer.terminate
