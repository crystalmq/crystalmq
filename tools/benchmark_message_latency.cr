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
    print "\rMessage Received #{(Time.now - start).to_f * 1000 * 1000}μs\n"
  end
end


producer = CMQ::Producer.new("localhost", topic)

loop do
  start = Time.now
  producer.write("This is a test string")
  sleep 0.1
end

producer.terminate
