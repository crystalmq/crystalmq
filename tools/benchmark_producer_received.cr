require "socket"
require "./lib.cr"

channel = ARGV[0]
message = ARGV[1]

producer = CMQ::Producer.new("localhost", ARGV[0])
  
start = Time.now

spawn do
  CMQ::Consumer.new("localhost", ARGV[0], ARGV[1]).consume do |message|
    puts "Message Received #{(Time.now - start).to_f * 1000} since start"
  end
end
  
loop do
  start = Time.now
  producer.write("This is a test string")
  sleep 0.01
end

producer.terminate