require "socket"
require "./lib.cr"

topic = ARGV[0]

producer = CMQ::Producer.new("localhost", topic)
  
total_messages_sent = 0
total_start = Time.now

spawn do
  loop do
    total_messages_sent = total_messages_sent + 1
    producer.write("This is a test string")
    sleep 0.000001
  end
end

loop do
  print "Sent #{total_messages_sent} in #{(Time.now - total_start).to_f}s (#{total_messages_sent/(Time.now - total_start).to_f}/sec)\r"
  sleep 0.5
end

producer.terminate
