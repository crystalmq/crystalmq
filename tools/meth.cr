require "socket"
require "msgpack"
require "option_parser"
require "./lib.cr"

consumer_benchmark = false
producer_benchmark = false
latency_benchmark = false
hostname = "localhost"

topic = "sunseries"
channel = "test-channel"

OptionParser.parse! do |parser|
  parser.banner = "Usage: router.cr [arguments]"
  parser.on("--consumer", "Consumer Benchmark") { consumer_benchmark = true }
  parser.on("--producer", "Producer Benchmark") { producer_benchmark = true }
  parser.on("--latency", "Latency Benchmark") { latency_benchmark = true }
  
  parser.on("-t TOPIC", "--topic=TOPIC", "Specify the Topic") { |t| topic = t }
  parser.on("-h HOSTNAME", "--hostname=HOSTNAME", "Specify the Hostname") { |h| hostname = h}
  parser.on("-c CHANNEL", "--channel=CHANNEL", "Specify the Message") { |c| channel = c }
  parser.on("-h", "--help", "Show this help") { puts parser; exit 0 }
end

puts "[meth] Using Hostname #{hostname}"
if consumer_benchmark
  puts "[meth] Using Topic #{topic}"
  puts "[meth] Using Channel #{channel}"
  total_start = Time.now
  total_messages_received = 1
  message_count = 1
  
  spawn do
    CMQ::Consumer.new(hostname, topic, channel).consume do |message|
      total_start = Time.now if total_messages_received == 1
      total_messages_received = total_messages_received + 1
      message_count = message_count + 1
    end
  end
  
  previous_second = 0
  start = Time.now

  loop do
    current_second = (Time.now - start).to_i
    if current_second == previous_second
      sleep 0.5
    else
      print "Received #{total_messages_received} in #{(Time.now - total_start).to_i}s (#{message_count}/sec)\r"      
      message_count = 0
      previous_second = current_second
    end
  end
end

if producer_benchmark
  puts "[meth] Using Topic #{topic}"

  producer = CMQ::Producer.new(hostname, topic)
  
  total_messages_sent = 0
  message_count = 0
  total_start = Time.now
  
  spawn do
    loop do
      total_messages_sent = total_messages_sent + 1
      message_count = message_count + 1
      producer.write("This is a test string")
      sleep 0.000001
    end
  end

  previous_second = 0
  start = Time.now

  loop do
    current_second = (Time.now - start).to_i
    if current_second == previous_second
      sleep 0.5
    else
      print "Sent #{total_messages_sent} in #{(Time.now - total_start).to_i}s (#{message_count}/sec)\r"      
      message_count = 0
      previous_second = current_second
    end
  end
end

if latency_benchmark
  start = Time.now
  total_start = Time.now
  total_messages_received = 1

  spawn do
    CMQ::Consumer.new(hostname, topic, channel).consume do |message|
      total_messages_received = total_messages_received + 1
      print "\rMessage Received #{(Time.now - start).to_f * 1000 * 1000}μs\n"
    end
  end

  producer = CMQ::Producer.new(hostname, topic)

  loop do
    start = Time.now
    producer.write("This is a test string")
    sleep 0.1
  end
end

