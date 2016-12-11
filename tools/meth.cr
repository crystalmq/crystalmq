require "socket"
require "msgpack"
require "option_parser"
require "./lib.cr"

consumer_benchmark = false
producer_benchmark = false
latency_benchmark = false

topic = ""
channel = ""

OptionParser.parse! do |parser|
  parser.banner = "Usage: router.cr [arguments]"
  parser.on("--consumer", "Consumer Benchmark") { consumer_benchmark = true }
  parser.on("--producer", "Producer Benchmark") { producer_benchmark = true }
  parser.on("--latency", "Latency Benchmark") { latency_benchmark = true }
  
  parser.on("-t TOPIC", "--topic=TOPIC", "Specify the Topic") { |t| topic = t }
  parser.on("-c CHANNEL", "--channel=CHANNEL", "Specify the Message") { |c| channel = c }
  parser.on("-h", "--help", "Show this help") { puts parser; exit 0 }
end

if consumer_benchmark
  total_start = Time.now
  total_messages_received = 1

  spawn do
    CMQ::Consumer.new("localhost", topic, channel).consume do |message|
      total_start = Time.now if total_messages_received == 1
      total_messages_received = total_messages_received + 1
    end
  end

  loop do
    request_per_second = total_messages_received/(Time.now - total_start).to_f
    if request_per_second == 0
      rps = 0
    else
      rps = request_per_second.to_i
    end
    
    print "Received #{total_messages_received} in #{(Time.now - total_start).to_i}s (#{rps}/sec)\r"
    sleep 0.5
  end
end

if producer_benchmark
  producer = CMQ::Producer.new("localhost", topic)
  
  total_messages_sent = 0
  total_start = Time.now
  
  spawn do
    loop do
      total_messages_sent = total_messages_sent + 1
      producer.write("This is a test string")
    end
  end

  loop do
    request_per_second = total_messages_sent/(Time.now - total_start).to_f
    if request_per_second == 0
      rps = 0
    else
      rps = request_per_second.to_i
    end

    print "Sent #{total_messages_sent} in #{(Time.now - total_start).to_i}s (#{rps}/sec)\r"
    sleep 0.5
  end
end

if latency_benchmark
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
end

