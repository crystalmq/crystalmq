# Usage crystal router.cr false
# NOTE: With DEBUG flag set to TRUE, speed will decrease by 50%

require "socket"
require "msgpack"
require "option_parser"

require "./lib/channel.cr"
require "./lib/client.cr"
require "./lib/consumer_handler.cr"
require "./lib/producer_handler.cr"
require "./lib/topic.cr"
require "./lib/msgpack_definitions.cr"

class MessageRouter
  class Configuration
    getter :consumer_port, :producer_port, :debug, :maximum_requests_per_second
    
    def initialize
      @debug = false
      @consumer_port = 1235
      @producer_port = 1234
      @maximum_requests_per_second = 50000
    
      OptionParser.parse! do |parser|
        parser.banner = "Usage: router.cr [arguments]"
        parser.on("-d", "--upcase", "Debug") { @debug = true }
        parser.on("-c PORT", "--consumer-port=PORT", "Specifies the Consumer Port") { |port| @consumer_port = port.to_i }
        parser.on("-p PORT", "--producer-port=PORT", "Specifies the Producer Port") { |port| @producer_port = port.to_i }
        parser.on("-m RPS", "--maximum-requests-per-second=RPS", "Specifies the Maximum Requests Per Second") { |rps| @maximum_requests_per_second = rps.to_i }
        parser.on("-h", "--help", "Show this help") { puts parser; exit 0 }
      end
    end
  end
  
  CONFIGURATION = Configuration.new
end

producer_server = TCPServer.new(MessageRouter::CONFIGURATION.producer_port)
consumer_server = TCPServer.new(MessageRouter::CONFIGURATION.consumer_port)

# Create the core "Repository" of active topics
topics = [] of MessageRouter::Topic

# Spawn the Consumer Socket
spawn do
  loop do
    puts "CONSUMER[core] Listening For Consumer Socket on ::#{MessageRouter::CONFIGURATION.consumer_port}" if MessageRouter::CONFIGURATION.debug
    socket = consumer_server.accept
    socket.sync = true
    puts "CONSUMER[core] Accepted New Socket #{socket.fd}" if MessageRouter::CONFIGURATION.debug
    
    spawn do
      cs = MessageRouter::ConsumerHandler.new(topics)
      loop do
        begin
          puts "CONSUMER[#{socket.fd}] Listening for message..." if MessageRouter::CONFIGURATION.debug
          message = MessageRouter::ConsumerPayload.from_msgpack(socket)
          cs.handle_message(message, socket)
          puts "CONSUMER[#{socket.fd}] Handled message #{message}" if MessageRouter::CONFIGURATION.debug
        rescue MessagePack::UnpackException
          socket.close
          cs.remove_client(socket)
          break
        end
      end
    end
  end  
end

# Spawn the Producer Socket
spawn do
  loop do
    puts "PRODUCER[core] Listening For Producer Socket on ::#{MessageRouter::CONFIGURATION.producer_port}" if MessageRouter::CONFIGURATION.debug
    socket = producer_server.accept
    socket.sync = true
    puts "PRODUCER[core] Accepted New Socket #{socket.fd}" if MessageRouter::CONFIGURATION.debug
    
    spawn do
      ps = MessageRouter::ProducerHandler.new(topics)
      
      maximum_requests_per_second = MessageRouter::CONFIGURATION.maximum_requests_per_second
      current_requests = 0
      current_second = 0
      total_start = Time.now
      
      loop do
        puts "PRODUCER[#{socket.fd}] Listening for message..." if MessageRouter::CONFIGURATION.debug
        begin
          if current_second == (Time.now - total_start).to_i
            current_requests = current_requests + 1
            if current_requests > maximum_requests_per_second
              puts "PRODUCER[#{socket.fd}] BACKOFF!" if MessageRouter::CONFIGURATION.debug
              Fiber.yield # Give up the resources to another fiber
            end
            message = MessageRouter::ProducerPayload.from_msgpack(socket)
            ps.handle_message(message, socket)
            puts "PRODUCER[#{socket.fd}] Handled message" if MessageRouter::CONFIGURATION.debug
          else
            current_second = (Time.now - total_start).to_i
            current_requests = 0
          end
        rescue MessagePack::UnpackException
          socket.close
          break
        rescue
          ps.handle_message(message, socket)
        end
      end
    end
  end  
end

# Loop forever, to keep the daemon running
loop do
  sleep 1
  ## Infinite
end
