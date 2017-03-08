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
      @maximum_requests_per_second = 1000000
    
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
  
  class TopicsContainer
    def initialize
      @mutex = Mutex.new
      @topics = [] of MessageRouter::Topic
    end
  
    def topics
      synchronize do
        @topics
      end
    end
  
    def synchronize
      @mutex.synchronize { yield }
    end
  end
  
  CONFIGURATION = Configuration.new
end

producer_server = TCPServer.new(MessageRouter::CONFIGURATION.producer_port)
consumer_server = TCPServer.new(MessageRouter::CONFIGURATION.consumer_port)

# Create the core "Repository" of active topics


tc = MessageRouter::TopicsContainer.new


# Spawn the Consumer Socket
spawn do
  loop do
    puts "CONSUMER[core] Listening For Consumer Socket on ::#{MessageRouter::CONFIGURATION.consumer_port}" if MessageRouter::CONFIGURATION.debug
    socket = consumer_server.accept
    socket.sync = true
    puts "CONSUMER[core] Accepted New Socket #{socket.fd}" if MessageRouter::CONFIGURATION.debug
    
    spawn do
      cs = MessageRouter::ConsumerHandler.new(tc)
      loop do
        begin
          message = MessageRouter::ConsumerPayload.from_msgpack(socket)
          cs.handle_message(message, socket)
        rescue MessagePack::UnpackException
          socket.close
          cs.remove_client(socket)
          break
        rescue ex : Errno
          if ex.errno == Errno::ECONNRESET
            puts "CONSUMER[#{socket.fd}] SOCKET FAILURE (Connection Reset By Peer)"
            socket.close
            break
          else
            puts "CONSUMER[#{socket.fd}] #{ex.errno} FAILURE???"
            socket.close
            break
            #raise ex
          end
        end
      end
      
      puts "CONSUMER[#{socket.fd}] SHUTTING DOWN"
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
      ps = MessageRouter::ProducerHandler.new(tc)
      
      maximum_requests_per_second = MessageRouter::CONFIGURATION.maximum_requests_per_second
      current_requests = 0
      current_second = 0
      total_start = Time.now
      
      loop do
        begin
          if current_second == (Time.now - total_start).to_i
            current_requests = current_requests + 1
            if current_requests > maximum_requests_per_second
              #puts "PRODUCER[#{socket.fd}] BACKOFF!" if MessageRouter::CONFIGURATION.debug
              Fiber.yield # Give up the resources to another fiber
            end
            message = MessageRouter::ProducerPayload.from_msgpack(socket)
            ps.handle_message(message, socket)
          else
            current_second = (Time.now - total_start).to_i
            current_requests = 0
          end
        rescue MessagePack::UnpackException
          socket.close
          break
        rescue ex : Errno
          if ex.errno == Errno::ECONNRESET
            puts "PRODUCER[#{socket.fd}] SOCKET FAILURE (Connection Reset By Peer)"
            socket.close
            break
          else
            puts "PRODUCER[#{socket.fd}] #{ex.errno} FAILURE???"
            socket.close
            break
            #raise ex
          end
        end
      end
      
      puts "PRODUCER[#{socket.fd}] SHUTTING DOWN"
    end
  end  
end

# Loop forever, to keep the daemon running
loop do
  sleep 1
  ## Infinite
end
