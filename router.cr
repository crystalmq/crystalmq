# Usage crystal router.cr false
# NOTE: With DEBUG flag set to TRUE, speed will decrease by 50%

require "socket"
require "msgpack"
require "./lib/channel.cr"
require "./lib/client.cr"
require "./lib/consumer_handler.cr"
require "./lib/producer_handler.cr"
require "./lib/topic.cr"
require "./lib/msgpack_definitions.cr"

class MessageRouter
  class Configuration
    getter :consumer_port, :producer_port, :debug
    
    def initialize      
      begin
        if ARGV.is_a?(Array)
          if ARGV.size > 0
            @debug = ARGV[0] == "debug" ? true : false
          end
          if ARGV.size > 1
            @producer_port = ARGV[1].to_i
          else
            @producer_port = 1234
          end
          if ARGV.size > 2
            @consumer_port = ARGV[2].to_i
          else
            @consumer_port = 1235
          end
        else
          @debug = false
          @producer_port = 1234
          @consumer_port = 1235
        end
      rescue e
        puts e.class
        puts e.message
        @debug = false
        @producer_port = 1234
        @consumer_port = 1235
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
    puts "CONSUMER[core] Listening For Consumer Socket" if MessageRouter::CONFIGURATION.debug
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
    puts "PRODUCER[core] Listening For Producer Socket" if MessageRouter::CONFIGURATION.debug
    socket = producer_server.accept
    socket.sync = true
    puts "PRODUCER[core] Accepted New Socket #{socket.fd}" if MessageRouter::CONFIGURATION.debug
    
    spawn do
      ps = MessageRouter::ProducerHandler.new(topics)
      loop do
        puts "PRODUCER[#{socket.fd}] Listening for message..." if MessageRouter::CONFIGURATION.debug
        begin
          message = MessageRouter::ProducerPayload.from_msgpack(socket)
          ps.handle_message(message, socket)
          puts "PRODUCER[#{socket.fd}] Handled message" if MessageRouter::CONFIGURATION.debug
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
