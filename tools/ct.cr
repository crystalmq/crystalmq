require "socket"
require "msgpack"
require "option_parser"
require "./lib.cr"

  class Consumer
    
    class ConsumerPayload
      getter :topic, :channel
      def initialize(topic : String, channel : String)
        @topic = topic
        @channel = channel
      end
      
      MessagePack.mapping({
        topic: String,
        channel: String
      })
    end
        
    def initialize(host : String, topic : String, channel : String)
      @socket = TCPSocket.new(host, 1235)
      @socket.sync = true
      @topic = topic
      @channel = channel
    end
    
    def consume
      @socket.write(ConsumerPayload.new(@topic, @channel).to_msgpack)
      loop do
		uint = Slice(UInt8).new(1024)
	  	@socket.read(uint)
		puts uint
		sleep 1
      end
      @socket.close
    end
  end


Consumer.new("172.16.1.250", "sunseries", "abcdef").consume
