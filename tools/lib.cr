require "msgpack"

class CMQ
  class MessagePayload
    getter :message
    
    def initialize(message : String)
      @message = message
    end
    
    MessagePack.mapping({
      message: String
    })
  end
  
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
      @topic = topic
      @channel = channel
    end
    
    def consume
      @socket.write(ConsumerPayload.new(@topic, @channel).to_msgpack)
      loop do
        message = MessagePayload.from_msgpack(@socket)
        yield message.message
      end
      @socket.close
    end
  end
  
  class Producer
    
    class ProducerPayload
      getter  :topic, :message
    
      def initialize(topic : String, message : String)
        @topic = topic
        @message = message
      end
    
      MessagePack.mapping({
        topic: String,
        message: String
      })
    end
    
    def initialize(host : String, topic : String)
      @socket = TCPSocket.new(host, 1234)
      @topic = topic
    end
  
    def write(message : String)
      @socket.write(ProducerPayload.new(@topic, message).to_msgpack)
    end
    
    def terminate
      @socket.close
    end
  end
end