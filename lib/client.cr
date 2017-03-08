class MessageRouter
  class Client
    getter :socket
    def initialize(socket : Socket)
      @mutex = Mutex.new
      @socket = socket
      @dead = false
    end
  
    def synchronize
      @mutex.synchronize { yield }
    end
    
    def send_message(message : String)
      synchronize do
        @socket.write(MessageRouter::MessagePayload.new(message).to_msgpack)
        puts "CONSUMER[#{@socket.fd}] Message Delievered." if MessageRouter::CONFIGURATION.debug
      end
    end
    
    def mark_as_dead
      @dead = true
      synchronize do
        puts "CONSUMER[#{@socket.fd}] DEAD CLIENT" if MessageRouter::CONFIGURATION.debug
        @socket.close
      end
    end
    
    def dead
      @dead
    end

  end
end