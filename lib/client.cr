class MessageRouter
  class Client
    getter :socket
    def initialize(socket : Socket)
      @socket = socket
      @dead = false
    end
  
    def send_message(message : String)
      @socket.write(MessageRouter::MessagePayload.new(message).to_msgpack)
      puts "CONSUMER[#{@socket.fd}] Message Delievered." if MessageRouter::CONFIGURATION.debug
    end
    
    def mark_as_dead
      @dead = true
      puts "CONSUMER[#{@socket.fd}] DEAD CLIENT" if MessageRouter::CONFIGURATION.debug
    end
    
    def dead
      @dead
    end

  end
end