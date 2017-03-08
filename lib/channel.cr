class MessageRouter
  class Channel
    getter :name
    getter :clients
    
    def initialize(name : String)
      @name = name
      @clients = [] of MessageRouter::Client
    end
  
    def add_client(client : MessageRouter::Client)
      @clients << client
    end
    
    def remove_client_with_socket(socket : TCPSocket)
      @clients.reject! { |c| c.socket.fd == socket.fd }
    end
  
    def send_message(message : String)
      begin
        if @clients.size == 0
          puts "No Clients..." if MessageRouter::CONFIGURATION.debug
        else
          selected_client = @clients.sample(1)[0]
          puts "CONSUMER[#{selected_client.socket.fd}] Sending Message to #{@name}." if MessageRouter::CONFIGURATION.debug
          selected_client.send_message(message)
        end
      rescue
        ## Client is dead!
        unless selected_client.nil?
          selected_client.mark_as_dead
        end
        @clients.reject! { |c| c.dead }
        
        if @clients.size == 0
          puts "ERROR: Run out of clients" if MessageRouter::CONFIGURATION.debug
          # Do nothing
        else
          # Get another
          selected_client = @clients.sample(1)[0]
          selected_client.send_message(message)
        end
      end
    end
  end
end