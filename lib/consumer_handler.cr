class MessageRouter
  class ConsumerHandler
    getter :messages
    def initialize(topics : Array(MessageRouter::Topic))
      @topics = topics
      @messages = 0
    end
    
    def handle_message(message : MessageRouter::ConsumerPayload, socket : TCPSocket)
      @messages = @messages + 1
      topic_name, channel_name = message.topic, message.channel
      found_topic = @topics.find { |topic| topic.name == topic_name }
      if found_topic.nil?
        puts "CONSUMER[#{socket.fd}] >>> Topic #{topic_name} not found" if MessageRouter::CONFIGURATION.debug
        new_topic = Topic.new(topic_name)
        new_channel = Channel.new(channel_name)
        new_channel.add_client(Client.new(socket))
        new_topic.add_channel(new_channel)
        @topics << new_topic
        puts "CONSUMER[#{socket.fd}] >>> Topic #{topic_name} created" if MessageRouter::CONFIGURATION.debug
      else
        puts "CONSUMER[#{socket.fd}] >>> Found Topic #{topic_name}" if MessageRouter::CONFIGURATION.debug
        found_channel = found_topic.channels.find { |c| c.name == channel_name }
        if found_channel.nil?
          puts "CONSUMER[#{socket.fd}] >>>> Cant Find Channel #{channel_name} inside topic #{topic_name}" if MessageRouter::CONFIGURATION.debug
          new_channel = Channel.new(channel_name)
          new_channel.add_client(Client.new(socket))
          found_topic.add_channel(new_channel)
          puts "CONSUMER[#{socket.fd}] >>>> Channel #{channel_name} inside topic #{topic_name} created" if MessageRouter::CONFIGURATION.debug
        else
          puts "CONSUMER[#{socket.fd}] >>>> Found Channel #{channel_name} inside topic #{topic_name}" if MessageRouter::CONFIGURATION.debug
          found_channel.add_client(Client.new(socket))
        end
      end
      puts "CONSUMER[#{socket.fd}] >>>> Counsumer added to channel #{channel_name} inside topic #{topic_name}" if MessageRouter::CONFIGURATION.debug
    end
    
    def remove_client(socket)
      @topics.each do |topic|
        topic.channels.each do |channel|
          channel.remove_client_with_socket(socket)
        end
      end
    end
    
    def handle_message(message : Nil, socket : TCPSocket)
      puts "Got Message #{message}"
    end 
  end
end