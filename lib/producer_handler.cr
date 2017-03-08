class MessageRouter
  class ProducerHandler
    def initialize(topic_container : MessageRouter::TopicsContainer)
      @tc = topic_container
      @messages = 0
    end
    
    def handle_message(message : MessageRouter::ProducerPayload, socket : TCPSocket)
      @messages = @messages + 1
      puts "PRODUCER[#{socket.fd}] >>> Handling Message: #{message.message}" if MessageRouter::CONFIGURATION.debug
      topic_name, message = message.topic, message.message

      @tc.topics.reject! { |t| t.channels.size == 0 }
      found_topic = @tc.topics.find { |topic| topic.name == topic_name }
      if found_topic.nil?
        #puts "PRODUCER[#{socket.fd}] >>> Topic #{topic_name} not found" if MessageRouter::CONFIGURATION.debug
      else
        #puts "PRODUCER[#{socket.fd}] >>> Topic #{topic_name} found" if MessageRouter::CONFIGURATION.debug
        found_topic.send_message(message)
        #puts "PRODUCER[#{socket.fd}] >>> Message Sent to #{topic_name}" if MessageRouter::CONFIGURATION.debug
      end
      puts "Finished--------------" if MessageRouter::CONFIGURATION.debug
    end
    
    def handle_message(message : Nil, socket : TCPSocket)
      puts "Got NIL Message"
    end
    
  end
end