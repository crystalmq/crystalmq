class MessageRouter  
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
    
  class MessagePayload
    getter :message
    
    def initialize(message : String)
      @message = message
    end
    
    MessagePack.mapping({
      message: String
    })
  end
end