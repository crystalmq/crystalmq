class MessageRouter
  class Topic
    getter :name
    getter :channels
    
    def initialize(name : String)
      @name = name
      @channels = [] of Channel
    end
  
    def add_channel(channel : Channel)
      @channels << channel
    end
  
    def send_message(message)
      @channels.each { |c| spawn { c.send_message(message) } }
    end
  
  end
end