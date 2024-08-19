local delayedSetKey = KEYS[1]
local streamKey = KEYS[2]
local now = tonumber(ARGV[1])
local redis = redis

-- Get messages that are ready to be processed
local messages = redis.call('ZRANGEBYSCORE', delayedSetKey, 0, now)

local processed = 0
if #messages > 0 then
    for i = 1, #messages do
        local messageData = messages[i]
        
        -- Add message to the stream
        redis.call('XADD', streamKey, '*', 'message', messageData)
        
        -- Remove the processed message from the delayed set
        redis.call('ZREM', delayedSetKey, messageData)
        
        processed = processed + 1
    end
end

return processed
