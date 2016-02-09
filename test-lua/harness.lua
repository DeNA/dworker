redis = require 'redis'
cjson = require 'cjson'
 
KEYS = {}
ARGV = {}
 
-- If you have some different host/port change it here
local host = "127.0.0.1"
local port = 6379
 
client = redis.connect(host, port)
 
-- Workaround for absence of redis.call
redis.call = function(cmd, ...)
  cmd = string.lower(cmd)
  local result = assert(loadstring('return client:'.. cmd ..'(...)'))(...)
 
  -- The redis-lua library returns some values differently to how `redis.call` works inside redis.
  -- this makes the responses look like those from the builtin redis
  local response_lookup = {
    type = function() return { ["ok"]= result } end,
    sadd = function() return tonumber(result) end,
    zrange = function()
      if type(result) == "table" and type(result[1]) == "table" then
        -- Deal with WITHSCORES...
        local new_result = {}
        for k,v in pairs(result) do
          table.insert(new_result, v[1])
          table.insert(new_result, v[2])
        end
        return new_result;
      end
 
      return result;
    end,
    zrank = function()
      if result == nil then
        return false;
      end

      return result;
    end
  }
 
  if response_lookup[cmd] then
    return response_lookup[cmd]()
  end
 
  return result;
end

-- Workaround for absence of redis.log
redis.LOG_DEBUG = "DEBUG"
redis.LOG_VERBOSE = "VERBOSE"
redis.LOG_NOTICE = "NOTICE"
redis.LOG_WARNING = "WARNING"
redis.log = function (level, message)
    print("[" .. level .. "]: " .. message)
end
 
function call_redis_script(script, keys, argv)
  -- This may not be strictly necessary 
  for k,v in pairs(ARGV) do ARGV[k] = nil end
  for k,v in pairs(KEYS) do KEYS[k] = nil end
 
  for k,v in pairs(keys) do table.insert(KEYS, v) end
  for k,v in pairs(argv) do table.insert(ARGV, v)  end
 
  return assert(dofile(script))
end
 
return call_redis_script;
