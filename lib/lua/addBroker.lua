-- Input parameters
--   KEYS[1] global hash key. (gh)
--   KEYS[2] worker list hash. (wh)
--   KEYS[3] broker list hash. (bh)
--   KEYS[4] per cluster broker ID & load(as score) sorted sets (cz)
--   KEYS[5] per cluster broker ID & hash key (as score) sorted sets (bz)
--   KEYS[6] worker sets (wz)
--   KEYS[7] recovery sets (rz)
--   ARGV[1] Broker ID
--   ARGV[2] Pubsub channel prefix
--   ARGV[3] Current load of this broker
--   ARGV[4] Cluster name this broker belongs to
--   ARGV[5] Transport address in the form of '1.2.3.4:48377'.
--   ARGV[6] Hash key used in bz table as a score
-- Output parameters

-- Establish / update global fields
redis.call("HSET", KEYS[1], 'chPrefix', ARGV[2]) 

local brokerId = ARGV[1]
local load = ARGV[3]
local hashKey = ARGV[6]
local brInfo

redis.log(redis.LOG_DEBUG, "addBroker: brokerId=" .. brokerId)

local tmp = redis.call("HGET", KEYS[3], brokerId)
if tmp then
    if pcall(function () brInfo = cjson.decode(tmp) end) then
        redis.log(redis.LOG_DEBUG, "stale broker " .. brokerId .. " (" .. brInfo.st .. "). salvaging its workers.")

        -- Salvage START: salvages the workers with the dead broker
        -- This section is shared (manually copied) across the scripts
        local function salvage(kWh, kWz, kRz)
            while redis.call("ZCARD", kWz) > 0 do
                -- `res` is an array with one entry. res[1]:workerId, res[2]:time created at.
                local res = redis.call("ZRANGE", kWz, 0, 0, 'withscores')
                local workerId = res[1]
                local createdAt = res[2]
                local sInfo = redis.call("HGET", kWh, workerId)
                if sInfo then
                    local workerInfo
                    if pcall(function () workerInfo = cjson.decode(sInfo) end) then
                        if workerInfo.attributes.recoverable then
                            workerInfo.brokerId = nil;
                            redis.call("HSET", kWh, workerId, cjson.encode(workerInfo))
                            redis.call("ZADD", kRz, createdAt, workerId)
                            redis.call("HINCRBY", KEYS[1], 'workersSalvaged', 1)
                            redis.log(redis.LOG_DEBUG, "salvaging worker " .. workerId)
                        else
                            redis.log(redis.LOG_DEBUG, "removing (recoverable=false) worker " .. workerId)
                            redis.call("HDEL", kWh, workerId)
                            redis.call("HINCRBY", KEYS[1], 'workersRemoved', 1)
                        end
                    else
                        redis.call("HINCRBY", KEYS[1], 'workersBroken', 1)
                    end
                else
                    redis.call("HINCRBY", KEYS[1], 'workersBroken', 1)
                end
                redis.call("ZREM", kWz, workerId)
            end
        end
        -- Salvage COMPLETE

        salvage(KEYS[2], KEYS[6], KEYS[7])
    else
        redis.call("HINCRBY", KEYS[1], 'brokersBroken', 1)
    end
else
    redis.log(redis.LOG_DEBUG, "New broker " .. brokerId)
end

-- Clear worker sets under this broker ID, if any.
redis.call("DEL", KEYS[6])

-- Add this broker to bh as 'active'
brInfo = {["cn"]=ARGV[4], ["st"]="active", ["addr"]=ARGV[5]};
redis.call("HSET", KEYS[3], brokerId, cjson.encode(brInfo))

-- Add this broker to cz with its load
redis.call("ZADD", KEYS[4], load, brokerId)
redis.call("ZADD", KEYS[5], hashKey, brokerId)

-- Increment brokersAdded.
redis.call("HINCRBY", KEYS[1], 'brokersAdded', 1)

return {0}
