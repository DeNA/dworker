-- Input parameters
--   KEYS[1] global hash key. (gh)
--   KEYS[2] worker list hash. (wh) - unused
--   KEYS[3] broker list hash. (bh)
--   KEYS[4] per cluster broker ID & load(as score) sorted sets (cz)
--   KEYS[5] per cluster broker ID & hash key (as score) sorted sets (bz)
--   ARGV[1] Broker ID
-- Output parameters
--   format: [{number}resultCode, {string}msg]
--     resultCode 0: Success, msg: undefined
--     resultCode 1: Salvage issued, msg: undefined
--     resultCode 2: Error. msg: Error message

local chPrefix = redis.call("HGET", KEYS[1], 'chPrefix')
local brokerId
local tmp
local nBrokers
local nRank
local brInfo

-- Check the number of brokers
nBrokers = redis.call("ZCARD", KEYS[5])
if nBrokers == 0 then
    return {2, "No broker is in the bz table"}
end

-- Check the rack of the broker in the bz
nRank = redis.call("ZRANK", KEYS[5], ARGV[1])
if not nRank then
    return {2, "broker ID is not in the bz table: " .. ARGV[1]}
end

-- Bail out if no one else to do health check
if nBrokers == 1 then
    return {0}
end

-- Determine the next rank (wrapped)
nRank = (nRank + 1) % nBrokers

-- Get the next broker's ID
tmp = redis.call("ZRANGE", KEYS[5], nRank, nRank)

-- Sanity check
if #tmp ~= 1 then
    return {2, "Failed to get next broker's ID"}
end

brokerId = tmp[1]

-- Obtain broker info
tmp = redis.call('HGET', KEYS[3], brokerId)
if not tmp then
    redis.call("ZREM", KEYS[4], brokerId)
    redis.call("ZREM", KEYS[5], brokerId)
    return {2, "Broker is not present in bh: " .. brokerId}
end

if not pcall(function () brInfo = cjson.decode(tmp) end) then
    redis.call("HDEL", KEYS[3], brokerId)
    redis.call("ZREM", KEYS[4], brokerId)
    redis.call("ZREM", KEYS[5], brokerId)
    return {2, "Removing corrupted broker: " .. brokerId}
end

if brInfo.addr == nil then
    redis.call("HDEL", KEYS[3], brokerId)
    redis.call("ZREM", KEYS[4], brokerId)
    redis.call("ZREM", KEYS[5], brokerId)
    return {2, "Removing old version of broker: " .. brokerId}
end

-- Check if the broker is active
if brInfo.st == 'active' then
    -- Check connectivity with the broker
    local ch = redis.call("HGET", KEYS[1], 'chPrefix') .. ':' .. brokerId;
    local nrecvs = redis.call("PUBLISH", ch, "")
    if nrecvs == 0 then
        -- Broken pipe. Assume the broker is dead.
        -- Invalidate the broker so that other brokers will know that
        -- the salvage is already taking place.
        brInfo.st = 'invalid'
        redis.call('HSET', KEYS[3], brokerId, cjson.encode(brInfo))

        -- Remove the broker from cz and bz so that the broker won't be
        -- picked to create a new worker or to do health check again.
        redis.call("ZREM", KEYS[4], brokerId)
        redis.call("ZREM", KEYS[5], brokerId)

        -- Tell other brokers to salvage workers held by this dead broker
        local msg = {
            ["sig"]="salvage",
            ["clustername"]=brInfo.cn,
            ["brokerId"]=brokerId
        }
        redis.call("PUBLISH", chPrefix .. ':*', cjson.encode(msg))
        return {1}
    end
end

return {0}

