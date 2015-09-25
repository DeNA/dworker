-- Input parameters
--   KEYS[1] global hash key. (gh)
--   KEYS[2] worker list hash. (wh)
--   KEYS[3] broker list hash. (bh)
--   ARGV[1] Broker ID - unused
--   ARGV[2] Worker ID
-- Output parameters
--   [{string}brokerId, {string}clustername, {string}status, {string}address]
--   Or, false (null for caller) if no broker is found.

local chPrefix = redis.call("HGET", KEYS[1], 'chPrefix')
local workerId = ARGV[2]
local tmp

-- Check if it already exists.
tmp = redis.call("HGET", KEYS[2], workerId)
if not tmp then
    -- There is not such worker in the system. (conclusive)
    return {1}
end

local info
if not pcall(function () info = cjson.decode(tmp) end) then
    -- This should never happen (likely a bug)
    redis.log(redis.LOG_WARNING, "removing corrupted worker info: " .. workerId)
    redis.call("HDEL", KEYS[2], workerId)
    return {1}
end

-- Check is brokerId is valid
if not info.brokerId then
    -- This worker may be under recovery. (retry suggested)
    return {2} -- try again later
end

-- Obtain broker info
tmp = redis.call('HGET', KEYS[3], info.brokerId)
if not tmp then
    -- This should never happen (known broker is not present)
    return {1}
end

local brInfo
if not pcall(function () brInfo = cjson.decode(tmp) end) then
    redis.log(redis.LOG_WARNING, "removing corrupted broker info: " .. info.brokerId)
    redis.call('HDEL', KEYS[3], info.brokerId)
    return {1}
end

if brInfo.addr == nil then
    redis.log(redis.LOG_WARNING, "removing old version of broker info: " .. info.brokerId)
    redis.call('HDEL', KEYS[3], info.brokerId)
    return {1}
end

if brInfo.st == 'active' then
    -- Check connectivity with the broker
    local ch = redis.call("HGET", KEYS[1], 'chPrefix') .. ':' .. info.brokerId;
    local nrecvs = redis.call("PUBLISH", ch, "")
    if nrecvs == 0 then
        local brokerId = info.brokerId;
        info.brokerId = nil
        redis.call("HSET", KEYS[2], workerId, cjson.encode(info))
        brInfo.st = 'invalid'
        redis.call('HSET', KEYS[3], brokerId, cjson.encode(brInfo))
        local msg = {
            ["sig"]="salvage",
            ["clustername"]=brInfo.cn,
            ["brokerId"]=brokerId
        }
        redis.call("PUBLISH", chPrefix .. ':*', cjson.encode(msg))
        return {2, brokerId} -- need salvage
    end
end

return {0, {info.brokerId, brInfo.cn, brInfo.st, brInfo.addr}}

