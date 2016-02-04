-- Input parameters
--   KEYS[1] global hash key. (gh)
--   KEYS[2] worker list hash. (wh)
--   KEYS[3] broker list hash. (bh)
--   KEYS[4] per cluster broker ID & load(as score) sorted sets (cz)
--   KEYS[5] per cluster broker ID & hash key (as score) sorted sets (bz)
--   KEYS[6] worker sets (wz)
--   KEYS[7] recovery sets (rz)
--   ARGV[1] Target Broker ID
--   ARGV[2] Mode - 0:Salvage, 1:Destroy, 2: Destroy w/o salvage
-- Output parameters

local brokerId = ARGV[1]
local mode = tonumber(ARGV[2])

redis.log(redis.LOG_DEBUG, "salvageWorkers: mode=" .. mode)

if mode == 0 then
    -- Check if the broker is still invalid
    local tmp = redis.call("HGET", KEYS[3], brokerId)
    if not tmp then
        -- Remove it from cz
        redis.call("ZREM", KEYS[4], brokerId)
        redis.call("ZREM", KEYS[5], brokerId)
        return
    end

    local brInfo
    if not pcall(function () brInfo = cjson.decode(tmp) end) then
        -- Remove the corrupted broker info.
        redis.call("HDEL", KEYS[3], brokerId)
        -- Remove it from cz
        redis.call("ZREM", KEYS[4], brokerId)
        redis.call("ZREM", KEYS[5], brokerId)
        return;
    end

    if brInfo.st ~= 'invalid' then
        -- Someone else may have already salvaged the workers.
        -- Nothing to do.
        return;
    end
end

-- Salvage START: salvages the workers with the dead broker
-- This section is shared (manually copied) across the scripts
local function salvage(kWh, kWz, kRz)
    while redis.call("ZCARD", kWz) > 0 do
        -- `res` is an array with one entry. res[1]:workerId, res[2]:time created at.
        local res = redis.call("ZRANGE", kWz, 0, 0, 'withscores')
        local workerId = res[1]
        local createdAt = tonumber(res[2])
        local sInfo = redis.call("HGET", kWh, workerId)
        redis.log(redis.LOG_DEBUG, "salvaging start for " .. workerId)
        if sInfo then
            local workerInfo
            if pcall(function () workerInfo = cjson.decode(sInfo) end) then
                if workerInfo.attributes.recoverable and mode ~= 2 then
                    workerInfo.brokerId = nil;
                    redis.call("HSET", kWh, workerId, cjson.encode(workerInfo))
                    redis.call("ZADD", kRz, createdAt, workerId)
                    redis.call("HINCRBY", KEYS[1], 'workersSalvaged', 1)
                    redis.log(redis.LOG_DEBUG, "salvaging worker " .. workerId)
                else
                    redis.log(redis.LOG_DEBUG, "removing  worker " .. workerId)
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
        redis.log(redis.LOG_DEBUG, "salvaging complete for " .. workerId)
    end

end
-- Salvage COMPLETE

salvage(KEYS[2], KEYS[6], KEYS[7])

-- Remove the broker info.
redis.call("HDEL", KEYS[3], brokerId)
-- Remove it from cz
redis.call("ZREM", KEYS[4], brokerId)
redis.call("ZREM", KEYS[5], brokerId)

if mode == 2 then
    redis.call("DEL", KEYS[6])
end

if redis.call("ZCARD", KEYS[7]) > 0 then
    local chPrefix = redis.call("HGET", KEYS[1], 'chPrefix')
    redis.call("PUBLISH", chPrefix .. ':*', '{"sig":"recover"}')
end

return;
