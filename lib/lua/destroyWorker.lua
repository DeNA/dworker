-- Input parameters
--   KEYS[1] global hash key. (gh)
--   KEYS[2] worker list hash. (wh)
--   KEYS[3] worker sets (wz)
--   KEYS[4] recovery sets (rz)
--   ARGV[1] Worker ID
--   ARGV[2] Mode. 0: Remove completely. 1: Recover if recoverable.
-- Output parameters
--   (no return)

local workerId = ARGV[1]
local mode = tonumber(ARGV[2])

redis.log(redis.LOG_DEBUG, "destroyWorker: mode=" .. mode)

local keepWorkerInfo = false
if mode == 1 then
    local sInfo = redis.call("HGET", KEYS[2], workerId)
    if sInfo then
        local info
        if pcall(function () info = cjson.decode(sInfo) end) then
            if info.attributes.recoverable then
                -- Retrieve createdTime from wz
                local createdAt = redis.call("ZSCORE", KEYS[3], workerId)
                if createdAt then
                    -- Throw it into rz
                    redis.call("ZADD", KEYS[4], createdAt, workerId)
                    redis.log(redis.LOG_DEBUG, "destroyWorker: salvaging worker " .. workerId)
                    keepWorkerInfo = true
                end
            end
        end
    end
end

if not keepWorkerInfo then
    -- Delete the worker from wh
    redis.call("HDEL", KEYS[2], workerId)
end

-- Delete the worker from wz
redis.call("ZREM", KEYS[3], workerId)

-- If rz is not empty, broadcast 'recover' signal
if redis.call("ZCARD", KEYS[4]) > 0 then
    local chPrefix = redis.call("HGET", KEYS[1], 'chPrefix')
    redis.call("PUBLISH", chPrefix .. ':*', '{"sig":"recover"}')
end

return -- Done!

