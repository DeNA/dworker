-- Input parameters
--   KEYS[1] global hash key. (gh) - unused
--   KEYS[2] worker list hash. (wh)
--   KEYS[3] broker list hash. (bh) - unused
--   KEYS[4] recovery sets (rz)
--   ARGV[1] Current time.
--   ARGV[2] Worker TTL in milliseconds
--   ARGV[3] Max number of workers to fetch
-- Output parameters

local now = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local nMaxFetch = tonumber(ARGV[3])
local kWh = KEYS[2]
local kRz = KEYS[4]
local results = {}

while redis.call("ZCARD", kRz) > 0 do
    local res = redis.call("ZRANGE", kRz, 0, 0, 'withscores')
    local workerId = res[1]
    local createdAt = tonumber(res[2])
    local tmp = redis.call("HGET", kWh, workerId)

    if tmp then
        local workerInfo
        if pcall(function () workerInfo = cjson.decode(tmp) end) then
            if workerInfo.attributes.recoverable then
                if ttl == 0 or now - createdAt < ttl then
                    workerInfo.id = workerId
                    table.insert(results, cjson.encode(workerInfo))
                end
            end
        end
    end

    -- Remove the worker from rz
    redis.call("ZREM", kRz, workerId)

    if #results >= nMaxFetch then
        break
    end
end

return { results, redis.call("ZCARD", kRz) }
