-- Input parameters
--   KEYS[1] global hash key. (gh)
--   KEYS[2] worker list hash. (wh)
--   KEYS[3] broker list hash. (bh)
--   KEYS[4] per cluster broker ID & load(as score) sorted sets (cz) - unused
--   KEYS[5] per cluster broker ID & hash key (as score) sorted sets (bz) - unused
--   KEYS[6] worker sets (wz)
--   KEYS[7] recovery sets (rz)
--   ARGV[1] Broker ID - if empty-string, find only
--   ARGV[2] Worker (class) name
--   ARGV[3] Worker ID
--   ARGV[4] Attributes {string} JSon-text.
--   ARGV[5] Current time.
--   ARGV[6] Worker TTL in milliseconds
--   ARGV[7] If this is for recovery. 0: new, 1: recovery
-- Output parameters
--   format: [{number}resultCode, arg1, arg2, ...])
--     Success         : [0, brokerId, workerId]
--     Broker invalid  : [1, brokerId]
--     Broker migrating: [2] => This should not happen

local workerName = nil
local workerId = ARGV[3]
local attributes = nil
local now = 0
local ttl = 0
local forRecovery = 0
local create = false
redis.log(redis.LOG_DEBUG, "ARGV[1]=" .. ARGV[1])
if type(ARGV[1]) == "string" and #ARGV[1] > 0 then
    create = true
    workerName = ARGV[2]
    attributes = cjson.decode(ARGV[4])
    now = tonumber(ARGV[5])
    ttl = tonumber(ARGV[6])
    forRecovery = tonumber(ARGV[7])

    -- Determine the workerId
    if workerId == "" or workerId == nil then
        -- If worker ID is not given, follow the default naming rule.
        if attributes.static then
            -- Static worker's ID will be the class name.
            workerId = workerName
        else
            -- Dynamic worker's ID will be <class-name> '#' <serial-number>
            workerId = workerName .. "#" .. redis.call("HINCRBY", KEYS[1], workerName, 1)
        end
    end
end

-- Set global per-class counter if not set.
redis.call("HSETNX", KEYS[1], workerName, 0)

local chPrefix = redis.call("HGET", KEYS[1], 'chPrefix')

-- Check if it already exists.
local notFound = false
local createdAt = now -- set `now` as default
local info

repeat
    local tmp = redis.call("HGET", KEYS[2], workerId)
    if not tmp then
        notFound = true
        break; -- new worker
    end

    if not pcall(function () info = cjson.decode(tmp) end) then
        notFound = true
        -- Not removing the entry because it will be overwritten.
        redis.call("HINCRBY", KEYS[1], 'workersBroken', 1)
        break; -- new worker
    end

    if info.brokerId then
        -- Check the broker's status first.
        tmp = redis.call('HGET', KEYS[3], info.brokerId)
        if not tmp then
            -- The broker no longer exists. Treat this case as if the
            -- worker does not exists.
            notFound = true
            break;
        end

        local brInfo
        if pcall(function () brInfo = cjson.decode(tmp) end) then
            if brInfo.st == 'active' then
                -- Check if the broker is reachable
                local ch = chPrefix .. ":" .. info.brokerId
                local nrecvs = redis.call("PUBLISH", ch, "")
                if nrecvs > 0 then
                    -- The broker is reachable. Return the broker ID as the result
                    break;
                end
                brInfo.st = 'invalid'
                redis.call('HSET', KEYS[3], info.brokerId, cjson.encode(brInfo))
            end

            local msg = {
                ["sig"]="salvage",
                ["clustername"]=brInfo.cn,
                ["brokerId"]=info.brokerId
            }

            redis.call("PUBLISH", chPrefix .. ':*', cjson.encode(msg))
            return {1} -- request caller to retry later
        end
        -- Remove the broken broker info
        redis.call('HDEL', KEYS[3], info.brokerId)
        redis.call("HINCRBY", KEYS[1], 'brokersBroken', 1)
        notFound = true;
        break;
    end

    if not create then
        redis.log(redis.LOG_DEBUG, "findWorker: found worker with no brokerId")
        -- The worker info does not have brokerId, which means the
        -- worker is under migration.
        return {1} -- request caller to retry later
    end

    -- Check rz to see if the worker is under recovery.
    tmp = redis.call("ZSCORE", KEYS[7], workerId)
    if not tmp then
        -- The worker does not exist in rz.
        -- Treat this case as creating a brand new worker.
        notFound = true
        break;
    end

    -- Remove it from rz
    redis.call("ZREM", KEYS[7], workerId)
    -- The worker is salvaged but not recovered yet.
    createdAt = tonumber(tmp) -- use original creation time
    if ttl == 0 or now - createdAt < ttl then
        -- Recover this worker.
        -- Set this broker as the worker's new broker
        info.brokerId = ARGV[1]
        redis.call("HSET", KEYS[2], workerId, cjson.encode(info))
        redis.call("ZADD", KEYS[6], createdAt, workerId)
        redis.call("HINCRBY", KEYS[1], 'workersRecovered', 1)
    end
until true

if notFound then
    if not create then
        return {0, false}
    end

    info = {}
    info.name = workerName
    info.brokerId = ARGV[1]
    info.attributes = attributes
    redis.call("HSET", KEYS[2], workerId, cjson.encode(info))
    redis.call("ZADD", KEYS[6], createdAt, workerId)
    if forRecovery ~= nil and forRecovery == 1 then
        redis.log(redis.LOG_DEBUG, "Recovering a worker: " .. workerId);
        redis.call("HINCRBY", KEYS[1], 'workersRecovered', 1)
    else
        redis.log(redis.LOG_DEBUG, "Creating a new worker: " .. workerId);
        redis.call("HINCRBY", KEYS[1], 'workersCreated', 1)
    end
else
    redis.log(redis.LOG_DEBUG, "Found an existing worker: " .. workerId);
end
 
return {0, {info.brokerId, info.name, workerId}}
