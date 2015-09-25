-- Input parameters
--   KEYS[1] global hash key. (gh)
--   KEYS[2] worker list hash. (wh)
--   KEYS[3] broker list hash. (bh)
--   KEYS[4] cluster sets (cz)
--   ARGV[1] max retries
-- Output parameters
--   [{string}brokerId, {string}clustername, {string}address]
 
local chPrefix = redis.call("HGET", KEYS[1], 'chPrefix')
local maxRetries = tonumber(ARGV[1])
local minBrId = nil
local brInfo = nil

if maxRetries == nil then
    maxRetries = 100 -- default
end

for i=1, maxRetries+1 do
    local bids = redis.call("ZRANGE", KEYS[4], 0, 0)
    if #bids == 0 then
        redis.log(redis.LOG_DEBUG, "#bids == 0")
        break -- No broker found in the given cluster.
    end

    -- Now, check if the broker is active.
    local tmp = redis.call('HGET', KEYS[3], bids[1])
    if tmp then
        if pcall(function () brInfo = cjson.decode(tmp) end) then
            if brInfo.st == 'active' then
                local ch = chPrefix .. ":" .. bids[1]
                local nrecvs = redis.call("PUBLISH", ch, "")
                if nrecvs == 1 then
                    minBrId = bids[1]
                    break

                elseif nrecvs > 1 then
                    -- TODO: this should not happen, may need to handle this
                    -- later. For now, return the broker found.
                    redis.log(redis.LOG_WARNING, "More than one broker with the same ID")
                    minBrId = bids[1]
                    break
                end

                -- The broker is no longer listening (assumed dead)
                -- Remove the broker from cz (below), change its state,
                -- then retry the process.
                brInfo.st = 'invalid'
                redis.call('HSET', KEYS[3], bids[1], cjson.encode(brInfo))

                local msg = {
                    ["sig"]="salvage",
                    ["clustername"]=brInfo.cn,
                    ["brokerId"]=bids[1]
                }
                redis.call("PUBLISH", chPrefix .. ':*', cjson.encode(msg))
            end
        else
            redis.call("HINCRBY", KEYS[1], 'brokersBroken', 1)
        end
    end

    -- The broker is either no loger present, or invalid (for migration),
    -- but somehow it was still in 'cz' table.
    -- Remove it, then try again.
    redis.log(redis.LOG_DEBUG, "deleting broker from cz table: " .. bids[1])
    redis.call("ZREM", KEYS[4], bids[1])
end

if not minBrId then
    return
end
return { minBrId, brInfo.cn, brInfo.addr }
