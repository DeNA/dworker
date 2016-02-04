local src_path = "lib/lua"
local call_redis_script = require "./test-lua/harness";
 
describe("addBroker.lua", function()
    local script = './lib/lua/addBroker.lua'
    local keys = {
        ["gh"]="test:gh",
        ["wh"]="test:wh",
        ["bh"]="test:bh",
        ["cz"]="test:cz:pvp",
        ["bz"]="test:bz:pvp",
        ["wz"]="test:wz:br01",
        ["rz"]="test:rz:br01"
    }
 
    -- Flush the database before running the tests
    setup(function()
        redis.call('FLUSHDB')
    end)

    before_each(function()
    end)

    it("Add new broker successfully", function()
        local chPrefix = "test:ch"
        local brId = "br01"
        local load = "10"
        local clustername = "pvp"
        local address = "1.2.3.4:6690"
        local hashKey = "3437877555704920"
        local result = call_redis_script(script,  {
            keys.gh,
            keys.wh,
            keys.bh,
            keys.cz,
            keys.bz,
            keys.wz,
            keys.rz,
        }, {
            brId,
            chPrefix,
            load,
            clustername,
            address,
            hashKey
        })
        assert.are.equals(type(result), "table")
        assert.are.equals(#result, 1)
        assert.are.equals(result[1], 0)

        local brInfo = cjson.decode(redis.call("HGET", keys.bh, brId))
        assert.are.equals(brInfo.cn, clustername)
        assert.are.equals(brInfo.st, "active")
        assert.are.equals(brInfo.addr, address)
    end)

    it("Add a broker that already exits but with no worker", function()
        local chPrefix = "test:ch"
        local brId = "br01"
        local load = "10"
        local clustername = "pvp"
        local address = "1.2.3.4:6690"
        local hashKey = "3437877555704920"
        local result = call_redis_script(script,  {
            keys.gh,
            keys.wh,
            keys.bh,
            keys.cz,
            keys.bz,
            keys.wz,
            keys.rz,
        }, {
            brId,
            chPrefix,
            load,
            clustername,
            address,
            hashKey
        })
        assert.are.equals(type(result), "table")
        assert.are.equals(#result, 1)
        assert.are.equals(result[1], 0)

        local brInfo = cjson.decode(redis.call("HGET", keys.bh, brId))
        assert.are.equals(brInfo.cn, clustername)
        assert.are.equals(brInfo.st, "active")
        assert.are.equals(brInfo.addr, address)
    end)

    it("Add a broker that already exits which has one non-recoverable worker", function()
        local chPrefix = "test:ch"
        local brId = "br01"
        local load = "10"
        local clustername = "pvp"
        local address = "1.2.3.4:6690"
        local hashKey = "3437877555704920"
        local createdAt = os.time()*1000

        -- Add a worker to wh and wz
        local workerId = "MyWorker#1"
        local workerName = "MyWorker"
        local workerInfo = {
            ["name"]=workerName,
            ["brokerId"]=brId,
            ["attributes"]={}
        }
        redis.call("HSET", keys.wh, workerId, cjson.encode(workerInfo))
        redis.call("ZADD", keys.wz, createdAt, workerId)

        local result = call_redis_script(script,  {
            keys.gh,
            keys.wh,
            keys.bh,
            keys.cz,
            keys.bz,
            keys.wz,
            keys.rz,
        }, {
            brId,
            chPrefix,
            load,
            clustername,
            address,
            hashKey
        })
        assert.are.equals(type(result), "table")
        assert.are.equals(#result, 1)
        assert.are.equals(result[1], 0)

        local brInfo = cjson.decode(redis.call("HGET", keys.bh, brId))
        assert.are.equals(brInfo.cn, clustername)
        assert.are.equals(brInfo.st, "active")
        assert.are.equals(brInfo.addr, address)

        local _createdAt = redis.call("ZSCORE", keys.wz, workerId)
        assert.is_falsy(_createdAt)
        assert.are.equals(redis.call("ZCARD", keys.wz), 0) -- wz has been cleared
        assert.are.equals(redis.call("ZCARD", keys.rz), 0) -- no recovery occurred
    end)

    it("Add a broker that already exits which has one recoverable worker", function()
        local chPrefix = "test:ch"
        local brId = "br01"
        local load = "10"
        local clustername = "pvp"
        local address = "1.2.3.4:6690"
        local hashKey = "3437877555704920"
        local createdAt = os.time()*1000

        -- Add a worker to wh and wz
        local workerId = "MyWorker#1"
        local workerName = "MyWorker"
        local workerInfo = {
            ["name"]=workerName,
            ["brokerId"]=brId,
            ["attributes"]={ ["recoverable"]=true }
        }
        redis.call("HSET", keys.wh, workerId, cjson.encode(workerInfo))
        redis.call("ZADD", keys.wz, createdAt, workerId)

        local result = call_redis_script(script,  {
            keys.gh,
            keys.wh,
            keys.bh,
            keys.cz,
            keys.bz,
            keys.wz,
            keys.rz,
        }, {
            brId,
            chPrefix,
            load,
            clustername,
            address,
            hashKey
        })
        assert.are.equals(type(result), "table")
        assert.are.equals(#result, 1)
        assert.are.equals(result[1], 0)

        local brInfo = cjson.decode(redis.call("HGET", keys.bh, brId))
        assert.are.equals(brInfo.cn, clustername)
        assert.are.equals(brInfo.st, "active")
        assert.are.equals(brInfo.addr, address)

        local _createdAt = redis.call("ZSCORE", keys.wz, workerId)
        assert.is_falsy(_createdAt)
        assert.are.equals(0, redis.call("ZCARD", keys.wz)) -- wz has been cleared
        assert.are.equals(1, redis.call("ZCARD", keys.rz)) -- the worker salvaged

        local _workerInfo = cjson.decode(redis.call("HGET", keys.wh, workerId))
        assert.is_truthy(_workerInfo)
        assert.are.equals(_workerInfo.workerName, workerInfo.workerName)
        assert.are.equals(_workerInfo.brokerId, nil)
        assert.are.is_true(_workerInfo.attributes.recoverable)
    end)

    it("Detects broken stale broker and count `brokersBroken` by 1", function()
        local chPrefix = "test:ch"
        local brId = "br01"
        local load = "10"
        local clustername = "pvp"
        local address = "1.2.3.4:6690"
        local hashKey = "3437877555704920"
        local createdAt = os.time()*1000
        local brokersBroken = tonumber(redis.call("HGET", keys.gh, "brokersBroken"))
        if brokersBroken == nil then
            brokersBroken = 0;
        end

        -- inject bad json text
        redis.call("HSET", keys.bh, brId, "{bad-json{")

        local result = call_redis_script(script,  {
            keys.gh,
            keys.wh,
            keys.bh,
            keys.cz,
            keys.bz,
            keys.wz,
            keys.rz,
        }, {
            brId,
            chPrefix,
            load,
            clustername,
            address,
            hashKey
        })
        assert.are.equals(type(result), "table")
        assert.are.equals(#result, 1)
        assert.are.equals(result[1], 0)

        local brInfo = cjson.decode(redis.call("HGET", keys.bh, brId))
        assert.are.equals(brInfo.cn, clustername)
        assert.are.equals(brInfo.st, "active")
        assert.are.equals(brInfo.addr, address)

        assert.are.equals(brokersBroken+1, tonumber(redis.call("HGET", keys.gh, "brokersBroken")))
    end)

    it("Add a broker that already exits which has a worker but not in wh", function()
        local chPrefix = "test:ch"
        local brId = "br01"
        local load = "10"
        local clustername = "pvp"
        local address = "1.2.3.4:6690"
        local hashKey = "3437877555704920"
        local createdAt = os.time()*1000
        local workersBroken = tonumber(redis.call("HGET", keys.gh, "workersBroken"))
        if workersBroken == nil then
            workersBroken = 0;
        end

        -- Add a worker to wh and wz
        local workerId = "MyWorker#1"
        redis.call("HDEL", keys.wh, workerId) -- just in case
        redis.call("ZADD", keys.wz, createdAt, workerId)

        local result = call_redis_script(script,  {
            keys.gh,
            keys.wh,
            keys.bh,
            keys.cz,
            keys.bz,
            keys.wz,
            keys.rz,
        }, {
            brId,
            chPrefix,
            load,
            clustername,
            address,
            hashKey
        })
        assert.are.equals(type(result), "table")
        assert.are.equals(#result, 1)
        assert.are.equals(result[1], 0)

        local brInfo = cjson.decode(redis.call("HGET", keys.bh, brId))
        assert.are.equals(brInfo.cn, clustername)
        assert.are.equals(brInfo.st, "active")
        assert.are.equals(brInfo.addr, address)

        local _createdAt = redis.call("ZSCORE", keys.wz, workerId)
        assert.is_falsy(_createdAt)
        assert.are.equals(redis.call("ZCARD", keys.wz), 0) -- wz has been cleared
        assert.are.equals(workersBroken+1, tonumber(redis.call("HGET", keys.gh, "workersBroken")))
    end)

    it("Add a broker that already exits which has a worker broken", function()
        local chPrefix = "test:ch"
        local brId = "br01"
        local load = "10"
        local clustername = "pvp"
        local address = "1.2.3.4:6690"
        local hashKey = "3437877555704920"
        local createdAt = os.time()*1000
        local workersBroken = tonumber(redis.call("HGET", keys.gh, "workersBroken"))
        if workersBroken == nil then
            workersBroken = 0;
        end

        -- Add a worker to wh and wz
        local workerId = "MyWorker#1"
        redis.call("HSET", keys.wh, workerId, "d$h*2=X")
        redis.call("ZADD", keys.wz, createdAt, workerId)

        local result = call_redis_script(script,  {
            keys.gh,
            keys.wh,
            keys.bh,
            keys.cz,
            keys.bz,
            keys.wz,
            keys.rz,
        }, {
            brId,
            chPrefix,
            load,
            clustername,
            address,
            hashKey
        })
        assert.are.equals(type(result), "table")
        assert.are.equals(#result, 1)
        assert.are.equals(result[1], 0)

        local brInfo = cjson.decode(redis.call("HGET", keys.bh, brId))
        assert.are.equals(brInfo.cn, clustername)
        assert.are.equals(brInfo.st, "active")
        assert.are.equals(brInfo.addr, address)

        local _createdAt = redis.call("ZSCORE", keys.wz, workerId)
        assert.is_falsy(_createdAt)
        assert.are.equals(redis.call("ZCARD", keys.wz), 0) -- wz has been cleared
        assert.are.equals(workersBroken+1, tonumber(redis.call("HGET", keys.gh, "workersBroken")))
    end)
end)
