local src_path = "lib/lua"
local call_redis_script = require "./test-lua/harness";
 
describe("healthCheck.lua", function()
    local script = './lib/lua/healthCheck.lua'
    local keys = {
        ["gh"]="test:gh",
        ["wh"]="test:wh",
        ["bh"]="test:bh",
        ["cz"]="test:cz:pvp",
        ["bz"]="test:bz:pvp",
        ["wz"]="test:wz:br01",
        ["rz"]="test:rz:br01"
    }
    local chPrefix = "test:ch"
 
    -- Flush the database before running the tests
    setup(function()
    end)

    before_each(function()
        redis.call('FLUSHDB')
        redis.call("HSET", keys.gh, "chPrefix", chPrefix) 
    end)

    it("Empty bz should fail", function()
        local brId = "br01"
        local result = call_redis_script(script,  {
            keys.gh,
            keys.wh,
            keys.bh,
            keys.cz,
            keys.bz
        }, {
            brId
        })
        assert.are.equals(type(result), "table")
        assert.are.equals(#result, 2)
        assert.are.equals(result[1], 2)
        print("Error msg: " .. result[2])
    end)

    it("Should fail if the broker is not in the bz table", function()
        redis.call("ZADD", keys.cz, 10, "br00")
        redis.call("ZADD", keys.bz, 123, "br00")
        local brId = "br01"
        local result = call_redis_script(script,  {
            keys.gh,
            keys.wh,
            keys.bh,
            keys.cz,
            keys.bz
        }, {
            brId
        })
        assert.are.equals(type(result), "table")
        assert.are.equals(#result, 2)
        assert.are.equals(result[1], 2)
        print("Error msg: " .. result[2])
    end)

    it("Should fail if the broker is not in the bz table", function()
        redis.call("ZADD", keys.cz, 10, "br00")
        redis.call("ZADD", keys.bz, 123, "br00")
        local brId = "br01"
        local result = call_redis_script(script,  {
            keys.gh,
            keys.wh,
            keys.bh,
            keys.cz,
            keys.bz
        }, {
            brId
        })
        assert.are.equals(type(result), "table")
        assert.are.equals(#result, 2)
        assert.are.equals(result[1], 2)
        print("Error msg: " .. result[2])
    end)

    it("Should succeed (with no op if cz/bz has only yourself", function()
        redis.call("ZADD", keys.cz, 10, "br01")
        redis.call("ZADD", keys.bz, 123, "br01")
        local brId = "br01"
        local result = call_redis_script(script,  {
            keys.gh,
            keys.wh,
            keys.bh,
            keys.cz,
            keys.bz
        }, {
            brId
        })

        assert.are.equals(type(result), "table")
        assert.are.equals(#result, 1)
        assert.are.equals(result[1], 0)
    end)

    it("Should fail if the next broker is not present in bh table", function()
        redis.call("ZADD", keys.cz, 10, "br01")
        redis.call("ZADD", keys.bz, 123, "br01")
        redis.call("ZADD", keys.cz, 10, "br02")
        redis.call("ZADD", keys.bz, 234, "br02")
        local brId = "br01"
        local result = call_redis_script(script,  {
            keys.gh,
            keys.wh,
            keys.bh,
            keys.cz,
            keys.bz
        }, {
            brId
        })

        assert.are.equals(type(result), "table")
        assert.are.equals(#result, 2)
        assert.are.equals(result[1], 2)
        print("Error msg: " .. result[2])
    end)

    it("Should fail if the next broker's info in bh is broken", function()
        redis.call("ZADD", keys.cz, 10, "br01")
        redis.call("ZADD", keys.bz, 123, "br01")
        redis.call("ZADD", keys.cz, 10, "br02")
        redis.call("ZADD", keys.bz, 234, "br02")
        redis.call("HSET", keys.bh, "br02", "{\"cn\":") -- corrupted data
        local brId = "br01"
        local result = call_redis_script(script,  {
            keys.gh,
            keys.wh,
            keys.bh,
            keys.cz,
            keys.bz
        }, {
            brId
        })

        assert.are.equals(type(result), "table")
        assert.are.equals(#result, 2)
        assert.are.equals(result[1], 2)
        print("Error msg: " .. result[2])
    end)

    it("Should fail if the next broker's info does not have addr property", function()
        redis.call("ZADD", keys.cz, 10, "br01")
        redis.call("ZADD", keys.bz, 123, "br01")
        redis.call("ZADD", keys.cz, 10, "br02")
        redis.call("ZADD", keys.bz, 234, "br02")
        redis.call("HSET", keys.bh, "br02", "{\"cn\":\"pvp\",\"st\":\"active\"}") -- no addr
        local brId = "br01"
        local result = call_redis_script(script,  {
            keys.gh,
            keys.wh,
            keys.bh,
            keys.cz,
            keys.bz
        }, {
            brId
        })

        assert.are.equals(type(result), "table")
        assert.are.equals(#result, 2)
        assert.are.equals(result[1], 2)
        print("Error msg: " .. result[2])
    end)

    it("Should remove the next broker that is dead", function()
        redis.call("ZADD", keys.cz, 10, "br01")
        redis.call("ZADD", keys.bz, 123, "br01")
        redis.call("ZADD", keys.cz, 10, "br02")
        redis.call("ZADD", keys.bz, 234, "br02")
        redis.call("HSET", keys.bh, "br02", "{\"cn\":\"pvp\",\"st\":\"active\",\"addr\":\"127.0.0.1:5678\"}")

        local brId = "br01"
        local result = call_redis_script(script,  {
            keys.gh,
            keys.wh,
            keys.bh,
            keys.cz,
            keys.bz
        }, {
            brId
        })

        assert.are.equals(type(result), "table")
        assert.are.equals(#result, 1)
        assert.are.equals(result[1], 1)
    end)
end)
