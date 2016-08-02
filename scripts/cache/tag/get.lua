--
-- parameters:
-- - ARGV[1] tag
-- - ARGV[2] tag_prefix
-- - ARGV[3] field_data
-- -----------------------------------------------------------------------------

local function is_empty(s)
    return s == nil or s == ""
end

-- -----------------------------------------------------------------------------

local tag = ARGV[1]
local tag_prefix = ARGV[2] or ''
local field_data = ARGV[3]

assert(not is_empty(tag), "tag cannot be empty")
assert(not is_empty(field_data), "field_data cannot be empty")

-- -----------------------------------------------------------------------------

local result = {}

for _, key in pairs(redis.call("smembers", tag_prefix .. tag)) do
    local data = redis.call("hget", key, field_data)

    if data then
        table.insert(result, data)
    else
        redis.call("srem", tag_prefix .. tag, key)
    end
end

return result
