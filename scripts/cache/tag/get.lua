-- Retrieve @field_data of all maps, whose keys are stored in @tag_namespace .. @tag. If the tag set contains
-- some expired keys, those keys shall be removed from tag set. Tags, that will no longer exist after this operation,
-- shall be also removed from @tag_namespace .. "__TAG_INDEX__".
--
-- parameters:
-- - ARGV[1] tag
-- - ARGV[2] tag_namespace
-- - ARGV[3] field_data

-- return table cached values belonging to @tag_namespace .. @tag (empty, if tag doesn't exist)
-- -----------------------------------------------------------------------------

local function is_empty(s)
    return s == nil or s == ""
end

-- -----------------------------------------------------------------------------

local tag = ARGV[1]
local tag_namespace = ARGV[2] or ''
local field_data = ARGV[3]

assert(not is_empty(tag), "tag cannot be empty")
assert(not is_empty(field_data), "field_data cannot be empty")

-- -----------------------------------------------------------------------------

local tag_index_key = "__TAG_INDEX__"
local result = {}

for _, key in pairs(redis.call("smembers", tag_namespace .. tag)) do
    local data = redis.call("hget", key, field_data)

    if data then
        table.insert(result, data)
    elseif redis.call("srem", tag_namespace .. tag, key) == 1 and redis.call("exists", tag_namespace .. tag) == 0 then
        redis.call("srem", tag_namespace .. tag_index_key, tag)
    end
end

return result
