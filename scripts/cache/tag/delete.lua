-- Delete all keys, that belong to @tag_namespace .. @tag. Also delete those keys from all tags they belong to.
-- Tags, that will no longer exist after this operation, shall be also removed from @tag_namespace .. "__TAG_INDEX__".
--
-- parameters:
-- - ARGV[1] tag
-- - ARGV[2] tag_namespace
-- - ARGV[3] field_tags
-- -----------------------------------------------------------------------------

local function is_empty(s)
    return s == nil or s == ""
end

function string:split(sep)
    local sep, fields = sep or ":", {}
    local pattern = string.format("([^%s]+)", sep)
    self:gsub(pattern, function(c) fields[#fields+1] = c end)

    return fields
end

-- -----------------------------------------------------------------------------

local tag = ARGV[1]
local tag_namespace = ARGV[2] or ''
local field_tags = ARGV[3]

assert(not is_empty(tag), "tag cannot be empty")
assert(not is_empty(field_tags), "field_tags cannot be empty")

-- -----------------------------------------------------------------------------

local tags_delimiter = ","
local tag_index_key = "__TAG_INDEX__"

for _, key in pairs(redis.call("smembers", tag_namespace .. tag)) do
    local tags = redis.call("hget", key, field_tags)

    if tags then
        for _, one_tag in pairs(tags:split(tags_delimiter)) do
            -- avoid deleting members one by one from @tag (the whole set can be deleted in a single command)
            if tag ~= one_tag then
                if redis.call("srem", tag_namespace .. one_tag, key) == 1 and redis.call("exists", tag_namespace .. one_tag) == 0 then
                    redis.call("srem", tag_namespace .. tag_index_key, one_tag)
                end
            end
        end
    end

    redis.call("del", key)
end

redis.call("srem", tag_namespace .. tag_index_key, tag)
redis.call("del", tag_namespace .. tag)

return redis.status_reply("OK")
