--
-- parameters:
-- - ARGV[1] tag
-- - ARGV[2] tag_prefix
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
local tag_prefix = ARGV[2] or ''
local field_tags = ARGV[3]

assert(not is_empty(tag), "tag cannot be empty")
assert(not is_empty(field_tags), "field_tags cannot be empty")

-- -----------------------------------------------------------------------------

local tags_delimiter = ","
local tag_index_key = "__TAG_INDEX__"

for _, key in pairs(redis.call("smembers", tag_prefix .. tag)) do
    local tags = redis.call("hget", key, field_tags)

    if tags then
        for _, one_tag in pairs(tags:split(tags_delimiter)) do
            -- avoid deleting members one by one from the only tag, that will be deleted as a last step
            if tag ~= one_tag then
                if redis.call("srem", tag_prefix .. one_tag, key) == 1 and redis.call("exists", tag_prefix .. one_tag) == 0 then
                    redis.call("srem", tag_prefix .. tag_index_key, one_tag)
                end
            end
        end
    end

    redis.call("del", key)
end

redis.call("srem", tag_prefix .. tag_index_key, tag)
redis.call("del", tag_prefix .. tag)
