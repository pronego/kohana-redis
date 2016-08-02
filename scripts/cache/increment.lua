--
-- parameters:
-- - KEYS[1] key
-- - ARGV[1] step - positive / negative integer / float
-- - ARGV[2] field_data
-- -----------------------------------------------------------------------------

local function is_empty(s)
    return s == nil or s == ""
end

-- -----------------------------------------------------------------------------

local key = KEYS[1]
local step = ARGV[1]
local field_data = ARGV[2]

assert(not is_empty(key), "key cannot be empty")
assert(not is_empty(field_data), "field_data cannot be empty")
assert(not is_empty(step), "step cannot be empty")

step = tonumber(step)

assert(not is_empty(step), "step must be number")

-- -----------------------------------------------------------------------------

if redis.call("exists", key) == 1 then
    -- only increment existing values
    local result = redis.pcall("hincrbyfloat", key, field_data, step)

    if type(result) == "string" then
        return result
    end
end
