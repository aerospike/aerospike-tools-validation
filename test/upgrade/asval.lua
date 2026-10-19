function bin_type(rec, name)
	local v = rec[name]

	if v == nil then
		return "nil"
	end

	local mt = getmetatable(v)

	if mt == getmetatable(list()) then
		return "list"
	end

	if mt == getmetatable(map()) then
		return "map"
	end

	return type(v)
end

function write_back(rec, name)
	rec[name] = rec[name]
	aerospike:update(rec)
	return 0
end
