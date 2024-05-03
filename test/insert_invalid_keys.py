import aerospike
from aerospike_helpers import cdt_ctx
from aerospike_helpers.operations import map_operations

k_ns = "test"
k_set = "store"
k_val = "test"

config = {
    'hosts': [
        ( '127.0.0.1', 3000 )
    ]
}

client = aerospike.client(config).connect()
print("Node Status:", client.info("status"))

for i in range(10):
	key = (k_ns, k_set, f'g{i}')
	client.map_put(key, "bin", '0', i * 10)
	client.map_put(key, "bin", '1', i * 10)
	client.map_put(key, "bin", '2', i * 10)
	client.map_put(key, "bin", '3', i * 10)
	client.map_put(key, "bin", '4', i * 10)

for i in range(10):
	key = (k_ns, k_set, f'f{i}')
	client.map_put(key, "bin", 1.1, i * 10)
	client.map_put(key, "bin", '1', i * 10)
	client.map_put(key, "bin", '2', i * 10)
	client.map_put(key, "bin", '3', i * 10)
	client.map_put(key, "bin", '4', i * 10)

for i in range(10):
	key = (k_ns, k_set, f'a{i}')
	client.map_put(key, "bin", '0', i * 10)
	client.map_put(key, "bin", '1', i * 10)
	client.map_put(key, "bin", '2', i * 10)
	client.map_put(key, "bin", '3', i * 10)
	client.map_put(key, "bin", '4', i * 10)

for i in range(10):
	key = (k_ns, k_set, f'l{i}')
	client.map_put(key, "bin", '0', i * 10)
	client.map_put(key, "bin", '1', i * 10)
	client.map_put(key, "bin", 1.1, i * 10)
	client.map_put(key, "bin", '2', i * 10)

for i in range(10):
	key = (k_ns, k_set, f'l2{i}')
	for j in range(10):
		ctx = [cdt_ctx.cdt_ctx_list_index_create(j)]
		client.operate(key, [
			map_operations.map_put("bin", '0', i * 10, ctx = ctx),
			map_operations.map_put("bin", '1', i * 10, ctx = ctx),
			map_operations.map_put("bin", '2', i * 10, ctx = ctx),
			map_operations.map_put("bin", 1.2, i * 10, ctx = ctx)
		])

client.close()
