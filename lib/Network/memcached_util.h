#ifndef MEMCACHED_UTIL_H
#define MEMCACHED_UTIL_H

#include <libmemcached/memcached.h>
#include <stdio.h>

#define MEMCACHED_PORT 10086
static char REG_IP[] = "10.0.0.11\n";
// static char *REG_IP = getenv("REG_IP");

static __thread memcached_st *memc = NULL;

static int int_to_char(int num, char *res) {
	int counter = 0;
	while(num) {
		res[counter ++ ] = '0' + (num % 10);
		num /= 10;
	}
	return counter;
}

static memcached_st* m_memc_create()
{


    memcached_server_st *servers = NULL;
    memcached_st *memc = memcached_create(NULL);
    memcached_return rc;

    memc = memcached_create(NULL);

    if (memc == NULL) {
        printf("Couldn't create memcached server.\n");
    }

    char *registry_ip = REG_IP;

    /* We run the memcached server on the default memcached port */
    printf("[Add Server List]\n");
    servers = memcached_server_list_append(servers,
        registry_ip, MEMCACHED_PORT, &rc);
    printf("[Add Server List end]\n");
    rc = memcached_server_push(memc, servers);
    if (rc != MEMCACHED_SUCCESS) {
        printf("Couldn't add memcached server.\n");
    } 

    return memc;
}

static void m_memc_publish(const char *key, void *value, int len)
{
    assert(key != NULL && value != NULL && len > 0);
    memcached_return rc;


    if(memc == NULL) {
        memc = m_memc_create();
    }

    rc = memcached_set(memc, key, strlen(key), (const char *) value, len, 
        (time_t) 0, (uint32_t) 0);
    if (rc != MEMCACHED_SUCCESS) {
        char *registry_ip = REG_IP;
        fprintf(stderr, "\tHRD: Failed to publish key %s. Error %s. "
            "Reg IP = %s\n", key, memcached_strerror(memc, rc), registry_ip);
        exit(-1);
    }
}

static int m_memc_get_published(const char *key, void **value)
{
    assert(key != NULL);

    if(memc == NULL) {
        memc = m_memc_create();
    }

    memcached_return rc;
    size_t value_length;
    uint32_t flags;

    *value = memcached_get(memc, key, strlen(key), &value_length, &flags, &rc);

    if(rc == MEMCACHED_SUCCESS ) {
        return (int) value_length;
    } else if (rc == MEMCACHED_NOTFOUND) {
        assert(*value == NULL);
        return -1;
    } else {
        char *registry_ip = REG_IP;
        fprintf(stderr, "HRD: Error finding value for key \"%s\": %s. "
            "Reg IP = %s\n", key, memcached_strerror(memc, rc), registry_ip);
        exit(-1);
    }
    
    /* Never reached */
    assert(false);
}

#endif // MEMCACHED_UTIL_H