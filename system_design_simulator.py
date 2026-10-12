"""
System Design Scaling Simulator
A hands on learning tool for Lead Engineer system design interviews
Simulates scaling from 100 to 1,000,000 users with real metrics.
Run: python3 system_design_simulator.py
"""

import time  ## Time and random are used to simulate real world scenarios
import random
import threading # threading provides locks and semaphores to simulate server capacity limits.
import statistics
import hashlib # Hashlib is used to simulate caching and load balancing with consistent hashing.
import queue # Queue is used to simulate request queues and rate limiting.
from concurrent.futures import ThreadPoolExecutor, as_completed # fires concurrent requests just like real clients hitting a server simulataneously.
from collections import OrderedDict # OrderedDict is used to simulate LRU caching and eviction policies.
from dataclasses import dataclass, field
from typing import Optional



# Below the imports , add the data classes that define the shape of every request and metrics result 
# Meaning of the data classes: is that it defines the structure of the data that will be used in the simulation. Each request and metric result will have a specific format, which is defined by these data classes. This allows for easy manipulation and analysis of the data during the simulation.
# Request: Represents a single request to the system, including its ID, type, and payload
# Metrics: Represents the metrics collected during the simulation, including response times, throughput, and error rates

@dataclass
class RequestResult: # This class represents the result of a single simulated request, including its latency, success status, server ID, and whether it was served from cache.
    """Result of a single simulated request.
        If the request was successful, the latency in milliseconds is recorded. If the request failed, the success flag is set to False and the latency is not recorded.
    """
    latency_ms: float # Latency in milliseconds for the request to complete
    success: bool # Whether the request was successful or not
    server_id: Optional[str] = None # The Id of the server that handled the request, if applicable 
    from_cache: bool = False # Whether the request was served from cache or not

@dataclass
class MetricsResult:
    """Aggregated metrics from a load test."""
    total_requests: int # Total number of requests made during the load test
    successful: int # Number of successful requests
    failed: int # Number of failed requests
    avg_latency_ms: float # Average latency in milliseconds for successful requests
    p50_latency_ms: float # 50th percentile latency in milliseconds for successful requests Meaning of p50: The 50th percentile latency is the value below which 50% of the latency measurements fall. It is also known as the median latency. In other words, half of the requests had a latency lower than this value, and half had a latency higher than this value.
    p95_latency_ms: float # 95th percentile latency in milliseconds for successful requests Meaning of p95: The 95th percentile latency is the value below which 95% of the latency measurements fall. It is a common metric used to measure the performance of a system under load,
    p99_latency_ms: float # 99th percentile latency in milliseconds for successful requests Meaning of p99: The 99th percentile latency is the value below which 99% of the latency measurements fall. It is a common metric used to measure the performance of a system under load, particularly for high-traffic applications where even small delays can have a significant impact on user experience.
    throughput_rps: float # Throughput in requests per second, calculated as total_requests / total_time_seconds
    error_rate: float # Error rate as a percentage, calculated as failed / total_requests * 100
    duration_seconds: float # Total duration of the load test in seconds
    extra: dict = field(default_factory=dict) # Extra metrics that can be added as needed, such as server utilization, cache hit rate, etc.

"""RequestResult holds the outcome of one simulated HTTP request:
 how long it took, whether it succeeded, and which server handled it.

MetricsResult aggregates hundreds of individual results into the numbers you discuss in an interview: 
p50, p95, p99 latency, throughput, and error rate.
"""

# Now add the metrics engine below your data classes. 
# This class turns raw request results into meaningful metrics that can be analyzed and
#  discussed in an interview. It calculates average latency, percentiles, throughput, and error rates based on the collected request results.


# =============================================================================
# METRICS COLLECTOR
# =============================================================================

class MetricsCollector:
    """Collects and calculates performance metrics from request results."""
    @staticmethod
    def calculate(results: list[RequestResult], duration: float) -> MetricsResult: # Calculates aggregated metrics from a list of RequestResult objects and the total duration of the test.
        if not results:  # If there are no results, return a MetricsResult with all values set to zero and the duration provided.
            return MetricsResult(0,0,0,0,0,0,0,0,0, duration)

        successful = [r for r in results if r.success] # Filter the results to only include successful requests
        failed = [r for r in results if not r.success] # Filter the results to only include failed requests

        latencies = [r.latency_ms for r in results] # Extract the latencies from all results, regardless of success or failure

        sorted_latencies = sorted(latencies) # Sort the latencies to calculate percentiles
        n = len(sorted_latencies)
        p50 = sorted_latencies[n//2] if n > 0 else 0 # Calculate the 50th percentile latency (median)
        p95 = sorted_latencies[int(n*0.95)] if n > 1 else sorted_latencies[-1] # whats happens if n is 1? It will return the only element in the list, which is the same as the 50th percentile. If n is 0, it will return 0.
        p99 = sorted_latencies[int(n*0.99)] if n > 1 else sorted_latencies[-1] # Calculate the 99th percentile latency

        return MetricsResult(
            total_requests = len(results),
            successful = len(successful),
            failed = len(failed),
            avg_latency_ms = statistics.mean(latencies) if latencies else 0, # Whats happending in the code is that it calculates the average latency by taking the mean of the latencies list. If the list is empty, it returns 0 to avoid division by zero errors.
            p50_latency_ms = p50,
            p95_latency_ms = p95,
            p99_latency_ms = p99,
            throughput_rps = len(results) / duration if duration > 0 else 0,
            error_rate = len(failed) / len(results) if results else 0,
            duration_seconds = duration,

        )

"""💡 What does MetricsCollector do?
It sorts all latencies and picks values at the 50th, 95th, and 99th percentile positions.
p95 means "95% of requests completed faster than this.
" In interviews, p99 is the number that matters most because it represents your worst-case user experience."""

# =============================================================================
# SIMULATED SERVER : With data structures and metrics in place, 
#                    you need something that actually handles requests.
#                    The SimulatedServer class mimics a real web server that slows down under 
#                    load and starts failing when overwhelmed.
# =============================================================================
class SimulatedServer: 
    """Simulates a server with configurable capacity and processing time. 
        It can handle requests, simulate processing time, and return results.
    """
    """
        💡 What does the Semaphore do?
        A Semaphore limits how many threads can enter a section of code at once.
        It is used to control access to a resource by multiple threads. 
        Here it enforces a maximum concurrent request limit. When all slots are taken,
        new requests must wait (adding latency) or time out (producing errors). 
        This is exactly how real servers behave under overload.
    """

    def __init__(self, server_id: str, max_concurrent: int = 10, base_latency_ms: float = 20.0):
        self.server_id = server_id # Unique identifier for the server
        self.max_concurrent = max_concurrent # Maximum number of concurrent requests the server can handle
        self.base_latency_ms = base_latency_ms # Base latency in milliseconds for processing a request
        self._semaphore = threading.Semaphore(max_concurrent) # Semaphore to limit concurrent access to the server
        self._current_load = 0 # Current number of requests being processed by the server
        self._lock = threading.Lock() # Lock to protect access to the current load variable

    def handle_request(self, timeout: float = 5.0) -> RequestResult:
        acquired = self._semaphore.acquire(timeout=timeout) # In this line of code the server attempts to acquire the semaphore, which represents a slot for processing a request. If the semaphore is available (i.e., the server is not at maximum capacity), it will be acquired, and the server can proceed to handle the request. If the semaphore cannot be acquired within the specified timeout, it indicates that the server is currently at its maximum capacity and cannot handle any more requests.
        if not acquired: # In this line of code what happens is that if the semaphore could not be acquired within the specified timeout, it means that the server is currently at its maximum capacity and cannot handle any more requests. Therefore, it returns a RequestResult indicating that the request failed due to timeout.
            return RequestResult(latency_ms=timeout * 1000,
                                 success=False,
                                 server_id=self.server_id) # In this line of code , it returns a RequestResult object indicating that the request failed due to timeout. The latency_ms is set to the timeout value multiplied by 1000 to convert it to milliseconds, success is set to False to indicate that the request was not successful, and server_id is set to the ID of the server that attempted to handle the request.

        try: # here we are trying to simulate the processing of the request. The code inside the try block will be executed if the semaphore was successfully acquired, meaning that the server has a slot available to handle the request.
            with self._lock: # the code means that we are acquiring a lock to ensure that only one thread can modify the _current_load variable at a time. This is important because multiple threads may be trying to handle requests simultaneously, and we want to avoid race conditions that could lead to incorrect values for _current_load.
                self._current_load += 1 # In this line of code , we are incrementing the _current_load variable by 1 to indicate that a new request is being processed by the server. This is done while holding the lock to ensure that no other thread can modify _current_load at the same time.
                load_factor = self._current_load / self.max_concurrent # In this line of code, we are calculating the load factor of the server by dividing the current load (number of requests being processed) by the maximum concurrent capacity of the server. The load factor represents how much of the server's capacity is currently being utilized. A load factor of 1.0 means the server is at full capacity, while a load factor less than 1.0 indicates that there is still available capacity to handle more requests.

                # Latency increases with load(simulates resource contention)
                jitter = random.uniform(0.5, 1.5) # This line of code, generates a random jitter value between 0.5 and 1.5 using the random.uniform function. The jitter is used to introduce some variability in the latency calculation, simulating real-world scenarios where response times can fluctuate due to various factors such as network conditions, server load, and other external influences. By multiplying the base latency by the load factor and the jitter, we can simulate a more realistic latency that reflects the dynamic nature of server performance under different loads.
                latency = self.base_latency_ms * (1 + load_factor * 2) * jitter # In this line of code, we are calculating the latency for processing the request based on the base latency, load factor, and jitter. The formula used is: latency = base_latency_ms * (1 + load_factor * 2) * jitter. This means that as the load factor increases (i.e., more requests are being processed), the latency will increase proportionally, simulating resource contention and slower response times under higher loads.
                time.sleep(latency / 1000)

                # Higher load = higher chance of errors
                error_chance = max(0, (load_factor - 0.8) * 0.5) # In this line of code, we are calculating the chance of an error occurring based on the load factor. The formula used is: error_chance = max(0, (load_factor - 0.8) * 0.5). This means that if the load factor is below 0.8, the error chance will be 0 (no errors expected). However, if the load factor exceeds 0.8, the error chance will increase proportionally, simulating a higher likelihood of errors occurring under heavier loads. The max function ensures that the error chance does not go below 0, preventing negative values.
                success = random.random() > error_chance # In this line of code, we are simulating the time taken to process the request by making the current thread sleep for a duration equal to the calculated latency in seconds. Since the latency is calculated in milliseconds, we divide it by 1000 to convert it to seconds before passing it to time.sleep(). This effectively simulates the delay that would occur in a real server when handling a request, allowing us to observe how the system behaves under different load conditions. 
                return RequestResult(latency_ms=latency, success=success, server_id=self.server_id)

        finally: # In this line of code, we are using a finally block to ensure that the code inside it is executed regardless of whether an exception occurred in the try block or not. This is important because we want to make sure that the semaphore is released and the current load is decremented even if an error occurs while processing the request. The finally block guarantees that these cleanup actions will always be performed, preventing potential deadlocks or incorrect load tracking.
            with self._lock: # In this line of code, we are acquiring the lock again to ensure that only one thread can modify the _current_load variable at a time. This is necessary because we are about to decrement the current load, and we want to avoid race conditions that could lead to incorrect values for _current_load.
                self._current_load -= 1 # In this line of code, we are decrementing the _current_load variable by 1 to indicate that a request has finished processing and is no longer being handled by the server. This is done while holding the lock to ensure that no other thread can modify _current_load at the same time, maintaining accurate tracking of the server's current load.
            self._semaphore.release() # In this line of code, we are releasing the semaphore that was acquired at the beginning of the handle_request method. This allows other threads waiting for a slot to process their requests to acquire the semaphore and proceed with their request handling. Releasing the semaphore is essential for maintaining proper concurrency control and ensuring that the server can continue to handle incoming requests efficiently.

            """
            💡 How does degradation work?
            The load_factor tracks how full the server is (0.0 to 1.0).
            As load grows, latency increases exponentially and error probability rises.
            Above 80% capacity, requests start failing randomly.This models real-world server saturation.
            """


"""
    Add Load Balancing and Caching

    In the previous step, your single server collapsed under 1,000 concurrent users.
    Latency spiked and errors appeared. The fix is horizontal scaling:
    spread traffic across multiple servers using a load balancer.
    But even with multiple servers, the database becomes the next bottleneck. 
    Every request still hits the same data store. A cache layer absorbs repeated reads and 
    drops latency dramatically. In this step, you will build both patterns and watch real metrics improve."""

"""A load balancer sits between users and your servers.
    It decides which server handles each incoming request.
    You will implement two common algorithms: round-robin 
    (each server takes turns) and least-connections 
    (pick the server with the least active work)."""
class LoadBalancer:
    def __init__(self, servers: list[SimulatedServer], algorithm: str = "round_robin"):
        self.servers = servers # this line of code initializes the LoadBalancer class with a list of SimulatedServer instances and a load balancing algorithm. The servers parameter is a list of SimulatedServer objects that the load balancer will distribute incoming requests to. The algorithm parameter specifies the load balancing strategy to use, with "round_robin" as the default option. The available algorithms can be extended in the future to include other strategies like least-connections or weighted round-robin.
        self.algorithm = algorithm # this explanatin of this code is that it sets the load balancing algorithm to be used by the LoadBalancer instance. The algorithm parameter is a string that specifies which load balancing strategy to use when distributing incoming requests to the available servers. In this case, the default value is "round_robin", but it can be changed to other strategies as needed. The chosen algorithm will determine how the load balancer selects which server should handle each incoming request.
        self._rr_index = 0 # this line of code initializes an internal variable _rr_index to 0. This variable is used to keep track of the current index in the list of servers when using the round-robin load balancing algorithm. Each time a request is handled, the _rr_index will be incremented to point to the next server in the list, ensuring that requests are distributed evenly across all available servers. When the end of the list is reached, the index will wrap around to the beginning, allowing for continuous cycling through the servers.
        self._lock = threading.Lock() # here we are creating a threading lock to ensure that access to shared resources (like the _rr_index) is thread-safe. Since multiple threads may be handling requests concurrently, we need to prevent race conditions and ensure that only one thread can modify the _rr_index at a time. The lock will be acquired before accessing or modifying the _rr_index and released afterward, ensuring that the round-robin index is updated correctly and consistently across all threads.

    """💡 What does this code do?

    The LoadBalancer accepts a list of servers and an algorithm choice.
    It uses a threading lock so multiple threads can safely request a server at the same time.
    Round-robin cycles through servers in order using _rr_index modulo the number of servers.
    Least-connections picks the server with the lowest _current_load value, 
    sending new requests to whichever server is least busy."""
    def get_server(self) -> SimulatedServer:
        with self._lock:
            if self.algorithm == "round_robin":
                server = self.servers[self._rr_index % len(self.servers)] # this line of code selects the next server in the list based on the current _rr_index value. The modulo operator (%) is used to wrap around the index when it reaches the end of the list, ensuring that the selection continues in a circular manner. This allows for even distribution of requests across all available servers in a round-robin fashion.
                self._rr_index += 1 # this line of code increments the _rr_index by
                return server # this line of code returns the selected server to handle the incoming request. The server is chosen based on the round-robin algorithm, which ensures that requests are distributed evenly across all available servers in a cyclic manner.
            else:
                return min(self.servers, key=lambda s: s._current_load) # this line of code selects the server with the least number of active requests (i.e., the least load) when the load balancing algorithm is set to "least_connections". The min function is used to find the server with the minimum _current_load value among all available servers. The key parameter specifies a lambda function that retrieves the _current_load attribute of each server, allowing the min function to compare and identify the server with the lowest load. This approach helps to distribute incoming requests more efficiently by directing them to servers that are currently handling fewer requests, potentially improving overall performance and reducing latency.

"""Add the LRU cache and its stage function
    Load balancing solved the server CPU bottleneck. But all 3 servers still
    hit the same database for every request. A cache stores frequently accessed data in
    memory so repeat reads skip the database entirely.
    Below the LoadBalancer class, add the first part of the LRUCache class with its constructor and get() method:"""

class LRUCache:
    def __init__(self, max_size: int = 1000, ttl_seconds: float = 60.0):
        self.max_size = max_size # this line of code initializes the maximum size of the cache, which determines how many items can be stored in the cache at any given time. If the number of items exceeds this limit, the least recently used (LRU) item will be evicted to make room for new items.
        self.ttl_seconds = ttl_seconds # this line of code initializes the time-to-live (TTL) for each cache entry, which specifies how long an item can remain in the cache before it is considered expired and eligible for eviction. The TTL is measured in seconds, and once an item exceeds this duration, it will be removed from the cache during the next access or cleanup operation.
        self._cache: OrderedDict = OrderedDict() # this line of code initializes an OrderedDict to store the cached items. An OrderedDict maintains the order of insertion, which allows us to easily track the least recently used items. When an item is accessed or added to the cache, it will be moved to the end of the OrderedDict, indicating that it was recently used. This behavior is essential for implementing the LRU eviction policy.
        self._timestamps: dict = {} # this line of code initializes a regular dictionary to store the timestamps of when each cache entry was last accessed or added. The timestamps are used to determine whether a cache entry has expired based on the specified TTL (time-to-live). When an item is accessed or added to the cache, its corresponding timestamp will be updated to the current time. During cache access or cleanup operations, the timestamps will be checked to identify and remove any expired entries from the cache.
        self._lock = threading.Lock() # this line of code creates a threading lock to ensure that access to the cache is thread-safe. Since multiple threads may be accessing or modifying the cache concurrently, we need to prevent race conditions and ensure that only one thread can perform operations on the cache at a time. The lock will be acquired before accessing or modifying the cache and released afterward, ensuring that all operations are performed safely and consistently across threads.
        self.hits = 0 # this line of code initializes a counter to track the number of cache hits, which occurs when a requested item is found in the cache and returned without needing to fetch it from the underlying data source. The hits counter can be used to calculate the cache hit rate, which is an important metric for evaluating the effectiveness of the cache in improving performance and reducing latency.
        self.misses = 0 # this line of code initializes a counter to track the number of cache misses, which occurs when a requested item is not found in the cache and must be fetched from the underlying data source. The misses counter can be used to calculate the cache miss rate, which is an important metric for evaluating the effectiveness of the cache in improving performance and reducing latency. A high miss rate may indicate that the cache size or eviction policy needs to be adjusted to better accommodate the workload.


    """💡 What does the get() method do?
        When you look up a key, the cache checks two things:
        does the key exist,
        and has it expired (TTL). If the key is fresh, it moves to the end of the 
        ordered dict (marking it as recently used) and increments hits.
        If the key is missing or expired, the method increments misses and returns None. 
        Expired entries are cleaned up immediately on access."""
    def get(self, key: str) -> Optional[str]:
        with self._lock:
            if key in self._cache:
                if time.time() - self._timestamps[key] > self.ttl_seconds:
                    # Entry expired
                    del self._cache[key]
                    del self._timestamps[key]
                    self.misses += 1
                    return None
                # Move to end to mark as recently used
                self._cache.move_to_end(key)
                self.hits += 1
                return self._cache[key]
            self.misses += 1
            return None


    """💡 What do put() and hit_rate do?
        put() stores a key-value pair. If the cache is full, 
        it evicts the least recently used item using popitem(last=False) which removes 
        the oldest entry from the ordered dict.

        hit_rate is a property that calculates the percentage of lookups 
        that found data in the cache vs going to the database. 
        A 90% hit rate means 90% of reads skip the database."""    
    def put(self, key: str, value: str) -> None:
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key) # Move to end to mark as recently used
            else:
                if len(self._cache) >= self.max_size:
                    oldest_key, _ = self._cache.popitem(last=False)
                    del self._timestamps[oldest_key]
                self._cache[key] = value
                self._timestamps[key] = time.time()

    @property
    # This property calculates the cache hit rate as a float between 0.0 and 1.0, representing the proportion of successful cache hits to total cache accesses (hits + misses). If there are no accesses, it returns 0.0 to avoid division by zero.
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0

"""
Implement Database Replicationa and Sharding
Your caching layer brought read latency down dramatically, 
but there is still a fundamental bottleneck lurking underneath.

Write-heavy workloads still funnel through a single database. 
Read replicas distribute read traffic, but when writes overwhelm the primary, 
sharding becomes the nuclear option. In this step, you will build both patterns 
and see exactly how consistent hashing prevents catastrophic key redistribution when you add or remove nodes.

Add the ConsistentHashRing class
Naive modulo-based sharding (key % N) breaks when you add or remove a server. 
Almost every key gets remapped. Consistent hashing solves this by placing nodes 
on a virtual ring so only a fraction of keys move.
"""

class ConsistentHashRing:
    def __init__(self, nodes: list[str], virtual_nodes: int = 150):
        self.virtual_nodes = virtual_nodes  # Number of virtual nodes per physical node
        self.ring: dict[int, str] = {}  # Maps hash values to node identifiers
        self.sorted_hashes: list[int] = []  # Sorted list of hash values for efficient lookup
        self._key_distribution: dict[str, int] = {}  # Tracks how many keys are assigned to each node
        for node in nodes:
            self.add_node(node)

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)  # Use MD5 hash for consistent hashing

    """💡 How does _hash work?
    It takes a string key and generates a consistent hash value using MD5. This ensures that the same key always produces the same hash, which is essential for consistent hashing."""
    def add_node(self, node: str) -> None: # Adds a new node to the consistent hash ring by creating virtual nodes and updating the ring and sorted hash list. It also initializes the key distribution for the new node.
        self._key_distribution[node] = 0
        for i in range(self.virtual_nodes):
            virtual_key = f"{node}:v{i}"
            hash_val = self._hash(virtual_key)
            self.ring[hash_val] = node
        self.sorted_keys = sorted(self.ring.keys())

    def remove_node(self, node: str) -> None: # Removes a node from the consistent hash ring by deleting its virtual nodes from the ring and updating the sorted hash list. It also removes the key distribution entry for the removed node.
        for i in range(self.virtual_nodes):
            virtual_key = f"{node}:v{i}"
            hash_val = self._hash(virtual_key)
            if hash_val in self.ring:
                del self.ring[hash_val]
        self.sorted_keys = sorted(self.ring.keys())
        if node in self._key_distribution:
            del self._key_distribution[node]

    """💡 What does this code do?

Each physical node gets 150 virtual positions on the ring. This ensures even key distribution even with a small number of physical nodes.
_hash() converts any string into a large integer using MD5. This positions keys and nodes on the same ring.
add_node() creates 150 virtual entries per node. More virtual nodes means smoother distribution.
remove_node() deletes all virtual entries for a node. Only keys that belonged to that node need remapping."""


    """💡 How does get_node work?
    It hashes the key, then walks clockwise around the ring (through sorted keys) until it 
    finds the first node position that is greater than or equal to the key's hash. If no 
    such position exists (the key hashes past the last node), it wraps around to the first node on the ring."""
    def get_node(self, key: str) -> str:
        if not self.ring:
            raise ValueError("Hash ring is empty")
        hash_val = self._hash(key)
        for ring_key in self.sorted_keys:
            if hash_val <= ring_key:
                node = self.ring[ring_key]
                self._key_distribution[node] = self._key_distribution.get(node, 0) + 1
                return node
        node = self.ring[self.sorted_keys[0]]
        self._key_distribution[node] = self._key_distribution.get(node, 0) + 1
        return node

    def get_distribution(self) -> dict[str, int]: # Returns a dictionary representing the distribution of keys across nodes, where the keys are node identifiers and the values are the number of keys assigned to each node. This allows for monitoring and analyzing how evenly the keys are distributed among the nodes in the consistent hash ring.
        return dict(self._key_distribution)


"""
Add the ReplicatedDatabase class
A replicated database separates reads from writes. The primary handles all writes, 
while read replicas absorb the 70%+ of traffic that is read-only.
"""

class ReplicatedDatabase:
    def __init__(self, num_replicas: int = 2):
        self.primary = SimulatedServer("primary-db", max_concurrent=20, base_latency_ms=30.0)
        self.replicas = [
            SimulatedServer(f"replica-{i}", max_concurrent=20, base_latency_ms=25.0)
            for i in range(num_replicas)
        ]
        self._read_index = 0
        self._lock = threading.Lock()

    def write(self, timeout: float = 5.0) -> RequestResult:
        return self.primary.handle_request(timeout)

    def read(self, timeout: float = 5.0) -> RequestResult:
        with self._lock:
            replica = self.replicas[self._read_index % len(self.replicas)]
            self._read_index += 1
        return replica.handle_request(timeout)

    """💡 What does this code do?
    The primary server has a slightly higher base latency (30ms) than replicas (25ms) because it also handles replication overhead in a real system.
    read() round-robins across replicas using a lock-protected index. This distributes read load evenly.
    write() always routes to the primary. In a real system, writes propagate to replicas asynchronously."""




# =============================================================================
# LOAD TEST RUNNER
# =============================================================================


"""
💡 What does ThreadPoolExecutor do here?

ThreadPoolExecutor spins up max_workers threads that each fire a request simultaneously.
This simulates real concurrent traffic hitting your server. time.perf_counter() provides
high-resolution timing so your throughput calculations are accurate.  
"""
def run_load_test(handler, num_requests: int, max_workers: int = 50) -> MetricsResult: # this function runs a load test by sending a specified number of requests to the provided handler function using a ThreadPoolExecutor to simulate concurrent requests. It collects the results of each request and calculates aggregated metrics using the MetricsEngine.
    """Run a load test using ThreadPoolExecutor to simulate concurrent requests to the handler function."""
    results: list[RequestResult] = [] # List to store the results of each request
    start_time = time.perf_counter() # Start the timer to measure the duration of the load test

    with ThreadPoolExecutor(max_workers=max_workers) as executor: # Create a ThreadPoolExecutor to manage concurrent request execution
        futures = [executor.submit(handler) for _ in range(num_requests)] # This line of futures means that we are submitting the handler function to the executor for execution, creating a list of Future objects representing the pending results of each request. The handler function is called num_requests times, simulating multiple concurrent requests to the server. Each Future object will eventually hold the result of the corresponding request once it has completed.
        for future in as_completed(futures):
            try:
                results.append(future.result()) # In this line of code, we are appending the result of each completed Future to the results list. The future.result() call blocks until the Future has completed and returns the result of the handler function (a RequestResult object). This allows us to collect all the results of the simulated requests for further analysis.
            except Exception:
                results.append(RequestResult(latency_ms=0, success=False))

    duration = time.perf_counter() - start_time # Calculate the total duration of the load test by subtracting the start time from the current time
    return MetricsCollector.calculate(results, duration) # Calculate and return the aggregated metrics using the MetricsEngine based on the collected request results and the total duration of the load testa                                             








# =============================================================================
# DISPLAY HELPERS
# =============================================================================

"""Temporary Banner"""
def display_banner() -> None:
    print("\n" + "=" * 70)
    print("   SYSTEM DESIGN SCALING SIMULATOR")
    print("   Lead Engineer Interview Preparation Tool")
    print("=" * 70)
    print("\n   Scaling Journey: 100 users --> 1,000,000 users")
    print("   Each stage shows: Theory --> Simulation --> Metrics --> Talking Points")
    print("\n" + "-" * 70)
    print("   STAGES:")
    print("   [1] Single Server (100-1K users) + Back-of-Envelope Estimation")
    print("   [2] Load Balancing (1K-5K users) + Stateless Design")
    print("   [3] Caching Layer (5K-100K users) + CAP Theorem")
    print("   [4] DB Replication & Sharding (100K-500K) + Consistency Models")
    print("   [5] Async Processing & Rate Limiting (500K-1M) + Circuit Breakers")
    print("   [6] Full Journey (all stages sequentially)")
    print("   [T] Theory Reference (all concepts)")
    print("   [Q] Quit")
    print("-" * 70)

def display_metrics(metrics: MetricsResult, label: str = "") -> None:
    print(f"\n   {'--- ' + label + ' ---' if label else '--- Metrics ---'}")
    print(f"   Requests:    {metrics.total_requests} total, "
          f"{metrics.successful} OK, {metrics.failed} FAILED")
    print(f"   Latency:     avg={metrics.avg_latency_ms:.1f}ms, "
          f"p50={metrics.p50_latency_ms:.1f}ms, "
          f"p95={metrics.p95_latency_ms:.1f}ms, "
          f"p99={metrics.p99_latency_ms:.1f}ms")
    print(f"   Throughput:  {metrics.throughput_rps:.1f} req/s")
    print(f"   Error Rate:  {metrics.error_rate * 100:.1f}%")
    print(f"   Duration:    {metrics.duration_seconds:.2f}s")

def display_architecture(stage: int) -> None:
    architectures = {
        1: "\n    [Users] --> [Single Server + DB]\n                (everything on one box)\n        ",
        2: "\n    [Users] --> [Load Balancer]\n                   /    |    \\\n            [App1] [App2] [App3]\n                   \\    |    /\n                    [Database]\n        ",
        3: "\n    [Users] --> [Load Balancer]\n                   /    |    \\\n            [App1] [App2] [App3]\n                   \\    |    /\n                [Cache Layer (Redis)]\n                       |\n                    [Database]\n        ",
        4: "\n    [Users] --> [Load Balancer]\n                   /    |    \\\n            [App1] [App2] [App3]\n                   \\    |    /\n                [Cache Layer]\n                   /        \\\n        [Primary DB]    [Read Replicas]\n              |          /     \\\n        [Shard 1] [Shard 2] [Shard 3]\n        ",
        5: "\n    [Users] --> [API Gateway + Rate Limiter]\n                        |\n                [Load Balancer]\n                /       |       \\\n         [App1]    [App2]    [App3]\n           |          |          |\n    [Cache] [Message Queue] [Circuit Breaker]\n           |          |          |\n    [DB Primary] [Workers]  [External APIs]\n           |\n    [Read Replicas / Shards]\n        "
    }
    print(architectures.get(stage, ""))

def print_section(title: str) -> None:
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")


def print_theory(title: str, content: str) -> None:
    print(f"\n  [THEORY] {title}")
    print(f"  {'-' * 60}")
    for line in content.strip().split("\n"):
        print(f"  {line}")
    print()


def print_lead_engineer(content: str) -> None:
    print(f"\n  [LEAD ENGINEER TALKING POINTS]")
    print(f"  {'-' * 60}")
    for line in content.strip().split("\n"):
        print(f"  {line}")
    print()



"""💡 Why the temporary entry point?
if __name__ == "__main__":
    display_banner()

The if __name__ == "__main__" block at the bottom runs
 display_banner() directly so you can see the menu right now.
You will replace this with the full interactive menu at the end of this step
."""

# IN this function we are going to display the first stage of the simulator which is the single server stage. We will display the architecture of the single server and also display some theory about back-of-envelope estimation and horizontal vs vertical scaling.
def stage_single_server() -> None:
    print_section("STAGE 1: Single Server (0 - 1,000 Users)")
    display_architecture(1)
    print_theory("Back-of-Envelope Estimation (DO THIS FIRST IN EVERY INTERVIEW)", """
WHAT: Quick math to estimate system requirements before designing.
WHEN: The FIRST thing after the interviewer states the problem.
WHY:  Determines which scaling patterns you actually need.
HOW:  QPS = DAU x requests/user / 86,400. Peak = QPS x 2-3.
      Storage = Users x data/user x retention.
      Bandwidth = QPS x avg response size.
      Powers of two: 1KB=1000B, 1MB=1M B, 1GB=1B B, 1TB=1T B.
    """)
    print_theory("Horizontal vs Vertical Scaling", """
Vertical (Scale Up): bigger machine. Horizontal (Scale Out): more machines.
Vertical FIRST until ceiling. Horizontal requires stateless design.
AWS: EC2 instance types (vertical), Auto Scaling Groups (horizontal).
GCP: Compute Engine machine types (vertical), Managed Instance Groups.
    """)

    print("\n  [SIMULATION] Running load test: Single server under pressure...")
    server = SimulatedServer("single-server", max_concurrent=10, base_latency_ms=20)
    # Test with 100 concurrent users
    print("\n  Test 1: 100 concurrent users...")
    metrics_100 = run_load_test(server.handle_request, num_requests=100, max_workers=20)
    display_metrics(metrics_100, "100 Users - Comfortable")
    # Test with 500 concurrent users
    print("\n  Test 2: 500 concurrent users (pushing limits)...")
    metrics_500 = run_load_test(server.handle_request, num_requests=500, max_workers=50)
    display_metrics(metrics_500, "500 Users - Under Pressure")
    # Test with 1000 concurrent users
    print("\n  Test 3: 1000 concurrent users (server overwhelmed)...")
    metrics_1000 = run_load_test(server.handle_request, num_requests=1000, max_workers=100)
    display_metrics(metrics_1000, "1000 Users - OVERWHELMED")
    # Lead Engineer interview tips for this stage
    print_lead_engineer("""
At 1K users: single server is fine. Do NOT add K8s or microservices.
Transition trigger: p99 > SLA or CPU > 70% sustained.
Failure modes: SPOF, no graceful degradation.
Cost: t3.medium $30/month handles 1K users easily.
    """)

    """
    What does this simulation do?
    This simulation demonstrates the behavior of a single server under different load conditions.
    A SimulatedServer with max_concurrent=10 uses a threading Semaphore to limit parallel processing. Requests beyond capacity queue up or time out.
    run_load_test() fires all requests concurrently using ThreadPoolExecutor. At 100 users, most requests get through smoothly. At 1,000, the server is overwhelmed and latency spikes.
    The print_lead_engineer() block gives you the exact phrasing to use in an interview: what not to build at this scale, the transition trigger, and cost reasoning.
    """


    """
    💡 Why does this matter for interviews?

    You just watched a server break under load. This is the experience that makes scaling patterns intuitive rather than memorized. In an interview, you can say: "A single server with 10 concurrent connections is fine for 100 users, but at 1,000 the p99 exceeds any reasonable SLA. That is my trigger to scale horizontally."
    🙋‍♀️ Simulation takes too long or hangs?
    The 1,000-user test may take 10-20 seconds because requests queue behind the 10-connection limit. This is expected. If it hangs beyond 30 seconds, press Ctrl+C and check that max_workers=100 is set in the third run_load_test() call.
    If you see a NameError for SimulatedServer or run_load_test, confirm that the class and function from Step 1 are still intact above your new code.
    """

def stage_load_balancing() -> None:
    print_section("STAGE 2: Load Balancing (1,000 - 5,000 Users)")
    display_architecture(2)
    print_theory("Load Balancing", "Round-robin, least-connections, IP-hash, weighted. L4 vs L7.")
    print_theory("The Stateless Server Rule", "No session state on server. Move to Redis/S3.")
    single = SimulatedServer("solo", max_concurrent=10, base_latency_ms=20)
    metrics_single = run_load_test(single.handle_request, num_requests=1000, max_workers=50)
    display_metrics(metrics_single, "1 Server, 1000 Requests")
    servers = [SimulatedServer(f"app-{i}", max_concurrent=10, base_latency_ms=20) for i in range(3)]
    lb = LoadBalancer(servers, algorithm="round_robin")
    def lb_request():
        return lb.get_server().handle_request()
    metrics_lb = run_load_test(lb_request, num_requests=1000, max_workers=50)
    display_metrics(metrics_lb, "3 Servers + Load Balancer, 1000 Requests")
    improvement = ((metrics_single.p95_latency_ms - metrics_lb.p95_latency_ms) / metrics_single.p95_latency_ms * 100)
    print(f"\n  >> P95 Latency Improvement: {improvement:.1f}%")
    print(f"  >> Error Rate: {metrics_single.error_rate*100:.1f}% -> {metrics_lb.error_rate*100:.1f}%")
    print_lead_engineer("Use ALB on AWS. Avoid sticky sessions. Stateless servers only.")


"""
    💡 What does the caching simulation demonstrate?

    The function runs the same 500 requests twice. First without a cache (every request hits the slow server). 
    Then with the LRUCache in front.

    The cached_request() inner function checks the cache first. On a hit, 
    it returns in under 2ms instead of the server's 40ms+ base latency. On a miss, 
    it fetches from the server and stores the result for next time. The 20 popular keys 
    simulate real access patterns where a small set of data is requested repeatedly
"""
def stage_caching() -> None:
    print_section("STAGE 3: Caching Layer (5,000 - 100,000 Users)")
    display_architecture(3)
    print_theory("Caching Strategies", "Cache-aside, write-through, write-back, write-around. LRU/LFU/TTL eviction.")
    print_theory("CAP Theorem", "C+A+P: choose CP or AP during partition. Redis=AP, ZooKeeper=CP.")
    server = SimulatedServer("app", max_concurrent=20, base_latency_ms=40)
    cache = LRUCache(max_size=200, ttl_seconds=30.0)
    metrics_no_cache = run_load_test(server.handle_request, num_requests=500, max_workers=50)
    display_metrics(metrics_no_cache, "No Cache - All DB Hits")
    popular_keys = [f"item:{i}" for i in range(20)]
    def cached_request():
        key = random.choice(popular_keys)
        cached = cache.get(key)
        if cached:
            return RequestResult(latency_ms=random.uniform(0.5, 2.0), success=True, from_cache=True)
        result = server.handle_request()
        if result.success:
            cache.put(key, f"data_for_{key}")
        return result
    metrics_cached = run_load_test(cached_request, num_requests=500, max_workers=50)
    display_metrics(metrics_cached, "With Cache")
    print(f"\n  >> Cache Hit Rate: {cache.hit_rate * 100:.1f}%")
    improvement = ((metrics_no_cache.p95_latency_ms - metrics_cached.p95_latency_ms) / metrics_no_cache.p95_latency_ms * 100)
    print(f"  >> P95 Latency Improvement: {improvement:.1f}%")
    print_lead_engineer("Redis cluster ~$200/month. 90% hit rate = 72% DB load reduction.")

    """
    💡 Why does CAP theorem appear in the caching stage?
        The moment you add a distributed cache like Redis, you have data in two places 
        (cache and database). During a network partition, you must choose: reject requests until the cache is consistent (CP), 
        or serve potentially stale cached data to stay available (AP).
        In interviews, the Lead Engineer answer is: "CAP is a spectrum, not a binary. 
        I choose consistency per operation. User login = strong consistency. 
        Like count = eventual consistency.
    """

def stage_database_scaling() -> None:
    print_section("STAGE 4: DB Replication & Sharding (100K - 500K Users)")
    display_architecture(4)
    print_theory("Database Replication", "Leader-follower. Reads to replicas. Replication lag.")
    print_theory("Database Sharding", "Range, hash, directory, geo. Consistent hashing.")
    print_theory("Consistency Models", "Strong, eventual, causal, read-your-writes.")
    single_db = SimulatedServer("single-db", max_concurrent=15, base_latency_ms=30)
    display_metrics(run_load_test(single_db.handle_request, num_requests=500, max_workers=50), "Single DB")
    rep_db = ReplicatedDatabase(num_replicas=2)
    def replicated_request():
        if random.random() < 0.7:
            return rep_db.read()
        return rep_db.write()
    display_metrics(run_load_test(replicated_request, num_requests=500, max_workers=50), "Replicated DB")
    ring = ConsistentHashRing(["shard-1", "shard-2", "shard-3", "shard-4"])
    for i in range(10000):
        ring.get_node(f"user:{i}")
    dist = ring.get_distribution()
    print("\n  Key distribution across 4 shards (10,000 keys):")
    for shard, count in sorted(dist.items()):
        bar = "#" * (count // 100)
        print(f"    {shard}: {count:5d} keys  {bar}")
    ring_new = ConsistentHashRing(["shard-1", "shard-2", "shard-3", "shard-4", "shard-5"])
    moved = sum(1 for i in range(10000) if ring.get_node(f"user:{i}") != ring_new.get_node(f"user:{i}"))
    print(f"  Keys redistributed: {moved}/10000 ({moved/100:.1f}%)")
    print_lead_engineer("Exhaust caching and replicas before sharding.")

"""What does this function do?

It first prints theory for replication, sharding strategies, and consistency models.
Then it runs 500 requests against a single database, followed by 500 requests against a replicated database with a 70/30 read/write split. You will see latency improve because reads go to faster replicas.
Finally it creates a consistent hash ring with 4 shards, distributes 10,000 keys, then adds a 5th shard to show that far fewer than 50% of keys get redistributed.
"""

""" You should see the replication simulation show lower latency for the replicated DB compared to the single DB. 
    Then the consistent hashing output shows 10,000 keys distributed roughly evenly across 4 shards, with less 
    than 25% of keys redistributed when shard-5 is added.

    💡 Why is less than 25% redistribution significant?
    With naive modulo hashing, adding a 5th node to 4 nodes would remap roughly 80% of all keys. 
    Consistent hashing keeps it close to the ideal 20% (K/N, where K is total keys and N is new node count). 
    This means adding capacity does not require a massive data migration.

"""

# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    display_banner()

    while True:
        choice = input("\n  Select stage [1-6, T, Q]: ").strip().upper()

        if choice == "1":
            stage_single_server()
        elif choice == "2":
            stage_load_balancing()
        elif choice == "3":
            stage_caching()
        elif choice == "4":
            stage_database_scaling()
        elif choice == "5":
            pass
        elif choice == "6":
            pass
        elif choice == "T":
            pass
        elif choice == "Q":
            print("\n  Good luck in your interview!\n")
            break
        else:
            print("  Invalid choice. Enter 1-6, T, or Q.")

        input("\n  Press Enter to return to menu...")
        display_banner()


if __name__ == "__main__":
    main()

