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
    banner = """
    ==========================================
    System Design Scaling Simulator
    A hands-on learning tool for Lead Engineer system design interviews
    Simulates scaling from 100 to 1,000,000 users with real metrics.
    Run: python3 system_design_simulator.py
    ==========================================
    """
    print(banner)

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

# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    display_banner()

    while True:
        choice = input("\n  Select stage [1-6, T, Q]: ").strip().upper()

        if choice == "1":
            pass
        elif choice == "2":
            pass
        elif choice == "3":
            pass
        elif choice == "4":
            pass
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

