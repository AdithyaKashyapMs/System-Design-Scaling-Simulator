# System Design Scaling Simulator

A hands-on Python simulator for learning and practicing system design concepts commonly discussed in Lead Engineer and system design interviews.

The simulator models how a system can evolve from a simple single-server architecture to a distributed architecture capable of supporting large-scale traffic.

## Features

- Simulates concurrent requests using Python threads
- Models server capacity and request processing
- Calculates important performance metrics:
  - Average latency
  - p50 latency
  - p95 latency
  - p99 latency
  - Throughput in requests per second
  - Error rate
- Demonstrates load testing concepts
- Includes architecture stages from a basic server setup to a distributed system
- Provides system design interview talking points
- Uses Python standard-library modules only

## Architecture Stages

The simulator is designed around the following scaling stages:

1. **Single Server**
   - Users connect to one application server and database.

2. **Load Balancer**
   - Traffic is distributed across multiple application servers.

3. **Caching Layer**
   - Frequently accessed data is served from a cache such as Redis.

4. **Database Scaling**
   - Uses database replication and sharding concepts.

5. **Advanced Distributed Architecture**
   - API gateway
   - Rate limiter
   - Load balancer
   - Application servers
   - Cache
   - Message queue
   - Background workers
   - Circuit breaker
   - Primary database
   - Read replicas
   - Database shards

## Requirements

- Python 3.12 or later
- macOS, Linux, or Windows
- No external Python packages are currently required

## Installation

Clone the repository:

```bash
git clone https://github.com/YOUR_USERNAME/system-design-scaling-simulator.git
cd system-design-scaling-simulator
```

Create a virtual environment:

### macOS or Linux

```bash
python3.12 -m venv .venv
source .venv/bin/activate
```

### Windows PowerShell

```powershell
py -3.12 -m venv .venv
.venv\Scripts\Activate.ps1
```

Verify the Python version:

```bash
python --version
```

## Running the Simulator

Run the application with:

```bash
python system_design_simulator.py
```

The simulator displays an interactive menu:

```text
Select stage [1-6, T, Q]:
```

Available options:

- `1-6` - Select a system scaling stage
- `T` - View system design theory
- `Q` - Quit the simulator

## Metrics

The simulator collects and displays the following metrics:

### Latency

- **Average latency** - The average time taken to process requests
- **p50 latency** - The median request latency
- **p95 latency** - The latency below which 95% of requests complete
- **p99 latency** - The latency below which 99% of requests complete

p95 and p99 latency are especially important for understanding the experience of users affected by slow requests.

### Throughput

Throughput measures how many requests the system can process per second.

```text
Throughput = Total Requests / Test Duration
```

### Error Rate

The error rate represents the percentage of requests that failed during the test.

```text
Error Rate = Failed Requests / Total Requests
```

## Project Structure

```text
system-design-scaling-simulator/
├── system_design_simulator.py
├── README.md
└── .venv/
```

The `.venv` directory should not be committed to GitHub. Add it to `.gitignore`:

```text
.venv/
__pycache__/
*.pyc
```

## Technologies Used

- Python 3.12
- `dataclasses`
- `threading`
- `ThreadPoolExecutor`
- `statistics`
- `time`
- `queue`
- `hashlib`
- `collections.OrderedDict`

## Learning Goals

This project is intended to help demonstrate and practice:

- Horizontal scaling
- Load balancing
- Concurrent request processing
- Server capacity limits
- Rate limiting
- Caching
- Message queues
- Database replication
- Database sharding
- Circuit breakers
- Latency and throughput analysis
- Performance testing
- System design trade-offs

## Current Status

The simulator currently includes the core data models, metrics collector, simulated server components, concurrent load-test runner, architecture diagrams, and interactive menu structure.

The stage-specific simulation scenarios are being expanded to provide increasingly realistic behavior for each architecture level.

## Future Improvements

- Implement complete behavior for each scaling stage
- Add configurable traffic volume
- Add cache hit and miss metrics
- Add rate limiter simulations
- Add message queue processing
- Add database read replicas
- Add database sharding behavior
- Add circuit breaker simulations
- Export test results to JSON or CSV
- Add graphs for latency and throughput
- Add automated tests
- Add a web-based visualization

## Contributing

Contributions are welcome.

1. Fork the repository
2. Create a feature branch:

```bash
git checkout -b feature/your-feature-name
```

3. Make your changes
4. Commit your changes:

```bash
git add .
git commit -m "Add your feature description"
```

5. Push the branch:

```bash
git push origin feature/your-feature-name
```

6. Open a pull request

## License

This project is available for educational and personal learning purposes.
```

Replace `YOUR_USERNAME` in the clone command with your GitHub username.
