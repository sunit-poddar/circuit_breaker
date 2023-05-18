# circuit_breaker
A circuit breaker in python to work in multiple pods with multiple workers. 

# Pre-requisites
1. python3
2. virtual env - `sudo pip3 install virtualenv`
3. django 4.2

# Steps
1. Create a virtual env - `virtualenv <env_name>` and activate `source <env_name>/bin/activate`
2. Install requirements - `pip3 install -r requirements`
3. Run migrations and server
4. Add APIs with the decorator `@circuit(<breaker_name>, failure_threshold=0.3, recovery_timeout=45)`

