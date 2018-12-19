# bitflyer executions history function launcher for AWS Lambda(Python)



## How to develop

1. git clone https://github.com/codecomfort/bitflyer_data_getter_launcher.git
2. cd bitflyer_data_getter_launcher
3. pipenv install



## How to run(local)

1. (optional)Set your environment variables
   - export DISCORD_POST_URL="your discord webhooks address"
   - export INVOKE_FUNCTION_NAME="bitflyer_data_getter"
2. Edit sample data in lambda_function.py
   - event
3. pipenv shell
4. python lambda_function.py
5. then check your bitflyer_data_getter invoked

