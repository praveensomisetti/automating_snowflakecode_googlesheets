FROM public.ecr.aws/lambda/python:3.9

# Install required Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code and dependencies
COPY app.py SQL_code.sql moonlit-watch-443509-q5-52b2f5b01263.json .env ./

# Command to run the Lambda function
CMD ["app.lambda_handler"]
