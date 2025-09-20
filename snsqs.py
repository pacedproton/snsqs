import boto3
import json
import redis
import uuid
import threading
import time
import logging
from datetime import datetime

# Set up AWS clients
sqs = boto3.client('sqs')
sns = boto3.client('sns')

# Set up Redis client
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Global variable for queue URL
INPUT_QUEUE_URL = None

class RedisHandler(logging.Handler):
    def __init__(self, redis_client, list_name):
        super().__init__()
        self.redis_client = redis_client
        self.list_name = list_name

    def emit(self, record):
        log_entry = self.format(record)
        self.redis_client.lpush(self.list_name, log_entry)

def setup_logging(log_file='app.log', redis_list='app_logs'):
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    redis_handler = RedisHandler(redis_client, redis_list)
    redis_handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(redis_handler)

    return logger

logger = setup_logging()

def get_redis_logs(start=0, end=-1):
    logs = redis_client.lrange('app_logs', start, end)
    return [log.decode('utf-8') for log in logs]

def create_queue_if_not_exists(queue_name):
    global INPUT_QUEUE_URL
    try:
        response = sqs.get_queue_url(QueueName=queue_name)
        INPUT_QUEUE_URL = response['QueueUrl']
        print(f"[INFO] Queue '{queue_name}' already exists. URL: {INPUT_QUEUE_URL}")
        logger.info(f"Queue '{queue_name}' already exists. URL: {INPUT_QUEUE_URL}")
    except sqs.exceptions.QueueDoesNotExist:
        response = sqs.create_queue(QueueName=queue_name)
        INPUT_QUEUE_URL = response['QueueUrl']
        print(f"[INFO] Queue '{queue_name}' created. URL: {INPUT_QUEUE_URL}")
        logger.info(f"Queue '{queue_name}' created. URL: {INPUT_QUEUE_URL}")
    return INPUT_QUEUE_URL

def create_session():
    session_id = str(uuid.uuid4())
    topic_name = f'chat-session-{session_id}'
    response = sns.create_topic(Name=topic_name)
    topic_arn = response['TopicArn']
    
    redis_client.hset(f"session:{session_id}", mapping={
        "topic_arn": topic_arn,
        "created_at": time.time()
    })
    
    print(f"[INFO] New session created. Session ID: {session_id}, Topic ARN: {topic_arn}")
    logger.info(f"New session created. Session ID: {session_id}, Topic ARN: {topic_arn}")
    return session_id, topic_arn

def send_message(session_id, message):
    # Step 1: Console to Redis
    print(f"[INFO] Sending message from Console to Redis channel chat:{session_id}")
    redis_client.publish(f'chat:{session_id}', json.dumps({'session_id': session_id, 'message': message}))
    
    # Step 2: Store in Redis
    message_id = str(uuid.uuid4())
    redis_client.hset(f"message:{message_id}", mapping={
        "session_id": session_id,
        "content": message,
        "timestamp": time.time()
    })
    redis_client.rpush(f"session_messages:{session_id}", message_id)
    
    # Step 3: Redis to SQS
    print(f"[INFO] Sending message from Redis to SQS queue {INPUT_QUEUE_URL}")
    sqs.send_message(
        QueueUrl=INPUT_QUEUE_URL,
        MessageBody=json.dumps({'session_id': session_id, 'message': message})
    )

def receive_messages(session_id):
    pubsub = redis_client.pubsub()
    pubsub.subscribe(f'chat:{session_id}')
    print(f"[INFO] Subscribed to Redis channel chat:{session_id}")
    
    for message in pubsub.listen():
        if message['type'] == 'message':
            data = json.loads(message['data'])
            print(f"[INFO] Received from Redis channel chat:{session_id}: {data['message']}")
            print(f"Message: {data['message']}")  # This simulates the message being displayed to the user

def process_messages():
    pubsub = redis_client.pubsub()
    pubsub.psubscribe('chat:*')
    print("[INFO] Subscribed to all Redis chat channels")
    
    while True:
        message = pubsub.get_message()
        if message and message['type'] == 'pmessage':
            channel = message['channel'].decode()
            data = json.loads(message['data'])
            session_id = data['session_id']
            print(f"[INFO] Processing message from Redis channel {channel}")
            
            # Step 4: Redis to SNS
            topic_arn = redis_client.hget(f"session:{session_id}", "topic_arn")
            if topic_arn:
                print(f"[INFO] Publishing message from Redis to SNS topic: {topic_arn.decode()}")
                sns.publish(
                    TopicArn=topic_arn.decode(),
                    Message=json.dumps(data['message'])
                )
            else:
                print(f"[WARNING] No topic ARN found for session {session_id}")
        
        # Step 5: SQS to Redis (loopback)
        response = sqs.receive_message(
            QueueUrl=INPUT_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=1
        )
        
        if 'Messages' in response:
            for message in response['Messages']:
                body = json.loads(message['Body'])
                print(f"[INFO] Received message from SQS queue {INPUT_QUEUE_URL}")
                
                # Republish to Redis for loopback
                print(f"[INFO] Republishing message from SQS to Redis channel chat:{body['session_id']} (loopback)")
                redis_client.publish(f"chat:{body['session_id']}", json.dumps(body))
                
                # Delete the message from the queue
                sqs.delete_message(
                    QueueUrl=INPUT_QUEUE_URL,
                    ReceiptHandle=message['ReceiptHandle']
                )

def get_session_messages(session_id):
    message_ids = redis_client.lrange(f"session_messages:{session_id}", 0, -1)
    messages = []
    for message_id in message_ids:
        message_data = redis_client.hgetall(f"message:{message_id.decode()}")
        messages.append({
            "content": message_data[b"content"].decode(),
            "timestamp": float(message_data[b"timestamp"])
        })
    return messages

def main():
    global INPUT_QUEUE_URL
    INPUT_QUEUE_URL = create_queue_if_not_exists('asdfqueue')

    process_thread = threading.Thread(target=process_messages)
    process_thread.daemon = True
    process_thread.start()

    session_id, topic_arn = create_session()
    print(f"[INFO] Chat session started. Session ID: {session_id}")
    
    receive_thread = threading.Thread(target=receive_messages, args=(session_id,))
    receive_thread.daemon = True
    receive_thread.start()
    
    try:
        while True:
            message = input("Enter your message (or 'quit' to exit, 'history' to see session messages): ")
            if message.lower() == 'quit':
                break
            elif message.lower() == 'history':
                messages = get_session_messages(session_id)
                for msg in messages:
                    print(f"{time.ctime(msg['timestamp'])}: {msg['content']}")
                print("[INFO] Message history displayed")
            else:
                send_message(session_id, message)
    except Exception as e:
        print(f"[ERROR] An error occurred in the main loop: {str(e)}")
        logger.error(f"An error occurred in the main loop: {str(e)}", exc_info=True)
    finally:
        sns.delete_topic(TopicArn=topic_arn)
        print("[INFO] Session ended and SNS topic deleted.")

    print("[INFO] Displaying recent logs from Redis:")
    for log in get_redis_logs(start=-10):
        print(log)

if __name__ == "__main__":
    main()